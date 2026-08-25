"""Postgres persistence for rolling qualification epochs and certificates."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any, cast

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .qualification import (
    CONTAINED_REASONS,
    QualificationDecision,
    QualificationFact,
    QualificationState,
)

ConnectionFactory = Callable[[], psycopg.Connection[Any]]

_STATEMENT_TIMEOUT_MS = 5_000
_LOCK_TIMEOUT_MS = 1_000
_CERTIFICATE_REQUIRED_KEYS = frozenset(
    {
        "identity",
        "bounds",
        "counts",
        "slo",
        "contained_incidents",
        "recovery_actions",
        "evidence_digest",
        "policy_version",
    }
)
_IDENTITY_REQUIRED_KEYS = frozenset(
    {"epoch_id", "policy_version", "release_id", "config_id", "role_identity"}
)
_BOUNDS_REQUIRED_KEYS = frozenset(
    {"started_at", "qualified_at", "required_seconds", "max_gap_seconds"}
)


class QualificationStoreError(RuntimeError):
    """Base class for qualification persistence failures."""


class QualificationEpochConflict(QualificationStoreError):
    """An epoch write lost its state/version compare-and-swap fence."""


class QualificationCertificateConflict(QualificationStoreError):
    """A certificate identity was replayed with different immutable content."""


@dataclass(frozen=True, slots=True)
class QualificationEpochRecord:
    epoch_id: str
    state: str
    version: int
    identity_key: str
    policy_version: str
    release_id: str
    config_id: str
    role_identity: tuple[str, ...]
    started_at: datetime
    last_fact_at: datetime | None
    invalidated_at: datetime | None
    invalidation_reason: str | None
    qualified_at: datetime | None
    previous_epoch_id: str | None
    fact_digests: tuple[tuple[str, str], ...]
    contained_recoveries: tuple[str, ...]
    coverage_seconds: int
    max_gap_seconds: int
    progress_count: int | None
    successful_count: int | None
    evidence_digest: str | None
    required_seconds: int | None
    slo: dict[str, object]
    contained_incident_details: tuple[dict[str, object], ...]
    recovery_action_details: tuple[dict[str, object], ...]
    writer_id: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class QualificationCertificateRecord:
    certificate_id: str
    epoch_id: str
    identity_key: str
    policy_version: str
    release_id: str
    config_id: str
    role_identity: tuple[str, ...]
    started_at: datetime
    qualified_at: datetime
    payload: dict[str, object]
    canonical_payload: str
    payload_sha256: str
    certificate_digest: str
    evidence_digest: str
    created_at: datetime


def canonical_certificate_bytes(payload: Mapping[str, object]) -> bytes:
    """Return sorted compact UTF-8 JSON bytes for a certificate payload."""

    normalized = _validated_certificate_payload(payload)
    try:
        return json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("certificate payload must be JSON-safe") from exc


def certificate_digest(payload: Mapping[str, object]) -> str:
    """Return the SHA-256 digest of the canonical certificate bytes."""

    return sha256(canonical_certificate_bytes(payload)).hexdigest()


def qualification_certificate_payload(decision: QualificationDecision) -> dict[str, object]:
    """Build the canonical certificate payload from one qualified decision."""

    if type(decision) is not QualificationDecision:
        raise TypeError("decision must be QualificationDecision")
    if decision.state is not QualificationState.QUALIFIED or decision.qualified_at is None:
        raise QualificationCertificateConflict("qualification decision is not qualified")
    required_seconds = _decision_required_seconds(decision)
    counts = _decision_counts(decision)
    evidence = _decision_derived_evidence(decision)
    payload: dict[str, object] = {
        "bounds": {
            "max_gap_seconds": decision.max_gap_seconds,
            "qualified_at": decision.qualified_at.isoformat(),
            "required_seconds": required_seconds,
            "started_at": decision.started_at.isoformat(),
        },
        "contained_incidents": evidence["contained_incidents"],
        "counts": counts,
        "evidence_digest": evidence["evidence_digest"],
        "identity": {
            "config_id": decision.config_id,
            "epoch_id": decision.epoch_id,
            "policy_version": decision.policy_version,
            "release_id": decision.release_id,
            "role_identity": list(decision.role_identity),
        },
        "policy_version": decision.policy_version,
        "recovery_actions": evidence["recovery_actions"],
        "slo": evidence["slo"],
    }
    return _validated_certificate_payload(payload)


def start_qualification_epoch(
    connection_factory: ConnectionFactory,
    decision: QualificationDecision,
    *,
    writer_id: str | None = None,
) -> QualificationEpochRecord:
    """Create one epoch projection, returning an exact replay if unchanged."""

    if type(decision) is not QualificationDecision:
        raise TypeError("decision must be QualificationDecision")
    if writer_id is not None and (type(writer_id) is not str or not writer_id):
        raise ValueError("writer_id must be non-empty when provided")
    identity_key = _epoch_identity_key(decision)
    evidence = _decision_derived_evidence(decision)
    role_identity = list(decision.role_identity)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_timeouts(cursor)
        cursor.execute(
            """
            INSERT INTO m1_qualification_epochs (
                epoch_id, state, version, identity_key, policy_version, release_id,
                config_id, role_identity, started_at, last_fact_at, invalidated_at,
                invalidation_reason, qualified_at, previous_epoch_id, fact_digests,
                contained_recoveries, coverage_seconds, max_gap_seconds,
                progress_count, successful_count, evidence_digest, required_seconds,
                slo, contained_incident_details, recovery_action_details, writer_id
            ) VALUES (
                %s, %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (epoch_id) DO NOTHING
            """,
            (
                decision.epoch_id,
                decision.state.value,
                identity_key,
                decision.policy_version,
                decision.release_id,
                decision.config_id,
                Jsonb(role_identity),
                _utc(decision.started_at, "started_at"),
                _utc_or_none(decision.last_fact_at, "last_fact_at"),
                _utc_or_none(decision.invalidated_at, "invalidated_at"),
                decision.invalidation_reason,
                _utc_or_none(decision.qualified_at, "qualified_at"),
                decision.previous_epoch_id,
                Jsonb([list(item) for item in decision.fact_digests]),
                Jsonb(list(decision.contained_recoveries)),
                decision.coverage_seconds,
                decision.max_gap_seconds,
                decision.progress_count,
                decision.successful_count,
                evidence["evidence_digest"],
                evidence["required_seconds"],
                Jsonb(evidence["slo"]),
                Jsonb(evidence["contained_incidents"]),
                Jsonb(evidence["recovery_actions"]),
                writer_id,
            ),
        )
        persisted = _fetch_epoch_cursor(cursor, epoch_id=decision.epoch_id, for_update=False)
        if persisted is None:
            raise QualificationStoreError("qualification epoch insert returned no row")
        if not _epoch_matches_decision(persisted, decision, identity_key):
            raise QualificationEpochConflict("qualification epoch identity conflicts")
        return persisted


def transition_qualification_epoch(
    connection_factory: ConnectionFactory,
    *,
    expected_epoch_id: str,
    expected_state: QualificationState | str,
    expected_version: int,
    next_decision: QualificationDecision,
    writer_id: str,
) -> QualificationEpochRecord:
    """Persist one state transition behind an exact state/version CAS fence."""

    _require_nonempty(expected_epoch_id=expected_epoch_id, writer_id=writer_id)
    if type(next_decision) is not QualificationDecision:
        raise TypeError("next_decision must be QualificationDecision")
    if next_decision.epoch_id != expected_epoch_id:
        raise QualificationEpochConflict("qualification epoch ID conflicts")
    if type(expected_version) is not int or expected_version <= 0:
        raise ValueError("expected_version must be positive")
    state_value = _state_value(expected_state)
    identity_key = _epoch_identity_key(next_decision)
    evidence = _decision_derived_evidence(next_decision)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_timeouts(cursor)
        cursor.execute(
            """
            UPDATE m1_qualification_epochs
            SET state = %s,
                version = version + 1,
                identity_key = %s,
                policy_version = %s,
                release_id = %s,
                config_id = %s,
                role_identity = %s,
                started_at = %s,
                last_fact_at = %s,
                invalidated_at = %s,
                invalidation_reason = %s,
                qualified_at = %s,
                previous_epoch_id = %s,
                fact_digests = %s,
                contained_recoveries = %s,
                coverage_seconds = %s,
                max_gap_seconds = %s,
                progress_count = %s,
                successful_count = %s,
                evidence_digest = %s,
                required_seconds = %s,
                slo = %s,
                contained_incident_details = %s,
                recovery_action_details = %s,
                writer_id = %s,
                updated_at = clock_timestamp()
            WHERE epoch_id = %s AND state = %s AND version = %s
            RETURNING *
            """,
            (
                next_decision.state.value,
                identity_key,
                next_decision.policy_version,
                next_decision.release_id,
                next_decision.config_id,
                Jsonb(list(next_decision.role_identity)),
                _utc(next_decision.started_at, "started_at"),
                _utc_or_none(next_decision.last_fact_at, "last_fact_at"),
                _utc_or_none(next_decision.invalidated_at, "invalidated_at"),
                next_decision.invalidation_reason,
                _utc_or_none(next_decision.qualified_at, "qualified_at"),
                next_decision.previous_epoch_id,
                Jsonb([list(item) for item in next_decision.fact_digests]),
                Jsonb(list(next_decision.contained_recoveries)),
                next_decision.coverage_seconds,
                next_decision.max_gap_seconds,
                next_decision.progress_count,
                next_decision.successful_count,
                evidence["evidence_digest"],
                evidence["required_seconds"],
                Jsonb(evidence["slo"]),
                Jsonb(evidence["contained_incidents"]),
                Jsonb(evidence["recovery_actions"]),
                writer_id,
                expected_epoch_id,
                state_value,
                expected_version,
            ),
        )
        row = cursor.fetchone()
        if row is None:
            raise QualificationEpochConflict("qualification epoch state/version CAS failed")
        return _epoch_from_row(row)


def read_qualification_epoch(
    connection_factory: ConnectionFactory,
    *,
    epoch_id: str,
) -> QualificationEpochRecord | None:
    """Read one epoch projection in a bounded read-only transaction."""

    _require_nonempty(epoch_id=epoch_id)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        _set_timeouts(cursor)
        return _fetch_epoch_cursor(cursor, epoch_id=epoch_id, for_update=False)


def insert_qualification_certificate(
    connection_factory: ConnectionFactory,
    *,
    decision: QualificationDecision,
) -> QualificationCertificateRecord:
    """Append one immutable certificate derived from a qualified decision.

    The caller cannot supply counts, SLO, bounds, or evidence digests.  Those
    values are rebuilt from ``decision`` and checked against the persisted epoch
    before the SECURITY DEFINER insert function is invoked.
    """

    if type(decision) is not QualificationDecision:
        raise TypeError("decision must be QualificationDecision")
    normalized = qualification_certificate_payload(decision)
    identity = cast(Mapping[str, object], normalized["identity"])
    epoch_id = cast(str, identity["epoch_id"])
    canonical = canonical_certificate_bytes(normalized)
    canonical_text = canonical.decode("utf-8")
    digest = sha256(canonical).hexdigest()
    identity_key = _certificate_identity_key(normalized)
    certificate_id = f"qualification-certificate:{digest}"
    evidence_digest = cast(str, normalized["evidence_digest"])
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        _set_timeouts(cursor)
        epoch = _fetch_epoch_cursor(cursor, epoch_id=epoch_id, for_update=True)
        if epoch is None:
            raise QualificationStoreError(f"qualification epoch {epoch_id!r} is missing")
        if epoch.state != QualificationState.QUALIFIED.value or epoch.qualified_at is None:
            raise QualificationCertificateConflict("qualification epoch is not qualified")
        if not _epoch_matches_decision(epoch, decision, _epoch_identity_key(decision)):
            raise QualificationCertificateConflict("qualification decision conflicts with epoch")
        _assert_certificate_matches_epoch(normalized, epoch)
        existing = _fetch_certificate_by_identity_cursor(cursor, identity_key=identity_key)
        if existing is not None:
            if (
                existing.certificate_digest != digest
                or existing.payload != normalized
                or existing.canonical_payload != canonical_text
            ):
                raise QualificationCertificateConflict("qualification certificate conflicts")
            return existing
        cursor.execute(
            """
            SELECT *
            FROM m1_insert_qualification_certificate(
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                certificate_id,
                epoch_id,
                identity_key,
                cast(str, identity["policy_version"]),
                cast(str, identity["release_id"]),
                cast(str, identity["config_id"]),
                Jsonb(cast(list[object], identity["role_identity"])),
                epoch.started_at,
                epoch.qualified_at,
                Jsonb(normalized),
                canonical_text,
                digest,
                digest,
                evidence_digest,
            ),
        )
        row = cursor.fetchone()
        if row is not None:
            return _certificate_from_row(row)
        raced = _fetch_certificate_by_identity_cursor(cursor, identity_key=identity_key)
        if raced is None:
            raise QualificationStoreError("qualification certificate raced without a row")
        if (
            raced.certificate_digest != digest
            or raced.payload != normalized
            or raced.canonical_payload != canonical_text
        ):
            raise QualificationCertificateConflict("qualification certificate conflicts")
        return raced


def read_qualification_certificate(
    connection_factory: ConnectionFactory,
    *,
    certificate_id: str,
) -> QualificationCertificateRecord | None:
    """Read and reverify one immutable certificate before returning it."""

    _require_nonempty(certificate_id=certificate_id)
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        _set_timeouts(cursor)
        cursor.execute(
            "SELECT * FROM m1_qualification_certificates WHERE certificate_id = %s",
            (certificate_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        record = _certificate_from_row(row)
        _verify_certificate_record(cursor, record)
        return record


def list_qualification_certificates(
    connection_factory: ConnectionFactory,
    *,
    limit: int = 20,
) -> tuple[QualificationCertificateRecord, ...]:
    """List recent certificates, reverifying every record before returning."""

    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("limit must be in 1..100")
    with connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        _set_timeouts(cursor)
        cursor.execute(
            """
            SELECT *
            FROM m1_qualification_certificates
            ORDER BY created_at DESC, certificate_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        records = tuple(_certificate_from_row(row) for row in cursor.fetchall())
        for record in records:
            _verify_certificate_record(cursor, record)
        return records


def _decision_derived_evidence(decision: QualificationDecision) -> dict[str, object]:
    required_seconds = (
        _decision_required_seconds(decision)
        if decision.state is QualificationState.QUALIFIED and decision.qualified_at is not None
        else None
    )
    contained_incidents = _contained_incident_details(decision)
    recovery_actions = _recovery_action_details(decision)
    evidence_payload = {
        "contained_incidents": contained_incidents,
        "epoch_id": decision.epoch_id,
        "fact_digests": [list(item) for item in decision.fact_digests],
        "recovery_actions": recovery_actions,
    }
    return {
        "contained_incidents": contained_incidents,
        "evidence_digest": _canonical_sha256(evidence_payload),
        "recovery_actions": recovery_actions,
        "required_seconds": required_seconds,
        "slo": _decision_slo(decision, required_seconds=required_seconds),
    }


def _decision_required_seconds(decision: QualificationDecision) -> int:
    if decision.qualified_at is None:
        raise QualificationCertificateConflict("qualification decision has no qualified_at")
    required_seconds = int((decision.qualified_at - decision.started_at).total_seconds())
    if required_seconds <= 0:
        raise QualificationCertificateConflict("qualification decision has invalid bounds")
    if decision.coverage_seconds < required_seconds:
        raise QualificationCertificateConflict("qualification coverage is incomplete")
    return required_seconds


def _decision_counts(decision: QualificationDecision) -> dict[str, int]:
    progress_count = decision.progress_count
    successful_count = decision.successful_count
    if type(progress_count) is not int or type(successful_count) is not int:
        raise QualificationCertificateConflict("qualified counts must be non-null integers")
    if progress_count < 0 or successful_count < 0:
        raise QualificationCertificateConflict("qualified counts must be non-negative")
    return {
        "progress_count": progress_count,
        "successful_count": successful_count,
    }


def _decision_slo(
    decision: QualificationDecision,
    *,
    required_seconds: int | None,
) -> dict[str, object]:
    return {
        "evidence_gap_seconds": decision.max_gap_seconds,
        "evidence_gap_status": "pass",
        "freshness": "pass",
        "recovery": "pass",
        "required_seconds": required_seconds,
    }


def _contained_incident_details(decision: QualificationDecision) -> list[dict[str, object]]:
    return [
        _contained_fact_payload(fact)
        for fact in decision.facts
        if fact.reason in CONTAINED_REASONS
    ]


def _recovery_action_details(decision: QualificationDecision) -> list[dict[str, object]]:
    return [
        {
            "action": fact.reason,
            "fact_id": fact.fact_id,
            "observed_at": fact.observed_at.isoformat(),
            "recovery_duration_seconds": fact.recovery_duration_seconds,
            "recovery_slo_seconds": fact.recovery_slo_seconds,
            "resolved": fact.resolved,
        }
        for fact in decision.facts
        if fact.reason in CONTAINED_REASONS
    ]


def _contained_fact_payload(fact: QualificationFact) -> dict[str, object]:
    payload: dict[str, object] = {
        "fact_id": fact.fact_id,
        "observed_at": fact.observed_at.isoformat(),
        "reason": fact.reason,
        "recovery_duration_seconds": fact.recovery_duration_seconds,
        "recovery_slo_seconds": fact.recovery_slo_seconds,
        "resolved": fact.resolved,
    }
    if fact.signature is not None:
        payload["signature"] = fact.signature
    return payload


def _set_timeouts(cursor: psycopg.Cursor[Any]) -> None:
    cursor.execute(
        sql.SQL("SET LOCAL statement_timeout = {}").format(
            sql.Literal(f"{_STATEMENT_TIMEOUT_MS}ms")
        )
    )
    cursor.execute(
        sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(f"{_LOCK_TIMEOUT_MS}ms"))
    )


def _require_nonempty(**values: str) -> None:
    for key, value in values.items():
        if type(value) is not str or not value:
            raise ValueError(f"{key} must be non-empty")


def _utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _utc_or_none(value: datetime | None, field: str) -> datetime | None:
    return None if value is None else _utc(value, field)


def _state_value(value: QualificationState | str) -> str:
    if type(value) is QualificationState:
        return value.value
    if type(value) is str and value in {state.value for state in QualificationState}:
        return value
    raise ValueError("expected_state must be a QualificationState value")


def _epoch_identity_key(decision: QualificationDecision) -> str:
    return _canonical_sha256(
        {
            "config_id": decision.config_id,
            "policy_version": decision.policy_version,
            "release_id": decision.release_id,
            "role_identity": list(decision.role_identity),
        }
    )


def _certificate_identity_key(payload: Mapping[str, object]) -> str:
    return _canonical_sha256(
        {
            "bounds": payload["bounds"],
            "identity": payload["identity"],
        }
    )


def _canonical_sha256(payload: Mapping[str, object]) -> str:
    return sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _validated_certificate_payload(payload: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(payload, Mapping):
        raise TypeError("certificate payload must be a mapping")
    missing = sorted(_CERTIFICATE_REQUIRED_KEYS.difference(payload.keys()))
    if missing:
        raise ValueError(f"certificate payload must include {', '.join(missing)}")
    normalized = _normalize_json_value(payload)
    assert isinstance(normalized, dict)
    identity = normalized["identity"]
    bounds = normalized["bounds"]
    if not isinstance(identity, dict):
        raise ValueError("certificate identity must be an object")
    if not isinstance(bounds, dict):
        raise ValueError("certificate bounds must be an object")
    missing_identity = sorted(_IDENTITY_REQUIRED_KEYS.difference(identity.keys()))
    if missing_identity:
        raise ValueError(f"certificate identity must include {', '.join(missing_identity)}")
    missing_bounds = sorted(_BOUNDS_REQUIRED_KEYS.difference(bounds.keys()))
    if missing_bounds:
        raise ValueError(f"certificate bounds must include {', '.join(missing_bounds)}")
    for key in ("epoch_id", "policy_version", "release_id", "config_id"):
        if type(identity[key]) is not str or not identity[key]:
            raise ValueError(f"certificate identity {key} must be a non-empty string")
    if normalized["policy_version"] != identity["policy_version"]:
        raise ValueError("certificate policy_version must match identity policy_version")
    roles = identity["role_identity"]
    if (
        not isinstance(roles, list)
        or not roles
        or any(type(role) is not str or not role for role in roles)
        or len(set(roles)) != len(roles)
        or roles != sorted(roles)
    ):
        raise ValueError("certificate role_identity must be a sorted non-empty string list")
    for key in ("started_at", "qualified_at"):
        if type(bounds[key]) is not str or not bounds[key]:
            raise ValueError(f"certificate bounds {key} must be an ISO string")
    started_at = _parse_utc_iso(cast(str, bounds["started_at"]), "started_at")
    qualified_at = _parse_utc_iso(cast(str, bounds["qualified_at"]), "qualified_at")
    if qualified_at <= started_at:
        raise ValueError("certificate bounds must be increasing")
    for key in ("required_seconds", "max_gap_seconds"):
        if type(bounds[key]) is not int or bounds[key] < 0:
            raise ValueError(f"certificate bounds {key} must be a non-negative integer")
    if type(normalized["evidence_digest"]) is not str or not _is_sha256(
        normalized["evidence_digest"]
    ):
        raise ValueError("certificate evidence_digest must be a lowercase SHA-256 hex digest")
    for key in ("counts", "slo"):
        if not isinstance(normalized[key], dict):
            raise ValueError(f"certificate {key} must be an object")
    counts = cast(Mapping[str, object], normalized["counts"])
    for key in ("progress_count", "successful_count"):
        if type(counts.get(key)) is not int or cast(int, counts[key]) < 0:
            raise ValueError(f"certificate counts {key} must be a non-negative integer")
    slo = cast(Mapping[str, object], normalized["slo"])
    for key in (
        "evidence_gap_seconds",
        "evidence_gap_status",
        "freshness",
        "recovery",
        "required_seconds",
    ):
        if key not in slo:
            raise ValueError(f"certificate slo must include {key}")
    for key in ("contained_incidents", "recovery_actions"):
        if not isinstance(normalized[key], list):
            raise ValueError(f"certificate {key} must be a list")
    return normalized


def _normalize_json_value(value: object) -> object:
    if value is None or type(value) in (str, bool):
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        raise ValueError("certificate payload must be JSON-safe without floats")
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("certificate payload JSON object keys must be strings")
            normalized[key] = _normalize_json_value(item)
        return normalized
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    raise ValueError("certificate payload must be JSON-safe canonical JSON values")


def _parse_utc_iso(value: str, field: str) -> datetime:
    if not (value.endswith("+00:00") or value.endswith("Z")):
        raise ValueError(f"certificate bounds {field} must be UTC")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"certificate bounds {field} must be UTC")
    return parsed.astimezone(UTC)


def _is_sha256(value: object) -> bool:
    return type(value) is str and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _assert_certificate_matches_epoch(
    payload: Mapping[str, object],
    epoch: QualificationEpochRecord,
) -> None:
    qualified_at = epoch.qualified_at
    if qualified_at is None:
        raise QualificationCertificateConflict("qualification epoch is not qualified")
    identity = cast(Mapping[str, object], payload["identity"])
    bounds = cast(Mapping[str, object], payload["bounds"])
    if (
        identity["epoch_id"] != epoch.epoch_id
        or identity["policy_version"] != epoch.policy_version
        or identity["release_id"] != epoch.release_id
        or identity["config_id"] != epoch.config_id
        or tuple(cast(Sequence[str], identity["role_identity"])) != epoch.role_identity
    ):
        raise QualificationCertificateConflict("qualification certificate identity conflicts")
    if (
        bounds["started_at"] != epoch.started_at.isoformat()
        or bounds["qualified_at"] != qualified_at.isoformat()
    ):
        raise QualificationCertificateConflict("qualification certificate bounds conflict")
    counts = cast(Mapping[str, object], payload["counts"])
    if (
        bounds["required_seconds"] != epoch.required_seconds
        or bounds["max_gap_seconds"] != epoch.max_gap_seconds
        or counts["progress_count"] != epoch.progress_count
        or counts["successful_count"] != epoch.successful_count
        or payload["slo"] != epoch.slo
        or payload["contained_incidents"] != list(epoch.contained_incident_details)
        or payload["recovery_actions"] != list(epoch.recovery_action_details)
        or payload["evidence_digest"] != epoch.evidence_digest
    ):
        raise QualificationCertificateConflict("qualification certificate payload conflicts")


def _verify_certificate_record(
    cursor: psycopg.Cursor[Any],
    record: QualificationCertificateRecord,
) -> None:
    try:
        payload = json.loads(record.canonical_payload)
    except json.JSONDecodeError as exc:
        raise QualificationCertificateConflict(
            "qualification certificate canonical JSON invalid"
        ) from exc
    if not isinstance(payload, Mapping):
        raise QualificationCertificateConflict("qualification certificate canonical JSON invalid")
    normalized = _validated_certificate_payload(cast(Mapping[str, object], payload))
    canonical_bytes = canonical_certificate_bytes(normalized)
    digest = sha256(canonical_bytes).hexdigest()
    if record.canonical_payload.encode("utf-8") != canonical_bytes:
        raise QualificationCertificateConflict("qualification certificate canonical bytes conflict")
    if (
        record.payload != normalized
        or record.payload_sha256 != digest
        or record.certificate_digest != digest
    ):
        raise QualificationCertificateConflict("qualification certificate digest conflict")
    epoch = _fetch_epoch_cursor(cursor, epoch_id=record.epoch_id, for_update=False)
    if epoch is None:
        raise QualificationCertificateConflict("qualification certificate epoch is missing")
    _assert_certificate_matches_epoch(normalized, epoch)
    if (
        record.policy_version != epoch.policy_version
        or record.release_id != epoch.release_id
        or record.config_id != epoch.config_id
        or record.role_identity != epoch.role_identity
        or record.started_at != epoch.started_at
        or record.qualified_at != epoch.qualified_at
        or record.evidence_digest != epoch.evidence_digest
    ):
        raise QualificationCertificateConflict("qualification certificate columns conflict")


def _epoch_matches_decision(
    record: QualificationEpochRecord,
    decision: QualificationDecision,
    identity_key: str,
) -> bool:
    evidence = _decision_derived_evidence(decision)
    return (
        record.state == decision.state.value
        and record.identity_key == identity_key
        and record.policy_version == decision.policy_version
        and record.release_id == decision.release_id
        and record.config_id == decision.config_id
        and record.role_identity == decision.role_identity
        and record.started_at == decision.started_at
        and record.last_fact_at == decision.last_fact_at
        and record.invalidated_at == decision.invalidated_at
        and record.invalidation_reason == decision.invalidation_reason
        and record.qualified_at == decision.qualified_at
        and record.previous_epoch_id == decision.previous_epoch_id
        and record.fact_digests == decision.fact_digests
        and record.contained_recoveries == decision.contained_recoveries
        and record.coverage_seconds == decision.coverage_seconds
        and record.max_gap_seconds == decision.max_gap_seconds
        and record.progress_count == decision.progress_count
        and record.successful_count == decision.successful_count
        and record.evidence_digest == evidence["evidence_digest"]
        and record.required_seconds == evidence["required_seconds"]
        and record.slo == evidence["slo"]
        and list(record.contained_incident_details) == evidence["contained_incidents"]
        and list(record.recovery_action_details) == evidence["recovery_actions"]
    )


def _fetch_epoch_cursor(
    cursor: psycopg.Cursor[Any],
    *,
    epoch_id: str,
    for_update: bool,
) -> QualificationEpochRecord | None:
    cursor.execute(
        "SELECT * FROM m1_qualification_epochs WHERE epoch_id = %s"
        + (" FOR UPDATE" if for_update else ""),
        (epoch_id,),
    )
    row = cursor.fetchone()
    return None if row is None else _epoch_from_row(row)


def _fetch_certificate_by_identity_cursor(
    cursor: psycopg.Cursor[Any],
    *,
    identity_key: str,
) -> QualificationCertificateRecord | None:
    cursor.execute(
        "SELECT * FROM m1_qualification_certificates WHERE identity_key = %s FOR UPDATE",
        (identity_key,),
    )
    row = cursor.fetchone()
    return None if row is None else _certificate_from_row(row)


def _epoch_from_row(row: Mapping[str, object]) -> QualificationEpochRecord:
    return QualificationEpochRecord(
        epoch_id=str(row["epoch_id"]),
        state=str(row["state"]),
        version=int(cast(int, row["version"])),
        identity_key=str(row["identity_key"]),
        policy_version=str(row["policy_version"]),
        release_id=str(row["release_id"]),
        config_id=str(row["config_id"]),
        role_identity=tuple(cast(Sequence[str], row["role_identity"])),
        started_at=_utc(cast(datetime, row["started_at"]), "started_at"),
        last_fact_at=_utc_or_none(cast(datetime | None, row["last_fact_at"]), "last_fact_at"),
        invalidated_at=_utc_or_none(
            cast(datetime | None, row["invalidated_at"]), "invalidated_at"
        ),
        invalidation_reason=(
            None if row["invalidation_reason"] is None else str(row["invalidation_reason"])
        ),
        qualified_at=_utc_or_none(cast(datetime | None, row["qualified_at"]), "qualified_at"),
        previous_epoch_id=(
            None if row["previous_epoch_id"] is None else str(row["previous_epoch_id"])
        ),
        fact_digests=tuple(
            (str(item[0]), str(item[1]))
            for item in cast(Sequence[Sequence[object]], row["fact_digests"])
        ),
        contained_recoveries=tuple(
            str(item) for item in cast(Sequence[object], row["contained_recoveries"])
        ),
        coverage_seconds=int(cast(int, row["coverage_seconds"])),
        max_gap_seconds=int(cast(int, row["max_gap_seconds"])),
        progress_count=(
            None if row["progress_count"] is None else int(cast(int, row["progress_count"]))
        ),
        successful_count=(
            None if row["successful_count"] is None else int(cast(int, row["successful_count"]))
        ),
        evidence_digest=None if row["evidence_digest"] is None else str(row["evidence_digest"]),
        required_seconds=(
            None if row["required_seconds"] is None else int(cast(int, row["required_seconds"]))
        ),
        slo=dict(cast(Mapping[str, object], row["slo"])),
        contained_incident_details=tuple(
            dict(cast(Mapping[str, object], item))
            for item in cast(Sequence[object], row["contained_incident_details"])
        ),
        recovery_action_details=tuple(
            dict(cast(Mapping[str, object], item))
            for item in cast(Sequence[object], row["recovery_action_details"])
        ),
        writer_id=None if row["writer_id"] is None else str(row["writer_id"]),
        created_at=_utc(cast(datetime, row["created_at"]), "created_at"),
        updated_at=_utc(cast(datetime, row["updated_at"]), "updated_at"),
    )


def _certificate_from_row(row: Mapping[str, object]) -> QualificationCertificateRecord:
    return QualificationCertificateRecord(
        certificate_id=str(row["certificate_id"]),
        epoch_id=str(row["epoch_id"]),
        identity_key=str(row["identity_key"]),
        policy_version=str(row["policy_version"]),
        release_id=str(row["release_id"]),
        config_id=str(row["config_id"]),
        role_identity=tuple(cast(Sequence[str], row["role_identity"])),
        started_at=_utc(cast(datetime, row["started_at"]), "started_at"),
        qualified_at=_utc(cast(datetime, row["qualified_at"]), "qualified_at"),
        payload=dict(cast(Mapping[str, object], row["payload"])),
        canonical_payload=str(row["canonical_payload"]),
        payload_sha256=str(row["payload_sha256"]),
        certificate_digest=str(row["certificate_digest"]),
        evidence_digest=str(row["evidence_digest"]),
        created_at=_utc(cast(datetime, row["created_at"]), "created_at"),
    )


__all__ = [
    "QualificationCertificateConflict",
    "QualificationCertificateRecord",
    "QualificationEpochConflict",
    "QualificationEpochRecord",
    "QualificationStoreError",
    "canonical_certificate_bytes",
    "certificate_digest",
    "insert_qualification_certificate",
    "list_qualification_certificates",
    "qualification_certificate_payload",
    "read_qualification_certificate",
    "read_qualification_epoch",
    "start_qualification_epoch",
    "transition_qualification_epoch",
]
