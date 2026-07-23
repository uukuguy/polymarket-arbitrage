"""Pure, deterministic verdicts for exact L3 soak evidence windows.

This module deliberately has no storage or CLI dependency.  A caller must bind an
explicit immutable manifest to one snapshot-consistent :class:`EvidenceWindow`.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Any
from uuid import UUID

from polyarb.observation.l3_evidence import (
    EVIDENCE_TABLES,
    AcceptanceConfig,
    EvidenceWindow,
    HealthStatus,
    PromoteStatus,
    RuntimeEventKind,
)


class VerdictStatus(StrEnum):
    PASS = "PASS"
    NOT_CLOSED = "NOT-CLOSED"


@dataclass(frozen=True, slots=True, order=True)
class VerdictReason:
    code: str
    message: str

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code or len(self.code) > 64:
            raise ValueError("verdict reason code must be 1..64 characters")
        if not isinstance(self.message, str) or not self.message:
            raise ValueError("verdict reason message must be non-empty")


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("canonical datetimes must be timezone-aware UTC")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise TypeError("canonical JSON rejects non-finite floats")
        return value
    if isinstance(value, datetime):
        return _utc_text(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON mapping keys must be strings")
            normalized[key] = _canonical_value(item)
        return normalized
    if isinstance(value, (set, frozenset)):
        normalized_items = [_canonical_value(item) for item in value]
        return sorted(
            normalized_items,
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ),
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical_value(item) for item in value]
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def _canonical_json_bytes(value: Mapping[str, object] | Sequence[object]) -> bytes:
    """The only serializer used for every manifest, interval, row and report hash."""
    return json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: Mapping[str, object] | Sequence[object]) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _require_sha256(name: str, value: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str")
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True)
class ManifestReport:
    checkpoint: str
    start: datetime
    end: datetime
    path: str

    def __post_init__(self) -> None:
        if self.checkpoint not in {"T+0", "T+6", "T+12", "T+18", "T+24"}:
            raise ValueError("unsupported manifest checkpoint")
        _utc_text(self.start)
        _utc_text(self.end)
        if self.start > self.end:
            raise ValueError("manifest report start must not follow end")
        if not isinstance(self.path, str) or not self.path:
            raise ValueError("manifest report path must be non-empty")

    def to_dict(self) -> dict[str, object]:
        return {
            "checkpoint": self.checkpoint,
            "start": self.start,
            "end": self.end,
            "path": self.path,
        }


@dataclass(frozen=True, slots=True)
class SoakManifest:
    schema_version: int
    t0: datetime
    t24: datetime
    reports: tuple[ManifestReport, ...]
    boot_id: UUID
    machine_id: str
    machine_version: str
    image_ref: str
    image_digest: str
    release_id: str
    code_version: str
    mapping_hash: str
    acceptance_config: AcceptanceConfig
    acceptance_config_hash: str
    allowed_event_kind_exceptions: frozenset[RuntimeEventKind] = frozenset()

    def __post_init__(self) -> None:
        if isinstance(self.schema_version, bool) or not isinstance(self.schema_version, int):
            raise TypeError("schema_version must be an int")
        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")
        _utc_text(self.t0)
        _utc_text(self.t24)
        if self.t24 - self.t0 != timedelta(hours=24):
            raise ValueError("manifest T24 must be exactly 24 hours after T0")
        if not isinstance(self.boot_id, UUID):
            raise TypeError("boot_id must be a UUID")
        for name in (
            "machine_id",
            "machine_version",
            "image_ref",
            "release_id",
            "code_version",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        for name in ("image_digest", "mapping_hash", "acceptance_config_hash"):
            _require_sha256(name, getattr(self, name))
        if not self.image_ref.endswith(f"@sha256:{self.image_digest}"):
            raise ValueError("image_ref digest must equal image_digest")
        if type(self.acceptance_config) is not AcceptanceConfig:
            raise TypeError("acceptance_config must be an exact AcceptanceConfig")

        reports = tuple(self.reports)
        expected = ("T+0", "T+6", "T+12", "T+18", "T+24")
        if tuple(report.checkpoint for report in reports) != expected:
            raise ValueError("manifest must contain ordered T+0/T+6/T+12/T+18/T+24 reports")
        expected_ends = (
            self.t0 + timedelta(seconds=self.acceptance_config.sample_interval_s),
            self.t0 + timedelta(hours=6),
            self.t0 + timedelta(hours=12),
            self.t0 + timedelta(hours=18),
            self.t24,
        )
        if any(
            report.start != self.t0 or report.end != expected_end
            for report, expected_end in zip(reports, expected_ends, strict=True)
        ):
            raise ValueError("manifest report bounds must be exact cumulative checkpoints")
        if len({report.path for report in reports}) != 5:
            raise ValueError("manifest report paths must be unique")
        object.__setattr__(self, "reports", reports)

        exceptions = frozenset(self.allowed_event_kind_exceptions)
        if any(type(kind) is not RuntimeEventKind for kind in exceptions):
            raise TypeError("event exceptions must be exact RuntimeEventKind values")
        if not exceptions <= DISALLOWED_EVENT_KINDS:
            raise ValueError("only default disallowed event kinds may be excepted")
        object.__setattr__(self, "allowed_event_kind_exceptions", exceptions)

    @property
    def soak_hash(self) -> str:
        return _sha256(
            {
                "start": self.t0,
                "boot_id": self.boot_id,
                "machine_id": self.machine_id,
                "machine_version": self.machine_version,
                "image_ref": self.image_ref,
                "image_digest": self.image_digest,
                "release_id": self.release_id,
                "code_version": self.code_version,
                "mapping_hash": self.mapping_hash,
                "acceptance_config_hash": self.acceptance_config_hash,
                "allowed_event_kind_exceptions": sorted(
                    kind.value for kind in self.allowed_event_kind_exceptions
                ),
            }
        )

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "t0": self.t0,
            "t24": self.t24,
            "reports": [report.to_dict() for report in self.reports],
            "boot_id": self.boot_id,
            "machine_id": self.machine_id,
            "machine_version": self.machine_version,
            "image_ref": self.image_ref,
            "image_digest": self.image_digest,
            "release_id": self.release_id,
            "code_version": self.code_version,
            "mapping_hash": self.mapping_hash,
            "acceptance_config": {
                field.name: getattr(self.acceptance_config, field.name)
                for field in fields(self.acceptance_config)
            },
            "acceptance_config_hash": self.acceptance_config_hash,
            "allowed_event_kind_exceptions": sorted(
                kind.value for kind in self.allowed_event_kind_exceptions
            ),
            "soak_hash": self.soak_hash,
        }

    @property
    def manifest_hash(self) -> str:
        return _sha256(self.hash_payload())

    def to_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "manifest_hash": self.manifest_hash}


DISALLOWED_EVENT_KINDS = frozenset(
    {
        RuntimeEventKind.WATCHDOG_STALE,
        RuntimeEventKind.RECONNECT_FAILED,
        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED,
        RuntimeEventKind.EVIDENCE_WRITER_FAILED,
    }
)


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(
        {
            key: _freeze_mapping(item) if isinstance(item, Mapping) else item
            for key, item in value.items()
        }
    )


@dataclass(frozen=True, slots=True)
class L3SoakReport:
    manifest_hash: str
    soak_hash: str
    interval_hash: str
    raw_row_set_hash: str
    start: datetime
    end: datetime
    require_24h: bool
    boot_id: UUID
    machine_id: str
    machine_version: str
    image_ref: str
    image_digest: str
    release_id: str
    code_version: str
    mapping_hash: str
    acceptance_config_hash: str
    row_counts: Mapping[str, int]
    expected_promoter_ticks: int
    recorded_promoter_ticks: int
    max_sample_gap_seconds: float | None
    max_promoter_start_gap_seconds: float | None
    minimum_cardinality: Mapping[str, int | None]
    maximum_freshness_ms: Mapping[str, int | None]
    per_market_freshness_ms: Mapping[str, Mapping[str, int | None]]
    per_market_coverage_counts: Mapping[str, Mapping[str, int]]
    event_counts: Mapping[str, int]
    book_coverage_counts: Mapping[str, int]
    yes_ohlc_coverage_counts: Mapping[str, int]
    status: VerdictStatus
    reasons: tuple[VerdictReason, ...]
    report_hash: str = ""

    def __post_init__(self) -> None:
        for name in ("manifest_hash", "soak_hash", "interval_hash", "raw_row_set_hash"):
            _require_sha256(name, getattr(self, name))
        _utc_text(self.start)
        _utc_text(self.end)
        object.__setattr__(self, "row_counts", _freeze_mapping(dict(self.row_counts)))
        object.__setattr__(
            self, "minimum_cardinality", _freeze_mapping(dict(self.minimum_cardinality))
        )
        object.__setattr__(
            self, "maximum_freshness_ms", _freeze_mapping(dict(self.maximum_freshness_ms))
        )
        object.__setattr__(
            self,
            "per_market_freshness_ms",
            _freeze_mapping(dict(self.per_market_freshness_ms)),
        )
        object.__setattr__(
            self,
            "per_market_coverage_counts",
            _freeze_mapping(dict(self.per_market_coverage_counts)),
        )
        object.__setattr__(self, "event_counts", _freeze_mapping(dict(self.event_counts)))
        object.__setattr__(
            self, "book_coverage_counts", _freeze_mapping(dict(self.book_coverage_counts))
        )
        object.__setattr__(
            self,
            "yes_ohlc_coverage_counts",
            _freeze_mapping(dict(self.yes_ohlc_coverage_counts)),
        )
        object.__setattr__(self, "reasons", tuple(self.reasons))
        expected = _sha256(self.hash_payload())
        if self.report_hash and self.report_hash != expected:
            raise ValueError("report_hash does not match canonical report payload")
        object.__setattr__(self, "report_hash", expected)

    def hash_payload(self) -> dict[str, object]:
        return {
            "manifest_hash": self.manifest_hash,
            "soak_hash": self.soak_hash,
            "interval_hash": self.interval_hash,
            "raw_row_set_hash": self.raw_row_set_hash,
            "start": self.start,
            "end": self.end,
            "require_24h": self.require_24h,
            "boot_id": self.boot_id,
            "machine_id": self.machine_id,
            "machine_version": self.machine_version,
            "image_ref": self.image_ref,
            "image_digest": self.image_digest,
            "release_id": self.release_id,
            "code_version": self.code_version,
            "mapping_hash": self.mapping_hash,
            "acceptance_config_hash": self.acceptance_config_hash,
            "row_counts": self.row_counts,
            "expected_promoter_ticks": self.expected_promoter_ticks,
            "recorded_promoter_ticks": self.recorded_promoter_ticks,
            "max_sample_gap_seconds": self.max_sample_gap_seconds,
            "max_promoter_start_gap_seconds": self.max_promoter_start_gap_seconds,
            "minimum_cardinality": self.minimum_cardinality,
            "maximum_freshness_ms": self.maximum_freshness_ms,
            "per_market_freshness_ms": self.per_market_freshness_ms,
            "per_market_coverage_counts": self.per_market_coverage_counts,
            "event_counts": self.event_counts,
            "book_coverage_counts": self.book_coverage_counts,
            "yes_ohlc_coverage_counts": self.yes_ohlc_coverage_counts,
            "status": self.status,
            "reasons": [
                {"code": reason.code, "message": reason.message} for reason in self.reasons
            ],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "report_hash": self.report_hash}


_TABLE_PRIMARY_KEYS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "l3_runtime_boots": ("boot_id",),
        "l3_promote_runs": ("id",),
        "l3_health_samples": ("boot_id", "sample_seq"),
        "l3_market_samples": ("boot_id", "sample_seq", "market_id"),
        "l3_runtime_events": ("event_id",),
    }
)

_TABLE_COLUMNS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "l3_runtime_boots": frozenset(
            {
                "boot_id",
                "started_at",
                "stopped_at",
                "machine_id",
                "machine_version",
                "image_ref",
                "release_id",
                "code_version",
                "acceptance_config_hash",
                "recorded_at",
            }
        ),
        "l3_promote_runs": frozenset(
            {
                "id",
                "boot_id",
                "run_seq",
                "scheduled_at",
                "started_at",
                "finished_at",
                "status",
                "reason_code",
                "selected_count",
                "desired_count",
                "committed_count",
                "evidenced_count",
                "add_count",
                "remove_count",
                "mapping_hash",
                "desired_hash",
                "committed_hash",
                "acceptance_config_hash",
                "ws_generation",
                "add_succeeded",
                "remove_succeeded",
                "mirror_succeeded",
                "duration_ms",
                "recorded_at",
            }
        ),
        "l3_health_samples": frozenset(
            {
                "boot_id",
                "sample_seq",
                "sampled_at",
                "desired_count",
                "committed_count",
                "evidenced_count",
                "promote_age_ms",
                "global_book_age_ms",
                "ws_age_ms",
                "mirror_age_ms",
                "candidate_age_ms",
                "reconciliation_age_ms",
                "listener_state",
                "cursor_lag",
                "watchdog_count",
                "reconnect_count",
                "ws_generation",
                "mapping_hash",
                "acceptance_config_hash",
                "status",
                "reason_code",
                "recorded_at",
            }
        ),
        "l3_market_samples": frozenset(
            {
                "boot_id",
                "sample_seq",
                "sampled_at",
                "market_id",
                "yes_token_id",
                "no_token_id",
                "yes_desired",
                "no_desired",
                "yes_committed",
                "no_committed",
                "yes_evidenced",
                "no_evidenced",
                "evidence_generation",
                "yes_book_at",
                "no_book_at",
                "yes_book_age_ms",
                "no_book_age_ms",
                "worst_book_age_ms",
                "yes_ohlc_at",
                "yes_ohlc_age_ms",
                "status",
                "reason_code",
                "recorded_at",
            }
        ),
        "l3_runtime_events": frozenset(
            {
                "event_id",
                "boot_id",
                "event_seq",
                "occurred_at",
                "kind",
                "severity",
                "generation",
                "reason_code",
                "detail",
                "recorded_at",
            }
        ),
    }
)

_TABLE_UNIQUE_KEYS: Mapping[str, tuple[tuple[str, ...], ...]] = MappingProxyType(
    {
        "l3_runtime_boots": (("boot_id",),),
        "l3_promote_runs": (("id",), ("boot_id", "run_seq")),
        "l3_health_samples": (("boot_id", "sample_seq"),),
        "l3_market_samples": (
            ("boot_id", "sample_seq", "market_id"),
            ("boot_id", "sample_seq", "yes_token_id"),
            ("boot_id", "sample_seq", "no_token_id"),
        ),
        "l3_runtime_events": (("event_id",), ("boot_id", "event_seq")),
    }
)


def _has_duplicate_key(rows: Sequence[Mapping[str, object]], key_fields: tuple[str, ...]) -> bool:
    keys = [_canonical_json_bytes({field: row.get(field) for field in key_fields}) for row in rows]
    return len(keys) != len(set(keys))


def _raw_row_set(
    evidence: EvidenceWindow,
) -> tuple[dict[str, object], list[VerdictReason]]:
    reasons: list[VerdictReason] = []
    raw_tables = set(evidence.raw_rows_by_table)
    if raw_tables != EVIDENCE_TABLES:
        reasons.append(
            VerdictReason(
                "raw_tables_missing",
                "raw evidence must contain exactly all five evidence tables",
            )
        )

    sorted_tables: dict[str, object] = {}
    for table in sorted(EVIDENCE_TABLES):
        rows = tuple(evidence.raw_rows_by_table.get(table, ()))
        primary_key = _TABLE_PRIMARY_KEYS[table]
        if any(set(row) != _TABLE_COLUMNS[table] for row in rows):
            reasons.append(
                VerdictReason(
                    "raw_schema_mismatch",
                    f"{table} raw rows must exactly match migration 007 columns",
                )
            )
        if any(any(key not in row for key in primary_key) for row in rows):
            reasons.append(
                VerdictReason("raw_primary_key_missing", f"{table} row lacks its primary key")
            )
        if any("recorded_at" not in row for row in rows):
            reasons.append(
                VerdictReason("raw_recorded_at_missing", f"{table} row lacks server recorded_at")
            )
        if any(_has_duplicate_key(rows, key) for key in _TABLE_UNIQUE_KEYS[table]):
            reasons.append(
                VerdictReason(
                    "raw_duplicate_key",
                    f"{table} contains a duplicate physical or logical key",
                )
            )

        def row_key(row: Mapping[str, object]) -> tuple[bytes, bytes]:
            key_payload = {key: row.get(key) for key in primary_key}
            return _canonical_json_bytes(key_payload), _canonical_json_bytes(row)

        sorted_tables[table] = [dict(row) for row in sorted(rows, key=row_key)]

    typed_counts = {
        "l3_runtime_boots": len(evidence.boots),
        "l3_promote_runs": len(evidence.promote_runs),
        "l3_health_samples": len(evidence.health_samples),
        "l3_market_samples": len(evidence.market_samples),
        "l3_runtime_events": len(evidence.runtime_events),
    }
    raw_counts = {
        table: len(evidence.raw_rows_by_table.get(table, ())) for table in EVIDENCE_TABLES
    }
    if raw_counts != typed_counts:
        reasons.append(
            VerdictReason(
                "raw_row_count_mismatch", "raw table row counts differ from decoded evidence"
            )
        )

    decoded_by_table = {
        "l3_runtime_boots": evidence.boots,
        "l3_promote_runs": evidence.promote_runs,
        "l3_health_samples": evidence.health_samples,
        "l3_market_samples": evidence.market_samples,
        "l3_runtime_events": evidence.runtime_events,
    }
    for table, decoded_rows in decoded_by_table.items():
        raw_rows = evidence.raw_rows_by_table.get(table, ())
        if len(raw_rows) != len(decoded_rows):
            continue
        decoded_payloads = sorted(
            _canonical_json_bytes({field.name: getattr(row, field.name) for field in fields(row)})
            for row in decoded_rows
        )
        raw_payloads: list[bytes] = []
        for raw_row in raw_rows:
            names = tuple(field.name for field in fields(decoded_rows[0])) if decoded_rows else ()
            if any(name not in raw_row for name in names):
                raw_payloads = []
                break
            raw_payloads.append(_canonical_json_bytes({name: raw_row[name] for name in names}))
        if sorted(raw_payloads) != decoded_payloads:
            reasons.append(
                VerdictReason(
                    "raw_decoded_mismatch",
                    f"{table} raw rows differ from decoded evidence",
                )
            )
    sorted_tables["book_coverage_counts"] = dict(evidence.book_coverage_counts)
    sorted_tables["yes_ohlc_coverage_counts"] = dict(evidence.yes_ohlc_coverage_counts)
    return sorted_tables, reasons


def _expected_promoter_schedule(
    *, boot_started_at: datetime, start: datetime, end: datetime, interval_s: int
) -> tuple[tuple[int, datetime], ...]:
    elapsed = (start - boot_started_at).total_seconds()
    first_seq = max(0, math.ceil(elapsed / interval_s))
    schedule: list[tuple[int, datetime]] = []
    sequence = first_seq
    while True:
        scheduled_at = boot_started_at + timedelta(seconds=sequence * interval_s)
        if scheduled_at >= end:
            break
        if scheduled_at >= start:
            schedule.append((sequence, scheduled_at))
        sequence += 1
    return tuple(schedule)


def _add(reasons: list[VerdictReason], code: str, message: str) -> None:
    reason = VerdictReason(code, message)
    if reason not in reasons:
        reasons.append(reason)


def _sampler_age_ms(sampled_at: datetime, observed_at: datetime | None) -> int | None:
    if observed_at is None:
        return None
    return max(0, int((sampled_at - observed_at).total_seconds() * 1_000))


def build_soak_report(
    evidence: EvidenceWindow,
    manifest: SoakManifest,
    start: datetime,
    end: datetime,
    require_24h: bool,
) -> L3SoakReport:
    """Evaluate one exact snapshot without reading or mutating external state."""
    if type(evidence) is not EvidenceWindow:
        raise TypeError("evidence must be an exact EvidenceWindow")
    if type(manifest) is not SoakManifest:
        raise TypeError("manifest must be an exact SoakManifest")
    _utc_text(start)
    _utc_text(end)
    if start >= end:
        raise ValueError("report start must precede end")
    if type(require_24h) is not bool:
        raise TypeError("require_24h must be a bool")

    reasons: list[VerdictReason] = []
    config = manifest.acceptance_config
    raw_payload, raw_reasons = _raw_row_set(evidence)
    reasons.extend(raw_reasons)
    raw_row_set_hash = _sha256(raw_payload)

    if (evidence.start, evidence.end) != (start, end):
        _add(reasons, "window_bounds_mismatch", "evidence bounds do not match requested bounds")
    declared_bounds = {(report.start, report.end) for report in manifest.reports}
    if (start, end) not in declared_bounds:
        _add(reasons, "manifest_bounds_mismatch", "requested bounds are not manifest-declared")
    if start != manifest.t0 or end > manifest.t24:
        _add(reasons, "manifest_bounds_mismatch", "report must be cumulative from T0 through T24")
    if require_24h and end - start != timedelta(hours=24):
        _add(reasons, "final_window_duration", "final verdict requires exactly 24 hours")

    if manifest.acceptance_config_hash != config.digest():
        _add(
            reasons,
            "acceptance_config_digest_mismatch",
            "manifest AcceptanceConfig does not match its bound digest",
        )
    locked_config = {
        "sample_interval_s": 30,
        "max_sample_gap_s": 75,
        "promote_interval_s": 300,
        "promote_max_start_gap_s": 360,
        "market_book_fresh_s": 120,
        "market_ohlc_fresh_s": 120,
        "expected_market_count": 5,
        "expected_token_count": 10,
        "schema_revision": "007",
    }
    if any(getattr(config, name) != value for name, value in locked_config.items()):
        _add(reasons, "acceptance_config_not_locked", "AcceptanceConfig differs from R08")
    if config.retention_days < 30 or config.code_version != manifest.code_version:
        _add(reasons, "acceptance_config_not_locked", "retention/code identity differs from R08")

    boot = evidence.boots[0] if len(evidence.boots) == 1 else None
    if len(evidence.boots) != 1:
        _add(reasons, "boot_cardinality", "exactly one runtime boot is required")
    if boot is not None:
        if boot.started_at > start:
            _add(
                reasons,
                "boot_after_window_start",
                "accepted boot must start no later than formal T0",
            )
        if boot.boot_id != manifest.boot_id:
            _add(reasons, "identity_mismatch", "runtime boot ID differs from manifest")
        boot_identity = (
            boot.machine_id,
            boot.machine_version,
            boot.image_ref,
            boot.release_id,
            boot.code_version,
        )
        manifest_identity = (
            manifest.machine_id,
            manifest.machine_version,
            manifest.image_ref,
            manifest.release_id,
            manifest.code_version,
        )
        if boot_identity != manifest_identity:
            _add(reasons, "identity_mismatch", "runtime identity differs from manifest")
        if boot.acceptance_config_hash != manifest.acceptance_config_hash:
            _add(
                reasons,
                "acceptance_config_hash_mismatch",
                "runtime boot AcceptanceConfig differs from manifest",
            )

    decoded_rows = (
        *evidence.promote_runs,
        *evidence.health_samples,
        *evidence.market_samples,
        *evidence.runtime_events,
    )
    if any(row.boot_id != manifest.boot_id for row in decoded_rows):
        _add(
            reasons,
            "row_boot_mismatch",
            "every decoded evidence row must share the manifest boot",
        )
    decoded_unique_specs = (
        (evidence.boots, (("boot_id",),)),
        (evidence.promote_runs, (("boot_id", "run_seq"),)),
        (evidence.health_samples, (("boot_id", "sample_seq"),)),
        (
            evidence.market_samples,
            (
                ("boot_id", "sample_seq", "market_id"),
                ("boot_id", "sample_seq", "yes_token_id"),
                ("boot_id", "sample_seq", "no_token_id"),
            ),
        ),
        (evidence.runtime_events, (("event_id",), ("boot_id", "event_seq"))),
    )
    if any(
        _has_duplicate_key(
            tuple({name: getattr(row, name) for name in key_fields} for row in decoded_table),
            key_fields,
        )
        for decoded_table, unique_keys in decoded_unique_specs
        for key_fields in unique_keys
    ):
        _add(
            reasons,
            "decoded_duplicate_key",
            "decoded evidence contains a duplicate physical or logical key",
        )
    if (
        any(not start <= row.scheduled_at < end for row in evidence.promote_runs)
        or any(not start <= row.sampled_at < end for row in evidence.health_samples)
        or any(not start <= row.sampled_at < end for row in evidence.market_samples)
        or any(not start <= row.occurred_at < end for row in evidence.runtime_events)
    ):
        _add(
            reasons,
            "occurrence_outside_window",
            "decoded occurrence timestamps must be inside exact [start,end)",
        )
    if boot is not None and (
        any(
            min(row.scheduled_at, row.started_at, row.finished_at) < boot.started_at
            for row in evidence.promote_runs
        )
        or any(row.sampled_at < boot.started_at for row in evidence.health_samples)
        or any(row.sampled_at < boot.started_at for row in evidence.market_samples)
        or any(row.occurred_at < boot.started_at for row in evidence.runtime_events)
    ):
        _add(
            reasons,
            "occurrence_before_boot",
            "decoded occurrence timestamps cannot precede the accepted boot",
        )

    promotes = tuple(sorted(evidence.promote_runs, key=lambda row: (row.scheduled_at, row.run_seq)))
    expected_schedule = (
        _expected_promoter_schedule(
            boot_started_at=boot.started_at,
            start=start,
            end=end,
            interval_s=config.promote_interval_s,
        )
        if boot is not None
        else ()
    )
    actual_schedule = tuple((row.run_seq, row.scheduled_at) for row in promotes)
    if actual_schedule != expected_schedule:
        _add(
            reasons,
            "promoter_tick_missing",
            "promoter rows are missing, extra, duplicated, or noncontiguous",
        )
    promoter_start_gaps = [(row.started_at - row.scheduled_at).total_seconds() for row in promotes]
    max_promoter_start_gap = max(promoter_start_gaps, default=None)
    if any(gap < 0 or gap > config.promote_max_start_gap_s for gap in promoter_start_gaps):
        _add(reasons, "promoter_start_gap", "promoter scheduled-to-start gap exceeds 360s")
    if any(row.status is not PromoteStatus.SUCCESS for row in promotes):
        _add(reasons, "promoter_non_success", "every promoter row must be successful")
    if any(
        row.selected_count != config.expected_market_count
        or row.desired_count != config.expected_token_count
        or row.committed_count != config.expected_token_count
        or row.evidenced_count != config.expected_token_count
        or row.add_succeeded is False
        or row.remove_succeeded is False
        or not row.mirror_succeeded
        for row in promotes
    ):
        _add(reasons, "promoter_cardinality", "promoter rows must preserve 5/10/10 truth")
    if any(row.desired_hash != row.committed_hash for row in promotes):
        _add(
            reasons,
            "promoter_membership_hash",
            "successful promoter desired and committed hashes must match",
        )
    if any(
        (row.add_count > 0 and row.add_succeeded is not True)
        or (row.remove_count > 0 and row.remove_succeeded is not True)
        for row in promotes
    ):
        _add(
            reasons,
            "promoter_operation_result",
            "positive add/remove counts require a true operation result",
        )
    if any(row.mapping_hash != manifest.mapping_hash for row in promotes):
        _add(reasons, "mapping_hash_mismatch", "promoter mapping differs from manifest")
    if any(row.acceptance_config_hash != manifest.acceptance_config_hash for row in promotes):
        _add(
            reasons,
            "acceptance_config_hash_mismatch",
            "promoter AcceptanceConfig differs from manifest",
        )

    samples = tuple(
        sorted(evidence.health_samples, key=lambda row: (row.sampled_at, row.sample_seq))
    )
    sample_times = [row.sampled_at for row in samples]
    boundary_times = [start, *sample_times, end]
    sample_gaps = [
        (later - earlier).total_seconds()
        for earlier, later in zip(boundary_times, boundary_times[1:])
    ]
    max_sample_gap = max(sample_gaps, default=None)
    if (
        not samples
        or sample_times[0] != start
        or any(gap < 0 or gap > config.max_sample_gap_s for gap in sample_gaps)
    ):
        _add(reasons, "sample_gap", "health sample boundary/consecutive gap exceeds 75s")
    if samples and any(
        next_row.sample_seq != row.sample_seq + 1 for row, next_row in zip(samples, samples[1:])
    ):
        _add(reasons, "sample_sequence_gap", "health sample sequence is not contiguous")
    if any(row.status is not HealthStatus.PASS for row in samples):
        _add(reasons, "sample_non_pass", "every process sample must pass")
    if any(
        row.desired_count != config.expected_token_count
        or row.committed_count != config.expected_token_count
        or row.evidenced_count != config.expected_token_count
        for row in samples
    ):
        _add(reasons, "sample_cardinality", "every sample must preserve 10/10/10 membership")
    if any(row.mapping_hash != manifest.mapping_hash for row in samples):
        _add(reasons, "mapping_hash_mismatch", "sample mapping differs from manifest")
    if any(row.acceptance_config_hash != manifest.acceptance_config_hash for row in samples):
        _add(
            reasons,
            "acceptance_config_hash_mismatch",
            "sample AcceptanceConfig differs from manifest",
        )

    markets_by_sample: dict[tuple[UUID, int], list[Any]] = defaultdict(list)
    for row in evidence.market_samples:
        markets_by_sample[(row.boot_id, row.sample_seq)].append(row)
    expected_sample_keys = {(row.boot_id, row.sample_seq) for row in samples}
    if set(markets_by_sample) != expected_sample_keys:
        _add(reasons, "market_sample_batch", "market sample batches do not match health samples")

    bound_pairs: set[tuple[str, str, str]] | None = None
    for sample in samples:
        rows = markets_by_sample.get((sample.boot_id, sample.sample_seq), [])
        pairs = {(row.market_id, row.yes_token_id, row.no_token_id) for row in rows}
        tokens = {token for row in rows for token in (row.yes_token_id, row.no_token_id)}
        yes_tokens = {row.yes_token_id for row in rows}
        no_tokens = {row.no_token_id for row in rows}
        if (
            len(rows) != config.expected_market_count
            or len(pairs) != config.expected_market_count
            or len(tokens) != config.expected_token_count
            or len(yes_tokens) != config.expected_market_count
            or len(no_tokens) != config.expected_market_count
        ):
            _add(reasons, "market_sample_cardinality", "each sample needs 5 pairs/10 tokens")
        if bound_pairs is None:
            bound_pairs = pairs
        elif pairs != bound_pairs:
            _add(reasons, "mapping_hash_mismatch", "market identities changed within window")
        if any(row.sampled_at != sample.sampled_at for row in rows):
            _add(
                reasons,
                "market_parent_timestamp",
                "market sampled_at must equal its health parent sampled_at",
            )
        if any(
            row.status is not HealthStatus.PASS
            or not all(
                (
                    row.yes_desired,
                    row.no_desired,
                    row.yes_committed,
                    row.no_committed,
                    row.yes_evidenced,
                    row.no_evidenced,
                )
            )
            or row.evidence_generation != sample.ws_generation
            for row in rows
        ):
            _add(reasons, "market_sample_non_pass", "every market membership row must pass")

    canonical_mapping = [
        {
            "market_id": market_id,
            "yes_token_id": yes_token_id,
            "no_token_id": no_token_id,
        }
        for market_id, yes_token_id, no_token_id in sorted(bound_pairs or set())
    ]
    if (
        len(canonical_mapping) != config.expected_market_count
        or _sha256(canonical_mapping) != manifest.mapping_hash
    ):
        _add(
            reasons,
            "mapping_identity_hash_mismatch",
            "canonical five-pair identity does not match the manifest mapping hash",
        )

    yes_book_ages = [row.yes_book_age_ms for row in evidence.market_samples]
    no_book_ages = [row.no_book_age_ms for row in evidence.market_samples]
    book_ages = [row.worst_book_age_ms for row in evidence.market_samples]
    ohlc_ages = [row.yes_ohlc_age_ms for row in evidence.market_samples]
    age_contract_invalid = any(
        row.yes_book_at is None
        or row.no_book_at is None
        or row.yes_ohlc_at is None
        or row.yes_book_age_ms is None
        or row.no_book_age_ms is None
        or row.worst_book_age_ms is None
        or row.yes_ohlc_age_ms is None
        or row.yes_book_at > row.sampled_at
        or row.no_book_at > row.sampled_at
        or row.yes_ohlc_at > row.sampled_at
        or row.yes_book_age_ms != _sampler_age_ms(row.sampled_at, row.yes_book_at)
        or row.no_book_age_ms != _sampler_age_ms(row.sampled_at, row.no_book_at)
        or row.yes_ohlc_age_ms != _sampler_age_ms(row.sampled_at, row.yes_ohlc_at)
        or row.worst_book_age_ms != max(row.yes_book_age_ms, row.no_book_age_ms)
        or row.yes_book_age_ms >= config.market_book_fresh_s * 1_000
        or row.no_book_age_ms >= config.market_book_fresh_s * 1_000
        or row.yes_ohlc_age_ms >= config.market_ohlc_fresh_s * 1_000
        for row in evidence.market_samples
    )
    if age_contract_invalid:
        _add(
            reasons,
            "market_age_contract",
            "per-source ages must match timestamps, worst age, and strict thresholds",
        )
        _add(reasons, "market_freshness", "book and OHLC ages must remain strictly below 120s")

    pairs_for_coverage = bound_pairs or set()
    expected_book_tokens = {
        token for _, yes_token, no_token in pairs_for_coverage for token in (yes_token, no_token)
    }
    expected_yes_tokens = {yes_token for _, yes_token, _ in pairs_for_coverage}
    if (
        set(evidence.book_coverage_counts) != expected_book_tokens
        or len(expected_book_tokens) != config.expected_token_count
        or any(count <= 0 for count in evidence.book_coverage_counts.values())
    ):
        _add(reasons, "book_coverage", "exact-window book coverage requires all 10 tokens")
    if (
        set(evidence.yes_ohlc_coverage_counts) != expected_yes_tokens
        or len(expected_yes_tokens) != config.expected_market_count
        or any(count <= 0 for count in evidence.yes_ohlc_coverage_counts.values())
    ):
        _add(reasons, "ohlc_coverage", "exact-window OHLC coverage requires all 5 Yes tokens")

    event_counts = Counter(event.kind.value for event in evidence.runtime_events)
    if any(event.boot_id != manifest.boot_id for event in evidence.runtime_events):
        _add(reasons, "identity_mismatch", "runtime event boot differs from manifest")
    rejected = sorted(
        {
            event.kind.value
            for event in evidence.runtime_events
            if event.kind in DISALLOWED_EVENT_KINDS
            and event.kind not in manifest.allowed_event_kind_exceptions
        }
    )
    if rejected:
        _add(
            reasons,
            "disallowed_runtime_event",
            "disallowed event kinds observed: " + ",".join(rejected),
        )

    row_counts = {
        "l3_runtime_boots": len(evidence.boots),
        "l3_promote_runs": len(evidence.promote_runs),
        "l3_health_samples": len(evidence.health_samples),
        "l3_market_samples": len(evidence.market_samples),
        "l3_runtime_events": len(evidence.runtime_events),
    }
    minimum_cardinality = {
        "selected_markets": min((row.selected_count for row in promotes), default=None),
        "desired_tokens": min((row.desired_count for row in samples), default=None),
        "committed_tokens": min((row.committed_count for row in samples), default=None),
        "evidenced_tokens": min((row.evidenced_count for row in samples), default=None),
    }
    maximum_freshness_ms = {
        "yes_book": max((age for age in yes_book_ages if age is not None), default=None),
        "no_book": max((age for age in no_book_ages if age is not None), default=None),
        "worst_book": max((age for age in book_ages if age is not None), default=None),
        "yes_ohlc": max((age for age in ohlc_ages if age is not None), default=None),
    }
    per_market_freshness_ms: dict[str, dict[str, int | None]] = {}
    per_market_coverage_counts: dict[str, dict[str, int]] = {}
    for market_id, yes_token_id, no_token_id in sorted(bound_pairs or set()):
        market_rows = [row for row in evidence.market_samples if row.market_id == market_id]

        def maximum(field_name: str) -> int | None:
            values = [getattr(row, field_name) for row in market_rows]
            return max((value for value in values if value is not None), default=None)

        per_market_freshness_ms[market_id] = {
            "yes_book": maximum("yes_book_age_ms"),
            "no_book": maximum("no_book_age_ms"),
            "worst_book": maximum("worst_book_age_ms"),
            "yes_ohlc": maximum("yes_ohlc_age_ms"),
        }
        per_market_coverage_counts[market_id] = {
            "yes_book": evidence.book_coverage_counts.get(yes_token_id, 0),
            "no_book": evidence.book_coverage_counts.get(no_token_id, 0),
            "yes_ohlc": evidence.yes_ohlc_coverage_counts.get(yes_token_id, 0),
        }
    ordered_reasons = tuple(sorted(set(reasons)))
    status = VerdictStatus.PASS if not ordered_reasons else VerdictStatus.NOT_CLOSED
    return L3SoakReport(
        manifest_hash=manifest.manifest_hash,
        soak_hash=manifest.soak_hash,
        interval_hash=_sha256({"soak_hash": manifest.soak_hash, "end": end}),
        raw_row_set_hash=raw_row_set_hash,
        start=start,
        end=end,
        require_24h=require_24h,
        boot_id=manifest.boot_id,
        machine_id=manifest.machine_id,
        machine_version=manifest.machine_version,
        image_ref=manifest.image_ref,
        image_digest=manifest.image_digest,
        release_id=manifest.release_id,
        code_version=manifest.code_version,
        mapping_hash=manifest.mapping_hash,
        acceptance_config_hash=manifest.acceptance_config_hash,
        row_counts=row_counts,
        expected_promoter_ticks=len(expected_schedule),
        recorded_promoter_ticks=len(promotes),
        max_sample_gap_seconds=max_sample_gap,
        max_promoter_start_gap_seconds=max_promoter_start_gap,
        minimum_cardinality=minimum_cardinality,
        maximum_freshness_ms=maximum_freshness_ms,
        per_market_freshness_ms=per_market_freshness_ms,
        per_market_coverage_counts=per_market_coverage_counts,
        event_counts=dict(sorted(event_counts.items())),
        book_coverage_counts=dict(evidence.book_coverage_counts),
        yes_ohlc_coverage_counts=dict(evidence.yes_ohlc_coverage_counts),
        status=status,
        reasons=ordered_reasons,
    )


def render_report(report: L3SoakReport) -> str:
    """Render deterministic operator Markdown from one canonical report."""
    if type(report) is not L3SoakReport:
        raise TypeError("report must be an exact L3SoakReport")

    def compact(value: Mapping[str, object]) -> str:
        return _canonical_json_bytes(value).decode("utf-8")

    lines = [
        "# L3 Soak Evidence Report",
        "",
        f"- Verdict: **{report.status.value}**",
        f"- Window: `{_utc_text(report.start)}` to `{_utc_text(report.end)}`",
        f"- Boot: `{report.boot_id}`",
        f"- Require 24h: `{str(report.require_24h).lower()}`",
        (
            f"- Release: `{report.release_id}` / machine "
            f"`{report.machine_id}` `{report.machine_version}`"
        ),
        f"- Code version: `{report.code_version}`",
        f"- Image ref: `{report.image_ref}`",
        f"- Image digest: `{report.image_digest}`",
        f"- Acceptance config hash: `{report.acceptance_config_hash}`",
        f"- Mapping hash: `{report.mapping_hash}`",
        f"- Manifest hash: `{report.manifest_hash}`",
        f"- Soak hash: `{report.soak_hash}`",
        f"- Interval hash: `{report.interval_hash}`",
        f"- Raw row set hash: `{report.raw_row_set_hash}`",
        f"- Report hash: `{report.report_hash}`",
        "",
        "## Evidence",
        "",
        f"- Promoter ticks: {report.recorded_promoter_ticks}/{report.expected_promoter_ticks}",
        f"- Maximum sample gap: {report.max_sample_gap_seconds}",
        f"- Maximum promoter start gap: {report.max_promoter_start_gap_seconds}",
        f"- Minimum cardinality: `{compact(report.minimum_cardinality)}`",
        f"- Maximum freshness: `{compact(report.maximum_freshness_ms)}`",
        f"- Event counts: `{compact(report.event_counts)}`",
        f"- Book coverage: `{compact(report.book_coverage_counts)}`",
        f"- Yes OHLC coverage: `{compact(report.yes_ohlc_coverage_counts)}`",
    ]
    lines.extend(f"- {table}: {count}" for table, count in sorted(report.row_counts.items()))
    lines.extend(["", "## Per-market freshness and coverage", ""])
    for market_id in sorted(report.per_market_freshness_ms):
        lines.append(
            f"- Per-market freshness `{market_id}`: "
            f"`{compact(report.per_market_freshness_ms[market_id])}`"
        )
        lines.append(
            f"- Per-market coverage `{market_id}`: "
            f"`{compact(report.per_market_coverage_counts[market_id])}`"
        )
    lines.extend(["", "## Reasons", ""])
    if report.reasons:
        lines.extend(f"- `{reason.code}`: {reason.message}" for reason in report.reasons)
    else:
        lines.append("- None")
    return "\n".join(lines) + "\n"


__all__ = [
    "DISALLOWED_EVENT_KINDS",
    "L3SoakReport",
    "ManifestReport",
    "SoakManifest",
    "VerdictReason",
    "VerdictStatus",
    "build_soak_report",
    "render_report",
]
