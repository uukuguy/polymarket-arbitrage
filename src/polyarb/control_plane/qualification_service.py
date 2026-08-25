"""Rolling qualification service over durable control-plane facts."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import total_ordering
from typing import Any, Protocol, Self, cast

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .qualification import (
    BREAKING_REASONS,
    QualificationDecision,
    QualificationFact,
    QualificationState,
    RollingQualificationPolicy,
)
from .qualification_store import (
    QualificationCertificateRecord,
    QualificationEpochRecord,
    canonical_certificate_bytes,
    certificate_digest,
    insert_qualification_certificate,
    list_qualification_certificates,
    qualification_certificate_payload,
)

ConnectionFactory = Callable[[], psycopg.Connection[Any]]

_STATEMENT_TIMEOUT_MS = 5_000
_LOCK_TIMEOUT_MS = 1_000
_SOURCE_RANK_RUNTIME = 10
_SOURCE_RANK_INCIDENT = 20
_SOURCE_RANK_RECOVERY = 30
_SOURCE_RANK_FRESHNESS = 40
_RUNTIME_KINDS = frozenset(
    {
        "job.started",
        "job.stage-changed",
        "job.lease-at-risk",
        "job.progress-stalled",
        "job.retryable-failed",
        "job.retry-scheduled",
        "job.recovery-started",
        "job.recovered",
        "job.terminal-failed",
        "job.succeeded",
    }
)
_RUNTIME_BREAKING_REASONS = {
    "lease.expired": "lease.expired",
    "job.lease-expired": "lease.expired",
    "job.lease-expired-risk": "lease.expired",
    "integrity.conflict": "integrity.conflict",
    "progress.regressed": "progress.regressed",
}
_RECOVERY_ACTION_REASONS = {
    "heartbeat-job": "recovery.heartbeat",
    "cancel-job": "recovery.started",
    "retry-job": "recovery.retry",
    "reclaim-job": "recovery.reclaim",
    "probe-circuit": "recovery.circuit-probe",
    "restart-worker-process": "recovery.process-replacement",
    "restart-machine": "recovery.machine-replacement",
}
_INCIDENT_KINDS = frozenset(
    {
        "attempt-failed",
        "circuit-opened",
        "circuit-probe-failed",
        "detected",
        "escalated",
        "recovery-started",
        "recovered",
        "resolved",
    }
)
_INCIDENT_RECOVERY_KINDS = frozenset({"recovered", "resolved"})
_INCIDENT_BREAKING_KINDS = frozenset(
    {"circuit-opened", "circuit-probe-failed", "escalated"}
)
_INCIDENT_RECOVERY_STARTED_KINDS = frozenset({"attempt-failed", "recovery-started"})
_INCIDENT_SEVERITIES = frozenset({"info", "warning", "critical"})
_INCIDENT_STATES = frozenset({"open", "acknowledged", "resolved"})
_FRESHNESS_PRODUCTS = frozenset({"structure", "quote", "opportunity"})


class QualificationServiceError(RuntimeError):
    """Base error for rolling qualification service failures."""


class QualificationCursorConflict(QualificationServiceError):
    """The durable source cursor lost its compare-and-swap fence."""


@total_ordering
@dataclass(frozen=True, slots=True)
class FactCursor:
    """Total durable ordering key for source facts."""

    observed_at: datetime
    source_rank: int
    stable_id: str
    ingest_seq: int | None = None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("cursor observed_at must be timezone-aware")
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(UTC))
        if type(self.source_rank) is not int or self.source_rank < 0:
            raise ValueError("cursor source_rank must be a non-negative integer")
        if type(self.stable_id) is not str or not self.stable_id:
            raise ValueError("cursor stable_id must be non-empty")
        if self.ingest_seq is not None and (
            type(self.ingest_seq) is not int or self.ingest_seq <= 0
        ):
            raise ValueError("cursor ingest_seq must be a positive integer when provided")

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, FactCursor):
            return NotImplemented
        return self._order_key() < other._order_key()

    def _order_key(self) -> tuple[object, ...]:
        if self.ingest_seq is not None:
            return (0, self.ingest_seq)
        return (1, self.observed_at, self.source_rank, self.stable_id)

    def to_json(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "observed_at": self.observed_at.isoformat(),
            "source_rank": self.source_rank,
            "stable_id": self.stable_id,
        }
        if self.ingest_seq is not None:
            payload["ingest_seq"] = self.ingest_seq
        return payload

    @classmethod
    def from_json(cls, value: Mapping[str, object] | None) -> Self | None:
        if value is None:
            return None
        try:
            observed_at = _parse_datetime(value["observed_at"], "source_cursor.observed_at")
            source_rank = value["source_rank"]
            stable_id = value["stable_id"]
            ingest_seq = value.get("ingest_seq")
        except KeyError as exc:
            raise ValueError("source cursor is malformed") from exc
        if type(source_rank) is not int or type(stable_id) is not str:
            raise ValueError("source cursor is malformed")
        if ingest_seq is not None and type(ingest_seq) is not int:
            raise ValueError("source cursor is malformed")
        return cls(observed_at, source_rank, stable_id, ingest_seq=ingest_seq)


@dataclass(frozen=True, slots=True)
class QualificationFactRecord:
    """One source row mapped to a policy fact and its total-order cursor."""

    cursor: FactCursor
    fact: QualificationFact
    source: str

    def __post_init__(self) -> None:
        if type(self.cursor) is not FactCursor:
            raise TypeError("cursor must be FactCursor")
        if type(self.fact) is not QualificationFact:
            raise TypeError("fact must be QualificationFact")
        if type(self.source) is not str or not self.source:
            raise ValueError("source must be non-empty")

    def to_json(self) -> dict[str, object]:
        return {
            "cursor": self.cursor.to_json(),
            "fact": _fact_to_json(self.fact),
            "source": self.source,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, object]) -> Self:
        try:
            cursor_payload = value["cursor"]
            fact_payload = value["fact"]
            source = value["source"]
        except KeyError as exc:
            raise ValueError("qualification fact record is malformed") from exc
        if not isinstance(cursor_payload, Mapping) or not isinstance(fact_payload, Mapping):
            raise ValueError("qualification fact record is malformed")
        if type(source) is not str:
            raise ValueError("qualification fact record is malformed")
        cursor = FactCursor.from_json(cast(Mapping[str, object], cursor_payload))
        if cursor is None:
            raise ValueError("qualification fact record cursor is missing")
        return cls(
            cursor=cursor,
            fact=_fact_from_json(cast(Mapping[str, object], fact_payload)),
            source=source,
        )


@dataclass(frozen=True, slots=True)
class QualificationTickResult:
    status: str
    applied: int
    cursor: FactCursor | None
    epoch_id: str
    state: QualificationState
    certificate_digest: str | None = None


class QualificationFactSource(Protocol):
    def read_after(
        self,
        cursor: FactCursor | None,
        *,
        limit: int,
        now: datetime,
    ) -> Sequence[QualificationFactRecord]: ...


class QualificationStateStore(Protocol):
    @property
    def cursor(self) -> FactCursor | None: ...

    @property
    def current(self) -> QualificationDecision: ...

    def initialize(self, policy: RollingQualificationPolicy, *, now: datetime) -> None: ...

    def apply_records(
        self,
        policy: RollingQualificationPolicy,
        records: Sequence[QualificationFactRecord],
        *,
        expected_cursor: FactCursor | None,
        writer_id: str,
    ) -> QualificationDecision: ...

    def ensure_certificate(
        self, decision: QualificationDecision
    ) -> Mapping[str, object] | None: ...


class StaticQualificationFactSource:
    """Deterministic test source that applies the same ordering as Postgres."""

    def __init__(self, records: Sequence[QualificationFactRecord]) -> None:
        self._records = tuple(sorted(records, key=lambda record: record.cursor))

    def read_after(
        self,
        cursor: FactCursor | None,
        *,
        limit: int,
        now: datetime,
    ) -> Sequence[QualificationFactRecord]:
        _require_aware(now, "now")
        if limit <= 0:
            raise ValueError("limit must be positive")
        return tuple(
            record
            for record in self._records
            if (cursor is None or record.cursor > cursor) and record.cursor.observed_at <= now
        )[:limit]


class InMemoryQualificationStore:
    """Small transactional test double for crash/replay service semantics."""

    def __init__(
        self,
        *,
        fail_before_commit_once: bool = False,
        fail_after_commit_once: bool = False,
    ) -> None:
        self.fail_before_commit_once = fail_before_commit_once
        self.fail_after_commit_once = fail_after_commit_once
        self._current: QualificationDecision | None = None
        self._cursor: FactCursor | None = None
        self._epochs: list[QualificationDecision] = []
        self._applied_cursors: list[FactCursor] = []
        self._last_applied_count = 0
        self._recovery_observations: list[QualificationFactRecord] = []
        self.certificates: list[dict[str, object]] = []

    @property
    def cursor(self) -> FactCursor | None:
        return self._cursor

    @property
    def current(self) -> QualificationDecision:
        if self._current is None:
            raise QualificationServiceError("qualification store is not initialized")
        return self._current

    @property
    def epochs(self) -> tuple[QualificationDecision, ...]:
        return tuple(self._epochs)

    @property
    def applied_cursors(self) -> tuple[FactCursor, ...]:
        return tuple(self._applied_cursors)

    @property
    def last_applied_count(self) -> int:
        return self._last_applied_count

    @property
    def recovery_observations(self) -> tuple[QualificationFactRecord, ...]:
        return tuple(self._recovery_observations)

    def initialize(self, policy: RollingQualificationPolicy, *, now: datetime) -> None:
        if self._current is None:
            self._current = policy.new_epoch(started_at=now)
            self._epochs.append(self._current)

    def apply_records(
        self,
        policy: RollingQualificationPolicy,
        records: Sequence[QualificationFactRecord],
        *,
        expected_cursor: FactCursor | None,
        writer_id: str,
    ) -> QualificationDecision:
        if writer_id == "":
            raise ValueError("writer_id must be non-empty")
        if expected_cursor != self._cursor:
            raise QualificationCursorConflict("qualification cursor CAS failed")
        if self.fail_before_commit_once:
            self.fail_before_commit_once = False
            raise RuntimeError("injected before commit")
        current = self.current
        epochs = list(self._epochs)
        applied = list(self._applied_cursors)
        observations = list(self._recovery_observations)
        cursor = self._cursor
        applied_count = 0
        for record in sorted(records, key=lambda item: item.cursor):
            if (
                current.state is QualificationState.RECOVERING
                and not _is_recovery_confirmation_fact(record.fact)
            ):
                observations.append(record)
                cursor = record.cursor
                applied.append(record.cursor)
                applied_count += 1
                continue
            opened_recovered_epoch = current.state is QualificationState.RECOVERING
            current = self._apply_one(policy, current, epochs, record.fact)
            epochs[-1] = current
            cursor = record.cursor
            applied.append(record.cursor)
            applied_count += 1
            if opened_recovered_epoch or current.state is QualificationState.QUALIFIED:
                break
        self._current = current
        self._epochs = epochs
        self._cursor = cursor
        self._applied_cursors = applied
        self._last_applied_count = applied_count
        self._recovery_observations = observations
        if self.fail_after_commit_once:
            self.fail_after_commit_once = False
            raise RuntimeError("injected after commit")
        return current

    def ensure_certificate(self, decision: QualificationDecision) -> Mapping[str, object] | None:
        if decision.state is not QualificationState.QUALIFIED:
            return None
        payload = qualification_certificate_payload(decision)
        digest = certificate_digest(payload)
        for certificate in self.certificates:
            if certificate["digest"] == digest:
                return certificate
        certificate = {"digest": digest, "payload": payload}
        self.certificates.append(certificate)
        return certificate

    @staticmethod
    def _apply_one(
        policy: RollingQualificationPolicy,
        current: QualificationDecision,
        epochs: list[QualificationDecision],
        fact: QualificationFact,
    ) -> QualificationDecision:
        if current.state is QualificationState.INVALIDATED:
            current = policy.recovering(
                current, started_at=current.invalidated_at or fact.observed_at
            )
            epochs.append(current)
        next_decision = policy.apply(current, fact)
        if next_decision.state is QualificationState.INVALIDATED:
            recovering = policy.recovering(
                next_decision,
                started_at=next_decision.invalidated_at or fact.observed_at,
            )
            epochs[-1] = next_decision
            epochs.append(recovering)
            return recovering
        if (
            current.state is QualificationState.RECOVERING
            and next_decision.state is QualificationState.ACCUMULATING
        ):
            epochs.append(next_decision)
        return next_decision


class QualificationService:
    """Apply bounded batches of ordered durable facts to one rolling epoch."""

    def __init__(
        self,
        *,
        policy: RollingQualificationPolicy,
        fact_source: QualificationFactSource,
        state_store: QualificationStateStore,
        writer_id: str,
        batch_size: int = 100,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if type(writer_id) is not str or not writer_id:
            raise ValueError("writer_id must be non-empty")
        self._policy = policy
        self._fact_source = fact_source
        self._state_store = state_store
        self._writer_id = writer_id
        self._batch_size = batch_size

    def tick(self, now: datetime) -> QualificationTickResult:
        observed_at = _require_aware(now, "now")
        self._state_store.initialize(self._policy, now=observed_at)
        cursor = self._state_store.cursor
        records = tuple(
            sorted(
                self._fact_source.read_after(cursor, limit=self._batch_size, now=observed_at),
                key=lambda record: record.cursor,
            )
        )
        _assert_cursor_batch(cursor, records)
        decision = self._state_store.apply_records(
            self._policy,
            records,
            expected_cursor=cursor,
            writer_id=self._writer_id,
        )
        certificate = None
        if decision.state is QualificationState.QUALIFIED:
            certificate = self._state_store.ensure_certificate(decision)
        return QualificationTickResult(
            status="ok",
            applied=int(getattr(self._state_store, "last_applied_count", len(records))),
            cursor=self._state_store.cursor,
            epoch_id=decision.epoch_id,
            state=decision.state,
            certificate_digest=(
                None if certificate is None else cast(str, certificate.get("digest"))
            ),
        )


class PostgresQualificationFactSource:
    """Source over the monotonic qualification ingress ledger."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def read_after(
        self,
        cursor: FactCursor | None,
        *,
        limit: int,
        now: datetime,
    ) -> Sequence[QualificationFactRecord]:
        observed_at = _require_aware(now, "now")
        if limit <= 0:
            raise ValueError("limit must be positive")
        lower_seq = 0 if cursor is None else cursor.ingest_seq
        if lower_seq is None:
            raise ValueError("Postgres qualification cursor requires ingest_seq")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as db,
        ):
            _set_timeouts(db)
            self._insert_freshness_observations(db, now=observed_at)
            db.execute(
                """
                SELECT ingest_seq, ingested_at, source, source_id, source_version,
                       original_observed_at, payload, payload_sha256,
                       %s::timestamptz AS qualification_observed_at
                FROM m1_qualification_ingress_ledger
                WHERE ingest_seq > %s
                ORDER BY ingest_seq
                LIMIT %s
                """,
                (observed_at, lower_seq, limit),
            )
            return tuple(ledger_row_to_fact_record(row) for row in db.fetchall())

    def _insert_freshness_observations(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        now: datetime,
    ) -> None:
        for product, query in (
            (
                "structure",
                """
                SELECT 'freshness:structure:' || %s || ':' || pointer.pointer_key AS fact_id,
                       'structure' AS data_product,
                       %s::timestamptz AS observed_at,
                       EXTRACT(EPOCH FROM (%s - manifest.published_at))::bigint
                           AS freshness_seconds,
                       900::bigint AS freshness_slo_seconds,
                       manifest.record_count AS progress_count,
                       manifest.record_count AS successful_count
                FROM m1_publication_pointers AS pointer
                JOIN m1_generation_manifests AS manifest
                  ON manifest.generation_key = pointer.generation_key
                WHERE pointer.pointer_key = 'structure:current'
                ORDER BY manifest.published_at DESC
                LIMIT 1
                """,
            ),
            (
                "quote",
                """
                SELECT 'freshness:quote:' || %s || ':' || pointer.pointer_key AS fact_id,
                       'quote' AS data_product,
                       %s::timestamptz AS observed_at,
                       EXTRACT(EPOCH FROM (%s - manifest.published_at))::bigint
                           AS freshness_seconds,
                       900::bigint AS freshness_slo_seconds,
                       manifest.record_count AS progress_count,
                       manifest.record_count AS successful_count
                FROM m1_publication_pointers AS pointer
                JOIN m1_generation_manifests AS manifest
                  ON manifest.generation_key = pointer.generation_key
                WHERE pointer.pointer_key = 'quote:current'
                ORDER BY manifest.published_at DESC
                LIMIT 1
                """,
            ),
            (
                "opportunity",
                """
                SELECT 'freshness:opportunity:' || %s || ':' || pointer.pointer_key AS fact_id,
                       'opportunity' AS data_product,
                       %s::timestamptz AS observed_at,
                       EXTRACT(EPOCH FROM (%s - projection.certified_at))::bigint
                           AS freshness_seconds,
                       900::bigint AS freshness_slo_seconds,
                       projection.record_count AS progress_count,
                       projection.record_count AS successful_count
                FROM m1_opportunity_publication_pointers AS pointer
                JOIN m1_opportunity_projections AS projection
                  ON projection.generation_key = pointer.generation_key
                WHERE pointer.pointer_key = 'opportunity:current'
                ORDER BY projection.certified_at DESC
                LIMIT 1
                """,
            ),
        ):
            observed_key = _cursor_time_key(now)
            cursor.execute(cast(Any, query), (observed_key, now, now))
            row = cursor.fetchone()
            payload = (
                _freshness_gap_payload(product=product, now=now)
                if row is None
                else _json_payload(row)
            )
            cursor.execute(
                """
                SELECT m1_record_qualification_ingress(
                    'freshness',
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                (
                    cast(str, payload["fact_id"]),
                    str(payload.get("data_product", product)),
                    now,
                    Jsonb(payload),
                ),
            )


class PostgresQualificationServiceStore:
    """Postgres-backed service state store with cursor/state atomicity."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory
        self._current: QualificationDecision | None = None
        self._cursor: FactCursor | None = None
        self._last_applied_count = 0

    @property
    def cursor(self) -> FactCursor | None:
        return self._cursor

    @property
    def current(self) -> QualificationDecision:
        if self._current is None:
            raise QualificationServiceError("qualification store is not initialized")
        return self._current

    @property
    def last_applied_count(self) -> int:
        return self._last_applied_count

    def initialize(self, policy: RollingQualificationPolicy, *, now: datetime) -> None:
        observed_at = _require_aware(now, "now")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_timeouts(cursor)
            record = _fetch_current_epoch(cursor, policy=policy, for_update=False)
            cursor_record = _ensure_source_cursor_row(cursor, policy=policy, writer_id=None)
            if record is None:
                decision = policy.new_epoch(started_at=observed_at)
                _insert_epoch_cursor(
                    cursor, decision, source_cursor=None, fact_records=(), writer_id=None
                )
                record = _fetch_epoch(cursor, decision.epoch_id, for_update=False)
                if record is None:
                    raise QualificationServiceError("qualification epoch insert returned no row")
            self._current = _decision_from_epoch(record)
            self._cursor = FactCursor.from_json(
                cast(Mapping[str, object] | None, cursor_record["source_cursor"])
            )

    def apply_records(
        self,
        policy: RollingQualificationPolicy,
        records: Sequence[QualificationFactRecord],
        *,
        expected_cursor: FactCursor | None,
        writer_id: str,
    ) -> QualificationDecision:
        if type(writer_id) is not str or not writer_id:
            raise ValueError("writer_id must be non-empty")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_timeouts(cursor)
            record = _fetch_current_epoch(cursor, policy=policy, for_update=True)
            if record is None:
                raise QualificationServiceError("qualification epoch is missing")
            cursor_record = _ensure_source_cursor_row(
                cursor,
                policy=policy,
                writer_id=writer_id,
                for_update=True,
            )
            persisted_cursor = FactCursor.from_json(
                cast(Mapping[str, object] | None, cursor_record["source_cursor"])
            )
            if persisted_cursor != expected_cursor:
                raise QualificationCursorConflict("qualification cursor CAS failed")
            current = _decision_from_epoch(record)
            epoch_fact_records = list(_records_from_epoch(record))
            source_cursor = persisted_cursor
            applied_count = 0
            for fact_record in sorted(records, key=lambda item: item.cursor):
                opened_recovered_epoch = False
                if current.state is QualificationState.RECOVERING:
                    if not _is_recovery_confirmation_fact(fact_record.fact):
                        _append_recovery_observation(
                            cursor,
                            recovering_epoch_id=current.epoch_id,
                            fact_record=fact_record,
                        )
                        source_cursor = fact_record.cursor
                        applied_count += 1
                        _update_source_cursor(
                            cursor,
                            policy=policy,
                            source_cursor=source_cursor,
                            writer_id=writer_id,
                        )
                        continue
                    current = policy.apply(current, fact_record.fact)
                    opened_recovered_epoch = True
                    if current.state is not QualificationState.ACCUMULATING:
                        raise QualificationServiceError(
                            "recovery confirmation did not open an accumulating epoch"
                        )
                    epoch_fact_records = [fact_record]
                    _insert_epoch_cursor(
                        cursor,
                        current,
                        source_cursor=None,
                        fact_records=epoch_fact_records,
                        writer_id=writer_id,
                    )
                else:
                    current = policy.apply(current, fact_record.fact)
                    epoch_fact_records.append(fact_record)
                    _update_epoch_cursor(
                        cursor,
                        current,
                        source_cursor=None,
                        fact_records=epoch_fact_records,
                        writer_id=writer_id,
                    )
                    if current.state is QualificationState.INVALIDATED:
                        current = policy.recovering(
                            current,
                            started_at=current.invalidated_at or fact_record.fact.observed_at,
                        )
                        epoch_fact_records = []
                        _insert_epoch_cursor(
                            cursor,
                            current,
                            source_cursor=None,
                            fact_records=(),
                            writer_id=writer_id,
                        )
                source_cursor = fact_record.cursor
                applied_count += 1
                _update_source_cursor(
                    cursor,
                    policy=policy,
                    source_cursor=source_cursor,
                    writer_id=writer_id,
                )
                if current.state is QualificationState.QUALIFIED:
                    break
                if opened_recovered_epoch:
                    break
            self._current = current
            self._cursor = source_cursor
            self._last_applied_count = applied_count
            return current

    def ensure_certificate(self, decision: QualificationDecision) -> Mapping[str, object] | None:
        if decision.state is not QualificationState.QUALIFIED:
            return None
        record = insert_qualification_certificate(self._connection_factory, decision=decision)
        return _certificate_payload(record)

    def status(self, *, now: datetime) -> dict[str, object]:
        observed_at = _require_aware(now, "now")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute("SET TRANSACTION READ ONLY")
            _set_timeouts(cursor)
            record = _fetch_latest_epoch(cursor)
            if record is None:
                return {
                    "epoch": None,
                    "duration_seconds": 0,
                    "evidence_gap_seconds": None,
                    "last_fact": None,
                    "last_breaker": None,
                    "contained_recoveries": [],
                    "certificate": None,
                }
            certificates = list_qualification_certificates(self._connection_factory, limit=1)
            last_fact = _last_fact_projection(_records_from_epoch(record))
            recovery_status = _recovery_observation_status(cursor, record)
            last_breaker = recovery_status["last_breaker"] or _epoch_breaker_projection(record)
            return {
                "epoch": {
                    "epoch_id": record.epoch_id,
                    "state": record.state,
                    "started_at": record.started_at.isoformat(),
                    "last_fact_at": None
                    if record.last_fact_at is None
                    else record.last_fact_at.isoformat(),
                    "invalidated_at": None
                    if record.invalidated_at is None
                    else record.invalidated_at.isoformat(),
                    "qualified_at": None
                    if record.qualified_at is None
                    else record.qualified_at.isoformat(),
                    "previous_epoch_id": record.previous_epoch_id,
                    "version": record.version,
                },
                "duration_seconds": max(0, int((observed_at - record.started_at).total_seconds())),
                "evidence_gap_seconds": record.max_gap_seconds,
                "last_fact": last_fact,
                "last_breaker": last_breaker,
                "last_recovery_observation": recovery_status["last_observation"],
                "last_recovery_breaking_observation": recovery_status["last_breaking_observation"],
                "recovery_observation_count": recovery_status["count"],
                "contained_recoveries": list(record.contained_recoveries),
                "certificate": None if not certificates else _certificate_payload(certificates[0]),
            }

    def certificates(self, *, limit: int) -> list[dict[str, object]]:
        return [
            _certificate_payload(record)
            for record in list_qualification_certificates(self._connection_factory, limit=limit)
        ]


async def run_qualification_service(
    service: QualificationService,
    *,
    interval_seconds: float,
    emit: Callable[[dict[str, object]], None] | None = None,
    stop_event: asyncio.Event | None = None,
) -> dict[str, object]:
    """Run non-overlapping qualification ticks; any tick error escapes."""

    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    stop = stop_event or asyncio.Event()
    ticks = 0
    while not stop.is_set():
        result = service.tick(datetime.now(UTC))
        ticks += 1
        if emit is not None:
            emit(_tick_payload(result))
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue
    return {"status": "stopped", "ticks": ticks}


def runtime_event_row_to_fact_record(row: Mapping[str, object]) -> QualificationFactRecord:
    try:
        event_id = _nonempty(row["event_id"], "event_id")
        kind = _nonempty(row["kind"], "kind")
        observed_at = _qualification_observed_at(row, "occurred_at")
        job_key = _nonempty(row["job_key"], "job_key")
        attempt_id = _nonempty(row["attempt_id"], "attempt_id")
        lease_epoch = _int(row["lease_epoch"], "lease_epoch")
        event_sequence = _int(row["event_sequence"], "event_sequence")
        detail = _detail(row.get("detail"))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("runtime event row is malformed") from exc
    if kind not in _RUNTIME_KINDS:
        raise ValueError(f"unknown runtime event kind: {kind}")
    reason = "healthy"
    reason_code = str(detail.get("reason_code", ""))
    if kind == "job.terminal-failed":
        reason = _RUNTIME_BREAKING_REASONS.get(reason_code, "recovery.human-intervention")
    elif kind == "job.progress-stalled":
        reason = "recovery.started"
    elif kind == "job.retryable-failed":
        reason = "recovery.started"
    elif kind == "job.recovered":
        reason = "recovery.confirmed"
    raw_progress_count = _optional_int(row.get("progress_current"), "progress_current")
    progress_count = raw_progress_count if kind == "job.terminal-failed" else None
    fact_kwargs: dict[str, Any] = {
        "epoch_id": str(detail["epoch_id"]) if isinstance(detail.get("epoch_id"), str) else None,
        "progress_count": progress_count,
    }
    if reason == "recovery.confirmed":
        fact_kwargs["recovery_confirmed"] = True
    return QualificationFactRecord(
        cursor=FactCursor(
            observed_at,
            _SOURCE_RANK_RUNTIME,
            _qualification_stable_id(row, event_id),
            ingest_seq=_qualification_ingest_seq(row),
        ),
        fact=QualificationFact(
            fact_id=f"runtime:{job_key}:{attempt_id}:{lease_epoch}:{event_sequence}:{event_id}",
            observed_at=observed_at,
            reason=reason,
            **fact_kwargs,
        ),
        source="runtime",
    )


def incident_event_row_to_fact_record(row: Mapping[str, object]) -> QualificationFactRecord:
    try:
        event_id = _nonempty(row["incident_event_id"], "incident_event_id")
        incident_key = _nonempty(row["incident_key"], "incident_key")
        kind = _nonempty(row["kind"], "kind")
        severity = _nonempty(row["severity"], "severity")
        state = _nonempty(row["state"], "state")
        observed_at = _qualification_observed_at(row, "occurred_at")
        detail = _detail(row.get("detail"))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("incident event row is malformed") from exc
    if kind not in _INCIDENT_KINDS:
        raise ValueError(f"unknown incident event kind: {kind}")
    if severity not in _INCIDENT_SEVERITIES:
        raise ValueError(f"unknown incident severity: {severity}")
    if state not in _INCIDENT_STATES:
        raise ValueError(f"unknown incident state: {state}")
    if kind in _INCIDENT_RECOVERY_KINDS or state == "resolved":
        reason = "recovery.confirmed"
        kwargs: dict[str, Any] = {"recovery_confirmed": True}
    elif (
        kind in _INCIDENT_BREAKING_KINDS
        or bool(detail.get("qualification_breaking"))
        or severity == "critical"
    ):
        reason = "incident.p1-slo"
        kwargs = {}
    elif kind in _INCIDENT_RECOVERY_STARTED_KINDS:
        reason = "recovery.started"
        kwargs = {}
    else:
        reason = "healthy"
        kwargs = {}
    return QualificationFactRecord(
        cursor=FactCursor(
            observed_at,
            _SOURCE_RANK_INCIDENT,
            _qualification_stable_id(row, event_id),
            ingest_seq=_qualification_ingest_seq(row),
        ),
        fact=QualificationFact(
            fact_id=f"incident:{incident_key}:{event_id}",
            observed_at=observed_at,
            reason=reason,
            **kwargs,
        ),
        source="incident",
    )


def recovery_action_row_to_fact_record(row: Mapping[str, object]) -> QualificationFactRecord:
    try:
        action_id = _nonempty(row["action_id"], "action_id")
        action_type = _nonempty(row["action_type"], "action_type")
        target_id = _nonempty(row["target_id"], "target_id")
        state = _nonempty(row["state"], "state")
        requested_at = _parse_datetime(row["requested_at"], "requested_at")
        detail = _detail(row.get("detail"))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("recovery action row is malformed") from exc
    started_at = _optional_datetime(row.get("started_at"), "started_at")
    finished_at = _optional_datetime(row.get("finished_at"), "finished_at")
    result_code = (
        None if row.get("result_code") is None else _nonempty(row["result_code"], "result_code")
    )
    observed_at = _qualification_recovery_observed_at(
        row,
        requested_at=requested_at,
        started_at=started_at,
        finished_at=finished_at,
    )
    reason = "healthy"
    kwargs: dict[str, Any] = {}
    if state == "completed" and result_code == "succeeded":
        if action_type not in _RECOVERY_ACTION_REASONS:
            raise ValueError(f"unknown successful recovery action type: {action_type}")
        reason = _RECOVERY_ACTION_REASONS[action_type]
        if reason in BREAKING_REASONS or reason == "recovery.started":
            kwargs = {}
        else:
            duration = (
                0 if finished_at is None else int((finished_at - requested_at).total_seconds())
            )
            kwargs = {
                "signature": str(detail.get("reason_code") or target_id),
                "recovery_duration_seconds": max(0, duration),
                "recovery_slo_seconds": _optional_int(
                    detail.get("recovery_slo_seconds"), "recovery_slo_seconds"
                )
                or 300,
                "resolved": True,
            }
    elif state == "completed" and result_code in {"failed", "disabled-action"}:
        reason = "recovery.human-intervention"
    elif state not in {"pending", "running", "completed"}:
        raise ValueError(f"unknown recovery action state: {state}")
    stable_id = _qualification_stable_id(row, action_id)
    return QualificationFactRecord(
        cursor=FactCursor(
            observed_at,
            _SOURCE_RANK_RECOVERY,
            stable_id,
            ingest_seq=_qualification_ingest_seq(row),
        ),
        fact=QualificationFact(
            fact_id=f"recovery:{target_id}:{stable_id}",
            observed_at=observed_at,
            reason=reason,
            **kwargs,
        ),
        source="recovery",
    )


def freshness_row_to_fact_record(row: Mapping[str, object]) -> QualificationFactRecord:
    try:
        fact_id = _nonempty(row["fact_id"], "fact_id")
        product = _nonempty(row["data_product"], "data_product")
        observed_at = _qualification_observed_at(row, "observed_at")
        freshness_seconds = _int(row["freshness_seconds"], "freshness_seconds")
        freshness_slo_seconds = _int(row["freshness_slo_seconds"], "freshness_slo_seconds")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("freshness row is malformed") from exc
    if product not in _FRESHNESS_PRODUCTS:
        raise ValueError(f"unknown freshness data product: {product}")
    reason = _optional_str(row.get("reason"), "reason") or "healthy"
    return QualificationFactRecord(
        cursor=FactCursor(
            observed_at,
            _SOURCE_RANK_FRESHNESS,
            _qualification_stable_id(row, fact_id),
            ingest_seq=_qualification_ingest_seq(row),
        ),
        fact=QualificationFact(
            fact_id=fact_id,
            observed_at=observed_at,
            reason=reason,
            freshness_product=product,
            freshness_seconds=freshness_seconds,
            freshness_slo_seconds=freshness_slo_seconds,
            progress_count=_optional_int(row.get("progress_count"), "progress_count"),
            successful_count=_optional_int(row.get("successful_count"), "successful_count"),
            evidence_complete=_bool(row.get("evidence_complete", True), "evidence_complete"),
        ),
        source="freshness",
    )


def ledger_row_to_fact_record(row: Mapping[str, object]) -> QualificationFactRecord:
    try:
        ingest_seq = _int(row["ingest_seq"], "ingest_seq")
        source = _nonempty(row["source"], "source")
        source_id = _nonempty(row["source_id"], "source_id")
        source_version = _nonempty(row["source_version"], "source_version")
        qualification_observed_at = _parse_datetime(
            row["qualification_observed_at"], "qualification_observed_at"
        )
        payload = _detail(row["payload"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("qualification ingress row is malformed") from exc
    payload["qualification_observed_at"] = qualification_observed_at
    payload["qualification_ingest_seq"] = ingest_seq
    payload["qualification_stable_id"] = f"{source}:{source_id}:{source_version}"
    if source == "runtime":
        return runtime_event_row_to_fact_record(payload)
    if source == "incident":
        return incident_event_row_to_fact_record(payload)
    if source == "recovery":
        return recovery_action_row_to_fact_record(payload)
    if source == "freshness":
        return freshness_row_to_fact_record(payload)
    raise ValueError(f"unknown qualification ingress source: {source}")


def _assert_cursor_batch(
    cursor: FactCursor | None,
    records: Sequence[QualificationFactRecord],
) -> None:
    previous = cursor
    for record in records:
        if previous is not None and record.cursor <= previous:
            raise QualificationCursorConflict("qualification source returned cursor replay")
        previous = record.cursor


def _is_recovery_confirmation_fact(fact: QualificationFact) -> bool:
    return fact.reason == "recovery.confirmed" and fact.recovery_confirmed


def _set_timeouts(cursor: psycopg.Cursor[Any]) -> None:
    cursor.execute(
        sql.SQL("SET LOCAL statement_timeout = {}").format(
            sql.Literal(f"{_STATEMENT_TIMEOUT_MS}ms")
        )
    )
    cursor.execute(
        sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(f"{_LOCK_TIMEOUT_MS}ms"))
    )


def _fetch_current_epoch(
    cursor: psycopg.Cursor[dict[str, Any]],
    *,
    policy: RollingQualificationPolicy,
    for_update: bool,
) -> QualificationEpochRecord | None:
    identity_key = _identity_key(policy)
    cursor.execute(
        "SELECT * FROM m1_qualification_epochs WHERE identity_key = %s "
        "AND (state IN ('accumulating', 'recovering') "
        "OR (state = 'qualified' AND NOT EXISTS ("
        "SELECT 1 FROM m1_qualification_certificates AS certificate "
        "WHERE certificate.epoch_id = m1_qualification_epochs.epoch_id))) "
        "ORDER BY started_at DESC, epoch_id DESC LIMIT 1" + (" FOR UPDATE" if for_update else ""),
        (identity_key,),
    )
    row = cursor.fetchone()
    return None if row is None else _epoch_from_row(row)


def _fetch_latest_epoch(cursor: psycopg.Cursor[dict[str, Any]]) -> QualificationEpochRecord | None:
    cursor.execute(
        "SELECT * FROM m1_qualification_epochs ORDER BY updated_at DESC, epoch_id DESC LIMIT 1"
    )
    row = cursor.fetchone()
    return None if row is None else _epoch_from_row(row)


def _fetch_epoch(
    cursor: psycopg.Cursor[dict[str, Any]], epoch_id: str, *, for_update: bool
) -> QualificationEpochRecord | None:
    cursor.execute(
        "SELECT * FROM m1_qualification_epochs WHERE epoch_id = %s"
        + (" FOR UPDATE" if for_update else ""),
        (epoch_id,),
    )
    row = cursor.fetchone()
    return None if row is None else _epoch_from_row(row)


def _ensure_source_cursor_row(
    cursor: psycopg.Cursor[dict[str, Any]],
    *,
    policy: RollingQualificationPolicy,
    writer_id: str | None,
    for_update: bool = False,
) -> Mapping[str, object]:
    identity_key = _identity_key(policy)
    cursor.execute(
        """
        INSERT INTO m1_qualification_source_cursors (
            identity_key, policy_version, release_id, config_id,
            role_identity, source_cursor, writer_id
        ) VALUES (%s, %s, %s, %s, %s, NULL, %s)
        ON CONFLICT (identity_key) DO NOTHING
        """,
        (
            identity_key,
            policy.policy_version,
            policy.release_id,
            policy.config_id,
            Jsonb(list(policy.role_identity)),
            writer_id,
        ),
    )
    cursor.execute(
        """
        SELECT identity_key, source_cursor
        FROM m1_qualification_source_cursors
        WHERE identity_key = %s
        """
        + (" FOR UPDATE" if for_update else ""),
        (identity_key,),
    )
    row = cursor.fetchone()
    if row is None:
        raise QualificationServiceError("qualification source cursor insert returned no row")
    return row


def _update_source_cursor(
    cursor: psycopg.Cursor[dict[str, Any]],
    *,
    policy: RollingQualificationPolicy,
    source_cursor: FactCursor,
    writer_id: str,
) -> None:
    cursor.execute(
        """
        UPDATE m1_qualification_source_cursors
        SET source_cursor = %s, writer_id = %s, updated_at = clock_timestamp()
        WHERE identity_key = %s
        """,
        (Jsonb(source_cursor.to_json()), writer_id, _identity_key(policy)),
    )
    if cursor.rowcount != 1:
        raise QualificationCursorConflict("qualification source cursor update failed")


def _append_recovery_observation(
    cursor: psycopg.Cursor[dict[str, Any]],
    *,
    recovering_epoch_id: str,
    fact_record: QualificationFactRecord,
) -> None:
    ingest_seq = fact_record.cursor.ingest_seq
    if ingest_seq is None:
        raise QualificationServiceError("Postgres recovery observation requires ingest_seq")
    record_payload = fact_record.to_json()
    record_digest = _sha256_json(record_payload)
    observation_id = f"qualification-recovery-observation:{recovering_epoch_id}:{ingest_seq}"
    cursor.execute(
        """
        INSERT INTO m1_qualification_recovery_observations (
            observation_id, recovering_epoch_id, ingest_seq, fact_id, reason,
            observed_at, fact_record, fact_record_sha256
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (recovering_epoch_id, ingest_seq) DO NOTHING
        """,
        (
            observation_id,
            recovering_epoch_id,
            ingest_seq,
            fact_record.fact.fact_id,
            fact_record.fact.reason,
            fact_record.fact.observed_at,
            Jsonb(record_payload),
            record_digest,
        ),
    )
    cursor.execute(
        """
        SELECT observation_id, fact_id, reason, observed_at, fact_record_sha256
        FROM m1_qualification_recovery_observations
        WHERE recovering_epoch_id = %s AND ingest_seq = %s
        """,
        (recovering_epoch_id, ingest_seq),
    )
    row = cursor.fetchone()
    if row is None:
        raise QualificationServiceError("recovery observation insert returned no row")
    if (
        str(row["observation_id"]) != observation_id
        or str(row["fact_id"]) != fact_record.fact.fact_id
        or str(row["reason"]) != fact_record.fact.reason
        or _require_aware(cast(datetime, row["observed_at"]), "observed_at")
        != fact_record.fact.observed_at
        or str(row["fact_record_sha256"]) != record_digest
    ):
        raise QualificationCursorConflict("recovery observation idempotency conflicts")


def _insert_epoch_cursor(
    cursor: psycopg.Cursor[dict[str, Any]],
    decision: QualificationDecision,
    *,
    source_cursor: FactCursor | None,
    fact_records: Sequence[QualificationFactRecord],
    writer_id: str | None,
) -> None:
    cursor.execute(
        """
        INSERT INTO m1_qualification_epochs (
            epoch_id, state, version, identity_key, policy_version, release_id,
            config_id, role_identity, started_at, last_fact_at, invalidated_at,
            invalidation_reason, qualified_at, previous_epoch_id, fact_digests,
            contained_recoveries, coverage_seconds, max_gap_seconds, progress_count,
            successful_count, evidence_digest, required_seconds, slo,
            contained_incident_details, recovery_action_details, writer_id,
            source_cursor, fact_records
        ) VALUES (
            %s, %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (epoch_id) DO NOTHING
        """,
        _epoch_values(
            decision, source_cursor=source_cursor, fact_records=fact_records, writer_id=writer_id
        ),
    )


def _update_epoch_cursor(
    cursor: psycopg.Cursor[dict[str, Any]],
    decision: QualificationDecision,
    *,
    source_cursor: FactCursor | None,
    fact_records: Sequence[QualificationFactRecord],
    writer_id: str,
) -> None:
    values = _epoch_values(
        decision, source_cursor=source_cursor, fact_records=fact_records, writer_id=writer_id
    )
    cursor.execute(
        """
        UPDATE m1_qualification_epochs
        SET state = %s, version = version + 1, identity_key = %s,
            policy_version = %s, release_id = %s, config_id = %s,
            role_identity = %s, started_at = %s, last_fact_at = %s,
            invalidated_at = %s, invalidation_reason = %s, qualified_at = %s,
            previous_epoch_id = %s, fact_digests = %s, contained_recoveries = %s,
            coverage_seconds = %s, max_gap_seconds = %s, progress_count = %s,
            successful_count = %s, evidence_digest = %s, required_seconds = %s,
            slo = %s, contained_incident_details = %s, recovery_action_details = %s,
            writer_id = %s, source_cursor = %s, fact_records = %s,
            updated_at = clock_timestamp()
        WHERE epoch_id = %s
        """,
        (*values[1:], decision.epoch_id),
    )
    if cursor.rowcount != 1:
        raise QualificationCursorConflict("qualification epoch update failed")


def _epoch_values(
    decision: QualificationDecision,
    *,
    source_cursor: FactCursor | None,
    fact_records: Sequence[QualificationFactRecord],
    writer_id: str | None,
) -> tuple[object, ...]:
    evidence = _derived_epoch_evidence(decision)
    return (
        decision.epoch_id,
        decision.state.value,
        _identity_key(decision),
        decision.policy_version,
        decision.release_id,
        decision.config_id,
        Jsonb(list(decision.role_identity)),
        decision.started_at,
        decision.last_fact_at,
        decision.invalidated_at,
        decision.invalidation_reason,
        decision.qualified_at,
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
        None if source_cursor is None else Jsonb(source_cursor.to_json()),
        Jsonb([record.to_json() for record in fact_records]),
    )


def _decision_from_epoch(record: QualificationEpochRecord) -> QualificationDecision:
    facts = tuple(record.fact for record in _records_from_epoch(record))
    return QualificationDecision(
        state=QualificationState(record.state),
        epoch_id=record.epoch_id,
        started_at=record.started_at,
        policy_version=record.policy_version,
        release_id=record.release_id,
        config_id=record.config_id,
        role_identity=record.role_identity,
        last_fact_at=record.last_fact_at,
        invalidated_at=record.invalidated_at,
        invalidation_reason=record.invalidation_reason,
        qualified_at=record.qualified_at,
        previous_epoch_id=record.previous_epoch_id,
        facts=facts,
        contained_recoveries=record.contained_recoveries,
        max_gap_seconds=record.max_gap_seconds,
        coverage_seconds=record.coverage_seconds,
        progress_count=record.progress_count,
        successful_count=record.successful_count,
    )


def _records_from_epoch(record: QualificationEpochRecord) -> tuple[QualificationFactRecord, ...]:
    return tuple(
        QualificationFactRecord.from_json(cast(Mapping[str, object], value))
        for value in cast(Sequence[object], record.fact_records)
    )


def _derived_epoch_evidence(decision: QualificationDecision) -> dict[str, object]:
    payload = (
        qualification_certificate_payload(decision)
        if decision.state is QualificationState.QUALIFIED
        else None
    )
    if payload is not None:
        return {
            "evidence_digest": payload["evidence_digest"],
            "required_seconds": cast(Mapping[str, object], payload["bounds"])["required_seconds"],
            "slo": payload["slo"],
            "contained_incidents": payload["contained_incidents"],
            "recovery_actions": payload["recovery_actions"],
        }
    evidence_payload = {
        "contained_incidents": [],
        "epoch_id": decision.epoch_id,
        "fact_digests": [list(item) for item in decision.fact_digests],
        "recovery_actions": [],
    }
    digest = json.dumps(
        evidence_payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    import hashlib

    return {
        "evidence_digest": hashlib.sha256(digest).hexdigest(),
        "required_seconds": None,
        "slo": {},
        "contained_incidents": [],
        "recovery_actions": [],
    }


def _epoch_from_row(row: Mapping[str, object]) -> QualificationEpochRecord:
    from .qualification_store import _epoch_from_row as store_epoch_from_row

    return store_epoch_from_row(row)


def _identity_key(value: RollingQualificationPolicy | QualificationDecision) -> str:
    if isinstance(value, RollingQualificationPolicy):
        payload = {
            "config_id": value.config_id,
            "policy_version": value.policy_version,
            "release_id": value.release_id,
            "role_identity": list(value.role_identity),
        }
    else:
        payload = {
            "config_id": value.config_id,
            "policy_version": value.policy_version,
            "release_id": value.release_id,
            "role_identity": list(value.role_identity),
        }
    return _sha256_json(payload)


def _sha256_json(payload: Mapping[str, object]) -> str:
    import hashlib

    return hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    ).hexdigest()


def _certificate_payload(record: QualificationCertificateRecord) -> dict[str, object]:
    if record.certificate_digest != certificate_digest(record.payload):
        raise QualificationServiceError("qualification certificate failed reverify")
    if record.canonical_payload.encode("utf-8") != canonical_certificate_bytes(record.payload):
        raise QualificationServiceError(
            "qualification certificate canonical payload failed reverify"
        )
    return {
        "certificate_id": record.certificate_id,
        "epoch_id": record.epoch_id,
        "certificate_digest": record.certificate_digest,
        "evidence_digest": record.evidence_digest,
        "created_at": record.created_at.isoformat(),
        "payload": record.payload,
        "reverified": True,
    }


def _tick_payload(result: QualificationTickResult) -> dict[str, object]:
    return {
        "status": result.status,
        "applied": result.applied,
        "cursor": None if result.cursor is None else result.cursor.to_json(),
        "epoch_id": result.epoch_id,
        "state": result.state.value,
        "certificate_digest": result.certificate_digest,
    }


def _last_fact_projection(records: Sequence[QualificationFactRecord]) -> dict[str, object] | None:
    if not records:
        return None
    record = records[-1]
    return {
        "fact_id": record.fact.fact_id,
        "reason": record.fact.reason,
        "observed_at": record.fact.observed_at.isoformat(),
        "source": record.source,
        "cursor": record.cursor.to_json(),
    }


def _epoch_breaker_projection(record: QualificationEpochRecord) -> dict[str, object] | None:
    if record.invalidation_reason is None:
        return None
    return {
        "reason": record.invalidation_reason,
        "observed_at": None if record.invalidated_at is None else record.invalidated_at.isoformat(),
    }


def _recovery_observation_status(
    cursor: psycopg.Cursor[dict[str, Any]],
    record: QualificationEpochRecord,
) -> dict[str, object]:
    if record.state != QualificationState.RECOVERING.value:
        return {
            "count": 0,
            "last_observation": None,
            "last_breaker": None,
            "last_breaking_observation": None,
        }
    cursor.execute(
        """
        SELECT count(*) AS observation_count
        FROM m1_qualification_recovery_observations
        WHERE recovering_epoch_id = %s
        """,
        (record.epoch_id,),
    )
    count_row = cursor.fetchone()
    count = 0 if count_row is None else int(cast(int, count_row["observation_count"]))
    cursor.execute(
        """
        SELECT ingest_seq, fact_id, reason, observed_at
        FROM m1_qualification_recovery_observations
        WHERE recovering_epoch_id = %s
        ORDER BY ingest_seq DESC
        LIMIT 1
        """,
        (record.epoch_id,),
    )
    last_row = cursor.fetchone()
    last_observation = None if last_row is None else _observation_projection(last_row)
    cursor.execute(
        """
        SELECT ingest_seq, fact_id, reason, observed_at
        FROM m1_qualification_recovery_observations
        WHERE recovering_epoch_id = %s AND reason = ANY(%s)
        ORDER BY ingest_seq DESC
        LIMIT 1
        """,
        (record.epoch_id, list(BREAKING_REASONS)),
    )
    breaker_row = cursor.fetchone()
    breaking_observation = None
    if breaker_row is not None:
        breaking_observation = _observation_projection(breaker_row)
        last_breaker: dict[str, object] | None = {
            "reason": str(breaker_row["reason"]),
            "observed_at": _require_aware(
                cast(datetime, breaker_row["observed_at"]), "observed_at"
            ).isoformat(),
        }
    elif record.previous_epoch_id is not None:
        previous = _fetch_epoch(cursor, record.previous_epoch_id, for_update=False)
        last_breaker = None if previous is None else _epoch_breaker_projection(previous)
    else:
        last_breaker = None
    return {
        "count": count,
        "last_observation": last_observation,
        "last_breaker": last_breaker,
        "last_breaking_observation": breaking_observation,
    }


def _observation_projection(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "fact_id": str(row["fact_id"]),
        "ingest_seq": int(cast(int, row["ingest_seq"])),
        "observed_at": _require_aware(
            cast(datetime, row["observed_at"]), "observed_at"
        ).isoformat(),
        "reason": str(row["reason"]),
    }


def _fact_to_json(fact: QualificationFact) -> dict[str, object]:
    return {
        "fact_id": fact.fact_id,
        "observed_at": fact.observed_at.isoformat(),
        "reason": fact.reason,
        "policy_version": fact.policy_version,
        "release_id": fact.release_id,
        "config_id": fact.config_id,
        "role_identity": None if fact.role_identity is None else list(fact.role_identity),
        "epoch_id": fact.epoch_id,
        "signature": fact.signature,
        "progress_count": fact.progress_count,
        "successful_count": fact.successful_count,
        "count": fact.count,
        "evidence_gap_seconds": fact.evidence_gap_seconds,
        "freshness_seconds": fact.freshness_seconds,
        "freshness_slo_seconds": fact.freshness_slo_seconds,
        "freshness_product": fact.freshness_product,
        "recovery_duration_seconds": fact.recovery_duration_seconds,
        "recovery_slo_seconds": fact.recovery_slo_seconds,
        "recovery_confirmed": fact.recovery_confirmed,
        "resolved": fact.resolved,
        "evidence_complete": fact.evidence_complete,
    }


def _fact_from_json(value: Mapping[str, object]) -> QualificationFact:
    return QualificationFact(
        fact_id=_nonempty(value["fact_id"], "fact_id"),
        observed_at=_parse_datetime(value["observed_at"], "observed_at"),
        reason=_nonempty(value["reason"], "reason"),
        policy_version=_optional_str(value.get("policy_version"), "policy_version"),
        release_id=_optional_str(value.get("release_id"), "release_id"),
        config_id=_optional_str(value.get("config_id"), "config_id"),
        role_identity=_optional_roles(value.get("role_identity")),
        epoch_id=_optional_str(value.get("epoch_id"), "epoch_id"),
        signature=_optional_str(value.get("signature"), "signature"),
        progress_count=_optional_int(value.get("progress_count"), "progress_count"),
        successful_count=_optional_int(value.get("successful_count"), "successful_count"),
        count=_optional_int(value.get("count"), "count"),
        evidence_gap_seconds=_optional_int(
            value.get("evidence_gap_seconds"), "evidence_gap_seconds"
        ),
        freshness_seconds=_optional_int(value.get("freshness_seconds"), "freshness_seconds"),
        freshness_slo_seconds=_optional_int(
            value.get("freshness_slo_seconds"), "freshness_slo_seconds"
        ),
        freshness_product=_optional_str(value.get("freshness_product"), "freshness_product"),
        recovery_duration_seconds=_optional_int(
            value.get("recovery_duration_seconds"), "recovery_duration_seconds"
        ),
        recovery_slo_seconds=_optional_int(
            value.get("recovery_slo_seconds"), "recovery_slo_seconds"
        ),
        recovery_confirmed=_bool(value.get("recovery_confirmed", False), "recovery_confirmed"),
        resolved=_bool(value.get("resolved", True), "resolved"),
        evidence_complete=_bool(value.get("evidence_complete", True), "evidence_complete"),
    )


def _require_aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_datetime(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        return _require_aware(value, field)
    if type(value) is str and value:
        return _require_aware(datetime.fromisoformat(value.replace("Z", "+00:00")), field)
    raise ValueError(f"{field} must be a timezone-aware datetime")


def _optional_datetime(value: object, field: str) -> datetime | None:
    return None if value is None else _parse_datetime(value, field)


def _nonempty(value: object, field: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{field} must be non-empty")
    return value


def _optional_str(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _nonempty(value, field)


def _optional_roles(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return (value,)
    if not isinstance(value, Sequence):
        raise ValueError("role_identity must be a sequence")
    roles = tuple(value)
    if any(type(role) is not str for role in roles):
        raise ValueError("role_identity must contain strings")
    return cast(tuple[str, ...], roles)


def _int(value: object, field: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{field} must be an integer")
    return value


def _optional_int(value: object, field: str) -> int | None:
    if value is None:
        return None
    return _int(value, field)


def _bool(value: object, field: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{field} must be a boolean")
    return value


def _detail(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("detail must be an object")
    return dict(value)


def _qualification_observed_at(row: Mapping[str, object], fallback_field: str) -> datetime:
    override = row.get("qualification_observed_at")
    return _parse_datetime(
        row[fallback_field] if override is None else override,
        "qualification_observed_at" if override is not None else fallback_field,
    )


def _qualification_recovery_observed_at(
    row: Mapping[str, object],
    *,
    requested_at: datetime,
    started_at: datetime | None,
    finished_at: datetime | None,
) -> datetime:
    override = row.get("qualification_observed_at")
    if override is not None:
        return _parse_datetime(override, "qualification_observed_at")
    return finished_at or started_at or requested_at


def _qualification_ingest_seq(row: Mapping[str, object]) -> int | None:
    return _optional_int(row.get("qualification_ingest_seq"), "qualification_ingest_seq")


def _qualification_stable_id(row: Mapping[str, object], fallback: str) -> str:
    return _optional_str(row.get("qualification_stable_id"), "qualification_stable_id") or fallback


def _cursor_time_key(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _freshness_gap_payload(*, product: str, now: datetime) -> dict[str, object]:
    return {
        "data_product": product,
        "evidence_complete": False,
        "fact_id": f"freshness:{product}:{_cursor_time_key(now)}:missing",
        "freshness_seconds": 0,
        "freshness_slo_seconds": 900,
        "observed_at": now.isoformat(),
        "progress_count": 0,
        "reason": "evidence.gap",
        "successful_count": 0,
    }


def _json_payload(row: Mapping[str, object]) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            payload[key] = value.astimezone(UTC).isoformat()
        else:
            payload[key] = value
    return payload


__all__ = [
    "FactCursor",
    "InMemoryQualificationStore",
    "ledger_row_to_fact_record",
    "PostgresQualificationFactSource",
    "PostgresQualificationServiceStore",
    "QualificationCursorConflict",
    "QualificationFactRecord",
    "QualificationService",
    "QualificationServiceError",
    "QualificationTickResult",
    "StaticQualificationFactSource",
    "freshness_row_to_fact_record",
    "incident_event_row_to_fact_record",
    "recovery_action_row_to_fact_record",
    "run_qualification_service",
    "runtime_event_row_to_fact_record",
]
