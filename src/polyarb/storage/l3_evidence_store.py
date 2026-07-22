"""Append-only asyncpg storage boundary for L3 continuous-soak evidence."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Any
from uuid import UUID

import asyncpg
import sentry_sdk
from loguru import logger

from polyarb.observation.l3_evidence import (
    EVIDENCE_TABLES,
    EvidenceWindow,
    HealthSampleRecord,
    HealthStatus,
    MarketSampleRecord,
    PromoteRunRecord,
    PromoteStatus,
    RetentionBounds,
    RuntimeBootRecord,
    RuntimeEventKind,
    RuntimeEventRecord,
    RuntimeEventSeverity,
    SampleBatch,
)


class L3EvidenceReadError(RuntimeError):
    """A bounded read failure that never exposes connection credentials."""


class L3RetentionError(RuntimeError):
    """A bounded retention failure that never exposes connection credentials."""


@dataclass(frozen=True, slots=True)
class RetentionCleanupResult:
    runtime_boots_deleted: int
    promote_runs_deleted: int
    health_samples_deleted: int
    market_samples_deleted: int
    runtime_events_deleted: int


_BOOT_INSERT = """
INSERT INTO l3_runtime_boots (
    boot_id, started_at, machine_id, machine_version, image_ref,
    release_id, code_version, acceptance_config_hash
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
"""

_PROMOTE_INSERT = """
INSERT INTO l3_promote_runs (
    boot_id, run_seq, scheduled_at, started_at, finished_at, status, reason_code,
    selected_count, desired_count, committed_count, evidenced_count, add_count,
    remove_count, mapping_hash, desired_hash, committed_hash,
    acceptance_config_hash, ws_generation, add_succeeded, remove_succeeded,
    mirror_succeeded, duration_ms
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22
)
"""

_HEALTH_INSERT = """
INSERT INTO l3_health_samples (
    boot_id, sample_seq, sampled_at, desired_count, committed_count, evidenced_count,
    promote_age_ms, global_book_age_ms, ws_age_ms, mirror_age_ms, candidate_age_ms,
    reconciliation_age_ms, listener_state, cursor_lag, watchdog_count,
    reconnect_count, ws_generation, mapping_hash, acceptance_config_hash, status,
    reason_code
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
)
"""

_MARKET_INSERT = """
INSERT INTO l3_market_samples (
    boot_id, sample_seq, sampled_at, market_id, yes_token_id, no_token_id,
    yes_desired, no_desired, yes_committed, no_committed, yes_evidenced,
    no_evidenced, evidence_generation, yes_book_at, no_book_at, yes_book_age_ms,
    no_book_age_ms, worst_book_age_ms, yes_ohlc_at, yes_ohlc_age_ms, status,
    reason_code
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22
)
"""

_EVENT_INSERT = """
INSERT INTO l3_runtime_events (
    event_id, boot_id, event_seq, occurred_at, kind, severity, generation,
    reason_code, detail
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
"""


class _AppendOperation(StrEnum):
    BOOT = "append_boot"
    PROMOTE_RUN = "append_promote_run"
    EVENT = "append_event"


_APPEND_STATEMENTS: Mapping[_AppendOperation, str] = MappingProxyType(
    {
        _AppendOperation.BOOT: _BOOT_INSERT,
        _AppendOperation.PROMOTE_RUN: _PROMOTE_INSERT,
        _AppendOperation.EVENT: _EVENT_INSERT,
    }
)

_PROMOTE_WINDOW = """
SELECT * FROM l3_promote_runs
WHERE scheduled_at >= $1 AND scheduled_at < $2
ORDER BY scheduled_at, boot_id, run_seq
"""

_HEALTH_WINDOW = """
SELECT * FROM l3_health_samples
WHERE sampled_at >= $1 AND sampled_at < $2
ORDER BY sampled_at, boot_id, sample_seq
"""

_MARKET_WINDOW = """
SELECT * FROM l3_market_samples
WHERE sampled_at >= $1 AND sampled_at < $2
ORDER BY sampled_at, boot_id, sample_seq, market_id
"""

_EVENT_WINDOW = """
SELECT * FROM l3_runtime_events
WHERE occurred_at >= $1 AND occurred_at < $2
ORDER BY occurred_at, boot_id, event_seq
"""

_BOOT_WINDOW = """
SELECT * FROM l3_runtime_boots
WHERE (started_at >= $1 AND started_at < $2) OR boot_id = ANY($3::uuid[])
ORDER BY started_at, boot_id
"""

_BOOK_COVERAGE = """
WITH identities AS (
    SELECT unnest($3::text[]) AS asset_id
), covered AS (
    SELECT asset_id, ts
    FROM l2_book_levels
    WHERE ts >= $1 AND ts < $2 AND asset_id = ANY($3::text[])
    GROUP BY asset_id, ts
)
SELECT identities.asset_id, count(covered.ts)::bigint AS coverage_count
FROM identities LEFT JOIN covered USING (asset_id)
GROUP BY identities.asset_id ORDER BY identities.asset_id
"""

_OHLC_COVERAGE = """
WITH identities AS (
    SELECT unnest($3::text[]) AS asset_id
), covered AS (
    SELECT asset_id, bucket_ts
    FROM l2_ohlc_1m
    WHERE bucket_ts >= $1 AND bucket_ts < $2 AND asset_id = ANY($3::text[])
    GROUP BY asset_id, bucket_ts
)
SELECT identities.asset_id, count(covered.bucket_ts)::bigint AS coverage_count
FROM identities LEFT JOIN covered USING (asset_id)
GROUP BY identities.asset_id ORDER BY identities.asset_id
"""

_STATUS = """
SELECT
    boot.boot_id,
    boot.acceptance_config_hash,
    boot.started_at,
    (SELECT max(recorded_at) FROM l3_promote_runs WHERE boot_id=boot.boot_id)
        AS latest_promote_recorded_at,
    (SELECT max(recorded_at) FROM l3_health_samples WHERE boot_id=boot.boot_id)
        AS latest_sample_recorded_at,
    (SELECT max(recorded_at) FROM l3_runtime_events WHERE boot_id=boot.boot_id)
        AS latest_event_recorded_at,
    (SELECT count(*) FROM l3_promote_runs WHERE boot_id=boot.boot_id)::bigint
        AS promote_count,
    (SELECT count(*) FROM l3_health_samples WHERE boot_id=boot.boot_id)::bigint
        AS health_sample_count,
    (SELECT count(*) FROM l3_market_samples WHERE boot_id=boot.boot_id)::bigint
        AS market_sample_count,
    (SELECT count(*) FROM l3_runtime_events WHERE boot_id=boot.boot_id)::bigint
        AS runtime_event_count
FROM l3_runtime_boots AS boot WHERE boot.boot_id=$1
"""

_RETENTION_BOUNDS = """
SELECT 'l3_runtime_boots' AS table_name, min(recorded_at) AS oldest,
       max(recorded_at) AS newest, count(*)::bigint AS row_count FROM l3_runtime_boots
UNION ALL
SELECT 'l3_promote_runs', min(recorded_at), max(recorded_at), count(*)::bigint
FROM l3_promote_runs
UNION ALL
SELECT 'l3_health_samples', min(recorded_at), max(recorded_at), count(*)::bigint
FROM l3_health_samples
UNION ALL
SELECT 'l3_market_samples', min(recorded_at), max(recorded_at), count(*)::bigint
FROM l3_market_samples
UNION ALL
SELECT 'l3_runtime_events', min(recorded_at), max(recorded_at), count(*)::bigint
FROM l3_runtime_events
"""

_RETENTION_AUTHORIZATION = """
SELECT
    role.rolcanlogin AS can_login,
    pg_has_role(current_user, $1::name, 'MEMBER') AS operator_member,
    pg_has_role(current_user, $2::name, 'MEMBER') AS daemon_member
FROM pg_roles AS role WHERE role.rolname=current_user
"""

_RETENTION_CALL = "SELECT * FROM l3_retention_cleanup($1,$2,$3)"

_EVIDENCE_AUTHORIZATION = """
SELECT
    role.rolsuper AS is_superuser,
    role.rolcanlogin AS can_login,
    pg_has_role(current_user, 'l3_evidence_daemon', 'MEMBER') AS daemon_member,
    pg_has_role(current_user, 'l3_retention_operator', 'MEMBER') AS retention_member
FROM pg_roles AS role WHERE role.rolname=current_user
"""


def _require_utc_interval(start: datetime, end: datetime) -> None:
    for name, value in (("start", start), ("end", end)):
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise ValueError(f"{name} must be timezone-aware UTC")
    if start >= end:
        raise ValueError("start must precede end")


def _report_failure(operation: str, error: BaseException) -> None:
    error_type = type(error).__name__
    logger.warning("l3 evidence operation={} failed error_type={}", operation, error_type)
    try:
        sentry_sdk.add_breadcrumb(
            category="l3-evidence",
            level="warning",
            message="evidence storage operation failed",
            data={"operation": operation, "error_type": error_type},
        )
    except Exception:  # noqa: BLE001 - observability cannot break the storage envelope
        logger.warning("l3 evidence operation={} breadcrumb_failed", operation)


async def _require_evidence_daemon(connection: asyncpg.Connection) -> None:
    authorization = await connection.fetchrow(_EVIDENCE_AUTHORIZATION)
    if (
        authorization is None
        or authorization["is_superuser"]
        or not authorization["can_login"]
        or not authorization["daemon_member"]
        or authorization["retention_member"]
    ):
        raise PermissionError("evidence credential is not authorized")


def _boot_args(record: RuntimeBootRecord) -> tuple[object, ...]:
    return (
        record.boot_id,
        record.started_at,
        record.machine_id,
        record.machine_version,
        record.image_ref,
        record.release_id,
        record.code_version,
        record.acceptance_config_hash,
    )


def _promote_args(record: PromoteRunRecord) -> tuple[object, ...]:
    return (
        record.boot_id,
        record.run_seq,
        record.scheduled_at,
        record.started_at,
        record.finished_at,
        record.status.value,
        record.reason_code,
        record.selected_count,
        record.desired_count,
        record.committed_count,
        record.evidenced_count,
        record.add_count,
        record.remove_count,
        record.mapping_hash,
        record.desired_hash,
        record.committed_hash,
        record.acceptance_config_hash,
        record.ws_generation,
        record.add_succeeded,
        record.remove_succeeded,
        record.mirror_succeeded,
        record.duration_ms,
    )


def _health_args(record: HealthSampleRecord) -> tuple[object, ...]:
    return (
        record.boot_id,
        record.sample_seq,
        record.sampled_at,
        record.desired_count,
        record.committed_count,
        record.evidenced_count,
        record.promote_age_ms,
        record.global_book_age_ms,
        record.ws_age_ms,
        record.mirror_age_ms,
        record.candidate_age_ms,
        record.reconciliation_age_ms,
        record.listener_state,
        record.cursor_lag,
        record.watchdog_count,
        record.reconnect_count,
        record.ws_generation,
        record.mapping_hash,
        record.acceptance_config_hash,
        record.status.value,
        record.reason_code,
    )


def _market_args(record: MarketSampleRecord) -> tuple[object, ...]:
    return (
        record.boot_id,
        record.sample_seq,
        record.sampled_at,
        record.market_id,
        record.yes_token_id,
        record.no_token_id,
        record.yes_desired,
        record.no_desired,
        record.yes_committed,
        record.no_committed,
        record.yes_evidenced,
        record.no_evidenced,
        record.evidence_generation,
        record.yes_book_at,
        record.no_book_at,
        record.yes_book_age_ms,
        record.no_book_age_ms,
        record.worst_book_age_ms,
        record.yes_ohlc_at,
        record.yes_ohlc_age_ms,
        record.status.value,
        record.reason_code,
    )


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _event_args(record: RuntimeEventRecord) -> tuple[object, ...]:
    return (
        record.event_id,
        record.boot_id,
        record.event_seq,
        record.occurred_at,
        record.kind.value,
        record.severity.value,
        record.generation,
        record.reason_code,
        json.dumps(_thaw_json(record.detail), sort_keys=True, separators=(",", ":")),
    )


def _decode_boot(row: Mapping[str, Any]) -> RuntimeBootRecord:
    return RuntimeBootRecord(
        boot_id=row["boot_id"],
        started_at=row["started_at"],
        machine_id=row["machine_id"],
        machine_version=row["machine_version"],
        image_ref=row["image_ref"],
        release_id=row["release_id"],
        code_version=row["code_version"],
        acceptance_config_hash=row["acceptance_config_hash"],
    )


def _decode_promote(row: Mapping[str, Any]) -> PromoteRunRecord:
    return PromoteRunRecord(
        boot_id=row["boot_id"],
        run_seq=row["run_seq"],
        scheduled_at=row["scheduled_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        status=PromoteStatus(row["status"]),
        reason_code=row["reason_code"],
        selected_count=row["selected_count"],
        desired_count=row["desired_count"],
        committed_count=row["committed_count"],
        evidenced_count=row["evidenced_count"],
        add_count=row["add_count"],
        remove_count=row["remove_count"],
        mapping_hash=row["mapping_hash"],
        desired_hash=row["desired_hash"],
        committed_hash=row["committed_hash"],
        acceptance_config_hash=row["acceptance_config_hash"],
        ws_generation=row["ws_generation"],
        add_succeeded=row["add_succeeded"],
        remove_succeeded=row["remove_succeeded"],
        mirror_succeeded=row["mirror_succeeded"],
        duration_ms=row["duration_ms"],
    )


def _decode_health(row: Mapping[str, Any]) -> HealthSampleRecord:
    return HealthSampleRecord(
        **{
            key: row[key]
            for key in (
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
                "reason_code",
            )
        },
        status=HealthStatus(row["status"]),
    )


def _decode_market(row: Mapping[str, Any]) -> MarketSampleRecord:
    return MarketSampleRecord(
        **{
            key: row[key]
            for key in (
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
                "reason_code",
            )
        },
        status=HealthStatus(row["status"]),
    )


def _decode_event(row: Mapping[str, Any]) -> RuntimeEventRecord:
    detail = row["detail"]
    if isinstance(detail, str):
        detail = json.loads(detail)
    return RuntimeEventRecord(
        event_id=row["event_id"],
        boot_id=row["boot_id"],
        event_seq=row["event_seq"],
        occurred_at=row["occurred_at"],
        kind=RuntimeEventKind(row["kind"]),
        severity=RuntimeEventSeverity(row["severity"]),
        generation=row["generation"],
        reason_code=row["reason_code"] or "",
        detail=detail or {},
    )


class L3EvidenceStore:
    """Short-lived, fail-soft appends and snapshot-consistent evidence reads."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    async def _append_one(
        self,
        operation: _AppendOperation,
        args_factory: Callable[[], tuple[object, ...]],
    ) -> bool:
        connection: asyncpg.Connection | None = None
        succeeded = False
        operation_name = (
            operation.value
            if isinstance(operation, _AppendOperation)
            else "invalid_append_operation"
        )
        try:
            statement = _APPEND_STATEMENTS[operation]
            args = args_factory()
            connection = await asyncpg.connect(dsn=self._dsn)
            await _require_evidence_daemon(connection)
            await connection.execute(statement, *args)
            succeeded = True
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - fail-soft evidence writer boundary
            _report_failure(operation_name, error)
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001 - close remains fail-soft
                    _report_failure(f"{operation_name}_close", error)
        return succeeded

    async def append_boot(self, record: RuntimeBootRecord) -> bool:
        return await self._append_one(_AppendOperation.BOOT, lambda: _boot_args(record))

    async def append_promote_run(self, record: PromoteRunRecord) -> bool:
        return await self._append_one(
            _AppendOperation.PROMOTE_RUN, lambda: _promote_args(record)
        )

    async def append_sample(self, batch: SampleBatch) -> bool:
        connection: asyncpg.Connection | None = None
        succeeded = False
        try:
            markets = tuple(batch.markets)
            if len(markets) != 5:
                raise ValueError("sample batch requires exactly five markets")
            if len({row.market_id for row in markets}) != 5:
                raise ValueError("sample batch market IDs must be distinct")
            yes_ids = {row.yes_token_id for row in markets}
            no_ids = {row.no_token_id for row in markets}
            if len(yes_ids) != 5 or len(no_ids) != 5 or len(yes_ids | no_ids) != 10:
                raise ValueError("sample batch token IDs must be distinct")
            if any(
                row.boot_id != batch.health.boot_id
                or row.sample_seq != batch.health.sample_seq
                or row.sampled_at != batch.health.sampled_at
                for row in markets
            ):
                raise ValueError("sample batch rows must share identity and occurrence time")
            connection = await asyncpg.connect(dsn=self._dsn)
            await _require_evidence_daemon(connection)
            async with connection.transaction():
                await connection.execute(_HEALTH_INSERT, *_health_args(batch.health))
                await connection.executemany(
                    _MARKET_INSERT, [_market_args(market) for market in markets]
                )
            succeeded = True
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - fail-soft evidence writer boundary
            _report_failure("append_sample", error)
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001 - close remains fail-soft
                    _report_failure("append_sample_close", error)
        return succeeded

    async def append_event(self, record: RuntimeEventRecord) -> bool:
        return await self._append_one(_AppendOperation.EVENT, lambda: _event_args(record))

    async def fetch_status(self, *, boot_id: UUID) -> Mapping[str, object] | None:
        connection: asyncpg.Connection | None = None
        try:
            connection = await asyncpg.connect(dsn=self._dsn)
            await _require_evidence_daemon(connection)
            row = await connection.fetchrow(_STATUS, boot_id)
            if row is None:
                return None
            return MappingProxyType(dict(row))
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - convert to bounded typed read error
            _report_failure("fetch_status", error)
            raise L3EvidenceReadError("l3 evidence status read failed") from None
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001
                    _report_failure("fetch_status_close", error)
                    raise L3EvidenceReadError("l3 evidence status read failed") from None

    async def fetch_window(self, start: datetime, end: datetime) -> EvidenceWindow:
        _require_utc_interval(start, end)
        connection: asyncpg.Connection | None = None
        try:
            connection = await asyncpg.connect(dsn=self._dsn)
            await _require_evidence_daemon(connection)
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                promote_rows = await connection.fetch(_PROMOTE_WINDOW, start, end)
                health_rows = await connection.fetch(_HEALTH_WINDOW, start, end)
                market_rows = await connection.fetch(_MARKET_WINDOW, start, end)
                event_rows = await connection.fetch(_EVENT_WINDOW, start, end)
                referenced_boots = sorted(
                    {
                        row["boot_id"]
                        for rows in (promote_rows, health_rows, market_rows, event_rows)
                        for row in rows
                    },
                    key=str,
                )
                boot_rows = await connection.fetch(
                    _BOOT_WINDOW, start, end, referenced_boots
                )
                yes_tokens = sorted({row["yes_token_id"] for row in market_rows})
                all_tokens = sorted(
                    {
                        token
                        for row in market_rows
                        for token in (row["yes_token_id"], row["no_token_id"])
                    }
                )
                book_rows = await connection.fetch(
                    _BOOK_COVERAGE, start, end, all_tokens
                )
                ohlc_rows = await connection.fetch(
                    _OHLC_COVERAGE, start, end, yes_tokens
                )
            raw = {
                "l3_runtime_boots": tuple(dict(row) for row in boot_rows),
                "l3_promote_runs": tuple(dict(row) for row in promote_rows),
                "l3_health_samples": tuple(dict(row) for row in health_rows),
                "l3_market_samples": tuple(dict(row) for row in market_rows),
                "l3_runtime_events": tuple(dict(row) for row in event_rows),
            }
            return EvidenceWindow(
                start=start,
                end=end,
                boots=tuple(_decode_boot(row) for row in boot_rows),
                promote_runs=tuple(_decode_promote(row) for row in promote_rows),
                health_samples=tuple(_decode_health(row) for row in health_rows),
                market_samples=tuple(_decode_market(row) for row in market_rows),
                runtime_events=tuple(_decode_event(row) for row in event_rows),
                book_coverage_counts={
                    row["asset_id"]: row["coverage_count"] for row in book_rows
                },
                yes_ohlc_coverage_counts={
                    row["asset_id"]: row["coverage_count"] for row in ohlc_rows
                },
                raw_rows_by_table=raw,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - never return a partial evidence window
            _report_failure("fetch_window", error)
            raise L3EvidenceReadError("l3 evidence window read failed") from None
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001
                    _report_failure("fetch_window_close", error)
                    raise L3EvidenceReadError("l3 evidence window read failed") from None

    async def retention_bounds(self) -> RetentionBounds:
        connection: asyncpg.Connection | None = None
        try:
            connection = await asyncpg.connect(dsn=self._dsn)
            await _require_evidence_daemon(connection)
            rows = await connection.fetch(_RETENTION_BOUNDS)
            by_table = {row["table_name"]: row for row in rows}
            if set(by_table) != EVIDENCE_TABLES:
                raise ValueError("retention bounds did not return all evidence tables")
            return RetentionBounds(
                oldest_recorded_at_by_table={
                    table: by_table[table]["oldest"] for table in EVIDENCE_TABLES
                },
                newest_recorded_at_by_table={
                    table: by_table[table]["newest"] for table in EVIDENCE_TABLES
                },
                row_count_by_table={
                    table: by_table[table]["row_count"] for table in EVIDENCE_TABLES
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - convert to bounded typed read error
            _report_failure("retention_bounds", error)
            raise L3EvidenceReadError("l3 evidence retention bounds read failed") from None
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001
                    _report_failure("retention_bounds_close", error)
                    raise L3EvidenceReadError(
                        "l3 evidence retention bounds read failed"
                    ) from None


class L3RetentionOperator:
    """Dedicated capability for invoking the protected retention function."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    async def run_retention_cleanup(
        self,
        *,
        cutoff: datetime,
        protected_start: datetime,
        protected_end: datetime,
    ) -> RetentionCleanupResult:
        _require_utc_interval(protected_start, protected_end)
        if cutoff.tzinfo is None or cutoff.utcoffset() != timedelta(0):
            raise ValueError("cutoff must be timezone-aware UTC")
        if cutoff > datetime.now(UTC) - timedelta(days=30):
            raise ValueError("retention cutoff must be at least 30 days old")

        connection: asyncpg.Connection | None = None
        try:
            connection = await asyncpg.connect(dsn=self._dsn)
            authorization = await connection.fetchrow(
                _RETENTION_AUTHORIZATION,
                "l3_retention_operator",
                "service_role",
            )
            if (
                authorization is None
                or not authorization["can_login"]
                or not authorization["operator_member"]
                or authorization["daemon_member"]
            ):
                raise L3RetentionError("retention credential is not authorized")
            row = await connection.fetchrow(
                _RETENTION_CALL, cutoff, protected_start, protected_end
            )
            if row is None:
                raise L3RetentionError("retention function returned no result")
            return RetentionCleanupResult(
                runtime_boots_deleted=row["runtime_boots_deleted"],
                promote_runs_deleted=row["promote_runs_deleted"],
                health_samples_deleted=row["health_samples_deleted"],
                market_samples_deleted=row["market_samples_deleted"],
                runtime_events_deleted=row["runtime_events_deleted"],
            )
        except asyncio.CancelledError:
            raise
        except L3RetentionError:
            raise
        except Exception as error:  # noqa: BLE001 - convert to bounded typed operator error
            _report_failure("retention_cleanup", error)
            raise L3RetentionError("retention cleanup failed") from None
        finally:
            if connection is not None:
                try:
                    await connection.close()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001
                    _report_failure("retention_cleanup_close", error)
                    raise L3RetentionError("retention cleanup failed") from None
