"""Append-only asyncpg boundary tests for L3 soak evidence."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import subprocess
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import UUID, uuid4

import asyncpg
import pytest

from polyarb.observation import l3_evidence as evidence_module
from polyarb.observation.l3_evidence import (
    HealthSampleRecord,
    HealthStatus,
    L3EvidenceRuntime,
    MarketSampleRecord,
    PromoteRunRecord,
    PromoteStatus,
    RuntimeBootRecord,
    RuntimeEventKind,
    RuntimeEventRecord,
    RuntimeEventSeverity,
    RuntimeIdentity,
    SampleBatch,
)
from polyarb.storage import l3_evidence_store as store_module
from polyarb.storage.l3_evidence_store import (
    L3EvidenceStore,
    L3RetentionError,
    L3RetentionOperator,
    RuntimeEventIntegrityConflict,
    SamplingMarketState,
)

HASH = "a" * 64
MIGRATION_ENV = {
    "POLYARB_ALLOW_EMPTY_SECRET": "1",
    "POLYARB_ALLOW_EXTERNAL_PATHS": "1",
}


def _boot(at: datetime, *, boot_id: UUID | None = None) -> RuntimeBootRecord:
    return RuntimeBootRecord(
        boot_id=boot_id or uuid4(),
        started_at=at,
        machine_id="machine-1",
        machine_version="v1",
        image_ref="image@sha256:test",
        release_id="release-1",
        code_version="code-1",
        acceptance_config_hash=HASH,
    )


def _promote(
    boot_id: UUID,
    at: datetime,
    *,
    seq: int = 0,
    status: PromoteStatus = PromoteStatus.SUCCESS,
) -> PromoteRunRecord:
    return PromoteRunRecord(
        boot_id=boot_id,
        run_seq=seq,
        scheduled_at=at,
        started_at=at,
        finished_at=at,
        status=status,
        reason_code=f"terminal-{status.value}",
        selected_count=5,
        desired_count=10,
        committed_count=10,
        evidenced_count=10,
        add_count=10,
        remove_count=0,
        mapping_hash="b" * 64,
        desired_hash="c" * 64,
        committed_hash="d" * 64,
        acceptance_config_hash=HASH,
        ws_generation=seq,
        add_succeeded=True,
        remove_succeeded=True,
        mirror_succeeded=True,
        duration_ms=25,
    )


def _batch(boot_id: UUID, at: datetime, *, seq: int = 0) -> SampleBatch:
    health = HealthSampleRecord(
        boot_id=boot_id,
        sample_seq=seq,
        scheduled_at=at,
        sampled_at=at,
        desired_count=10,
        committed_count=10,
        evidenced_count=10,
        promote_age_ms=100,
        global_book_age_ms=50,
        ws_age_ms=20,
        mirror_age_ms=30,
        candidate_age_ms=40,
        reconciliation_age_ms=45,
        listener_state="connected",
        cursor_lag=0,
        watchdog_count=0,
        reconnect_count=0,
        ws_generation=seq,
        mapping_hash="b" * 64,
        acceptance_config_hash=HASH,
        status=HealthStatus.PASS,
        reason_code="healthy",
    )
    markets = tuple(
        MarketSampleRecord(
            boot_id=boot_id,
            sample_seq=seq,
            sampled_at=at,
            market_id=f"market-{index}",
            yes_token_id=f"yes-{index}",
            no_token_id=f"no-{index}",
            yes_desired=True,
            no_desired=True,
            yes_committed=True,
            no_committed=True,
            yes_evidenced=True,
            no_evidenced=True,
            evidence_generation=seq,
            yes_book_at=at,
            no_book_at=at,
            yes_book_age_ms=0,
            no_book_age_ms=0,
            worst_book_age_ms=0,
            yes_ohlc_at=at,
            yes_ohlc_age_ms=0,
            status=HealthStatus.PASS,
            reason_code="healthy",
        )
        for index in range(5)
    )
    return SampleBatch(health=health, markets=markets)


def _event(boot_id: UUID, at: datetime, *, seq: int = 0) -> RuntimeEventRecord:
    return RuntimeEventRecord(
        event_id=uuid4(),
        boot_id=boot_id,
        event_seq=seq,
        occurred_at=at,
        kind=RuntimeEventKind.SHUTDOWN_SIGNAL,
        severity=RuntimeEventSeverity.INFO,
        generation=seq,
        reason_code="test",
        detail={"signal": "SIGTERM"},
    )


class _FakeTransaction:
    def __init__(self, connection: _FakeConnection, options: dict[str, object]) -> None:
        self.connection = connection
        self.options = options

    async def __aenter__(self) -> _FakeTransaction:
        self.connection.transaction_entries += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        self.connection.transaction_exits.append(exc_type)
        return False


class _FakeConnection:
    def __init__(
        self,
        *,
        execute_error: BaseException | None = None,
        close_error: BaseException | None = None,
        authorization: MappingProxyType[str, object] | None = None,
    ) -> None:
        self.execute_error = execute_error
        self.close_error = close_error
        self.authorization = authorization or MappingProxyType(
            {
                "is_superuser": False,
                "can_login": True,
                "service_member": False,
                "daemon_member": True,
                "retention_member": False,
            }
        )
        self.calls: list[tuple[str, str, tuple[object, ...]]] = []
        self.transaction_options: list[dict[str, object]] = []
        self.transaction_entries = 0
        self.transaction_exits: list[object] = []
        self.closed = False

    def transaction(self, **options: object) -> _FakeTransaction:
        self.transaction_options.append(options)
        return _FakeTransaction(self, options)

    async def execute(self, sql: str, *args: object) -> str:
        self.calls.append(("execute", sql, args))
        if self.execute_error is not None:
            raise self.execute_error
        return "INSERT 0 1"

    async def executemany(self, sql: str, args: list[tuple[object, ...]]) -> None:
        self.calls.append(("executemany", sql, tuple(args)))
        if self.execute_error is not None:
            raise self.execute_error

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object] | None:
        self.calls.append(("fetchrow", sql, args))
        if "FROM pg_roles" in sql:
            return dict(self.authorization)
        return None

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.calls.append(("fetch", sql, args))
        return []

    async def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


@pytest.mark.parametrize(
    ("method_name", "record_factory", "table"),
    [
        ("append_boot", lambda boot, at: _boot(at, boot_id=boot), "l3_runtime_boots"),
        ("append_promote_run", lambda boot, at: _promote(boot, at), "l3_promote_runs"),
        ("append_event", lambda boot, at: _event(boot, at), "l3_runtime_events"),
    ],
)
async def test_one_shot_appends_are_parameterized_and_close(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    record_factory: object,
    table: str,
) -> None:
    connection = _FakeConnection()
    connect = AsyncMock(return_value=connection)
    monkeypatch.setattr(store_module.asyncpg, "connect", connect)
    boot_id = uuid4()
    record = record_factory(boot_id, datetime.now(UTC))  # type: ignore[operator]

    assert await getattr(L3EvidenceStore("postgresql://secret"), method_name)(record)

    connect.assert_awaited_once_with(dsn="postgresql://secret")
    assert connection.closed
    assert [call[0] for call in connection.calls] == ["fetchrow", "execute"]
    assert "pg_has_role" in connection.calls[0][1]
    assert all(
        role_name in connection.calls[0][1]
        for role_name in ("service_role", "l3_evidence_daemon", "l3_retention_operator")
    )
    _, sql, args = connection.calls[1]
    assert f"INSERT INTO {table}" in sql
    assert "$1" in sql
    assert str(boot_id) not in sql
    assert "postgresql://secret" not in sql
    assert args


async def test_sample_append_uses_one_transaction_and_exactly_five_market_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    monkeypatch.setattr(store_module.asyncpg, "connect", AsyncMock(return_value=connection))
    batch = _batch(uuid4(), datetime.now(UTC))

    assert await L3EvidenceStore("postgresql://secret").append_sample(batch)

    assert connection.transaction_entries == 1
    assert connection.transaction_exits == [None]
    assert [call[0] for call in connection.calls] == ["fetchrow", "execute", "executemany"]
    assert "l3_health_samples" in connection.calls[1][1]
    assert "l3_market_samples" in connection.calls[2][1]
    assert len(connection.calls[2][2]) == 5


async def test_sampling_market_state_uses_one_aggregate_query_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    at = datetime.now(UTC)
    connection = _FakeConnection()
    rows = [
        {
            "market_id": f"market-{index}",
            "yes_token_id": f"yes-{index}",
            "no_token_id": f"no-{index}",
            "yes_book_at": at,
            "no_book_at": at - timedelta(seconds=1),
            "yes_ohlc_at": at,
        }
        for index in range(5)
    ]
    connection.fetch = AsyncMock(return_value=rows)
    connect = AsyncMock(return_value=connection)
    monkeypatch.setattr(store_module.asyncpg, "connect", connect)
    token_ids = sorted({token for index in range(5) for token in (f"yes-{index}", f"no-{index}")})

    result = await L3EvidenceStore("postgresql://secret").fetch_sampling_market_state(token_ids)

    assert result == tuple(SamplingMarketState(**row) for row in rows)
    fetch_calls = [call for call in connection.calls if call[0] == "fetch"]
    assert len(fetch_calls) == 0  # AsyncMock owns the one aggregate call.
    connection.fetch.assert_awaited_once()
    sql, supplied_tokens = connection.fetch.await_args.args
    assert "l2_book_levels" in sql
    assert "l2_top_of_book" in sql
    assert "mid_price IS NOT NULL" in sql
    assert sql.count("LEFT JOIN LATERAL") == 3
    assert sql.count("ORDER BY ts DESC") == 3
    assert sql.count("LIMIT 1") == 3
    assert "max(ts)" not in sql
    assert "GROUP BY asset_id" not in sql
    assert "l2_ohlc_1m" not in sql
    assert "markets_latest" in sql
    assert supplied_tokens == token_ids
    assert connection.closed


async def test_candidate_market_fallback_reads_runtime_authorized_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [{"market_id": "market-1", "yes_token_id": "yes-1"}]
    connection = _FakeConnection()
    connection.fetch = AsyncMock(return_value=rows)
    monkeypatch.setattr(
        store_module.asyncpg,
        "connect",
        AsyncMock(return_value=connection),
    )

    result = await L3EvidenceStore(
        "postgresql://runtime-secret"
    ).fetch_candidate_markets_latest()

    assert result == rows
    connection.fetch.assert_awaited_once()
    assert "SELECT * FROM markets_latest" in connection.fetch.await_args.args[0]
    assert connection.closed


def _soak_lock_row(
    boot_id: UUID,
    *,
    t0: datetime,
    t24: datetime,
    mapping_hash: str = "b" * 64,
    recorded_at: datetime | None = None,
    **detail_overrides: object,
) -> dict[str, object]:
    return {
        "boot_id": boot_id,
        "recorded_at": recorded_at or t0 - timedelta(seconds=1),
        "detail": {
            "manifest_sha256": "c" * 64,
            "mapping_hash": mapping_hash,
            "t0": t0.isoformat().replace("+00:00", "Z"),
            "t24": t24.isoformat().replace("+00:00", "Z"),
            **detail_overrides,
        },
    }


def test_active_soak_mapping_lock_is_t0_inclusive_t24_exclusive() -> None:
    boot_id = uuid4()
    t0 = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
    t24 = t0 + timedelta(hours=24)
    rows = [_soak_lock_row(boot_id, t0=t0, t24=t24)]

    assert (
        store_module._active_soak_mapping_lock(
            rows,
            boot_id=boot_id,
            observed_at=t0 - timedelta(microseconds=1),
        )
        is None
    )
    active = store_module._active_soak_mapping_lock(
        rows,
        boot_id=boot_id,
        observed_at=t0,
    )
    assert active.mapping_hash == "b" * 64
    assert (active.t0, active.t24) == (t0, t24)
    assert (
        store_module._active_soak_mapping_lock(
            rows,
            boot_id=boot_id,
            observed_at=t24,
        )
        is None
    )


def test_active_soak_mapping_lock_accepts_same_hash_overlap_and_rejects_conflict() -> None:
    boot_id = uuid4()
    t0 = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
    t24 = t0 + timedelta(hours=24)
    rows = [
        _soak_lock_row(boot_id, t0=t0, t24=t24),
        _soak_lock_row(
            boot_id,
            t0=t0 + timedelta(minutes=5),
            t24=t24 + timedelta(minutes=5),
        ),
    ]
    active = store_module._active_soak_mapping_lock(
        rows,
        boot_id=boot_id,
        observed_at=t0 + timedelta(minutes=6),
    )
    assert active.mapping_hash == "b" * 64

    rows[1] = _soak_lock_row(
        boot_id,
        t0=t0 + timedelta(minutes=5),
        t24=t24 + timedelta(minutes=5),
        mapping_hash="d" * 64,
    )
    with pytest.raises(store_module.L3EvidenceReadError, match="conflicting"):
        store_module._active_soak_mapping_lock(
            rows,
            boot_id=boot_id,
            observed_at=t0 + timedelta(minutes=6),
        )


@pytest.mark.parametrize(
    "row",
    [
        lambda boot, t0, t24: _soak_lock_row(
            uuid4(),
            t0=t0,
            t24=t24,
        ),
        lambda boot, t0, t24: _soak_lock_row(
            boot,
            t0=t0,
            t24=t24,
            recorded_at=t0,
        ),
        lambda boot, t0, t24: _soak_lock_row(
            boot,
            t0=t0,
            t24=t24,
            mapping_hash="wrong",
        ),
        lambda boot, t0, t24: (
            lambda valid: {
                **valid,
                "detail": {**valid["detail"], "t0": "not-a-time"},
            }
        )(_soak_lock_row(boot, t0=t0, t24=t24)),
    ],
)
def test_active_soak_mapping_lock_rejects_malformed_rows(row: object) -> None:
    boot_id = uuid4()
    t0 = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
    t24 = t0 + timedelta(hours=24)
    with pytest.raises(store_module.L3EvidenceReadError):
        store_module._active_soak_mapping_lock(
            [row(boot_id, t0, t24)],  # type: ignore[operator]
            boot_id=boot_id,
            observed_at=t0 + timedelta(minutes=1),
        )


async def test_store_reads_active_soak_mapping_lock_with_runtime_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    boot_id = uuid4()
    t0 = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
    connection = _FakeConnection()
    connection.fetch = AsyncMock(
        return_value=[
            _soak_lock_row(
                boot_id,
                t0=t0,
                t24=t0 + timedelta(hours=24),
            )
        ]
    )
    monkeypatch.setattr(
        store_module.asyncpg,
        "connect",
        AsyncMock(return_value=connection),
    )

    lock = await L3EvidenceStore("postgresql://secret").fetch_active_soak_mapping_lock(
        boot_id=boot_id,
        observed_at=t0,
    )

    assert lock.mapping_hash == "b" * 64
    connection.fetch.assert_awaited_once()
    assert "soak_manifest_bound" in connection.fetch.await_args.args[0]
    assert connection.closed


@pytest.mark.parametrize("append_kind", ["one", "sample"])
async def test_durable_append_ack_survives_connection_close_failure(
    monkeypatch: pytest.MonkeyPatch,
    append_kind: str,
) -> None:
    connection = _FakeConnection(close_error=RuntimeError("close credential=never-log"))
    logs: list[str] = []
    breadcrumbs: list[dict[str, object]] = []
    monkeypatch.setattr(store_module.asyncpg, "connect", AsyncMock(return_value=connection))
    monkeypatch.setattr(
        store_module.logger,
        "warning",
        lambda message, *args: logs.append(message.format(*args)),
    )
    monkeypatch.setattr(
        store_module.sentry_sdk,
        "add_breadcrumb",
        lambda **data: breadcrumbs.append(data),
    )
    boot_id = uuid4()
    store = L3EvidenceStore("postgresql://secret")

    result = (
        await store.append_boot(_boot(datetime.now(UTC), boot_id=boot_id))
        if append_kind == "one"
        else await store.append_sample(_batch(boot_id, datetime.now(UTC)))
    )

    assert result is True
    assert connection.closed
    rendered = repr((logs, breadcrumbs))
    assert "credential" not in rendered
    assert "RuntimeError" in rendered


@pytest.mark.parametrize(
    "authorization",
    [
        {
            "is_superuser": True,
            "can_login": True,
            "service_member": False,
            "daemon_member": True,
            "retention_member": False,
        },
        {
            "is_superuser": False,
            "can_login": False,
            "service_member": False,
            "daemon_member": True,
            "retention_member": False,
        },
        {
            "is_superuser": False,
            "can_login": True,
            "service_member": False,
            "daemon_member": False,
            "retention_member": False,
        },
        {
            "is_superuser": False,
            "can_login": True,
            "service_member": False,
            "daemon_member": True,
            "retention_member": True,
        },
        {
            "is_superuser": False,
            "can_login": True,
            "service_member": True,
            "daemon_member": True,
            "retention_member": False,
        },
    ],
)
async def test_evidence_store_rejects_wrong_credential_topology_before_operation(
    monkeypatch: pytest.MonkeyPatch,
    authorization: dict[str, bool],
) -> None:
    connection = _FakeConnection(authorization=MappingProxyType(authorization))
    monkeypatch.setattr(store_module.asyncpg, "connect", AsyncMock(return_value=connection))

    assert not await L3EvidenceStore("postgresql://masked").append_boot(_boot(datetime.now(UTC)))

    assert [call[0] for call in connection.calls] == ["fetchrow"]
    assert connection.closed


@pytest.mark.parametrize("error", [RuntimeError("credential=never-log"), ValueError("bad")])
async def test_append_failures_are_bounded_secret_free_and_return_false(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    connection = _FakeConnection(execute_error=error)
    logs: list[str] = []
    breadcrumbs: list[dict[str, object]] = []
    monkeypatch.setattr(store_module.asyncpg, "connect", AsyncMock(return_value=connection))
    monkeypatch.setattr(
        store_module.logger,
        "warning",
        lambda message, *args: logs.append(message.format(*args)),
    )
    monkeypatch.setattr(
        store_module.sentry_sdk,
        "add_breadcrumb",
        lambda **data: breadcrumbs.append(data),
    )
    secret = "postgresql://daemon:credential@db/private"

    assert not await L3EvidenceStore(secret).append_event(_event(uuid4(), datetime.now(UTC)))

    rendered = repr((logs, breadcrumbs))
    assert "credential" not in rendered
    assert secret not in rendered
    assert type(error).__name__ in rendered
    assert connection.closed


async def test_connect_failure_is_secret_free_and_cancelled_error_is_reraised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs: list[str] = []
    monkeypatch.setattr(
        store_module.logger,
        "warning",
        lambda message, *args: logs.append(message.format(*args)),
    )
    connect = AsyncMock(side_effect=RuntimeError("postgresql://user:password@host/db"))
    monkeypatch.setattr(store_module.asyncpg, "connect", connect)
    store = L3EvidenceStore("postgresql://user:password@host/db")
    assert not await store.append_boot(_boot(datetime.now(UTC)))
    assert "password" not in repr(logs)

    monkeypatch.setattr(
        store_module.asyncpg,
        "connect",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    with pytest.raises(asyncio.CancelledError):
        await store.append_boot(_boot(datetime.now(UTC)))


async def test_event_append_serializes_whitelisted_immutable_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _FakeConnection()
    monkeypatch.setattr(store_module.asyncpg, "connect", AsyncMock(return_value=connection))
    event = RuntimeEventRecord(
        event_id=uuid4(),
        boot_id=uuid4(),
        event_seq=0,
        occurred_at=datetime.now(UTC),
        kind=RuntimeEventKind.SHUTDOWN_SIGNAL,
        detail={"signal": "SIGTERM"},
    )

    assert await L3EvidenceStore("postgresql://secret").append_event(event)
    assert connection.calls[1][2][-1] == '{"signal":"SIGTERM"}'


def _row_from_event_args(args: tuple[object, ...]) -> dict[str, object]:
    return {
        "event_id": args[0],
        "boot_id": args[1],
        "event_seq": args[2],
        "occurred_at": args[3],
        "kind": args[4],
        "severity": args[5],
        "generation": args[6],
        "reason_code": args[7],
        "detail": json.loads(str(args[8])),
    }


class _ReplayEventConnection(_FakeConnection):
    def __init__(
        self,
        shared: dict[str, object],
        *,
        commit_then_raise: bool = False,
    ) -> None:
        super().__init__()
        self.shared = shared
        self.commit_then_raise = commit_then_raise

    async def execute(self, sql: str, *args: object) -> str:
        self.calls.append(("execute", sql, args))
        if "INSERT INTO l3_runtime_events" not in sql:
            return "INSERT 0 1"
        if "row" not in self.shared:
            self.shared["row"] = _row_from_event_args(args)
            if self.commit_then_raise:
                raise ConnectionError("commit response lost")
            return "INSERT 0 1"
        return "INSERT 0 0"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.calls.append(("fetch", sql, args))
        if "FROM l3_runtime_events" in sql:
            return [dict(self.shared["row"])]  # type: ignore[arg-type]
        return []


async def test_event_append_commit_then_client_error_replays_idempotently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared: dict[str, object] = {}
    first = _ReplayEventConnection(shared, commit_then_raise=True)
    second = _ReplayEventConnection(shared)
    monkeypatch.setattr(
        store_module.asyncpg,
        "connect",
        AsyncMock(side_effect=[first, second]),
    )
    record = _event(uuid4(), datetime.now(UTC))
    store = L3EvidenceStore("postgresql://secret")

    assert await store.append_event(record) is False
    assert await store.append_event(record) is True

    assert first.closed and second.closed
    assert "ON CONFLICT DO NOTHING" in first.calls[1][1]
    assert [call[0] for call in second.calls] == ["fetchrow", "execute", "fetch"]


async def test_event_append_conflicting_replay_fails_visibly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = _event(uuid4(), datetime.now(UTC))
    shared: dict[str, object] = {"row": _row_from_event_args(store_module._event_args(original))}
    connection = _ReplayEventConnection(shared)
    monkeypatch.setattr(
        store_module.asyncpg,
        "connect",
        AsyncMock(return_value=connection),
    )

    assert hasattr(store_module, "RuntimeEventIntegrityConflict")
    with pytest.raises(
        store_module.RuntimeEventIntegrityConflict,
        match="runtime event replay payload conflict",
    ):
        await L3EvidenceStore("postgresql://secret").append_event(
            replace(original, reason_code="payload_changed")
        )
    assert connection.closed


async def test_oversize_postgres_jsonb_detail_is_rejected_before_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = AsyncMock()
    monkeypatch.setattr(store_module.asyncpg, "connect", connect)
    empty_size = len(evidence_module._postgres_jsonb_text({"payload": ""}).encode("utf-8"))
    accepted_count = (2048 - empty_size) // len("界".encode())

    with pytest.raises(ValueError, match="not allowed"):
        RuntimeEventRecord(
            event_id=uuid4(),
            boot_id=uuid4(),
            event_seq=0,
            occurred_at=datetime.now(UTC),
            kind=RuntimeEventKind.SHUTDOWN_SIGNAL,
            detail={"payload": "界" * (accepted_count + 1)},
        )

    connect.assert_not_awaited()


async def test_event_store_rejects_record_impostor_before_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = AsyncMock()
    monkeypatch.setattr(store_module.asyncpg, "connect", connect)
    forged = SimpleNamespace(
        event_id=uuid4(),
        boot_id=uuid4(),
        event_seq=0,
        occurred_at=datetime.now(UTC),
        kind=RuntimeEventKind.SHUTDOWN_SIGNAL,
        severity=RuntimeEventSeverity.INFO,
        generation=0,
        reason_code="signal",
        detail={"dsn": "postgresql://secret"},
    )

    assert not await L3EvidenceStore("postgresql://secret").append_event(forged)
    connect.assert_not_awaited()


@pytest.mark.parametrize("method_name", ["append_boot", "append_promote_run", "append_event"])
async def test_one_shot_argument_failures_stay_inside_fail_soft_envelope(
    monkeypatch: pytest.MonkeyPatch, method_name: str
) -> None:
    connect = AsyncMock()
    monkeypatch.setattr(store_module.asyncpg, "connect", connect)

    assert not await getattr(L3EvidenceStore("postgresql://secret"), method_name)(object())
    connect.assert_not_awaited()


async def test_store_does_not_own_runtime_anchor_transitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = RuntimeIdentity(
        machine_id="machine",
        machine_version="v1",
        image_ref="image",
        release_id="release",
        code_version="code",
        recipe_sha256="1" * 64,
        acceptance_config_hash=HASH,
    )
    runtime = L3EvidenceRuntime(identity, started_at=datetime.now(UTC))
    boot_id = runtime.snapshot().boot_id
    connection = _FakeConnection()
    monkeypatch.setattr(store_module.asyncpg, "connect", AsyncMock(return_value=connection))

    assert await L3EvidenceStore("postgresql://secret").append_promote_run(
        _promote(boot_id, datetime.now(UTC))
    )

    assert runtime.snapshot().last_promote_persisted_at is None
    assert runtime.snapshot().last_sample_persisted_at is None


def test_store_surface_is_append_only_without_generic_mutators() -> None:
    forbidden = {
        "execute",
        "delete",
        "cleanup",
        "finalize_boot",
        "update_boot",
        "run_retention_cleanup",
    }
    assert forbidden.isdisjoint(dir(L3EvidenceStore))
    source = Path(store_module.__file__).read_text(encoding="utf-8")
    store_source = source[
        source.index("class L3EvidenceStore") : source.index("class L3RetentionOperator")
    ]
    assert " UPDATE " not in store_source.upper()
    assert " DELETE " not in store_source.upper()


def test_store_has_no_method_that_accepts_caller_supplied_sql() -> None:
    sql_parameter_names = {"sql", "query", "statement", "command"}
    for name, method in inspect.getmembers(L3EvidenceStore, inspect.iscoroutinefunction):
        parameters = set(inspect.signature(method).parameters)
        assert parameters.isdisjoint(sql_parameter_names), (
            f"{name} accepts caller-provided SQL through {parameters & sql_parameter_names}"
        )

    source = inspect.getsource(L3EvidenceStore)
    assert "connection.execute(sql" not in source


async def test_closed_append_helper_rejects_unknown_operation_without_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect = AsyncMock()
    monkeypatch.setattr(store_module.asyncpg, "connect", connect)
    store = L3EvidenceStore("postgresql://secret")

    assert not await store._append_one("DELETE FROM l3_runtime_boots", lambda: ())  # type: ignore[arg-type]
    connect.assert_not_awaited()


def _credential_dsn(admin_dsn: str, username: str, password: str) -> str:
    parts = urlsplit(admin_dsn)
    hostname = parts.hostname or "localhost"
    port = f":{parts.port}" if parts.port else ""
    netloc = f"{quote(username)}:{quote(password)}@{hostname}{port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


async def _prepare_roles(dsn: str) -> None:
    connection = await asyncpg.connect(dsn=dsn)
    try:
        for role in ("anon", "authenticated", "service_role"):
            await connection.execute(f"CREATE ROLE {role} NOLOGIN")
    finally:
        await connection.close()


@pytest.fixture(scope="module")
def postgres_dsns() -> Iterator[dict[str, str]]:
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        admin_dsn = postgres.get_connection_url()
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if admin_dsn.startswith(prefix):
                admin_dsn = "postgresql://" + admin_dsn[len(prefix) :]
        asyncio.run(_prepare_roles(admin_dsn))
        result = subprocess.run(
            ["uv", "run", "alembic", "upgrade", "007"],
            env={**os.environ, **MIGRATION_ENV, "POLYARB_SUPABASE_DB_DSN": admin_dsn},
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        asyncio.run(
            _admin_execute(
                admin_dsn,
                "CREATE ROLE daemon_login LOGIN PASSWORD 'daemon-test-secret' "
                "IN ROLE l3_evidence_daemon",
                "CREATE ROLE service_login LOGIN PASSWORD 'service-test-secret' "
                "IN ROLE service_role",
                "CREATE ROLE retention_login LOGIN PASSWORD 'retention-test-secret'",
                "GRANT l3_retention_operator TO retention_login",
                "CREATE ROLE daemon_service_login LOGIN PASSWORD 'daemon-service-secret' "
                "IN ROLE l3_evidence_daemon, service_role",
                "CREATE ROLE daemon_retention_login LOGIN PASSWORD 'daemon-retention-secret' "
                "IN ROLE l3_evidence_daemon, l3_retention_operator",
                "CREATE ROLE service_retention_login LOGIN PASSWORD 'service-retention-secret' "
                "IN ROLE service_role, l3_retention_operator",
                "CREATE ROLE super_operator_login LOGIN SUPERUSER "
                "PASSWORD 'super-operator-secret' IN ROLE l3_retention_operator",
            )
        )
        yield {
            "admin": admin_dsn,
            "daemon": _credential_dsn(admin_dsn, "daemon_login", "daemon-test-secret"),
            "service": _credential_dsn(admin_dsn, "service_login", "service-test-secret"),
            "retention": _credential_dsn(admin_dsn, "retention_login", "retention-test-secret"),
            "daemon_service": _credential_dsn(
                admin_dsn, "daemon_service_login", "daemon-service-secret"
            ),
            "daemon_retention": _credential_dsn(
                admin_dsn, "daemon_retention_login", "daemon-retention-secret"
            ),
            "service_retention": _credential_dsn(
                admin_dsn, "service_retention_login", "service-retention-secret"
            ),
            "super_operator": _credential_dsn(
                admin_dsn, "super_operator_login", "super-operator-secret"
            ),
        }


async def _admin_execute(dsn: str, *sql: str, args: tuple[object, ...] = ()) -> None:
    connection = await asyncpg.connect(dsn=dsn)
    try:
        for statement in sql:
            await connection.execute(statement, *args)
    finally:
        await connection.close()


def _postgres_window_bounds(
    now: datetime,
) -> tuple[datetime, datetime, datetime, int]:
    start = now.replace(second=0, microsecond=0)
    end = now + timedelta(seconds=20)
    ohlc_bucket = end.replace(second=0, microsecond=0)
    ohlc_bucket_count = int((ohlc_bucket - start) / timedelta(minutes=1)) + 1
    return start, end, ohlc_bucket, ohlc_bucket_count


@pytest.mark.parametrize(
    ("second", "expected_bucket_minute", "expected_bucket_count"),
    ((5, 0, 1), (55, 1, 2)),
)
def test_real_postgres_window_bounds_follow_end_bucket_across_minute(
    second: int, expected_bucket_minute: int, expected_bucket_count: int
) -> None:
    now = datetime(2030, 1, 1, 12, 0, second, tzinfo=UTC)
    start, end, ohlc_bucket, ohlc_bucket_count = _postgres_window_bounds(now)

    assert start <= now < end
    assert end == now + timedelta(seconds=20)
    assert ohlc_bucket == start + timedelta(minutes=expected_bucket_minute)
    assert ohlc_bucket_count == expected_bucket_count


@pytest.mark.slow
async def test_real_postgres_appends_duplicates_atomicity_windows_and_bounds(
    postgres_dsns: dict[str, str],
) -> None:
    now = datetime.now(UTC)
    start, end, expected_ohlc_at, expected_ohlc_count = _postgres_window_bounds(now)
    boot = _boot(start - timedelta(minutes=2))
    store = L3EvidenceStore(postgres_dsns["daemon"])

    assert await store.append_boot(boot)
    assert not await store.append_boot(boot)
    for seq, status in enumerate(PromoteStatus):
        assert await store.append_promote_run(_promote(boot.boot_id, now, seq=seq, status=status))
    assert not await store.append_promote_run(_promote(boot.boot_id, now, seq=0))
    batch = _batch(boot.boot_id, now)
    assert await store.append_sample(batch)
    assert not await store.append_sample(batch)
    first_event = _event(boot.boot_id, start)
    assert await store.append_event(first_event)
    assert await store.append_event(first_event)
    boundary_detail = {"signal": "SIGTERM"}
    boundary_event = RuntimeEventRecord(
        event_id=uuid4(),
        boot_id=boot.boot_id,
        event_seq=8,
        occurred_at=start,
        kind=RuntimeEventKind.SHUTDOWN_SIGNAL,
        detail=boundary_detail,
    )
    assert len(evidence_module._postgres_jsonb_text(boundary_detail).encode("utf-8")) <= 2048
    assert await store.append_event(boundary_event)
    assert await store.append_event(_event(boot.boot_id, end, seq=1))
    with pytest.raises(RuntimeEventIntegrityConflict):
        await store.append_event(_event(boot.boot_id, start, seq=0))

    await _admin_execute(
        postgres_dsns["admin"],
        "CREATE FUNCTION reject_market_three() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN IF NEW.market_id = 'market-3' THEN RAISE EXCEPTION 'reject'; "
        "END IF; RETURN NEW; END $$",
        "CREATE TRIGGER reject_market_three BEFORE INSERT ON l3_market_samples "
        "FOR EACH ROW EXECUTE FUNCTION reject_market_three()",
    )
    failed_batch = _batch(boot.boot_id, now + timedelta(seconds=10), seq=9)
    assert not await store.append_sample(failed_batch)
    await _admin_execute(
        postgres_dsns["admin"],
        "DROP TRIGGER reject_market_three ON l3_market_samples",
        "DROP FUNCTION reject_market_three()",
    )
    admin = await asyncpg.connect(dsn=postgres_dsns["admin"])
    try:
        assert (
            await admin.fetchval(
                "SELECT count(*) FROM l3_health_samples WHERE boot_id=$1 AND sample_seq=9",
                boot.boot_id,
            )
            == 0
        )
        assert (
            await admin.fetchval(
                "SELECT count(*) FROM l3_market_samples WHERE boot_id=$1 AND sample_seq=9",
                boot.boot_id,
            )
            == 0
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await admin.execute(
                "INSERT INTO l3_runtime_events "
                "(event_id, boot_id, event_seq, occurred_at, kind, severity, detail) "
                "VALUES (gen_random_uuid(), $1, 90, $2, 'shutdown_signal', 'info', "
                "jsonb_build_object('payload', repeat('x', 1800), 'value', 1e300::numeric))",
                boot.boot_id,
                start,
            )
        with pytest.raises(asyncpg.PostgresError):
            await admin.execute(
                "INSERT INTO l3_runtime_events "
                "(event_id, boot_id, event_seq, occurred_at, kind, severity, detail) "
                "VALUES (gen_random_uuid(), $1, 91, $2, 'shutdown_signal', 'info', $3::jsonb)",
                boot.boot_id,
                start,
                json.dumps({"nested": {"value": "before\x00after"}}),
            )
        await admin.executemany(
            "INSERT INTO l2_book_levels (asset_id, ts, side, level, price, size) "
            "VALUES ($1,$2,'BUY',1,0.5,1)",
            [("yes-0", start), ("no-0", start), ("yes-0", end)],
        )
        await admin.executemany(
            "INSERT INTO l2_top_of_book (asset_id, ts, mid_price) VALUES ($1,$2,0.5)",
            [("yes-0", start), ("yes-0", end)],
        )
        await admin.executemany(
            "INSERT INTO markets_latest (market_id, yes_token_id, no_token_id) VALUES ($1,$2,$3)",
            [(f"market-{index}", f"yes-{index}", f"no-{index}") for index in range(5)],
        )
    finally:
        await admin.close()

    sampling_state = await store.fetch_sampling_market_state(
        sorted({token for index in range(5) for token in (f"yes-{index}", f"no-{index}")})
    )
    assert len(sampling_state) == 5
    assert sampling_state[0] == SamplingMarketState(
        market_id="market-0",
        yes_token_id="yes-0",
        no_token_id="no-0",
        yes_book_at=end,
        no_book_at=start,
        yes_ohlc_at=end,
    )
    assert all(
        market.yes_book_at is None and market.no_book_at is None and market.yes_ohlc_at is None
        for market in sampling_state[1:]
    )

    status = await store.fetch_status(boot_id=boot.boot_id)
    assert status is not None
    assert isinstance(status, MappingProxyType)
    assert set(status) == {
        "boot_id",
        "acceptance_config_hash",
        "started_at",
        "latest_promote_recorded_at",
        "latest_sample_recorded_at",
        "latest_event_recorded_at",
        "promote_count",
        "health_sample_count",
        "market_sample_count",
        "runtime_event_count",
    }
    assert status["promote_count"] == 4
    assert status["market_sample_count"] == 5
    assert await store.fetch_status(boot_id=uuid4()) is None

    window = await store.fetch_window(start, end)
    assert window.boots == (boot,)
    assert [row.status for row in window.promote_runs] == list(PromoteStatus)
    assert len(window.health_samples) == 1
    assert len(window.market_samples) == 5
    assert len(window.runtime_events) == 2
    assert {event.event_seq for event in window.runtime_events} == {0, 8}
    assert all(event.occurred_at == start for event in window.runtime_events)
    assert next(event for event in window.runtime_events if event.event_seq == 8).detail == {
        "signal": "SIGTERM"
    }
    assert set(window.book_coverage_counts) == {
        *(f"yes-{index}" for index in range(5)),
        *(f"no-{index}" for index in range(5)),
    }
    assert window.book_coverage_counts["yes-0"] == 1
    assert window.book_coverage_counts["no-0"] == 1
    assert window.yes_ohlc_coverage_counts["yes-0"] == expected_ohlc_count
    assert all(
        count == 0 for token, count in window.yes_ohlc_coverage_counts.items() if token != "yes-0"
    )
    assert "id" in window.raw_rows_by_table["l3_promote_runs"][0]
    assert all("recorded_at" in rows[0] for rows in window.raw_rows_by_table.values() if rows)
    bounds = await store.retention_bounds()
    assert set(bounds.row_count_by_table) == {
        "l3_runtime_boots",
        "l3_promote_runs",
        "l3_health_samples",
        "l3_market_samples",
        "l3_runtime_events",
    }
    assert bounds.row_count_by_table["l3_runtime_boots"] == 1


@pytest.mark.slow
@pytest.mark.parametrize("credential", ["admin", "service", "retention"])
async def test_real_postgres_store_rejects_non_daemon_credentials(
    postgres_dsns: dict[str, str],
    credential: str,
) -> None:
    assert not await L3EvidenceStore(postgres_dsns[credential]).append_boot(
        _boot(datetime.now(UTC))
    )


@pytest.mark.slow
@pytest.mark.parametrize(
    ("credential", "boundary"),
    [
        ("daemon_service", "evidence"),
        ("daemon_retention", "retention"),
        ("service_retention", "retention"),
        ("super_operator", "retention"),
    ],
)
async def test_real_postgres_rejects_nonexclusive_role_combinations(
    postgres_dsns: dict[str, str],
    credential: str,
    boundary: str,
) -> None:
    if boundary == "evidence":
        assert not await L3EvidenceStore(postgres_dsns[credential]).append_boot(
            _boot(datetime.now(UTC))
        )
        return

    now = datetime.now(UTC)
    with pytest.raises(L3RetentionError, match="credential is not authorized"):
        await L3RetentionOperator(postgres_dsns[credential]).run_retention_cleanup(
            cutoff=now - timedelta(days=31),
            protected_start=now - timedelta(days=36),
            protected_end=now - timedelta(days=34),
        )


@pytest.mark.slow
async def test_real_postgres_retention_requires_separate_login_and_protected_function(
    postgres_dsns: dict[str, str],
) -> None:
    now = datetime.now(UTC)
    eligible = _boot(now)
    protected = _boot(now)
    store = L3EvidenceStore(postgres_dsns["daemon"])
    assert await store.append_boot(eligible)
    assert await store.append_promote_run(_promote(eligible.boot_id, now))
    assert await store.append_sample(_batch(eligible.boot_id, now))
    assert await store.append_event(_event(eligible.boot_id, now))
    assert await store.append_boot(protected)
    assert await store.append_event(_event(protected.boot_id, now))

    admin = await asyncpg.connect(dsn=postgres_dsns["admin"])
    try:
        async with admin.transaction():
            await admin.execute("SET LOCAL polyarb.retention_cleanup = 'on'")
            for table, occurrence in (
                ("l3_runtime_boots", "started_at"),
                ("l3_market_samples", "sampled_at"),
                ("l3_runtime_events", "occurred_at"),
            ):
                await admin.execute(
                    f"UPDATE {table} SET recorded_at=$1, {occurrence}=$1 WHERE boot_id=$2",
                    now - timedelta(days=40),
                    eligible.boot_id,
                )
            await admin.execute(
                "UPDATE l3_health_samples SET recorded_at=$1, sampled_at=$1, "
                "scheduled_at=$1 WHERE boot_id=$2",
                now - timedelta(days=40),
                eligible.boot_id,
            )
            await admin.execute(
                "UPDATE l3_promote_runs SET recorded_at=$1, scheduled_at=$1, "
                "started_at=$1, finished_at=$1 WHERE boot_id=$2",
                now - timedelta(days=40),
                eligible.boot_id,
            )
            await admin.execute(
                "UPDATE l3_runtime_boots SET recorded_at=$1, started_at=$1 WHERE boot_id=$2",
                now - timedelta(days=35),
                protected.boot_id,
            )
            await admin.execute(
                "UPDATE l3_runtime_events SET recorded_at=$1, occurred_at=$1 WHERE boot_id=$2",
                now - timedelta(days=35),
                protected.boot_id,
            )
    finally:
        await admin.close()

    with pytest.raises(L3RetentionError, match="credential is not authorized"):
        await L3RetentionOperator(postgres_dsns["daemon"]).run_retention_cleanup(
            cutoff=now - timedelta(days=31),
            protected_start=now - timedelta(days=36),
            protected_end=now - timedelta(days=34),
        )

    operator = L3RetentionOperator(postgres_dsns["retention"])
    with pytest.raises(ValueError):
        await operator.run_retention_cleanup(
            cutoff=now - timedelta(days=29),
            protected_start=now - timedelta(days=36),
            protected_end=now - timedelta(days=34),
        )
    with pytest.raises(ValueError):
        await operator.run_retention_cleanup(
            cutoff=now - timedelta(days=31),
            protected_start=now - timedelta(days=34),
            protected_end=now - timedelta(days=36),
        )
    result = await operator.run_retention_cleanup(
        cutoff=now - timedelta(days=31),
        protected_start=now - timedelta(days=36),
        protected_end=now - timedelta(days=34),
    )
    assert result.runtime_boots_deleted == 1
    assert result.promote_runs_deleted == 1
    assert result.health_samples_deleted == 1
    assert result.market_samples_deleted == 5
    assert result.runtime_events_deleted == 1

    admin = await asyncpg.connect(dsn=postgres_dsns["admin"])
    try:
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT 1 FROM l3_runtime_boots WHERE boot_id=$1)", eligible.boot_id
        )
        assert await admin.fetchval(
            "SELECT EXISTS(SELECT 1 FROM l3_runtime_boots WHERE boot_id=$1)", protected.boot_id
        )
    finally:
        await admin.close()


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (datetime.now(), datetime.now(UTC)),
        (datetime.now(UTC), datetime.now()),
        (datetime.now(UTC), datetime.now(UTC) - timedelta(seconds=1)),
    ],
)
async def test_read_window_rejects_non_utc_or_reversed_intervals(
    start: datetime, end: datetime
) -> None:
    with pytest.raises(ValueError):
        await L3EvidenceStore("postgresql://unused").fetch_window(start, end)
