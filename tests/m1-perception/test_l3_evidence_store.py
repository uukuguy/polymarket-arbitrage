"""Append-only asyncpg boundary tests for L3 soak evidence."""

from __future__ import annotations

import asyncio
import inspect
import os
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from unittest.mock import AsyncMock
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import UUID, uuid4

import asyncpg
import pytest

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
        detail={"signal": "TERM"},
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
    def __init__(self, *, execute_error: BaseException | None = None) -> None:
        self.execute_error = execute_error
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
        return None

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.calls.append(("fetch", sql, args))
        return []

    async def close(self) -> None:
        self.closed = True


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
    _, sql, args = connection.calls[0]
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
    assert [call[0] for call in connection.calls] == ["execute", "executemany"]
    assert "l3_health_samples" in connection.calls[0][1]
    assert "l3_market_samples" in connection.calls[1][1]
    assert len(connection.calls[1][2]) == 5


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


async def test_event_append_serializes_nested_immutable_detail(
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
        detail={"nested": {"signal": "TERM"}, "attempts": [1, 2]},
    )

    assert await L3EvidenceStore("postgresql://secret").append_event(event)
    assert connection.calls[0][2][-1] == '{"attempts":[1,2],"nested":{"signal":"TERM"}}'


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
                "CREATE ROLE daemon_login LOGIN PASSWORD 'daemon-test-secret' IN ROLE service_role",
                "CREATE ROLE retention_login LOGIN PASSWORD 'retention-test-secret'",
                "GRANT l3_retention_operator TO retention_login",
                "GRANT SELECT ON l2_book_levels, l2_top_of_book, l2_ohlc_1m TO service_role",
            )
        )
        yield {
            "admin": admin_dsn,
            "daemon": _credential_dsn(admin_dsn, "daemon_login", "daemon-test-secret"),
            "retention": _credential_dsn(
                admin_dsn, "retention_login", "retention-test-secret"
            ),
        }


async def _admin_execute(dsn: str, *sql: str, args: tuple[object, ...] = ()) -> None:
    connection = await asyncpg.connect(dsn=dsn)
    try:
        for statement in sql:
            await connection.execute(statement, *args)
    finally:
        await connection.close()


@pytest.mark.slow
async def test_real_postgres_appends_duplicates_atomicity_windows_and_bounds(
    postgres_dsns: dict[str, str],
) -> None:
    now = datetime.now(UTC)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(minutes=1)
    boot = _boot(start - timedelta(minutes=2))
    store = L3EvidenceStore(postgres_dsns["daemon"])

    assert await store.append_boot(boot)
    assert not await store.append_boot(boot)
    for seq, status in enumerate(PromoteStatus):
        assert await store.append_promote_run(
            _promote(boot.boot_id, start + timedelta(seconds=seq), seq=seq, status=status)
        )
    assert not await store.append_promote_run(_promote(boot.boot_id, start, seq=0))
    batch = _batch(boot.boot_id, start)
    assert await store.append_sample(batch)
    assert not await store.append_sample(batch)
    assert await store.append_event(_event(boot.boot_id, start))
    assert await store.append_event(_event(boot.boot_id, end, seq=1))
    assert not await store.append_event(_event(boot.boot_id, start, seq=0))

    await _admin_execute(
        postgres_dsns["admin"],
        "CREATE FUNCTION reject_market_three() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN IF NEW.market_id = 'market-3' THEN RAISE EXCEPTION 'reject'; "
        "END IF; RETURN NEW; END $$",
        "CREATE TRIGGER reject_market_three BEFORE INSERT ON l3_market_samples "
        "FOR EACH ROW EXECUTE FUNCTION reject_market_three()",
    )
    failed_batch = _batch(boot.boot_id, start + timedelta(seconds=10), seq=9)
    assert not await store.append_sample(failed_batch)
    await _admin_execute(
        postgres_dsns["admin"],
        "DROP TRIGGER reject_market_three ON l3_market_samples",
        "DROP FUNCTION reject_market_three()",
    )
    admin = await asyncpg.connect(dsn=postgres_dsns["admin"])
    try:
        assert await admin.fetchval(
            "SELECT count(*) FROM l3_health_samples WHERE boot_id=$1 AND sample_seq=9",
            boot.boot_id,
        ) == 0
        assert await admin.fetchval(
            "SELECT count(*) FROM l3_market_samples WHERE boot_id=$1 AND sample_seq=9",
            boot.boot_id,
        ) == 0
        await admin.executemany(
            "INSERT INTO l2_book_levels (asset_id, ts, side, level, price, size) "
            "VALUES ($1,$2,'BUY',1,0.5,1)",
            [("yes-0", start), ("no-0", start), ("yes-0", end)],
        )
        await admin.executemany(
            "INSERT INTO l2_top_of_book (asset_id, ts, mid_price) VALUES ($1,$2,0.5)",
            [("yes-0", start), ("yes-0", end)],
        )
    finally:
        await admin.close()

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
    assert len(window.runtime_events) == 1
    assert window.runtime_events[0].occurred_at == start
    assert set(window.book_coverage_counts) == {
        *(f"yes-{index}" for index in range(5)),
        *(f"no-{index}" for index in range(5)),
    }
    assert window.book_coverage_counts["yes-0"] == 1
    assert window.book_coverage_counts["no-0"] == 1
    assert window.yes_ohlc_coverage_counts["yes-0"] == 1
    assert all(
        count == 0
        for token, count in window.yes_ohlc_coverage_counts.items()
        if token != "yes-0"
    )
    assert "id" in window.raw_rows_by_table["l3_promote_runs"][0]
    assert all(
        "recorded_at" in rows[0]
        for rows in window.raw_rows_by_table.values()
        if rows
    )
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
                ("l3_health_samples", "sampled_at"),
                ("l3_market_samples", "sampled_at"),
                ("l3_runtime_events", "occurred_at"),
            ):
                await admin.execute(
                    f"UPDATE {table} SET recorded_at=$1, {occurrence}=$1 WHERE boot_id=$2",
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
