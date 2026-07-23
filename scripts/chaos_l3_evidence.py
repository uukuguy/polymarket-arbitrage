"""Local full-chain chaos evidence for the five Phase 05.4 L3 failure modes.

The harness creates a disposable PostgreSQL 16 Testcontainer, migrates it to
revision 007, and exercises the production sampler, evidence store, runtime,
Starlette health endpoint, and exact-window verdict.  It never discovers or
contacts a deployed application.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from urllib.parse import quote, urlsplit, urlunsplit

import asyncpg
from pydantic import SecretStr
from starlette.testclient import TestClient
from testcontainers.postgres import PostgresContainer

from polyarb.daemon import ws_watchdog
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.http.l2_app import create_l2_app
from polyarb.observation import l3_sampler
from polyarb.observation.l3_evidence import (
    AcceptanceConfig,
    HealthStatus,
    L3EvidenceRuntime,
    PromoteRunRecord,
    PromoteStatus,
    RuntimeBootRecord,
    RuntimeEventKind,
    RuntimeIdentity,
    stable_sha256,
)
from polyarb.observation.l3_soak_verdict import (
    ManifestReport,
    SoakManifest,
    VerdictStatus,
    build_soak_report,
)
from polyarb.storage.l3_evidence_store import L3EvidenceStore

MODES = ("sampler", "writer", "ws-false", "one-hot", "restart")
IMAGE_DIGEST = "c" * 64
IMAGE_REF = "local/polyarb@sha256:" + IMAGE_DIGEST
RECIPE_HASH = "a" * 64
CODE_VERSION = "0.1.0"
MIGRATION_ENV = {
    "POLYARB_ALLOW_EMPTY_SECRET": "1",
    "POLYARB_ALLOW_EXTERNAL_PATHS": "1",
}


class ProofFailure(RuntimeError):
    """A local chain assertion failed without exposing connection details."""


class _LocalWs:
    def __init__(self) -> None:
        self.reject_control = False

    async def send(self, _payload: str) -> None:
        if self.reject_control:
            raise RuntimeError("local control rejection")

    async def close(self) -> None:
        return None


@dataclass(frozen=True)
class _Chain:
    runtime: L3EvidenceRuntime
    store: L3EvidenceStore
    consumer: WsConsumer
    websocket: _LocalWs
    started_at: datetime
    mapping_hash: str


def _normalize_dsn(dsn: str) -> str:
    for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    return dsn


def _credential_dsn(admin_dsn: str, username: str, password: str) -> str:
    parts = urlsplit(admin_dsn)
    hostname = parts.hostname or "localhost"
    port = f":{parts.port}" if parts.port else ""
    netloc = f"{quote(username)}:{quote(password)}@{hostname}{port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _acceptance() -> AcceptanceConfig:
    return AcceptanceConfig(
        recipe_sha256=RECIPE_HASH,
        sample_interval_s=30,
        max_sample_gap_s=75,
        promote_interval_s=300,
        promote_max_start_gap_s=360,
        market_book_fresh_s=120,
        market_ohlc_fresh_s=120,
        expected_market_count=5,
        expected_token_count=10,
        retention_days=30,
        schema_revision="007",
        code_version=CODE_VERSION,
    )


def _mapping_rows() -> tuple[dict[str, str], ...]:
    return tuple(
        {
            "market_id": f"market-{index}",
            "yes_token_id": f"yes-{index}",
            "no_token_id": f"no-{index}",
        }
        for index in range(5)
    )


def _tokens() -> frozenset[str]:
    return frozenset(
        token
        for row in _mapping_rows()
        for token in (row["yes_token_id"], row["no_token_id"])
    )


def _manifest(chain: _Chain) -> SoakManifest:
    acceptance = _acceptance()
    t0 = chain.started_at
    reports = tuple(
        ManifestReport(
            checkpoint=label,
            start=t0,
            end=(
                t0 + timedelta(seconds=acceptance.sample_interval_s)
                if hours == 0
                else t0 + timedelta(hours=hours)
            ),
            path=f"local/{label.lower().replace('+', '')}.json",
        )
        for label, hours in (
            ("T+0", 0),
            ("T+6", 6),
            ("T+12", 12),
            ("T+18", 18),
            ("T+24", 24),
        )
    )
    status = chain.runtime.snapshot()
    return SoakManifest(
        schema_version=1,
        t0=t0,
        t24=t0 + timedelta(hours=24),
        reports=reports,
        boot_id=status.boot_id,
        machine_id="local-machine",
        machine_version="local-version",
        image_ref=IMAGE_REF,
        image_digest=IMAGE_DIGEST,
        release_id="local-release",
        code_version=CODE_VERSION,
        mapping_hash=chain.mapping_hash,
        acceptance_config=acceptance,
        acceptance_config_hash=acceptance.digest(),
    )


def _codes(report: object) -> set[str]:
    return {reason.code for reason in report.reasons}  # type: ignore[attr-defined]


class LocalEvidenceHarness:
    """One disposable setup/teardown path shared by every chaos mode."""

    def __init__(self) -> None:
        self._postgres = PostgresContainer("postgres:16-alpine")
        self._admin_dsn = ""
        self._daemon_dsn = ""

    def start(self) -> None:
        self._postgres.start()
        self._admin_dsn = _normalize_dsn(self._postgres.get_connection_url())
        asyncio.run(self._prepare_database())
        result = subprocess.run(
            ["uv", "run", "alembic", "upgrade", "007"],
            env={
                **os.environ,
                **MIGRATION_ENV,
                "POLYARB_SUPABASE_DB_DSN": self._admin_dsn,
            },
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if result.returncode != 0:
            raise ProofFailure("local migration to revision 007 failed")
        asyncio.run(self._create_daemon_login())

    def stop(self) -> Exception | None:
        """Attempt cleanup for no-handle, partial-start, and ready containers."""
        try:
            self._postgres.stop()
        except Exception as error:  # cleanup failure is secondary to any run failure
            return error
        return None

    async def _prepare_database(self) -> None:
        connection = await asyncpg.connect(dsn=self._admin_dsn)
        try:
            for role in ("anon", "authenticated", "service_role"):
                await connection.execute(f"CREATE ROLE {role} NOLOGIN")
        finally:
            await connection.close()

    async def _create_daemon_login(self) -> None:
        connection = await asyncpg.connect(dsn=self._admin_dsn)
        try:
            await connection.execute(
                "CREATE ROLE l3_chaos_daemon LOGIN PASSWORD 'local-chaos-secret' "
                "IN ROLE l3_evidence_daemon"
            )
        finally:
            await connection.close()
        self._daemon_dsn = _credential_dsn(
            self._admin_dsn,
            "l3_chaos_daemon",
            "local-chaos-secret",
        )

    async def _admin_execute(self, statement: str, *args: object) -> None:
        connection = await asyncpg.connect(dsn=self._admin_dsn)
        try:
            await connection.execute(statement, *args)
        finally:
            await connection.close()

    async def _server_now(self) -> datetime:
        connection = await asyncpg.connect(dsn=self._admin_dsn)
        try:
            return await connection.fetchval("SELECT clock_timestamp()")
        finally:
            await connection.close()

    async def _seed_market_inputs(
        self,
        *,
        observed_at: datetime,
        stale_markets: frozenset[int] = frozenset(),
    ) -> None:
        connection = await asyncpg.connect(dsn=self._admin_dsn)
        try:
            await connection.executemany(
                "INSERT INTO markets_latest (market_id, yes_token_id, no_token_id) "
                "VALUES ($1,$2,$3)",
                [
                    (row["market_id"], row["yes_token_id"], row["no_token_id"])
                    for row in _mapping_rows()
                ],
            )
            books: list[tuple[str, datetime]] = []
            tops: list[tuple[str, datetime]] = []
            for index, row in enumerate(_mapping_rows()):
                at = observed_at - timedelta(
                    seconds=123 if index in stale_markets else 5
                )
                books.extend(((row["yes_token_id"], at), (row["no_token_id"], at)))
                tops.append((row["yes_token_id"], at))
            await connection.executemany(
                "INSERT INTO l2_book_levels "
                "(asset_id, ts, side, level, price, size) "
                "VALUES ($1,$2,'BUY',1,0.5,1)",
                books,
            )
            await connection.executemany(
                "INSERT INTO l2_top_of_book (asset_id, ts, mid_price) "
                "VALUES ($1,$2,0.5)",
                tops,
            )
        finally:
            await connection.close()

    async def _new_chain(
        self,
        *,
        started_at: datetime,
        stale_markets: frozenset[int] = frozenset(),
    ) -> _Chain:
        now = await self._server_now()
        await self._seed_market_inputs(observed_at=now, stale_markets=stale_markets)
        acceptance = _acceptance()
        identity = RuntimeIdentity(
            machine_id="local-machine",
            machine_version="local-version",
            image_ref=IMAGE_REF,
            release_id="local-release",
            code_version=CODE_VERSION,
            recipe_sha256=RECIPE_HASH,
            acceptance_config_hash=acceptance.digest(),
        )
        runtime = L3EvidenceRuntime(identity, started_at=started_at)
        store = L3EvidenceStore(self._daemon_dsn)
        status = runtime.snapshot()
        boot = RuntimeBootRecord(
            boot_id=status.boot_id,
            started_at=status.started_at,
            machine_id=identity.machine_id,
            machine_version=identity.machine_version,
            image_ref=identity.image_ref,
            release_id=identity.release_id,
            code_version=identity.code_version,
            acceptance_config_hash=identity.acceptance_config_hash,
        )
        if not await store.append_boot(boot):
            raise ProofFailure("real evidence store rejected the local boot")
        runtime.note_writer_result(True, datetime.now(UTC), "ok", channel="boot")

        consumer = WsConsumer(
            settings=SimpleNamespace(),
            watchdog=ws_watchdog.WsWatchdog(stale_s=30.0),
            on_event=lambda _event: None,
            initial_assets=[],
            membership_observer=runtime.update_membership,
            event_recorder=runtime.record_event,
        )
        tokens = _tokens()
        consumer.set_l3_desired(tokens)
        websocket = _LocalWs()
        await consumer._initialize_connection(websocket)

        states = await store.fetch_sampling_market_state(sorted(tokens))
        source_times = {
            token_id: book_at
            for state in states
            for token_id, book_at in (
                (state.yes_token_id, state.yes_book_at),
                (state.no_token_id, state.no_book_at),
            )
        }
        for token in tokens:
            evidence_at = source_times[token]
            if evidence_at is None:
                raise ProofFailure("seeded book evidence is missing")
            consumer.record_book_evidence(
                asset_id=token,
                generation=consumer.l3_membership_snapshot().generation,
                book_levels_succeeded=True,
                observed_at=evidence_at,
            )

        mapping_hash = stable_sha256(_mapping_rows())
        promote = PromoteRunRecord(
            boot_id=status.boot_id,
            run_seq=0,
            scheduled_at=started_at,
            started_at=started_at + timedelta(seconds=1),
            finished_at=started_at + timedelta(seconds=2),
            status=PromoteStatus.SUCCESS,
            reason_code="ok",
            selected_count=5,
            desired_count=10,
            committed_count=10,
            evidenced_count=10,
            add_count=0,
            remove_count=0,
            mapping_hash=mapping_hash,
            desired_hash=stable_sha256(sorted(tokens)),
            committed_hash=stable_sha256(sorted(tokens)),
            acceptance_config_hash=acceptance.digest(),
            ws_generation=consumer.l3_membership_snapshot().generation,
            add_succeeded=None,
            remove_succeeded=None,
            mirror_succeeded=True,
            duration_ms=2_000,
        )
        if not await store.append_promote_run(promote):
            raise ProofFailure("real evidence store rejected the local promoter row")
        runtime.note_writer_result(True, datetime.now(UTC), "ok", channel="promoter")
        runtime.mark_promote_persisted(promote.finished_at)
        return _Chain(runtime, store, consumer, websocket, started_at, mapping_hash)

    @staticmethod
    def _sampler_settings() -> SimpleNamespace:
        return SimpleNamespace(
            l3_evidence_sample_interval_s=30,
            l3_market_book_fresh_s=120,
            l3_market_ohlc_fresh_s=120,
        )

    @staticmethod
    def _reconciliation(now: datetime) -> SimpleNamespace:
        return SimpleNamespace(
            is_connected=True,
            reconnect_count=0,
            cursor_lag=0,
            last_reconciliation_success_s=now.timestamp(),
        )

    async def _sample(self, chain: _Chain, *, scheduled_at: datetime, sample_seq: int) -> bool:
        now = datetime.now(UTC)
        return await l3_sampler.sample_once(
            scheduled_at=scheduled_at,
            sample_seq=sample_seq,
            settings=self._sampler_settings(),
            ws_consumer=chain.consumer,
            reconciliation_state=self._reconciliation(now),
            runtime=chain.runtime,
            store=chain.store,
        )

    async def _baseline(self, *, stale_markets: frozenset[int] = frozenset()) -> _Chain:
        started_at = await self._server_now() - timedelta(seconds=5)
        return await self._new_chain(started_at=started_at, stale_markets=stale_markets)

    async def _require_baseline_sample(self, chain: _Chain, failure: str) -> None:
        if not await self._sample(chain, scheduled_at=chain.started_at, sample_seq=0):
            raise ProofFailure(failure)

    @staticmethod
    def _strict(chain: _Chain) -> tuple[int, dict[str, object]]:
        settings = SimpleNamespace(
            scan_shared_secret=SecretStr("local-chaos-secret"),
            version="local",
            release_id="local",
            supabase_url="",
        )
        app = create_l2_app(
            sqlite_store=SimpleNamespace(),
            settings=settings,
            ws_consumer=SimpleNamespace(
                current_state="CONNECTED",
                last_event_at_s=datetime.now(UTC).timestamp(),
                subscribed_assets=sorted(_tokens()),
            ),
            evidence_runtime=chain.runtime,
        )
        with TestClient(app) as client:
            response = client.get("/health")
            probe = client.get("/healthz")
        if probe.status_code != 200:
            raise ProofFailure("local /healthz did not remain routable")
        return response.status_code, response.json()

    @staticmethod
    async def _persist_pending_events(chain: _Chain) -> None:
        while (event := chain.runtime.peek_pending_event()) is not None:
            if not await chain.store.append_event(event):
                raise ProofFailure("causal runtime event was not durable")
            chain.runtime.acknowledge_pending_event(event)

    async def _verdict(self, chain: _Chain):
        sampled_at = chain.runtime.snapshot().last_sample_persisted_at
        end = datetime.now(UTC) + timedelta(seconds=1)
        if sampled_at is not None:
            end = max(end, sampled_at + timedelta(seconds=1))
        evidence = await chain.store.fetch_window(chain.started_at, end)
        return build_soak_report(
            evidence,
            _manifest(chain),
            chain.started_at,
            end,
            False,
        )

    async def _run_sampler(self) -> None:
        # Model a paused loop without sleeping: the boot is 76s old, elapsed
        # T0/+30 slots are not backfilled, and the real sampler writes only the
        # current +60 slot.  Its actual sampled_at remains the gap clock.
        started_at = await self._server_now() - timedelta(seconds=76)
        chain = await self._new_chain(started_at=started_at)
        if not await self._sample(
            chain,
            scheduled_at=started_at + timedelta(seconds=60),
            sample_seq=0,
        ):
            raise ProofFailure("real current-slot sampler append failed")
        report = await self._verdict(chain)
        codes = _codes(report)
        if (
            report.status is not VerdictStatus.NOT_CLOSED
            or "sample_gap" not in codes
            or "sample_schedule_grid" not in codes
            or report.max_sample_gap_seconds is None
            or report.max_sample_gap_seconds <= 75
        ):
            raise ProofFailure("paused sampler did not preserve its exact gap cause")
        print(
            "NOT-CLOSED gap_seconds>75 "
            f"actual_gap={report.max_sample_gap_seconds:.3f} "
            "reasons=sample_gap,sample_schedule_grid"
        )

    async def _run_writer(self) -> None:
        chain = await self._baseline()
        await self._admin_execute(
            "CREATE FUNCTION reject_l3_chaos_sample() RETURNS trigger LANGUAGE plpgsql "
            "AS $$ BEGIN RAISE EXCEPTION 'local sample rejection'; END $$"
        )
        await self._admin_execute(
            "CREATE TRIGGER reject_l3_chaos_sample BEFORE INSERT ON l3_health_samples "
            "FOR EACH ROW EXECUTE FUNCTION reject_l3_chaos_sample()"
        )
        if await self._sample(chain, scheduled_at=chain.started_at, sample_seq=0):
            raise ProofFailure("rejected evidence write unexpectedly succeeded")
        await self._persist_pending_events(chain)
        end = datetime.now(UTC) + timedelta(seconds=1)
        evidence = await chain.store.fetch_window(chain.started_at, end)
        if not any(
            event.kind is RuntimeEventKind.EVIDENCE_WRITER_FAILED
            for event in evidence.runtime_events
        ):
            raise ProofFailure("durable evidence_writer_failed event is missing")
        status, _body = self._strict(chain)
        if status != 503 or chain.runtime.snapshot().reason_code != "evidence_writer_failed":
            raise ProofFailure("writer rejection did not fail strict health")
        print("STRICT-FAIL evidence_writer_failed durable_event=present")

    async def _run_ws_false(self) -> None:
        chain = await self._baseline()
        await self._require_baseline_sample(chain, "WS-control baseline sample failed")
        removed = sorted(_tokens())[0]
        chain.consumer.set_l3_desired(_tokens() - {removed})
        chain.websocket.reject_control = True
        if await chain.consumer.remove_subscriptions([removed]):
            raise ProofFailure("WS false control unexpectedly committed")
        if chain.runtime.snapshot().desired == chain.runtime.snapshot().committed:
            raise ProofFailure("WS false control did not preserve membership mismatch")
        await self._persist_pending_events(chain)
        end = datetime.now(UTC) + timedelta(seconds=1)
        evidence = await chain.store.fetch_window(chain.started_at, end)
        if not any(
            event.kind is RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED
            for event in evidence.runtime_events
        ):
            raise ProofFailure("subscription_control_failed event is missing")
        status, body = self._strict(chain)
        check = body["checks"]["l3:membership_convergence"][0]  # type: ignore[index]
        if status != 503 or check["status"] != "fail" or check["observedValue"] != "mismatch":
            raise ProofFailure("WS false control did not fail the membership health chain")
        print("STRICT-FAIL l3:membership_convergence runtime_event=subscription_control_failed")

    async def _run_one_hot(self) -> None:
        chain = await self._baseline(stale_markets=frozenset({1, 2, 3, 4}))
        await self._require_baseline_sample(chain, "one-hot sample did not persist")
        rows = chain.runtime.snapshot().last_market_samples
        if rows[0].status is not HealthStatus.PASS or any(
            row.status is not HealthStatus.FAIL for row in rows[1:]
        ):
            raise ProofFailure("per-market one-hot causality was not preserved")
        status, body = self._strict(chain)
        check = body["checks"]["l3:worst_market_freshness"][0]  # type: ignore[index]
        if status != 503 or check["status"] != "fail" or check["observedValue"] < 120:
            raise ProofFailure("four silent markets did not fail worst-market health")
        print("STRICT-FAIL l3:worst_market_freshness hot=1 silent=4")

    async def _run_restart(self) -> None:
        chain = await self._baseline()
        await self._require_baseline_sample(chain, "restart baseline sample failed")
        second_started_at = await self._server_now()
        second_runtime = L3EvidenceRuntime(
            chain.runtime.snapshot().identity,
            started_at=second_started_at,
        )
        second_boot = RuntimeBootRecord(
            boot_id=second_runtime.snapshot().boot_id,
            started_at=second_started_at,
            machine_id="local-machine",
            machine_version="local-version",
            image_ref=IMAGE_REF,
            release_id="local-release",
            code_version=CODE_VERSION,
            acceptance_config_hash=_acceptance().digest(),
        )
        if not await chain.store.append_boot(second_boot):
            raise ProofFailure("second_boot was not durably created")
        report = await self._verdict(chain)
        if report.status is not VerdictStatus.NOT_CLOSED or "boot_cardinality" not in _codes(
            report
        ):
            raise ProofFailure("second_boot did not fail exact-window boot cardinality")
        print("NOT-CLOSED second_boot reason=boot_cardinality")

    async def run(self, mode: str) -> None:
        runners = {
            "sampler": self._run_sampler,
            "writer": self._run_writer,
            "ws-false": self._run_ws_false,
            "one-hot": self._run_one_hot,
            "restart": self._run_restart,
        }
        await runners[mode]()


def _run_harness(mode: str, harness: LocalEvidenceHarness) -> int:
    primary_error: Exception | None = None
    cleanup_error: Exception | None = None
    try:
        harness.start()
        asyncio.run(harness.run(mode))
    except Exception as error:  # bounded CLI: never print external exception messages
        primary_error = error
    finally:
        cleanup_error = harness.stop()

    if primary_error is not None:
        print(f"FAIL {mode}: {type(primary_error).__name__}", file=sys.stderr)
        if cleanup_error is not None:
            print(
                f"cleanup_error_type={type(cleanup_error).__name__}",
                file=sys.stderr,
            )
        return 1
    if cleanup_error is not None:
        print(
            f"FAIL {mode}: cleanup_error_type={type(cleanup_error).__name__}",
            file=sys.stderr,
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=MODES)
    args = parser.parse_args()
    return _run_harness(args.mode, LocalEvidenceHarness())


if __name__ == "__main__":
    raise SystemExit(main())
