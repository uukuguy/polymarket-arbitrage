from __future__ import annotations

import asyncio
import json
import os
import sys
import time

import pytest

from polyarb.http.health import read_producer_liveness_health
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.models import GroupLeg, GroupRevision
from polyarb.perception.store import OpportunityPerceptionStore, ProducerReceipt
from polyarb.perception.supervisor import (
    PRODUCER_COMMANDS,
    ProducerSpec,
    ProducerSupervisor,
)
from polyarb.perception.worker_cli import run_component


def _supervisor(tmp_path, *, component=None, command=None):
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    overrides = None
    if component is not None and command is not None:
        overrides = {**PRODUCER_COMMANDS, component: command}
    return store, ProducerSupervisor(
        store=store,
        incidents=IncidentManager(store),
        _test_commands=overrides,
    )


def test_exact_commands_are_shell_free() -> None:
    assert PRODUCER_COMMANDS["candidate"] == (
        sys.executable,
        "-m",
        "polyarb.perception.worker_cli",
        "candidate",
    )
    assert all(isinstance(command, tuple) for command in PRODUCER_COMMANDS.values())


@pytest.mark.asyncio
async def test_worker_cli_rejects_disabled_component_without_touching_network() -> None:
    class Settings:
        opportunity_producer_supervisor_enabled = False
        opportunity_first_watcher_enabled = True

    with pytest.raises(RuntimeError, match="producer-supervisor-disabled"):
        await run_component("candidate", Settings())


@pytest.mark.asyncio
async def test_nonzero_child_retries_then_escalates_without_hot_loop(tmp_path) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(sys.executable, "-c", "raise SystemExit(7)"),
    )
    spec = ProducerSpec(
        component="candidate",
        timeout_s=2,
        max_restarts=1,
        backoff_initial_s=0.01,
    )
    await supervisor.run(spec, asyncio.Event())
    receipts = store.producer_receipts("candidate")
    assert [receipt.outcome for receipt in receipts] == ["nonzero", "nonzero"]
    escalated = store.open_incidents()[0]
    assert escalated.state == "escalated"
    assert escalated.evidence["action"] == "operator-intervention"
    assert escalated.evidence["retry_count"] == 1
    with store._connect() as con:
        recovery = con.execute(
            "SELECT evidence_json FROM neg_risk_incident_events "
            "WHERE incident_id=? AND state='recovering'",
            (escalated.id,),
        ).fetchone()
    recovery_evidence = json.loads(recovery["evidence_json"])
    assert recovery_evidence["action"] == "retry-producer"
    assert recovery_evidence["retry_count"] == 1
    assert type(recovery_evidence["next_retry_at_ms"]) is int


@pytest.mark.asyncio
async def test_unexpected_zero_exit_restarts_then_escalates(tmp_path) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(sys.executable, "-c", "raise SystemExit(0)"),
    )
    await supervisor.run(
        ProducerSpec(
            component="candidate",
            timeout_s=2,
            max_restarts=1,
            backoff_initial_s=0.01,
        ),
        asyncio.Event(),
    )
    assert [receipt.outcome for receipt in store.producer_receipts("candidate")] == [
        "success",
        "success",
    ]
    incident = store.open_incidents()[0]
    assert incident.kind == "child-unexpected-exit"
    assert incident.state == "escalated"
    liveness = read_producer_liveness_health(
        store.db_path,
        "candidate",
        now_ms=store.producer_receipts("candidate")[-1].finished_at_ms + 1,
        stall_timeout_ms=2_000,
    )
    assert liveness.state == "unexpected-exit"
    assert liveness.evidence_consistent is True


@pytest.mark.asyncio
async def test_progress_read_flapping_never_extends_stall_deadline(
    tmp_path,
    monkeypatch,
) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(sys.executable, "-c", "import time; time.sleep(0.39)"),
    )
    reads = 0

    def flapping_marker(component, supervisor_run_id, attempt):
        nonlocal reads
        reads += 1
        return (0, 0, 0) if reads % 2 else ("unavailable",)

    monkeypatch.setattr(supervisor, "_progress_marker", flapping_marker)
    started = time.monotonic()
    await supervisor.run(
        ProducerSpec(
            component="candidate",
            timeout_s=0.08,
            terminate_grace_s=0.05,
            max_restarts=0,
        ),
        asyncio.Event(),
    )

    assert store.producer_receipts("candidate")[0].outcome == "timeout"
    assert time.monotonic() - started < 0.25


@pytest.mark.asyncio
async def test_health_binds_liveness_to_latest_exact_child(tmp_path) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(
            sys.executable,
            "-c",
            (
                "import time\n"
                "from polyarb.perception.store import OpportunityPerceptionStore\n"
                f"s=OpportunityPerceptionStore({str(tmp_path / 'state.db')!r})\n"
                "s.claim_producer_heartbeat_authority('candidate')\n"
                "now=int(time.time()*1000)\n"
                "s.record_producer_heartbeat('candidate',observed_at_ms=now)\n"
                "time.sleep(5)\n"
            ),
        ),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(
        supervisor.run(
            ProducerSpec(component="candidate", timeout_s=2),
            stop,
        )
    )
    await asyncio.sleep(1.2)
    latest = store.latest_producer_heartbeat_ms("candidate")
    assert latest is not None
    liveness = read_producer_liveness_health(
        store.db_path,
        "candidate",
        now_ms=latest + 1,
        stall_timeout_ms=2_000,
    )
    assert liveness.state == "running"
    assert liveness.evidence_consistent is True
    stop.set()
    await task


@pytest.mark.asyncio
async def test_parent_cannot_forge_heartbeat_from_database_hash(tmp_path) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(
            sys.executable,
            "-c",
            (
                "import time\n"
                "from polyarb.perception.store import OpportunityPerceptionStore\n"
                f"s=OpportunityPerceptionStore({str(tmp_path / 'state.db')!r})\n"
                "s.claim_producer_heartbeat_authority('candidate')\n"
                "s.record_producer_heartbeat("
                "'candidate',observed_at_ms=int(time.time()*1000))\n"
                "time.sleep(5)\n"
            ),
        ),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(
        supervisor.run(ProducerSpec(component="candidate", timeout_s=2), stop)
    )
    await asyncio.sleep(1.0)
    with store._connect() as con:
        row = con.execute(
            "SELECT supervisor_run_id,attempt,child_auth_hash "
            "FROM neg_risk_producer_child_starts WHERE component='candidate'"
        ).fetchone()
    assert row["child_auth_hash"]
    with pytest.raises(PermissionError, match="heartbeat-authority"):
        store.record_producer_heartbeat(
            "candidate",
            observed_at_ms=int(asyncio.get_running_loop().time() * 1_000),
            supervisor_run_id=row["supervisor_run_id"],
            attempt=row["attempt"],
            _preimage=row["child_auth_hash"],
        )
    stop.set()
    await task


def test_liveness_replays_and_rejects_corrupt_old_attempt(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    for attempt in (1, 2):
        store.reserve_producer_attempt(
            "candidate",
            supervisor_run_id=f"run-{attempt}",
            started_at_ms=attempt * 1_000,
        )
        store.record_producer_receipt(
            ProducerReceipt(
                component="candidate",
                attempt=attempt,
                started_at_ms=attempt * 1_000,
                finished_at_ms=attempt * 1_000 + 10,
                outcome="success",
                exit_code=0,
                stdout_tail="",
                stderr_tail="",
                supervisor_run_id=f"run-{attempt}",
                child_auth_hash=None,
            )
        )
    with store._connect() as con:
        con.execute(
            "UPDATE neg_risk_producer_child_starts "
            "SET auth_domain='forged' WHERE attempt=1"
        )

    health = read_producer_liveness_health(
        store.db_path,
        "candidate",
        now_ms=3_000,
        stall_timeout_ms=2_000,
    )
    assert health.state == "unavailable"
    assert health.evidence_consistent is False


def test_liveness_rejects_success_receipt_with_nonzero_exit_code(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.reserve_producer_attempt(
        "candidate",
        supervisor_run_id="run-1",
        started_at_ms=1_000,
    )
    store.record_producer_receipt(
        ProducerReceipt(
            component="candidate",
            attempt=1,
            started_at_ms=1_000,
            finished_at_ms=1_010,
            outcome="success",
            exit_code=0,
            stdout_tail="",
            stderr_tail="",
            supervisor_run_id="run-1",
            child_auth_hash=None,
        )
    )
    with store._connect() as con:
        con.execute(
            "UPDATE neg_risk_producer_receipts SET exit_code=9 "
            "WHERE component='candidate' AND attempt=1"
        )

    health = read_producer_liveness_health(
        store.db_path,
        "candidate",
        now_ms=2_000,
        stall_timeout_ms=2_000,
    )
    assert health.state == "unavailable"
    assert health.evidence_consistent is False


@pytest.mark.parametrize(
    "tamper_sql",
    (
        "stdout_tail=zeroblob(20000)",
        "stderr_tail=42",
    ),
)
def test_liveness_rejects_non_text_or_oversized_historical_tails(
    tmp_path,
    tamper_sql,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.reserve_producer_attempt(
        "candidate",
        supervisor_run_id="run-1",
        started_at_ms=1_000,
    )
    store.record_producer_receipt(
        ProducerReceipt(
            component="candidate",
            attempt=1,
            started_at_ms=1_000,
            finished_at_ms=1_010,
            outcome="success",
            exit_code=0,
            stdout_tail="",
            stderr_tail="",
            supervisor_run_id="run-1",
            child_auth_hash=None,
        )
    )
    healthy = read_producer_liveness_health(
        store.db_path,
        "candidate",
        now_ms=2_000,
        stall_timeout_ms=2_000,
    )
    assert healthy.evidence_consistent is True
    with store._connect() as con:
        con.execute(
            f"UPDATE neg_risk_producer_receipts SET {tamper_sql} "
            "WHERE component='candidate' AND attempt=1"
        )

    corrupt = read_producer_liveness_health(
        store.db_path,
        "candidate",
        now_ms=2_000,
        stall_timeout_ms=2_000,
    )
    assert corrupt.state == "unavailable"
    assert corrupt.evidence_consistent is False


@pytest.mark.parametrize("tail", (b"", 42, None, "€" * 5_462))
def test_receipt_writer_rejects_non_text_or_oversized_tails(tmp_path, tail) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.reserve_producer_attempt(
        "candidate",
        supervisor_run_id="run-1",
        started_at_ms=1_000,
    )

    with pytest.raises(ValueError, match="invalid-producer-receipt"):
        store.record_producer_receipt(
            ProducerReceipt(
                component="candidate",
                attempt=1,
                started_at_ms=1_000,
                finished_at_ms=1_010,
                outcome="success",
                exit_code=0,
                stdout_tail=tail,
                stderr_tail="",
                supervisor_run_id="run-1",
                child_auth_hash=None,
            )
        )


def test_output_hash_migration_backfills_legacy_receipt_and_is_idempotent(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.reserve_producer_attempt(
        "candidate",
        supervisor_run_id="run-1",
        started_at_ms=1_000,
    )
    store.record_producer_receipt(
        ProducerReceipt(
            component="candidate",
            attempt=1,
            started_at_ms=1_000,
            finished_at_ms=1_010,
            outcome="success",
            exit_code=0,
            stdout_tail="",
            stderr_tail="",
            supervisor_run_id="run-1",
            child_auth_hash=None,
        )
    )
    with store._connect() as con:
        con.execute("ALTER TABLE neg_risk_producer_receipts DROP COLUMN output_hash")

    store.init_schema()
    store.init_schema()

    with store._connect() as con:
        output_hash = con.execute(
            "SELECT output_hash FROM neg_risk_producer_receipts "
            "WHERE component='candidate' AND attempt=1"
        ).fetchone()[0]
    assert isinstance(output_hash, str)
    assert len(output_hash) == 64
    health = read_producer_liveness_health(
        store.db_path,
        "candidate",
        now_ms=2_000,
        stall_timeout_ms=2_000,
    )
    assert health.evidence_consistent is True
    supervisor = ProducerSupervisor(store=store, incidents=IncidentManager(store))
    assert supervisor._progress_marker("candidate", "run-1", 1) == (0, 0, 0)


def test_output_hash_migration_rejects_invalid_legacy_tail_without_partial_write(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    for attempt in (1, 2):
        store.reserve_producer_attempt(
            "candidate",
            supervisor_run_id=f"run-{attempt}",
            started_at_ms=attempt * 1_000,
        )
        store.record_producer_receipt(
            ProducerReceipt(
                component="candidate",
                attempt=attempt,
                started_at_ms=attempt * 1_000,
                finished_at_ms=attempt * 1_000 + 10,
                outcome="success",
                exit_code=0,
                stdout_tail="",
                stderr_tail="",
                supervisor_run_id=f"run-{attempt}",
                child_auth_hash=None,
            )
        )
    with store._connect() as con:
        con.execute("ALTER TABLE neg_risk_producer_receipts DROP COLUMN output_hash")
        con.execute(
            "UPDATE neg_risk_producer_receipts SET stdout_tail=zeroblob(20000) "
            "WHERE attempt=2"
        )

    with pytest.raises(ValueError, match="invalid-producer-receipt-output-migration"):
        store.init_schema()

    with store._connect() as con:
        columns = {
            row["name"]
            for row in con.execute("PRAGMA table_info(neg_risk_producer_receipts)")
        }
        rows = con.execute(
            "SELECT attempt,stdout_tail FROM neg_risk_producer_receipts ORDER BY attempt"
        ).fetchall()
    assert "output_hash" not in columns
    assert rows[0]["stdout_tail"] == ""
    assert isinstance(rows[1]["stdout_tail"], bytes)


@pytest.mark.parametrize(
    ("outcome", "exit_code", "consistent"),
    (
        ("success", 0, True),
        ("nonzero", 9, True),
        ("timeout", None, True),
        ("cancelled", None, True),
        ("spawn-error", None, True),
        ("success", 9, False),
        ("nonzero", 0, False),
        ("nonzero", None, False),
        ("timeout", 0, False),
        ("cancelled", 0, False),
        ("spawn-error", 0, False),
    ),
)
def test_liveness_enforces_exact_outcome_exit_code_matrix(
    tmp_path,
    outcome,
    exit_code,
    consistent,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.reserve_producer_attempt(
        "candidate",
        supervisor_run_id="run-1",
        started_at_ms=1_000,
    )
    store.record_producer_receipt(
        ProducerReceipt(
            component="candidate",
            attempt=1,
            started_at_ms=1_000,
            finished_at_ms=1_010,
            outcome=outcome,
            exit_code=exit_code,
            stdout_tail="",
            stderr_tail="",
            supervisor_run_id="run-1",
            child_auth_hash=None,
        )
    )

    health = read_producer_liveness_health(
        store.db_path,
        "candidate",
        now_ms=2_000,
        stall_timeout_ms=2_000,
    )
    assert health.evidence_consistent is consistent
    assert (health.state != "unavailable") is consistent


@pytest.mark.asyncio
async def test_restart_converges_abandoned_reservation_before_new_child(tmp_path) -> None:
    marker = tmp_path / "child.pid"
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    assert (
        store.reserve_producer_attempt(
            "candidate",
            supervisor_run_id="crashed-supervisor",
            started_at_ms=1,
        )
        == 1
    )
    script = (
        "import os,time\n"
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(10)\n"
    )
    supervisor = ProducerSupervisor(
        store=store,
        incidents=IncidentManager(store),
        _test_commands={
            **PRODUCER_COMMANDS,
            "candidate": (sys.executable, "-c", script),
        },
    )
    stop = asyncio.Event()

    async def cancel_child() -> None:
        while not marker.exists():
            await asyncio.sleep(0.01)
        stop.set()

    await asyncio.gather(
        supervisor.run(
            ProducerSpec(
                component="candidate",
                timeout_s=2,
                terminate_grace_s=0.05,
            ),
            stop,
        ),
        cancel_child(),
    )

    receipts = store.producer_receipts("candidate")
    assert [(item.attempt, item.outcome) for item in receipts] == [
        (1, "spawn-error"),
        (2, "cancelled"),
    ]
    assert "abandoned-reservation" in receipts[0].stderr_tail
    pid = int(marker.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.asyncio
async def test_timeout_terminates_child_and_records_terminal_receipt(tmp_path) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="discovery",
        command=(sys.executable, "-c", "import time; time.sleep(10)"),
    )
    spec = ProducerSpec(
        component="discovery",
        timeout_s=0.05,
        terminate_grace_s=0.05,
        max_restarts=0,
    )
    await supervisor.run(spec, asyncio.Event())
    receipt = store.producer_receipts("discovery")[0]
    assert receipt.outcome == "timeout"
    assert receipt.finished_at_ms >= receipt.started_at_ms


@pytest.mark.asyncio
async def test_child_output_is_bounded_and_common_secret_forms_are_redacted(tmp_path) -> None:
    script = "import sys; print('x'*20000); print('token=abc123', file=sys.stderr); sys.exit(3)"
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(sys.executable, "-c", script),
    )
    await supervisor.run(
        ProducerSpec(
            component="candidate",
            timeout_s=2,
            max_restarts=0,
            output_limit_bytes=128,
        ),
        asyncio.Event(),
    )
    receipt = store.producer_receipts("candidate")[0]
    assert len(receipt.stdout_tail.encode()) <= 128
    assert "abc123" not in receipt.stderr_tail
    assert "[REDACTED]" in receipt.stderr_tail


@pytest.mark.asyncio
async def test_redaction_covers_uri_json_headers_cookies_and_percent_encoding(
    tmp_path,
) -> None:
    secrets = (
        "postgres://alice:hunter2@db/x?api_key=query-secret",
        '{"ToKeN":"json-secret","password":"p4ss"}',
        "Authorization: Bearer bearer-secret",
        "Authorization: Basic basic-secret",
        "Cookie: session=cookie-secret",
        "token%3Dpercent-secret",
    )
    script = "print(" + repr("\n".join(secrets)) + "); raise SystemExit(3)"
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(sys.executable, "-c", script),
    )
    await supervisor.run(
        ProducerSpec(component="candidate", timeout_s=2, max_restarts=0),
        asyncio.Event(),
    )
    output = store.producer_receipts("candidate")[0].stdout_tail
    for secret in (
        "hunter2",
        "query-secret",
        "json-secret",
        "p4ss",
        "bearer-secret",
        "basic-secret",
        "cookie-secret",
        "percent-secret",
    ):
        assert secret not in output


@pytest.mark.asyncio
async def test_redaction_covers_prefixed_and_suffixed_project_secret_keys(tmp_path) -> None:
    secrets = (
        "POLYARB_SUPABASE_SERVICE_KEY=service-value",
        "POLYARB_TELEGRAM_BOT_TOKEN=telegram-value",
        '{"R2_SECRET_ACCESS_KEY":"r2-value"}',
        "https://example.test/?OPENAI_API_KEY=openai-value",
    )
    script = "print(" + repr("\n".join(secrets)) + "); raise SystemExit(3)"
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(sys.executable, "-c", script),
    )
    await supervisor.run(
        ProducerSpec(component="candidate", timeout_s=2, max_restarts=0),
        asyncio.Event(),
    )
    output = store.producer_receipts("candidate")[0].stdout_tail
    for secret in ("service-value", "telegram-value", "r2-value", "openai-value"):
        assert secret not in output


@pytest.mark.asyncio
async def test_invalid_utf8_still_records_terminal_receipt_and_incident(tmp_path) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(
            sys.executable,
            "-c",
            "import os; os.write(1,b'\\xff'*16384); raise SystemExit(9)",
        ),
    )
    await supervisor.run(
        ProducerSpec(component="candidate", timeout_s=2, max_restarts=0),
        asyncio.Event(),
    )
    assert store.producer_receipts("candidate")[0].outcome == "nonzero"
    assert store.open_incidents()[0].state == "escalated"


@pytest.mark.asyncio
async def test_parent_or_old_heartbeat_cannot_keep_wedged_child_alive(tmp_path) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="candidate",
        command=(sys.executable, "-c", "import time; time.sleep(1.5)"),
    )

    async def forge_parent_heartbeats() -> None:
        for sequence in range(7):
            await asyncio.sleep(0.2)
            store.record_producer_heartbeat(
                "candidate",
                observed_at_ms=10_000 + sequence,
            )

    await asyncio.gather(
        supervisor.run(
            ProducerSpec(
                component="candidate",
                timeout_s=1.1,
                terminate_grace_s=0.05,
                max_restarts=0,
            ),
            asyncio.Event(),
        ),
        forge_parent_heartbeats(),
    )
    assert store.producer_receipts("candidate")[0].outcome == "timeout"


@pytest.mark.asyncio
async def test_cancellation_cleans_up_child_without_orphan(tmp_path) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="reconciliation",
        command=(sys.executable, "-c", "import time; time.sleep(10)"),
    )
    spec = ProducerSpec(
        component="reconciliation",
        timeout_s=10,
    )
    task = asyncio.create_task(supervisor.run(spec, asyncio.Event()))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.producer_receipts("reconciliation")[-1].outcome == "cancelled"


@pytest.mark.asyncio
async def test_component_failure_is_isolated(tmp_path) -> None:
    store, supervisor = _supervisor(
        tmp_path,
        component="discovery",
        command=(sys.executable, "-c", "raise SystemExit(2)"),
    )
    await supervisor.run(
        ProducerSpec(
            component="discovery",
            timeout_s=1,
            max_restarts=0,
        ),
        asyncio.Event(),
    )
    assert store.producer_state("candidate") == "never-started"
    assert store.producer_state("discovery") == "nonzero"


@pytest.mark.asyncio
async def test_restart_verifies_only_from_post_recovery_candidate_writer(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    revision = GroupRevision.certified(
        group_id="g-1",
        event_id="e-1",
        revision=1,
        started_at_ms=1,
        observed_at_ms=2,
        source_cursor="c",
        legs=(
            GroupLeg("m1", "c1", "t1", "one"),
            GroupLeg("m2", "c2", "t2", "two"),
        ),
    )
    store.publish_group_revision(revision)
    marker = tmp_path / "attempted"
    script = (
        "from pathlib import Path\n"
        "import time\n"
        "from polyarb.perception.store import OpportunityPerceptionStore\n"
        "from polyarb.perception.models import GroupQuoteBatch,GroupQuoteLeg\n"
        f"marker=Path({str(marker)!r})\n"
        "if not marker.exists():\n"
        " marker.touch(); raise SystemExit(7)\n"
        f"s=OpportunityPerceptionStore(Path({str(store.db_path)!r}))\n"
        "s.claim_producer_heartbeat_authority('candidate')\n"
        f"h={revision.membership_hash!r}\n"
        "now=int(time.time()*1000)\n"
        "b=GroupQuoteBatch.complete("
        "group_id='g-1',membership_hash=h,quote_batch_id='recovery-qb',"
        "started_at_ms=now-1,quoted_at_ms=now,"
        "legs=(GroupQuoteLeg('t1',h,.4,10,'executable'),"
        "GroupQuoteLeg('t2',h,.5,10,'executable')))\n"
        "s.publish_candidate_success(b,observed_at_ms=now,last_result='watching',"
        "reason=None,bundle_cost=.9,gross_edge_bps=1000,max_bundle_size=10,"
        "priority_class='high',consecutive_failures=0,effective_interval_s=15,"
        "schedule_reason='test',next_due_at_ms=now+15000)\n"
        "time.sleep(.2)\n"
        "s.record_producer_heartbeat('candidate',observed_at_ms=now+200)\n"
        "time.sleep(5)\n"
    )
    supervisor = ProducerSupervisor(
        store=store,
        incidents=IncidentManager(store),
        _test_commands={
            **PRODUCER_COMMANDS,
            "candidate": (sys.executable, "-c", script),
        },
    )
    stop = asyncio.Event()

    async def stop_after_recovery() -> None:
        # The supervisor samples child progress once per second. Allow the
        # restarted interpreter to publish and one complete sample boundary
        # to verify the durable post-recovery row anchors.
        await asyncio.sleep(2.2)
        stop.set()

    await asyncio.gather(
        supervisor.run(
            ProducerSpec(
                component="candidate",
                timeout_s=2,
                max_restarts=1,
                backoff_initial_s=0.01,
            ),
            stop,
        ),
        stop_after_recovery(),
    )
    assert store.open_incidents() == ()
    assert [r.outcome for r in store.producer_receipts("candidate")] == [
        "nonzero",
        "cancelled",
    ]
