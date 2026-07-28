from __future__ import annotations

import asyncio
import sys

import pytest

from polyarb.http.health import read_producer_liveness_health
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.models import GroupLeg, GroupRevision
from polyarb.perception.store import OpportunityPerceptionStore
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
    assert store.open_incidents()[0].state == "escalated"


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
