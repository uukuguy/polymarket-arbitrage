from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import (
    FaultAuthorization,
    FaultCall,
    FaultCallClass,
    FaultEventState,
    FaultIntent,
    FaultIntentRequest,
    FaultKind,
    FaultOwnershipCapability,
    FaultRecoveryReceipt,
    FaultRecoveryWriter,
    FaultRuntimeIdentity,
    fault_call_binding_digest,
)
from polyarb.perception.fault_runtime import (
    CleanupResult,
    FaultRecoveryOutcome,
    FaultRuntime,
    PassThroughFaultRuntime,
    build_fault_runtime,
)
from polyarb.perception.store import (
    DiscoveryAdmissionProof,
    OpportunityPerceptionStore,
    reconciliation_authority_checkpoint_hash,
)
from polyarb.perception.worker_cli import _build_child_fault_runtime

IDENTITY = FaultRuntimeIdentity(
    component="candidate",
    release_id="a" * 40,
    machine_id="machine-1",
    boot_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
)


def _intent(*, ownership=None) -> FaultIntent:
    return FaultIntent(
        fault_id="fault-1",
        kind=FaultKind.CLOB_429,
        call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
        target_key="group-1",
        parameters={},
        ttl_ms=30_000,
        runtime=IDENTITY,
        nonce_digest="b" * 64,
        accepted_at_ms=1_000,
        ownership_capability=ownership,
    )


class _Authority:
    def __init__(self, intent: FaultIntent | None = None) -> None:
        self.intent = intent
        self.claims: list[tuple[FaultRuntimeIdentity, int]] = []
        self.events: list[tuple] = []

    def claim_pending(self, identity, *, claimed_at_ms):
        self.claims.append((identity, claimed_at_ms))
        value, self.intent = self.intent, None
        return value

    def append_event(self, fault_id, state, **kwargs):
        self.events.append((fault_id, state, kwargs))

    def relinquish_claim(self, fault_id, **kwargs):
        state = (
            FaultEventState.CLEANED
            if self.events and self.events[-1][1] is FaultEventState.CONTAINED
            else FaultEventState.ABANDONED
        )
        self.events.append((fault_id, state, kwargs))
        return SimpleNamespace(state=state)

    def append_recovery_event(self, receipt, **kwargs):
        self.events.append(
            (
                receipt.fault_id,
                FaultEventState.RECOVERED,
                {**kwargs, "evidence": {"recovery_id": str(receipt.writer_id)}},
            )
        )
        return SimpleNamespace(state=FaultEventState.RECOVERED)


@pytest.mark.asyncio
async def test_sync_claims_once_and_hot_path_only_consumes_memory() -> None:
    authority = _Authority(_intent())
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=authority,
        clock_ms=lambda: 1_100,
        monotonic=lambda: 10.0,
    )

    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    )

    assert decision.inject is True
    assert authority.claims == [(IDENTITY, 1_100)]


@pytest.mark.asyncio
async def test_store_claim_failure_is_redacted_pass_through(caplog) -> None:
    class BrokenAuthority(_Authority):
        def claim_pending(self, identity, *, claimed_at_ms):
            raise RuntimeError("Authorization: Bearer secret-value")

    upstream_called = False
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=BrokenAuthority(),
        clock_ms=lambda: 1_100,
    )

    await runtime.sync_before_batch()
    upstream_called = True

    assert upstream_called is True
    assert runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    ).inject is False
    assert "secret-value" not in caplog.text


@pytest.mark.asyncio
async def test_injection_and_incident_links_are_process_owned_and_ordered() -> None:
    ownership = FaultOwnershipCapability(
        fault_id="fault-1",
        runtime=IDENTITY,
        token="f" * 64,
    )
    authority = _Authority(_intent(ownership=ownership))
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=authority,
        clock_ms=iter((1_100, 1_101, 1_102, 1_103, 1_104)).__next__,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    )

    injection = await runtime.record_injection(decision.fault_id)
    linked = await runtime.link_detection(
        decision.fault_id,
        kind=FaultKind.CLOB_429,
        detection_id="incident-1",
    )

    assert injection is not None
    assert linked is True
    assert [event[1] for event in authority.events] == [
        FaultEventState.INJECTED,
        FaultEventState.DETECTED,
        FaultEventState.CONTAINED,
    ]
    assert all(
        event[2]["ownership"] is ownership
        for event in (authority.events[0], authority.events[2])
    )
    assert authority.events[1][2].get("ownership") is None


@pytest.mark.asyncio
async def test_injection_receipt_failure_freezes_future_hot_path() -> None:
    class BrokenAuthority(_Authority):
        def append_event(self, fault_id, state, **kwargs):
            raise RuntimeError("corrupt-authority")

    ownership = FaultOwnershipCapability(
        fault_id="fault-1",
        runtime=IDENTITY,
        token="f" * 64,
    )
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=BrokenAuthority(_intent(ownership=ownership)),
        clock_ms=lambda: 1_100,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    )

    assert await runtime.record_injection(decision.fault_id) is None
    assert runtime.degraded is True
    assert runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    ).inject is False


@pytest.mark.asyncio
async def test_containment_write_failure_freezes_and_cannot_create_recovery() -> None:
    class ContainmentBrokenAuthority(_Authority):
        def append_event(self, fault_id, state, **kwargs):
            if state is FaultEventState.CONTAINED:
                raise RuntimeError("containment-write-failed")
            super().append_event(fault_id, state, **kwargs)

    ownership = FaultOwnershipCapability(
        fault_id="fault-1",
        runtime=IDENTITY,
        token="f" * 64,
    )
    authority = ContainmentBrokenAuthority(_intent(ownership=ownership))
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=authority,
        clock_ms=iter(
            (1_100, 1_101, 1_102, 1_103, 1_104, 1_105)
        ).__next__,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    )
    await runtime.record_injection(decision.fault_id)

    assert (
        await runtime.link_detection(
            decision.fault_id,
            kind=FaultKind.CLOB_429,
            detection_id="incident-1",
        )
        is False
    )
    assert runtime.degraded is True
    cleanup = await runtime.cleanup(decision.fault_id, "containment-failed")

    assert cleanup.terminal_state is FaultEventState.ABANDONED
    assert runtime.pending_recovery_fault_id is None
    assert (
        runtime.make_recovery_receipt(
            FaultRecoveryWriter.DISCOVERY_BATCH,
            writer_id=1,
            writer_occurred_at_ms=1_200,
        )
        is None
    )
    assert [event[1] for event in authority.events] == [
        FaultEventState.INJECTED,
        FaultEventState.DETECTED,
        FaultEventState.ABANDONED,
    ]


@pytest.mark.asyncio
async def test_pass_through_runtime_receipts_are_noop() -> None:
    runtime = PassThroughFaultRuntime(degraded=True)

    assert await runtime.record_injection("fault-1") is None
    assert (
        await runtime.link_detection(
            "fault-1",
            kind=FaultKind.GAMMA_TIMEOUT,
            detection_id="incident-1",
        )
        is False
    )
    assert runtime.pending_recovery_fault_id is None
    assert (
        runtime.make_recovery_receipt(
            FaultRecoveryWriter.DISCOVERY_BATCH,
            writer_id=1,
            writer_occurred_at_ms=1_200,
        )
        is None
    )


@pytest.mark.asyncio
async def test_cleaned_fault_retains_owning_capability_for_exact_recovery() -> None:
    ownership = FaultOwnershipCapability(
        fault_id="fault-1",
        runtime=IDENTITY,
        token="f" * 64,
    )
    authority = _Authority(_intent(ownership=ownership))
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=authority,
        clock_ms=iter(
            (1_100, 1_101, 1_102, 1_103, 1_104, 1_105, 1_106)
        ).__next__,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    )
    await runtime.record_injection(decision.fault_id)
    assert await runtime.link_detection(
        decision.fault_id,
        kind=FaultKind.CLOB_429,
        detection_id="incident-1",
    )

    cleanup = await runtime.cleanup(decision.fault_id, "contained")
    receipt = runtime.make_recovery_receipt(
        FaultRecoveryWriter.DISCOVERY_BATCH,
        writer_id=7,
        writer_occurred_at_ms=1_104,
    )
    assert receipt is not None
    recovered = await runtime.record_recovery(receipt)

    assert cleanup.terminal_state is FaultEventState.CLEANED
    assert recovered is True
    assert runtime.pending_recovery_fault_id is None
    assert [event[1] for event in authority.events] == [
        FaultEventState.INJECTED,
        FaultEventState.DETECTED,
        FaultEventState.CONTAINED,
        FaultEventState.CLEANED,
        FaultEventState.RECOVERED,
    ]
    assert authority.events[-1][2]["ownership"] is ownership


@pytest.mark.asyncio
async def test_partial_detection_uses_coverage_evidence_not_incident_evidence() -> None:
    gamma_identity = FaultRuntimeIdentity(
        component="discovery",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    )
    ownership = FaultOwnershipCapability(
        fault_id="fault-partial",
        runtime=gamma_identity,
        token="f" * 64,
    )
    intent = FaultIntent(
        fault_id="fault-partial",
        kind=FaultKind.GAMMA_PARTIAL,
        call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
        target_key="discovery",
        parameters={"keep_events": 1},
        ttl_ms=30_000,
        runtime=gamma_identity,
        nonce_digest="b" * 64,
        accepted_at_ms=1_000,
        ownership_capability=ownership,
    )
    authority = _Authority(intent)
    runtime = FaultRuntime(
        identity=gamma_identity,
        authority=authority,
        clock_ms=iter((1_100, 1_101, 1_102, 1_103)).__next__,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE, "discovery")
    )
    await runtime.record_injection(decision.fault_id)

    assert await runtime.link_detection(
        decision.fault_id,
        kind=FaultKind.GAMMA_PARTIAL,
        detection_id="coverage-" + "c" * 64,
    )
    assert authority.events[1][2]["evidence"] == {
        "coverage_id": "coverage-" + "c" * 64
    }


@pytest.mark.asyncio
async def test_recovery_requires_exact_real_discovery_writer_receipt(
    tmp_path: Path,
) -> None:
    path = tmp_path / "recovery.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    proof = DiscoveryAdmissionProof(
        effective_capacity=2,
        candidate_max_wait_ms=60_000,
        selection_budget_ms=6_000,
        poll_interval_ms=1_000,
        group_timeout_ms=10_000,
        terminal_write_budget_ms=5_000,
        high_burst_groups=1,
        reserved_non_high_slots=2,
    )
    store.configure_discovery_admission(proof, now_ms=0)
    old_batch_id, _ = store.publish_discovery_batch(
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        started_at_ms=900,
        finished_at_ms=900,
        page_event_count=0,
        candidates=(),
        admission_proof=proof,
    )
    identity = FaultRuntimeIdentity(
        component="discovery",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
    )
    authority = FaultAuthorityStore(path)
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-recovery",
        attempt=1,
        started_at_ms=1_000,
    )
    admission = authority.accept_intent(
        FaultIntentRequest(
            fault_id="fault-recovery",
            kind=FaultKind.GAMMA_TIMEOUT,
            call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
            target_key="discovery",
            parameters={"delay_ms": 1},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="1" * 64,
            authorization_digest="2" * 64,
        ),
        accepted_at_ms=1_001,
    )
    assert admission.accepted
    wall = [1_100]
    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=lambda: wall[0],
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE, "discovery")
    )
    await runtime.record_injection(decision.fault_id)
    assert await runtime.link_detection(
        decision.fault_id,
        kind=FaultKind.GAMMA_TIMEOUT,
        detection_id="incident-recovery",
    )
    cleanup = await runtime.cleanup(decision.fault_id, "contained")
    assert cleanup.terminal_state is FaultEventState.CLEANED
    good_batch_id, _ = store.publish_discovery_batch(
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        started_at_ms=1_200,
        finished_at_ms=1_200,
        page_event_count=0,
        candidates=(),
        admission_proof=proof,
    )
    wall[0] = 1_199
    correct = FaultRecoveryReceipt(
        fault_id="fault-recovery",
        kind=FaultKind.GAMMA_TIMEOUT,
        call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
        component="discovery",
        runtime=identity,
        writer=FaultRecoveryWriter.DISCOVERY_BATCH,
        writer_id=good_batch_id,
        writer_occurred_at_ms=1_200,
    )
    assert await runtime.record_recovery(correct) is False
    assert next(
        event.state
        for event in reversed(authority.validate_history("fault-recovery").events)
        if event.state is not None
    ) is (
        FaultEventState.CLEANED
    )
    wall[0] = 1_300
    other_runtime = FaultRuntimeIdentity(
        component="discovery",
        release_id="a" * 40,
        machine_id="other-machine",
        boot_id=UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
    )
    rejected = (
        FaultRecoveryReceipt(
            fault_id="fault-other",
            kind=correct.kind,
            call_class=correct.call_class,
            component=correct.component,
            runtime=correct.runtime,
            writer=correct.writer,
            writer_id=correct.writer_id,
            writer_occurred_at_ms=correct.writer_occurred_at_ms,
        ),
        FaultRecoveryReceipt(
            fault_id=correct.fault_id,
            kind=FaultKind.GAMMA_PARTIAL,
            call_class=correct.call_class,
            component=correct.component,
            runtime=correct.runtime,
            writer=correct.writer,
            writer_id=correct.writer_id,
            writer_occurred_at_ms=correct.writer_occurred_at_ms,
        ),
        FaultRecoveryReceipt(
            fault_id=correct.fault_id,
            kind=correct.kind,
            call_class=correct.call_class,
            component=correct.component,
            runtime=correct.runtime,
            writer=FaultRecoveryWriter.RECONCILIATION_CHECKPOINT,
            writer_id="window-other",
            writer_occurred_at_ms=correct.writer_occurred_at_ms,
        ),
        FaultRecoveryReceipt(
            fault_id=correct.fault_id,
            kind=correct.kind,
            call_class=correct.call_class,
            component=correct.component,
            runtime=correct.runtime,
            writer=correct.writer,
            writer_id=old_batch_id,
            writer_occurred_at_ms=900,
        ),
        FaultRecoveryReceipt(
            fault_id=correct.fault_id,
            kind=correct.kind,
            call_class=correct.call_class,
            component=correct.component,
            runtime=correct.runtime,
            writer=correct.writer,
            writer_id=999_999,
            writer_occurred_at_ms=correct.writer_occurred_at_ms,
        ),
        FaultRecoveryReceipt(
            fault_id=correct.fault_id,
            kind=correct.kind,
            call_class=correct.call_class,
            component=correct.component,
            runtime=other_runtime,
            writer=correct.writer,
            writer_id=correct.writer_id,
            writer_occurred_at_ms=correct.writer_occurred_at_ms,
        ),
        FaultRecoveryReceipt(
            fault_id=correct.fault_id,
            kind=correct.kind,
            call_class=correct.call_class,
            component=correct.component,
            runtime=correct.runtime,
            writer=correct.writer,
            writer_id=correct.writer_id,
            writer_occurred_at_ms=9_999,
        ),
    )

    for receipt in rejected:
        assert await runtime.record_recovery(receipt) is False
        assert next(
            event.state
            for event in reversed(authority.validate_history("fault-recovery").events)
            if event.state is not None
        ) is (
            FaultEventState.CLEANED
        )

    assert await runtime.record_recovery(correct) is True
    history = authority.validate_history("fault-recovery")
    assert history.events[-1].state is FaultEventState.RECOVERED
    assert history.events[-1].evidence == {
        "recovery_id": f"discovery-batch-{good_batch_id}"
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    (
        "checkpoint-hash",
        "noncanonical-anchor",
        "staging-digest",
        "compacted-sample-count",
        "retained-prefix-row",
    ),
)
async def test_reconciliation_recovery_rejects_corrupt_authority_checkpoint(
    tmp_path: Path,
    corruption: str,
) -> None:
    path = tmp_path / f"{corruption}.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    identity = FaultRuntimeIdentity(
        component="reconciliation",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=uuid4(),
    )
    authority = FaultAuthorityStore(path)
    authority.register_runtime_start(
        identity,
        supervisor_run_id=f"run-{corruption}",
        attempt=1,
        started_at_ms=1_000,
    )
    admission = authority.accept_intent(
        FaultIntentRequest(
            fault_id=f"fault-{corruption}",
            kind=FaultKind.GAMMA_CURSOR,
            call_class=FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE,
            target_key="reconciliation",
            parameters={},
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest=hashlib.sha256(corruption.encode()).hexdigest(),
            authorization_digest="2" * 64,
        ),
        accepted_at_ms=1_001,
    )
    assert admission.accepted
    wall = [1_100]
    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=lambda: wall[0],
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(
            FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE,
            "reconciliation",
        )
    )
    await runtime.record_injection(decision.fault_id)
    assert await runtime.link_detection(
        decision.fault_id,
        kind=FaultKind.GAMMA_CURSOR,
        detection_id=f"incident-{corruption}",
    )
    assert (
        await runtime.cleanup(decision.fault_id, "contained")
    ).terminal_state is FaultEventState.CLEANED

    window = store.begin_reconciliation(started_at_ms=1_200)
    committed = store.publish_reconciliation_batch(
        window_id=window.id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        started_at_ms=1_200,
        finished_at_ms=1_201,
        page_event_count=0,
        candidates=(),
    )
    store.apply_reconciliation_diff(committed.id)
    receipt = runtime.make_recovery_receipt(
        FaultRecoveryWriter.RECONCILIATION_CHECKPOINT,
        writer_id=committed.id,
        writer_occurred_at_ms=1_201,
    )
    assert receipt is not None
    wall[0] = 1_300

    with sqlite3.connect(path) as con:
        con.row_factory = sqlite3.Row
        checkpoint = con.execute(
            "SELECT * FROM neg_risk_reconciliation_authority_checkpoints "
            "WHERE window_id=?",
            (committed.id,),
        ).fetchone()
        assert checkpoint is not None
        if corruption == "checkpoint-hash":
            con.execute(
                "UPDATE neg_risk_reconciliation_authority_checkpoints "
                "SET checkpoint_hash='tampered' WHERE window_id=?",
                (committed.id,),
            )
        elif corruption == "noncanonical-anchor":
            con.execute(
                "UPDATE neg_risk_reconciliation_authority_checkpoints "
                "SET anchor_json=' ' || anchor_json WHERE window_id=?",
                (committed.id,),
            )
        elif corruption == "staging-digest":
            anchor = json.loads(checkpoint["anchor_json"])
            anchor["staging_digest"] = "sha256:" + "0" * 64
            anchor_json = json.dumps(
                anchor,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            anchor_digest = (
                f"sha256:{hashlib.sha256(anchor_json.encode()).hexdigest()}"
            )
            checkpoint_hash = reconciliation_authority_checkpoint_hash(
                window_id=checkpoint["window_id"],
                domain=checkpoint["domain"],
                version=checkpoint["version"],
                generation=checkpoint["generation"],
                through_batch_id=checkpoint["through_batch_id"],
                through_sequence=checkpoint["through_sequence"],
                compacted_batch_rows=checkpoint["compacted_batch_rows"],
                compacted_sample_rows=checkpoint["compacted_sample_rows"],
                prefix_digest=checkpoint["prefix_digest"],
                anchor_digest=anchor_digest,
            )
            con.execute(
                "UPDATE neg_risk_reconciliation_authority_checkpoints SET "
                "anchor_json=?,anchor_digest=?,checkpoint_hash=? WHERE window_id=?",
                (
                    anchor_json,
                    anchor_digest,
                    checkpoint_hash,
                    committed.id,
                ),
            )
        elif corruption == "compacted-sample-count":
            con.execute(
                "UPDATE neg_risk_reconciliation_authority_checkpoints "
                "SET compacted_sample_rows=compacted_sample_rows+1 "
                "WHERE window_id=?",
                (committed.id,),
            )
        else:
            con.execute(
                "INSERT INTO neg_risk_reconciliation_batches("
                "id,window_id,batch_sequence,requested_cursor,next_cursor,"
                "completed,started_at_ms,finished_at_ms,page_event_count,"
                "groups_staged,observed_count,unique_count,update_count,"
                "duplicate_count,rejected_count"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    checkpoint["through_batch_id"],
                    committed.id,
                    checkpoint["through_sequence"],
                    None,
                    None,
                    1,
                    1_200,
                    1_201,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ),
            )

    with pytest.raises(ValueError, match="authority-checkpoint"):
        store.current_reconciliation()
    assert await runtime.record_recovery(receipt) is False
    assert runtime.degraded is True
    assert next(
        event.state
        for event in reversed(authority.validate_history(decision.fault_id).events)
        if event.state is not None
    ) is (
        FaultEventState.CLEANED
    )


@pytest.mark.asyncio
async def test_cleanup_clears_memory_before_append() -> None:
    observations: list[bool] = []
    receipts: list[dict[str, object]] = []
    authority = _Authority(_intent())
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=authority,
        clock_ms=lambda: 1_100,
    )
    await runtime.sync_before_batch()

    def relinquish_claim(fault_id, **kwargs):
        observations.append(runtime.active_fault_id is None)
        receipts.append(kwargs)
        authority.events.append((fault_id, FaultEventState.ABANDONED, kwargs))
        return SimpleNamespace(state=FaultEventState.ABANDONED)

    authority.relinquish_claim = relinquish_claim
    result = await runtime.cleanup("fault-1", "cancelled")

    assert result.memory_cleared is True
    assert result.receipt_persisted is True
    assert observations == [True]
    assert receipts[0]["memory_cleared_at_ms"] <= receipts[0]["occurred_at_ms"]
    assert authority.events[0][1] is FaultEventState.ABANDONED


@pytest.mark.asyncio
async def test_post_injection_cleanup_write_failure_freezes_all_future_fault_io() -> None:
    class CleanupBrokenAuthority(_Authority):
        def relinquish_claim(self, fault_id, **kwargs):
            raise RuntimeError("cleanup-authority-unavailable")

    ownership = FaultOwnershipCapability(
        fault_id="fault-1",
        runtime=IDENTITY,
        token="f" * 64,
    )
    authority = CleanupBrokenAuthority(_intent(ownership=ownership))
    runtime = FaultRuntime(
        identity=IDENTITY,
        authority=authority,
        clock_ms=iter((1_100, 1_101, 1_102)).__next__,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    )
    assert await runtime.record_injection(decision.fault_id) is not None
    claims_before = tuple(authority.claims)

    cleanup = await runtime.cleanup(decision.fault_id, "forced-write-failure")

    assert cleanup == CleanupResult(
        memory_cleared=True,
        receipt_persisted=False,
        degraded=True,
    )
    assert runtime.degraded is True
    assert runtime._evidence_frozen is True
    assert runtime._injected_fault_id is None
    assert runtime._last_injection is None
    assert runtime.pending_recovery_fault_id is None
    assert runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    ).inject is False

    authority.intent = _intent(ownership=ownership)
    await runtime.sync_before_batch()

    assert tuple(authority.claims) == claims_before


def test_disabled_or_unavailable_builder_is_pass_through(tmp_path: Path) -> None:
    disabled = build_fault_runtime(
        enabled=False,
        db_path=tmp_path / "missing.db",
        identity=IDENTITY,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=1_000,
    )
    unavailable = build_fault_runtime(
        enabled=True,
        db_path=tmp_path / "missing" / "fault.db",
        identity=IDENTITY,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=1_000,
    )

    assert isinstance(disabled, PassThroughFaultRuntime)
    assert disabled.degraded is False
    assert isinstance(unavailable, PassThroughFaultRuntime)
    assert unavailable.degraded is True
    assert unavailable.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    ).inject is False


@pytest.mark.asyncio
async def test_pass_through_runtime_never_blocks_batches() -> None:
    runtime = PassThroughFaultRuntime(degraded=True)
    await runtime.sync_before_batch()
    assert (await runtime.cleanup("unused", "cancelled")).memory_cleared is False


async def _pending_real_recovery(
    tmp_path: Path,
    *,
    name: str,
    component: str,
    kind: FaultKind,
    call_class: FaultCallClass,
    target_key: str,
    parameters: dict[str, int],
) -> tuple[FaultRuntime, FaultAuthorityStore]:
    path = tmp_path / f"{name}.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    identity = FaultRuntimeIdentity(
        component=component,
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=uuid4(),
    )
    authority = FaultAuthorityStore(path)
    authority.register_runtime_start(
        identity,
        supervisor_run_id="run-1",
        attempt=1,
        started_at_ms=1_000,
    )
    assert authority.accept_intent(
        FaultIntentRequest(
            fault_id=f"fault-{name}",
            kind=kind,
            call_class=call_class,
            target_key=target_key,
            parameters=parameters,
            ttl_ms=30_000,
            runtime=identity,
        ),
        auth=FaultAuthorization(
            nonce_digest="a" * 64,
            authorization_digest="b" * 64,
        ),
        accepted_at_ms=1_001,
    ).accepted
    now_ms = 1_010

    def clock_ms() -> int:
        nonlocal now_ms
        now_ms += 1
        return now_ms

    runtime = FaultRuntime(
        identity=identity,
        authority=authority,
        clock_ms=clock_ms,
        monotonic=lambda: 10.0,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(FaultCall(call_class, target_key))
    assert await runtime.record_injection(decision.fault_id) is not None
    assert await runtime.link_detection(
        decision.fault_id,
        kind=kind,
        detection_id=f"incident-{name}",
    )
    cleanup = await runtime.cleanup(decision.fault_id, "contained")
    assert cleanup.terminal_state is FaultEventState.CLEANED
    assert runtime.pending_recovery_fault_id == decision.fault_id
    return runtime, authority


@pytest.mark.parametrize(
    (
        "name",
        "component",
        "kind",
        "call_class",
        "target_key",
        "parameters",
        "wrong_writer",
        "writer_id",
    ),
    [
        (
            "telegram-cross-family",
            "notification",
            FaultKind.TELEGRAM_FAILURE,
            FaultCallClass.TELEGRAM_OPPORTUNITY_CARD,
            "1",
            {},
            FaultRecoveryWriter.CANDIDATE_SUCCESS,
            "candidate-qb",
        ),
        (
            "candidate-cross-family",
            "candidate",
            FaultKind.CLOB_429,
            FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
            "group-1",
            {},
            FaultRecoveryWriter.TELEGRAM_DELIVERY,
            1,
        ),
        (
            "discovery-cross-family",
            "discovery",
            FaultKind.GAMMA_TIMEOUT,
            FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
            "discovery",
            {"delay_ms": 1},
            FaultRecoveryWriter.CANDIDATE_SUCCESS,
            "candidate-qb",
        ),
        (
            "reconciliation-cross-family",
            "reconciliation",
            FaultKind.GAMMA_CURSOR,
            FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE,
            "reconciliation",
            {},
            FaultRecoveryWriter.TELEGRAM_DELIVERY,
            1,
        ),
    ],
)
@pytest.mark.asyncio
async def test_exact_target_cross_family_recovery_is_evidence_invalid(
    tmp_path: Path,
    name: str,
    component: str,
    kind: FaultKind,
    call_class: FaultCallClass,
    target_key: str,
    parameters: dict[str, int],
    wrong_writer: FaultRecoveryWriter,
    writer_id: int | str,
) -> None:
    runtime, authority = await _pending_real_recovery(
        tmp_path,
        name=name,
        component=component,
        kind=kind,
        call_class=call_class,
        target_key=target_key,
        parameters=parameters,
    )

    outcome = await runtime.record_writer_recovery_outcome(
        wrong_writer,
        target_key=target_key,
        writer_id=writer_id,
        writer_occurred_at_ms=2_000,
    )

    assert outcome is FaultRecoveryOutcome.INVALID
    history = authority.validate_history(f"fault-{name}")
    assert history.valid
    assert history.events[-1].state is FaultEventState.EVIDENCE_INVALID
    assert runtime.degraded is True
    assert runtime.pending_recovery_fault_id is None


@pytest.mark.parametrize(
    (
        "name",
        "component",
        "kind",
        "call_class",
        "target_key",
        "parameters",
        "writer",
        "writer_id",
    ),
    [
        (
            "telegram-other-target",
            "notification",
            FaultKind.TELEGRAM_FAILURE,
            FaultCallClass.TELEGRAM_OPPORTUNITY_CARD,
            "1",
            {},
            FaultRecoveryWriter.TELEGRAM_DELIVERY,
            1,
        ),
        (
            "candidate-other-target",
            "candidate",
            FaultKind.CLOB_429,
            FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
            "group-1",
            {},
            FaultRecoveryWriter.CANDIDATE_SUCCESS,
            "candidate-qb",
        ),
        (
            "discovery-other-target",
            "discovery",
            FaultKind.GAMMA_TIMEOUT,
            FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
            "discovery",
            {"delay_ms": 1},
            FaultRecoveryWriter.DISCOVERY_BATCH,
            1,
        ),
        (
            "reconciliation-other-target",
            "reconciliation",
            FaultKind.GAMMA_CURSOR,
            FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE,
            "reconciliation",
            {},
            FaultRecoveryWriter.RECONCILIATION_CHECKPOINT,
            "window-1",
        ),
    ],
)
@pytest.mark.asyncio
async def test_different_target_recovery_is_not_applicable_without_mutation(
    tmp_path: Path,
    name: str,
    component: str,
    kind: FaultKind,
    call_class: FaultCallClass,
    target_key: str,
    parameters: dict[str, int],
    writer: FaultRecoveryWriter,
    writer_id: int | str,
) -> None:
    runtime, authority = await _pending_real_recovery(
        tmp_path,
        name=name,
        component=component,
        kind=kind,
        call_class=call_class,
        target_key=target_key,
        parameters=parameters,
    )
    before = authority.validate_history(f"fault-{name}")

    outcome = await runtime.record_writer_recovery_outcome(
        writer,
        target_key=f"other-{target_key}",
        writer_id=writer_id,
        writer_occurred_at_ms=2_000,
    )

    after = authority.validate_history(f"fault-{name}")
    assert outcome is FaultRecoveryOutcome.NOT_APPLICABLE
    assert after.events == before.events
    assert runtime.degraded is False
    assert runtime.pending_recovery_fault_id == f"fault-{name}"


@pytest.mark.asyncio
async def test_exact_recovery_authority_unavailable_is_not_evidence_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, authority = await _pending_real_recovery(
        tmp_path,
        name="telegram-authority-unavailable",
        component="notification",
        kind=FaultKind.TELEGRAM_FAILURE,
        call_class=FaultCallClass.TELEGRAM_OPPORTUNITY_CARD,
        target_key="1",
        parameters={},
    )

    def unavailable(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(authority, "append_recovery_event", unavailable)
    outcome = await runtime.record_writer_recovery_outcome(
        FaultRecoveryWriter.TELEGRAM_DELIVERY,
        target_key="1",
        writer_id=1,
        writer_occurred_at_ms=2_000,
    )

    assert outcome is FaultRecoveryOutcome.UNAVAILABLE
    history = authority.validate_history("fault-telegram-authority-unavailable")
    assert history.valid
    assert next(
        event.state for event in reversed(history.events) if event.state is not None
    ) is FaultEventState.CLEANED
    assert runtime.degraded is True
    assert runtime.pending_recovery_fault_id is None


def _break_evidence_invalid_append(
    monkeypatch: pytest.MonkeyPatch,
    authority: FaultAuthorityStore,
) -> None:
    original = authority.append_event

    def append_event(fault_id, state, **kwargs):
        if state is FaultEventState.EVIDENCE_INVALID:
            raise sqlite3.OperationalError("database is locked")
        return original(fault_id, state, **kwargs)

    monkeypatch.setattr(authority, "append_event", append_event)


@pytest.mark.asyncio
async def test_cross_family_invalid_append_failure_returns_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, authority = await _pending_real_recovery(
        tmp_path,
        name="cross-family-invalid-append-failure",
        component="notification",
        kind=FaultKind.TELEGRAM_FAILURE,
        call_class=FaultCallClass.TELEGRAM_OPPORTUNITY_CARD,
        target_key="1",
        parameters={},
    )
    _break_evidence_invalid_append(monkeypatch, authority)

    outcome = await runtime.record_writer_recovery_outcome(
        FaultRecoveryWriter.CANDIDATE_SUCCESS,
        target_key="1",
        writer_id="candidate-qb",
        writer_occurred_at_ms=2_000,
    )

    assert outcome is FaultRecoveryOutcome.UNAVAILABLE
    history = authority.validate_history("fault-cross-family-invalid-append-failure")
    assert history.valid
    assert next(
        event.state for event in reversed(history.events) if event.state is not None
    ) is FaultEventState.CLEANED
    assert runtime.degraded is True
    assert runtime.pending_recovery_fault_id is None


@pytest.mark.asyncio
async def test_post_validation_invalid_append_failure_returns_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, authority = await _pending_real_recovery(
        tmp_path,
        name="post-validation-invalid-append-failure",
        component="notification",
        kind=FaultKind.TELEGRAM_FAILURE,
        call_class=FaultCallClass.TELEGRAM_OPPORTUNITY_CARD,
        target_key="1",
        parameters={},
    )
    _break_evidence_invalid_append(monkeypatch, authority)

    # Writer family and target are legal, but attempt 999 does not exist, so
    # the same-transaction authority validator returns semantic invalidity.
    outcome = await runtime.record_writer_recovery_outcome(
        FaultRecoveryWriter.TELEGRAM_DELIVERY,
        target_key="1",
        writer_id=999,
        writer_occurred_at_ms=2_000,
    )

    assert outcome is FaultRecoveryOutcome.UNAVAILABLE
    history = authority.validate_history("fault-post-validation-invalid-append-failure")
    assert history.valid
    assert next(
        event.state for event in reversed(history.events) if event.state is not None
    ) is FaultEventState.CLEANED
    assert runtime.degraded is True
    assert runtime.pending_recovery_fault_id is None


def test_each_builder_registration_uses_exact_boot_identity(tmp_path: Path) -> None:
    from polyarb.perception.store import OpportunityPerceptionStore

    path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    runtime = build_fault_runtime(
        enabled=True,
        db_path=path,
        identity=IDENTITY,
        supervisor_run_id="run-7",
        attempt=3,
        started_at_ms=1_234,
    )

    assert isinstance(runtime, FaultRuntime)
    with store._connect() as con:
        row = con.execute(
            "SELECT component,release_id,machine_id,boot_id,"
            "supervisor_run_id,attempt FROM neg_risk_fault_runtime_starts"
        ).fetchone()
    assert tuple(row) == (
        "candidate",
        "a" * 40,
        "machine-1",
        str(IDENTITY.boot_id),
        "run-7",
        3,
    )


def test_boot_ids_are_uuid4() -> None:
    assert uuid4().version == 4


class _BoundaryRuntime:
    degraded = False

    def __init__(self) -> None:
        self.synced = False
        self.sync_event = asyncio.Event()
        self.cleanup_reasons: list[str] = []

    @property
    def active_fault_id(self):
        return "fault-1"

    async def sync_before_batch(self) -> None:
        self.synced = True
        self.sync_event.set()

    async def cleanup(self, fault_id: str, reason: str) -> CleanupResult:
        assert fault_id == "fault-1"
        self.cleanup_reasons.append(reason)
        return CleanupResult(True, True)

    def consume(self, call):
        raise AssertionError("hot-path adapters are a later task")


@pytest.mark.asyncio
async def test_candidate_claims_before_loading_selection(tmp_path: Path) -> None:
    from polyarb.perception.candidate_watcher import (
        CandidateWatcherRuntime,
        CandidateWatcherScheduler,
    )
    from polyarb.perception.store import OpportunityPerceptionStore

    boundary = _BoundaryRuntime()

    class Store(OpportunityPerceptionStore):
        def candidate_scheduling_snapshot(self, group_ids):
            assert boundary.synced is True
            return ()

    store = Store(tmp_path / "candidate.db")
    store.init_schema()
    scheduler = CandidateWatcherScheduler(
        watcher=SimpleNamespace(),
        store=store,
        candidate_group_ids=lambda: (),
        runtime=CandidateWatcherRuntime(),
        fault_runtime=boundary,
    )
    try:
        await scheduler.run_due_once()
    finally:
        scheduler.close()


@pytest.mark.asyncio
async def test_candidate_cancellation_invokes_fault_cleanup(tmp_path: Path) -> None:
    from polyarb.perception.candidate_watcher import (
        CandidateWatcherRuntime,
        CandidateWatcherScheduler,
    )
    from polyarb.perception.store import OpportunityPerceptionStore

    boundary = _BoundaryRuntime()
    store = OpportunityPerceptionStore(tmp_path / "cancel.db")
    store.init_schema()
    scheduler = CandidateWatcherScheduler(
        watcher=SimpleNamespace(),
        store=store,
        candidate_group_ids=lambda: (),
        runtime=CandidateWatcherRuntime(),
        fault_runtime=boundary,
    )
    task = asyncio.create_task(scheduler.run(asyncio.Event()))
    await asyncio.wait_for(boundary.sync_event.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert boundary.cleanup_reasons == ["candidate-stopped"]


@pytest.mark.asyncio
@pytest.mark.parametrize("component", ["discovery", "reconciliation"])
async def test_gamma_workers_claim_immediately_before_page_fetch(
    tmp_path: Path,
    component: str,
) -> None:
    from polyarb.clients.gamma_client import EventPage
    from polyarb.perception.discovery import DiscoveryWorker
    from polyarb.perception.reconciliation import ReconciliationWorker
    from polyarb.perception.store import OpportunityPerceptionStore

    boundary = _BoundaryRuntime()

    class Gamma:
        async def fetch_active_event_page(self, cursor, limit):
            assert boundary.synced is True
            return EventPage(
                events=(),
                requested_cursor=cursor,
                next_cursor=None,
                completed=True,
                started_at_ms=1_000,
                finished_at_ms=1_001,
            )

    store = OpportunityPerceptionStore(tmp_path / f"{component}.db")
    store.init_schema()
    if component == "discovery":
        worker = DiscoveryWorker(
            gamma=Gamma(),
            store=store,
            clock_ms=lambda: 1_000,
            fault_runtime=boundary,
        )
    else:
        worker = ReconciliationWorker(
            gamma=Gamma(),
            store=store,
            clock_ms=lambda: 1_000,
            fault_runtime=boundary,
        )

    await worker.run_batch()


def test_invalid_or_absent_child_boot_is_degraded_pass_through(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = SimpleNamespace(
        upstream_fault_control_enabled=True,
        release_id="a" * 40,
        db_path=tmp_path / "state.db",
    )
    monkeypatch.setenv("POLYARB_PRODUCER_SUPERVISOR_RUN_ID", "run-1")
    monkeypatch.setenv("POLYARB_PRODUCER_ATTEMPT", "1")
    monkeypatch.delenv("POLYARB_PRODUCER_BOOT_ID", raising=False)
    absent = _build_child_fault_runtime("candidate", settings)
    monkeypatch.setenv("POLYARB_PRODUCER_BOOT_ID", "not-a-uuid")
    invalid = _build_child_fault_runtime("candidate", settings)

    assert isinstance(absent, PassThroughFaultRuntime) and absent.degraded
    assert isinstance(invalid, PassThroughFaultRuntime) and invalid.degraded


def test_child_registration_binds_supervisor_attempt_and_exact_environment_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from polyarb.perception.store import OpportunityPerceptionStore

    path = tmp_path / "child.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    boot_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    settings = SimpleNamespace(
        upstream_fault_control_enabled=True,
        release_id="c" * 40,
        db_path=path,
    )
    monkeypatch.setenv("FLY_MACHINE_ID", "machine-child")
    monkeypatch.setenv("POLYARB_PRODUCER_BOOT_ID", str(boot_id))
    monkeypatch.setenv("POLYARB_PRODUCER_SUPERVISOR_RUN_ID", "run-child")
    monkeypatch.setenv("POLYARB_PRODUCER_ATTEMPT", "4")

    runtime = _build_child_fault_runtime("discovery", settings)

    assert isinstance(runtime, FaultRuntime)
    with store._connect() as con:
        row = con.execute(
            "SELECT component,release_id,machine_id,boot_id,"
            "supervisor_run_id,attempt FROM neg_risk_fault_runtime_starts"
        ).fetchone()
    assert tuple(row) == (
        "discovery",
        "c" * 40,
        "machine-child",
        str(boot_id),
        "run-child",
        4,
    )


def _real_runtime(
    tmp_path: Path,
    *,
    authority_type=FaultAuthorityStore,
    clock_ms=lambda: 1_200,
    monotonic=lambda: 10.0,
):
    path = tmp_path / "runtime.db"
    OpportunityPerceptionStore(path).init_schema()
    authority = authority_type(path)
    authority.register_runtime_start(
        IDENTITY,
        supervisor_run_id="run-real",
        attempt=1,
        started_at_ms=1_000,
    )
    return (
        path,
        authority,
        FaultRuntime(
            identity=IDENTITY,
            authority=authority,
            clock_ms=clock_ms,
            monotonic=monotonic,
        ),
    )


def _accept(
    authority: FaultAuthorityStore,
    *,
    fault_id: str,
    target_key: str,
    accepted_at_ms: int,
    ttl_ms: int = 10_000,
    nonce: str = "d",
) -> None:
    admission = authority.accept_intent(
        FaultIntentRequest(
            fault_id=fault_id,
            kind=FaultKind.CLOB_429,
            call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
            target_key=target_key,
            parameters={},
            ttl_ms=ttl_ms,
            runtime=IDENTITY,
        ),
        auth=FaultAuthorization(
            nonce_digest=nonce * 64,
            authorization_digest="e" * 64,
        ),
        accepted_at_ms=accepted_at_ms,
    )
    assert admission.accepted is True


@pytest.mark.asyncio
async def test_real_sqlite_armed_cleanup_persists_valid_abandoned_terminal(
    tmp_path: Path,
) -> None:
    _, authority, runtime = _real_runtime(tmp_path, clock_ms=lambda: 1_300)
    _accept(
        authority,
        fault_id="fault-real",
        target_key="group-real",
        accepted_at_ms=1_100,
    )
    await runtime.sync_before_batch()

    result = await runtime.cleanup("fault-real", "cancelled")

    history = authority.validate_history("fault-real")
    assert result.memory_cleared is True
    assert result.receipt_persisted is True
    assert result.terminal_state is FaultEventState.ABANDONED
    assert history.valid is True
    assert history.events[-1].state is FaultEventState.ABANDONED


@pytest.mark.asyncio
async def test_real_sqlite_contained_cleanup_returns_actual_cleaned_terminal(
    tmp_path: Path,
) -> None:
    wall = [1_200]
    _, authority, runtime = _real_runtime(tmp_path, clock_ms=lambda: wall[0])
    _accept(
        authority,
        fault_id="fault-contained",
        target_key="group-contained",
        accepted_at_ms=1_100,
    )
    await runtime.sync_before_batch()
    active = runtime._controller.active
    assert active is not None and active.intent.ownership_capability is not None
    ownership = active.intent.ownership_capability
    authority.append_event(
        "fault-contained",
        FaultEventState.INJECTED,
        occurred_at_ms=1_201,
        evidence={
            "call_id": "call-contained",
            "call_binding_digest": fault_call_binding_digest(
                fault_id="fault-contained",
                kind=FaultKind.CLOB_429.value,
                call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH.value,
                target_key="group-contained",
                runtime={
                    "component": IDENTITY.component,
                    "release_id": IDENTITY.release_id,
                    "machine_id": IDENTITY.machine_id,
                    "boot_id": str(IDENTITY.boot_id),
                },
                call_id="call-contained",
            ),
        },
        ownership=ownership,
    )
    authority.append_event(
        "fault-contained",
        FaultEventState.DETECTED,
        occurred_at_ms=1_202,
        evidence={"incident_id": "incident-contained"},
    )
    authority.append_event(
        "fault-contained",
        FaultEventState.CONTAINED,
        occurred_at_ms=1_203,
        evidence={"containment_id": "containment-contained"},
    )
    wall[0] = 1_300

    result = await runtime.cleanup("fault-contained", "contained")

    assert result.receipt_persisted is True
    assert result.terminal_state is FaultEventState.CLEANED
    assert authority.validate_history("fault-contained").valid is True


@pytest.mark.asyncio
async def test_cancelled_blocked_claim_settles_and_relinquishes_real_sqlite_chain(
    tmp_path: Path,
) -> None:
    committed = threading.Event()
    release = threading.Event()

    class BlockingAuthority(FaultAuthorityStore):
        def claim_pending(self, identity, *, claimed_at_ms):
            result = super().claim_pending(identity, claimed_at_ms=claimed_at_ms)
            committed.set()
            assert release.wait(timeout=2)
            return result

    _, authority, runtime = _real_runtime(tmp_path, authority_type=BlockingAuthority)
    _accept(
        authority,
        fault_id="fault-cancelled-claim",
        target_key="group-cancelled",
        accepted_at_ms=1_100,
    )
    task = asyncio.create_task(runtime.sync_before_batch())
    assert await asyncio.to_thread(committed.wait, 2)
    task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    history = authority.validate_history("fault-cancelled-claim")
    assert runtime.active_fault_id is None
    assert history.valid is True
    assert history.events[-1].state is FaultEventState.ABANDONED


@pytest.mark.asyncio
async def test_cancelled_committed_injection_installs_receipt_then_abandons_chain(
    tmp_path: Path,
) -> None:
    committed = threading.Event()
    release = threading.Event()

    class BlockingAuthority(FaultAuthorityStore):
        def append_event(self, fault_id, state, **kwargs):
            result = super().append_event(fault_id, state, **kwargs)
            if state is FaultEventState.INJECTED:
                committed.set()
                assert release.wait(timeout=2)
            return result

    wall = iter((1_200, 1_201, 1_202, 1_203))
    _, authority, runtime = _real_runtime(
        tmp_path,
        authority_type=BlockingAuthority,
        clock_ms=wall.__next__,
    )
    _accept(
        authority,
        fault_id="fault-cancelled-injection",
        target_key="group-cancelled",
        accepted_at_ms=1_100,
    )
    await runtime.sync_before_batch()
    decision = runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-cancelled")
    )
    task = asyncio.create_task(runtime.record_injection(decision.fault_id))
    assert await asyncio.to_thread(committed.wait, 2)
    task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    history = authority.validate_history("fault-cancelled-injection")
    assert history.valid is True
    assert history.events[-1].state is FaultEventState.ABANDONED
    assert runtime.active_fault_id is None
    assert runtime.pending_recovery_fault_id is None


@pytest.mark.asyncio
async def test_expired_unmatched_fault_is_relinquished_then_same_boot_claims_next(
    tmp_path: Path,
) -> None:
    wall = [1_200]
    monotonic = [10.0]
    _, authority, runtime = _real_runtime(
        tmp_path,
        clock_ms=lambda: wall[0],
        monotonic=lambda: monotonic[0],
    )
    _accept(
        authority,
        fault_id="fault-expired",
        target_key="group-unmatched",
        accepted_at_ms=1_100,
        ttl_ms=1_000,
    )
    await runtime.sync_before_batch()
    assert runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-other")
    ).inject is False
    wall[0] = 2_200
    monotonic[0] = 11.1

    await runtime.sync_before_batch()
    expired = authority.validate_history("fault-expired")
    assert expired.valid is True
    assert expired.events[-1].state is FaultEventState.EXPIRED
    _accept(
        authority,
        fault_id="fault-next",
        target_key="group-next",
        accepted_at_ms=2_300,
        nonce="f",
    )
    wall[0] = 2_400
    await runtime.sync_before_batch()

    assert runtime.active_fault_id == "fault-next"


@pytest.mark.asyncio
async def test_frozen_controller_performs_zero_future_authority_claim_work(
    tmp_path: Path,
) -> None:
    class FailingRelinquishAuthority(FaultAuthorityStore):
        claim_count = 0

        def claim_pending(self, identity, *, claimed_at_ms):
            self.claim_count += 1
            return super().claim_pending(identity, claimed_at_ms=claimed_at_ms)

        def relinquish_claim(self, fault_id, *, occurred_at_ms, ownership):
            raise RuntimeError("forced-receipt-failure")

    _, authority, runtime = _real_runtime(
        tmp_path,
        authority_type=FailingRelinquishAuthority,
    )
    _accept(
        authority,
        fault_id="fault-freeze",
        target_key="group-freeze",
        accepted_at_ms=1_100,
    )
    await runtime.sync_before_batch()
    before = authority.validate_history("fault-freeze")
    cleanup = await runtime.cleanup("fault-freeze", "forced-failure")
    assert cleanup.receipt_persisted is False
    assert runtime._controller.frozen is True
    assert authority.claim_count == 1

    await runtime.sync_before_batch()

    after = authority.validate_history("fault-freeze")
    assert authority.claim_count == 1
    assert after == before
    assert runtime.active_fault_id is None
    assert runtime.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-freeze")
    ).inject is False
