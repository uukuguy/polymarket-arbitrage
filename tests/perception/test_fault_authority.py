from __future__ import annotations

import sqlite3
import threading
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import (
    FaultAuthorization,
    FaultCallClass,
    FaultEventState,
    FaultIntentRequest,
    FaultKind,
    FaultRuntimeIdentity,
)
from polyarb.storage.schemas import DDL

RUNTIME = FaultRuntimeIdentity(
    component="candidate",
    release_id="a" * 40,
    machine_id="machine-1",
    boot_id=UUID("12345678-1234-4678-9234-567812345678"),
)


def request(**changes: object) -> FaultIntentRequest:
    values = {
        "fault_id": "fault-1",
        "kind": FaultKind.CLOB_LATENCY,
        "call_class": FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
        "target_key": "group-1",
        "parameters": {"delay_ms": 10},
        "ttl_ms": 10_000,
        "runtime": RUNTIME,
    }
    values.update(changes)
    return FaultIntentRequest(**values)


def auth(value: str = "b") -> FaultAuthorization:
    return FaultAuthorization(nonce_digest=value * 64, authorization_digest="c" * 64)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "fault.db"
    with sqlite3.connect(path) as con:
        con.executescript(DDL)
    return path


@pytest.fixture
def store(db_path: Path) -> FaultAuthorityStore:
    value = FaultAuthorityStore(db_path)
    value.register_runtime_start(RUNTIME, supervisor_run_id="run-1", attempt=1, started_at_ms=1_000)
    return value


def test_runtime_registration_is_append_only_and_duplicate_identity_is_idempotent(
    db_path: Path,
) -> None:
    store = FaultAuthorityStore(db_path)
    first = store.register_runtime_start(
        RUNTIME, supervisor_run_id="run-1", attempt=1, started_at_ms=1_000
    )
    second = store.register_runtime_start(
        RUNTIME, supervisor_run_id="run-1", attempt=1, started_at_ms=1_000
    )
    assert first == second
    with sqlite3.connect(db_path) as con:
        assert con.execute("SELECT count(*) FROM neg_risk_fault_runtime_starts").fetchone()[0] == 1


def test_accepted_intent_persists_only_digests_and_canonical_parameters(
    store: FaultAuthorityStore, db_path: Path
) -> None:
    admission = store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    assert admission.accepted
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM neg_risk_fault_intents").fetchone()
    assert row["parameters_json"] == '{"delay_ms":10}'
    assert row["nonce_digest"] == "b" * 64
    assert row["authorization_digest"] == "c" * 64
    assert "secret" not in set(row.keys())


def test_replay_runtime_mismatch_stale_runtime_and_second_active_reject(
    store: FaultAuthorityStore,
) -> None:
    assert store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100).accepted
    assert not store.accept_intent(
        request(fault_id="fault-replay"), auth=auth(), accepted_at_ms=1_101
    ).accepted
    assert not store.accept_intent(
        request(fault_id="fault-second"), auth=auth("f"), accepted_at_ms=1_102
    ).accepted
    mismatch = replace(RUNTIME, machine_id="machine-2")
    assert not store.accept_intent(
        request(fault_id="fault-mismatch", runtime=mismatch),
        auth=auth("d"),
        accepted_at_ms=1_103,
    ).accepted
    newer = replace(RUNTIME, boot_id=UUID("87654321-4321-4876-9234-567812345678"))
    store.register_runtime_start(newer, supervisor_run_id="run-2", attempt=2, started_at_ms=1_200)
    assert not store.accept_intent(
        request(fault_id="fault-stale"), auth=auth("e"), accepted_at_ms=1_201
    ).accepted


def test_rejected_request_cannot_be_claimed(store: FaultAuthorityStore) -> None:
    bad = replace(RUNTIME, machine_id="wrong-machine")
    admission = store.accept_intent(request(runtime=bad), auth=auth(), accepted_at_ms=1_100)
    assert not admission.accepted
    assert store.claim_pending(bad, claimed_at_ms=1_200) is None


def test_only_exact_runtime_claims_and_claim_is_single_use_across_connections(
    store: FaultAuthorityStore, db_path: Path
) -> None:
    assert store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100).accepted
    assert store.claim_pending(replace(RUNTIME, machine_id="other"), claimed_at_ms=1_200) is None
    results: list[object] = []
    barrier = threading.Barrier(2)

    def claim() -> None:
        other = FaultAuthorityStore(db_path)
        barrier.wait()
        results.append(other.claim_pending(RUNTIME, claimed_at_ms=1_300))

    threads = [threading.Thread(target=claim) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(value is not None for value in results) == 1


def test_missing_or_corrupt_schema_projects_unavailable(tmp_path: Path) -> None:
    missing = FaultAuthorityStore(tmp_path / "missing.db", read_only=True)
    assert not missing.project_fault("fault-1", now_ms=2_000).available
    corrupt_path = tmp_path / "corrupt.db"
    with sqlite3.connect(corrupt_path) as con:
        con.execute("CREATE TABLE neg_risk_fault_intents (fault_id TEXT)")
        con.execute("INSERT INTO neg_risk_fault_intents VALUES ('fault-1')")
    assert (
        not FaultAuthorityStore(corrupt_path, read_only=True)
        .project_fault("fault-1", now_ms=2_000)
        .available
    )


def test_lifecycle_hashes_validate_and_tampering_is_detected(
    store: FaultAuthorityStore, db_path: Path
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    store.claim_pending(RUNTIME, claimed_at_ms=1_200)
    store.append_event(
        "fault-1", FaultEventState.INJECTED, occurred_at_ms=1_300, evidence={"call": 1}
    )
    store.append_event(
        "fault-1", FaultEventState.DETECTED, occurred_at_ms=1_400, evidence={"incident": "i-1"}
    )
    store.append_event("fault-1", FaultEventState.CONTAINED, occurred_at_ms=1_500, evidence={})
    store.append_event("fault-1", FaultEventState.CLEANED, occurred_at_ms=1_600, evidence={})
    store.append_event("fault-1", FaultEventState.RECOVERED, occurred_at_ms=1_700, evidence={})
    assert store.validate_history("fault-1").valid
    with sqlite3.connect(db_path) as con:
        con.execute(
            "UPDATE neg_risk_fault_events SET evidence_json='{}' "
            "WHERE fault_id='fault-1' AND state='detected'"
        )
    assert not store.validate_history("fault-1").valid


@pytest.mark.parametrize(
    "mutation",
    [
        "two-active",
        "missing-predecessor",
        "regression",
        "injection-without-claim",
        "cleanup-before-injection",
    ],
)
def test_projection_fails_closed_for_invalid_chains(
    store: FaultAuthorityStore, db_path: Path, mutation: str
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    if mutation == "two-active":
        with sqlite3.connect(db_path) as con:
            con.execute(
                "INSERT INTO neg_risk_fault_intents "
                "SELECT 'fault-2',kind,call_class,target_key,parameters_json,parameter_digest,"
                "ttl_ms,component,release_id,machine_id,boot_id,nonce_digest,"
                "authorization_digest,accepted_at_ms,'accepted',NULL FROM neg_risk_fault_intents "
                "WHERE fault_id='fault-1'"
            )
        store.append_event(
            "fault-2",
            FaultEventState.AUTHORIZED,
            occurred_at_ms=1_100,
            evidence={"reason": "accepted"},
        )
    elif mutation == "injection-without-claim":
        store.append_event("fault-1", FaultEventState.INJECTED, occurred_at_ms=1_300, evidence={})
    else:
        store.claim_pending(RUNTIME, claimed_at_ms=1_200)
        if mutation == "missing-predecessor":
            with sqlite3.connect(db_path) as con:
                con.execute(
                    "UPDATE neg_risk_fault_events SET previous_hash=? "
                    "WHERE fault_id=? AND state='armed'",
                    ("0" * 64, "fault-1"),
                )
        elif mutation == "regression":
            store.append_event(
                "fault-1", FaultEventState.INJECTED, occurred_at_ms=1_300, evidence={}
            )
            with sqlite3.connect(db_path) as con:
                con.execute(
                    "UPDATE neg_risk_fault_events SET state='authorized' "
                    "WHERE fault_id='fault-1' AND state='injected'"
                )
        else:
            store.append_event(
                "fault-1", FaultEventState.CLEANED, occurred_at_ms=1_300, evidence={}
            )
    assert not store.project_fault("fault-1", now_ms=2_000).available


@pytest.mark.parametrize("state", [None, FaultEventState.INJECTED])
def test_stale_active_chain_projects_abandoned_and_is_never_claimable(
    store: FaultAuthorityStore, state: FaultEventState | None
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    if state is not None:
        store.claim_pending(RUNTIME, claimed_at_ms=1_200)
        store.append_event("fault-1", state, occurred_at_ms=1_300, evidence={})
    newer = replace(RUNTIME, boot_id=UUID("87654321-4321-4876-9234-567812345678"))
    store.register_runtime_start(newer, supervisor_run_id="run-2", attempt=2, started_at_ms=1_400)
    projection = store.project_fault("fault-1", now_ms=1_500)
    assert projection.available and projection.state == FaultEventState.ABANDONED
    assert store.claim_pending(RUNTIME, claimed_at_ms=1_600) is None
    assert store.claim_pending(newer, claimed_at_ms=1_600) is None
    current = store.accept_intent(
        request(fault_id="fault-current", runtime=newer),
        auth=auth("d"),
        accepted_at_ms=1_700,
    )
    assert current.accepted
    assert store.project_fault("fault-current", now_ms=1_800).available


def test_read_only_projection_does_not_mutate_database(
    store: FaultAuthorityStore, db_path: Path
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    with sqlite3.connect(db_path) as con:
        before = tuple(con.iterdump())
    projection = FaultAuthorityStore(db_path, read_only=True).project_fault("fault-1", now_ms=1_200)
    assert projection.available
    with sqlite3.connect(db_path) as con:
        assert tuple(con.iterdump()) == before
