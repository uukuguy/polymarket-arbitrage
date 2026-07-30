from __future__ import annotations

import sqlite3
import threading
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

from polyarb.perception.fault_authority import FaultAuthorityStore, _intent_hash
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


def disable_append_only(con: sqlite3.Connection, table: str) -> None:
    con.execute(f"DROP TRIGGER trg_{table}_no_update")
    con.execute(f"DROP TRIGGER trg_{table}_no_delete")


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


@pytest.mark.parametrize(
    ("tail", "occurred_at_ms", "expected"),
    [
        (FaultEventState.ARMED, 1_200, FaultEventState.ABANDONED),
        (FaultEventState.ARMED, 11_100, FaultEventState.EXPIRED),
        (FaultEventState.INJECTED, 1_400, FaultEventState.ABANDONED),
        (FaultEventState.DETECTED, 1_500, FaultEventState.ABANDONED),
        (FaultEventState.CONTAINED, 1_600, FaultEventState.CLEANED),
    ],
)
def test_owned_relinquish_selects_a_lifecycle_valid_durable_terminal(
    store: FaultAuthorityStore,
    tail: FaultEventState,
    occurred_at_ms: int,
    expected: FaultEventState,
) -> None:
    assert store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100).accepted
    claimed = store.claim_pending(RUNTIME, claimed_at_ms=1_150)
    assert claimed is not None and claimed.ownership_capability is not None
    ownership = claimed.ownership_capability
    if tail in {
        FaultEventState.INJECTED,
        FaultEventState.DETECTED,
        FaultEventState.CONTAINED,
    }:
        store.append_event(
            "fault-1",
            FaultEventState.INJECTED,
            occurred_at_ms=1_200,
            evidence={"call_id": "call-1"},
            ownership=ownership,
        )
    if tail in {FaultEventState.DETECTED, FaultEventState.CONTAINED}:
        store.append_event(
            "fault-1",
            FaultEventState.DETECTED,
            occurred_at_ms=1_300,
            evidence={"incident_id": "incident-1"},
        )
    if tail is FaultEventState.CONTAINED:
        store.append_event(
            "fault-1",
            FaultEventState.CONTAINED,
            occurred_at_ms=1_400,
            evidence={"containment_id": "containment-1"},
        )

    event = store.relinquish_claim(
        "fault-1",
        occurred_at_ms=occurred_at_ms,
        ownership=ownership,
    )

    assert event.state is expected
    history = store.validate_history("fault-1")
    assert history.valid is True
    assert history.events[-1].state is expected


@pytest.mark.parametrize(
    ("state", "evidence"),
    [
        (FaultEventState.ABANDONED, {"reason": "process-relinquished"}),
        (FaultEventState.EXPIRED, {"reason": "intent-expired"}),
    ],
)
def test_claimed_abandon_and_expiry_require_exact_ownership(
    store: FaultAuthorityStore,
    state: FaultEventState,
    evidence: dict[str, str],
) -> None:
    assert store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100).accepted
    claimed = store.claim_pending(RUNTIME, claimed_at_ms=1_150)
    assert claimed is not None

    with pytest.raises(PermissionError, match="ownership-capability-required"):
        store.append_event(
            "fault-1",
            state,
            occurred_at_ms=1_200,
            evidence=evidence,
        )
    with pytest.raises(PermissionError, match="ownership-capability-required"):
        store.relinquish_claim(
            "fault-1",
            occurred_at_ms=1_200,
            ownership=None,
        )


@pytest.mark.parametrize(
    "terminal_state",
    [FaultEventState.EXPIRED, FaultEventState.ABANDONED, FaultEventState.ESCALATED],
)
def test_claim_requires_an_intact_latest_authorized_chain(
    store: FaultAuthorityStore,
    db_path: Path,
    terminal_state: FaultEventState,
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    store.append_event("fault-1", terminal_state, occurred_at_ms=1_200, evidence={})
    assert store.claim_pending(RUNTIME, claimed_at_ms=1_300) is None

    with sqlite3.connect(db_path) as con:
        disable_append_only(con, "neg_risk_fault_events")
        con.execute(
            "DELETE FROM neg_risk_fault_events WHERE fault_id='fault-1' AND state=?",
            (terminal_state.value,),
        )
        con.execute(
            "UPDATE neg_risk_fault_events SET evidence_json='{}' "
            "WHERE fault_id='fault-1' AND state='authorized'"
        )
    assert store.claim_pending(RUNTIME, claimed_at_ms=1_300) is None


def test_claim_rejects_injection_without_a_claim(
    store: FaultAuthorityStore,
    db_path: Path,
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    claimed = store.claim_pending(RUNTIME, claimed_at_ms=1_150)
    assert claimed is not None
    store.append_event(
        "fault-1",
        FaultEventState.INJECTED,
        occurred_at_ms=1_200,
        evidence={"call_id": "call-1"},
        ownership=claimed.ownership_capability,
    )
    with sqlite3.connect(db_path) as con:
        disable_append_only(con, "neg_risk_fault_events")
        con.execute("DELETE FROM neg_risk_fault_events WHERE fault_id='fault-1' AND state='armed'")
    assert store.claim_pending(RUNTIME, claimed_at_ms=1_300) is None


@pytest.mark.parametrize("fact", ["intent", "nonce", "runtime"])
def test_runtime_nonce_and_intent_hashes_cover_persisted_facts(
    store: FaultAuthorityStore,
    db_path: Path,
    fact: str,
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    if fact == "intent":
        with sqlite3.connect(db_path) as con:
            disable_append_only(con, "neg_risk_fault_intents")
            con.execute(
                "UPDATE neg_risk_fault_intents SET target_key='group-2' WHERE fault_id='fault-1'"
            )
    elif fact == "nonce":
        with sqlite3.connect(db_path) as con:
            disable_append_only(con, "neg_risk_fault_auth_nonces")
            con.execute(
                "UPDATE neg_risk_fault_auth_nonces SET authorization_digest=?",
                ("d" * 64,),
            )
    else:
        with sqlite3.connect(db_path) as con:
            disable_append_only(con, "neg_risk_fault_runtime_starts")
            con.execute("UPDATE neg_risk_fault_runtime_starts SET supervisor_run_id='run-tampered'")
        assert store.current_runtime("candidate") is None
    assert store.claim_pending(RUNTIME, claimed_at_ms=1_200) is None
    assert not store.validate_history("fault-1").valid


@pytest.mark.parametrize(
    "table",
    [
        "neg_risk_fault_runtime_starts",
        "neg_risk_fault_auth_nonces",
        "neg_risk_fault_intents",
        "neg_risk_fault_events",
    ],
)
def test_fault_authority_tables_reject_update_and_delete(
    store: FaultAuthorityStore,
    db_path: Path,
    table: str,
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    with sqlite3.connect(db_path) as con:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            con.execute(f"UPDATE {table} SET rowid=rowid")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            con.execute(f"DELETE FROM {table}")


def test_process_owned_lifecycle_events_require_claim_capability(
    store: FaultAuthorityStore,
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    claimed = store.claim_pending(RUNTIME, claimed_at_ms=1_200)
    assert claimed is not None and claimed.ownership_capability is not None

    with pytest.raises(PermissionError, match="ownership-capability-required"):
        store.append_event(
            "fault-1",
            FaultEventState.INJECTED,
            occurred_at_ms=1_300,
            evidence={"call_id": "call-1"},
        )
    wrong_runtime = replace(
        claimed.ownership_capability,
        runtime=replace(RUNTIME, machine_id="machine-2"),
    )
    with pytest.raises(PermissionError, match="ownership-capability-required"):
        store.append_event(
            "fault-1",
            FaultEventState.INJECTED,
            occurred_at_ms=1_300,
            evidence={"call_id": "call-1"},
            ownership=wrong_runtime,
        )
    injected = store.append_event(
        "fault-1",
        FaultEventState.INJECTED,
        occurred_at_ms=1_300,
        evidence={"call_id": "call-1"},
        ownership=claimed.ownership_capability,
    )
    assert injected.state is FaultEventState.INJECTED
    store.append_event(
        "fault-1",
        FaultEventState.DETECTED,
        occurred_at_ms=1_400,
        evidence={"incident_id": "incident-1"},
    )
    store.append_event(
        "fault-1",
        FaultEventState.CONTAINED,
        occurred_at_ms=1_500,
        evidence={"containment_id": "containment-1"},
    )
    with pytest.raises(PermissionError, match="ownership-capability-required"):
        store.append_event(
            "fault-1",
            FaultEventState.CLEANED,
            occurred_at_ms=1_600,
            evidence={"cleanup_id": "cleanup-1"},
        )
    store.append_event(
        "fault-1",
        FaultEventState.CLEANED,
        occurred_at_ms=1_600,
        evidence={"cleanup_id": "cleanup-1"},
        ownership=claimed.ownership_capability,
    )
    with pytest.raises(PermissionError, match="ownership-capability-required"):
        store.append_event(
            "fault-1",
            FaultEventState.RECOVERED,
            occurred_at_ms=1_700,
            evidence={"recovery_id": "recovery-1"},
        )


@pytest.mark.parametrize(
    "evidence",
    [
        {"authorization": "Bearer secret"},
        {"incident_id": "https://example.test/incident"},
        {"incident_id": "token=secret"},
        {"response_body": "raw"},
    ],
)
def test_evidence_rejects_unknown_or_secret_like_fields(
    store: FaultAuthorityStore,
    evidence: dict[str, object],
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    with pytest.raises(ValueError, match="invalid-evidence"):
        store.append_event(
            "fault-1",
            FaultEventState.DETECTED,
            occurred_at_ms=1_200,
            evidence=evidence,
        )


@pytest.mark.parametrize(
    "supervisor_run_id",
    ["https://supervisor", "token=secret", "x" * 129],
)
def test_runtime_registration_rejects_unsafe_supervisor_run_id(
    db_path: Path,
    supervisor_run_id: str,
) -> None:
    with pytest.raises(ValueError, match="invalid-supervisor-run-id"):
        FaultAuthorityStore(db_path).register_runtime_start(
            RUNTIME,
            supervisor_run_id=supervisor_run_id,
            attempt=1,
            started_at_ms=1_000,
        )


@pytest.mark.parametrize(
    "supervisor_run_id",
    [
        "123456:ABCDEF",
        "header-value",
        "cookie-value",
        "authorization-value",
        "response-body",
        "client-secret",
    ],
)
def test_runtime_registration_rejects_sensitive_supervisor_shapes(
    db_path: Path,
    supervisor_run_id: str,
) -> None:
    with pytest.raises(ValueError, match="invalid-supervisor-run-id"):
        FaultAuthorityStore(db_path).register_runtime_start(
            RUNTIME,
            supervisor_run_id=supervisor_run_id,
            attempt=1,
            started_at_ms=1_000,
        )


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
    claimed = store.claim_pending(RUNTIME, claimed_at_ms=1_200)
    assert claimed is not None
    store.append_event(
        "fault-1",
        FaultEventState.INJECTED,
        occurred_at_ms=1_300,
        evidence={"call_id": "call-1"},
        ownership=claimed.ownership_capability,
    )
    store.append_event(
        "fault-1",
        FaultEventState.DETECTED,
        occurred_at_ms=1_400,
        evidence={"incident_id": "incident-1"},
    )
    store.append_event(
        "fault-1",
        FaultEventState.CONTAINED,
        occurred_at_ms=1_500,
        evidence={"containment_id": "containment-1"},
    )
    store.append_event(
        "fault-1",
        FaultEventState.CLEANED,
        occurred_at_ms=1_600,
        evidence={"cleanup_id": "cleanup-1"},
        ownership=claimed.ownership_capability,
    )
    store.append_event(
        "fault-1",
        FaultEventState.RECOVERED,
        occurred_at_ms=1_700,
        evidence={"recovery_id": "recovery-1"},
        ownership=claimed.ownership_capability,
    )
    assert store.validate_history("fault-1").valid
    with sqlite3.connect(db_path) as con:
        disable_append_only(con, "neg_risk_fault_events")
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
            con.row_factory = sqlite3.Row
            source = dict(
                con.execute(
                    "SELECT * FROM neg_risk_fault_intents WHERE fault_id='fault-1'"
                ).fetchone()
            )
            source["fault_id"] = "fault-2"
            source["intent_hash"] = _intent_hash(source)
            con.execute(
                "INSERT INTO neg_risk_fault_intents("
                "fault_id,kind,call_class,target_key,parameters_json,parameter_digest,"
                "ttl_ms,component,release_id,machine_id,boot_id,nonce_digest,"
                "authorization_digest,accepted_at_ms,status,rejection_reason,intent_hash)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuple(
                    source[key]
                    for key in (
                        "fault_id",
                        "kind",
                        "call_class",
                        "target_key",
                        "parameters_json",
                        "parameter_digest",
                        "ttl_ms",
                        "component",
                        "release_id",
                        "machine_id",
                        "boot_id",
                        "nonce_digest",
                        "authorization_digest",
                        "accepted_at_ms",
                        "status",
                        "rejection_reason",
                        "intent_hash",
                    )
                ),
            )
        store.append_event(
            "fault-2",
            FaultEventState.AUTHORIZED,
            occurred_at_ms=1_100,
            evidence={"reason": "accepted"},
        )
    else:
        claimed = store.claim_pending(RUNTIME, claimed_at_ms=1_200)
        assert claimed is not None
        if mutation == "missing-predecessor":
            with sqlite3.connect(db_path) as con:
                disable_append_only(con, "neg_risk_fault_events")
                con.execute(
                    "UPDATE neg_risk_fault_events SET previous_hash=? "
                    "WHERE fault_id=? AND state='armed'",
                    ("0" * 64, "fault-1"),
                )
        elif mutation == "regression":
            store.append_event(
                "fault-1",
                FaultEventState.INJECTED,
                occurred_at_ms=1_300,
                evidence={"call_id": "call-1"},
                ownership=claimed.ownership_capability,
            )
            with sqlite3.connect(db_path) as con:
                disable_append_only(con, "neg_risk_fault_events")
                con.execute(
                    "UPDATE neg_risk_fault_events SET state='authorized' "
                    "WHERE fault_id='fault-1' AND state='injected'"
                )
        elif mutation == "injection-without-claim":
            store.append_event(
                "fault-1",
                FaultEventState.INJECTED,
                occurred_at_ms=1_300,
                evidence={"call_id": "call-1"},
                ownership=claimed.ownership_capability,
            )
            with sqlite3.connect(db_path) as con:
                disable_append_only(con, "neg_risk_fault_events")
                con.execute(
                    "DELETE FROM neg_risk_fault_events WHERE fault_id='fault-1' AND state='armed'"
                )
        else:
            store.append_event(
                "fault-1",
                FaultEventState.CLEANED,
                occurred_at_ms=1_300,
                evidence={"cleanup_id": "cleanup-1"},
                ownership=claimed.ownership_capability,
            )
    assert not store.project_fault("fault-1", now_ms=2_000).available


def test_projection_rejects_any_corrupt_current_runtime_accepted_chain(
    store: FaultAuthorityStore,
    db_path: Path,
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        source = dict(
            con.execute("SELECT * FROM neg_risk_fault_intents WHERE fault_id='fault-1'").fetchone()
        )
        source["fault_id"] = "fault-corrupt"
        source["intent_hash"] = "0" * 64
        con.execute(
            "INSERT INTO neg_risk_fault_intents("
            "fault_id,kind,call_class,target_key,parameters_json,parameter_digest,"
            "ttl_ms,component,release_id,machine_id,boot_id,nonce_digest,"
            "authorization_digest,accepted_at_ms,status,rejection_reason,intent_hash)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(
                source[key]
                for key in (
                    "fault_id",
                    "kind",
                    "call_class",
                    "target_key",
                    "parameters_json",
                    "parameter_digest",
                    "ttl_ms",
                    "component",
                    "release_id",
                    "machine_id",
                    "boot_id",
                    "nonce_digest",
                    "authorization_digest",
                    "accepted_at_ms",
                    "status",
                    "rejection_reason",
                    "intent_hash",
                )
            ),
        )
    projection = store.project_fault("fault-1", now_ms=1_200)
    assert not projection.available
    assert projection.state is FaultEventState.EVIDENCE_INVALID
    assert projection.reason == "evidence-invalid"


@pytest.mark.parametrize("state", [None, FaultEventState.INJECTED])
def test_stale_active_chain_projects_abandoned_and_is_never_claimable(
    store: FaultAuthorityStore, state: FaultEventState | None
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    if state is not None:
        claimed = store.claim_pending(RUNTIME, claimed_at_ms=1_200)
        assert claimed is not None
        store.append_event(
            "fault-1",
            state,
            occurred_at_ms=1_300,
            evidence={"call_id": "call-1"},
            ownership=claimed.ownership_capability,
        )
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
