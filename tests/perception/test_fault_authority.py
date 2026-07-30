from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

import scripts.perception_fault_acceptance as fault_acceptance
import scripts.perception_fault_readonly as fault_readonly
from polyarb.perception.fault_authority import (
    FaultAuthorityStore,
    _intent_hash,
    _nonce_hash,
)
from polyarb.perception.fault_control import (
    FaultAuthorization,
    FaultCallClass,
    FaultEventAction,
    FaultEventState,
    FaultIntentRequest,
    FaultKind,
    FaultRuntimeIdentity,
    fault_call_binding_digest,
)
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.storage.schemas import DDL
from polyarb.storage.sqlite_store import SQLiteStore

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


def rehash_auth_row(con: sqlite3.Connection, row_id: int) -> None:
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT * FROM neg_risk_fault_auth_nonces WHERE id=?", (row_id,)
    ).fetchone()
    assert row is not None
    digest = _nonce_hash(
        record_type=row["record_type"],
        nonce_digest=row["nonce_digest"],
        authorization_digest=row["authorization_digest"],
        operation=row["operation"],
        fault_id=row["fault_id"],
        request_digest=row["request_digest"],
        outcome=row["outcome"],
        reason=row["reason"],
        occurred_at_ms=row["occurred_at_ms"],
        reservation_id=row["reservation_id"],
    )
    con.execute(
        "UPDATE neg_risk_fault_auth_nonces SET row_hash=? WHERE id=?",
        (digest, row_id),
    )


def insert_cloned_intent(
    con: sqlite3.Connection,
    source: dict[str, object],
    *,
    fault_id: str,
    corrupt_intent_hash: bool = False,
) -> None:
    nonce_digest = hashlib.sha256(f"{fault_id}:nonce".encode()).hexdigest()
    authorization_digest = hashlib.sha256(f"{fault_id}:auth".encode()).hexdigest()
    request_digest = hashlib.sha256(f"{fault_id}:request".encode()).hexdigest()
    reservation_fields = {
        "record_type": "reservation",
        "nonce_digest": nonce_digest,
        "authorization_digest": authorization_digest,
        "operation": "arm",
        "fault_id": fault_id,
        "request_digest": request_digest,
        "outcome": None,
        "reason": None,
        "occurred_at_ms": source["accepted_at_ms"],
        "reservation_id": None,
    }
    reservation_id = con.execute(
        "INSERT INTO neg_risk_fault_auth_nonces("
        "record_type,nonce_digest,authorization_digest,operation,fault_id,"
        "request_digest,outcome,reason,occurred_at_ms,reservation_id,row_hash)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (*reservation_fields.values(), _nonce_hash(**reservation_fields)),
    ).lastrowid
    attempt_fields = {
        **reservation_fields,
        "record_type": "attempt",
        "outcome": "accepted",
        "reason": "accepted",
        "reservation_id": reservation_id,
    }
    attempt_id = con.execute(
        "INSERT INTO neg_risk_fault_auth_nonces("
        "record_type,nonce_digest,authorization_digest,operation,fault_id,"
        "request_digest,outcome,reason,occurred_at_ms,reservation_id,row_hash)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (*attempt_fields.values(), _nonce_hash(**attempt_fields)),
    ).lastrowid
    source.update(
        {
            "fault_id": fault_id,
            "nonce_digest": nonce_digest,
            "authorization_digest": authorization_digest,
            "request_digest": request_digest,
            "auth_reservation_id": reservation_id,
            "auth_attempt_id": attempt_id,
        }
    )
    source["intent_hash"] = (
        "0" * 64 if corrupt_intent_hash else _intent_hash(source)
    )
    columns = (
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
        "request_digest",
        "auth_reservation_id",
        "auth_attempt_id",
        "accepted_at_ms",
        "status",
        "rejection_reason",
        "intent_hash",
    )
    con.execute(
        f"INSERT INTO neg_risk_fault_intents({','.join(columns)}) "
        f"VALUES ({','.join('?' for _ in columns)})",
        tuple(source[column] for column in columns),
    )


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


def test_cleanup_request_is_action_only_hash_chained_and_idempotent(
    store: FaultAuthorityStore, db_path: Path
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    cleanup_auth = auth("d")

    first = store.request_cleanup(
        "fault-1", auth=cleanup_auth, requested_at_ms=1_200
    )
    second = store.request_cleanup(
        "fault-1", auth=auth("e"), requested_at_ms=1_201
    )

    assert first == second
    assert first.state is None
    assert first.action is FaultEventAction.CLEANUP_REQUESTED
    history = store.validate_history("fault-1")
    assert history.valid
    assert [event.state for event in history.events] == [
        FaultEventState.AUTHORIZED,
        None,
    ]
    projection = store.project_fault("fault-1", now_ms=1_201)
    assert projection.available and projection.active
    assert projection.state is FaultEventState.AUTHORIZED
    with sqlite3.connect(db_path) as con:
        row = con.execute(
            "SELECT state,action,evidence_json FROM neg_risk_fault_events "
            "WHERE action='cleanup-requested'"
        ).fetchone()
        attempts = con.execute(
            "SELECT outcome,reason FROM neg_risk_fault_auth_nonces "
            "WHERE record_type='attempt' ORDER BY id"
        ).fetchall()
        assert row[0:2] == (None, "cleanup-requested")
        action_evidence = json.loads(row[2])
        assert action_evidence["authorization_digest"] == "c" * 64
        assert action_evidence["nonce_digest"] == "d" * 64
        assert len(action_evidence["request_digest"]) == 64
        assert action_evidence["reservation_id"] > 0
        assert action_evidence["attempt_id"] > 0
    assert attempts == [
        ("accepted", "accepted"),
        ("accepted", "cleanup-requested"),
        ("accepted", "cleanup-already-requested"),
    ]


def test_cleanup_request_replay_cannot_target_another_fault(
    store: FaultAuthorityStore,
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    cleanup_auth = auth("d")
    store.request_cleanup("fault-1", auth=cleanup_auth, requested_at_ms=1_200)
    with pytest.raises(ValueError, match="nonce-replay"):
        store.request_cleanup(
            "other-fault", auth=cleanup_auth, requested_at_ms=1_200
        )


def test_cleanup_action_does_not_block_lifecycle_claim(
    store: FaultAuthorityStore,
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    store.request_cleanup("fault-1", auth=auth("d"), requested_at_ms=1_150)

    claimed = store.claim_pending(RUNTIME, claimed_at_ms=1_200)

    assert claimed is not None
    assert store.validate_history("fault-1").valid


def test_replay_runtime_mismatch_stale_runtime_and_second_active_reject(
    store: FaultAuthorityStore,
    db_path: Path,
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
    with sqlite3.connect(db_path) as con:
        assert con.execute("SELECT count(*) FROM neg_risk_fault_intents").fetchone() == (1,)
        assert con.execute("SELECT count(*) FROM neg_risk_fault_events").fetchone() == (1,)
        attempts = con.execute(
            "SELECT outcome,reason FROM neg_risk_fault_auth_nonces "
            "WHERE record_type='attempt' ORDER BY id"
        ).fetchall()
    assert attempts == [
        ("accepted", "accepted"),
        ("rejected", "nonce-replay"),
        ("rejected", "fault-already-active"),
        ("rejected", "runtime-mismatch"),
        ("rejected", "runtime-mismatch"),
    ]


def test_expired_deadline_rolls_back_nonce_intent_and_event(
    store: FaultAuthorityStore,
    db_path: Path,
) -> None:
    with pytest.raises(TimeoutError, match="fault-authority-deadline"):
        store.accept_intent(
            request(),
            auth=auth(),
            accepted_at_ms=1_100,
            deadline_monotonic=time.monotonic() - 1,
        )
    with sqlite3.connect(db_path) as con:
        assert con.execute("SELECT count(*) FROM neg_risk_fault_auth_nonces").fetchone() == (0,)
        assert con.execute("SELECT count(*) FROM neg_risk_fault_intents").fetchone() == (0,)
        assert con.execute("SELECT count(*) FROM neg_risk_fault_events").fetchone() == (0,)


def test_locked_write_settles_by_deadline_without_later_commit(
    store: FaultAuthorityStore,
    db_path: Path,
) -> None:
    lock = sqlite3.connect(db_path, isolation_level=None)
    lock.execute("BEGIN IMMEDIATE")
    finished = threading.Event()
    errors: list[BaseException] = []

    def attempt() -> None:
        try:
            store.accept_intent(
                request(),
                auth=auth(),
                accepted_at_ms=1_100,
                deadline_monotonic=time.monotonic() + 0.05,
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    worker = threading.Thread(target=attempt)
    worker.start()
    assert finished.wait(timeout=1)
    lock.execute("ROLLBACK")
    lock.close()
    worker.join(timeout=1)

    assert len(errors) == 1
    assert isinstance(errors[0], TimeoutError)
    with sqlite3.connect(db_path) as con:
        assert con.execute("SELECT count(*) FROM neg_risk_fault_auth_nonces").fetchone() == (0,)
        assert con.execute("SELECT count(*) FROM neg_risk_fault_intents").fetchone() == (0,)
        assert con.execute("SELECT count(*) FROM neg_risk_fault_events").fetchone() == (0,)


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
    ("column", "value"),
    [
        ("operation", "cleanup"),
        ("fault_id", "fault-other"),
        ("request_digest", "9" * 64),
        ("authorization_digest", "8" * 64),
    ],
)
def test_arm_attempt_must_exactly_match_its_reservation_and_intent(
    store: FaultAuthorityStore,
    db_path: Path,
    column: str,
    value: str,
) -> None:
    store.accept_intent(
        request(),
        auth=auth(),
        accepted_at_ms=1_100,
        request_digest="7" * 64,
    )
    with sqlite3.connect(db_path) as con:
        disable_append_only(con, "neg_risk_fault_auth_nonces")
        attempt_id = con.execute(
            "SELECT id FROM neg_risk_fault_auth_nonces "
            "WHERE record_type='attempt' AND outcome='accepted'"
        ).fetchone()[0]
        con.execute(
            f"UPDATE neg_risk_fault_auth_nonces SET {column}=? WHERE id=?",
            (value, attempt_id),
        )
        rehash_auth_row(con, attempt_id)

    snapshot = store.read_snapshot(now_ms=1_200, fault_id="fault-1")
    assert not snapshot.available
    assert not store.validate_history("fault-1").valid


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("operation", "arm"),
        ("fault_id", "fault-other"),
        ("request_digest", "9" * 64),
        ("authorization_digest", "8" * 64),
    ],
)
def test_cleanup_action_must_exactly_match_accepted_attempt_and_reservation(
    store: FaultAuthorityStore,
    db_path: Path,
    column: str,
    value: str,
) -> None:
    store.accept_intent(
        request(),
        auth=auth(),
        accepted_at_ms=1_100,
        request_digest="7" * 64,
    )
    store.request_cleanup(
        "fault-1",
        auth=auth("d"),
        requested_at_ms=1_200,
        request_digest="6" * 64,
    )
    with sqlite3.connect(db_path) as con:
        disable_append_only(con, "neg_risk_fault_auth_nonces")
        attempt_id = con.execute(
            "SELECT id FROM neg_risk_fault_auth_nonces "
            "WHERE record_type='attempt' AND outcome='accepted' "
            "AND reason='cleanup-requested'"
        ).fetchone()[0]
        con.execute(
            f"UPDATE neg_risk_fault_auth_nonces SET {column}=? WHERE id=?",
            (value, attempt_id),
        )
        rehash_auth_row(con, attempt_id)

    snapshot = store.read_snapshot(now_ms=1_300, fault_id="fault-1")
    assert not snapshot.available
    assert not store.validate_history("fault-1").valid


def test_rejected_nonce_replay_may_differ_only_as_explicit_replay(
    store: FaultAuthorityStore,
) -> None:
    store.accept_intent(
        request(),
        auth=auth(),
        accepted_at_ms=1_100,
        request_digest="7" * 64,
    )
    replay = store.accept_intent(
        request(fault_id="fault-replay"),
        auth=auth(),
        accepted_at_ms=1_101,
        request_digest="6" * 64,
    )
    assert not replay.accepted and replay.reason == "nonce-replay"
    assert store.read_snapshot(now_ms=1_200, fault_id="fault-1").available


def test_snapshot_queries_only_current_candidates_and_exact_auth_links(
    store: FaultAuthorityStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for index in range(40):
        fault_id = f"historical-{index}"
        digest = hashlib.sha256(f"nonce-{index}".encode()).hexdigest()
        authorization = hashlib.sha256(f"auth-{index}".encode()).hexdigest()
        admission = store.accept_intent(
            request(fault_id=fault_id),
            auth=FaultAuthorization(digest, authorization),
            accepted_at_ms=1_100 + index * 2,
            request_digest=hashlib.sha256(f"request-{index}".encode()).hexdigest(),
        )
        assert admission.accepted
        store.append_event(
            fault_id,
            FaultEventState.ABANDONED,
            occurred_at_ms=1_101 + index * 2,
            evidence={},
        )
    admission = store.accept_intent(
        request(),
        auth=auth(),
        accepted_at_ms=1_300,
        request_digest="7" * 64,
    )
    assert admission.accepted

    statements: list[str] = []
    original_connect = store._connect

    def traced_connect(deadline_monotonic=None):
        con = original_connect(deadline_monotonic)
        con.set_trace_callback(statements.append)
        return con

    monkeypatch.setattr(store, "_connect", traced_connect)
    snapshot = store.read_snapshot(
        now_ms=1_400,
        fault_id="fault-1",
        deadline_monotonic=time.monotonic() + 0.2,
    )

    assert snapshot.available
    assert not any(
        "neg_risk_fault_auth_nonces order by id" in statement.lower()
        for statement in statements
    )
    exact_history_reads = [
        statement
        for statement in statements
        if "select * from neg_risk_fault_intents where fault_id=" in statement.lower()
    ]
    assert len(exact_history_reads) <= 2


def test_fault_auth_indexes_cover_scoped_link_validation(db_path: Path) -> None:
    with sqlite3.connect(db_path) as con:
        indexes = {
            row[1] for row in con.execute(
                "PRAGMA index_list('neg_risk_fault_auth_nonces')"
            )
        }
        assert {
            "idx_neg_risk_fault_auth_reservation_attempt",
            "idx_neg_risk_fault_auth_fault_operation",
        } <= indexes
        plan = " ".join(
            str(column)
            for row in con.execute(
                "EXPLAIN QUERY PLAN SELECT id FROM neg_risk_fault_auth_nonces "
                "WHERE fault_id=? AND operation=? AND record_type=?",
                ("fault-1", "cleanup", "attempt"),
            )
            for column in row
        )
    assert "idx_neg_risk_fault_auth_fault_operation" in plan


def test_active_candidate_query_is_runtime_indexed_without_global_sort(
    store: FaultAuthorityStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store.accept_intent(
        request(),
        auth=auth(),
        accepted_at_ms=1_100,
        request_digest="7" * 64,
    )
    statements: list[str] = []
    original_connect = store._connect

    def traced_connect(deadline_monotonic=None):
        con = original_connect(deadline_monotonic)
        con.set_trace_callback(statements.append)
        return con

    monkeypatch.setattr(store, "_connect", traced_connect)
    assert store.read_snapshot(now_ms=1_200, fault_id="fault-1").available
    active_statements = [
        statement
        for statement in statements
        if "neg_risk_fault_intents" in statement
        and "accepted_at_ms" in statement
        and "limit" in statement.lower()
    ]
    assert active_statements
    assert all(
        "join neg_risk_fault_runtime_starts" not in statement.lower()
        and "component=" in statement.lower()
        and "release_id=" in statement.lower()
        and "machine_id=" in statement.lower()
        and "boot_id=" in statement.lower()
        and "not in" in statement.lower()
        for statement in active_statements
    )

    terminal_values = (
        "verified",
        "rejected",
        "expired",
        "abandoned",
        "cleanup-failed",
        "recovery-timeout",
        "evidence-invalid",
        "escalated",
    )
    placeholders = ",".join("?" for _ in terminal_values)
    with sqlite3.connect(store._db_path) as con:
        plan = " ".join(
            str(column)
            for row in con.execute(
                "EXPLAIN QUERY PLAN "
                "SELECT i.fault_id FROM neg_risk_fault_intents i "
                "WHERE i.component=? AND i.release_id=? "
                "AND i.machine_id=? AND i.boot_id=? AND i.status='accepted' "
                "AND COALESCE(("
                " SELECT e.state FROM neg_risk_fault_events e "
                " WHERE e.fault_id=i.fault_id AND e.state IS NOT NULL "
                " ORDER BY e.sequence DESC LIMIT 1"
                "), '') NOT IN (" + placeholders + ") "
                "ORDER BY i.accepted_at_ms DESC,i.fault_id DESC LIMIT 2",
                (
                    RUNTIME.component,
                    RUNTIME.release_id,
                    RUNTIME.machine_id,
                    str(RUNTIME.boot_id),
                    *terminal_values,
                ),
            )
            for column in row
        )
    assert "idx_neg_risk_fault_intent_active_runtime" in plan
    assert "SCAN neg_risk_fault_intents" not in plan
    assert "USE TEMP B-TREE" not in plan


@pytest.mark.parametrize("terminal_count", [1, 4])
def test_terminal_intents_cannot_hide_an_older_second_active_chain(
    store: FaultAuthorityStore,
    db_path: Path,
    terminal_count: int,
) -> None:
    store.accept_intent(
        request(),
        auth=auth(),
        accepted_at_ms=1_100,
        request_digest="7" * 64,
    )
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        base = dict(
            con.execute(
                "SELECT * FROM neg_risk_fault_intents WHERE fault_id='fault-1'"
            ).fetchone()
        )
        for index in range(terminal_count):
            source = dict(base)
            source["accepted_at_ms"] = 1_200 + index * 10
            insert_cloned_intent(
                con,
                source,
                fault_id=f"fault-terminal-{index}",
            )
        newest = dict(base)
        newest["accepted_at_ms"] = 1_300 + terminal_count * 10
        insert_cloned_intent(con, newest, fault_id="fault-newest-active")

    for index in range(terminal_count):
        fault_id = f"fault-terminal-{index}"
        occurred_at_ms = 1_200 + index * 10
        store.append_event(
            fault_id,
            FaultEventState.AUTHORIZED,
            occurred_at_ms=occurred_at_ms,
            evidence={"reason": "accepted"},
        )
        store.append_event(
            fault_id,
            FaultEventState.ABANDONED,
            occurred_at_ms=occurred_at_ms + 1,
            evidence={},
        )
    store.append_event(
        "fault-newest-active",
        FaultEventState.AUTHORIZED,
        occurred_at_ms=1_300 + terminal_count * 10,
        evidence={"reason": "accepted"},
    )

    snapshot = store.read_snapshot(now_ms=2_000, fault_id="fault-1")
    assert not snapshot.available
    assert snapshot.projection is not None
    assert snapshot.projection.reason == "multiple-active-chains"


def test_existing_cleanup_action_history_inherits_absolute_deadline(
    store: FaultAuthorityStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    store.request_cleanup("fault-1", auth=auth("d"), requested_at_ms=1_200)
    inherited: list[float | None] = []
    original = store._validate_history_in_connection

    def observed(con, fault_id, *, deadline_monotonic=None):
        inherited.append(deadline_monotonic)
        return original(
            con,
            fault_id,
            deadline_monotonic=deadline_monotonic,
        )

    monkeypatch.setattr(store, "_validate_history_in_connection", observed)
    deadline = time.monotonic() + 0.2
    store.request_cleanup(
        "fault-1",
        auth=auth("e"),
        requested_at_ms=1_201,
        deadline_monotonic=deadline,
    )
    assert inherited == [deadline]


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


def test_control_finalizer_requires_exact_recovered_chain_and_is_idempotent(
    store: FaultAuthorityStore, db_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    perception_store = OpportunityPerceptionStore(db_path)
    perception_store.init_schema()
    group = GroupRevision.certified(
        group_id="group-1",
        event_id="event-1",
        revision=1,
        started_at_ms=900,
        observed_at_ms=1_000,
        source_cursor="cursor-1",
        legs=(
            GroupLeg("market-1", "condition-1", "token-1", "one"),
            GroupLeg("market-2", "condition-2", "token-2", "two"),
        ),
    )
    perception_store.publish_group_revision(group)
    store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    claimed = store.claim_pending(RUNTIME, claimed_at_ms=1_200)
    assert claimed is not None
    ownership = claimed.ownership_capability
    store.append_event(
        "fault-1", FaultEventState.INJECTED, occurred_at_ms=1_300,
        evidence={
            "call_id": "call-1",
            "call_binding_digest": fault_call_binding_digest(
                fault_id="fault-1",
                kind=FaultKind.CLOB_LATENCY.value,
                call_class=FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH.value,
                target_key="group-1",
                runtime={
                    "component": RUNTIME.component,
                    "release_id": RUNTIME.release_id,
                    "machine_id": RUNTIME.machine_id,
                    "boot_id": str(RUNTIME.boot_id),
                },
                call_id="call-1",
            ),
        },
        ownership=ownership,
    )
    incident_clock = [1_400]
    manager = IncidentManager(
        perception_store,
        clock_ms=lambda: incident_clock[0],
    )
    incident = manager.detect("candidate:group-1", "clob-latency", {})
    store.append_event(
        "fault-1", FaultEventState.DETECTED, occurred_at_ms=1_400,
        evidence={"incident_id": incident.id},
    )
    incident_clock[0] = 1_450
    manager.transition(incident.id, "classified", {})
    incident_clock[0] = 1_500
    manager.transition(incident.id, "contained", {})
    incident_clock[0] = 1_550
    manager.transition(incident.id, "recovering", {})
    store.append_event(
        "fault-1", FaultEventState.CONTAINED, occurred_at_ms=1_500,
        evidence={"containment_id": "containment-1"},
    )
    store.append_event(
        "fault-1", FaultEventState.CLEANED, occurred_at_ms=1_600,
        evidence={"cleanup_id": "cleanup-1"}, ownership=ownership,
    )
    quote = GroupQuoteBatch.complete(
        group_id=group.group_id,
        membership_hash=group.membership_hash,
        quote_batch_id="quote-recovery-1",
        started_at_ms=1_610,
        quoted_at_ms=1_650,
        legs=tuple(
            GroupQuoteLeg(
                leg.yes_token_id,
                group.membership_hash,
                0.4,
                10,
                "executable",
            )
            for leg in group.legs
        ),
    )
    perception_store.publish_candidate_success(
        quote,
        observed_at_ms=1_650,
        last_result="watching",
        reason=None,
        bundle_cost=0.8,
        gross_edge_bps=2_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="recovered",
        next_due_at_ms=20_000,
    )
    incident_clock[0] = 1_660
    manager.transition(
        incident.id,
        "verified",
        {
            "quote_batch_id": quote.quote_batch_id,
            "group_id": group.group_id,
            "membership_hash": group.membership_hash,
        },
    )
    recovered = store.append_event(
        "fault-1", FaultEventState.RECOVERED, occurred_at_ms=1_700,
        evidence={"recovery_id": "candidate-success-1"}, ownership=ownership,
    )
    with sqlite3.connect(db_path) as con:
        before_export = con.total_changes
        row_counts = tuple(
            con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "neg_risk_fault_runtime_starts",
                "neg_risk_fault_auth_nonces",
                "neg_risk_fault_intents",
                "neg_risk_fault_events",
            )
        )
    envelope = fault_readonly.export_fault_envelope(
        db_path,
        "fault-1",
        now_ms=1_750,
    )
    exported_verdict = fault_acceptance.evaluate_fault_envelope(
        envelope, mode="candidate"
    )
    assert exported_verdict.reasons == ()
    monkeypatch.setenv(
        "POLYARB_UPSTREAM_FAULT_EVALUATOR_PRIVATE_KEY",
        "ed25519-v1:test-key:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE",
    )
    monkeypatch.setenv(
        "POLYARB_UPSTREAM_FAULT_EVALUATOR_PUBLIC_KEY",
        "ed25519-v1:test-key:iojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1w",
    )
    artifact = fault_acceptance.build_candidate_artifact(envelope)
    with sqlite3.connect(db_path) as con:
        assert con.total_changes == before_export
        assert tuple(
            con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "neg_risk_fault_runtime_starts",
                "neg_risk_fault_auth_nonces",
                "neg_risk_fault_intents",
                "neg_risk_fault_events",
            )
        ) == row_counts

    verified = store.finalize_verdict(
        "fault-1",
        verdict_id=str(artifact["verdict_id"]),
        verdict_digest=str(artifact["artifact_digest"]),
        source_tail_hash=recovered.event_hash,
        runtime=RUNTIME,
        auth=auth("e"),
        request_digest="f" * 64,
        occurred_at_ms=1_800,
    )

    assert verified.state is FaultEventState.VERIFIED
    assert verified.evidence == {
        "verdict_id": artifact["verdict_id"],
        "verdict_digest": artifact["artifact_digest"],
    }
    final_envelope = fault_readonly.export_fault_envelope(
        db_path,
        "fault-1",
        now_ms=1_850,
    )
    assert fault_acceptance.evaluate_fault_envelope(
        final_envelope, mode="final", candidate_artifact=artifact
    ).reasons == ()
    same = store.finalize_verdict(
        "fault-1",
        verdict_id=str(artifact["verdict_id"]),
        verdict_digest=str(artifact["artifact_digest"]),
        source_tail_hash=recovered.event_hash,
        runtime=RUNTIME,
        auth=auth("f"),
        request_digest="a" * 64,
        occurred_at_ms=1_900,
    )
    assert same == verified
    with pytest.raises(ValueError, match="verdict-conflict"):
        store.finalize_verdict(
            "fault-1",
            verdict_id="verdict-other",
            verdict_digest="0" * 64,
            source_tail_hash=recovered.event_hash,
            runtime=RUNTIME,
            auth=auth("1"),
            request_digest="2" * 64,
            occurred_at_ms=2_000,
        )
    IncidentManager(
        perception_store,
        clock_ms=lambda: 2_100,
    ).detect("candidate:other", "clob-429", {})
    dirty = fault_readonly.export_fault_envelope(
        db_path,
        "fault-1",
        now_ms=2_200,
    )
    assert dirty["open_incident_count"] == 1
    assert "open-incident" in fault_acceptance.evaluate_fault_envelope(
        dirty, mode="final", candidate_artifact=artifact
    ).reasons
    with sqlite3.connect(db_path) as con:
        con.execute("DELETE FROM neg_risk_candidate_success_receipts WHERE id=1")
    with pytest.raises(ValueError, match="fault-recovery-source-missing"):
        fault_readonly.export_fault_envelope(
            db_path,
            "fault-1",
            now_ms=2_300,
        )


def test_task3_auth_schema_upgrades_without_changing_existing_audit_hashes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "old-task3.db"
    old_ddl = DDL.replace(
        "operation TEXT NOT NULL CHECK(operation IN ('arm','cleanup','finalize'))",
        "operation TEXT NOT NULL CHECK(operation IN ('arm','cleanup'))",
    )
    with sqlite3.connect(db_path) as con:
        con.executescript(old_ddl)
    old_store = FaultAuthorityStore(db_path)
    old_store.register_runtime_start(
        RUNTIME, supervisor_run_id="run-1", attempt=1, started_at_ms=1_000
    )
    old_store.accept_intent(request(), auth=auth(), accepted_at_ms=1_100)
    before = old_store.validate_history("fault-1")
    assert before.valid
    with sqlite3.connect(db_path) as con:
        audit_before = con.execute(
            "SELECT id,row_hash FROM neg_risk_fault_auth_nonces ORDER BY id"
        ).fetchall()

    SQLiteStore(db_path).init_schema()

    after = FaultAuthorityStore(db_path).validate_history("fault-1")
    assert after.valid
    assert after.events == before.events
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT id,row_hash FROM neg_risk_fault_auth_nonces ORDER BY id"
        ).fetchall() == audit_before
        table_sql = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='neg_risk_fault_auth_nonces'"
        ).fetchone()[0]
        assert con.execute("PRAGMA foreign_key_check").fetchall() == []
        intent_fk_targets = {
            row[2] for row in con.execute("PRAGMA foreign_key_list(neg_risk_fault_intents)")
        }
    assert "'finalize'" in table_sql
    assert intent_fk_targets == {"neg_risk_fault_auth_nonces"}


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
            insert_cloned_intent(con, source, fault_id="fault-2")
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
        insert_cloned_intent(
            con,
            source,
            fault_id="fault-corrupt",
            corrupt_intent_hash=True,
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
