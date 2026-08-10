from __future__ import annotations

import sqlite3

from polyarb.perception.incidents import IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.perception.supervisor import ProducerSupervisor
from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult


def _manager(tmp_path):
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    return IncidentManager(store, clock_ms=lambda: 1_000)


def test_timeout_creates_recovering_quote_incident_with_disposition(tmp_path) -> None:
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle

    incident = QuoteIncidentLifecycle(_manager(tmp_path)).record_timeout(
        run_id=1908,
        requested_token_count=38_972,
        deadline_s=120,
        consecutive_failures=3,
        last_success_age_s=3_057.8,
    )

    assert (incident.scope, incident.kind, incident.state) == (
        "quote-collection",
        "quote-collection-timeout",
        "recovering",
    )
    assert incident.evidence["automatic_action"] == "retry-immediately"
    assert incident.evidence["next_action"] == "inspect-clob-and-child-io"
    assert incident.evidence["impact"] == "feed-unavailable"


def test_hard_timeout_incident_exposes_stage_and_bounded_budget_disposition(tmp_path) -> None:
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle

    incident = QuoteIncidentLifecycle(_manager(tmp_path)).record_timeout(
        attempt_id=484,
        run_id=2275,
        requested_token_count=38_972,
        deadline_s=180,
        consecutive_failures=4,
        last_success_age_s=735.5,
        failure_kind="child-hard-timeout",
        attempt_phase="failed",
        phase_timings={"admission_ms": 15_597, "universe_ms": 9_921},
    )

    assert incident.evidence["failure_kind"] == "child-hard-timeout"
    assert incident.evidence["attempt_phase"] == "failed"
    assert incident.evidence["phase_timings"] == {
        "admission_ms": 15_597,
        "universe_ms": 9_921,
    }
    assert incident.evidence["next_action"] == (
        "inspect-stage-checkpoint-and-rebalance-child-budget"
    )


def test_repeated_timeout_reuses_the_open_incident(tmp_path) -> None:
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle

    lifecycle = QuoteIncidentLifecycle(_manager(tmp_path))
    first = lifecycle.record_timeout(
        run_id=1908, requested_token_count=10, deadline_s=120,
        consecutive_failures=1, last_success_age_s=20.0,
    )
    second = lifecycle.record_timeout(
        run_id=1909, requested_token_count=10, deadline_s=120,
        consecutive_failures=2, last_success_age_s=320.0,
    )

    assert first.id == second.id
    assert second.sequence > first.sequence


def test_unavailable_quote_timeout_is_a_p1_incident(tmp_path) -> None:
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle

    incident = QuoteIncidentLifecycle(_manager(tmp_path)).record_timeout(
        run_id=1908,
        requested_token_count=38_972,
        deadline_s=120,
        consecutive_failures=3,
        last_success_age_s=301.0,
    )

    assert incident.evidence["severity"] == "p1"
    assert incident.evidence["reminder_interval_s"] == 300


def test_non_timeout_child_failure_creates_operator_incident(tmp_path) -> None:
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        QuoteWorkerRuntime,
    )

    runtime = QuoteWorkerRuntime()
    runtime.mark_failure(QuoteCollectionSubprocessError("failed"))
    incident = QuoteIncidentLifecycle(_manager(tmp_path)).record_failure(
        error=QuoteCollectionSubprocessError(
            "failed", diagnostic="QuoteUniverseUnavailableError"
        ),
        runtime=runtime,
    )

    assert (incident.kind, incident.state) == ("quote-collection-failure", "recovering")
    assert incident.evidence["next_action"] == "inspect-child-stderr"
    assert incident.evidence["failure_reason"] == "quote-collection-subprocess-failed"


def test_certified_quote_run_verifies_open_timeout_incident(tmp_path) -> None:
    """A retry is not recovery: only a complete post-incident run closes it."""
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle

    now = [1_000]
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    lifecycle = QuoteIncidentLifecycle(IncidentManager(store, clock_ms=lambda: now[0]))
    lifecycle.record_timeout(
        run_id=17,
        requested_token_count=2,
        deadline_s=120,
        consecutive_failures=1,
        last_success_age_s=None,
    )

    # This fixture is deliberately written at the storage boundary.  The
    # IncidentManager proof queries this durable quote-run record, rather than
    # trusting the worker's in-memory success callback.
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,is_valid,parquet_path) VALUES(?,?,?,?,?,?,?,?)",
            (900, 901, "subset", 2, 1, "structure", 1, "fixture.parquet"),
        )
        snapshot_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.execute(
            "INSERT INTO neg_risk_quote_runs("
            "universe_snapshot_id,universe_taken_at_ms,quoted_at_ms,"
            "requested_token_count,successful_response_count,lease_expires_at_ms,"
            "status,completed_at_ms) VALUES(?,?,?,?,?,?,?,?)",
            (snapshot_id, 900, 1_001, 2, 2, 0, "complete", 1_001),
        )
        run_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])

    now[0] = 1_002
    verified = lifecycle.record_certified_success(
        QuoteCollectionResult(
            run_id=run_id,
            status="complete",
            universe_snapshot_id=snapshot_id,
            requested_token_count=2,
            successful_response_count=2,
            quote_taken_at_ms=1_001,
            elapsed_ms=1,
        )
    )

    assert verified is not None
    assert verified.state == "verified"


def test_certified_quote_run_skips_contained_legacy_incident(tmp_path) -> None:
    """A non-recovering legacy row cannot block a later certifiable P1."""
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle

    now = [1_000]
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    incidents = IncidentManager(store, clock_ms=lambda: now[0])
    legacy = incidents.detect("quote-collection", "legacy-timeout", {"attempt": 1})
    legacy = incidents.transition(legacy.id, "classified", {"action": "classify"})
    incidents.transition(legacy.id, "contained", {"action": "retry"})
    lifecycle = QuoteIncidentLifecycle(incidents)
    active = lifecycle.record_timeout(
        run_id=17,
        requested_token_count=2,
        deadline_s=120,
        consecutive_failures=1,
        last_success_age_s=None,
    )

    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,is_valid,parquet_path) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (900, 901, "subset", 2, 1, "structure", 1, "fixture.parquet"),
        )
        snapshot_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.execute(
            "INSERT INTO neg_risk_quote_runs(universe_snapshot_id,universe_taken_at_ms,"
            "quoted_at_ms,requested_token_count,successful_response_count,"
            "lease_expires_at_ms,status,completed_at_ms) VALUES(?,?,?,?,?,?,?,?)",
            (snapshot_id, 900, 1_001, 2, 2, 0, "complete", 1_001),
        )
        run_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])

    now[0] = 1_002
    verified = lifecycle.record_certified_success(
        QuoteCollectionResult(
            run_id=run_id, status="complete", universe_snapshot_id=snapshot_id,
            requested_token_count=2, successful_response_count=2,
            quote_taken_at_ms=1_001, elapsed_ms=1,
        )
    )

    assert verified is not None
    assert verified.id == active.id
    assert verified.state == "verified"
    assert {item.id: item.state for item in incidents.open_incidents()} == {
        legacy.id: "contained"
    }


def test_certified_quote_run_closes_escalated_quote_supervisor_incident_after_restart(
    tmp_path,
) -> None:
    """A new supervisor handoff needs a later complete run, not a manual clear."""
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle

    now = [1_000]
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    incidents = IncidentManager(store, clock_ms=lambda: now[0])
    incident = incidents.detect("quote", "child-nonzero", {"attempt": 1})
    incident = incidents.transition(incident.id, "classified", {"action": "classify"})
    incident = incidents.transition(incident.id, "contained", {"action": "restart"})
    incident = incidents.transition(incident.id, "recovering", {"retry": 1})
    incident = incidents.transition(incident.id, "escalated", {"action": "operator"})

    # The restarted supervisor records a new recovery boundary before the
    # collection succeeds.  An old successful run is therefore insufficient.
    ProducerSupervisor(
        store=store,
        incidents=incidents,
        clock_ms=lambda: now[0],
    )._resume_open_incidents("quote")

    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,is_valid,parquet_path) VALUES(?,?,?,?,?,?,?,?)",
            (900, 901, "subset", 2, 1, "structure", 1, "fixture.parquet"),
        )
        snapshot_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.execute(
            "INSERT INTO neg_risk_quote_runs("
            "universe_snapshot_id,universe_taken_at_ms,quoted_at_ms,"
            "requested_token_count,successful_response_count,lease_expires_at_ms,"
            "status,completed_at_ms) VALUES(?,?,?,?,?,?,?,?)",
            (snapshot_id, 900, 1_001, 2, 2, 0, "complete", 1_001),
        )
        run_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])

    now[0] = 1_002
    verified = QuoteIncidentLifecycle(incidents).record_certified_success(
        QuoteCollectionResult(
            run_id=run_id,
            status="complete",
            universe_snapshot_id=snapshot_id,
            requested_token_count=2,
            successful_response_count=2,
            quote_taken_at_ms=1_001,
            elapsed_ms=1,
        )
    )

    assert verified is not None
    assert verified.id == incident.id
    assert verified.state == "verified"
