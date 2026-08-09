from __future__ import annotations

import sqlite3

from polyarb.perception.incidents import IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore
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
