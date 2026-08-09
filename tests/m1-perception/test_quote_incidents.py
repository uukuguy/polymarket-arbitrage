from __future__ import annotations

from polyarb.perception.incidents import IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore


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
