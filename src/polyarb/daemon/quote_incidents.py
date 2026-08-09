"""Durable, operator-readable lifecycle for Quote collection outages."""

from __future__ import annotations

from typing import Any

from polyarb.perception.incidents import Incident, IncidentManager
from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult


class QuoteIncidentLifecycle:
    """Record retrying Quote timeouts without mistaking retry for recovery."""

    _SCOPE = "quote-collection"
    _KIND = "quote-collection-timeout"

    def __init__(self, incidents: IncidentManager) -> None:
        self._incidents = incidents

    def record_timeout(
        self,
        *,
        run_id: int | None,
        requested_token_count: int | None,
        deadline_s: int,
        consecutive_failures: int,
        last_success_age_s: float | None,
    ) -> Incident:
        if deadline_s <= 0 or consecutive_failures < 1:
            raise ValueError("invalid-quote-timeout-evidence")
        impact = (
            "feed-unavailable"
            if last_success_age_s is None or last_success_age_s > 300
            else "feed-at-risk"
        )
        evidence: dict[str, Any] = {
            "run_id": run_id,
            "requested_token_count": requested_token_count,
            "deadline_s": deadline_s,
            "consecutive_failures": consecutive_failures,
            "last_success_age_s": last_success_age_s,
            "impact": impact,
            "automatic_action": "retry-immediately",
            "next_action": "inspect-clob-and-child-io",
            "failure_reason": "quote-collection-subprocess-timeout",
        }
        incident = self._incidents.detect(self._SCOPE, self._KIND, evidence)
        if incident.state == "detected":
            incident = self._incidents.transition(incident.id, "classified", evidence)
            incident = self._incidents.transition(incident.id, "contained", evidence)
            return self._incidents.transition(incident.id, "recovering", evidence)
        if incident.state == "recovering":
            incident = self._incidents.transition(incident.id, "contained", evidence)
            return self._incidents.transition(incident.id, "recovering", evidence)
        return incident

    def record_certified_success(self, result: QuoteCollectionResult) -> Incident | None:
        active = next(
            (
                incident
                for incident in self._incidents.open_incidents()
                if incident.scope == self._SCOPE and incident.kind == self._KIND
            ),
            None,
        )
        if active is None:
            return None
        return self._incidents.transition(
            active.id,
            "verified",
            {
                "run_id": result.run_id,
                "requested_token_count": result.requested_token_count,
                "successful_response_count": result.successful_response_count,
                "automatic_action": "certified-recovery",
            },
        )
