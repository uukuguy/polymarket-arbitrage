"""Durable, operator-readable lifecycle for Quote collection outages."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from polyarb.perception.incidents import Incident, IncidentManager
from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult

if TYPE_CHECKING:
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        QuoteWorkerRuntime,
    )


class QuoteIncidentLifecycle:
    """Record retrying Quote timeouts without mistaking retry for recovery."""

    _SCOPE = "quote-collection"
    _KIND = "quote-collection-timeout"

    def __init__(self, incidents: IncidentManager) -> None:
        self._incidents = incidents

    def record_timeout(
        self,
        *,
        attempt_id: int | None = None,
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
        severity = (
            "p1"
            if impact == "feed-unavailable" or consecutive_failures >= 3
            else "p2"
        )
        evidence: dict[str, Any] = {
            "attempt_id": attempt_id,
            "run_id": run_id,
            "requested_token_count": requested_token_count,
            "deadline_s": deadline_s,
            "consecutive_failures": consecutive_failures,
            "last_success_age_s": last_success_age_s,
            "impact": impact,
            "automatic_action": "retry-immediately",
            "next_action": "inspect-clob-and-child-io",
            "failure_reason": "quote-collection-subprocess-timeout",
            "severity": severity,
            "reminder_interval_s": 300 if severity == "p1" else 1800,
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

    def record_failure(
        self,
        *,
        error: QuoteCollectionSubprocessError,
        runtime: QuoteWorkerRuntime,
    ) -> Incident:
        """Persist every non-timeout child failure as an operator-visible incident."""
        snapshot = runtime.snapshot()
        age = (
            None
            if snapshot.last_success_at_s is None
            else max(0.0, time.time() - snapshot.last_success_at_s)
        )
        impact = "feed-unavailable" if age is None or age > 300 else "feed-at-risk"
        severity = (
            "p1"
            if impact == "feed-unavailable" or snapshot.consecutive_failures >= 3
            else "p2"
        )
        evidence: dict[str, Any] = {
            "failure_reason": f"quote-collection-subprocess-{error.reason}",
            "deadline_s": 120,
            "consecutive_failures": snapshot.consecutive_failures,
            "last_success_age_s": age,
            "impact": impact,
            "automatic_action": "retry-at-next-cadence",
            "next_action": "inspect-child-stderr",
            "severity": severity,
            "reminder_interval_s": 300 if severity == "p1" else 1800,
        }
        if error.diagnostic:
            evidence["diagnostic"] = error.diagnostic
        incident = self._incidents.detect(
            self._SCOPE, "quote-collection-failure", evidence
        )
        if incident.state == "detected":
            incident = self._incidents.transition(incident.id, "classified", evidence)
            incident = self._incidents.transition(incident.id, "contained", evidence)
            return self._incidents.transition(incident.id, "recovering", evidence)
        if incident.state == "recovering":
            incident = self._incidents.transition(incident.id, "contained", evidence)
            return self._incidents.transition(incident.id, "recovering", evidence)
        return incident

    def record_certified_success(self, result: QuoteCollectionResult) -> Incident | None:
        verified: Incident | None = None
        for active in self._incidents.open_incidents():
            if active.scope != self._SCOPE:
                continue
            verified = self._incidents.transition(
                active.id,
                "verified",
                {
                    "run_id": result.run_id,
                    "requested_token_count": result.requested_token_count,
                    "successful_response_count": result.successful_response_count,
                    "automatic_action": "certified-recovery",
                },
            )
        return verified
