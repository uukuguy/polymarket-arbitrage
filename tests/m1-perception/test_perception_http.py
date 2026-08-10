from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from polyarb.http import perception
from polyarb.http.opportunity_read_health import BoundedReadLane
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.resource_controller import (
    ResourceController,
    ResourceSample,
)
from polyarb.perception.store import DiscoveryAdmissionProof, OpportunityPerceptionStore


def _seed_candidate_authority(db_path) -> None:
    store = OpportunityPerceptionStore(db_path)
    legs = (
        GroupLeg("m-1", "c-1", "yes-1", "One"),
        GroupLeg("m-2", "c-2", "yes-2", "Two"),
    )
    revision = GroupRevision.certified(
        group_id="g-1",
        event_id="e-1",
        revision=1,
        started_at_ms=1,
        observed_at_ms=2,
        source_cursor="cursor",
        legs=legs,
    )
    store.publish_group_revision(revision)
    batch = GroupQuoteBatch.complete(
        group_id="g-1",
        membership_hash=revision.membership_hash,
        quote_batch_id="q-1",
        started_at_ms=3,
        quoted_at_ms=4,
        legs=tuple(
            GroupQuoteLeg(
                leg.yes_token_id,
                revision.membership_hash,
                0.4,
                10.0,
                "executable",
            )
            for leg in legs
        ),
    )
    store.publish_candidate_success(
        batch,
        observed_at_ms=4,
        last_result="watching",
        reason=None,
        bundle_cost=0.8,
        gross_edge_bps=2_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="edge",
        next_due_at_ms=15_004,
    )


def _seed_equal_timestamp_group_timeline(db_path) -> None:
    store = OpportunityPerceptionStore(db_path)
    legs = (
        GroupLeg("m-1", "c-1", "yes-1", "One"),
        GroupLeg("m-2", "c-2", "yes-2", "Two"),
    )
    revision = GroupRevision.certified(
        group_id="g-1",
        event_id="e-1",
        revision=1,
        started_at_ms=1,
        observed_at_ms=4,
        source_cursor="cursor",
        legs=legs,
    )
    store.publish_group_revision(revision)
    batch = GroupQuoteBatch.complete(
        group_id="g-1",
        membership_hash=revision.membership_hash,
        quote_batch_id="q-1",
        started_at_ms=3,
        quoted_at_ms=4,
        legs=tuple(
            GroupQuoteLeg(
                leg.yes_token_id,
                revision.membership_hash,
                0.4,
                10.0,
                "executable",
            )
            for leg in legs
        ),
    )
    store.publish_candidate_success(
        batch,
        observed_at_ms=4,
        last_result="watching",
        reason=None,
        bundle_cost=0.8,
        gross_edge_bps=2_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="edge",
        next_due_at_ms=15_004,
    )
    IncidentManager(store, clock_ms=lambda: 4).detect(
        "candidate:g-1",
        "worker-failure",
        {"group_id": "g-1"},
    )


def _publish_timeline_success(
    store: OpportunityPerceptionStore,
    revision: GroupRevision,
    *,
    quote_batch_id: str,
    observed_at_ms: int,
    last_result: str,
) -> None:
    edge = 2_000 if last_result == "watching" else 0
    batch = GroupQuoteBatch.complete(
        group_id=revision.group_id,
        membership_hash=revision.membership_hash,
        quote_batch_id=quote_batch_id,
        started_at_ms=observed_at_ms - 1,
        quoted_at_ms=observed_at_ms,
        legs=tuple(
            GroupQuoteLeg(
                leg.yes_token_id,
                revision.membership_hash,
                0.4,
                10.0,
                "executable",
            )
            for leg in revision.legs
        ),
    )
    store.publish_candidate_success(
        batch,
        observed_at_ms=observed_at_ms,
        last_result=last_result,
        reason=None,
        bundle_cost=0.8,
        gross_edge_bps=edge,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="timeline-test",
        next_due_at_ms=observed_at_ms + 15_000,
    )


def test_perception_routes_exist_and_limits_are_validated(http_test_client) -> None:
    assert http_test_client.get("/perception/status").status_code == 200
    for path in (
        "/perception/groups?limit=0",
        "/perception/groups?limit=501",
        "/perception/groups?limit=1%20OR%201",
        "/perception/incidents?limit=-1",
        "/perception/resources?limit=0",
        "/perception/groups/g-1/timeline?limit=0",
    ):
        response = http_test_client.get(path)
        assert response.status_code == 400
        assert response.json() == {
            "status": "invalid-request",
            "reason": "limit-must-be-an-integer-from-1-to-500",
        }


def test_perception_console_is_a_direct_operator_view(http_test_client) -> None:
    response = http_test_client.get("/perception/console")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "M1 incident console" in response.text
    assert "/perception/incidents?limit=100" in response.text
    assert "Automatic action" in response.text
    assert "Next operator action" in response.text
    assert "Failed attempt" in response.text
    assert "read-model-unavailable" in response.text
    assert "Recent recovered severe incidents" in response.text
    assert "/perception/producer-arbitration" in response.text
    assert "/perception/producer-progress" in response.text
    assert "Current producer checkpoints" in response.text
    assert "/perception/incidents/recent?scope=quote-collection" in response.text
    assert (
        'const recentQuoteSupervisorEndpoint="/perception/incidents/recent?scope=quote";'
        in response.text
    )


def test_producer_arbitration_status_is_a_direct_operator_view(http_test_client) -> None:
    response = http_test_client.get("/perception/producer-arbitration")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "available"
    assert body["current_lease"] is None
    assert "SQLite BEGIN IMMEDIATE" in body["automatic_action"]
    assert "next scheduled" in body["operator_action"]


def test_producer_progress_exposes_current_quote_and_structure_checkpoints(
    http_test_client,
) -> None:
    response = http_test_client.get("/perception/producer-progress")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "available"
    assert body["quote"]["attempt"] is None
    assert body["quote"]["hydration"]["consecutive_failures"] == 0
    assert body["structure"]["attempt"] is None
    assert body["structure"]["comparison"] is None
    assert "checkpoint" in body["automatic_action"]


@pytest.mark.asyncio
async def test_perception_read_uses_dedicated_lane_when_default_executor_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An overloaded producer executor cannot starve the operator read model."""
    lane = BoundedReadLane("test-perception-read", capacity=1)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(perception_read_lane=lane)))
    blocked = threading.Event()
    real_to_thread = asyncio.to_thread

    async def blocked_default_executor(*_args, **_kwargs):
        await real_to_thread(blocked.wait)

    monkeypatch.setattr(perception.asyncio, "to_thread", blocked_default_executor)
    monkeypatch.setattr(perception, "_TIMEOUT_S", 0.05)
    asyncio.get_running_loop().call_later(0.1, blocked.set)

    try:
        response = await perception._serve(request, lambda: {"status": "available"})
    finally:
        blocked.set()
        lane.shutdown()

    assert response.status_code == 200
    assert response.body == b'{"status":"available"}'


def test_perception_status_distinguishes_available_zero_from_corrupt_evidence(
    http_test_client,
) -> None:
    response = http_test_client.get("/perception/status")
    assert response.status_code == 200
    assert response.json()["opportunities"] == {
        "status": "available",
        "count": 0,
        "reason": "no-certified-edge",
    }

    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_incident_events("
            "incident_id,sequence,scope,kind,state,occurred_at_ms,evidence_json"
            ") VALUES('bad',2,'candidate','worker-failure','detected',1,'{}')"
        )
    response = http_test_client.get("/perception/status")
    assert response.status_code == 503
    assert response.json()["opportunities"]["status"] == "unavailable"
    assert "traceback" not in response.text.lower()
    assert str(db_path) not in response.text


def test_status_and_current_opportunities_expose_authenticated_candidate_state(
    http_test_client,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    _seed_candidate_authority(db_path)

    status = http_test_client.get("/perception/status")
    opportunities = http_test_client.get("/perception/opportunities?limit=1&after_group_id=")

    assert status.status_code == 200
    status_body = status.json()
    assert type(status_body["server_time_ms"]) is int
    assert status_body["server_time_ms"] >= 0
    assert status_body["current_candidate_group_count"] == 1
    assert status_body["candidate_state_counts"] == {
        "watching": 1,
        "no-edge": 0,
        "unavailable": 0,
    }
    assert status_body["candidate_authority_hash"].startswith("sha256:")
    assert opportunities.status_code == 200
    opportunities_body = opportunities.json()
    assert opportunities_body.pop("server_time_ms") >= 0
    assert (
        opportunities_body.pop("candidate_authority_hash")
        == status_body["candidate_authority_hash"]
    )
    assert opportunities_body.pop("current_opportunity_count") == 1
    assert opportunities_body == {
        "status": "available",
        "items": [
            {
                "group_id": "g-1",
                "event_id": "e-1",
                "group_revision": 1,
                "membership_hash": (
                    OpportunityPerceptionStore(db_path)
                    .current_opportunities(after_group_id="", limit=1)[0][0]
                    .membership_hash
                ),
                "quote_batch_id": "q-1",
                "fact_id": 1,
                "bundle_cost": 0.8,
                "gross_edge_bps": 2_000.0,
                "max_bundle_size": 10.0,
                "structure_observed_at_ms": 2,
                "quote_started_at_ms": 3,
                "quote_quoted_at_ms": 4,
            }
        ],
        "limit": 1,
        "next_after_group_id": None,
    }


def test_group_timeline_merges_four_classes_with_stable_equal_time_cursor(
    http_test_client,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    _seed_equal_timestamp_group_timeline(db_path)

    first = http_test_client.get("/perception/groups/g-1/timeline?limit=2")

    assert first.status_code == 200
    body = first.json()
    assert [item["class"] for item in body["items"]] == [
        "membership_revision",
        "quote_batch",
    ]
    assert all(item["occurred_at_ms"] == 4 for item in body["items"])
    assert body["next_before"]
    assert body["history_complete"] == {
        "membership": True,
        "quote": True,
        "opportunity": True,
        "incident": True,
    }

    second = http_test_client.get(
        "/perception/groups/g-1/timeline",
        params={"limit": 2, "before": body["next_before"]},
    )

    assert second.status_code == 200
    assert [item["class"] for item in second.json()["items"]] == [
        "opportunity_transition",
        "incident_event",
    ]
    assert second.json()["next_before"] is None


def test_group_timeline_cursor_is_canonical_and_bound_to_group_identity(
    http_test_client,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    _seed_equal_timestamp_group_timeline(db_path)
    cursor = http_test_client.get("/perception/groups/g-1/timeline?limit=1").json()["next_before"]

    wrong_group = http_test_client.get(
        "/perception/groups/g-2/timeline",
        params={"limit": 1, "before": cursor},
    )
    padded = http_test_client.get(
        "/perception/groups/g-1/timeline",
        params={"limit": 1, "before": cursor + "="},
    )

    assert wrong_group.status_code == 400
    assert wrong_group.json()["reason"] == "invalid-group-timeline-cursor"
    assert padded.status_code == 400
    assert padded.json()["reason"] == "invalid-group-timeline-cursor"


def test_group_timeline_represents_transition_across_candidate_floor(
    http_test_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    store = OpportunityPerceptionStore(db_path)
    revision_a = GroupRevision.certified(
        group_id="g-a",
        event_id="e-a",
        revision=1,
        started_at_ms=1,
        observed_at_ms=2,
        source_cursor="a",
        legs=(
            GroupLeg("m-a1", "c-a1", "yes-a1", "A1"),
            GroupLeg("m-a2", "c-a2", "yes-a2", "A2"),
        ),
    )
    store.publish_group_revision(revision_a)
    revision_b = GroupRevision.certified(
        group_id="g-b",
        event_id="e-b",
        revision=1,
        started_at_ms=1,
        observed_at_ms=3,
        source_cursor="b",
        legs=(
            GroupLeg("m-b1", "c-b1", "yes-b1", "B1"),
            GroupLeg("m-b2", "c-b2", "yes-b2", "B2"),
        ),
    )
    store.publish_group_revision(revision_b)
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        1,
    )
    _publish_timeline_success(
        store,
        revision_a,
        quote_batch_id="q-a1",
        observed_at_ms=4,
        last_result="watching",
    )
    _publish_timeline_success(
        store,
        revision_b,
        quote_batch_id="q-b1",
        observed_at_ms=5,
        last_result="watching",
    )
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        10_000,
    )
    _publish_timeline_success(
        store,
        revision_a,
        quote_batch_id="q-a2",
        observed_at_ms=6,
        last_result="no-edge",
    )

    body_a = http_test_client.get("/perception/groups/g-a/timeline?limit=100").json()
    transition = next(item for item in body_a["items"] if item["class"] == "opportunity_transition")
    assert transition["from"] == {
        "last_result": "watching",
        "opportunity": True,
    }
    assert transition["to"] == {
        "last_result": "no-edge",
        "opportunity": False,
    }
    assert {
        item["quote_batch_id"] for item in body_a["items"] if item["class"] == "quote_batch"
    } == {"q-a2"}
    assert body_a["history_complete"] == {
        "membership": True,
        "quote": False,
        "opportunity": False,
        "incident": True,
    }

    body_b = http_test_client.get("/perception/groups/g-b/timeline?limit=100").json()
    assert body_b["history_complete"]["quote"] is False
    assert body_b["history_complete"]["opportunity"] is False
    assert body_b["history_floor"]["quote"]["scope"] == "global"
    assert body_b["history_floor"]["incident"] == {
        "scope": "candidate:g-b",
        "through_id": 0,
        "compacted_count": 0,
    }


def test_group_timeline_enforces_shared_deadline_and_response_cap(
    http_test_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    finished = threading.Event()

    def slow_timeline(*_args):
        con = perception._connect(db_path)
        try:
            con.execute(
                "WITH RECURSIVE counter(value) AS ("
                "SELECT 1 UNION ALL SELECT value + 1 FROM counter "
                "WHERE value < 10000000"
                ") SELECT sum(value) FROM counter"
            ).fetchone()
            return {"status": "unexpected"}
        finally:
            con.close()
            finished.set()

    monkeypatch.setattr(perception, "_timeline", slow_timeline)
    started = time.monotonic()
    response = http_test_client.get("/perception/groups/g-1/timeline?limit=2")

    assert response.status_code == 503
    assert time.monotonic() - started <= 1.1
    assert finished.is_set()

    monkeypatch.setattr(
        perception,
        "_timeline",
        lambda *_args: {"status": "available", "oversized": "x" * 1_048_576},
    )
    response = http_test_client.get("/perception/groups/g-1/timeline?limit=2")
    assert response.status_code == 503
    assert response.json() == {
        "status": "unavailable",
        "reason": "durable-evidence-invalid",
    }


def test_status_uses_bounded_current_projection_not_full_candidate_replay(
    http_test_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        OpportunityPerceptionStore,
        "validated_candidate_opportunity_count",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("status-must-not-run-full-candidate-replay")
        ),
    )

    response = http_test_client.get("/perception/status")

    assert response.status_code == 200
    assert response.json()["current_candidate_group_count"] == 0


def test_current_opportunity_cursor_is_bounded_and_invalid_evidence_fails_closed(
    http_test_client,
) -> None:
    for path in (
        "/perception/opportunities?limit=0",
        "/perception/opportunities?limit=501",
        "/perception/opportunities?after_group_id=%00",
        "/perception/opportunities?after_group_id=" + "x" * 257,
    ):
        response = http_test_client.get(path)
        assert response.status_code == 400

    db_path = http_test_client.app.state.sqlite_store.db_path
    _seed_candidate_authority(db_path)
    with sqlite3.connect(db_path) as con:
        con.execute(
            "UPDATE neg_risk_candidate_current_authority "
            "SET canonical_json='{}' WHERE group_id='g-1'"
        )

    response = http_test_client.get("/perception/opportunities?limit=100")
    assert response.status_code == 503
    assert response.json() == {
        "status": "unavailable",
        "reason": "durable-evidence-invalid",
    }


def test_group_history_is_bounded_and_corruption_fails_closed(http_test_client) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    now = int(time.time() * 1000)
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_group_revisions("
            "group_id,event_id,revision,membership_hash,started_at_ms,observed_at_ms,"
            "source_cursor,status,legs_json) VALUES(?,?,?,?,?,?,?,?,?)",
            ("g/quoted", "e-1", 1, "forged", now, now, "c", "certified", "[]"),
        )
    response = http_test_client.get("/perception/groups/g%2Fquoted/history?limit=10")
    assert response.status_code == 503
    assert response.json()["status"] == "unavailable"


def test_group_reads_page_safely_beyond_total_history_bound(
    http_test_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    legs = (
        GroupLeg("m-1", "c-1", "yes-1", "One"),
        GroupLeg("m-2", "c-2", "yes-2", "Two"),
    )
    for revision_number in range(1, 4):
        store.publish_group_revision(
            GroupRevision.certified(
                group_id="long-history",
                event_id="event-1",
                revision=revision_number,
                started_at_ms=revision_number,
                observed_at_ms=revision_number,
                source_cursor=f"cursor-{revision_number}",
                legs=legs,
            )
        )

    groups = http_test_client.get("/perception/groups?limit=10")
    assert groups.status_code == 200
    assert groups.json()["items"][0]["revision"] == 3
    latest = http_test_client.get("/perception/groups/long-history/history?limit=2")
    assert latest.status_code == 200
    assert [item["revision"] for item in latest.json()["items"]] == [3, 2]
    assert latest.json()["next_before_revision"] == 2
    oldest = http_test_client.get(
        "/perception/groups/long-history/history?limit=2&before_revision=2"
    )
    assert oldest.status_code == 200
    assert [item["revision"] for item in oldest.json()["items"]] == [1]
    assert oldest.json()["next_before_revision"] is None


def test_discovery_reconciliation_and_incidents_use_stable_envelopes(
    http_test_client,
) -> None:
    assert http_test_client.get("/perception/discovery").json() == {
        "status": "available",
        "discovery": None,
    }
    assert http_test_client.get("/perception/reconciliation").json() == {
        "status": "available",
        "reconciliation": None,
    }
    assert http_test_client.get("/perception/incidents?limit=5").json() == {
        "status": "available",
        "items": [],
        "limit": 5,
        "open_count": 0,
        "next_before": None,
    }
    assert http_test_client.get("/perception/resources?limit=5").json() == {
        "status": "available",
        "current": None,
        "items": [],
        "limit": 5,
        "next_before_sequence": None,
        "history_floor": None,
    }


def test_quote_timeout_incident_exposes_operator_diagnosis(http_test_client) -> None:
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle

    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    QuoteIncidentLifecycle(IncidentManager(store, clock_ms=lambda: 1_000)).record_timeout(
        run_id=1908,
        requested_token_count=38_972,
        deadline_s=120,
        consecutive_failures=3,
        last_success_age_s=3_057.8,
    )

    response = http_test_client.get("/perception/incidents?limit=10")

    assert response.status_code == 200
    assert response.json()["items"][0]["diagnosis"] == {
        "severity": "p1",
        "reminder_interval_s": 300,
        "impact": "feed-unavailable",
        "automatic_action": "retry-immediately",
        "next_action": "inspect-clob-and-child-io",
        "deadline_s": 120,
        "consecutive_failures": 3,
        "last_success_age_s": 3057.8,
        "free_percent": None,
        "failure_reason": "quote-collection-subprocess-timeout",
    }


def test_quote_timeout_with_integral_float_deadline_exposes_operator_diagnosis(
    http_test_client,
) -> None:
    """Production timeout budgets are floats and must not make dashboard diagnosis blank."""
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle

    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    QuoteIncidentLifecycle(IncidentManager(store, clock_ms=lambda: 1_000)).record_timeout(
        run_id=1909,
        requested_token_count=38_972,
        deadline_s=180.0,
        consecutive_failures=1,
        last_success_age_s=None,
        failure_kind="child-hard-timeout",
    )

    diagnosis = http_test_client.get("/perception/incidents?limit=10").json()["items"][0][
        "diagnosis"
    ]

    assert diagnosis is not None
    assert diagnosis["severity"] == "p1"
    assert diagnosis["deadline_s"] == 180.0
    assert diagnosis["next_action"] == "inspect-stage-checkpoint-and-rebalance-child-budget"


def test_quote_projection_failure_exposes_operator_diagnosis(http_test_client) -> None:
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle
    from polyarb.daemon.quote_worker import QuoteWorkerRuntime
    from polyarb.perception.incidents import IncidentManager
    from polyarb.perception.store import OpportunityPerceptionStore
    from polyarb.routing.opportunity_scanner import StaleUniverseError

    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    runtime = QuoteWorkerRuntime()
    runtime.mark_failure(StaleUniverseError("universe is stale"))
    QuoteIncidentLifecycle(IncidentManager(store, clock_ms=lambda: 1_000)).record_pipeline_failure(
        error=StaleUniverseError("universe is stale"), runtime=runtime, attempt_id=7, run_id=9
    )

    item = http_test_client.get("/perception/incidents?limit=10").json()["items"][0]

    assert item["kind"] == "quote-projection-failure"
    assert item["diagnosis"] is not None
    assert item["diagnosis"]["deadline_s"] is None
    assert item["diagnosis"]["next_action"] == "inspect-structure-publication-checkpoint"


def test_structure_incident_exposes_operator_diagnosis(http_test_client) -> None:
    """A failed Structure publication is actionable from the main console API."""
    from polyarb.daemon.structure_incidents import StructureIncidentLifecycle

    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    StructureIncidentLifecycle(IncidentManager(store, clock_ms=lambda: 1_000)).record_failure(
        failure_kind="snapshot-subprocess-timeout",
        elapsed_ms=45_198,
        last_stage="persist",
    )

    item = http_test_client.get("/perception/incidents?limit=10").json()["items"][0]

    assert item["scope"] == "structure"
    assert item["diagnosis"] == {
        "severity": "p1",
        "reminder_interval_s": 300,
        "impact": "market-map-stale",
        "automatic_action": "retry-bounded-structure-child",
        "next_action": "inspect-stage-checkpoint-and-child-budget",
        "deadline_s": None,
        "consecutive_failures": 1,
        "last_success_age_s": None,
        "free_percent": None,
        "failure_reason": "snapshot-subprocess-timeout",
        "elapsed_ms": 45_198,
        "last_stage": "persist",
        "cooperative_slice_budget_s": 45,
        "child_hard_limit_s": 75,
    }


def test_structure_lock_incident_exposes_diagnosis_without_child_timing(
    http_test_client,
) -> None:
    """A parent-side SQLite lock is still a P1 even without child diagnostics."""
    from polyarb.daemon.structure_incidents import StructureIncidentLifecycle

    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    StructureIncidentLifecycle(IncidentManager(store, clock_ms=lambda: 1_000)).record_failure(
        failure_kind="database is locked",
        elapsed_ms=None,
        last_stage=None,
    )

    diagnosis = http_test_client.get("/perception/incidents?limit=10").json()["items"][0][
        "diagnosis"
    ]

    assert diagnosis is not None
    assert diagnosis["failure_reason"] == "database is locked"
    assert diagnosis["elapsed_ms"] is None
    assert diagnosis["last_stage"] is None


def test_legacy_structure_incident_receives_static_budget_context(http_test_client) -> None:
    """A pre-budget P1 remains readable immediately after the Dashboard deploy."""
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    IncidentManager(store, clock_ms=lambda: 1_000).detect(
        "structure",
        "structure-producer-failure",
        {
            "severity": "p1",
            "impact": "market-map-stale",
            "automatic_action": "retry-bounded-structure-child",
            "next_action": "inspect-stage-checkpoint-and-child-budget",
            "failure_reason": "snapshot-subprocess-timeout",
            "elapsed_ms": 75_000,
            "last_stage": "persist",
        },
    )

    diagnosis = http_test_client.get("/perception/incidents?limit=10").json()["items"][0][
        "diagnosis"
    ]

    assert diagnosis["cooperative_slice_budget_s"] == 45
    assert diagnosis["child_hard_limit_s"] == 75


def test_quote_child_failure_exposes_diagnosis_without_prior_success(
    http_test_client,
) -> None:
    """The first failure is P1 too; absent age cannot hide its operator plan."""
    from polyarb.daemon.quote_incidents import QuoteIncidentLifecycle
    from polyarb.daemon.quote_worker import QuoteCollectionSubprocessError, QuoteWorkerRuntime

    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    runtime = QuoteWorkerRuntime()
    runtime.mark_failure(QuoteCollectionSubprocessError("failed"))
    QuoteIncidentLifecycle(IncidentManager(store, clock_ms=lambda: 1_000)).record_failure(
        error=QuoteCollectionSubprocessError(
            "failed", diagnostic="PolyApiException: Request exception"
        ),
        runtime=runtime,
    )

    item = http_test_client.get("/perception/incidents?limit=10").json()["items"][0]

    assert item["kind"] == "quote-collection-failure"
    assert item["diagnosis"] is not None
    assert item["diagnosis"]["severity"] == "p1"
    assert item["diagnosis"]["last_success_age_s"] is None
    assert item["diagnosis"]["next_action"] == "inspect-child-stderr"


def test_quote_supervisor_escalation_exposes_p1_operator_disposition(
    http_test_client,
) -> None:
    """A dead supervised Quote child is P1, not opaque raw evidence."""
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    manager = IncidentManager(store, clock_ms=lambda: 1_000)
    incident = manager.detect(
        "quote",
        "child-nonzero",
        {"action": "restart-producer", "retry_count": 3},
    )
    manager.transition(
        incident.id,
        "classified",
        {"action": "classify-producer-failure", "retry_count": 3},
    )
    manager.transition(
        incident.id,
        "contained",
        {"action": "restart-producer", "retry_count": 3},
    )
    manager.transition(
        incident.id,
        "escalated",
        {
            "action": "operator-intervention",
            "retry_count": 3,
            "retry_limit": 3,
        },
    )

    item = http_test_client.get("/perception/incidents?limit=10").json()["items"][0]

    assert item["diagnosis"] == {
        "severity": "p1",
        "reminder_interval_s": 300,
        "impact": "feed-unavailable",
        "automatic_action": "automatic-retries-exhausted",
        "next_action": "inspect-producer-receipt-and-restart",
        "deadline_s": None,
        "consecutive_failures": 4,
        "last_success_age_s": None,
        "free_percent": None,
        "failure_reason": "child-nonzero",
    }


def test_capacity_incident_exposes_operator_diagnosis(http_test_client) -> None:
    from polyarb.perception.capacity_incidents import CapacityIncidentLifecycle
    from polyarb.storage.sqlite_store import SQLiteStore

    db_path = http_test_client.app.state.sqlite_store.db_path
    runtime = SQLiteStore(db_path).record_capacity_controller_measurement(
        state="critical",
        free_bytes=11,
        free_percent=11.0,
        observed_at_ms=1_000,
    )
    CapacityIncidentLifecycle(
        IncidentManager(OpportunityPerceptionStore(db_path), clock_ms=lambda: 1_000)
    ).observe(runtime)

    item = http_test_client.get("/perception/incidents?limit=10").json()["items"][0]

    assert item["scope"] == "capacity"
    assert item["diagnosis"] == {
        "severity": "p1",
        "impact": "storage-exhaustion-risk",
        "automatic_action": "reclaim-bounded-history",
        "next_action": "inspect-capacity-receipts",
        "deadline_s": None,
        "free_percent": 11.0,
        "consecutive_failures": 0,
        "last_success_age_s": None,
        "failure_reason": None,
        "reminder_interval_s": 300,
    }


def test_incident_history_endpoint_exposes_exact_bounded_lifecycle(
    http_test_client,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    now = [1_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect("candidate", "child-failed", {"attempt": 1})
    now[0] += 1
    manager.transition(incident.id, "classified", {"action": "classify-producer-failure"})
    now[0] += 1
    manager.transition(incident.id, "contained", {"action": "restart-producer"})
    now[0] += 1
    manager.transition(incident.id, "recovering", {"retry": 1})

    response = http_test_client.get(f"/perception/incidents/{incident.id}/history")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "available"
    assert body["incident_id"] == incident.id
    assert body["scope"] == "candidate"
    assert body["kind"] == "child-failed"
    assert body["history_complete"] is True
    assert body["recovery_writer_receipt"] is None
    assert [item["sequence"] for item in body["items"]] == [1, 2, 3, 4]
    assert [item["state"] for item in body["items"]] == [
        "detected",
        "classified",
        "contained",
        "recovering",
    ]


def test_incident_history_endpoint_exposes_verified_candidate_writer(
    http_test_client,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    now = [1]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect("candidate", "child-failed", {"attempt": 1})
    manager.transition(incident.id, "classified", {})
    manager.transition(incident.id, "contained", {})
    manager.transition(incident.id, "recovering", {"retry": 1})
    _seed_candidate_authority(store.db_path)
    with sqlite3.connect(store.db_path) as con:
        receipt_id = con.execute(
            "SELECT MAX(id) FROM neg_risk_candidate_success_receipts"
        ).fetchone()[0]
    now[0] = 5
    manager.transition(
        incident.id,
        "verified",
        {
            "candidate_success_receipt_id": receipt_id,
            "group_id": "g-1",
            "membership_hash": store.current_group("g-1").membership_hash,
            "quote_batch_id": "q-1",
        },
    )

    response = http_test_client.get(f"/perception/incidents/{incident.id}/history")

    assert response.status_code == 200
    body = response.json()
    assert body["items"][-1]["state"] == "verified"
    assert body["recovery_writer_receipt"] == {
        "component": "candidate",
        "receipt_row_id": receipt_id,
    }


@pytest.mark.parametrize("incident_id", ["bad", "A" * 32, "a" * 31, "g" * 32])
def test_incident_history_endpoint_rejects_invalid_identity(
    http_test_client,
    incident_id: str,
) -> None:
    response = http_test_client.get(f"/perception/incidents/{incident_id}/history")

    assert response.status_code == 400
    assert response.json()["reason"] == "invalid-incident-id"


def test_incident_history_endpoint_returns_not_found_without_guessing(
    http_test_client,
) -> None:
    response = http_test_client.get(f"/perception/incidents/{'a' * 32}/history")

    assert response.status_code == 404
    assert response.json() == {
        "status": "unavailable",
        "reason": "incident-not-found-or-retained",
    }


def test_open_incident_history_survives_event_prefix_compaction(
    http_test_client,
) -> None:
    """An open P1 remains inspectable even after its event prefix is compacted."""
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    now = [1_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect("structure", "structure-producer-failure", {"attempt": 1})
    now[0] += 1
    manager.transition(incident.id, "classified", {"action": "classify"})
    now[0] += 1
    manager.transition(incident.id, "contained", {"action": "retry"})

    # Keep the target incident open, then advance unrelated lifecycle traffic
    # beyond the 512-event suffix budget so all target events leave the raw
    # event table while its open-authority row remains canonical.
    for index in range(260):
        now[0] += 1
        other = manager.detect(f"candidate:{index}", "worker-failure", {"attempt": index})
        now[0] += 1
        manager.transition(other.id, "classified", {"action": "classify"})

    response = http_test_client.get(f"/perception/incidents/{incident.id}/history")

    assert response.status_code == 200
    body = response.json()
    assert body["history_complete"] is False
    assert body["scope"] == "structure"
    assert body["kind"] == "structure-producer-failure"
    assert body["items"][-1]["state"] == "contained"


def test_recent_incident_endpoint_discovers_latest_state_after_open_removal(
    http_test_client,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    now = [1_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect("candidate", "child-nonzero", {"attempt": 1})
    now[0] += 1
    manager.transition(incident.id, "classified", {})

    response = http_test_client.get(
        "/perception/incidents/recent",
        params={"scope": "candidate", "after_ms": "999", "limit": "5"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "available",
        "scope": "candidate",
        "after_ms": 999,
        "limit": 5,
        "items": [
            {
                "incident_id": incident.id,
                "sequence": 2,
                "kind": "child-nonzero",
                "state": "classified",
                "occurred_at_ms": 1_001,
                "evidence": {},
            }
        ],
    }


def test_recent_incident_endpoint_uses_dedicated_operator_lane_and_budget(
    http_test_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical incident evidence cannot be starved by broad perception reads."""
    observed: dict[str, object] = {}

    async def capture_serve(_request, _reader, **kwargs):
        observed.update(kwargs)
        return perception.JSONResponse({"status": "available", "items": []})

    monkeypatch.setattr(perception, "_serve", capture_serve)

    request = SimpleNamespace(
        query_params={"scope": "quote-collection", "after_ms": "0", "limit": "5"},
        app=http_test_client.app,
    )
    response = asyncio.run(perception.perception_recent_incidents(request))

    assert response.status_code == 200
    assert observed == {
        "lane_name": "incident_read_lane",
        "timeout_s": 3.0,
        "sql_deadline_s": 2.5,
    }


@pytest.mark.parametrize(
    ("params", "reason"),
    [
        ({"scope": "", "after_ms": "1"}, "invalid-incident-scope"),
        ({"scope": "candidate", "after_ms": "-1"}, "invalid-after-ms"),
        ({"scope": "candidate", "after_ms": "01"}, "invalid-after-ms"),
    ],
)
def test_recent_incident_endpoint_rejects_ambiguous_query(
    http_test_client,
    params: dict[str, str],
    reason: str,
) -> None:
    response = http_test_client.get("/perception/incidents/recent", params=params)

    assert response.status_code == 400
    assert response.json()["reason"] == reason


def test_qualification_endpoint_exposes_explicit_zero_counters(
    http_test_client,
) -> None:
    response = http_test_client.get("/perception/qualification")

    assert response.status_code == 200
    assert response.json() == {
        "status": "available",
        "cross_membership_quote_batches": 0,
        "orphan_collecting_runs": 0,
    }


def test_qualification_endpoint_counts_mismatch_and_expired_collecting_lease(
    http_test_client,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        complete = con.execute(
            "INSERT INTO neg_risk_quote_runs("
            "universe_snapshot_id,universe_taken_at_ms,universe_hash,"
            "source_truth_hash,quoted_at_ms,requested_token_count,"
            "successful_response_count,lease_expires_at_ms,status,completed_at_ms"
            ") VALUES(1,1,'u','s',1,1,1,3,'collecting',NULL)"
        ).lastrowid
        con.execute(
            "INSERT INTO neg_risk_quote_run_legs("
            "quote_run_id,neg_risk_market_id,event_id,membership_hash,"
            "market_id,condition_id,slug,yes_token_id"
            ") VALUES(?,?,?,?,?,?,?,?)",
            (complete, "g-1", "e-1", "membership-a", "m-1", "c-1", "slug", "t-1"),
        )
        con.execute(
            "INSERT INTO neg_risk_quotes("
            "quote_run_id,neg_risk_market_id,event_id,membership_hash,"
            "market_id,condition_id,slug,yes_token_id,terminal_state,"
            "best_ask_price,best_ask_size"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                complete,
                "g-1",
                "e-1",
                "membership-b",
                "m-1",
                "c-1",
                "slug",
                "t-1",
                "executable",
                0.4,
                10,
            ),
        )
        con.execute(
            "UPDATE neg_risk_quote_runs SET status='complete',completed_at_ms=2 WHERE id=?",
            (complete,),
        )
        con.execute(
            "INSERT INTO neg_risk_quote_runs("
            "universe_snapshot_id,universe_taken_at_ms,universe_hash,"
            "source_truth_hash,quoted_at_ms,requested_token_count,"
            "successful_response_count,lease_expires_at_ms,status"
            ") VALUES(1,1,'u','s',1,0,0,1,'collecting')"
        )

    response = http_test_client.get("/perception/qualification")

    assert response.status_code == 200
    assert response.json()["cross_membership_quote_batches"] == 1
    assert response.json()["orphan_collecting_runs"] == 1


def test_resource_endpoint_returns_current_and_keyset_history(
    http_test_client,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    now = [2_000]
    controller = ResourceController(
        store,
        clock_ms=lambda: now[0],
        cooldown_ms=0,
        _verify_store_authority=False,
    )
    samples = (
        ResourceSample(0, None, 0, True, True, False, 50, 2_000),
        ResourceSample(2, 25_000, 0, True, True, True, 50, 2_001),
        ResourceSample(2, 5_000, 0, True, True, True, 50, 2_002),
    )
    for sample in samples:
        now[0] = sample.observed_at_ms
        controller.decide(sample)

    first = http_test_client.get("/perception/resources?limit=2")

    assert first.status_code == 200
    body = first.json()
    assert body["current"]["sequence"] == 3
    assert body["current"]["mode"] == "normal"
    assert [item["decision"]["sequence"] for item in body["items"]] == [3, 2]
    assert body["items"][0]["sample"]["candidate_quote_p95_ms"] == 5_000
    assert body["next_before_sequence"] == 2
    assert body["history_floor"] is None

    second = http_test_client.get("/perception/resources?limit=2&before_sequence=2")
    assert second.status_code == 200
    assert [item["decision"]["sequence"] for item in second.json()["items"]] == [1]
    assert second.json()["next_before_sequence"] is None


@pytest.mark.parametrize("value", ["0", "-1", "01", "abc"])
def test_resource_endpoint_rejects_invalid_sequence_cursor(
    http_test_client,
    value: str,
) -> None:
    response = http_test_client.get(
        "/perception/resources",
        params={"before_sequence": value},
    )
    assert response.status_code == 400
    assert response.json()["reason"] == ("before-sequence-must-be-a-positive-integer")


def test_incident_endpoint_pages_more_than_legacy_history_cap(
    http_test_client,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    now = [1_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    for sequence in range(600):
        now[0] += 1
        manager.detect(
            f"candidate:http-page-{sequence}",
            "clob-timeout",
            {"sequence": sequence},
        )

    first = http_test_client.get("/perception/incidents?limit=100")

    assert first.status_code == 200
    first_body = first.json()
    assert len(first_body["items"]) == 100
    assert isinstance(first_body["next_before"], str)
    second = http_test_client.get(
        "/perception/incidents",
        params={"limit": 100, "before": first_body["next_before"]},
    )
    assert second.status_code == 200
    second_body = second.json()
    assert len(second_body["items"]) == 100
    assert {item["incident_id"] for item in first_body["items"]}.isdisjoint(
        item["incident_id"] for item in second_body["items"]
    )


def test_status_uses_open_aggregate_after_incident_compaction(
    http_test_client,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    now = [1_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    for sequence in range(600):
        now[0] += 1
        manager.detect(
            f"operator:{sequence}",
            "manual-investigation",
            {"sequence": sequence},
        )

    response = http_test_client.get("/perception/status")

    assert response.status_code == 200
    assert response.json()["open_incident_count"] == 600


def test_discovery_status_does_not_permanently_fail_on_old_receipt_volume(
    http_test_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    store.configure_discovery_admission(
        DiscoveryAdmissionProof(
            effective_capacity=2,
            candidate_max_wait_ms=60_000,
            selection_budget_ms=6_000,
            poll_interval_ms=1_000,
            group_timeout_ms=10_000,
            terminal_write_budget_ms=5_000,
            high_burst_groups=1,
            reserved_non_high_slots=3,
        ),
        now_ms=1,
    )
    for sweep in range(1, 4):
        store.publish_discovery_batch(
            requested_cursor=None,
            next_cursor=None,
            completed=True,
            started_at_ms=sweep,
            finished_at_ms=sweep,
            page_event_count=0,
            candidates=(),
            admission_proof=store.discovery_admission_proof(),
        )

    response = http_test_client.get("/perception/discovery")
    assert response.status_code == 200
    assert response.json()["discovery"]["completed"] is True
    assert response.json()["discovery"]["groups_seen"] == 0


def test_discovery_exposes_validated_coverage_load_and_admission_evidence(
    http_test_client,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    proof = DiscoveryAdmissionProof(
        effective_capacity=2,
        candidate_max_wait_ms=60_000,
        selection_budget_ms=6_000,
        poll_interval_ms=1_000,
        group_timeout_ms=10_000,
        terminal_write_budget_ms=5_000,
        high_burst_groups=1,
        reserved_non_high_slots=3,
    )
    store.configure_discovery_admission(proof, now_ms=1)
    store.record_discovery_load_decision(
        degraded_reason="candidate-quote-stale",
        probe_every_cycles=3,
        now_ms=2,
    )
    store.publish_discovery_batch(
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        started_at_ms=3,
        finished_at_ms=4,
        page_event_count=0,
        candidates=(),
        admission_proof=proof,
    )

    response = http_test_client.get("/perception/discovery")

    assert response.status_code == 200
    discovery = response.json()["discovery"]
    assert discovery["coverage"] == {
        "known_groups": 0,
        "total_liquidity_weight": 0.0,
        "by_minutes": {
            "15": {
                "visited_groups": 0,
                "raw_fraction": 0.0,
                "liquidity_weighted_fraction": 0.0,
            },
            "30": {
                "visited_groups": 0,
                "raw_fraction": 0.0,
                "liquidity_weighted_fraction": 0.0,
            },
            "60": {
                "visited_groups": 0,
                "raw_fraction": 0.0,
                "liquidity_weighted_fraction": 0.0,
            },
        },
    }
    assert discovery["load_state"] == {
        "degraded_streak": 1,
        "last_reason": "candidate-quote-stale",
        "last_decision": "yield",
        "probe_every_cycles": 3,
        "updated_at_ms": 2,
    }
    assert discovery["admission_proof"]["effective_capacity"] == 2
    assert discovery["candidate_attempt_start_count"] == 0
    assert discovery["candidate_start_deadline_breach_count"] == 0


def test_reconciliation_exposes_validated_duration_and_diff_counts(
    http_test_client,
) -> None:
    store = OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    window = store.begin_reconciliation(started_at_ms=10)

    response = http_test_client.get("/perception/reconciliation")

    assert response.status_code == 200
    reconciliation = response.json()["reconciliation"]
    assert reconciliation["id"] == window.id
    assert reconciliation["duration_ms"] == 0
    assert reconciliation["observations_count"] == 0
    assert reconciliation["baseline_count"] == 0
    assert reconciliation["added_count"] is None
    assert reconciliation["changed_count"] is None
    assert reconciliation["closed_count"] is None
    assert reconciliation["unchanged_count"] is None
    assert reconciliation["applied_rejected_count"] is None


def test_incidents_recursively_redact_legacy_secret_shapes(http_test_client) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    manager = IncidentManager(OpportunityPerceptionStore(db_path), clock_ms=lambda: 1)
    manager.detect(
        "discovery",
        "worker-failure",
        {
            "action": "Bearer top-level-secret",
            "outer": {
                "API_KEY": "hunter2",
                "authorization": "Bearer abc123",
                "db": "postgresql://user:hunter2@db.invalid/x?password=hunter2",
                "note": "password=hunter2",
                "cookie": "session=hunter2",
            },
        },
    )
    response = http_test_client.get("/perception/incidents")
    assert response.status_code == 200
    rendered = response.text.lower()
    for secret in (
        "hunter2",
        "abc123",
        "top-level-secret",
        "postgresql://",
        "bearer ",
    ):
        assert secret not in rendered
    assert "[redacted]" in rendered
    assert response.json()["items"][0]["action"] is None


def test_status_rejects_forged_candidate_receipt_rowid(http_test_client) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    _seed_candidate_authority(db_path)
    with sqlite3.connect(db_path) as con:
        con.execute("UPDATE neg_risk_candidate_success_receipts SET group_revision_row_id=999")
    assert http_test_client.get("/perception/status").status_code == 503


def test_status_rejects_quote_leg_membership_forgery(http_test_client) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    _seed_candidate_authority(db_path)
    with sqlite3.connect(db_path) as con:
        legs = con.execute(
            "SELECT legs_json FROM neg_risk_group_quote_batches WHERE id='q-1'"
        ).fetchone()[0]
        con.execute(
            "UPDATE neg_risk_group_quote_batches SET legs_json=? WHERE id='q-1'",
            (legs.replace('"yes-1","', '"yes-1","forged-'),),
        )
    assert http_test_client.get("/perception/status").status_code == 503


def test_groups_remain_available_with_bounded_multi_revision_page(
    http_test_client,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    legs = (
        GroupLeg("m-1", "c-1", "yes-1", "One"),
        GroupLeg("m-2", "c-2", "yes-2", "Two"),
    )
    membership_hash = GroupRevision.membership_digest(legs)
    legs_json = '[["m-1","c-1","yes-1","One"],["m-2","c-2","yes-2","Two"]]'
    with sqlite3.connect(db_path) as con:
        con.executemany(
            "INSERT INTO neg_risk_group_revisions("
            "group_id,event_id,revision,membership_hash,started_at_ms,"
            "observed_at_ms,source_cursor,status,legs_json) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (
                (
                    f"g-{group:03d}",
                    f"e-{group:03d}",
                    revision,
                    membership_hash,
                    revision,
                    revision,
                    f"c-{revision}",
                    "certified",
                    legs_json,
                )
                for group in range(100)
                for revision in range(1, 7)
            ),
        )
    response = http_test_client.get("/perception/groups?limit=100")
    assert response.status_code == 200
    assert len(response.json()["items"]) == 100


def test_slow_read_is_interrupted_and_worker_converges_before_response(
    http_test_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    finished = threading.Event()

    def deliberately_slow_status(_db_path):
        con = perception._connect(db_path)
        try:
            con.execute(
                "WITH RECURSIVE counter(value) AS ("
                "SELECT 1 UNION ALL SELECT value + 1 FROM counter "
                "WHERE value < 10000000"
                ") SELECT sum(value) FROM counter"
            ).fetchone()
            return {"status": "unexpected"}
        finally:
            con.close()
            finished.set()

    monkeypatch.setattr(perception, "_status", deliberately_slow_status)
    started = time.monotonic()
    response = http_test_client.get("/perception/status")
    elapsed = time.monotonic() - started
    converged_before_response = finished.is_set()
    finished.wait(5)

    assert response.status_code == 503
    assert elapsed <= 1.1
    assert converged_before_response


@pytest.mark.parametrize(
    ("group_revision", "started_at_ms", "quoted_at_ms", "failure_reason", "legs_json"),
    (
        (999, 5, 6, "upstream", "[]"),
        (1, 9, 1, "upstream", "[]"),
        (1, 5, 6, None, "[]"),
        (1, 5, 6, "upstream", "not-json"),
    ),
)
def test_status_replays_failed_quote_contract(
    http_test_client,
    group_revision,
    started_at_ms,
    quoted_at_ms,
    failure_reason,
    legs_json,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    _seed_candidate_authority(db_path)
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_group_quote_batches("
            "id,group_id,group_revision,membership_hash,started_at_ms,"
            "quoted_at_ms,status,failure_reason,legs_json"
            ") SELECT 'failed','g-1',?,membership_hash,?,?,'failed',?,? "
            "FROM neg_risk_group_revisions WHERE group_id='g-1'",
            (
                group_revision,
                started_at_ms,
                quoted_at_ms,
                failure_reason,
                legs_json,
            ),
        )
    assert http_test_client.get("/perception/status").status_code == 503


def test_status_rejects_superseded_quote_without_exact_revision_authority(
    http_test_client,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    _seed_candidate_authority(db_path)
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_group_quote_batches("
            "id,group_id,group_revision,membership_hash,started_at_ms,"
            "quoted_at_ms,status,failure_reason,legs_json"
            ") SELECT 'superseded','g-1',999,membership_hash,3,4,"
            "'superseded',NULL,legs_json FROM neg_risk_group_quote_batches "
            "WHERE id='q-1'"
        )
    assert http_test_client.get("/perception/status").status_code == 503


@pytest.mark.parametrize(
    ("membership_hash", "quote_batch_id", "reason"),
    (
        ("ghost", None, "unavailable"),
        (None, "ghost", "unavailable"),
        (None, None, None),
    ),
)
def test_status_replays_unavailable_candidate_fact_contract(
    http_test_client,
    membership_hash,
    quote_batch_id,
    reason,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_candidate_watch_facts("
            "group_id,membership_hash,quote_batch_id,observed_at_ms,last_result,"
            "reason,bundle_cost,gross_edge_bps,max_bundle_size,priority_class,"
            "consecutive_failures,effective_interval_s,schedule_reason,next_due_at_ms"
            ") VALUES('ghost',?,?,1,'unavailable',?,NULL,NULL,NULL,"
            "'normal',1,1,'failure',2)",
            (membership_hash, quote_batch_id, reason),
        )
    assert http_test_client.get("/perception/status").status_code == 503


@pytest.mark.parametrize(
    ("event_id", "revision", "started_at_ms", "observed_at_ms"),
    (
        ("event-b", 2, 50, 50),
        ("e-1", 3, 101, 101),
    ),
)
def test_status_rejects_non_contiguous_or_identity_changing_group_history(
    http_test_client,
    event_id,
    revision,
    started_at_ms,
    observed_at_ms,
) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    _seed_candidate_authority(db_path)
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_group_revisions("
            "group_id,event_id,revision,membership_hash,started_at_ms,"
            "observed_at_ms,source_cursor,status,legs_json"
            ") SELECT group_id,?, ?,membership_hash,?,?,source_cursor,"
            "'invalidated',legs_json FROM neg_risk_group_revisions "
            "WHERE group_id='g-1' AND revision=1",
            (event_id, revision, started_at_ms, observed_at_ms),
        )
    for path in (
        "/perception/status",
        "/perception/groups?limit=10",
        "/perception/groups/g-1/history?limit=10",
    ):
        assert http_test_client.get(path).status_code == 503
