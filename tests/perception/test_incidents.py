from __future__ import annotations

import sqlite3

import pytest

from polyarb.http.health import read_perception_recovery_health
from polyarb.perception.incidents import (
    IncidentManager,
    InvalidIncidentTransitionError,
    RecoveryEvidenceRequiredError,
)
from polyarb.perception.models import GroupLeg, GroupQuoteBatch, GroupQuoteLeg, GroupRevision
from polyarb.perception.resource_controller import ResourceController, ResourceSample
from polyarb.perception.store import OpportunityPerceptionStore


def _store(tmp_path):
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    return store


def _publish_quote(store, *, quoted_at_ms: int = 4_000):
    revision = GroupRevision.certified(
        group_id="g-1",
        event_id="e-1",
        revision=1,
        started_at_ms=500,
        observed_at_ms=1_000,
        source_cursor="c-1",
        legs=(
            GroupLeg("m-1", "c-1", "t-1", "one"),
            GroupLeg("m-2", "c-2", "t-2", "two"),
        ),
    )
    store.publish_group_revision(revision)
    store.publish_quote_batch(
        GroupQuoteBatch.complete(
            group_id="g-1",
            membership_hash=revision.membership_hash,
            quote_batch_id="qb-1",
            started_at_ms=quoted_at_ms - 100,
            quoted_at_ms=quoted_at_ms,
            legs=(
                GroupQuoteLeg("t-1", revision.membership_hash, 0.4, 10, "executable"),
                GroupQuoteLeg("t-2", revision.membership_hash, 0.5, 10, "executable"),
            ),
        )
    )
    return revision


def test_incident_cannot_close_without_post_recovery_writer_evidence(tmp_path) -> None:
    store = _store(tmp_path)
    now = [2_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect(
        "candidate:g-1",
        "clob-timeout",
        {"membership_hash": "pending"},
    )
    manager.transition(incident.id, "classified", {"class": "upstream"})
    manager.transition(incident.id, "contained", {"circuit_open": True})
    manager.transition(incident.id, "recovering", {"retry": 1})

    with pytest.raises(RecoveryEvidenceRequiredError):
        manager.transition(incident.id, "verified", {"quote_batch_id": "qb-1"})

    revision = _publish_quote(store)
    now[0] = 5_000
    verified = manager.transition(
        incident.id,
        "verified",
        {
            "quote_batch_id": "qb-1",
            "group_id": "g-1",
            "membership_hash": revision.membership_hash,
        },
    )
    assert verified.state == "verified"


def test_detect_is_idempotent_and_terminal_incident_does_not_reopen(tmp_path) -> None:
    manager = IncidentManager(_store(tmp_path), clock_ms=lambda: 1_000)
    first = manager.detect("discovery", "timeout", {"cursor": "c-1"})
    duplicate = manager.detect("discovery", "timeout", {"cursor": "c-1"})
    assert duplicate.id == first.id

    with pytest.raises(InvalidIncidentTransitionError):
        manager.transition(first.id, "contained", {})


def test_concurrent_transition_uses_latest_append_only_state(tmp_path) -> None:
    store = _store(tmp_path)
    manager = IncidentManager(store, clock_ms=lambda: 1_000)
    incident = manager.detect("http", "unresponsive", {"release_id": "r-1"})
    manager.transition(incident.id, "classified", {})

    other = IncidentManager(
        OpportunityPerceptionStore(store.db_path),
        clock_ms=lambda: 1_001,
    )
    manager.transition(incident.id, "contained", {})
    with pytest.raises(InvalidIncidentTransitionError):
        other.transition(incident.id, "contained", {})

    with sqlite3.connect(store.db_path) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM neg_risk_incident_events WHERE incident_id=?",
            (incident.id,),
        ).fetchone()[0]
    assert count == 3


def test_http_verification_requires_expected_release_and_bounded_probe(tmp_path) -> None:
    store = _store(tmp_path)
    now = [2_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect("http", "unresponsive", {"release_id": "r-2"})
    manager.transition(incident.id, "classified", {})
    manager.transition(incident.id, "contained", {})
    manager.transition(incident.id, "recovering", {"release_id": "r-2"})
    store.record_http_probe(
        release_id="r-wrong",
        started_at_ms=2_100,
        finished_at_ms=2_120,
        responsive=True,
    )
    now[0] = 2_300
    with pytest.raises(RecoveryEvidenceRequiredError):
        manager.transition(incident.id, "verified", {"release_id": "r-2"})
    store.record_http_probe(
        release_id="r-2",
        started_at_ms=2_200,
        finished_at_ms=2_250,
        responsive=True,
    )
    assert manager.transition(incident.id, "verified", {"release_id": "r-2"}).state == "verified"


def test_health_fails_closed_when_old_resource_evidence_is_rewritten(tmp_path) -> None:
    store = _store(tmp_path)
    controller = ResourceController(store, clock_ms=lambda: 2_000)
    sample = ResourceSample(
        candidate_count=0,
        candidate_quote_p95_ms=None,
        candidate_missing_quote_count=0,
        candidate_worker_ok=True,
        discovery_worker_ok=True,
        reconciliation_running=False,
        previous_discovery_batch_limit=50,
        observed_at_ms=2_000,
    )
    controller.decide(sample)
    controller.decide(sample)
    healthy = read_perception_recovery_health(store.db_path)
    assert healthy.evidence_consistent is True
    assert healthy.resource_mode == "empty-candidate-exploration"

    with sqlite3.connect(store.db_path) as con:
        con.execute("UPDATE neg_risk_resource_decisions SET mode='normal' WHERE id=1")
    corrupt = read_perception_recovery_health(store.db_path)
    assert corrupt.evidence_consistent is False
    assert corrupt.resource_mode == "unavailable"


def test_resource_incident_requires_post_recovery_durable_decision(tmp_path) -> None:
    store = _store(tmp_path)
    now = [2_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect("resource", "controller-failure", {})
    manager.transition(incident.id, "classified", {})
    manager.transition(incident.id, "contained", {})
    manager.transition(incident.id, "recovering", {"retry": 1})
    with pytest.raises(RecoveryEvidenceRequiredError):
        manager.transition(incident.id, "verified", {"decision_id": 1})

    controller = ResourceController(store, clock_ms=lambda: 2_100)
    sample = controller.capture_sample(
        reconciliation_running=False,
        previous_discovery_batch_limit=50,
    )
    controller.decide(sample)
    now[0] = 2_200
    assert (
        manager.transition(
            incident.id,
            "verified",
            {"decision_id": store.latest_resource_decision_id()},
        ).state
        == "verified"
    )
