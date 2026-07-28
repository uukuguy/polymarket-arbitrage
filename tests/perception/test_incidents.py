from __future__ import annotations

import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from polyarb.http.health import read_perception_recovery_health
from polyarb.perception.http_probe import BoundedHttpProbeWriter
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
    batch = GroupQuoteBatch.complete(
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
    store.publish_candidate_success(
        batch,
        observed_at_ms=quoted_at_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="test",
        next_due_at_ms=quoted_at_ms + 15_000,
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


def test_candidate_recovery_rejects_split_quote_and_fact_writes(tmp_path) -> None:
    store = _store(tmp_path)
    now = [2_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect("candidate:g-1", "clob-timeout", {})
    manager.transition(incident.id, "classified", {})
    manager.transition(incident.id, "contained", {})
    manager.transition(incident.id, "recovering", {"retry": 1})

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
    batch = GroupQuoteBatch.complete(
        group_id="g-1",
        membership_hash=revision.membership_hash,
        quote_batch_id="qb-split",
        started_at_ms=2_050,
        quoted_at_ms=2_100,
        legs=(
            GroupQuoteLeg("t-1", revision.membership_hash, 0.4, 10, "executable"),
            GroupQuoteLeg("t-2", revision.membership_hash, 0.5, 10, "executable"),
        ),
    )
    store.publish_quote_batch(batch)
    store.record_candidate_watch_fact(
        group_id="g-1",
        membership_hash=revision.membership_hash,
        quote_batch_id=batch.quote_batch_id,
        observed_at_ms=batch.quoted_at_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="split",
        next_due_at_ms=17_100,
    )
    now[0] = 2_200

    with pytest.raises(RecoveryEvidenceRequiredError):
        manager.transition(
            incident.id,
            "verified",
            {
                "quote_batch_id": batch.quote_batch_id,
                "group_id": "g-1",
                "membership_hash": revision.membership_hash,
            },
        )


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


def test_transition_rejects_clock_regression_without_appending(tmp_path) -> None:
    store = _store(tmp_path)
    now = [2_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect("discovery", "timeout", {})
    now[0] = 1_000

    with pytest.raises(InvalidIncidentTransitionError, match="incident-clock-regression"):
        manager.transition(incident.id, "classified", {})

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_incident_events WHERE incident_id=?",
            (incident.id,),
        ).fetchone()[0] == 1


def test_http_verification_requires_expected_release_and_bounded_probe(tmp_path) -> None:
    store = _store(tmp_path)
    manager = IncidentManager(store)
    incident = manager.detect("http", "unresponsive", {"release_id": "r-2"})
    manager.transition(incident.id, "classified", {})
    manager.transition(incident.id, "contained", {})
    manager.transition(
        incident.id,
        "recovering",
        {"release_id": "r-2", "probe_nonce": "recovery-nonce"},
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            body = b'{"releaseId":"r-2"}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        writer = BoundedHttpProbeWriter(store, timeout_s=2.0)
        wrong = writer.probe(
            f"http://127.0.0.1:{server.server_port}/healthz",
            expected_release_id="r-wrong",
            probe_nonce="wrong-nonce",
        )
        assert wrong.responsive is False
        with pytest.raises(RecoveryEvidenceRequiredError):
            manager.transition(
                incident.id,
                "verified",
                {"release_id": "r-2", "probe_nonce": "recovery-nonce"},
            )
        valid = writer.probe(
            f"http://127.0.0.1:{server.server_port}/healthz",
            expected_release_id="r-2",
            probe_nonce="recovery-nonce",
        )
        assert valid.responsive is True
        assert (
            manager.transition(
                incident.id,
                "verified",
                {"release_id": "r-2", "probe_nonce": "recovery-nonce"},
            ).state
            == "verified"
        )
    finally:
        server.shutdown()
        server.server_close()

    with pytest.raises(PermissionError):
        store.record_http_probe(
            release_id="r-2",
            started_at_ms=1,
            finished_at_ms=2,
            responsive=True,
        )


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


def test_historical_non_object_incident_evidence_fails_closed(tmp_path) -> None:
    store = _store(tmp_path)
    manager = IncidentManager(store, clock_ms=lambda: 1_000)
    incident = manager.detect("discovery", "timeout", {"cursor": "c-1"})
    manager.transition(incident.id, "classified", {})
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE neg_risk_incident_events SET evidence_json='[]' "
            "WHERE incident_id=? AND sequence=1",
            (incident.id,),
        )
    with pytest.raises(ValueError, match="invalid-incident"):
        manager.open_incidents()
    assert read_perception_recovery_health(store.db_path).evidence_consistent is False


def test_candidate_recovery_rejects_malformed_complete_quote_row(tmp_path) -> None:
    store = _store(tmp_path)
    now = [2_000]
    manager = IncidentManager(store, clock_ms=lambda: now[0])
    incident = manager.detect("candidate:g-1", "clob-timeout", {})
    manager.transition(incident.id, "classified", {})
    manager.transition(incident.id, "contained", {})
    manager.transition(incident.id, "recovering", {"retry": 1})
    revision = _publish_quote(store, quoted_at_ms=2_100)
    with sqlite3.connect(store.db_path) as con:
        malformed = [
            ["wrong-token", revision.membership_hash, 0.4, 10.0, "executable"],
            ["t-2", revision.membership_hash, 0.5, 10.0, "executable"],
        ]
        con.execute(
            "UPDATE neg_risk_group_quote_batches SET legs_json=? WHERE id='qb-1'",
            (json.dumps(malformed),),
        )
    now[0] = 2_200
    with pytest.raises(RecoveryEvidenceRequiredError):
        manager.transition(
            incident.id,
            "verified",
            {
                "quote_batch_id": "qb-1",
                "group_id": "g-1",
                "membership_hash": revision.membership_hash,
            },
        )
