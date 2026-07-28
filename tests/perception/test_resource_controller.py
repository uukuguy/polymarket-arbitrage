from __future__ import annotations

import pytest

from polyarb.perception.incidents import IncidentManager
from polyarb.perception.resource_controller import ResourceController, ResourceSample
from polyarb.perception.store import OpportunityPerceptionStore


def _controller(tmp_path, clock):
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    return ResourceController(
        store,
        clock_ms=clock,
        cooldown_ms=1_000,
        _verify_store_authority=False,
    )


def _sample(**changes):
    values = {
        "candidate_count": 2,
        "candidate_quote_p95_ms": 5_000,
        "candidate_missing_quote_count": 0,
        "candidate_worker_ok": True,
        "discovery_worker_ok": True,
        "reconciliation_running": True,
        "previous_discovery_batch_limit": 50,
        "observed_at_ms": 2_000,
    }
    values.update(changes)
    return ResourceSample(**values)


def test_hot_quote_age_sheds_reconciliation_before_discovery(tmp_path) -> None:
    controller = _controller(tmp_path, lambda: 2_000)
    decision = controller.decide(_sample(candidate_quote_p95_ms=25_000))
    assert decision.mode == "protect-hot-path"
    assert decision.reconciliation_enabled is False
    assert decision.discovery_batch_limit < decision.previous_discovery_batch_limit
    assert decision.high_candidate_interval_multiplier == 1.0


def test_empty_candidate_set_expands_discovery_without_claiming_health(tmp_path) -> None:
    controller = _controller(tmp_path, lambda: 2_000)
    decision = controller.decide(_sample(candidate_count=0))
    assert decision.discovery_batch_limit > decision.previous_discovery_batch_limit
    assert decision.reason == "empty-candidate-exploration"
    assert decision.health_claimed is False


def test_controller_rejects_nonfinite_or_negative_samples(tmp_path) -> None:
    controller = _controller(tmp_path, lambda: 2_000)
    with pytest.raises(ValueError, match="invalid-resource-sample"):
        controller.decide(_sample(candidate_quote_p95_ms=float("nan")))
    with pytest.raises(ValueError, match="invalid-resource-sample"):
        controller.decide(_sample(candidate_count=-1))


def test_cooldown_prevents_flapping_but_allows_more_shedding(tmp_path) -> None:
    now = [2_000]
    controller = _controller(tmp_path, lambda: now[0])
    hot = controller.decide(_sample(candidate_quote_p95_ms=25_000))
    now[0] = 2_100
    still_hot = controller.decide(_sample(candidate_quote_p95_ms=5_000, observed_at_ms=2_100))
    assert still_hot.mode == hot.mode
    now[0] = 3_100
    recovered = controller.decide(_sample(candidate_quote_p95_ms=5_000, observed_at_ms=3_100))
    assert recovered.mode == "normal"


def test_default_controller_rejects_forged_candidate_authority(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    controller = ResourceController(store, clock_ms=lambda: 2_000)
    with pytest.raises(ValueError, match="resource-sample-authority-mismatch"):
        controller.decide(_sample(candidate_count=2))

    actual = controller.capture_sample(
        reconciliation_running=False,
        previous_discovery_batch_limit=50,
    )
    assert controller.decide(actual).reason == "empty-candidate-exploration"


def test_runtime_replays_all_resource_evidence_before_applying_latest(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    controller = ResourceController(store, clock_ms=lambda: 2_000)
    sample = controller.capture_sample(
        reconciliation_running=False,
        previous_discovery_batch_limit=50,
    )
    controller.decide(sample)
    controller.decide(sample)
    with store._connect() as con:
        con.execute("UPDATE neg_risk_resource_samples SET sample_json='[]' WHERE id=1")
    with pytest.raises(ValueError, match="invalid-resource"):
        store.latest_resource_decision()


def test_resource_history_rejects_stale_sequence_and_sample_mismatch(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    controller = ResourceController(store, clock_ms=lambda: 2_000)
    sample = controller.capture_sample(
        reconciliation_running=False,
        previous_discovery_batch_limit=50,
    )
    controller.decide(sample)
    controller.decide(sample)
    with store._connect() as con:
        con.execute("UPDATE neg_risk_resource_decisions SET sample_id=1 WHERE id=2")
    with pytest.raises(ValueError, match="invalid-resource"):
        store.latest_resource_decision()


def test_repeated_samples_do_not_extend_hysteresis_transition_anchor(tmp_path) -> None:
    now = [2_000]
    controller = _controller(tmp_path, lambda: now[0])
    controller.decide(_sample(candidate_quote_p95_ms=25_000))
    for observed in (2_200, 2_400, 2_600, 2_800):
        now[0] = observed
        assert (
            controller.decide(_sample(candidate_quote_p95_ms=5_000, observed_at_ms=observed)).mode
            == "protect-hot-path"
        )
    now[0] = 3_100
    assert (
        controller.decide(_sample(candidate_quote_p95_ms=5_000, observed_at_ms=3_100)).mode
        == "normal"
    )


def test_discovery_incident_does_not_slow_normal_candidate(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    manager = IncidentManager(store, clock_ms=lambda: 2_000)
    incident = manager.detect("discovery", "child-timeout", {})
    manager.transition(incident.id, "classified", {})
    controller = ResourceController(store, clock_ms=lambda: 2_000)
    sample = controller.capture_sample(
        reconciliation_running=False,
        previous_discovery_batch_limit=50,
    )
    decision = controller.decide(sample)
    assert decision.normal_candidate_interval_multiplier == 1.0
    assert decision.mode == "empty-candidate-exploration"


def test_expired_resource_decision_fails_closed_at_runtime(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    controller = ResourceController(
        store,
        clock_ms=lambda: 2_000,
        decision_ttl_ms=500,
    )
    sample = controller.capture_sample(
        reconciliation_running=False,
        previous_discovery_batch_limit=50,
    )
    controller.decide(sample)
    with pytest.raises(ValueError, match="stale-resource-decision"):
        store.latest_resource_decision(now_ms=2_501, required=True)
