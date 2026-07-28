from __future__ import annotations

import pytest

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
