from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from polyarb.perception import resource_controller as resource_module
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.resource_controller import (
    ResourceController,
    ResourceSample,
    validate_resource_history,
)
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
        store.latest_resource_decision(required=True)


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
        store.latest_resource_decision(required=True)


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


def test_disabled_resource_consumer_does_not_parse_corrupt_history(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        con.execute(
            "INSERT INTO neg_risk_resource_samples(observed_at_ms,sample_json) "
            "VALUES(1,'not-json')"
        )

    assert store.latest_resource_decision(now_ms=2_000, required=False) is None


def test_resource_history_compacts_to_checkpoint_and_bounded_suffix(tmp_path) -> None:
    now = [2_000]
    controller = _controller(tmp_path, lambda: now[0])
    latest = None
    for sequence in range(1, 514):
        now[0] = 2_000 + sequence
        latest = controller.decide(_sample(observed_at_ms=now[0]))

    with controller._store._connect() as con:
        checkpoint = con.execute(
            "SELECT * FROM neg_risk_resource_authority_checkpoint WHERE id=1"
        ).fetchone()
        sample_count = con.execute(
            "SELECT COUNT(*) FROM neg_risk_resource_samples"
        ).fetchone()[0]
        decision_count = con.execute(
            "SELECT COUNT(*) FROM neg_risk_resource_decisions"
        ).fetchone()[0]
        replayed = validate_resource_history(con)

    assert checkpoint is not None
    assert checkpoint["through_sequence"] == 257
    assert checkpoint["compacted_sample_count"] == 257
    assert checkpoint["compacted_decision_count"] == 257
    assert sample_count == decision_count == 256
    assert replayed == latest
    assert replayed is not None
    assert replayed.sequence == 513


def test_resource_checkpoint_binds_suffix_tail_against_deletion(tmp_path) -> None:
    now = [2_000]
    controller = _controller(tmp_path, lambda: now[0])
    for sequence in range(1, 4):
        now[0] += 1
        controller.decide(_sample(observed_at_ms=now[0]))

    with sqlite3.connect(controller._store.db_path) as con:
        con.execute("DELETE FROM neg_risk_resource_decisions WHERE sequence=3")
        con.execute("DELETE FROM neg_risk_resource_samples WHERE id=3")
    with controller._store._connect() as con:
        with pytest.raises(ValueError, match="invalid-resource-history"):
            validate_resource_history(con)


def test_resource_checkpoint_hash_rejects_coherent_field_tamper(tmp_path) -> None:
    controller = _controller(tmp_path, lambda: 2_000)
    controller.decide(_sample())

    with sqlite3.connect(controller._store.db_path) as con:
        trigger = con.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND tbl_name='neg_risk_resource_authority_checkpoint' "
            "AND name LIKE '%update'"
        ).fetchone()
        assert trigger is not None
        con.execute(f'DROP TRIGGER "{trigger[0]}"')
        con.execute(
            "UPDATE neg_risk_resource_authority_checkpoint "
            "SET through_sequence=1"
        )
    with controller._store._connect() as con:
        with pytest.raises(ValueError, match="invalid-resource-checkpoint"):
            validate_resource_history(con)


def test_resource_consumer_rejects_missing_owner_checkpoint_trigger(tmp_path) -> None:
    controller = _controller(tmp_path, lambda: 2_000)
    controller.decide(_sample())
    with sqlite3.connect(controller._store.db_path) as con:
        trigger = con.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND tbl_name='neg_risk_resource_authority_checkpoint' "
            "AND name LIKE '%update'"
        ).fetchone()
        assert trigger is not None
        con.execute(f'DROP TRIGGER "{trigger[0]}"')

    with pytest.raises(ValueError, match="invalid-owner-authority-manifest"):
        controller._store.latest_resource_decision(
            now_ms=2_000,
            required=True,
        )


def test_resource_history_repeated_compaction_stays_bounded_across_restart(
    tmp_path,
) -> None:
    now = [2_000]
    controller = _controller(tmp_path, lambda: now[0])
    for sequence in range(1, 2_001):
        now[0] += 1
        controller.decide(_sample(observed_at_ms=now[0]))

    restarted = ResourceController(
        controller._store,
        clock_ms=lambda: now[0],
        cooldown_ms=1_000,
        _verify_store_authority=False,
    )
    with controller._store._connect() as con:
        checkpoint = con.execute(
            "SELECT * FROM neg_risk_resource_authority_checkpoint WHERE id=1"
        ).fetchone()
        sample_count = con.execute(
            "SELECT COUNT(*) FROM neg_risk_resource_samples"
        ).fetchone()[0]
        decision_count = con.execute(
            "SELECT COUNT(*) FROM neg_risk_resource_decisions"
        ).fetchone()[0]
        latest = validate_resource_history(con)

    assert checkpoint["generation"] == 2_000
    assert checkpoint["through_sequence"] == 1_542
    assert checkpoint["compacted_sample_count"] == 1_542
    assert checkpoint["compacted_decision_count"] == 1_542
    assert sample_count == decision_count == 458
    assert latest is not None and latest.sequence == 2_000
    now[0] += 1
    assert restarted.decide(_sample(observed_at_ms=now[0])).sequence == 2_001


def test_resource_validation_queries_only_cap_plus_one_suffix_rows(tmp_path) -> None:
    now = [2_000]
    controller = _controller(tmp_path, lambda: now[0])
    for _ in range(600):
        now[0] += 1
        controller.decide(_sample(observed_at_ms=now[0]))
    statements: list[str] = []

    with controller._store._connect() as con:
        con.set_trace_callback(statements.append)
        assert validate_resource_history(con) is not None

    normalized = tuple(" ".join(statement.split()) for statement in statements)
    assert any(
        'SELECT COUNT(*) FROM (SELECT 1 FROM "neg_risk_resource_samples" LIMIT 1025)'
        in statement
        for statement in normalized
    )
    assert any(
        'SELECT COUNT(*) FROM (SELECT 1 FROM "neg_risk_resource_decisions" LIMIT 1025)'
        in statement
        for statement in normalized
    )
    assert any(
        "ORDER BY d.sequence LIMIT 1025" in statement for statement in normalized
    )


def test_concurrent_high_water_resource_writers_serialize_without_loss(
    tmp_path,
) -> None:
    now = [2_000]
    controller = _controller(tmp_path, lambda: now[0])
    for _ in range(512):
        now[0] += 1
        controller.decide(_sample(observed_at_ms=now[0]))
    observed_at_ms = now[0] + 1

    def write_one() -> int:
        writer = ResourceController(
            controller._store,
            clock_ms=lambda: observed_at_ms,
            cooldown_ms=1_000,
            _verify_store_authority=False,
        )
        return writer.decide(_sample(observed_at_ms=observed_at_ms)).sequence

    with ThreadPoolExecutor(max_workers=2) as pool:
        sequences = sorted(pool.map(lambda _index: write_one(), range(2)))

    assert sequences == [513, 514]
    with controller._store._connect() as con:
        latest = validate_resource_history(con)
        assert latest is not None and latest.sequence == 514
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_resource_decisions"
        ).fetchone()[0] == 257


def test_resource_checkpoint_failure_rolls_back_sample_and_decision(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = _controller(tmp_path, lambda: 2_000)

    def fail_checkpoint(*_args, **_kwargs) -> None:
        raise RuntimeError("checkpoint-crash")

    monkeypatch.setattr(
        resource_module,
        "_publish_resource_checkpoint",
        fail_checkpoint,
    )
    with pytest.raises(RuntimeError, match="checkpoint-crash"):
        controller.decide(_sample())

    with controller._store._connect() as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_resource_samples"
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_resource_decisions"
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_resource_authority_checkpoint"
        ).fetchone()[0] == 0
        controller._store._assert_owner_journal_clean(con)


def test_resource_hard_limit_records_breadcrumb_and_next_writer_recovers(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [2_000]
    controller = _controller(tmp_path, lambda: now[0])
    monkeypatch.setattr(resource_module, "RESOURCE_SUFFIX_HARD_MAX_PAIRS", 1)
    controller.decide(_sample())
    now[0] = 2_001

    with pytest.raises(ValueError, match="resource-history-hard-limit"):
        controller.decide(_sample(observed_at_ms=now[0]))

    with controller._store._connect() as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_resource_decisions"
        ).fetchone()[0] == 1
        failure = con.execute(
            "SELECT * FROM neg_risk_evidence_failures WHERE component='resource'"
        ).fetchone()
        assert failure is not None
        assert failure["reason"] == "authority-invalid"
        assert failure["recovered_at_ms"] is None

    monkeypatch.setattr(resource_module, "RESOURCE_SUFFIX_HARD_MAX_PAIRS", 1_024)
    now[0] = 2_002
    assert controller.decide(_sample(observed_at_ms=now[0])).sequence == 2
    with controller._store._connect() as con:
        failure = con.execute(
            "SELECT * FROM neg_risk_evidence_failures WHERE component='resource'"
        ).fetchone()
        assert failure["recovered_at_ms"] == 2_002


def test_unresolved_resource_breadcrumb_blocks_component_control(tmp_path) -> None:
    controller = _controller(tmp_path, lambda: 2_000)
    controller.decide(_sample())
    resource_module._record_resource_evidence_failure(
        controller._store,
        2_001,
    )

    with controller._store._connect() as con:
        with pytest.raises(
            ValueError,
            match="unresolved-resource-evidence-failure",
        ):
            controller._store._validate_component_control_permission(
                con,
                "discovery",
                now_ms=2_001,
                require_resource_decision=True,
            )
