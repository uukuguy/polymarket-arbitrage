from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from polyarb.control_plane.qualification import (
    BREAKING_REASONS,
    QualificationError,
    QualificationFact,
    QualificationFactConflict,
    QualificationState,
    QualificationTerminalError,
    RollingQualificationPolicy,
)

NOW = datetime(2026, 8, 25, 0, 0, tzinfo=UTC)


def _policy(**kwargs: Any) -> RollingQualificationPolicy:
    return RollingQualificationPolicy(
        release_id="release-a",
        config_id="config-a",
        role_identity=("structure", "quote", "opportunity"),
        **kwargs,
    )


def _state(policy: RollingQualificationPolicy | None = None):
    selected = policy or _policy()
    return selected.new_epoch(started_at=NOW, epoch_id="epoch-a")


def _fact(
    fact_id: str,
    at: datetime,
    *,
    reason: str = "healthy",
    **kwargs: Any,
) -> QualificationFact:
    return QualificationFact(fact_id=fact_id, observed_at=at, reason=reason, **kwargs)


def test_contained_retry_keeps_epoch_accumulating() -> None:
    policy = _policy(max_gap_seconds=14_400)
    result = policy.apply(
        _state(policy),
        _fact(
            "retry-1",
            NOW + timedelta(hours=4),
            reason="recovery.retry",
            signature="upstream.timeout",
            recovery_duration_seconds=20,
            recovery_slo_seconds=60,
        ),
    )

    assert result.state is QualificationState.ACCUMULATING
    assert result.invalidated_at is None
    assert result.contained_recoveries == ("retry-1",)


def test_integrity_or_expired_lease_invalidates_exact_epoch() -> None:
    policy = _policy()
    for reason in ("lease.expired", "integrity.conflict"):
        result = policy.apply(
            _state(policy),
            _fact("break-" + reason, NOW + timedelta(hours=1), reason=reason),
        )
        assert result.state is QualificationState.INVALIDATED
        assert result.invalidated_at == NOW + timedelta(hours=1)
        assert result.invalidation_reason == reason


def test_recovery_confirmation_opens_new_epoch_automatically() -> None:
    policy = _policy()
    invalidated = policy.apply(
        _state(policy),
        _fact("lease", NOW + timedelta(hours=1), reason="lease.expired"),
    )
    recovering = policy.recovering(
        invalidated,
        started_at=NOW + timedelta(hours=1, seconds=1),
    )
    result = policy.apply(
        recovering,
        _fact(
            "recovery-ok",
            NOW + timedelta(hours=1, seconds=30),
            reason="recovery.confirmed",
            recovery_confirmed=True,
        ),
    )

    assert recovering.state is QualificationState.RECOVERING
    assert invalidated.state is QualificationState.INVALIDATED
    assert invalidated.invalidated_at == NOW + timedelta(hours=1)
    assert invalidated.invalidation_reason == "lease.expired"
    assert recovering.invalidated_at is None
    assert recovering.invalidation_reason is None
    assert recovering.previous_epoch_id == invalidated.epoch_id
    assert recovering.epoch_id != invalidated.epoch_id
    assert result.state is QualificationState.ACCUMULATING
    assert result.started_at == NOW + timedelta(hours=1, seconds=30)
    assert result.previous_epoch_id == invalidated.epoch_id
    assert result.epoch_id not in {invalidated.epoch_id, recovering.epoch_id}
    assert result.facts == (
        _fact(
            "recovery-ok",
            NOW + timedelta(hours=1, seconds=30),
            reason="recovery.confirmed",
            recovery_confirmed=True,
        ),
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("policy_version", "policy-b"),
        ("release_id", "release-b"),
        ("config_id", "config-b"),
        ("role_identity", ("other-role",)),
    ],
)
def test_recovery_confirmation_identity_drift_fails_closed(
    field: str,
    value: object,
) -> None:
    policy = _policy()
    invalidated = policy.apply(
        _state(policy),
        _fact("lease", NOW + timedelta(hours=1), reason="lease.expired"),
    )
    recovering = policy.recovering(
        invalidated,
        started_at=NOW + timedelta(hours=1, seconds=1),
    )
    with pytest.raises(QualificationError, match="identity conflict"):
        policy.apply(
            recovering,
            _fact(
                "recovery-drift-" + field,
                NOW + timedelta(hours=1, seconds=30),
                reason="recovery.confirmed",
                recovery_confirmed=True,
                **{field: value},
            ),
        )

    assert recovering.state is QualificationState.RECOVERING
    assert recovering.previous_epoch_id == invalidated.epoch_id


def test_recovery_confirmation_accepts_omitted_or_matching_identity() -> None:
    policy = _policy()
    invalidated = policy.apply(
        _state(policy),
        _fact("lease", NOW + timedelta(hours=1), reason="lease.expired"),
    )
    recovering = policy.recovering(
        invalidated,
        started_at=NOW + timedelta(hours=1, seconds=1),
    )
    result = policy.apply(
        recovering,
        _fact(
            "recovery-ok-explicit",
            NOW + timedelta(hours=1, seconds=30),
            reason="recovery.confirmed",
            recovery_confirmed=True,
            policy_version=policy.policy_version,
            release_id=policy.release_id,
            config_id=policy.config_id,
            role_identity=policy.role_identity,
        ),
    )
    assert result.state is QualificationState.ACCUMULATING
    assert result.previous_epoch_id == invalidated.epoch_id


def test_exact_24_hour_boundary_qualifies_only_with_coverage() -> None:
    policy = _policy(required_seconds=86_400, max_gap_seconds=3_600)
    state = _state(policy)
    for index in range(25):
        state = policy.apply(
            state,
            _fact(f"sample-{index}", NOW + timedelta(hours=index)),
        )

    assert state.state is QualificationState.QUALIFIED
    assert state.qualified_at == NOW + timedelta(hours=24)
    assert state.coverage_seconds == 86_400


def test_gap_equal_to_limit_is_allowed_but_gap_over_limit_breaks() -> None:
    policy = _policy(max_gap_seconds=60)
    allowed = policy.apply(_state(policy), _fact("at-limit", NOW + timedelta(seconds=60)))
    assert allowed.state is QualificationState.ACCUMULATING
    broken = policy.apply(
        allowed,
        _fact("over-limit", NOW + timedelta(seconds=121)),
    )
    assert broken.state is QualificationState.INVALIDATED
    assert broken.invalidation_reason == "evidence.gap"


def test_recovery_started_appends_without_qualifying_accumulating_epoch() -> None:
    policy = _policy(required_seconds=60, max_gap_seconds=3_600)
    started = policy.apply(
        _state(policy),
        _fact("retryable-runtime", NOW + timedelta(seconds=120), reason="recovery.started"),
    )

    assert started.state is QualificationState.ACCUMULATING
    assert started.qualified_at is None
    assert started.last_fact_at == NOW + timedelta(seconds=120)
    assert started.facts == (
        _fact("retryable-runtime", NOW + timedelta(seconds=120), reason="recovery.started"),
    )


@pytest.mark.parametrize("reason", ["healthy", "progress"])
def test_pending_recovery_start_blocks_healthy_or_progress_from_qualifying(
    reason: str,
) -> None:
    policy = _policy(required_seconds=60, max_gap_seconds=3_600)
    started = policy.apply(
        _state(policy),
        _fact("retryable-runtime", NOW + timedelta(seconds=60), reason="recovery.started"),
    )

    observed = policy.apply(
        started,
        _fact("post-start-" + reason, NOW + timedelta(seconds=61), reason=reason),
    )

    assert observed.state is QualificationState.ACCUMULATING
    assert observed.qualified_at is None
    assert observed.last_fact_at == NOW + timedelta(seconds=61)
    assert [fact.reason for fact in observed.facts] == ["recovery.started", reason]


def test_contained_recovery_clears_pending_start_and_allows_qualification() -> None:
    policy = _policy(required_seconds=60, max_gap_seconds=3_600)
    started = policy.apply(
        _state(policy),
        _fact("retryable-runtime", NOW + timedelta(seconds=60), reason="recovery.started"),
    )
    observed = policy.apply(started, _fact("post-start-healthy", NOW + timedelta(seconds=61)))

    qualified = policy.apply(
        observed,
        _fact(
            "contained-retry",
            NOW + timedelta(seconds=62),
            reason="recovery.retry",
            signature="upstream.timeout",
            recovery_duration_seconds=20,
            recovery_slo_seconds=60,
        ),
    )

    assert qualified.state is QualificationState.QUALIFIED
    assert qualified.qualified_at == NOW + timedelta(seconds=60)
    assert [fact.reason for fact in qualified.facts] == [
        "recovery.started",
        "healthy",
        "recovery.retry",
    ]


def test_recovery_confirmed_clears_pending_start_and_allows_qualification() -> None:
    policy = _policy(required_seconds=60, max_gap_seconds=3_600)
    started = policy.apply(
        _state(policy),
        _fact("retryable-runtime", NOW + timedelta(seconds=60), reason="recovery.started"),
    )
    observed = policy.apply(started, _fact("post-start-healthy", NOW + timedelta(seconds=61)))

    qualified = policy.apply(
        observed,
        _fact(
            "confirmed",
            NOW + timedelta(seconds=62),
            reason="recovery.confirmed",
            recovery_confirmed=True,
        ),
    )

    assert qualified.state is QualificationState.QUALIFIED
    assert qualified.qualified_at == NOW + timedelta(seconds=60)
    assert [fact.reason for fact in qualified.facts] == [
        "recovery.started",
        "healthy",
        "recovery.confirmed",
    ]


@pytest.mark.parametrize(
    "reason",
    sorted(BREAKING_REASONS),
)
def test_breaking_reason_matrix_invalidates(reason: str) -> None:
    policy = _policy()
    result = policy.apply(_state(policy), _fact("break", NOW, reason=reason))
    assert result.state is QualificationState.INVALIDATED
    assert result.invalidation_reason == reason


def test_unresolved_p1_and_three_freshness_classes_break() -> None:
    policy = _policy()
    for index, reason in enumerate(
        ("incident.p1-slo", "freshness.structure", "freshness.quote", "freshness.opportunity")
    ):
        result = policy.apply(
            _state(policy),
            _fact(f"fact-{index}", NOW, reason=reason),
        )
        assert result.state is QualificationState.INVALIDATED


def test_repeated_signature_budget_breaks_on_next_occurrence() -> None:
    policy = _policy(signature_budget=2)
    state = _state(policy)
    for index in range(2):
        state = policy.apply(
            state,
            _fact(
                f"retry-{index}",
                NOW + timedelta(minutes=index),
                reason="recovery.retry",
                signature="same-failure",
                recovery_duration_seconds=1,
                recovery_slo_seconds=10,
            ),
        )
    result = policy.apply(
        state,
        _fact(
            "retry-2",
            NOW + timedelta(minutes=2),
            reason="recovery.retry",
            signature="same-failure",
            recovery_duration_seconds=1,
            recovery_slo_seconds=10,
        ),
    )
    assert result.state is QualificationState.INVALIDATED
    assert result.invalidation_reason == "recovery.signature-budget"


def test_contained_process_replacement_must_finish_within_slo() -> None:
    policy = _policy()
    state = _state(policy)
    contained = policy.apply(
        state,
        _fact(
            "process-replaced",
            NOW,
            reason="recovery.process-replacement",
            recovery_duration_seconds=30,
            recovery_slo_seconds=60,
        ),
    )
    assert contained.state is QualificationState.ACCUMULATING
    broken = policy.apply(
        state,
        _fact(
            "process-too-slow",
            NOW,
            reason="recovery.process-replacement",
            recovery_duration_seconds=61,
            recovery_slo_seconds=60,
        ),
    )
    assert broken.state is QualificationState.INVALIDATED
    assert broken.invalidation_reason == "recovery.slo"


def test_identity_drift_and_progress_regression_break() -> None:
    policy = _policy()
    identity = policy.apply(
        _state(policy),
        _fact("release-drift", NOW, release_id="release-b"),
    )
    assert identity.state is QualificationState.INVALIDATED
    assert identity.invalidation_reason == "identity.release"

    state = policy.apply(_state(policy), _fact("progress-1", NOW, progress_count=10))
    regression = policy.apply(
        state,
        _fact("progress-2", NOW + timedelta(seconds=1), progress_count=9),
    )
    assert regression.state is QualificationState.INVALIDATED
    assert regression.invalidation_reason == "progress.regressed"


def test_duplicate_fact_is_idempotent_and_conflict_fails_closed() -> None:
    policy = _policy()
    state = _state(policy)
    fact = _fact("same", NOW)
    first = policy.apply(state, fact)
    assert policy.apply(first, fact) is first
    with pytest.raises(QualificationFactConflict):
        policy.apply(first, _fact("same", NOW + timedelta(seconds=1)))


def test_terminal_epochs_replay_exact_fact_but_reject_new_mutation() -> None:
    policy = _policy()
    invalidated = policy.apply(_state(policy), _fact("break", NOW, reason="lease.expired"))
    assert policy.apply(invalidated, _fact("break", NOW, reason="lease.expired")) is invalidated
    with pytest.raises(QualificationTerminalError):
        policy.apply(invalidated, _fact("new", NOW + timedelta(seconds=1)))
    with pytest.raises(QualificationTerminalError):
        policy.apply(
            invalidated,
            _fact("recovery-start", NOW + timedelta(seconds=1), reason="recovery.started"),
        )
    with pytest.raises(QualificationTerminalError):
        policy.apply(
            invalidated,
            _fact(
                "recovery-ok",
                NOW + timedelta(seconds=1),
                reason="recovery.confirmed",
                recovery_confirmed=True,
            ),
        )

    short_policy = _policy(required_seconds=1, max_gap_seconds=60)
    state = _state(short_policy)
    qualified = short_policy.apply(state, _fact("finish", NOW + timedelta(seconds=1)))
    assert qualified.state is QualificationState.QUALIFIED
    assert short_policy.apply(qualified, _fact("finish", NOW + timedelta(seconds=1))) is qualified
    with pytest.raises(QualificationTerminalError):
        short_policy.apply(qualified, _fact("new", NOW + timedelta(seconds=2)))


def test_decision_rejects_impossible_four_state_combinations() -> None:
    policy = _policy()
    initial = _state(policy)
    with pytest.raises(ValueError, match="only invalidated"):
        replace(initial, invalidated_at=NOW, invalidation_reason="lease.expired")
    with pytest.raises(ValueError, match="qualified decision"):
        replace(initial, state=QualificationState.QUALIFIED)
    with pytest.raises(ValueError, match="previous_epoch_id"):
        replace(initial, state=QualificationState.RECOVERING)
    fact = _fact("sample", NOW)
    with pytest.raises(ValueError, match="only previous epoch"):
        replace(
            initial,
            state=QualificationState.RECOVERING,
            previous_epoch_id="epoch-old",
            facts=(fact,),
            fact_digests=((fact.fact_id, fact.digest),),
        )


@pytest.mark.parametrize(
    "fact_kwargs",
    [
        {"fact_id": "naive", "observed_at": datetime(2026, 8, 25), "reason": "healthy"},
        {"fact_id": "negative", "observed_at": NOW, "reason": "healthy", "count": -1},
        {"fact_id": "unknown", "observed_at": NOW, "reason": "not-a-reason"},
    ],
)
def test_invalid_fact_values_are_rejected(fact_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        fact = QualificationFact(**fact_kwargs)
        policy = RollingQualificationPolicy()
        policy.apply(policy.new_epoch(started_at=NOW), fact)


def test_out_of_order_fact_is_rejected() -> None:
    policy = _policy()
    state = policy.apply(_state(policy), _fact("later", NOW + timedelta(seconds=10)))
    with pytest.raises(ValueError, match="ordered"):
        policy.apply(state, _fact("earlier", NOW + timedelta(seconds=9)))
