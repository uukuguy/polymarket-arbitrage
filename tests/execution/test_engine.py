"""Tests for the T4 Execution Pipeline orchestration shell.

Locks the atomic-execution contract:
- Sequential execution of plan.legs.
- First-leg failure → ABORT all subsequent legs (atomic invariant).
- Subsequent failure → PARTIAL status with explicit per-leg detail.
- Per-leg retry up to ExecutionConfig.retry_attempts before declaring fail.
- Position tracker mutated ONLY for successful legs.
- Pluggable leg executor — tests inject simulators; production injects
  py-clob-client adapter (T5+ scope).
"""
from __future__ import annotations

import pytest

from polyarb.execution.engine import (
    ExecutionEngine,
    ExecutionLegResult,
    ExecutionResult,
    ExecutionStatus,
)
from polyarb.models.signal import (
    ExecutionLeg,
    ExecutionPlan,
    RoutingDecision,
)
from polyarb.routing.config import ExecutionConfig
from polyarb.routing.position_tracker import PositionTracker


def _make_leg(idx: int, action: str = "buy") -> ExecutionLeg:
    return ExecutionLeg(
        leg_id=f"leg-{idx}",
        exchange="polymarket" if idx == 0 else "clob",
        action=action,
        asset=f"asset-{idx}",
        size=100.0,
        limit_price=None if idx == 0 else 0.5,
        estimated_price=0.5,
        estimated_cost=2.5,
        hedge_ratio=1.0,
    )


def _make_decision(n_legs: int = 2, profit_abs: float = 5.0) -> RoutingDecision:
    legs = [_make_leg(i) for i in range(n_legs)]
    plan = ExecutionPlan(
        signal_id="sig-1",
        legs=legs,
        total_estimated_cost=sum(l.estimated_cost for l in legs),
        profit_threshold_pct=1.0,
    )
    return RoutingDecision(
        signal_id="sig-1",
        plan=plan,
        is_profitable=True,
        expected_profit_pct=5.0,
        expected_profit_abs=profit_abs,
        reason="test",
    )


# ──────────────────────────────────────────────────────────────────────────
# Happy paths
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_all_legs_succeed_completed_status():
    """Default executor (always succeeds) → COMPLETED."""
    engine = ExecutionEngine()
    decision = _make_decision(n_legs=2)
    result = await engine.execute(decision)

    assert result.status == ExecutionStatus.COMPLETED
    assert result.legs_executed == 2
    assert result.legs_total == 2
    assert len(result.leg_results) == 2
    assert all(r.success and r.attempts == 1 for r in result.leg_results)
    assert result.error_message is None
    assert result.realized_pnl == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_executor_receives_leg_and_attempt():
    """Pluggable executor receives the ExecutionLeg + 1-indexed attempt num."""
    seen: list[tuple[str, int]] = []

    async def spy(leg: ExecutionLeg, attempt: int):
        seen.append((leg.leg_id, attempt))
        return True, None

    engine = ExecutionEngine(leg_executor=spy)
    decision = _make_decision(n_legs=2)
    await engine.execute(decision)

    assert seen == [("leg-0", 1), ("leg-1", 1)], (
        f"executor must see (leg_id, attempt) in sequential order; saw {seen}"
    )


# ──────────────────────────────────────────────────────────────────────────
# Atomic abort invariant
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_first_leg_fail_aborts_subsequent_legs():
    """First leg fails → leg 1 + 2 NOT attempted, status=ABORTED."""
    seen: list[str] = []

    async def fail_first(leg: ExecutionLeg, attempt: int):
        seen.append(leg.leg_id)
        return False, "venue rejected"

    # No retries so the first failure is final.
    config = ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0)
    engine = ExecutionEngine(config=config, leg_executor=fail_first)
    decision = _make_decision(n_legs=3)
    result = await engine.execute(decision)

    assert result.status == ExecutionStatus.ABORTED
    assert result.legs_executed == 0
    assert seen == ["leg-0"], (
        f"only leg-0 should be attempted under abort; got {seen}"
    )
    # leg_results still has 3 entries — legs 1 and 2 marked skipped.
    assert len(result.leg_results) == 3
    assert result.leg_results[0].success is False
    assert result.leg_results[1].skipped is True
    assert result.leg_results[2].skipped is True
    assert "leg[0] failed" in result.error_message
    assert "leg[1] skipped" in result.error_message


@pytest.mark.asyncio
async def test_second_leg_fail_marks_partial_not_aborted():
    """Leg-0 succeeds, leg-1 fails → PARTIAL (atomic invariant intact since
    leg-0 already committed; we'd hedge or unwind later — T5+ scope)."""
    call_count = {"n": 0}

    async def fail_second(leg: ExecutionLeg, attempt: int):
        call_count["n"] += 1
        if leg.leg_id == "leg-1":
            return False, "venue timeout"
        return True, None

    config = ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0)
    engine = ExecutionEngine(config=config, leg_executor=fail_second)
    decision = _make_decision(n_legs=2)
    result = await engine.execute(decision)

    assert result.status == ExecutionStatus.PARTIAL
    assert result.legs_executed == 1
    assert result.leg_results[0].success is True
    assert result.leg_results[1].success is False
    assert result.leg_results[1].skipped is False
    # PnL pro-rated to 50%
    assert result.realized_pnl == pytest.approx(2.5)


# ──────────────────────────────────────────────────────────────────────────
# Retry policy
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_retry_succeeds_on_second_attempt():
    """Leg fails attempt 1, succeeds attempt 2 → COMPLETED, attempts=2."""
    state = {"attempts": 0}

    async def flaky(leg: ExecutionLeg, attempt: int):
        state["attempts"] += 1
        if attempt == 1:
            return False, "transient"
        return True, None

    config = ExecutionConfig(retry_attempts=3, retry_delay_seconds=0.0)
    engine = ExecutionEngine(config=config, leg_executor=flaky)
    decision = _make_decision(n_legs=1)
    result = await engine.execute(decision)

    assert result.status == ExecutionStatus.COMPLETED
    assert result.legs_executed == 1
    assert result.leg_results[0].attempts == 2
    assert result.leg_results[0].success is True
    assert state["attempts"] == 2  # didn't go to 3rd


@pytest.mark.asyncio
async def test_retry_exhausts_marks_failed():
    """Leg fails all retry_attempts attempts → status=ABORTED (first-leg
    case) with attempts=max."""
    state = {"calls": 0}

    async def always_fails(leg: ExecutionLeg, attempt: int):
        state["calls"] += 1
        return False, f"attempt {attempt} failed"

    config = ExecutionConfig(retry_attempts=3, retry_delay_seconds=0.0)
    engine = ExecutionEngine(config=config, leg_executor=always_fails)
    decision = _make_decision(n_legs=1)
    result = await engine.execute(decision)

    assert result.status == ExecutionStatus.ABORTED
    assert result.legs_executed == 0
    assert result.leg_results[0].attempts == 3
    assert result.leg_results[0].success is False
    assert state["calls"] == 3


@pytest.mark.asyncio
async def test_executor_exception_treated_as_failure():
    """If the executor raises, engine catches, records as failed attempt,
    and respects retry semantics (NOT crash)."""
    state = {"calls": 0}

    async def raises(leg: ExecutionLeg, attempt: int):
        state["calls"] += 1
        raise RuntimeError("simulated network blowup")

    config = ExecutionConfig(retry_attempts=2, retry_delay_seconds=0.0)
    engine = ExecutionEngine(config=config, leg_executor=raises)
    decision = _make_decision(n_legs=1)
    result = await engine.execute(decision)

    assert result.status == ExecutionStatus.ABORTED
    assert result.leg_results[0].attempts == 2
    assert state["calls"] == 2
    assert "executor raised" in result.leg_results[0].error


# ──────────────────────────────────────────────────────────────────────────
# Position tracker integration
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_position_tracker_called_only_for_successful_legs():
    """Failed legs must NOT call tracker.open_position — would corrupt PnL.

    Pre-T4 bug: the old engine opened positions for every leg regardless
    of success. Revision 7 fixes it.
    """
    tracker = PositionTracker()

    async def fail_second(leg: ExecutionLeg, attempt: int):
        return leg.leg_id != "leg-1", "fail" if leg.leg_id == "leg-1" else None

    config = ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0)
    engine = ExecutionEngine(config=config, tracker=tracker, leg_executor=fail_second)
    decision = _make_decision(n_legs=2)
    await engine.execute(decision)

    # Only leg-0 succeeded → tracker has exactly 1 open position.
    assert len(tracker._open_positions) == 1
    assert "asset-0" in tracker._open_positions
    assert "asset-1" not in tracker._open_positions
