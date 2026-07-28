"""Bounded adaptive timing policy for Structure snapshot attempts."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

MIN_SUCCESS_SAMPLES = 10
MIN_TIMEOUT_S = 180
MAX_TIMEOUT_S = 600
MIN_CADENCE_S = 300
MAX_CADENCE_S = 900
ADJUSTMENT_COOLDOWN_ATTEMPTS = 3
MIN_ADJUSTMENT_S = 15


@dataclass(frozen=True)
class StructureScheduleDecision:
    timeout_s: int
    cadence_s: int
    success_sample_count: int
    success_p95_s: int | None
    reason: str


def _terminal_duration_s(attempt: Mapping[str, object]) -> int | None:
    elapsed_ms = attempt.get("elapsed_ms")
    if isinstance(elapsed_ms, int) and elapsed_ms >= 0:
        return math.ceil(elapsed_ms / 1_000)
    started_at_ms = attempt.get("started_at_ms")
    finished_at_ms = attempt.get("finished_at_ms")
    if (
        isinstance(started_at_ms, int)
        and isinstance(finished_at_ms, int)
        and finished_at_ms >= started_at_ms
    ):
        return math.ceil((finished_at_ms - started_at_ms) / 1_000)
    return None


def _nearest_rank_p95(values: Sequence[int]) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


def _clamp(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


def derive_structure_schedule(
    attempts: Sequence[Mapping[str, object]],
    *,
    configured_timeout_s: int,
    configured_cadence_s: int,
    previous_timeout_s: int,
    previous_cadence_s: int,
    attempts_since_adjustment: int,
) -> StructureScheduleDecision:
    """Derive one bounded schedule from durable terminal attempt evidence."""
    successes = [
        duration_s
        for attempt in attempts
        if attempt.get("outcome") == "succeeded"
        and (duration_s := _terminal_duration_s(attempt)) is not None
    ]
    success_p95_s = (
        _nearest_rank_p95(successes)
        if len(successes) >= MIN_SUCCESS_SAMPLES
        else None
    )
    latest = max(
        attempts,
        key=lambda attempt: int(attempt.get("id", 0)),
        default=None,
    )
    latest_timed_out = (
        latest is not None
        and latest.get("failure_kind") == "snapshot-subprocess-timeout"
    )

    if latest_timed_out:
        timeout_s = _clamp(
            max(
                math.ceil(previous_timeout_s * 1.2),
                (success_p95_s + 30) if success_p95_s is not None else 0,
            ),
            MIN_TIMEOUT_S,
            MAX_TIMEOUT_S,
        )
        cadence_s = _clamp(
            max(
                previous_cadence_s,
                timeout_s + 60,
                (success_p95_s + 90) if success_p95_s is not None else 0,
            ),
            MIN_CADENCE_S,
            MAX_CADENCE_S,
        )
        reason = "timeout-backoff"
    elif attempts_since_adjustment < ADJUSTMENT_COOLDOWN_ATTEMPTS:
        timeout_s = previous_timeout_s
        cadence_s = previous_cadence_s
        reason = "cooldown"
    elif success_p95_s is None:
        timeout_s = configured_timeout_s
        cadence_s = max(configured_cadence_s, timeout_s + 60)
        reason = "bootstrap"
    else:
        timeout_s = _clamp(success_p95_s + 30, MIN_TIMEOUT_S, MAX_TIMEOUT_S)
        cadence_s = _clamp(
            max(timeout_s + 60, success_p95_s + 90),
            MIN_CADENCE_S,
            MAX_CADENCE_S,
        )
        if (
            abs(timeout_s - previous_timeout_s) < MIN_ADJUSTMENT_S
            and abs(cadence_s - previous_cadence_s) < MIN_ADJUSTMENT_S
        ):
            timeout_s = previous_timeout_s
            cadence_s = previous_cadence_s
            reason = "stable"
        else:
            reason = "success-p95"

    return StructureScheduleDecision(
        timeout_s=timeout_s,
        cadence_s=cadence_s,
        success_sample_count=len(successes),
        success_p95_s=success_p95_s,
        reason=reason,
    )
