"""Deterministic, read-only replay of historical M1 soak observations."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from .runtime_policy import (
    _REASON_ORDER,
    RUNTIME_REASON_CODES,
    RuntimeRuleResult,
    _canonical_sort_key,
    _normalized_observation,
    _NormalizedObservation,
    evaluate_soak_observation,
)
from .soak_evidence import SoakEvidenceError


@dataclass(frozen=True, slots=True)
class RuntimeReplayResult:
    """Summary of a complete immutable replay, with each decision retained."""

    first_breaking_at: datetime | None
    reason_codes: tuple[str, ...]
    sample_count: int
    max_gap_seconds: float
    samples: tuple[RuntimeRuleResult, ...]

    def __post_init__(self) -> None:
        if self.first_breaking_at is not None and (
            self.first_breaking_at.tzinfo is None or self.first_breaking_at.utcoffset() is None
        ):
            raise ValueError("first breaking time must be timezone-aware")
        if self.sample_count != len(self.samples) or self.sample_count < 1:
            raise ValueError("replay sample count is invalid")
        if not math.isfinite(self.max_gap_seconds) or self.max_gap_seconds < 0:
            raise ValueError("replay maximum gap is invalid")
        if any(code not in RUNTIME_REASON_CODES for code in self.reason_codes):
            raise ValueError("replay contains an unknown reason code")
        if tuple(sorted(set(self.reason_codes), key=_REASON_ORDER.index)) != self.reason_codes:
            raise ValueError("replay reason codes must be unique and ordered")
        if self.first_breaking_at is None and self.reason_codes:
            raise ValueError("replay reasons require a breaking sample")

    @property
    def status(self) -> str:
        return "BREAKING" if self.first_breaking_at is not None else "PASS"


def replay_soak_observations(
    records: Sequence[Mapping[str, object]], *, max_gap_seconds: float | None = None
) -> RuntimeReplayResult:
    """Replay every sample, reporting the first live-policy break immediately.

    Inputs are copied and normalized before sorting.  No caller-owned mapping
    is mutated, and equal timestamps receive a canonical JSON tie-breaker so a
    replay is stable even when fed rows from a non-ordered source.
    """
    if max_gap_seconds is not None and (
        not math.isfinite(max_gap_seconds) or max_gap_seconds <= 0
    ):
        raise ValueError("max_gap_seconds must be finite and positive")
    if not records:
        raise SoakEvidenceError("soak evidence is empty")

    normalized: list[tuple[_NormalizedObservation, str, Mapping[str, object]]] = []
    for record in records:
        if not isinstance(record, Mapping):
            raise SoakEvidenceError("soak observation must be an object")
        parsed = _normalized_observation(record)
        normalized.append((parsed, _canonical_sort_key(record), record))
    normalized.sort(key=lambda item: (item[0].observed_at, item[1]))

    baseline = normalized[0][0]
    previous = None
    maximum_gap = 0.0
    decisions: list[RuntimeRuleResult] = []
    first_breaking_at: datetime | None = None
    aggregate_reasons: set[str] = set()
    for current, _sort_key, _record in normalized:
        if previous is not None:
            gap = (current.observed_at - previous.observed_at).total_seconds()
            if gap < 0:
                # Sorting makes this unreachable, but retain an explicit
                # fail-closed guard if the normalized type changes later.
                raise SoakEvidenceError("sample times are not ordered")
            maximum_gap = max(maximum_gap, gap)
        decision = evaluate_soak_observation(
            current,
            baseline,
            previous=previous,
            max_gap_seconds=max_gap_seconds,
        )
        decisions.append(decision)
        if decision.breaking:
            if first_breaking_at is None:
                first_breaking_at = decision.observed_at
            aggregate_reasons.update(decision.reason_codes)
        previous = current

    return RuntimeReplayResult(
        first_breaking_at=first_breaking_at,
        reason_codes=tuple(code for code in _REASON_ORDER if code in aggregate_reasons),
        sample_count=len(decisions),
        max_gap_seconds=maximum_gap,
        samples=tuple(decisions),
    )


__all__ = ["RuntimeReplayResult", "replay_soak_observations"]
