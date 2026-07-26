"""Immutable source-coverage and neg-risk membership truth."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

Quality = Literal[
    "complete-supported",
    "complete-unsupported",
    "incomplete-source",
    "incomplete-quotes",
]
FailureSource = Literal["markets", "events"]
MemberKind = Literal["named", "other", "inactive-reserved"]
NegRiskType = Literal["standard", "augmented"]

MISSING_EVENT_MEMBERSHIP_REASON = "event-membership-missing-or-empty"
INVALID_EVENT_MEMBER_REASON = "event-membership-member-invalid"
CONFLICTING_EVENT_MEMBERSHIP_REASON = "market-id-conflict-across-events"


@dataclass(frozen=True)
class EventMember:
    event_id: str
    group_id: str
    market_id: str
    member_kind: MemberKind
    active: bool
    closed: bool


@dataclass(frozen=True)
class GroupTruth:
    event_id: str
    group_id: str
    neg_risk_type: NegRiskType
    expected_member_count: int
    active_named_count: int
    membership_hash: str
    quality: Quality
    reason: str | None


@dataclass(frozen=True)
class SourceCoverage:
    completed: bool
    market_items: int
    event_items: int
    failure_source: FailureSource | None
    failure_reason: str | None

    def __post_init__(self) -> None:
        if type(self.completed) is not bool:
            raise TypeError("completed must be a bool")
        self._validate_count("market_items", self.market_items)
        self._validate_count("event_items", self.event_items)

        if self.completed:
            if self.failure_source is not None or self.failure_reason is not None:
                raise ValueError("complete coverage cannot carry failure details")
            return

        if self.failure_source not in ("markets", "events"):
            raise ValueError("incomplete coverage requires failure_source markets or events")
        if not isinstance(self.failure_reason, str):
            raise TypeError("incomplete coverage requires a string failure_reason")
        reason = self.failure_reason.strip()
        if not reason:
            raise ValueError("incomplete coverage requires a non-empty failure_reason")
        object.__setattr__(self, "failure_reason", reason[:200])

    @staticmethod
    def _validate_count(name: str, value: int) -> None:
        if type(value) is not int:
            raise TypeError(f"{name} must be an int")
        if value < 0:
            raise ValueError(f"{name} must be non-negative")

    @classmethod
    def complete(cls, market_items: int, event_items: int) -> SourceCoverage:
        return cls(
            completed=True,
            market_items=market_items,
            event_items=event_items,
            failure_source=None,
            failure_reason=None,
        )

    @classmethod
    def incomplete(
        cls,
        failure_source: FailureSource,
        market_items: int,
        event_items: int,
        failure_reason: str,
    ) -> SourceCoverage:
        return cls(
            completed=False,
            market_items=market_items,
            event_items=event_items,
            failure_source=failure_source,
            failure_reason=failure_reason,
        )


def membership_hash(
    event_id: str,
    group_id: str,
    members: Sequence[EventMember],
) -> str:
    canonical = [
        (member.market_id, member.member_kind, member.active, member.closed)
        for member in sorted(members, key=lambda item: item.market_id)
    ]
    raw = json.dumps([event_id, group_id, canonical], separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()
