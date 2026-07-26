"""Immutable source-coverage and neg-risk membership truth."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
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
INVALID_NEG_RISK_FLAGS_REASON = "event-neg-risk-flags-invalid"
NEG_RISK_ENABLEMENT_CONFLICT_REASON = "event-neg-risk-enablement-conflict"


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


def _strict_market_identity(raw: object) -> str | None:
    if type(raw) is not str:
        return None
    value = raw.strip()
    if not value or value != raw:
        return None
    return value


def _strict_market_bool(raw: object) -> bool | None:
    if type(raw) is bool:
        return raw
    if type(raw) is int and raw in (0, 1):
        return bool(raw)
    return None


class MarketTruthSemanticValidator:
    """Validate one normalized market against immutable event-side truth."""

    def __init__(
        self,
        event_members: Sequence[EventMember],
        group_truths: Sequence[GroupTruth],
    ) -> None:
        # Gamma's /markets active stream intentionally excludes inactive and
        # closed nested event members. Keep those structural members in truth
        # storage/hash, but do not require them in the published active view.
        self.member_ids = frozenset(
            member.market_id for member in event_members if member.active and not member.closed
        )
        self._members_by_id = {member.market_id: member for member in event_members}
        self._truth_keys = frozenset((truth.event_id, truth.group_id) for truth in group_truths)

    def row_mismatch_reason(self, row: Mapping[str, object]) -> str | None:
        """Return the first mismatch for one row without retaining that row."""
        market_id = _strict_market_identity(row.get("market_id"))
        if market_id is None:
            return "published-market-truth-mismatch:market-id"
        active = _strict_market_bool(row.get("active"))
        closed = _strict_market_bool(row.get("closed"))
        neg_risk = _strict_market_bool(row.get("neg_risk"))
        for field, value in (
            ("active", active),
            ("closed", closed),
            ("neg-risk", neg_risk),
        ):
            if value is None:
                return f"published-market-truth-mismatch:{field}-invalid:{market_id}"

        event_id = _strict_market_identity(row.get("event_id"))
        group_id = _strict_market_identity(row.get("neg_risk_market_id"))
        member = self._members_by_id.get(market_id)
        if member is not None:
            if event_id != member.event_id:
                return f"published-market-truth-mismatch:event-id:{market_id}"
            if group_id != member.group_id:
                return f"published-market-truth-mismatch:group-id:{market_id}"
            if neg_risk is not True:
                return f"published-market-truth-mismatch:neg-risk-false:{market_id}"
            if active != member.active:
                return f"published-market-truth-mismatch:active:{market_id}"
            if closed != member.closed:
                return f"published-market-truth-mismatch:closed:{market_id}"

        # Either side of the market-level neg-risk claim activates the reverse
        # proof obligation. This rejects both true-without-group and
        # false-with-group rows instead of allowing either partial claim through.
        if neg_risk is True or group_id is not None:
            if (
                event_id is None
                or group_id is None
                or (event_id, group_id) not in self._truth_keys
                or member is None
                or member.event_id != event_id
                or member.group_id != group_id
            ):
                return f"published-neg-risk-without-truth:{market_id}"
        return None


def market_truth_mismatch_reason(
    event_members: Sequence[EventMember],
    group_truths: Sequence[GroupTruth],
    market_rows: Sequence[Mapping[str, object]],
) -> str | None:
    """Return the first semantic mismatch between published rows and event truth."""
    validator = MarketTruthSemanticValidator(event_members, group_truths)
    seen_market_ids: set[str] = set()

    for row in market_rows:
        market_id = _strict_market_identity(row.get("market_id"))
        if market_id is None:
            return "published-market-truth-mismatch:market-id"
        if market_id in seen_market_ids:
            return f"published-market-truth-mismatch:duplicate-market-id:{market_id}"
        seen_market_ids.add(market_id)
        reason = validator.row_mismatch_reason(row)
        if reason is not None:
            return reason

    missing_members = validator.member_ids - seen_market_ids
    if missing_members:
        return f"published-members-missing:{min(missing_members)}"
    return None
