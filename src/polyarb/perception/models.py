"""Immutable contracts for certified groups and atomic all-leg quote batches."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class GroupLeg:
    market_id: str
    condition_id: str
    yes_token_id: str
    title: str


@dataclass(frozen=True)
class GroupRevision:
    group_id: str
    event_id: str
    revision: int
    membership_hash: str
    started_at_ms: int
    observed_at_ms: int
    source_cursor: str
    status: Literal["discovered", "certified", "stale", "invalidated", "closed"]
    legs: tuple[GroupLeg, ...]

    @classmethod
    def certified(
        cls,
        *,
        group_id: str,
        event_id: str,
        revision: int,
        started_at_ms: int,
        observed_at_ms: int,
        source_cursor: str,
        legs: Sequence[GroupLeg],
    ) -> GroupRevision:
        normalized_legs = tuple(legs)
        if len(normalized_legs) < 2:
            raise ValueError("incomplete-group-membership")
        if started_at_ms > observed_at_ms:
            raise ValueError("invalid-timestamp-order")
        return cls(
            group_id=group_id,
            event_id=event_id,
            revision=revision,
            membership_hash=cls.membership_digest(normalized_legs),
            started_at_ms=started_at_ms,
            observed_at_ms=observed_at_ms,
            source_cursor=source_cursor,
            status="certified",
            legs=normalized_legs,
        )

    @staticmethod
    def membership_digest(legs: Sequence[GroupLeg]) -> str:
        identity = [
            [leg.market_id, leg.condition_id, leg.yes_token_id, leg.title]
            for leg in legs
        ]
        encoded = json.dumps(identity, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class GroupQuoteLeg:
    yes_token_id: str
    membership_hash: str
    best_ask_price: float
    best_ask_size: float
    terminal_state: str


@dataclass(frozen=True)
class GroupQuoteBatch:
    group_id: str
    membership_hash: str
    quote_batch_id: str
    started_at_ms: int
    quoted_at_ms: int
    status: Literal["complete", "failed", "superseded"]
    failure_reason: str | None
    legs: tuple[GroupQuoteLeg, ...]

    @classmethod
    def complete(
        cls,
        *,
        group_id: str,
        membership_hash: str,
        quote_batch_id: str,
        started_at_ms: int,
        quoted_at_ms: int,
        legs: Sequence[GroupQuoteLeg],
    ) -> GroupQuoteBatch:
        normalized_legs = tuple(legs)
        if started_at_ms > quoted_at_ms:
            raise ValueError("invalid-timestamp-order")

        seen_tokens: set[str] = set()
        for leg in normalized_legs:
            if leg.membership_hash != membership_hash:
                raise ValueError("membership-hash-mismatch")
            if leg.yes_token_id in seen_tokens:
                raise ValueError("duplicate-quote-leg")
            seen_tokens.add(leg.yes_token_id)
            if leg.terminal_state != "executable":
                raise ValueError("incomplete-quote-leg")
            if not math.isfinite(leg.best_ask_price) or leg.best_ask_price <= 0:
                raise ValueError("invalid-best-ask-price")
            if not math.isfinite(leg.best_ask_size) or leg.best_ask_size <= 0:
                raise ValueError("invalid-best-ask-size")

        return cls(
            group_id=group_id,
            membership_hash=membership_hash,
            quote_batch_id=quote_batch_id,
            started_at_ms=started_at_ms,
            quoted_at_ms=quoted_at_ms,
            status="complete",
            failure_reason=None,
            legs=normalized_legs,
        )
