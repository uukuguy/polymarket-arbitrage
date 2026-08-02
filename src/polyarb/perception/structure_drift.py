"""Independent source projection for drift-safe Structure comparison."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass

from polyarb.perception.market_truth import EventMember, GroupTruth
from polyarb.perception.structure_publication import (
    event_only_member_quarantine_issue,
    market_quarantine_issue,
    project_event_structure,
)
from polyarb.snapshot.normalizer import normalize_market


@dataclass(frozen=True)
class LegacyCompatibleEventProjection:
    members: tuple[EventMember, ...]
    truths: tuple[GroupTruth, ...]
    issues: tuple[Mapping[str, object], ...]


@dataclass(frozen=True)
class LegacyCompatibleMarketProjection:
    row: Mapping[str, object] | None
    issue: Mapping[str, object] | None


@dataclass(frozen=True)
class LegacyCompatibleProjectionReceipt:
    eligible_market_count: int
    universe_hash: str
    group_truth_hash: str


def project_legacy_compatible_event(
    raw_event: dict[str, object],
    *,
    event_source_ordinal: int,
    complete_market_ids: frozenset[str],
) -> LegacyCompatibleEventProjection:
    """Project one pinned event through legacy normalization plus exact quarantine."""
    raw_members = raw_event.get("markets")
    issues: list[Mapping[str, object]] = []
    quarantined_ids: set[str] = set()
    if isinstance(raw_members, list):
        for raw_member in raw_members:
            if not isinstance(raw_member, dict):
                continue
            market_id = raw_member.get("id")
            if not isinstance(market_id, str) or market_id in complete_market_ids:
                continue
            issue = event_only_member_quarantine_issue(
                raw_event,
                event_source_ordinal=event_source_ordinal,
                market_id=market_id,
            )
            if issue is not None:
                quarantined_ids.add(market_id)
                issues.append(issue)
    members, truths = project_event_structure(raw_event, quarantined_ids)
    return LegacyCompatibleEventProjection(
        tuple(members),
        tuple(truths),
        tuple(issues),
    )


def project_legacy_compatible_market(
    raw_market: dict[str, object],
    *,
    event_ids: tuple[str, ...],
    taken_at_ms: int,
) -> LegacyCompatibleMarketProjection:
    """Project one pinned market without consulting generation component rows."""
    market_id = raw_market.get("id")
    if not isinstance(market_id, str):
        return LegacyCompatibleMarketProjection(None, None)
    issue = market_quarantine_issue(market_id, raw_market, event_ids)
    if issue is not None:
        return LegacyCompatibleMarketProjection(None, issue)
    event_id = event_ids[0] if len(event_ids) == 1 else None
    row = normalize_market(
        raw_market,
        {market_id: event_id} if event_id is not None else {},
    )
    if row is None:
        return LegacyCompatibleMarketProjection(None, None)
    row["fetched_at_ms"] = taken_at_ms
    return LegacyCompatibleMarketProjection(row, None)


def _canonical_list_hash(rows: list[tuple[object, ...]]) -> str:
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, row in enumerate(rows):
        if index:
            digest.update(b",")
        digest.update(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()
        )
    digest.update(b"]")
    return digest.hexdigest()


def hash_legacy_compatible_projection(
    events: tuple[LegacyCompatibleEventProjection, ...],
    markets: tuple[LegacyCompatibleMarketProjection, ...],
) -> LegacyCompatibleProjectionReceipt:
    """Hash the fresh reader universe and its independently recomputed group truth."""
    market_rows = {
        str(projected.row["market_id"]): projected.row
        for projected in markets
        if projected.row is not None
    }
    universe_rows: list[tuple[object, ...]] = []
    truth_rows: list[tuple[object, ...]] = []
    for projected in events:
        members_by_key: dict[tuple[str, str], list[EventMember]] = {}
        for member in projected.members:
            members_by_key.setdefault((member.event_id, member.group_id), []).append(
                member
            )
        for truth in projected.truths:
            truth_rows.append(
                (
                    truth.event_id,
                    truth.group_id,
                    truth.neg_risk_type,
                    truth.expected_member_count,
                    truth.active_named_count,
                    truth.membership_hash,
                    truth.quality,
                    truth.reason,
                )
            )
            if (
                truth.neg_risk_type != "standard"
                or truth.quality != "complete-supported"
            ):
                continue
            for member in members_by_key.get((truth.event_id, truth.group_id), []):
                market = market_rows.get(member.market_id)
                if (
                    member.member_kind != "named"
                    or not member.active
                    or member.closed
                    or market is None
                    or market.get("event_id") != truth.event_id
                    or market.get("neg_risk_market_id") != truth.group_id
                    or market.get("active") is not True
                    or market.get("closed") is not False
                    or market.get("incomplete") is not False
                    or not str(market.get("yes_token_id") or "").strip()
                ):
                    continue
                universe_rows.append(
                    (
                        truth.group_id,
                        truth.membership_hash,
                        member.market_id,
                        str(market["yes_token_id"]),
                    )
                )
    universe_rows.sort()
    truth_rows.sort(key=lambda row: (str(row[1]), str(row[0])))
    return LegacyCompatibleProjectionReceipt(
        len(universe_rows),
        _canonical_list_hash(universe_rows),
        _canonical_list_hash(truth_rows),
    )
