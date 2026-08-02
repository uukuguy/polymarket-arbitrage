"""Independent source projection for drift-safe Structure comparison."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass

from polyarb.perception.market_truth import EventMember, GroupTruth
from polyarb.perception.structure_publication import (
    event_only_member_quarantine_issue,
    market_quarantine_issue,
    project_event_structure,
)
from polyarb.snapshot.normalizer import normalize_events, normalize_market


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


@dataclass(frozen=True)
class StructuralMemberIdentity:
    event_id: str
    group_id: str
    market_id: str
    member_kind: str
    active: bool
    closed: bool
    condition_id: str
    yes_token_id: str
    no_token_id: str
    neg_risk: bool
    incomplete: bool


@dataclass(frozen=True)
class FreshMemberEvidence:
    source_present: bool
    current_active: bool
    current_closed: bool
    projector_matches: bool
    generation_certified: bool
    event_only_quarantine: bool
    market_side_quarantine: bool
    absent_from_event_catalog: bool
    absent_from_market_catalog: bool
    projected_member: StructuralMemberIdentity | None = None


@dataclass(frozen=True)
class StructureMemberDriftResult:
    legacy_count: int
    generation_count: int
    shared: tuple[StructuralMemberIdentity, ...]
    fresh_additions: tuple[StructuralMemberIdentity, ...]
    legacy_removals: Mapping[str, tuple[StructuralMemberIdentity, ...]]
    overlap_conflicts: tuple[StructuralMemberIdentity, ...]
    unclassified: tuple[StructuralMemberIdentity, ...]
    class_digests: Mapping[str, str]
    legacy_reconstruction_root: str
    generation_reconstruction_root: str

    @property
    def shared_count(self) -> int:
        return len(self.shared)

    @property
    def fresh_addition_count(self) -> int:
        return len(self.fresh_additions)

    @property
    def legacy_removal_counts(self) -> dict[str, int]:
        return {
            reason: len(rows)
            for reason, rows in self.legacy_removals.items()
            if rows
        }

    @property
    def symmetric_difference_count(self) -> int:
        return self.fresh_addition_count + sum(self.legacy_removal_counts.values())

    @property
    def authorized(self) -> bool:
        return not self.overlap_conflicts and not self.unclassified


def reconstruction_root_from_class_commitments(
    *,
    class_counts: Mapping[str, int],
    class_digests: Mapping[str, str],
    tags: tuple[str, ...],
) -> str:
    """Bind one reconstructed universe to its complete tagged partitions."""
    commitments: list[tuple[str, int, str]] = []
    for tag in sorted(tags):
        count = class_counts.get(tag, 0)
        digest = class_digests.get(tag)
        if type(count) is not int or count < 0:
            raise ValueError("invalid-structure-drift-class-count")
        if count == 0:
            if digest is not None:
                raise ValueError("unexpected-empty-structure-drift-class-digest")
            continue
        if not isinstance(digest, str) or len(digest) != 64:
            raise ValueError("invalid-structure-drift-class-digest")
        commitments.append((tag, count, digest))
    return _canonical_list_hash(commitments)


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


def _member_tuple(member: StructuralMemberIdentity) -> tuple[object, ...]:
    return (
        member.event_id,
        member.group_id,
        member.market_id,
        member.member_kind,
        member.active,
        member.closed,
        member.condition_id,
        member.yes_token_id,
        member.no_token_id,
        member.neg_risk,
        member.incomplete,
    )


def _tagged_member_hash(
    tag: str,
    members: tuple[StructuralMemberIdentity, ...],
) -> str:
    return _canonical_list_hash(
        [(tag, *_member_tuple(member)) for member in sorted(members, key=_member_tuple)]
    )


def _legacy_removal_reasons(evidence: FreshMemberEvidence) -> tuple[str, ...]:
    reasons: list[str] = []
    if evidence.source_present and (
        not evidence.current_active or evidence.current_closed
    ):
        reasons.append("current-nontradable")
    if evidence.event_only_quarantine:
        reasons.append("event-only-quarantine")
    if evidence.market_side_quarantine:
        reasons.append("market-side-quarantine")
    if (
        evidence.absent_from_event_catalog
        and evidence.absent_from_market_catalog
    ):
        reasons.append("fresh-source-absent")
    return tuple(reasons)


def build_fresh_member_evidence(
    member: StructuralMemberIdentity,
    *,
    raw_market: dict[str, object] | None,
    event_sources: tuple[tuple[str, int, dict[str, object]], ...],
    generation_certified: bool,
) -> FreshMemberEvidence:
    """Recompute one member's evidence from pinned raw catalogues only."""
    event_ids = tuple(source[0] for source in event_sources)
    market_issue = (
        None
        if raw_market is None
        else market_quarantine_issue(member.market_id, raw_market, event_ids)
    )
    event_only_issue = None
    if raw_market is None and len(event_sources) == 1:
        _event_id, source_ordinal, raw_event = event_sources[0]
        event_only_issue = event_only_member_quarantine_issue(
            raw_event,
            event_source_ordinal=source_ordinal,
            market_id=member.market_id,
        )

    current_active = False
    current_closed = True
    if raw_market is not None:
        current_active = raw_market.get("active") is True
        current_closed = raw_market.get("closed") is True
    else:
        for _event_id, _source_ordinal, raw_event in event_sources:
            raw_members = raw_event.get("markets")
            if not isinstance(raw_members, list):
                continue
            matches = [
                raw
                for raw in raw_members
                if isinstance(raw, dict) and raw.get("id") == member.market_id
            ]
            if len(matches) == 1:
                current_active = matches[0].get("active") is True
                current_closed = matches[0].get("closed") is True
                break

    projected_member = None
    if raw_market is not None and len(event_sources) == 1 and market_issue is None:
        event_id, _source_ordinal, raw_event = event_sources[0]
        projected_market = project_legacy_compatible_market(
            raw_market,
            event_ids=(event_id,),
            taken_at_ms=0,
        )
        _events, _tags, _mapping, source_members, _truths = normalize_events(
            [raw_event]
        )
        source_member = next(
            (item for item in source_members if item.market_id == member.market_id),
            None,
        )
        row = projected_market.row
        if row is not None and source_member is not None:
            projected_member = StructuralMemberIdentity(
                event_id=source_member.event_id,
                group_id=source_member.group_id,
                market_id=source_member.market_id,
                member_kind=source_member.member_kind,
                active=source_member.active,
                closed=source_member.closed,
                condition_id=str(row.get("condition_id") or ""),
                yes_token_id=str(row.get("yes_token_id") or ""),
                no_token_id=str(row.get("no_token_id") or ""),
                neg_risk=row.get("neg_risk") is True,
                incomplete=row.get("incomplete") is True,
            )
    projector_matches = projected_member == member
    return FreshMemberEvidence(
        source_present=raw_market is not None or bool(event_sources),
        current_active=current_active,
        current_closed=current_closed,
        projector_matches=projector_matches,
        generation_certified=generation_certified,
        event_only_quarantine=event_only_issue is not None,
        market_side_quarantine=market_issue is not None,
        absent_from_event_catalog=not event_sources,
        absent_from_market_catalog=raw_market is None,
        projected_member=projected_member,
    )


def classify_structure_member_drift(
    *,
    legacy: tuple[StructuralMemberIdentity, ...],
    generation: tuple[StructuralMemberIdentity, ...],
    evidence: Mapping[str, FreshMemberEvidence],
) -> StructureMemberDriftResult:
    """Partition one complete member universe without count or age tolerance."""
    duplicate_ids = {
        market_id
        for counts in (
            Counter(member.market_id for member in legacy),
            Counter(member.market_id for member in generation),
        )
        for market_id, count in counts.items()
        if count > 1
    }
    legacy_by_id = {member.market_id: member for member in legacy}
    generation_by_id = {member.market_id: member for member in generation}
    shared: list[StructuralMemberIdentity] = []
    additions: list[StructuralMemberIdentity] = []
    removals: dict[str, list[StructuralMemberIdentity]] = {}
    conflicts: list[StructuralMemberIdentity] = []
    unclassified: list[StructuralMemberIdentity] = [
        generation_by_id.get(market_id) or legacy_by_id[market_id]
        for market_id in sorted(duplicate_ids)
    ]

    for market_id in sorted(set(legacy_by_id) | set(generation_by_id)):
        if market_id in duplicate_ids:
            continue
        legacy_member = legacy_by_id.get(market_id)
        generation_member = generation_by_id.get(market_id)
        if legacy_member is not None and generation_member is not None:
            if legacy_member == generation_member:
                shared.append(generation_member)
            else:
                conflicts.append(generation_member)
            continue
        member_evidence = evidence.get(market_id)
        if generation_member is not None:
            if (
                member_evidence is not None
                and member_evidence.source_present
                and member_evidence.current_active
                and not member_evidence.current_closed
                and member_evidence.projector_matches
                and member_evidence.generation_certified
                and not member_evidence.event_only_quarantine
                and not member_evidence.market_side_quarantine
            ):
                additions.append(generation_member)
            else:
                unclassified.append(generation_member)
            continue
        assert legacy_member is not None
        reasons = (
            ()
            if member_evidence is None
            else _legacy_removal_reasons(member_evidence)
        )
        if len(reasons) == 1:
            removals.setdefault(reasons[0], []).append(legacy_member)
        else:
            unclassified.append(legacy_member)

    frozen_shared = tuple(shared)
    frozen_additions = tuple(additions)
    frozen_removals = {reason: tuple(rows) for reason, rows in removals.items()}
    classes: dict[str, tuple[StructuralMemberIdentity, ...]] = {
        "shared": frozen_shared,
        "fresh-addition": frozen_additions,
        **frozen_removals,
    }
    class_digests = {
        tag: _tagged_member_hash(tag, rows)
        for tag, rows in classes.items()
        if rows
    }
    class_counts = {tag: len(rows) for tag, rows in classes.items() if rows}
    removal_tags = tuple(frozen_removals)
    return StructureMemberDriftResult(
        len(legacy),
        len(generation),
        frozen_shared,
        frozen_additions,
        frozen_removals,
        tuple(conflicts),
        tuple(unclassified),
        class_digests,
        reconstruction_root_from_class_commitments(
            class_counts=class_counts,
            class_digests=class_digests,
            tags=("shared", *removal_tags),
        ),
        reconstruction_root_from_class_commitments(
            class_counts=class_counts,
            class_digests=class_digests,
            tags=("shared", "fresh-addition"),
        ),
    )


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
