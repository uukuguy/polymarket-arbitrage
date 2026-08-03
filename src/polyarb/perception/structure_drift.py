"""Independent source projection for drift-safe Structure comparison."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Literal

from polyarb.perception.market_truth import EventMember, GroupTruth
from polyarb.perception.structure_contract import (
    STRUCTURE_DRIFT_CLASSIFIER_V1,
    STRUCTURE_DRIFT_CLASSIFIER_V2,
    STRUCTURE_DRIFT_DIAGNOSTIC_CODES,
)
from polyarb.perception.structure_publication import (
    event_only_member_quarantine_issue,
    market_quarantine_issue,
    project_event_structure,
)
from polyarb.snapshot.normalizer import normalize_events, normalize_market
from polyarb.storage.row_chain_sha256 import RowChainSHA256


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
class FreshProjectionCursor:
    stream: Literal["market", "event-only"]
    market_id: str | None
    event_id: str | None
    source_ordinal: int | None
    member_ordinal: int | None


@dataclass(frozen=True)
class FreshProjectionChunk:
    cursor: FreshProjectionCursor | None
    members: tuple[StructuralMemberIdentity, ...]
    diagnostics: tuple[StructureDriftDiagnostic, ...]
    candidates_processed: int

    @property
    def count(self) -> int:
        return len(self.members)

    @property
    def root(self) -> str:
        digest = RowChainSHA256.new("projection-member")
        for member in sorted(self.members, key=_member_tuple):
            digest.update(_member_tuple(member))
        return digest.hexdigest()


@dataclass(frozen=True)
class FreshProjectionCommitment:
    publication_id: str
    generation_snapshot_id: int
    member_receipt_digest: str
    cursor: FreshProjectionCursor | None
    candidates_processed: int
    member_count: int
    member_digest_state: str
    diagnostic_count: int
    diagnostic_digest_state: str
    complete: bool

    @classmethod
    def initial(
        cls,
        *,
        publication_id: str,
        generation_snapshot_id: int,
        member_receipt_digest: str,
    ) -> FreshProjectionCommitment:
        if (
            not publication_id
            or generation_snapshot_id < 1
            or not isinstance(member_receipt_digest, str)
            or len(member_receipt_digest) != 64
        ):
            raise ValueError("invalid-fresh-projection-commitment-identity")
        return cls(
            publication_id=publication_id,
            generation_snapshot_id=generation_snapshot_id,
            member_receipt_digest=member_receipt_digest,
            cursor=None,
            candidates_processed=0,
            member_count=0,
            member_digest_state=RowChainSHA256.new("projection-member").to_json(),
            diagnostic_count=0,
            diagnostic_digest_state=RowChainSHA256.new(
                "diagnostic/unclassified"
            ).to_json(),
            complete=False,
        )

    @property
    def root(self) -> str:
        return RowChainSHA256.from_json(
            self.member_digest_state,
            expected_domain="projection-member",
        ).hexdigest()

    @property
    def diagnostic_root(self) -> str:
        return RowChainSHA256.from_json(
            self.diagnostic_digest_state,
            expected_domain="diagnostic/unclassified",
        ).hexdigest()

    def matches_generation(
        self, *, count: int, root: str, member_receipt_digest: str | None = None
    ) -> bool:
        return (
            self.complete
            and self.diagnostic_count == 0
            and type(count) is int
            and count >= 0
            and isinstance(root, str)
            and len(root) == 64
            and self.member_count == count
            and self.root == root
            and member_receipt_digest == self.member_receipt_digest
        )


def advance_fresh_projection_commitment(
    commitment: FreshProjectionCommitment,
    chunk: FreshProjectionChunk,
) -> FreshProjectionCommitment:
    """Advance a bounded canonical projection commitment without retaining rows."""
    if commitment.complete:
        raise ValueError("fresh-projection-commitment-complete")
    member_digest = RowChainSHA256.from_json(
        commitment.member_digest_state,
        expected_domain="projection-member",
    )
    if member_digest.count != commitment.member_count:
        raise ValueError("fresh-projection-member-state-count-mismatch")
    for member in chunk.members:
        member_digest.update(_member_tuple(member))
    diagnostic_digest = RowChainSHA256.from_json(
        commitment.diagnostic_digest_state,
        expected_domain="diagnostic/unclassified",
    )
    if diagnostic_digest.count != commitment.diagnostic_count:
        raise ValueError("fresh-projection-diagnostic-state-count-mismatch")
    if not 0 <= chunk.candidates_processed <= 500:
        raise ValueError("invalid-fresh-projection-chunk-count")
    for diagnostic in chunk.diagnostics:
        envelope = diagnostic.envelope
        diagnostic_digest.update(
            (
                diagnostic.code,
                *envelope.identity_fields.values(),
                envelope.source_ordinal,
                envelope.member_ordinal,
                envelope.raw_event_hash,
                envelope.raw_market_hash,
            )
        )
    return FreshProjectionCommitment(
        publication_id=commitment.publication_id,
        generation_snapshot_id=commitment.generation_snapshot_id,
        member_receipt_digest=commitment.member_receipt_digest,
        cursor=chunk.cursor,
        candidates_processed=(
            commitment.candidates_processed + chunk.candidates_processed
        ),
        member_count=commitment.member_count + len(chunk.members),
        member_digest_state=member_digest.to_json(),
        diagnostic_count=commitment.diagnostic_count + len(chunk.diagnostics),
        diagnostic_digest_state=diagnostic_digest.to_json(),
        complete=chunk.cursor is None,
    )


@dataclass(frozen=True)
class FreshGroupEvidence:
    event_id: str
    group_id: str
    neg_risk_type: str
    quality: str
    reason: str | None
    membership_hash: str
    global_relation_conflict: bool


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
    event_source_count: int = 0
    exact_source_member: StructuralMemberIdentity | None = None
    group_truth: FreshGroupEvidence | None = None
    duplicate_market_identity: bool = False
    identity_revalidated: bool = True
    invalid_neg_risk_classification: bool = False
    invalid_event_membership: bool = False
    uncertified_event_only_member: bool = False
    source_ordinal: int | None = None
    member_ordinal: int | None = None
    raw_event_hash: str | None = None
    raw_market_hash: str | None = None


@dataclass(frozen=True)
class StructureDriftCandidateEnvelope:
    side: Literal["legacy-only", "generation-only"]
    event_id: str | None
    group_id: str | None
    market_id: str
    member_kind: str | None
    active: bool | None
    closed: bool | None
    condition_id: str | None
    yes_token_id: str | None
    no_token_id: str | None
    neg_risk: bool | None
    incomplete: bool | None
    source_ordinal: int | None
    member_ordinal: int | None
    raw_event_hash: str | None
    raw_market_hash: str | None

    @property
    def identity_fields(self) -> Mapping[str, object]:
        return {
            "event_id": self.event_id,
            "group_id": self.group_id,
            "market_id": self.market_id,
            "member_kind": self.member_kind,
            "active": self.active,
            "closed": self.closed,
            "condition_id": self.condition_id,
            "yes_token_id": self.yes_token_id,
            "no_token_id": self.no_token_id,
            "neg_risk": self.neg_risk,
            "incomplete": self.incomplete,
        }


@dataclass(frozen=True)
class StructureDriftDiagnostic:
    side: Literal["legacy-only", "generation-only"]
    code: str
    envelope: StructureDriftCandidateEnvelope
    predicate_bits: tuple[bool, ...]


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
    diagnostics: tuple[StructureDriftDiagnostic, ...] = ()

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

    @property
    def diagnostic_counts(self) -> dict[str, int]:
        return dict(Counter(row.code for row in self.diagnostics))


def reconstruction_root_from_class_commitments(
    *,
    class_counts: Mapping[str, int],
    class_digests: Mapping[str, str],
    tags: tuple[str, ...],
    domain: str,
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
    digest = RowChainSHA256.new(domain)
    for commitment in commitments:
        digest.update(commitment)
    return digest.hexdigest()


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
    digest = RowChainSHA256.new(f"class/{tag}")
    for member in sorted(members, key=_member_tuple):
        digest.update((tag, *_member_tuple(member)))
    return digest.hexdigest()


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


def _is_fresh_group_ineligible(
    member: StructuralMemberIdentity,
    evidence: FreshMemberEvidence,
) -> bool:
    truth = evidence.group_truth
    return (
        evidence.generation_certified
        and evidence.event_source_count == 1
        and evidence.exact_source_member == member
        and evidence.current_active
        and not evidence.current_closed
        and not evidence.event_only_quarantine
        and not evidence.market_side_quarantine
        and truth is not None
        and truth.event_id == member.event_id
        and truth.group_id == member.group_id
        and truth.neg_risk_type == "standard"
        and truth.quality == "complete-unsupported"
        and truth.reason == "standard-neg-risk-has-non-tradable-members"
        and not truth.global_relation_conflict
    )


def _is_fresh_addition(
    member: StructuralMemberIdentity,
    evidence: FreshMemberEvidence,
    *,
    classifier_v2: bool,
) -> bool:
    truth = evidence.group_truth
    return (
        evidence.source_present
        and evidence.current_active
        and not evidence.current_closed
        and evidence.projector_matches
        and evidence.generation_certified
        and not evidence.event_only_quarantine
        and not evidence.market_side_quarantine
        and (
            not classifier_v2
            or (
                evidence.projected_member == member
                and truth is not None
                and truth.event_id == member.event_id
                and truth.group_id == member.group_id
                and truth.neg_risk_type == "standard"
                and truth.quality == "complete-supported"
                and not truth.global_relation_conflict
            )
        )
    )


def _candidate_envelope(
    *,
    side: Literal["legacy-only", "generation-only"],
    member: StructuralMemberIdentity | StructureDriftCandidateEnvelope,
    evidence: FreshMemberEvidence | None,
) -> StructureDriftCandidateEnvelope:
    if isinstance(member, StructureDriftCandidateEnvelope):
        if member.side != side:
            raise ValueError("structure-drift-candidate-side-mismatch")
        return member
    return StructureDriftCandidateEnvelope(
        side=side,
        event_id=member.event_id,
        group_id=member.group_id,
        market_id=member.market_id,
        member_kind=member.member_kind,
        active=member.active,
        closed=member.closed,
        condition_id=member.condition_id,
        yes_token_id=member.yes_token_id,
        no_token_id=member.no_token_id,
        neg_risk=member.neg_risk,
        incomplete=member.incomplete,
        source_ordinal=None if evidence is None else evidence.source_ordinal,
        member_ordinal=None if evidence is None else evidence.member_ordinal,
        raw_event_hash=None if evidence is None else evidence.raw_event_hash,
        raw_market_hash=None if evidence is None else evidence.raw_market_hash,
    )


def _duplicate_identity_evidence(
    member: StructuralMemberIdentity,
    evidence: FreshMemberEvidence | None,
) -> FreshMemberEvidence:
    if evidence is not None:
        return replace(evidence, duplicate_market_identity=True)
    return FreshMemberEvidence(
        source_present=False,
        current_active=member.active,
        current_closed=member.closed,
        projector_matches=False,
        generation_certified=False,
        event_only_quarantine=False,
        market_side_quarantine=False,
        absent_from_event_catalog=False,
        absent_from_market_catalog=False,
        duplicate_market_identity=True,
        identity_revalidated=False,
    )


def diagnose_unresolved_member(
    *,
    side: Literal["legacy-only", "generation-only"],
    member: StructuralMemberIdentity | StructureDriftCandidateEnvelope,
    evidence: FreshMemberEvidence | None,
    authorized_removal_reasons: tuple[str, ...],
) -> StructureDriftDiagnostic:
    """Assign exactly one unresolved code using the frozen first-match table."""
    predicates = _diagnostic_predicates(
        side=side,
        member=member,
        evidence=evidence,
        authorized_removal_reasons=authorized_removal_reasons,
    )
    return _diagnostic_from_predicates(
        side=side,
        member=member,
        evidence=evidence,
        predicates=predicates,
    )


def _diagnostic_predicates(
    *,
    side: Literal["legacy-only", "generation-only"],
    member: StructuralMemberIdentity | StructureDriftCandidateEnvelope,
    evidence: FreshMemberEvidence | None,
    authorized_removal_reasons: tuple[str, ...],
) -> tuple[bool, ...]:
    truth = None if evidence is None else evidence.group_truth
    truth_identity_mismatch = truth is not None and (
        (
            member.event_id is not None
            and truth.event_id != member.event_id
        )
        or (
            member.group_id is not None
            and truth.group_id != member.group_id
        )
    )
    active_open_projection_required = (
        evidence is not None
        and evidence.source_present
        and evidence.current_active
        and not evidence.current_closed
        and not evidence.event_only_quarantine
        and not evidence.market_side_quarantine
    )
    preceding_predicates = (
        evidence is not None and evidence.duplicate_market_identity,
        evidence is None or not evidence.identity_revalidated,
        side == "generation-only" and evidence is not None and not evidence.generation_certified,
        side == "generation-only"
        and evidence is not None
        and evidence.absent_from_event_catalog
        and evidence.absent_from_market_catalog,
        truth is not None and truth.global_relation_conflict,
        evidence is not None and evidence.invalid_neg_risk_classification,
        evidence is not None
        and (evidence.invalid_event_membership or truth_identity_mismatch),
        evidence is not None and evidence.uncertified_event_only_member,
        truth is not None
        and truth.quality == "incomplete-source"
        and not truth.global_relation_conflict,
        truth is not None and truth.neg_risk_type == "augmented",
        truth is not None
        and truth.quality == "complete-unsupported"
        and truth.neg_risk_type != "augmented"
        and truth.reason != "standard-neg-risk-has-non-tradable-members",
        side == "generation-only" and evidence is not None and evidence.event_only_quarantine,
        side == "generation-only" and evidence is not None and evidence.market_side_quarantine,
        side == "generation-only"
        and evidence is not None
        and (not evidence.current_active or evidence.current_closed),
        active_open_projection_required and evidence.projected_member is None,
        active_open_projection_required
        and evidence.projected_member is not None
        and isinstance(member, StructuralMemberIdentity)
        and evidence.projected_member != member,
        side == "legacy-only" and len(authorized_removal_reasons) > 1,
        side == "legacy-only" and not authorized_removal_reasons,
    )
    return (
        *preceding_predicates,
        side == "generation-only" and not any(preceding_predicates),
    )


def _diagnostic_from_predicates(
    *,
    side: Literal["legacy-only", "generation-only"],
    member: StructuralMemberIdentity | StructureDriftCandidateEnvelope,
    evidence: FreshMemberEvidence | None,
    predicates: tuple[bool, ...],
) -> StructureDriftDiagnostic:
    code = next(
        code
        for code, predicate in zip(STRUCTURE_DRIFT_DIAGNOSTIC_CODES, predicates, strict=True)
        if predicate
    )
    return StructureDriftDiagnostic(
        side=side,
        code=code,
        envelope=_candidate_envelope(
            side=side,
            member=member,
            evidence=evidence,
        ),
        predicate_bits=predicates,
    )


def _v2_authorization_blocker(
    *,
    side: Literal["legacy-only", "generation-only"],
    member: StructuralMemberIdentity,
    evidence: FreshMemberEvidence | None,
    authorized_removal_reasons: tuple[str, ...],
) -> StructureDriftDiagnostic | None:
    predicates = _diagnostic_predicates(
        side=side,
        member=member,
        evidence=evidence,
        authorized_removal_reasons=authorized_removal_reasons,
    )
    if not any(predicates[:16]):
        return None
    return _diagnostic_from_predicates(
        side=side,
        member=member,
        evidence=evidence,
        predicates=predicates,
    )


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
    classifier_contract: str = STRUCTURE_DRIFT_CLASSIFIER_V1,
) -> StructureMemberDriftResult:
    """Partition one complete member universe without count or age tolerance."""
    if classifier_contract not in {
        STRUCTURE_DRIFT_CLASSIFIER_V1,
        STRUCTURE_DRIFT_CLASSIFIER_V2,
    }:
        raise ValueError("invalid-structure-drift-classifier-contract")
    classifier_v2 = classifier_contract == STRUCTURE_DRIFT_CLASSIFIER_V2
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
    diagnostics: list[StructureDriftDiagnostic] = []
    unclassified: list[StructuralMemberIdentity] = [
        generation_by_id.get(market_id) or legacy_by_id[market_id]
        for market_id in sorted(duplicate_ids)
    ]
    if classifier_v2:
        for member in unclassified:
            member_evidence = _duplicate_identity_evidence(
                member,
                evidence.get(member.market_id),
            )
            diagnostics.append(
                diagnose_unresolved_member(
                    side=(
                        "generation-only" if member.market_id in generation_by_id else "legacy-only"
                    ),
                    member=member,
                    evidence=member_evidence,
                    authorized_removal_reasons=(),
                )
            )

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
                if classifier_v2:
                    member_evidence = _duplicate_identity_evidence(
                        generation_member,
                        evidence.get(market_id),
                    )
                    diagnostics.append(
                        diagnose_unresolved_member(
                            side="generation-only",
                            member=generation_member,
                            evidence=member_evidence,
                            authorized_removal_reasons=(),
                        )
                    )
            continue
        member_evidence = evidence.get(market_id)
        if generation_member is not None:
            blocker = (
                _v2_authorization_blocker(
                    side="generation-only",
                    member=generation_member,
                    evidence=member_evidence,
                    authorized_removal_reasons=(),
                )
                if classifier_v2
                else None
            )
            if blocker is not None:
                unclassified.append(generation_member)
                diagnostics.append(blocker)
                continue
            if member_evidence is not None and _is_fresh_addition(
                generation_member,
                member_evidence,
                classifier_v2=classifier_v2,
            ):
                additions.append(generation_member)
            else:
                unclassified.append(generation_member)
                if classifier_v2:
                    diagnostics.append(
                        diagnose_unresolved_member(
                            side="generation-only",
                            member=generation_member,
                            evidence=member_evidence,
                            authorized_removal_reasons=(),
                        )
                    )
            continue
        assert legacy_member is not None
        reasons = () if member_evidence is None else _legacy_removal_reasons(member_evidence)
        if (
            classifier_v2
            and member_evidence is not None
            and _is_fresh_group_ineligible(legacy_member, member_evidence)
        ):
            reasons = (*reasons, "fresh-group-ineligible")
        blocker = (
            _v2_authorization_blocker(
                side="legacy-only",
                member=legacy_member,
                evidence=member_evidence,
                authorized_removal_reasons=reasons,
            )
            if classifier_v2
            else None
        )
        if blocker is not None:
            unclassified.append(legacy_member)
            diagnostics.append(blocker)
            continue
        if len(reasons) == 1:
            removals.setdefault(reasons[0], []).append(legacy_member)
        else:
            unclassified.append(legacy_member)
            if classifier_v2:
                diagnostics.append(
                    diagnose_unresolved_member(
                        side="legacy-only",
                        member=legacy_member,
                        evidence=member_evidence,
                        authorized_removal_reasons=reasons,
                    )
                )

    frozen_shared = tuple(shared)
    frozen_additions = tuple(additions)
    frozen_removals = {reason: tuple(rows) for reason, rows in removals.items()}
    classes: dict[str, tuple[StructuralMemberIdentity, ...]] = {
        "shared": frozen_shared,
        "fresh-addition": frozen_additions,
        **frozen_removals,
    }
    class_digests = {tag: _tagged_member_hash(tag, rows) for tag, rows in classes.items() if rows}
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
            domain="legacy-reconstruction",
        ),
        reconstruction_root_from_class_commitments(
            class_counts=class_counts,
            class_digests=class_digests,
            tags=("shared", "fresh-addition"),
            domain="generation-reconstruction",
        ),
        tuple(diagnostics),
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
