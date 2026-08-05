from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from polyarb.perception.structure_contract import (
    STRUCTURE_DRIFT_CLASS_TAGS_V2,
    STRUCTURE_DRIFT_CLASSIFIER_V1,
    STRUCTURE_DRIFT_CLASSIFIER_V2,
    STRUCTURE_DRIFT_CLASSIFIER_V3,
    STRUCTURE_DRIFT_DIAGNOSTIC_CODES,
)
from polyarb.perception.structure_drift import (
    FreshGroupEvidence,
    FreshMemberEvidence,
    StructuralMemberIdentity,
    StructureDriftCandidateEnvelope,
    classify_structure_member_drift,
    diagnose_unresolved_member,
    reconstruction_root_from_class_commitments,
)
from polyarb.storage.sqlite_store import SQLiteStore


def _member(market_id: str = "market-1") -> StructuralMemberIdentity:
    return StructuralMemberIdentity(
        event_id="event-1",
        group_id="group-1",
        market_id=market_id,
        member_kind="named",
        active=True,
        closed=False,
        condition_id=f"condition-{market_id}",
        yes_token_id=f"yes-{market_id}",
        no_token_id=f"no-{market_id}",
        neg_risk=True,
        incomplete=False,
    )


def _addition_evidence() -> FreshMemberEvidence:
    return FreshMemberEvidence(
        source_present=True,
        current_active=True,
        current_closed=False,
        projector_matches=True,
        generation_certified=True,
        event_only_quarantine=False,
        market_side_quarantine=False,
        absent_from_event_catalog=False,
        absent_from_market_catalog=False,
    )


def test_classifier_contract_vocabulary_is_frozen() -> None:
    assert STRUCTURE_DRIFT_CLASSIFIER_V1 == "structure-drift-classifier-v1"
    assert STRUCTURE_DRIFT_CLASSIFIER_V2 == "structure-drift-classifier-v2"
    assert STRUCTURE_DRIFT_CLASS_TAGS_V2 == (
        "shared",
        "fresh-addition",
        "current-nontradable",
        "event-only-quarantine",
        "market-side-quarantine",
        "fresh-source-absent",
        "fresh-group-ineligible",
        "overlap-conflict",
        "unclassified",
    )
    assert STRUCTURE_DRIFT_DIAGNOSTIC_CODES == (
        "duplicate-market-identity",
        "evidence-missing",
        "generation-addition-not-certified",
        "generation-addition-source-absent",
        "conflicting-event-membership",
        "invalid-neg-risk-classification",
        "invalid-event-membership",
        "uncertified-event-only-member",
        "group-incomplete-source",
        "augmented-group",
        "group-complete-unsupported-unknown-reason",
        "generation-addition-event-only-quarantine",
        "generation-addition-market-side-quarantine",
        "generation-addition-current-nontradable",
        "active-open-projection-missing",
        "active-open-projection-mismatch",
        "multiple-removal-reasons",
        "other-zero-removal-reason",
        "generation-addition-other",
    )


def _v2_evidence(
    member: StructuralMemberIdentity,
    **changes: object,
) -> FreshMemberEvidence:
    truth = FreshGroupEvidence(
        event_id="event-1",
        group_id="group-1",
        neg_risk_type="standard",
        quality="complete-supported",
        reason=None,
        membership_hash="a" * 64,
        global_relation_conflict=False,
    )
    base = FreshMemberEvidence(
        source_present=True,
        current_active=True,
        current_closed=False,
        projector_matches=True,
        generation_certified=True,
        event_only_quarantine=False,
        market_side_quarantine=False,
        absent_from_event_catalog=False,
        absent_from_market_catalog=False,
        projected_member=member,
        event_source_count=1,
        exact_source_member=member,
        group_truth=truth,
    )
    return replace(base, **changes)


def test_unknown_unsupported_reason_precedes_quarantine_lookalike() -> None:
    member = _member()
    evidence = _v2_evidence(
        member,
        market_side_quarantine=True,
        group_truth=FreshGroupEvidence(
            event_id=member.event_id,
            group_id=member.group_id,
            neg_risk_type="standard",
            quality="complete-unsupported",
            reason="unknown-reason",
            membership_hash="a" * 64,
            global_relation_conflict=False,
        ),
    )
    diagnostic = diagnose_unresolved_member(
        side="generation-only",
        member=member,
        evidence=evidence,
        authorized_removal_reasons=(),
    )
    assert diagnostic.code == "group-complete-unsupported-unknown-reason"


def test_group_ineligible_active_sibling_is_classified_by_v2() -> None:
    inactive = replace(_member("market-a"), active=False)
    active = _member("market-b")
    truth = FreshGroupEvidence(
        event_id="event-1",
        group_id="group-1",
        neg_risk_type="standard",
        quality="complete-unsupported",
        reason="standard-neg-risk-has-non-tradable-members",
        membership_hash="b" * 64,
        global_relation_conflict=False,
    )
    result = classify_structure_member_drift(
        legacy=(inactive, active),
        generation=(),
        evidence={
            "market-a": replace(_v2_evidence(inactive), current_active=False, group_truth=truth),
            "market-b": replace(_v2_evidence(active), group_truth=truth),
        },
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V2,
    )
    assert result.legacy_removal_counts == {
        "current-nontradable": 1,
        "fresh-group-ineligible": 1,
    }
    assert result.unclassified == ()
    assert result.diagnostics == ()
    class_counts = {
        "current-nontradable": 1,
        "fresh-group-ineligible": 1,
    }
    assert result.legacy_reconstruction_root == reconstruction_root_from_class_commitments(
        class_counts=class_counts,
        class_digests=result.class_digests,
        tags=("current-nontradable", "fresh-group-ineligible"),
        domain="legacy-reconstruction",
    )
    assert result.generation_reconstruction_root == (
        reconstruction_root_from_class_commitments(
            class_counts=class_counts,
            class_digests=result.class_digests,
            tags=("shared", "fresh-addition"),
            domain="generation-reconstruction",
        )
    )


def test_group_ineligible_active_sibling_remains_unclassified_by_default_v1() -> None:
    member = _member("market-b")
    truth = FreshGroupEvidence(
        event_id="event-1",
        group_id="group-1",
        neg_risk_type="standard",
        quality="complete-unsupported",
        reason="standard-neg-risk-has-non-tradable-members",
        membership_hash="b" * 64,
        global_relation_conflict=False,
    )
    result = classify_structure_member_drift(
        legacy=(member,),
        generation=(),
        evidence={"market-b": replace(_v2_evidence(member), group_truth=truth)},
    )
    assert result.legacy_removal_counts == {}
    assert result.unclassified == (member,)
    assert result.diagnostics == ()


def test_classifier_v3_uses_exact_v2_strict_member_semantics() -> None:
    shared = _member("shared")
    addition = _member("addition")
    removal = _member("removal")
    legacy_conflict = _member("conflict")
    generation_conflict = replace(legacy_conflict, yes_token_id="changed")
    kwargs = {
        "legacy": (shared, removal, legacy_conflict),
        "generation": (shared, addition, generation_conflict),
        "evidence": {
            "addition": _v2_evidence(addition),
            "removal": replace(_v2_evidence(removal), current_active=False),
            "conflict": _v2_evidence(generation_conflict),
        },
    }
    v2 = classify_structure_member_drift(
        **kwargs, classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V2
    )
    v3 = classify_structure_member_drift(
        **kwargs, classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3
    )

    assert v3 == v2
    assert v3.shared == (shared,)
    assert v3.fresh_additions == (addition,)
    assert v3.legacy_removal_counts == {"current-nontradable": 1}
    assert v3.overlap_conflicts == (generation_conflict,)
    assert v3.diagnostics


def test_global_conflict_precedes_local_group_ineligible_reason() -> None:
    member = _member("market-b")
    truth = FreshGroupEvidence(
        event_id="event-1",
        group_id="group-1",
        neg_risk_type="standard",
        quality="incomplete-source",
        reason="conflicting-event-membership",
        membership_hash="b" * 64,
        global_relation_conflict=True,
    )
    result = classify_structure_member_drift(
        legacy=(member,),
        generation=(),
        evidence={"market-b": replace(_v2_evidence(member), group_truth=truth)},
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V2,
    )
    assert result.legacy_removal_counts == {}
    assert result.unclassified == (member,)
    assert result.diagnostic_counts == {"conflicting-event-membership": 1}
    assert result.diagnostics[0].code == "conflicting-event-membership"


@pytest.mark.parametrize(
    ("changes", "expected_code"),
    [
        ({"identity_revalidated": False}, "evidence-missing"),
        (
            {
                "group_truth": FreshGroupEvidence(
                    "event-1",
                    "group-1",
                    "standard",
                    "incomplete-source",
                    "conflicting-event-membership",
                    "a" * 64,
                    True,
                )
            },
            "conflicting-event-membership",
        ),
        ({"invalid_neg_risk_classification": True}, "invalid-neg-risk-classification"),
        ({"invalid_event_membership": True}, "invalid-event-membership"),
        (
            {
                "group_truth": FreshGroupEvidence(
                    "event-1",
                    "group-1",
                    "standard",
                    "incomplete-source",
                    "missing-source-member",
                    "a" * 64,
                    False,
                )
            },
            "group-incomplete-source",
        ),
        (
            {
                "group_truth": FreshGroupEvidence(
                    "event-1",
                    "group-1",
                    "augmented",
                    "complete-unsupported",
                    "augmented-neg-risk-not-supported",
                    "a" * 64,
                    False,
                )
            },
            "augmented-group",
        ),
        (
            {
                "group_truth": FreshGroupEvidence(
                    "event-1",
                    "group-1",
                    "standard",
                    "complete-unsupported",
                    "unknown-reason",
                    "a" * 64,
                    False,
                )
            },
            "group-complete-unsupported-unknown-reason",
        ),
    ],
)
def test_v2_fresh_addition_authorized_path_blocking_precedes_classification(
    changes: dict[str, object],
    expected_code: str,
) -> None:
    member = _member()
    result = classify_structure_member_drift(
        legacy=(),
        generation=(member,),
        evidence={"market-1": _v2_evidence(member, **changes)},
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V2,
    )
    assert result.fresh_addition_count == 0
    assert result.unclassified == (member,)
    assert result.diagnostic_counts == {expected_code: 1}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("event_id", "event-2", id="event-id"),
        pytest.param("group_id", "group-2", id="group-id"),
    ],
)
def test_v2_fresh_addition_truth_identity_mismatch_is_invalid_event_membership(
    field: str,
    value: str,
) -> None:
    member = _member()
    evidence = _v2_evidence(member)
    assert evidence.group_truth is not None
    mismatched_truth = replace(evidence.group_truth, **{field: value})
    mismatched_evidence = replace(evidence, group_truth=mismatched_truth)
    diagnostic = diagnose_unresolved_member(
        side="generation-only",
        member=member,
        evidence=mismatched_evidence,
        authorized_removal_reasons=(),
    )
    assert diagnostic.code == "invalid-event-membership"
    assert diagnostic.predicate_bits[6] is True
    result = classify_structure_member_drift(
        legacy=(),
        generation=(member,),
        evidence={"market-1": mismatched_evidence},
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V2,
    )
    assert result.fresh_addition_count == 0
    assert result.unclassified == (member,)
    assert result.diagnostic_counts == {"invalid-event-membership": 1}
    assert result.diagnostics[0].predicate_bits[6] is True


def test_v2_legacy_truth_identity_mismatch_blocks_current_nontradable() -> None:
    member = _member()
    evidence = _v2_evidence(member)
    assert evidence.group_truth is not None
    mismatched_evidence = replace(
        evidence,
        current_active=False,
        group_truth=replace(evidence.group_truth, group_id="group-2"),
    )
    result = classify_structure_member_drift(
        legacy=(member,),
        generation=(),
        evidence={"market-1": mismatched_evidence},
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V2,
    )
    assert result.legacy_removal_counts == {}
    assert result.unclassified == (member,)
    assert result.diagnostic_counts == {"invalid-event-membership": 1}


@pytest.mark.parametrize(
    ("evidence_changes", "expected_code"),
    [
        (
            {
                "current_active": False,
                "group_truth": FreshGroupEvidence(
                    "event-1",
                    "group-1",
                    "standard",
                    "incomplete-source",
                    "conflicting-event-membership",
                    "a" * 64,
                    True,
                ),
            },
            "conflicting-event-membership",
        ),
        (
            {
                "event_only_quarantine": True,
                "invalid_neg_risk_classification": True,
            },
            "invalid-neg-risk-classification",
        ),
        (
            {
                "market_side_quarantine": True,
                "identity_revalidated": False,
            },
            "evidence-missing",
        ),
        (
            {
                "source_present": False,
                "absent_from_event_catalog": True,
                "absent_from_market_catalog": True,
                "invalid_event_membership": True,
            },
            "invalid-event-membership",
        ),
    ],
)
def test_v2_legacy_removal_authorized_path_blocking_precedes_classification(
    evidence_changes: dict[str, object],
    expected_code: str,
) -> None:
    member = _member()
    result = classify_structure_member_drift(
        legacy=(member,),
        generation=(),
        evidence={"market-1": _v2_evidence(member, **evidence_changes)},
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V2,
    )
    assert result.legacy_removal_counts == {}
    assert result.unclassified == (member,)
    assert result.diagnostic_counts == {expected_code: 1}


@pytest.mark.parametrize(
    ("evidence_changes", "expected_code"),
    [
        (
            {"invalid_neg_risk_classification": True},
            "invalid-neg-risk-classification",
        ),
        (
            {"projected_member": replace(_member(), condition_id="mismatch")},
            "active-open-projection-mismatch",
        ),
    ],
)
def test_v2_group_ineligible_authorized_path_blocking_precedes_classification(
    evidence_changes: dict[str, object],
    expected_code: str,
) -> None:
    member = _member()
    truth = FreshGroupEvidence(
        event_id="event-1",
        group_id="group-1",
        neg_risk_type="standard",
        quality="complete-unsupported",
        reason="standard-neg-risk-has-non-tradable-members",
        membership_hash="b" * 64,
        global_relation_conflict=False,
    )
    result = classify_structure_member_drift(
        legacy=(member,),
        generation=(),
        evidence={
            "market-1": _v2_evidence(
                member,
                group_truth=truth,
                **evidence_changes,
            )
        },
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V2,
    )
    assert result.legacy_removal_counts == {}
    assert result.unclassified == (member,)
    assert result.diagnostic_counts == {expected_code: 1}


def test_v1_authorized_paths_ignore_v2_blocking_predicates() -> None:
    member = _member()
    addition = classify_structure_member_drift(
        legacy=(),
        generation=(member,),
        evidence={
            "market-1": _v2_evidence(
                member,
                invalid_event_membership=True,
            )
        },
    )
    assert addition.fresh_addition_count == 1
    assert addition.diagnostics == ()

    conflict_truth = FreshGroupEvidence(
        event_id="event-1",
        group_id="group-1",
        neg_risk_type="standard",
        quality="incomplete-source",
        reason="conflicting-event-membership",
        membership_hash="a" * 64,
        global_relation_conflict=True,
    )
    removal = classify_structure_member_drift(
        legacy=(member,),
        generation=(),
        evidence={
            "market-1": _v2_evidence(
                member,
                current_active=False,
                group_truth=conflict_truth,
            )
        },
    )
    assert removal.legacy_removal_counts == {"current-nontradable": 1}
    assert removal.diagnostics == ()


@pytest.mark.parametrize(
    ("side", "changes", "authorized_reasons", "expected"),
    [
        ("legacy-only", {"duplicate_market_identity": True}, (), "duplicate-market-identity"),
        ("generation-only", {"identity_revalidated": False}, (), "evidence-missing"),
        (
            "generation-only",
            {"generation_certified": False},
            (),
            "generation-addition-not-certified",
        ),
        (
            "generation-only",
            {
                "source_present": False,
                "absent_from_event_catalog": True,
                "absent_from_market_catalog": True,
            },
            (),
            "generation-addition-source-absent",
        ),
        (
            "legacy-only",
            {
                "group_truth": replace(
                    FreshGroupEvidence(
                        "event-1",
                        "group-1",
                        "standard",
                        "complete-supported",
                        None,
                        "a" * 64,
                        False,
                    ),
                    global_relation_conflict=True,
                )
            },
            (),
            "conflicting-event-membership",
        ),
        (
            "legacy-only",
            {"invalid_neg_risk_classification": True},
            (),
            "invalid-neg-risk-classification",
        ),
        ("legacy-only", {"invalid_event_membership": True}, (), "invalid-event-membership"),
        (
            "legacy-only",
            {"uncertified_event_only_member": True},
            (),
            "uncertified-event-only-member",
        ),
        (
            "legacy-only",
            {
                "group_truth": FreshGroupEvidence(
                    "event-1",
                    "group-1",
                    "standard",
                    "incomplete-source",
                    "missing-source-member",
                    "a" * 64,
                    False,
                )
            },
            (),
            "group-incomplete-source",
        ),
        (
            "legacy-only",
            {
                "group_truth": FreshGroupEvidence(
                    "event-1",
                    "group-1",
                    "augmented",
                    "complete-unsupported",
                    "augmented-neg-risk-not-supported",
                    "a" * 64,
                    False,
                )
            },
            (),
            "augmented-group",
        ),
        (
            "legacy-only",
            {
                "group_truth": FreshGroupEvidence(
                    "event-1",
                    "group-1",
                    "standard",
                    "complete-unsupported",
                    "unknown",
                    "a" * 64,
                    False,
                )
            },
            (),
            "group-complete-unsupported-unknown-reason",
        ),
        (
            "generation-only",
            {"event_only_quarantine": True},
            (),
            "generation-addition-event-only-quarantine",
        ),
        (
            "generation-only",
            {"market_side_quarantine": True},
            (),
            "generation-addition-market-side-quarantine",
        ),
        (
            "generation-only",
            {"current_active": False},
            (),
            "generation-addition-current-nontradable",
        ),
        ("legacy-only", {"projected_member": None}, (), "active-open-projection-missing"),
        (
            "legacy-only",
            {"projected_member": replace(_member(), condition_id="different")},
            (),
            "active-open-projection-mismatch",
        ),
        ("legacy-only", {}, ("one", "two"), "multiple-removal-reasons"),
        ("legacy-only", {}, (), "other-zero-removal-reason"),
        ("generation-only", {}, (), "generation-addition-other"),
    ],
)
def test_diagnostic_total_and_exclusive(
    side: str,
    changes: dict[str, object],
    authorized_reasons: tuple[str, ...],
    expected: str,
) -> None:
    member = _member()
    evidence = _v2_evidence(member, **changes)
    diagnostic = diagnose_unresolved_member(
        side=side,
        member=member,
        evidence=evidence,
        authorized_removal_reasons=authorized_reasons,
    )
    assert diagnostic.code == expected
    assert diagnostic.side == side
    assert diagnostic.envelope.identity_fields["market_id"] == "market-1"
    assert len(diagnostic.predicate_bits) == len(STRUCTURE_DRIFT_DIAGNOSTIC_CODES)
    assert sum(diagnostic.predicate_bits) >= 1
    if side == "generation-only" and expected != "generation-addition-other":
        assert diagnostic.predicate_bits[-1] is False


def test_diagnostic_precedence_uses_first_matching_predicate() -> None:
    member = _member()
    evidence = _v2_evidence(
        member,
        invalid_event_membership=True,
        market_side_quarantine=True,
        current_active=False,
    )
    diagnostic = diagnose_unresolved_member(
        side="generation-only",
        member=member,
        evidence=evidence,
        authorized_removal_reasons=(),
    )
    assert diagnostic.code == "invalid-event-membership"
    assert diagnostic.predicate_bits[6] is True
    assert diagnostic.predicate_bits[12] is True
    assert diagnostic.predicate_bits[13] is True


def test_nullable_envelope_is_canonical() -> None:
    envelope = StructureDriftCandidateEnvelope(
        side="generation-only",
        event_id=None,
        group_id=None,
        market_id="market-1",
        member_kind=None,
        active=None,
        closed=None,
        condition_id=None,
        yes_token_id=None,
        no_token_id=None,
        neg_risk=None,
        incomplete=None,
        source_ordinal=3,
        member_ordinal=4,
        raw_event_hash=None,
        raw_market_hash=None,
    )
    assert envelope.identity_fields == {
        "event_id": None,
        "group_id": None,
        "market_id": "market-1",
        "member_kind": None,
        "active": None,
        "closed": None,
        "condition_id": None,
        "yes_token_id": None,
        "no_token_id": None,
        "neg_risk": None,
        "incomplete": None,
    }
    diagnostic = diagnose_unresolved_member(
        side="generation-only",
        member=envelope,
        evidence=replace(
            _v2_evidence(_member()),
            invalid_event_membership=True,
        ),
        authorized_removal_reasons=(),
    )
    assert diagnostic.code == "invalid-event-membership"
    assert diagnostic.envelope is envelope


def test_shared_structural_identity_is_exact() -> None:
    member = _member()
    result = classify_structure_member_drift(
        legacy=(member,),
        generation=(member,),
        evidence={},
    )
    assert result.shared_count == 1
    assert result.fresh_addition_count == 0
    assert result.legacy_removal_counts == {}
    assert result.overlap_conflicts == ()
    assert result.unclassified == ()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", "changed-event"),
        ("group_id", "changed-group"),
        ("member_kind", "other"),
        ("active", False),
        ("closed", True),
        ("condition_id", "changed-condition"),
        ("yes_token_id", "changed-yes"),
        ("no_token_id", "changed-no"),
        ("neg_risk", False),
        ("incomplete", True),
    ],
)
def test_every_shared_structural_mutation_is_an_overlap_conflict(
    field: str,
    value: object,
) -> None:
    legacy = _member()
    generation = replace(legacy, **{field: value})
    result = classify_structure_member_drift(
        legacy=(legacy,),
        generation=(generation,),
        evidence={},
    )
    assert result.shared_count == 0
    assert [row.market_id for row in result.overlap_conflicts] == ["market-1"]
    assert result.authorized is False


@pytest.mark.parametrize(
    "change",
    [
        {"source_present": False},
        {"projector_matches": False},
        {"generation_certified": False},
        {"event_only_quarantine": True},
        {"market_side_quarantine": True},
    ],
)
def test_fresh_addition_requires_every_source_and_certification_fact(
    change: dict[str, bool],
) -> None:
    evidence = replace(_addition_evidence(), **change)
    result = classify_structure_member_drift(
        legacy=(),
        generation=(_member(),),
        evidence={"market-1": evidence},
    )
    assert result.fresh_addition_count == 0
    assert [row.market_id for row in result.unclassified] == ["market-1"]
    assert result.authorized is False


@pytest.mark.parametrize(
    ("reason", "evidence"),
    [
        (
            "current-nontradable",
            replace(_addition_evidence(), current_active=False),
        ),
        (
            "event-only-quarantine",
            replace(
                _addition_evidence(),
                source_present=False,
                projector_matches=False,
                generation_certified=False,
                event_only_quarantine=True,
                absent_from_market_catalog=True,
            ),
        ),
        (
            "market-side-quarantine",
            replace(
                _addition_evidence(),
                projector_matches=False,
                generation_certified=False,
                market_side_quarantine=True,
            ),
        ),
        (
            "fresh-source-absent",
            replace(
                _addition_evidence(),
                source_present=False,
                projector_matches=False,
                generation_certified=False,
                absent_from_event_catalog=True,
                absent_from_market_catalog=True,
            ),
        ),
    ],
)
def test_legacy_removal_requires_one_exact_reason(
    reason: str,
    evidence: FreshMemberEvidence,
) -> None:
    result = classify_structure_member_drift(
        legacy=(_member(),),
        generation=(),
        evidence={"market-1": evidence},
    )
    assert result.legacy_removal_counts == {reason: 1}
    assert result.unclassified == ()
    assert result.authorized is True


def test_ambiguous_legacy_removal_is_unclassified() -> None:
    ambiguous = replace(
        _addition_evidence(),
        current_active=False,
        event_only_quarantine=True,
        absent_from_market_catalog=True,
    )
    result = classify_structure_member_drift(
        legacy=(_member(),),
        generation=(),
        evidence={"market-1": ambiguous},
    )
    assert result.legacy_removal_counts == {}
    assert [row.market_id for row in result.unclassified] == ["market-1"]
    assert result.authorized is False


def test_full_symmetric_difference_cannot_cancel_at_net_zero() -> None:
    removed = _member("removed")
    added = _member("added")
    result = classify_structure_member_drift(
        legacy=(removed,),
        generation=(added,),
        evidence={
            "removed": FreshMemberEvidence(
                source_present=False,
                current_active=False,
                current_closed=True,
                projector_matches=False,
                generation_certified=False,
                event_only_quarantine=False,
                market_side_quarantine=False,
                absent_from_event_catalog=True,
                absent_from_market_catalog=True,
            ),
            "added": _addition_evidence(),
        },
    )
    assert result.fresh_addition_count == 1
    assert result.legacy_removal_counts == {"fresh-source-absent": 1}
    assert result.symmetric_difference_count == 2
    assert result.generation_count - result.legacy_count == 0
    assert result.authorized is True
    assert result.class_digests["fresh-addition"] != result.class_digests[
        "fresh-source-absent"
    ]


@pytest.mark.parametrize("side", ["legacy", "generation"])
def test_duplicate_member_identity_is_unclassified(side: str) -> None:
    member = _member()
    kwargs = {
        "legacy": (member, member) if side == "legacy" else (member,),
        "generation": (member, member) if side == "generation" else (member,),
        "evidence": {},
    }
    result = classify_structure_member_drift(**kwargs)
    assert result.authorized is False
    assert [row.market_id for row in result.unclassified] == ["market-1"]


def test_legacy_and_generation_member_loaders_are_bounded_keysets(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "member-loaders.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (1,1000,1001,'full',501,1,'structure','legacy','ok',1,'')"
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality) "
            "VALUES (1,'event-1','group-1','standard',501,501,?,'complete-supported')",
            ("a" * 64,),
        )
        con.execute(
            "INSERT INTO structure_generation_group_truth SELECT * FROM "
            "neg_risk_group_truth WHERE snapshot_id=1"
        )
        members = []
        markets = []
        for index in range(501):
            market_id = f"market-{index:03d}"
            members.append((1, "event-1", "group-1", market_id))
            markets.append(
                (
                    market_id,
                    f"condition-{index}",
                    f"yes-{index}",
                    f"no-{index}",
                    1,
                )
            )
        con.executemany(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,active,closed) "
            "VALUES (?,?,?,?,'named',1,0)",
            members,
        )
        con.execute(
            "INSERT INTO structure_generation_memberships SELECT * FROM "
            "event_market_memberships WHERE snapshot_id=1"
        )
        con.executemany(
            "INSERT INTO markets(market_id,condition_id,yes_token_id,no_token_id,"
            "active,closed,neg_risk,neg_risk_market_id,fetched_at_ms,snapshot_id,"
            "incomplete,event_id) VALUES (?,?,?,?,1,0,1,'group-1',1000,?,0,'event-1')",
            markets,
        )
        market_columns = (
            "snapshot_id,market_id,condition_id,slug,question,yes_token_id,no_token_id,"
            "mid_price,liquidity_usd,volume_usd,best_bid_price,best_bid_size,"
            "best_ask_price,best_ask_size,end_time_ms,active,closed,neg_risk,"
            "neg_risk_market_id,fetched_at_ms,page_fetched_at_ms,incomplete,event_id"
        )
        con.execute(
            f"INSERT INTO structure_generation_markets({market_columns}) "
            f"SELECT {market_columns} FROM markets WHERE snapshot_id=1"
        )

    statements: list[str] = []
    generation = store.fetch_structure_drift_member_chunk(
        snapshot_id=1,
        generation=True,
        after_market_id=None,
        limit=500,
        trace_callback=statements.append,
    )
    legacy = store.fetch_structure_drift_member_chunk(
        snapshot_id=1,
        generation=False,
        after_market_id=None,
        limit=500,
        trace_callback=statements.append,
    )
    assert generation == legacy
    assert len(generation) == 500
    assert len([sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]) == 2
    tail = store.fetch_structure_drift_member_chunk(
        snapshot_id=1,
        generation=True,
        after_market_id=generation[-1].market_id,
        limit=500,
    )
    assert [row.market_id for row in tail] == ["market-500"]
    overlap = store.fetch_structure_drift_members_by_id(
        snapshot_id=1,
        generation=False,
        market_ids=[row.market_id for row in generation],
    )
    assert overlap == generation
