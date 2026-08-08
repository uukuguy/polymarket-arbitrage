"""Single vocabulary for bounded Structure publication checkpoints."""

STRUCTURE_COMPONENTS = (
    "events",
    "event_tags",
    "memberships",
    "group_truth",
    "markets",
    "issues",
)
STRUCTURE_SOURCE_COMPONENTS = ("source_events", "source_markets")
STRUCTURE_CERTIFICATION_COMPONENTS = (
    *STRUCTURE_COMPONENTS,
    *STRUCTURE_SOURCE_COMPONENTS,
)
STRUCTURE_COMPARISON_PHASES = (
    "legacy-universe",
    "generation-universe",
    "legacy-rejections",
    "generation-rejections",
)
STRUCTURE_PUBLICATION_CHECKPOINT_COMPONENTS = (
    *STRUCTURE_CERTIFICATION_COMPONENTS,
    *STRUCTURE_COMPARISON_PHASES,
)
STRUCTURE_PUBLICATION_CHECKPOINT_STAGES = (
    "normalizing",
    "certifying",
    "ready",
    "superseded",
)
STRUCTURE_PUBLICATION_MAX_ROWS = 500
STRUCTURE_DRIFT_SOURCE_EVENT_MAX_ROWS = 100
STRUCTURE_DRIFT_SOURCE_EVENT_MAX_MEMBER_WORK = 500
STRUCTURE_DRIFT_SOURCE_EVENT_MAX_PAYLOAD_BYTES = 512 * 1024
STRUCTURE_PUBLICATION_MIN_CHUNK_REMAINING_S = 10.0
STRUCTURE_GENERATION_CHILD_HARD_LIMIT_S = 75.0
STRUCTURE_POINTER_SWITCH_TRANSACTION_DEADLINE_S = 15.0
STRUCTURE_POINTER_SWITCH_WRITER_LOCK_TIMEOUT_S = 5.0
STRUCTURE_NORMALIZATION_CONTRACT_VERSION = "2026-08-02-event-only-quarantine-v2"
STRUCTURE_DRIFT_CLASSIFIER_V1 = "structure-drift-classifier-v1"
STRUCTURE_DRIFT_CLASSIFIER_V2 = "structure-drift-classifier-v2"
STRUCTURE_DRIFT_CLASSIFIER_V3 = "structure-drift-classifier-v3"
# v4 preserves v3 receipt/digest semantics while narrowing one explicitly
# observed, safe ordinary-event shape: negRisk may be null when every other
# ordinary marker is present and the event member has no group.
STRUCTURE_DRIFT_CLASSIFIER_V4 = "structure-drift-classifier-v4"
STRUCTURE_DRIFT_CLASSIFIERS_V3_COMPATIBLE = frozenset(
    (STRUCTURE_DRIFT_CLASSIFIER_V3, STRUCTURE_DRIFT_CLASSIFIER_V4)
)
STRUCTURE_PROJECTION_EXCLUSION_REASONS = (
    "non-neg-risk-market",
    "market-side-quarantine",
    "non-neg-risk-event-member",
    "current-nontradable-event-member",
    "augmented-group",
    "fresh-group-ineligible",
    "event-only-quarantine",
)
STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT = "structure-event-member-staging-v1"
STRUCTURE_EVENT_SOURCE_CONTRACT = "structure-event-source-v1"
STRUCTURE_DRIFT_CLASS_TAGS_V2 = (
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
STRUCTURE_DRIFT_DIAGNOSTIC_CODES = (
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


def valid_structure_publication_checkpoint(
    stage: object,
    component: object,
) -> bool:
    """Validate the exact stage/component pairs emitted by the worker."""
    if stage == "normalizing":
        return component in STRUCTURE_COMPONENTS
    if stage == "certifying":
        return component in STRUCTURE_PUBLICATION_CHECKPOINT_COMPONENTS
    if stage == "superseded":
        return component is None
    return stage == "ready" and component is None
