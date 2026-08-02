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
STRUCTURE_PUBLICATION_MIN_CHUNK_REMAINING_S = 10.0
STRUCTURE_NORMALIZATION_CONTRACT_VERSION = "2026-08-02-neg-risk-quarantine-v1"


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
