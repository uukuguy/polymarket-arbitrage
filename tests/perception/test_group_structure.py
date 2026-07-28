from __future__ import annotations

from pathlib import Path

import pytest

from polyarb.perception.group_structure import (
    GroupStructureReader,
    GroupStructureUnavailableError,
)
from polyarb.perception.models import GroupLeg, GroupRevision
from polyarb.perception.store import OpportunityPerceptionStore


def _revision(*, status: str = "certified") -> GroupRevision:
    certified = GroupRevision.certified(
        group_id="g-1",
        event_id="e-1",
        revision=7,
        started_at_ms=1_000,
        observed_at_ms=2_000,
        source_cursor="cursor-1",
        legs=(
            GroupLeg("m-1", "c-1", "yes-1", "First"),
            GroupLeg("m-2", "c-2", "yes-2", "Second"),
        ),
    )
    if status == "certified":
        return certified
    return GroupRevision(
        **{**certified.__dict__, "status": status},
    )


@pytest.mark.asyncio
async def test_group_structure_reader_returns_only_current_certified_revision(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    revision = _revision()
    store.publish_group_revision(revision)

    assert await GroupStructureReader(store).read_group("g-1") == revision


@pytest.mark.asyncio
async def test_group_structure_reader_fails_closed_for_non_certified_group(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(_revision(status="stale"))

    with pytest.raises(GroupStructureUnavailableError, match="group-not-certified"):
        await GroupStructureReader(store).read_group("g-1")
