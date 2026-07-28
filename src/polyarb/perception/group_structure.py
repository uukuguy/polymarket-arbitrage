"""Independent, fail-closed reads of one certified neg-risk group."""

from __future__ import annotations

import asyncio

from polyarb.perception.models import GroupRevision
from polyarb.perception.store import OpportunityPerceptionStore


class GroupStructureUnavailableError(RuntimeError):
    """The requested group has no current certified membership."""


class GroupStructureReader:
    """Read the exact group authority without blocking the daemon event loop."""

    def __init__(self, store: OpportunityPerceptionStore) -> None:
        self._store = store

    async def read_group(self, group_id: str) -> GroupRevision:
        revision = await asyncio.to_thread(self._store.current_group, group_id)
        if revision is None or revision.status != "certified":
            raise GroupStructureUnavailableError("group-not-certified")
        return revision
