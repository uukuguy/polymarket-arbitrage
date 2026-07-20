"""Process contract for the mutation-free L3 promoter diagnostic."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_dry_run_requests_plan_only_and_prints_proposed_state(capsys) -> None:
    from scripts import l3_promote_dry_run

    settings = object()
    promote = AsyncMock(
        return_value={
            "added": ["yes-a", "no-a"],
            "removed": [],
            "proposed_active": ["no-a", "yes-a"],
            "dry_run": True,
        }
    )

    with patch("polyarb.config.load_settings", return_value=settings), patch(
        "polyarb.observation.l3_promote.promote_run", promote
    ):
        result = await l3_promote_dry_run._main()

    assert result == 0
    promote.assert_awaited_once()
    kwargs = promote.await_args.kwargs
    assert kwargs["settings"] is settings
    assert kwargs["apply_mutations"] is False
    output = capsys.readouterr().out
    assert "proposed_active" in output
    assert "after dry-run" not in output
