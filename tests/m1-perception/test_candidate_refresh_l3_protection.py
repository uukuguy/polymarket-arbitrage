"""Phase 05 Plan 02 Task 1 — L3 protection regression (Pitfall 5).

Until Plan 02 Task 3 lands, `l2_candidate_refresh.on_snapshot_complete` does
``ws_consumer._subscribed_assets = list(new_asset_ids)`` — a full overwrite
that clobbers L3 tokens (Pitfall 5 from 05-PATTERNS.md / 05-RESEARCH.md §Pitfall 5).

These tests assert that AFTER Plan 02:
- candidate refresh mutates only `_candidate_set`
- `_l3_active_set` is untouched
- the public `subscribed_assets` view returns the union

Test 10 — full e2e: simulated NOTIFY → candidate set replaced, L3 preserved
Test 11 — lint: `on_snapshot_complete` source no longer assigns to `_subscribed_assets`
"""

from __future__ import annotations

import inspect
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

# Same convention as conftest.py — allow tmp_path-external Settings.
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


def _make_consumer(initial_assets: list[str] | None = None):
    """Construct a real WsConsumer (NOT MagicMock) so set/property behavior is exercised."""
    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    return WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=initial_assets,
    )


def _settings_no_supabase(tmp_path: Path):
    """Settings with empty Supabase config so the fetch branch short-circuits."""
    from pydantic import SecretStr

    from polyarb.config import Settings

    s = Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        supabase_url="",
        supabase_service_key=SecretStr(""),
    )
    s.candidate_scanner_yaml = None
    s.candidate_watchlist_yaml = None
    return s


def _reset_refresh_state() -> None:
    """Reset module-level debounce + last-known state — pre-test hygiene."""
    import polyarb.observation.l2_candidate_refresh as mod

    # Match conftest pattern: set to negative cold-start sentinel so the first
    # call in the test passes the debounce gate.
    mod._last_refresh_at_s = -mod.REFRESH_DEBOUNCE_S - 1.0
    mod._last_known_markets_rows = None
    mod._last_fetch_success_at_s = None


# ─────────────────────────────────────────────────────────────────────────────
# Test 10 — on_snapshot_complete does NOT clobber _l3_active_set
# ─────────────────────────────────────────────────────────────────────────────


async def test_on_snapshot_complete_does_not_clobber_l3_active_set(tmp_path: Path) -> None:
    """Race regression — refresh updates _candidate_set only; L3 set untouched.

    Pre-state:
        _candidate_set = {"existing_candidate"}
        _l3_active_set = {"l3_token_1", "l3_token_2"}

    Action: on_snapshot_complete with mocked compute_candidates returning
    [CandidateRow(asset_id="new_cand_1"), CandidateRow(asset_id="new_cand_2")].

    Post-state (Plan 02 contract):
        _candidate_set == {"new_cand_1", "new_cand_2"}
        _l3_active_set == {"l3_token_1", "l3_token_2"}  ← UNTOUCHED
        subscribed_assets == union of both
    """
    _reset_refresh_state()
    import polyarb.observation.l2_candidate_refresh as mod
    from polyarb.observation.l2_candidate_refresh import CandidateRow

    consumer = _make_consumer(initial_assets=["existing_candidate"])
    # Manually populate L3 set (simulating L3 promoter having run earlier).
    consumer._l3_active_set = {"l3_token_1", "l3_token_2"}

    settings = _settings_no_supabase(tmp_path)

    # Mock compute_candidates → return 2 new candidate rows.
    fake_rows = [
        CandidateRow(
            asset_id="new_cand_1",
            market_id="m1",
            event_id="e1",
            recipe_name="test",
            source="recipe",
            ranking_score={"liquidity": 1.0, "volume": 1.0},
        ),
        CandidateRow(
            asset_id="new_cand_2",
            market_id="m2",
            event_id="e2",
            recipe_name="test",
            source="recipe",
            ranking_score={"liquidity": 1.0, "volume": 1.0},
        ),
    ]

    with patch.object(mod, "compute_candidates", return_value=fake_rows):
        ok = await mod.on_snapshot_complete(
            {"snapshot_id": 1, "taken_at_ms": 1},
            ws_consumer=consumer,
            settings=settings,
        )

    # The real consumer has no live socket in this unit test. Phase 05.1's
    # success-only convergence contract therefore publishes the desired local
    # state but returns False so the durable cursor remains retryable.
    assert ok is False

    # CORE ASSERTIONS — the race regression we are guarding:
    assert consumer._l3_active_set == {"l3_token_1", "l3_token_2"}, (
        f"L3 set must be UNTOUCHED; got {consumer._l3_active_set}"
    )
    assert consumer._candidate_set == {"new_cand_1", "new_cand_2"}, (
        f"_candidate_set must be replaced with new rows; got {consumer._candidate_set}"
    )
    assert set(consumer.subscribed_assets) == {
        "new_cand_1",
        "new_cand_2",
        "l3_token_1",
        "l3_token_2",
    }, f"subscribed_assets must be union; got {consumer.subscribed_assets}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 11 — lint: refresh source no longer assigns to _subscribed_assets
# ─────────────────────────────────────────────────────────────────────────────


def test_refresh_source_does_not_assign_to_subscribed_assets_directly() -> None:
    """Source-lint: on_snapshot_complete must NOT do ``ws_consumer._subscribed_assets = ...``.

    Plan 02 Task 3 migrates the mutation to ``ws_consumer.update_candidate_set(...)``.
    This lint catches a regression where the assignment sneaks back.
    """
    import polyarb.observation.l2_candidate_refresh as mod

    src = inspect.getsource(mod.on_snapshot_complete)
    # Allow comments/docstrings referencing the field; only fail on an actual
    # assignment expression. We forbid the literal pattern
    # ``_subscribed_assets =`` (with optional whitespace).
    # A simple heuristic: split into lines, strip leading whitespace + comments.
    offending: list[str] = []
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        # Look for assignment to ws_consumer._subscribed_assets specifically.
        if "_subscribed_assets" in stripped and "=" in stripped:
            # Must be an assignment, not a comparison or attribute READ.
            # Heuristic: assignment LHS ends with `_subscribed_assets` before `=`.
            lhs = stripped.split("=")[0].strip()
            if lhs.endswith("_subscribed_assets"):
                offending.append(stripped)

    assert not offending, (
        "on_snapshot_complete must NOT assign to ws_consumer._subscribed_assets "
        "directly (use update_candidate_set helper). Offending lines:\n" + "\n".join(offending)
    )
