"""G-01 regression test — Phase 04 cold-start debounce trap.

Prod evidence (2026-05-28, polyarb-l2 v17): 31 event-bus catchup snapshots
were all silently debounced in 9ms after process start because
`_last_refresh_at_s` initialized to 0.0, and `time.monotonic()` returns
~0..N seconds since process start → `elapsed = monotonic - 0` always falls
below `REFRESH_DEBOUNCE_S=60`, so the first NOTIFY is dropped. D-01 fetch
path never ran; ws:subscribed_count stayed at 3 bootstrap assets.

Fix: initialize `_last_refresh_at_s` to `-REFRESH_DEBOUNCE_S - 1.0` so the
first call after process start passes the debounce check. This test pins
that contract.

Memory: feedback_cold-start-debounce-trap-2026-05.
"""
from __future__ import annotations

import asyncio
import importlib
import os
from unittest.mock import MagicMock

# F-3 SECURITY ESCAPE HATCH (Phase 02.1 — pytest tmp_path lives outside project root)
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


def test_cold_start_initial_value_is_below_negative_debounce():
    """The module-level _last_refresh_at_s initial value must be < -REFRESH_DEBOUNCE_S.

    This is the static side of the G-01 contract: any other initial value
    (most notably 0.0) breaks the cold-start path. importlib.reload guarantees
    we observe the literal module-load state, not any runtime monkeypatch.
    """
    import polyarb.observation.l2_candidate_refresh as mod
    importlib.reload(mod)

    assert mod._last_refresh_at_s < -mod.REFRESH_DEBOUNCE_S, (
        f"G-01: _last_refresh_at_s={mod._last_refresh_at_s} must be < "
        f"-REFRESH_DEBOUNCE_S={-mod.REFRESH_DEBOUNCE_S} so first call after "
        f"process start passes the debounce check. Setting it to 0.0 (or any "
        f"value >= 0) silently drops the first NOTIFY."
    )


def test_first_call_after_module_load_passes_debounce(tmp_path):
    """The dynamic side: import + first on_snapshot_complete must run the
    refresh path (not debounce). We use importlib.reload to simulate a true
    cold start, then call the handler with Settings that have empty Supabase
    credentials so the path short-circuits before any network attempt.

    Pre-G-01-fix, this test fails: 'first call after process start must run'
    assertion trips because debounce_branch is taken (elapsed < 60s).
    Post-G-01-fix, it passes: cold-start init -REFRESH_DEBOUNCE_S - 1.0 →
    elapsed > 60s → run branch is taken.
    """
    from pydantic import SecretStr

    import polyarb.observation.l2_candidate_refresh as mod
    importlib.reload(mod)

    # Confirm cold-start state (no prior monkeypatch leakage from other tests)
    assert mod._last_refresh_at_s < 0, (
        "post-reload _last_refresh_at_s must be < 0 (cold-start sentinel)"
    )

    from polyarb.config import Settings
    settings = Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        supabase_url="",  # empty → fetch path short-circuits (no network)
        supabase_service_key=SecretStr(""),
    )
    settings.candidate_scanner_yaml = None
    settings.candidate_watchlist_yaml = None

    fake_ws = MagicMock()
    fake_ws.subscribed_assets = []
    fake_ws._subscribed_assets = []

    ran = asyncio.run(
        mod.on_snapshot_complete(
            {"snapshot_id": 1, "taken_at_ms": 1},
            ws_consumer=fake_ws,
            settings=settings,
        )
    )

    assert ran is True, (
        "G-01: first call after module load must RUN (not debounce). "
        "If this fails, _last_refresh_at_s cold-start init is broken."
    )

    # And after that first run, debounce floor IS established for a real
    # subsequent call (no regression to the opposite extreme — disabling
    # debounce entirely).
    second_ran = asyncio.run(
        mod.on_snapshot_complete(
            {"snapshot_id": 2, "taken_at_ms": 2},
            ws_consumer=fake_ws,
            settings=settings,
        )
    )
    assert second_ran is False, (
        "second immediate call must still debounce — fix must not disable "
        "debounce, only let the first call through"
    )
