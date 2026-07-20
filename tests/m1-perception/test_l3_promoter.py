"""RED → GREEN tests for the Phase 05 Plan 04 L3 promoter.

Covers:

- l3-promote.yaml schema + Blocker #2 ts predicate (INTEGER epoch ms)
- scanner.py docstring documents trusted-yaml tier (Warning #10)
- promote_run happy / freeze / fail-soft paths
- 5 markets → 10 tokens Yes+No expansion (D-05 / Warning #13)
- l2_candidates.l3_promoted_at_ts write-through (Blocker #1)
- run_periodic raw asyncio.wait_for loop (no apscheduler — Warning #4)

The recipe runs against scanner.run_recipe which is HARD-CODED to query
``FROM markets m LEFT JOIN question_translations qt`` (see scanner.py:142-156).
Therefore the L3 promoter's temp DB must create a ``markets`` table holding
the tob columns (asset_id, ts, spread, depth_yes_usd, …) plus an empty
``question_translations`` table for the LEFT JOIN — this is the Plan 04
adapter pattern documented in l3_promote._build_tob_temp_db.
"""
from __future__ import annotations

import asyncio
import inspect
import os
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

# Test-mode env hatches (matches conftest.py module-top setdefault).
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


RECIPE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "polyarb"
    / "scan_recipes"
    / "l3-promote.yaml"
)


@pytest.fixture(autouse=True)
def _reset_l3_promote_state() -> Any:
    """Reset l3_promote module-level state between tests (state is global)."""
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = set()
    l3_promote._last_promote_at_s = None
    l3_promote._last_book_levels_write_at_s = None
    # Plan 04 augment additions — only assert presence if defined
    if hasattr(l3_promote, "_last_known_tob_rows"):
        l3_promote._last_known_tob_rows = None
    if hasattr(l3_promote, "_last_known_market_token_map"):
        l3_promote._last_known_market_token_map = None
    yield


def _make_settings(
    *,
    supabase_url: str = "https://x.supabase.co",
    service_key: str = "test-service-key",
) -> Any:
    """Settings with auto-detected Supabase enabled (matches conftest pattern)."""
    from pydantic import SecretStr

    from polyarb.config import Settings

    return Settings(
        supabase_url=supabase_url,
        supabase_service_key=SecretStr(service_key) if service_key else "",
    )


# ────────────────────────────────────────────────────────────────────────
# Test 1 — l3-promote.yaml schema
# ────────────────────────────────────────────────────────────────────────


def test_l3_promote_yaml_parses() -> None:
    """yaml file exists with D-13 thresholds + INTEGER epoch ms ts predicate."""
    assert RECIPE_PATH.exists(), f"l3-promote.yaml missing at {RECIPE_PATH}"

    with RECIPE_PATH.open() as f:
        data = yaml.safe_load(f)

    assert "recipes" in data, "yaml must have top-level 'recipes' key"
    assert "l3-promote" in data["recipes"], "recipes must include 'l3-promote'"

    body = data["recipes"]["l3-promote"]
    assert "description" in body, "recipe must have description"

    where = body["where"]
    assert "spread < 0.02" in where, "D-13 spread threshold missing"
    assert "depth_yes_usd > 500" in where, "D-13 depth threshold missing"
    assert "strftime" in where, "Blocker #2 — strftime epoch ms predicate missing"
    assert "* 1000" in where, "Blocker #2 — epoch-ms scaling missing"
    assert (
        "datetime('now'" not in where
    ), "Blocker #2 anti-regression — lex comparison anti-pattern present"

    assert body["order_by"].strip() == "depth_yes_usd DESC", body["order_by"]
    assert int(body["limit"]) == 5, body["limit"]


# ────────────────────────────────────────────────────────────────────────
# Test 2 — scanner.py docstring documents trusted-yaml tier (Warning #10)
# ────────────────────────────────────────────────────────────────────────


def test_scanner_documents_trusted_yaml_tier() -> None:
    """scanner.py module docstring must document the 3rd recipe trust tier."""
    scanner_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "polyarb"
        / "observation"
        / "scanner.py"
    )
    text = scanner_path.read_text()
    assert (
        "Source-controlled yaml" in text or "trusted-yaml" in text
    ), "scanner.py must document trusted-yaml tier per Warning #10"


# ────────────────────────────────────────────────────────────────────────
# Test 3 — recipe ts predicate filters synthetic rows (Blocker #2)
# ────────────────────────────────────────────────────────────────────────


def test_recipe_ts_predicate_filters_synthetic_rows_correctly() -> None:
    """Build a temp DB with one row 30min ago, one row 90min ago. The recipe
    predicate `ts > strftime('%s','now','-1 hour') * 1000` must keep the
    30min row and drop the 90min row.
    """
    from polyarb.observation import l3_promote

    now_ms = int(time.time() * 1000)
    rows = [
        {
            "asset_id": "a",
            "ts": now_ms - 30 * 60 * 1000,  # 30 min ago — within 1h window
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 1000.0,
            "depth_no_usd": 1000.0,
        },
        {
            "asset_id": "b",
            "ts": now_ms - 90 * 60 * 1000,  # 90 min ago — outside window
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 1000.0,
            "depth_no_usd": 1000.0,
        },
    ]

    db_path = l3_promote._build_tob_temp_db(rows)
    try:
        recipe = l3_promote._load_recipe(RECIPE_PATH)
        from polyarb.observation.scanner import run_recipe

        df = run_recipe(db_path, recipe)
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass

    # Row B must be filtered out (90min > 1h window)
    asset_ids = sorted(str(x) for x in df["asset_id"].tolist())
    assert asset_ids == ["a"], f"expected only 'a' (recent), got {asset_ids}"


# ────────────────────────────────────────────────────────────────────────
# Test 3b — Quick task 260602-diag-depth: env override for depth threshold
# ────────────────────────────────────────────────────────────────────────


def test_load_recipe_default_threshold_is_yaml_baseline(monkeypatch) -> None:
    """Without POLYARB_L3_DEPTH_MIN_USD env, recipe WHERE keeps yaml default 500.

    Baseline invariant (CLAUDE.md "experiment values never touch baseline defaults"):
    the yaml file is the source of truth; absence of env must not change anything.
    """
    from polyarb.observation import l3_promote

    monkeypatch.delenv("POLYARB_L3_DEPTH_MIN_USD", raising=False)
    recipe = l3_promote._load_recipe(RECIPE_PATH)
    assert "depth_yes_usd > 500" in recipe.where, (
        f"baseline must remain depth_yes_usd > 500 when env unset; got: {recipe.where!r}"
    )


def test_load_recipe_env_override_substitutes_threshold(monkeypatch) -> None:
    """With POLYARB_L3_DEPTH_MIN_USD=0, recipe WHERE swaps to depth_yes_usd > 0.

    Diagnostic surface for prod bug 2 (Phase 05 Wave 5 carry): we cannot start
    24h soak with N=0 active and the only way to learn what's actually in the
    tob table is to drop the depth filter and observe. Override path keeps the
    yaml default clean.
    """
    from polyarb.observation import l3_promote

    monkeypatch.setenv("POLYARB_L3_DEPTH_MIN_USD", "0")
    recipe = l3_promote._load_recipe(RECIPE_PATH)
    assert "depth_yes_usd > 0" in recipe.where, (
        f"override should rewrite threshold to 0; got: {recipe.where!r}"
    )
    assert "depth_yes_usd > 500" not in recipe.where, (
        f"baseline 500 must be replaced, not kept alongside; got: {recipe.where!r}"
    )


def test_load_recipe_env_override_invalid_falls_back_to_yaml(monkeypatch) -> None:
    """An unparseable env value must NOT silently break the recipe — fall back
    to yaml baseline + log a warning. Defensive: a typo in fly secret shouldn't
    turn off the L3 promoter."""
    from polyarb.observation import l3_promote

    monkeypatch.setenv("POLYARB_L3_DEPTH_MIN_USD", "not-a-number")
    recipe = l3_promote._load_recipe(RECIPE_PATH)
    assert "depth_yes_usd > 500" in recipe.where, (
        f"invalid env must fall back to yaml baseline; got: {recipe.where!r}"
    )


# ────────────────────────────────────────────────────────────────────────
# Test 4 — Supabase creds missing → _l3_active_set frozen (Open Q #5)
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_no_supabase_keeps_l3_active_set() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = {"x", "y"}
    l3_promote._last_promote_at_s = 1000.0

    settings = _make_settings(supabase_url="", service_key="")
    mock_consumer = MagicMock()
    mock_consumer.add_subscriptions = AsyncMock(return_value=True)
    mock_consumer.remove_subscriptions = AsyncMock(return_value=True)

    result = await l3_promote.promote_run(
        settings=settings, ws_consumer=mock_consumer, recipe_yaml_path=RECIPE_PATH
    )

    assert l3_promote._l3_active_set == {"x", "y"}, "set must freeze"
    assert l3_promote._last_promote_at_s == 1000.0, "timestamp must not advance"
    mock_consumer.add_subscriptions.assert_not_called()
    mock_consumer.remove_subscriptions.assert_not_called()
    assert result.get("skipped"), f"result should indicate skip: {result}"


# ────────────────────────────────────────────────────────────────────────
# Test 5 — Supabase outage → _l3_active_set frozen
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_supabase_outage_freezes_set() -> None:
    from polyarb.observation import l3_promote

    l3_promote._l3_active_set = {"a"}
    settings = _make_settings()

    mock_consumer = MagicMock()
    mock_consumer.add_subscriptions = AsyncMock(return_value=True)
    mock_consumer.remove_subscriptions = AsyncMock(return_value=True)

    # create_client raises — simulates Supabase outage at client init.
    with patch(
        "polyarb.observation.l3_promote.create_client",
        side_effect=RuntimeError("503 Service Unavailable"),
    ):
        # No crash; _l3_active_set frozen.
        await l3_promote.promote_run(
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    assert l3_promote._l3_active_set == {"a"}, "set must freeze on outage"


# ────────────────────────────────────────────────────────────────────────
# Helpers for happy-path / diff tests — mock supabase client
# ────────────────────────────────────────────────────────────────────────


def _make_supabase_client_mock(
    tob_rows: list[dict],
    token_map_rows: list[dict],
    capture_updates: list[dict] | None = None,
) -> MagicMock:
    """Build a MagicMock supabase client that yields tob_rows for
    l2_top_of_book, token_map_rows for markets_latest, and captures any
    update() calls on l2_candidates into capture_updates (if provided)."""
    capture = capture_updates if capture_updates is not None else []

    def _table_side_effect(name: str) -> MagicMock:
        tbl = MagicMock()
        if name == "l2_top_of_book":
            tbl.select.return_value = tbl
            tbl.gte.return_value = tbl
            tbl.order.return_value = tbl
            tbl.limit.return_value = tbl
            tbl.execute.return_value = MagicMock(data=list(tob_rows))
        elif name == "markets_latest":
            tbl.select.return_value = tbl
            tbl.in_.return_value = tbl
            tbl.execute.return_value = MagicMock(data=list(token_map_rows))
        elif name == "l2_candidates":
            # update(...).in_(...).execute() — capture the update dict + ids.
            class _UpdateChain:
                def __init__(self, payload: dict) -> None:
                    self._payload = payload

                def in_(self, col: str, ids: list[str]) -> _UpdateChain:
                    capture.append(
                        {"payload": self._payload, "col": col, "ids": list(ids)}
                    )
                    return self

                def execute(self) -> MagicMock:
                    return MagicMock(data=[])

            tbl.update.side_effect = lambda payload: _UpdateChain(payload)
        else:
            tbl.execute.return_value = MagicMock(data=[])
        return tbl

    client = MagicMock()
    client.table.side_effect = _table_side_effect
    return client


def test_fetch_market_token_map_queries_real_production_columns() -> None:
    from polyarb.observation import l3_promote

    table = MagicMock()
    table.select.return_value = table
    table.in_.return_value = table
    table.execute.return_value = MagicMock(
        data=[{"yes_token_id": "yes-1", "no_token_id": "no-1"}]
    )
    client = MagicMock()
    client.table.return_value = table

    result = l3_promote._fetch_market_token_map(client, ["yes-1"])

    client.table.assert_called_once_with("markets_latest")
    table.select.assert_called_once_with("yes_token_id, no_token_id")
    table.in_.assert_called_once_with("yes_token_id", ["yes-1"])
    assert result == {"yes-1": ("yes-1", "no-1")}


# ────────────────────────────────────────────────────────────────────────
# Test 6 — happy path: 5 markets → 10 tokens, l2_candidates write-through
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_happy_path_top_5_markets_expanded_to_10_tokens_yes_no() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    now_ms = int(time.time() * 1000)

    # 5 markets passing thresholds, varying depths so ORDER BY DESC is deterministic.
    tob_rows = [
        {
            "asset_id": f"yes_m{i}",
            "ts": now_ms - 5 * 60 * 1000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 5000.0 - i * 100,  # m0 > m1 > m2 > m3 > m4
            "depth_no_usd": 5000.0,
        }
        for i in range(5)
    ]
    # Add 3 markets that fail thresholds — must be filtered out.
    tob_rows.extend(
        [
            {
                "asset_id": "fail-spread",
                "ts": now_ms - 5 * 60 * 1000,
                "best_bid": 0.50,
                "best_ask": 0.55,
                "spread": 0.05,  # > 0.02 → filtered
                "mid_price": 0.525,
                "depth_yes_usd": 10000.0,
                "depth_no_usd": 10000.0,
            },
            {
                "asset_id": "fail-depth",
                "ts": now_ms - 5 * 60 * 1000,
                "best_bid": 0.50,
                "best_ask": 0.51,
                "spread": 0.01,
                "mid_price": 0.505,
                "depth_yes_usd": 100.0,  # < 500 → filtered
                "depth_no_usd": 100.0,
            },
            {
                "asset_id": "fail-stale",
                "ts": now_ms - 120 * 60 * 1000,  # > 1h ago → filtered
                "best_bid": 0.50,
                "best_ask": 0.51,
                "spread": 0.01,
                "mid_price": 0.505,
                "depth_yes_usd": 10000.0,
                "depth_no_usd": 10000.0,
            },
        ]
    )

    token_map_rows = [
        {"yes_token_id": f"yes_m{i}", "no_token_id": f"no_m{i}"}
        for i in range(5)
    ]
    capture_updates: list[dict] = []
    client = _make_supabase_client_mock(tob_rows, token_map_rows, capture_updates)

    mock_consumer = MagicMock()
    mock_consumer.add_subscriptions = AsyncMock(return_value=True)
    mock_consumer.remove_subscriptions = AsyncMock(return_value=True)

    with patch(
        "polyarb.observation.l3_promote.create_client", return_value=client
    ):
        before = time.time()
        await l3_promote.promote_run(
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )
        after = time.time()

    # add_subscriptions called once with exactly 10 unique sorted tokens.
    assert mock_consumer.add_subscriptions.call_count == 1
    args, _kwargs = mock_consumer.add_subscriptions.call_args
    added_tokens = args[0]
    assert sorted(added_tokens) == added_tokens, "list must be sorted"
    assert len(added_tokens) == 10, f"expected 10 tokens, got {len(added_tokens)}: {added_tokens}"
    expected = {f"yes_m{i}" for i in range(5)} | {f"no_m{i}" for i in range(5)}
    assert set(added_tokens) == expected

    # remove_subscriptions NOT called (set was empty).
    mock_consumer.remove_subscriptions.assert_not_called()

    # State mutated.
    assert len(l3_promote._l3_active_set) == 10
    assert set(l3_promote._l3_active_set) == expected
    assert l3_promote._last_promote_at_s is not None
    assert before <= l3_promote._last_promote_at_s <= after

    # l2_candidates write-through (Blocker #1) — one update payload with
    # `l3_promoted_at_ts: <iso>` and 5 market asset_ids.
    add_updates = [
        u for u in capture_updates if u["payload"].get("l3_promoted_at_ts") is not None
    ]
    assert len(add_updates) == 1, (
        f"expected 1 add-update, got {len(add_updates)}: {capture_updates}"
    )
    assert sorted(add_updates[0]["ids"]) == sorted(f"yes_m{i}" for i in range(5))
    assert add_updates[0]["col"] == "asset_id"
    # iso string check
    iso_val = add_updates[0]["payload"]["l3_promoted_at_ts"]
    assert isinstance(iso_val, str) and "T" in iso_val and iso_val.endswith("Z")


@pytest.mark.asyncio
async def test_promote_run_rejects_incomplete_or_duplicate_token_pairs() -> None:
    from polyarb.observation import l3_promote

    now_ms = int(time.time() * 1000)
    tob_rows = [
        {
            "asset_id": f"yes_{i}",
            "ts": now_ms - 60_000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 5_000.0 - i,
            "depth_no_usd": 5_000.0,
        }
        for i in range(5)
    ]
    token_rows = [
        {"yes_token_id": "yes_0", "no_token_id": "no_0"},
        {"yes_token_id": "yes_1", "no_token_id": "no_0"},
        {"yes_token_id": "yes_2", "no_token_id": None},
        {"yes_token_id": "yes_3", "no_token_id": "yes_3"},
        # yes_4 is intentionally missing.
    ]
    consumer = MagicMock()
    consumer.add_subscriptions = AsyncMock(return_value=True)
    consumer.remove_subscriptions = AsyncMock(return_value=True)

    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(tob_rows, token_rows),
    ):
        await l3_promote.promote_run(
            settings=_make_settings(),
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    assert l3_promote._l3_active_set == {"yes_0", "no_0"}
    assert "yes_1" not in l3_promote._l3_active_set
    assert "yes_2" not in l3_promote._l3_active_set
    assert "yes_3" not in l3_promote._l3_active_set
    assert "yes_4" not in l3_promote._l3_active_set


@pytest.mark.asyncio
async def test_promote_run_dry_run_has_zero_mutations() -> None:
    from polyarb.observation import l3_promote

    now_ms = int(time.time() * 1000)
    tob_rows = [
        {
            "asset_id": f"yes_dry_{i}",
            "ts": now_ms - 60_000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 5_000.0 - i,
            "depth_no_usd": 5_000.0,
        }
        for i in range(5)
    ]
    token_rows = [
        {"yes_token_id": f"yes_dry_{i}", "no_token_id": f"no_dry_{i}"}
        for i in range(5)
    ]
    capture_updates: list[dict] = []
    consumer = MagicMock()
    consumer.add_subscriptions = AsyncMock(return_value=True)
    consumer.remove_subscriptions = AsyncMock(return_value=True)

    before_active = {"old-active"}
    before_tob = [{"sentinel": "tob"}]
    before_map = {"old-yes": ("old-yes", "old-no")}
    before_promote = 123.0
    l3_promote._l3_active_set = before_active
    l3_promote._last_known_tob_rows = before_tob
    l3_promote._last_known_market_token_map = before_map
    l3_promote._last_promote_at_s = before_promote

    with patch(
        "polyarb.observation.l3_promote.create_client",
        return_value=_make_supabase_client_mock(
            tob_rows, token_rows, capture_updates
        ),
    ):
        result = await l3_promote.promote_run(
            settings=_make_settings(),
            ws_consumer=consumer,
            recipe_yaml_path=RECIPE_PATH,
            apply_mutations=False,
        )

    consumer.add_subscriptions.assert_not_awaited()
    consumer.remove_subscriptions.assert_not_awaited()
    assert capture_updates == []
    assert l3_promote._l3_active_set is before_active
    assert l3_promote._last_known_tob_rows is before_tob
    assert l3_promote._last_known_market_token_map is before_map
    assert l3_promote._last_promote_at_s == before_promote
    assert result["dry_run"] is True
    assert len(result["proposed_active"]) == 10


# ────────────────────────────────────────────────────────────────────────
# Test 7 — diff calls add AND remove with Yes/No expansion
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_diff_calls_add_AND_remove_with_yes_no_expansion() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    now_ms = int(time.time() * 1000)

    # Seed: 2 markets (old1 stays, old2 removed). Pre-populate the reverse map
    # so the promoter knows the old token→market mapping for the diff.
    l3_promote._l3_active_set = {"yes_old1", "no_old1", "yes_old2", "no_old2"}
    l3_promote._last_known_market_token_map = {
        "yes_old1": ("yes_old1", "no_old1"),
        "yes_old2": ("yes_old2", "no_old2"),
    }

    # New top-5: old1 + 4 new markets.
    tob_rows = [
        {
            "asset_id": aid,
            "ts": now_ms - 5 * 60 * 1000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": d,
            "depth_no_usd": 5000.0,
        }
        for aid, d in [
            ("yes_old1", 5000),
            ("yes_new1", 4900),
            ("yes_new2", 4800),
            ("yes_new3", 4700),
            ("yes_new4", 4600),
        ]
    ]
    token_map_rows = [
        {"yes_token_id": "yes_old1", "no_token_id": "no_old1"},
        {"yes_token_id": "yes_new1", "no_token_id": "no_new1"},
        {"yes_token_id": "yes_new2", "no_token_id": "no_new2"},
        {"yes_token_id": "yes_new3", "no_token_id": "no_new3"},
        {"yes_token_id": "yes_new4", "no_token_id": "no_new4"},
    ]
    capture_updates: list[dict] = []
    client = _make_supabase_client_mock(tob_rows, token_map_rows, capture_updates)

    mock_consumer = MagicMock()
    mock_consumer.add_subscriptions = AsyncMock(return_value=True)
    mock_consumer.remove_subscriptions = AsyncMock(return_value=True)

    with patch(
        "polyarb.observation.l3_promote.create_client", return_value=client
    ):
        await l3_promote.promote_run(
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    # 4 new markets × 2 tokens = 8 added.
    added = mock_consumer.add_subscriptions.call_args[0][0]
    expected_added = {f"yes_new{i}" for i in range(1, 5)} | {f"no_new{i}" for i in range(1, 5)}
    assert set(added) == expected_added
    assert sorted(added) == added, "list sorted"

    # 1 removed market × 2 tokens = 2 removed.
    removed = mock_consumer.remove_subscriptions.call_args[0][0]
    assert set(removed) == {"yes_old2", "no_old2"}
    assert sorted(removed) == removed


# ────────────────────────────────────────────────────────────────────────
# Test 8 — Blocker #1: write-through l3_promoted_at_ts on add + None on remove
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_writes_l3_promoted_at_ts_on_add_and_clears_on_remove() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    now_ms = int(time.time() * 1000)

    # Seed: market m1 active.
    l3_promote._l3_active_set = {"yes_m1", "no_m1"}
    l3_promote._last_known_market_token_map = {"yes_m1": ("yes_m1", "no_m1")}

    # New top: just m2. m1 removed.
    tob_rows = [
        {
            "asset_id": "yes_m2",
            "ts": now_ms - 5 * 60 * 1000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 1000.0,
            "depth_no_usd": 1000.0,
        }
    ]
    token_map_rows = [
        {"yes_token_id": "yes_m2", "no_token_id": "no_m2"}
    ]
    capture_updates: list[dict] = []
    client = _make_supabase_client_mock(tob_rows, token_map_rows, capture_updates)

    mock_consumer = MagicMock()
    mock_consumer.add_subscriptions = AsyncMock(return_value=True)
    mock_consumer.remove_subscriptions = AsyncMock(return_value=True)

    with patch(
        "polyarb.observation.l3_promote.create_client", return_value=client
    ):
        await l3_promote.promote_run(
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    # Find the add update and the remove update.
    add_ups = [
        u for u in capture_updates if u["payload"].get("l3_promoted_at_ts") is not None
    ]
    rm_ups = [
        u for u in capture_updates if u["payload"].get("l3_promoted_at_ts") is None
    ]
    assert len(add_ups) == 1
    assert add_ups[0]["ids"] == ["yes_m2"]
    assert len(rm_ups) == 1
    assert rm_ups[0]["ids"] == ["yes_m1"]

    # Lint: helper contains `category="l2-mirror"` (chain-truth breadcrumb).
    src = inspect.getsource(l3_promote._mirror_l3_promoted_at_ts)
    assert 'category="l2-mirror"' in src or "category='l2-mirror'" in src


# ────────────────────────────────────────────────────────────────────────
# Test 9 — write-through failure does not abort promote_run
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_write_through_failure_does_not_abort_promote_run() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    now_ms = int(time.time() * 1000)

    l3_promote._l3_active_set = {"yes_m1", "no_m1"}
    l3_promote._last_known_market_token_map = {"yes_m1": ("yes_m1", "no_m1")}

    tob_rows = [
        {
            "asset_id": "yes_m2",
            "ts": now_ms - 5 * 60 * 1000,
            "best_bid": 0.50,
            "best_ask": 0.51,
            "spread": 0.01,
            "mid_price": 0.505,
            "depth_yes_usd": 1000.0,
            "depth_no_usd": 1000.0,
        }
    ]
    token_map_rows = [
        {"yes_token_id": "yes_m2", "no_token_id": "no_m2"}
    ]

    # Build a client where l2_candidates update.in_().execute() raises.
    def _table_side_effect(name: str) -> MagicMock:
        tbl = MagicMock()
        if name == "l2_top_of_book":
            tbl.select.return_value = tbl
            tbl.gte.return_value = tbl
            tbl.order.return_value = tbl
            tbl.limit.return_value = tbl
            tbl.execute.return_value = MagicMock(data=list(tob_rows))
        elif name == "markets_latest":
            tbl.select.return_value = tbl
            tbl.in_.return_value = tbl
            tbl.execute.return_value = MagicMock(data=list(token_map_rows))
        elif name == "l2_candidates":
            class _UpdateChainFails:
                def __init__(self, _payload: dict) -> None:
                    pass

                def in_(self, _col: str, _ids: list[str]) -> _UpdateChainFails:
                    return self

                def execute(self) -> Any:
                    raise RuntimeError("PostgREST 503")

            tbl.update.side_effect = lambda payload: _UpdateChainFails(payload)
        return tbl

    client = MagicMock()
    client.table.side_effect = _table_side_effect

    mock_consumer = MagicMock()
    mock_consumer.add_subscriptions = AsyncMock(return_value=True)
    mock_consumer.remove_subscriptions = AsyncMock(return_value=True)

    sentry_calls: list[dict] = []

    def _capture_breadcrumb(**kw: Any) -> None:
        sentry_calls.append(kw)

    with patch(
        "polyarb.observation.l3_promote.create_client", return_value=client
    ), patch(
        "polyarb.observation.l3_promote.sentry_sdk.add_breadcrumb",
        side_effect=_capture_breadcrumb,
    ):
        # MUST not raise.
        await l3_promote.promote_run(
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    # In-memory state advanced anyway (chain-truth: mirror is a surface, not source of truth).
    assert l3_promote._l3_active_set == {"yes_m2", "no_m2"}

    # At least one warning-level breadcrumb with category=l2-mirror.
    mirror_warn = [
        b for b in sentry_calls
        if b.get("category") == "l2-mirror" and b.get("level") == "warning"
    ]
    assert len(mirror_warn) >= 1, f"expected mirror-warning breadcrumb, got: {sentry_calls}"


# ────────────────────────────────────────────────────────────────────────
# Test 10 — fail-soft on scanner exception
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_promote_run_fail_soft_on_scanner_exception() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    l3_promote._l3_active_set = {"a"}

    client = _make_supabase_client_mock([], [])

    mock_consumer = MagicMock()
    mock_consumer.add_subscriptions = AsyncMock(return_value=True)
    mock_consumer.remove_subscriptions = AsyncMock(return_value=True)

    with patch(
        "polyarb.observation.l3_promote.create_client", return_value=client
    ), patch(
        "polyarb.observation.scanner.run_recipe",
        side_effect=ValueError("recipe invalid"),
    ):
        # No raise.
        await l3_promote.promote_run(
            settings=settings,
            ws_consumer=mock_consumer,
            recipe_yaml_path=RECIPE_PATH,
        )

    # _l3_active_set frozen.
    assert l3_promote._l3_active_set == {"a"}


# ────────────────────────────────────────────────────────────────────────
# Test 11 — run_periodic loops until stop_event
# ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_periodic_calls_promote_run_until_stop_event() -> None:
    from polyarb.observation import l3_promote

    settings = _make_settings()
    stop_event = asyncio.Event()
    mock_consumer = MagicMock()
    mock_consumer.add_subscriptions = AsyncMock(return_value=True)
    mock_consumer.remove_subscriptions = AsyncMock(return_value=True)

    call_count = {"n": 0}

    async def _fake_promote(**_kw: Any) -> dict:
        call_count["n"] += 1
        return {"added": [], "removed": []}

    with patch.object(l3_promote, "promote_run", side_effect=_fake_promote):
        task = asyncio.create_task(
            l3_promote.run_periodic(
                stop_event=stop_event,
                settings=settings,
                ws_consumer=mock_consumer,
                recipe_yaml_path=RECIPE_PATH,
                interval_s=0.05,
            )
        )
        # Let the loop run ~3 cycles.
        await asyncio.sleep(0.17)
        stop_event.set()
        await asyncio.wait_for(task, timeout=2.0)

    assert call_count["n"] >= 2, f"expected ≥2 promote_run invocations, got {call_count['n']}"


# ────────────────────────────────────────────────────────────────────────
# Test 12 — run_periodic uses raw asyncio.wait_for pattern (lint)
# ────────────────────────────────────────────────────────────────────────


def test_run_periodic_uses_wait_for_pattern() -> None:
    from polyarb.observation import l3_promote

    src = inspect.getsource(l3_promote.run_periodic)
    assert "asyncio.wait_for" in src, "must use asyncio.wait_for (cross-pattern decision #4)"
    assert "stop_event.wait" in src, "must wait on stop_event (matches ws_consumer.run)"
    # Anti-regression — no apscheduler import.
    assert "apscheduler" not in src.lower(), "raw asyncio loop only — no apscheduler"
