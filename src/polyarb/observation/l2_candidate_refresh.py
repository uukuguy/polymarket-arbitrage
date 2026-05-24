"""L2 candidate refresh — scanner-recipes ∪ watchlist (D-04 + D-05).

The refresh engine consumes `snapshot_complete` NOTIFY events from the
event bus (Plan 03-05 listener) and updates `ws_consumer._subscribed_assets`
to reflect the current candidate universe.

Composition (D-04 union — verbatim REUSE from Phase 01.1):
- scanner-recipes — `scanner.run_recipe` over all `recipes/*.yaml` plus the
  BUILTIN_RECIPES dict (see `polyarb.observation.scanner`).
- watchlist — `watchlist.load_watchlist` returns hand-curated `slug`s that
  the operator wants to track regardless of any recipe outcome.

Dedup rule (RESEARCH Focus 5): later sources win — watchlist OVERRIDES a
recipe match on the same asset_id (source='watchlist').

Hard cap (R9): final set is truncated to MAX_CANDIDATES=500 assets to
keep the Polymarket WS initial_dump payload bounded. Watchlist entries are
ALWAYS retained; only recipe matches are eligible for truncation.

Debounce (SP8 cross-bug pre-check #1 + R1): a refresh is rate-limited to
once per REFRESH_DEBOUNCE_S=60.0s window. Multiple NOTIFYs inside the
window collapse to a single refresh.

Cross-plan contract (Plan 04 ↔ Plan 05): `on_snapshot_complete` mutates
`ws_consumer._subscribed_assets` directly. The WsConsumer public
`subscribed_assets` property returns a defensive copy, so the handler
MUST write to the private `_subscribed_assets` attribute.
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger

# REUSE Phase 01.1 scanner verbatim (D-04)
from polyarb.observation.scanner import list_all_recipes, run_recipe
from polyarb.observation.watchlist import load_watchlist

# ── Module constants (locked) ─────────────────────────────────────────────
MAX_CANDIDATES: int = 500  # R9 hard cap
REFRESH_DEBOUNCE_S: float = 60.0  # SP8 cross-bug check #1

# Module-level debounce state. Acceptable for Phase 03 (single-process L2
# daemon). If a future phase introduces multi-instance L2, move this into
# SQLite (l2_candidates) or Redis so all instances share the floor.
_last_refresh_at_s: float = 0.0


@dataclass(frozen=True)
class CandidateRow:
    """Single candidate asset row — produced by recipe scanner or watchlist."""

    asset_id: str
    market_id: str | None
    event_id: str | None
    recipe_name: str
    source: str  # 'recipe' | 'watchlist'
    ranking_score: dict | None  # arbitrary scoring blob (liquidity, vol, ...)


def _safe_str(v: Any) -> str | None:
    """Coerce a pandas/SQLite cell into a stripped str, or None if blank."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _ranking_score_from_row(row: dict) -> dict:
    """Pull a small ranking blob — used for cap-truncation ordering."""
    return {
        "liquidity": float(row["liquidity_usd"])
        if row.get("liquidity_usd") is not None
        else None,
        "volume": float(row["volume_usd"]) if row.get("volume_usd") is not None else None,
    }


def compute_candidates(
    settings: Any,
    scanner_yaml: Path | None = None,
    watchlist_yaml: Path | None = None,
) -> list[CandidateRow]:
    """Build the current candidate set as union(scanner-recipes, watchlist).

    Args:
        settings: object exposing ``db_path`` (Path to L1 SQLite).
        scanner_yaml: optional path to user recipes YAML (BUILTIN_RECIPES are
                      always merged regardless).
        watchlist_yaml: optional path to watchlist YAML.

    Returns:
        list[CandidateRow] capped at MAX_CANDIDATES; watchlist always retained.

    Dedup rule: watchlist OVERRIDES recipe on overlap (later source wins).

    Failure isolation: if a single recipe raises, log warning + continue
    with the other recipes (Rule 1 — never block refresh on one bad recipe).
    """
    db_path = Path(settings.db_path)
    out: dict[str, CandidateRow] = {}

    # ── 1) Scanner recipes (D-04 verbatim REUSE) ─────────────────────────
    if scanner_yaml is not None or _builtin_recipes_present():
        recipes = list_all_recipes(scanner_yaml) if scanner_yaml else {}
        for name, recipe in recipes.items():
            # Skip GROUP BY recipes — they produce aggregations, not asset rows.
            if recipe.group_by is not None:
                continue
            try:
                df = run_recipe(db_path, recipe)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"recipe {name!r} failed during candidate refresh: {e!r}")
                continue
            for _idx, row in df.iterrows():
                yes_tid = _safe_str(row.get("yes_token_id"))
                if not yes_tid:
                    continue
                out[yes_tid] = CandidateRow(
                    asset_id=yes_tid,
                    market_id=_safe_str(row.get("market_id")),
                    event_id=_safe_str(row.get("event_id")),
                    recipe_name=name,
                    source="recipe",
                    ranking_score=_ranking_score_from_row(dict(row)),
                )

    # ── 2) Watchlist (overrides recipe on overlap) ───────────────────────
    if watchlist_yaml is not None:
        try:
            entries = load_watchlist(watchlist_yaml)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"load_watchlist failed: {e!r}")
            entries = []
        if entries:
            # Resolve slug → yes_token_id via SQLite read-only URI.
            uri = f"file:{db_path}?mode=ro"
            con = sqlite3.connect(uri, uri=True)
            try:
                con.row_factory = sqlite3.Row
                for entry in entries:
                    cur = con.execute(
                        "SELECT market_id, yes_token_id, event_id, "
                        "liquidity_usd, volume_usd "
                        "FROM markets WHERE slug = ?",
                        (entry.slug,),
                    )
                    r = cur.fetchone()
                    if r is None:
                        logger.warning(
                            f"watchlist entry {entry.slug!r}: slug not in markets, skipped"
                        )
                        continue
                    yes_tid = _safe_str(r["yes_token_id"])
                    if not yes_tid:
                        continue
                    out[yes_tid] = CandidateRow(  # overrides any recipe row
                        asset_id=yes_tid,
                        market_id=_safe_str(r["market_id"]),
                        event_id=_safe_str(r["event_id"]),
                        recipe_name="watchlist",
                        source="watchlist",
                        ranking_score={
                            "liquidity": float(r["liquidity_usd"])
                            if r["liquidity_usd"] is not None
                            else None,
                            "volume": float(r["volume_usd"])
                            if r["volume_usd"] is not None
                            else None,
                        },
                    )
            finally:
                con.close()

    # ── 3) Apply MAX_CANDIDATES cap — watchlist always retained ──────────
    watchlist_rows = [r for r in out.values() if r.source == "watchlist"]
    recipe_rows = [r for r in out.values() if r.source == "recipe"]
    # Sort recipe rows by liquidity desc, None-safe (None goes last).
    recipe_rows.sort(
        key=lambda r: (
            -(r.ranking_score.get("liquidity") if r.ranking_score and r.ranking_score.get("liquidity") is not None else -1e18),
        )
    )
    headroom = max(0, MAX_CANDIDATES - len(watchlist_rows))
    capped_recipes = recipe_rows[:headroom]
    return watchlist_rows + capped_recipes


def _builtin_recipes_present() -> bool:
    """Always True — BUILTIN_RECIPES are merged by list_all_recipes."""
    return True


def diff_candidate_sets(
    old_asset_ids: set[str],
    new_rows: list[CandidateRow],
) -> tuple[set[str], list[CandidateRow]]:
    """Return (removed_asset_ids, added_rows) given old + new sets.

    `removed`: in old but not in new — caller must WS-unsubscribe.
    `added`: rows whose asset_id is not in old — caller must WS-subscribe.
    """
    new_asset_ids = {r.asset_id for r in new_rows}
    removed = old_asset_ids - new_asset_ids
    added_rows = [r for r in new_rows if r.asset_id not in old_asset_ids]
    return removed, added_rows


async def on_snapshot_complete(
    payload: dict,
    *,
    ws_consumer: Any,
    settings: Any,
) -> bool:
    """NOTIFY handler — recompute candidates + mutate ws_consumer subscription.

    Returns True if a refresh ran, False if debounced.

    Debounce: only one refresh runs per REFRESH_DEBOUNCE_S window. Multiple
    NOTIFYs collapse to a single refresh.

    Mutation contract (Plan 04 ↔ Plan 05): writes to
    ``ws_consumer._subscribed_assets`` directly (private attribute). The
    public ``subscribed_assets`` property returns a defensive copy.
    """
    global _last_refresh_at_s
    now = time.monotonic()
    elapsed = now - _last_refresh_at_s
    if elapsed < REFRESH_DEBOUNCE_S:
        logger.info(
            f"candidate refresh debounced "
            f"(elapsed={elapsed:.1f}s < {REFRESH_DEBOUNCE_S}s) "
            f"snapshot_id={payload.get('snapshot_id')}"
        )
        return False
    _last_refresh_at_s = now

    new_rows = compute_candidates(
        settings,
        getattr(settings, "candidate_scanner_yaml", None),
        getattr(settings, "candidate_watchlist_yaml", None),
    )
    new_asset_ids = [r.asset_id for r in new_rows]

    # Diff for log surface — mutation is unconditional (we hand the new list
    # to ws_consumer; the consumer applies internal subscribe/unsubscribe on
    # next reconnect — Plan 04 contract).
    old_asset_ids = set(ws_consumer.subscribed_assets)
    removed, added = diff_candidate_sets(old_asset_ids, new_rows)
    logger.info(
        f"candidate refresh: +{len(added)} -{len(removed)} "
        f"total={len(new_asset_ids)} (cap={MAX_CANDIDATES}) "
        f"snapshot_id={payload.get('snapshot_id')}"
    )

    # Mutate _subscribed_assets directly — Plan 04 design contract.
    ws_consumer._subscribed_assets = list(new_asset_ids)
    return True
