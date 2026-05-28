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

import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger
from supabase import create_client

# REUSE Phase 01.1 scanner verbatim (D-04)
from polyarb.observation.l2_temp_db import build_temp_db, warn_null_filled_recipe_columns
from polyarb.observation.scanner import list_all_recipes, run_recipe
from polyarb.observation.watchlist import load_watchlist

# ── Module constants (locked) ─────────────────────────────────────────────
MAX_CANDIDATES: int = 500  # R9 hard cap
REFRESH_DEBOUNCE_S: float = 60.0  # SP8 cross-bug check #1

# Module-level debounce state. Acceptable for Phase 03 (single-process L2
# daemon). If a future phase introduces multi-instance L2, move this into
# SQLite (l2_candidates) or Redis so all instances share the floor.
_last_refresh_at_s: float = 0.0

# ── Phase 04 Plan 02 — D-01 fail-soft state ──────────────────────────────
# Last successfully-fetched markets_latest rows. on_snapshot_complete falls
# back to this snapshot if a fresh Supabase fetch raises (fail-soft envelope,
# same pattern as the per-recipe failure isolation at line 116-118).
_last_known_markets_rows: list[dict] | None = None

# Wall-clock seconds since epoch of the last SUCCESSFUL Supabase fetch.
# Read by the /health candidates:supabase_fetch_age_seconds sub-check
# (Task 3 — chain-truth surface). None == cold-start (never fetched).
_last_fetch_success_at_s: float | None = None


def _record_fetch_success() -> None:
    """Mark a successful Supabase fetch — drives /health sub-check freshness."""
    global _last_fetch_success_at_s
    _last_fetch_success_at_s = time.time()


def get_last_fetch_success_at_s() -> float | None:
    """Public getter for /health candidates:supabase_fetch_age_seconds.

    Returned value is the wall-clock timestamp of the last successful Supabase
    fetch, or None if no fetch has ever succeeded (cold-start). The /health
    sub-check (l2_health.py) maps this to status pass/warn/fail by age.

    Chain-truth note (§1.6): this getter reads a field that the fetch path
    REALLY mutates (every successful fetch calls _record_fetch_success). It is
    NOT a dead-code config flag — Inj L2-2 RCA prevention.
    """
    return _last_fetch_success_at_s


# ── Phase 04 Plan 02 — D-01 Supabase pagination ──────────────────────────
def _fetch_all_markets_latest(client: Any) -> list[dict]:
    """Fetch all markets_latest rows with pagination.

    PostgREST default cap = 1000 rows. ``markets_latest`` has ~6729 rows.
    A plain ``.select("*").execute()`` silently truncates to 1000 (RESEARCH
    Pitfall 2). We MUST page via ``.range(offset, offset+page_size-1)`` and
    terminate when ``len(batch) < page_size``.
    """
    rows: list[dict] = []
    offset = 0
    page_size = 1000
    while True:
        resp = (
            client.table("markets_latest")
            .select("*")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch: list[dict] = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


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
    markets_rows: list[dict] | None = None,
) -> list[CandidateRow]:
    """Build the current candidate set as union(scanner-recipes, watchlist).

    Args:
        settings: object exposing ``db_path`` (Path to L1 SQLite).
        scanner_yaml: optional path to user recipes YAML (BUILTIN_RECIPES are
                      always merged regardless).
        watchlist_yaml: optional path to watchlist YAML.
        markets_rows: Phase 04 D-01 — when provided, build a named-temp-file
            SQLite from these Supabase ``markets_latest`` rows and use that
            for the scanner; when None, fall back to ``Path(settings.db_path)``
            (D-04 cold-start path, preserved for backwards-compat / tests).

    Returns:
        list[CandidateRow] capped at MAX_CANDIDATES; watchlist always retained.

    Dedup rule: watchlist OVERRIDES recipe on overlap (later source wins).

    Failure isolation: if a single recipe raises, log warning + continue
    with the other recipes (Rule 1 — never block refresh on one bad recipe).

    Temp-file cleanup (D-02): when ``markets_rows`` is provided, the built
    temp DB is ``os.unlink``ed in the finally block so /tmp does not leak.
    """
    # Phase 04 D-01: prefer the freshly-fetched Supabase rows when present.
    if markets_rows is not None:
        db_path = build_temp_db(markets_rows)
        cleanup_tmp = True
    else:
        db_path = Path(settings.db_path)
        cleanup_tmp = False

    try:
        return _compute_candidates_against(db_path, scanner_yaml, watchlist_yaml)
    finally:
        if cleanup_tmp:
            try:
                os.unlink(db_path)
            except OSError:
                # Best-effort cleanup — log but never block refresh on this.
                logger.warning(f"temp DB cleanup failed: {db_path}")


def _compute_candidates_against(
    db_path: Path,
    scanner_yaml: Path | None,
    watchlist_yaml: Path | None,
) -> list[CandidateRow]:
    """Internal: run recipes + watchlist against ``db_path`` (extracted to
    isolate temp-file cleanup in the wrapper). Body verbatim from the
    pre-D-01 ``compute_candidates`` — only the db_path argument changes."""
    out: dict[str, CandidateRow] = {}

    # ── 1) Scanner recipes (D-04 verbatim REUSE) ─────────────────────────
    if scanner_yaml is not None or _builtin_recipes_present():
        # Bugfix (Phase 04 Plan 02 — Rule 1 deviation): the previous
        # `list_all_recipes(scanner_yaml) if scanner_yaml else {}` dropped the
        # BUILTINS when scanner_yaml was None. list_all_recipes(None) returns
        # just the BUILTIN_RECIPES dict — exactly what the outer `or
        # _builtin_recipes_present()` invited. Phase 04 D-01 path is
        # scanner_yaml=None most of the time, so this latent bug had to be
        # fixed for builtins (near-end / coin-flip / etc) to drive candidates.
        recipes = list_all_recipes(scanner_yaml)
        for name, recipe in recipes.items():
            # Skip GROUP BY recipes — they produce aggregations, not asset rows.
            if recipe.group_by is not None:
                continue
            # Phase 04 D-02 fail-loud: warn when a recipe references columns
            # NULL-filled in the Supabase narrow projection.
            warn_null_filled_recipe_columns(recipe)
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
    def _liquidity_key(r: CandidateRow) -> tuple[float]:
        if r.ranking_score:
            liq = r.ranking_score.get("liquidity")
            if liq is not None:
                return (-float(liq),)
        return (1e18,)  # None / missing → goes last (most-positive key)

    recipe_rows.sort(key=_liquidity_key)
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
    mirror: Any | None = None,
) -> bool:
    """NOTIFY handler — recompute candidates + mutate ws_consumer subscription.

    Returns True if a refresh ran, False if debounced.

    Debounce: only one refresh runs per REFRESH_DEBOUNCE_S window. Multiple
    NOTIFYs collapse to a single refresh.

    Mutation contract (Plan 04 ↔ Plan 05): writes to
    ``ws_consumer._subscribed_assets`` directly (private attribute). The
    public ``subscribed_assets`` property returns a defensive copy.

    Plan 06 extension (D-07): if `mirror` is provided (L2SupabaseMirror
    instance), persist the diff to l2_candidates:
      - `added` rows → mirror.upsert_candidates(...)
      - `removed` asset_ids → mirror.mark_candidates_removed(...)
    Mirror calls are fail-soft (mirror's own envelope) — any failure surfaces
    in the mirror's loguru + Sentry breadcrumb but does NOT block the refresh.
    """
    global _last_refresh_at_s, _last_known_markets_rows
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

    # ── Phase 04 D-01: fetch markets_latest from Supabase (fail-soft) ────
    # NOTIFY payload only carries snapshot_id (RESEARCH Q5); the candidate
    # compute path needs the FULL markets_latest snapshot, so we round-trip
    # through Supabase REST. On failure we fall back to the last known good
    # rows so the candidate set freezes rather than collapses to empty.
    markets_rows: list[dict] | None = None
    supabase_url = getattr(settings, "supabase_url", "")
    service_key = ""
    try:
        service_key = settings.supabase_service_key.get_secret_value()
    except AttributeError:
        # service_key is not a SecretStr (defensive — possible under test mocks).
        service_key = ""
    if supabase_url and service_key:
        try:
            client = create_client(supabase_url, service_key)
            markets_rows = _fetch_all_markets_latest(client)
            _last_known_markets_rows = markets_rows
            _record_fetch_success()
            logger.info(
                f"candidate refresh: fetched {len(markets_rows)} rows from markets_latest"
            )
        except Exception as e:  # noqa: BLE001 — fail-soft envelope (same as recipe loop)
            logger.error(
                f"candidate refresh: supabase fetch failed: {e!r} — "
                f"using last known rows (count={len(_last_known_markets_rows or [])})"
            )
            markets_rows = _last_known_markets_rows
    # else: Supabase not configured (D-04 cold-start) — fall through with
    #       markets_rows=None so compute_candidates uses settings.db_path.

    new_rows = compute_candidates(
        settings,
        getattr(settings, "candidate_scanner_yaml", None),
        getattr(settings, "candidate_watchlist_yaml", None),
        markets_rows=markets_rows,
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

    # Plan 06 D-07: persist diff to dashboard mirror (fail-soft via mirror itself).
    if mirror is not None:
        if removed:
            mirror.mark_candidates_removed(list(removed))
        if added:
            snapshot_id = payload.get("snapshot_id")
            added_rows_dicts = [
                {
                    "snapshot_id": snapshot_id,
                    "recipe_name": r.recipe_name,
                    "asset_id": r.asset_id,
                    "market_id": r.market_id,
                    "event_id": r.event_id,
                    "source": r.source,
                    "ranking_score": r.ranking_score,
                }
                for r in added
            ]
            mirror.upsert_candidates(added_rows_dicts)

    return True
