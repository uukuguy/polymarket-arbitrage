"""L3 promote module — module-level state + public getters + promoter.

Phase 05 D-02 / D-05 / D-09 / D-13 / D-14. Two-plan delivery:

- **Plan 05-03 (scaffold):** state + getters + STUB ``promote_run`` /
  ``run_periodic``. The mirror needs ``_last_book_levels_write_at_s`` to
  exist NOW so the chain-truth anchor mutation compiles and is observable
  from /health (Plan 04 sub-check).
- **Plan 05-04 (this file, AUGMENT):** real ``promote_run`` body + raw
  asyncio ``run_periodic`` cron + Yes/No expansion (D-05 N=5 → 10 tokens)
  + l2_candidates.l3_promoted_at_ts write-through (D-08 surface, Blocker
  #1). Module-level state declarations + getter functions PRESERVED
  VERBATIM from Plan 03 (Warning #6 — wave-2-delete-wave-3 anti-pattern
  prevention).

═══ Chain-truth contract ════════════════════════════════════════════════
Every getter here reads a field that the write side really mutates:

- ``_last_promote_at_s``            ← ``promote_run`` success path
- ``_last_book_levels_write_at_s``  ← ``L2SupabaseMirror.push_book_levels`` (Plan 03)
- ``_l3_active_set``                ← ``promote_run`` success path

There is **no config-flag gate** between the write-side mutation and the
getter — surfacing through getters is the chain. /health policy converts
None to "cold-start: warn" (CLAUDE.md §chain-truth + feedback memo
``feedback_code-vs-chain-truth-2026-05``).

═══ Cold-start trap ═════════════════════════════════════════════════════
Both timestamp anchors use ``None`` as the sentinel — /health treats
``None`` as cold-start (warn), NOT as "fresh just now". **Never**
initialize either anchor to ``0.0`` (precedent
``feedback_cold-start-debounce-trap-2026-05``).

═══ Recipe trust tier ═══════════════════════════════════════════════════
``l3-promote.yaml`` is the 3rd recipe trust tier (scanner.py module
docstring): source-controlled yaml on disk loaded via DIRECT
``Recipe(..., _is_trusted=True)`` construction (NOT ``Recipe.from_yaml``
which hard-codes ``_is_trusted=False``). Trust is granted because the
file lives in the repo and is modified via PR + review.

═══ Supabase outage policy (Open Question #5) ═══════════════════════════
On Supabase outage (create_client raises, tob fetch raises, scanner
raises, markets_latest fetch raises), the promoter FREEZES
``_l3_active_set`` — last-known-good — rather than clearing. This lets
the live websocket keep streaming the existing tokens; /health surfaces
the staleness via ``l3:last_promote_at_s`` age.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import sentry_sdk
from loguru import logger
from supabase import create_client

# ─────────────────────────────────────────────────────────────────────────
# Module-level state — DURABLE (Plan 03 scaffold; Plan 04 augments BODIES
# but PRESERVES declarations + getter functions verbatim per Warning #6).
# ─────────────────────────────────────────────────────────────────────────
_l3_active_set: set[str] = set()
_last_promote_at_s: float | None = None
_last_book_levels_write_at_s: float | None = None

# Plan 04 additions — last-known-good fallbacks for Open Q #5 freeze policy
# AND the reverse map needed to compute MARKET-level diffs from a
# TOKEN-level _l3_active_set on subsequent ticks.
_last_known_tob_rows: list[dict] | None = None
_last_known_market_token_map: dict[str, tuple[str | None, str | None]] | None = None


# ─────────────────────────────────────────────────────────────────────────
# Public getters — DURABLE; consumed by /health (Plan 04) and dashboard.
# (Preserved verbatim from Plan 03 scaffold — Warning #6.)
# ─────────────────────────────────────────────────────────────────────────


def get_l3_active_set() -> set[str]:
    """Return a defensive copy of the L3 active asset_id set.

    Callers must NOT mutate the returned set — it is a copy of the module's
    private ``_l3_active_set``. The promoter (this module) is the sole
    writer.
    """
    return set(_l3_active_set)


def get_l3_active_count() -> int:
    """Cardinality of the L3 active set — read by /health l3:active_count."""
    return len(_l3_active_set)


def get_last_promote_at_s() -> float | None:
    """Wall-clock seconds since epoch of the last successful ``promote_run``.

    Returns ``None`` if the promoter has never run (cold-start). The /health
    sub-check maps this to pass/warn/fail by age.
    """
    return _last_promote_at_s


def get_last_book_levels_write_at_s() -> float | None:
    """Wall-clock seconds since epoch of the last successful book_levels write.

    Mutated by ``L2SupabaseMirror.push_book_levels`` (Plan 05-03 success path)
    — chain-truth anchor for /health l3:last_book_levels_write_at_s.
    Returns ``None`` if no write has succeeded yet (cold-start sentinel).
    """
    return _last_book_levels_write_at_s


def is_book_levels_write_overdue(now_s: float, warn_s: float = 120.0) -> bool:
    """True when the last book_levels write is older than ``warn_s`` (or never).

    Predicate consumed by /health l3:last_book_levels_write_at_s.
    Exposed here so Plan 03 callers can also use it in tests or local
    diagnostics. The ``warn_s`` default is the 2-minute pass/warn threshold.
    """
    if _last_book_levels_write_at_s is None:
        return True
    return (now_s - float(_last_book_levels_write_at_s)) >= warn_s


# ─────────────────────────────────────────────────────────────────────────
# Internal helpers — fetch / temp-DB / recipe load / mirror write-through.
# ─────────────────────────────────────────────────────────────────────────


def _fetch_latest_tob_rows_from_supabase(client: Any) -> list[dict]:
    """Pull recent ``l2_top_of_book`` rows from the last hour.

    Bounded to first 1000 rows (PostgREST page-cap per Phase 04 D-01
    research) — N=5 selection happens server-side via ORDER BY + LIMIT in
    the recipe SQL, so we don't paginate here.
    """
    cutoff_ms = int(time.time() * 1000) - 3600 * 1000
    cutoff_iso = (
        datetime.fromtimestamp(cutoff_ms / 1000, tz=UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )
    resp = (
        client.table("l2_top_of_book")
        .select(
            "asset_id, ts, best_bid, best_ask, spread, "
            "mid_price, depth_yes_usd, depth_no_usd"
        )
        .gte("ts", cutoff_iso)
        .order("ts", desc=True)
        .limit(1000)
        .execute()
    )
    return list(resp.data or [])


def _fetch_market_token_map(
    client: Any, yes_asset_ids: list[str]
) -> dict[str, tuple[str | None, str | None]]:
    """Fetch complete outcome identity for selected Yes-side TOB assets.

    ``l2_top_of_book.asset_id`` is the Yes token ID. Alembic 006 makes the
    paired No token durable in ``markets_latest``. The returned mapping is
    therefore keyed by the selected Yes token, not by a nonexistent market
    ``asset_id`` column.
    """
    if not yes_asset_ids:
        return {}
    resp = (
        client.table("markets_latest")
        .select("yes_token_id, no_token_id")
        .in_("yes_token_id", yes_asset_ids)
        .execute()
    )
    out: dict[str, tuple[str | None, str | None]] = {}
    for row in resp.data or []:
        yes_token_id = row.get("yes_token_id")
        if yes_token_id:
            key = str(yes_token_id).strip()
            if key:
                out[key] = (yes_token_id, row.get("no_token_id"))
    return out


def _iso_to_epoch_ms(ts_val: Any) -> int | None:
    """Convert tob ``ts`` to INTEGER epoch ms (Blocker #2).

    Accepts:
      - int/float (assumed ms if > 1e12 else seconds)
      - str ISO 8601 with optional ``Z`` / timezone offset
      - None → None
    """
    if ts_val is None:
        return None
    if isinstance(ts_val, bool):  # bool is subclass of int — reject
        return None
    if isinstance(ts_val, (int, float)):
        return int(ts_val) if ts_val > 1e12 else int(ts_val * 1000)
    if isinstance(ts_val, str):
        try:
            s = ts_val.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return int(dt.timestamp() * 1000)
        except (ValueError, TypeError):
            return None
    return None


def _build_tob_temp_db(rows: list[dict]) -> Path:
    """Materialize tob rows into a named-temp-file SQLite for scanner.

    ═══ Why a NAMED TEMP FILE, not :memory: ═════════════════════════════
    ``scanner.run_recipe`` opens a SEPARATE connection via
    ``sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)``
    (scanner.py:142-143). Two ``:memory:`` connections are two independent
    empty databases. A real file is shared across connections. Same
    rationale as ``l2_temp_db.build_temp_db`` (Phase 04 Plan 02 Pitfall 1).

    ═══ Why a ``markets`` table (and empty ``question_translations``)? ═══
    ``scanner.run_recipe`` hard-codes
    ``SELECT m.*, qt.question_zh FROM markets m LEFT JOIN
    question_translations qt ON qt.question_en = m.question``
    (scanner.py:147-154). The recipe's WHERE references ``spread``,
    ``depth_yes_usd``, ``ts``. So we create a ``markets`` table holding
    the tob columns (plus a NULL ``question`` for the JOIN to be a no-op)
    and an empty ``question_translations`` table so the JOIN doesn't fault
    on a missing table.

    ═══ Blocker #2: ts as INTEGER epoch ms ══════════════════════════════
    ``ts`` is stored as INTEGER epoch milliseconds. The recipe predicate
    ``ts > (strftime('%s','now','-1 hour') * 1000)`` performs a pure
    numeric comparison. ISO-TEXT lex comparison would silently mis-order
    across timezone / ``T`` format variations.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
    tmp.close()
    path = Path(tmp.name)
    conn = sqlite3.connect(str(path))
    try:
        # The ``markets`` table holds the tob columns the recipe needs.
        # Adding ``question TEXT`` so scanner's LEFT JOIN does not fail.
        conn.execute(
            """
            CREATE TABLE markets (
                asset_id      TEXT,
                ts            INTEGER,
                best_bid      REAL,
                best_ask      REAL,
                spread        REAL,
                mid_price     REAL,
                depth_yes_usd REAL,
                depth_no_usd  REAL,
                question      TEXT
            )
            """
        )
        # Empty question_translations — scanner.run_recipe's LEFT JOIN
        # needs the table to exist; rows are not required.
        conn.execute(
            """
            CREATE TABLE question_translations (
                question_hash    TEXT PRIMARY KEY,
                question_en      TEXT,
                question_zh      TEXT
            )
            """
        )

        cols = (
            "asset_id",
            "ts",
            "best_bid",
            "best_ask",
            "spread",
            "mid_price",
            "depth_yes_usd",
            "depth_no_usd",
        )
        normalized: list[tuple[Any, ...]] = []
        for r in rows:
            vals: list[Any] = []
            for c in cols:
                v = r.get(c)
                if c == "ts":
                    v = _iso_to_epoch_ms(v)
                vals.append(v)
            # Append NULL `question` — LEFT JOIN remains a no-op.
            vals.append(None)
            normalized.append(tuple(vals))
        placeholder = ",".join(["?"] * (len(cols) + 1))
        conn.executemany(
            f"INSERT INTO markets ({','.join(cols)}, question) VALUES ({placeholder})",
            normalized,
        )
        conn.commit()
    finally:
        conn.close()
    return path


def _load_recipe(recipe_yaml_path: Path) -> Any:
    """Load l3-promote.yaml as a *trusted* (source-controlled) Recipe.

    This is the 3rd recipe tier (see scanner.py docstring §Trusted-recipe
    tiers): yaml on disk loaded with ``_is_trusted=True`` because the file
    lives in the repo and is modified via PR review. Layer 1 (read-only
    URI) + Layer 4 (limit) still apply via ``scanner.run_recipe``.

    Diagnostic env override (quick task 260602-diag-depth):
    ``POLYARB_L3_DEPTH_MIN_USD`` — when set to a float, rewrites the WHERE
    clause's ``depth_yes_usd > 500`` filter to ``depth_yes_usd > <env>``.
    Baseline yaml (=500) is untouched on disk. Invalid env values fall back
    to the yaml baseline + log warning (defensive: a typo in fly secret
    must not silently disable the L3 promoter). Per CLAUDE.md "experiment
    values never touch baseline defaults".
    """
    import yaml

    from polyarb.observation.recipes import Recipe

    with open(recipe_yaml_path) as f:
        data = yaml.safe_load(f)
    body = data["recipes"]["l3-promote"]

    where = body["where"]
    env_val = os.environ.get("POLYARB_L3_DEPTH_MIN_USD")
    if env_val is not None:
        try:
            override_min = float(env_val)
            if "depth_yes_usd > 500" not in where:
                logger.warning(
                    "l3-promote: POLYARB_L3_DEPTH_MIN_USD set but baseline "
                    "'depth_yes_usd > 500' not found in yaml — leaving WHERE unchanged"
                )
            else:
                where = where.replace(
                    "depth_yes_usd > 500", f"depth_yes_usd > {override_min:g}"
                )
                logger.info(
                    f"l3-promote: depth threshold overridden via "
                    f"POLYARB_L3_DEPTH_MIN_USD={override_min:g} (yaml baseline=500)"
                )
        except (ValueError, TypeError):
            logger.warning(
                f"l3-promote: invalid POLYARB_L3_DEPTH_MIN_USD={env_val!r} — "
                f"falling back to yaml baseline"
            )

    return Recipe(
        name="l3-promote",
        description=body.get("description", ""),
        where=where,
        order_by=body["order_by"],
        limit=int(body["limit"]),
        _is_trusted=True,
    )


def _mirror_l3_promoted_at_ts(
    client: Any,
    added_yes_asset_ids: list[str],
    removed_yes_asset_ids: list[str],
) -> None:
    """Write-through ``l2_candidates.l3_promoted_at_ts`` (Blocker #1).

    Fail-soft per D-12 envelope: failure logs + Sentry breadcrumb
    ``category="l2-mirror"`` and returns; never raises. The in-memory
    ``_l3_active_set`` is the source of truth — this mirror only feeds the
    dashboard L3 badge (``/candidates``).

    Called with the Yes token IDs stored as ``l2_candidates.asset_id``. The
    Yes/No expansion happens at WS-subscribe time; the dashboard candidate
    surface remains keyed by the L2 Yes asset.
    """
    if not added_yes_asset_ids and not removed_yes_asset_ids:
        return
    now_iso = (
        datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    try:
        if added_yes_asset_ids:
            (
                client.table("l2_candidates")
                .update({"l3_promoted_at_ts": now_iso})
                .in_("asset_id", list(added_yes_asset_ids))
                .execute()
            )
        if removed_yes_asset_ids:
            (
                client.table("l2_candidates")
                .update({"l3_promoted_at_ts": None})
                .in_("asset_id", list(removed_yes_asset_ids))
                .execute()
            )
        sentry_sdk.add_breadcrumb(
            category="l2-mirror",
            level="info",
            message=(
                f"l3_promoted_at_ts mirror "
                f"+{len(added_yes_asset_ids)} "
                f"-{len(removed_yes_asset_ids)}"
            ),
            data={
                "added": len(added_yes_asset_ids),
                "removed": len(removed_yes_asset_ids),
                "table": "l2_candidates",
            },
        )
    except Exception as e:  # noqa: BLE001 — fail-soft per D-12
        logger.warning(
            f"l3-promote: l2_candidates write-through failed "
            f"(mirror only, in-memory state intact): {e!r}"
        )
        sentry_sdk.add_breadcrumb(
            category="l2-mirror",
            level="warning",
            message="l3_promoted_at_ts mirror failed",
            data={
                "error": str(e)[:200],
                "added": len(added_yes_asset_ids),
                "removed": len(removed_yes_asset_ids),
                "table": "l2_candidates",
            },
        )


# ─────────────────────────────────────────────────────────────────────────
# Public API — promote_run + run_periodic.
# (Plan 04 REPLACES Plan 03 stub BODIES; state + getters preserved above.)
# ─────────────────────────────────────────────────────────────────────────


async def promote_run(
    *,
    settings: Any,
    ws_consumer: Any,
    recipe_yaml_path: Path,
) -> dict:
    """One promote tick. Fail-soft per D-12 envelope; freeze on outage.

    Steps:
      1. Resolve Supabase creds; freeze on missing.
      2. Build supabase client; freeze on create_client error.
      3. Fetch latest tob rows (last 1h) — on outage, use last-known-good.
      4. Apply recipe (yaml → trusted Recipe) via scanner+temp-DB
         (Blocker #2 epoch-ms predicate).
      5. Fetch ``yes_token_id`` + ``no_token_id`` from ``markets_latest``
         for the 5 promoted markets — expand to 10 tokens (D-05 N=5).
      6. Diff new token set vs ``_l3_active_set``; map to MARKET-level
         diff for the l2_candidates write-through.
      7. Apply ws_consumer.add_subscriptions / remove_subscriptions.
      8. Write-through l2_candidates.l3_promoted_at_ts (Blocker #1,
         fail-soft).
      9. Mutate state + chain-truth anchor.

    Returns ``{"added": [...], "removed": [...], ...}`` for logging.
    """
    global _l3_active_set, _last_promote_at_s
    global _last_known_tob_rows, _last_known_market_token_map

    # ── 1) Resolve creds ────────────────────────────────────────────────
    supabase_url = getattr(settings, "supabase_url", "")
    service_key = ""
    try:
        service_key = settings.supabase_service_key.get_secret_value()
    except AttributeError:
        # Defensive — service_key may not be a SecretStr under test mocks.
        service_key = ""

    if not (supabase_url and service_key):
        logger.warning(
            "l3-promote: supabase creds missing — freezing _l3_active_set"
        )
        return {"added": [], "removed": [], "skipped": "no-supabase-creds"}

    # ── 2) Build client ─────────────────────────────────────────────────
    try:
        client = create_client(supabase_url, service_key)
    except Exception as e:  # noqa: BLE001 — fail-soft envelope
        logger.error(f"l3-promote: create_client failed: {e!r}")
        sentry_sdk.add_breadcrumb(
            category="l3-promote",
            level="warning",
            message="create_client failed",
            data={"error": str(e)[:200]},
        )
        return {"added": [], "removed": [], "skipped": "create-client-failed"}

    # ── 3) Fetch tob ────────────────────────────────────────────────────
    try:
        tob_rows = _fetch_latest_tob_rows_from_supabase(client)
        _last_known_tob_rows = tob_rows
    except Exception as e:  # noqa: BLE001
        logger.error(
            f"l3-promote: tob fetch failed: {e!r} — using last-known rows"
        )
        sentry_sdk.add_breadcrumb(
            category="l3-promote",
            level="warning",
            message="tob fetch failed",
            data={"error": str(e)[:200]},
        )
        if _last_known_tob_rows is None:
            logger.warning(
                "l3-promote: no last-known tob rows either — freezing"
            )
            return {
                "added": [],
                "removed": [],
                "skipped": "fetch-failed-no-fallback",
            }
        tob_rows = _last_known_tob_rows

    # ── 4) Apply recipe via scanner + temp DB ───────────────────────────
    try:
        from polyarb.observation.scanner import run_recipe

        recipe = _load_recipe(recipe_yaml_path)
        db_path = _build_tob_temp_db(tob_rows)
        try:
            df = run_recipe(db_path, recipe)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                logger.warning(
                    f"l3-promote: temp DB cleanup failed: {db_path}"
                )
    except Exception as e:  # noqa: BLE001
        logger.error(
            f"l3-promote: scanner failed: {e!r} — freezing _l3_active_set"
        )
        sentry_sdk.add_breadcrumb(
            category="l3-promote",
            level="warning",
            message="scanner failed",
            data={"error": str(e)[:200]},
        )
        return {"added": [], "removed": [], "skipped": "scanner-failed"}

    # df row count == N=5 Yes-side TOB assets (or fewer if not enough qualify).
    new_yes_asset_ids = sorted(
        str(aid) for aid in df["asset_id"].tolist() if aid
    )

    # Snapshot the prior MARKET→TOKEN map BEFORE step 5 overwrites it, so
    # step 6 can compute the MARKET-level removed set correctly. Without
    # this snapshot, step 5's _fetch_market_token_map result (containing
    # only NEW markets) would replace the prior map, leaving step 6's
    # reverse lookup blind to any removed market (Blocker #1 regression).
    prior_market_token_map: dict[str, tuple[str | None, str | None]] = dict(
        _last_known_market_token_map or {}
    )

    # ── 5) Expand 5 markets → 10 tokens (Yes+No per D-05 / Warning #13) ─
    try:
        token_map = _fetch_market_token_map(client, new_yes_asset_ids)
        _last_known_market_token_map = token_map
    except Exception as e:  # noqa: BLE001
        logger.error(
            f"l3-promote: market token map fetch failed: {e!r} — using last-known"
        )
        sentry_sdk.add_breadcrumb(
            category="l3-promote",
            level="warning",
            message="market token map fetch failed",
            data={"error": str(e)[:200]},
        )
        token_map = _last_known_market_token_map or {}

    new_token_set: set[str] = set()
    accepted_yes_asset_ids: set[str] = set()
    for yes_asset_id in new_yes_asset_ids:
        raw_yes, raw_no = token_map.get(yes_asset_id, (None, None))
        yes_token = str(raw_yes).strip() if raw_yes is not None else ""
        no_token = str(raw_no).strip() if raw_no is not None else ""
        pair = {yes_token, no_token}
        if (
            not yes_token
            or not no_token
            or yes_token != yes_asset_id
            or len(pair) != 2
            or bool(pair & new_token_set)
        ):
            logger.warning(
                f"l3-promote: rejecting incomplete or duplicate token pair "
                f"for yes_asset_id={yes_asset_id}"
            )
            continue
        new_token_set.update(pair)
        accepted_yes_asset_ids.add(yes_asset_id)

    # ── 6) Diff token sets + MARKET-level diff for the mirror ───────────
    added = sorted(new_token_set - _l3_active_set)
    removed = sorted(_l3_active_set - new_token_set)

    old_market_set: set[str] = set()
    for aid, (yes_tok, no_tok) in prior_market_token_map.items():
        if (yes_tok and yes_tok in _l3_active_set) or (
            no_tok and no_tok in _l3_active_set
        ):
            old_market_set.add(aid)
    new_market_set: set[str] = accepted_yes_asset_ids
    added_markets = sorted(new_market_set - old_market_set)
    removed_markets = sorted(old_market_set - new_market_set)

    # ── 7) Apply WS subscribe diff (fail-soft per D-12) ─────────────────
    try:
        if added:
            await ws_consumer.add_subscriptions(added)
        if removed:
            await ws_consumer.remove_subscriptions(removed)
    except Exception as e:  # noqa: BLE001
        logger.error(f"l3-promote: ws_consumer mutation failed: {e!r}")
        sentry_sdk.add_breadcrumb(
            category="l3-promote",
            level="warning",
            message="ws mutation failed",
            data={"error": str(e)[:200]},
        )
        # Fall through — still mutate state + mirror so /health reflects intent.

    # ── 8) Write-through l2_candidates.l3_promoted_at_ts (Blocker #1) ───
    _mirror_l3_promoted_at_ts(client, added_markets, removed_markets)

    # ── 9) Mutate state + chain-truth anchor ────────────────────────────
    _l3_active_set = new_token_set
    _last_promote_at_s = time.time()

    sentry_sdk.add_breadcrumb(
        category="l3-promote",
        level="info",
        message=(
            f"promote_run ok +{len(added)} -{len(removed)} "
            f"markets={len(accepted_yes_asset_ids)} tokens={len(new_token_set)}"
        ),
        data={
            "added": len(added),
            "removed": len(removed),
            "markets": len(accepted_yes_asset_ids),
            "tokens": len(new_token_set),
        },
    )
    logger.info(
        f"l3-promote: +{len(added)} -{len(removed)} "
        f"markets={len(accepted_yes_asset_ids)} tokens={len(new_token_set)} "
        f"(chain-truth: _last_promote_at_s={_last_promote_at_s:.0f})"
    )
    return {
        "added": added,
        "removed": removed,
        "added_markets": added_markets,
        "removed_markets": removed_markets,
    }


async def run_periodic(
    *,
    stop_event: asyncio.Event,
    settings: Any,
    ws_consumer: Any,
    recipe_yaml_path: Path,
    interval_s: float = 300.0,
) -> None:
    """5-min cron loop matching ws_consumer.run wait_for pattern.

    Per orchestrator cross-pattern decision #4: no scheduler dep is added
    here (not in pyproject); use raw
    ``asyncio.wait_for(stop_event.wait(), timeout=...)`` instead — same
    pattern as ws_consumer.run's idle-wait loop.
    """
    logger.info(f"l3-promote: run_periodic started (interval={interval_s}s)")
    # Run once immediately so /health gets a fresh anchor without waiting
    # the full cron interval.
    try:
        await promote_run(
            settings=settings,
            ws_consumer=ws_consumer,
            recipe_yaml_path=recipe_yaml_path,
        )
    except Exception as e:  # noqa: BLE001 — defense-in-depth
        logger.error(f"l3-promote initial run failed: {e!r}")

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_s)
            # stop_event signalled — exit cleanly.
            break
        except TimeoutError:
            pass  # interval elapsed — run another tick
        try:
            await promote_run(
                settings=settings,
                ws_consumer=ws_consumer,
                recipe_yaml_path=recipe_yaml_path,
            )
        except Exception as e:  # noqa: BLE001 — defense-in-depth
            logger.error(f"l3-promote tick failed: {e!r}")
    logger.info("l3-promote: run_periodic stopped")
