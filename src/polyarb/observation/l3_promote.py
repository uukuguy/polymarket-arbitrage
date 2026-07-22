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

- ``_last_promote_at_s``            ← durable terminal promote-row append
- ``_last_book_levels_write_at_s``  ← ``L2SupabaseMirror.push_book_levels`` (Plan 03)
- ``_l3_active_set``                ← control-committed WS membership snapshot

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
the live websocket keep streaming the existing tokens; the append-only
terminal row surfaces the frozen reason without pretending control changed.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import sentry_sdk
from loguru import logger
from supabase import create_client

from polyarb.observation.l3_evidence import (
    AcceptanceConfig,
    L3EvidenceRuntime,
    PromoteRunRecord,
    PromoteRunResult,
    PromoteStatus,
    WsMembershipSnapshot,
    stable_sha256,
)
from polyarb.storage.l3_evidence_store import L3EvidenceStore

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
# Last durably recorded current dashboard target.  This is a bounded diagnostic
# cache only; stale recovery always reads l2_candidates in bounded DB pages.
_last_mirrored_market_ids: frozenset[str] = frozenset()
_MAX_TOKEN_MAP_CACHE = 1010  # 1000 fetched rows + exact current L3 identities
_MIRROR_RECONCILE_BATCH_SIZE = 100


def _utc_now() -> datetime:
    """Return the scheduler clock in UTC through one testable boundary."""
    return datetime.now(UTC)


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
    """Epoch seconds of the last durably persisted terminal promote run.

    Frozen, underfilled, and failed terminal rows also advance this persistence
    anchor. Their status remains explicit in the append-only ledger. A failed
    ledger write never advances it.
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
    """Pull the newest recent ``l2_top_of_book`` row per asset.

    Bounded to first 1000 rows (PostgREST page-cap per Phase 04 D-01
    research). PostgREST returns rows newest-first; collapse repeated snapshots
    before the recipe LIMIT so five rows mean five distinct markets.
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
    latest: list[dict] = []
    seen_assets: set[str] = set()
    for row in resp.data or []:
        asset_id = str(row.get("asset_id") or "").strip()
        if not asset_id or asset_id in seen_assets:
            continue
        seen_assets.add(asset_id)
        latest.append(row)
    return latest


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


@dataclass(frozen=True)
class _MirrorReconcileResult:
    succeeded: bool
    cleanup_pending: bool = False


def _mirror_l3_promoted_at_ts(
    client: Any,
    committed_yes_asset_ids: list[str],
) -> _MirrorReconcileResult:
    """Write-through ``l2_candidates.l3_promoted_at_ts`` (Blocker #1).

    Fail-soft per D-12 envelope: failure logs + Sentry breadcrumb
    ``category="l2-mirror"`` and returns; never raises. The in-memory
    The post-control committed snapshot is the source of truth — this mirror
    only feeds the dashboard L3 badge (``/candidates``).

    Called with the complete committed Yes-token target stored as
    ``l2_candidates.asset_id``.  The Yes/No expansion happens at WS-subscribe
    time; the dashboard candidate surface remains keyed by the L2 Yes asset.

    Stale recovery is intentionally database-driven.  Each tick reads at most
    ``_MIRROR_RECONCILE_BATCH_SIZE`` non-null, non-current rows and clears only
    those returned identities.  A full page is a conservative indication that
    more rows may remain, so the durable terminal outcome stays non-success and
    the next tick continues.
    """
    committed = sorted(set(committed_yes_asset_ids))
    if len(committed) > _MIRROR_RECONCILE_BATCH_SIZE:
        logger.warning(
            "l3-promote: committed mirror target exceeds bounded payload "
            "rows={}",
            len(committed),
        )
        return _MirrorReconcileResult(succeeded=False)
    now_iso = (
        datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    try:
        if committed:
            (
                client.table("l2_candidates")
                .update({"l3_promoted_at_ts": now_iso})
                .in_("asset_id", committed)
                .execute()
            )

        query = (
            client.table("l2_candidates")
            .select("asset_id")
            .not_.is_("l3_promoted_at_ts", "null")
        )
        if committed:
            query = query.not_.in_("asset_id", committed)
        response = (
            query.order("id")
            .limit(_MIRROR_RECONCILE_BATCH_SIZE)
            .execute()
        )
        rows = list(response.data or [])
        if len(rows) > _MIRROR_RECONCILE_BATCH_SIZE:
            raise ValueError("candidate badge query exceeded reconciliation cap")
        stale: set[str] = set()
        committed_set = set(committed)
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("candidate badge query returned a non-object row")
            asset_id = str(row.get("asset_id") or "").strip()
            if not asset_id:
                raise ValueError("candidate badge query returned an empty asset_id")
            if asset_id not in committed_set:
                stale.add(asset_id)
        stale_batch = sorted(stale)
        if len(stale_batch) > _MIRROR_RECONCILE_BATCH_SIZE:  # pragma: no cover - invariant
            raise ValueError("candidate badge cleanup exceeded reconciliation cap")
        if stale_batch:
            (
                client.table("l2_candidates")
                .update({"l3_promoted_at_ts": None})
                .in_("asset_id", stale_batch)
                .execute()
            )
        cleanup_pending = len(rows) == _MIRROR_RECONCILE_BATCH_SIZE
        sentry_sdk.add_breadcrumb(
            category="l2-mirror",
            level="info",
            message=(
                f"l3_promoted_at_ts mirror "
                f"committed={len(committed)} "
                f"stale={len(stale_batch)} "
                f"pending={cleanup_pending}"
            ),
            data={
                "committed": len(committed),
                "stale": len(stale_batch),
                "pending": cleanup_pending,
                "batch_cap": _MIRROR_RECONCILE_BATCH_SIZE,
                "table": "l2_candidates",
            },
        )
        return _MirrorReconcileResult(
            succeeded=True,
            cleanup_pending=cleanup_pending,
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
                "committed": len(committed),
                "batch_cap": _MIRROR_RECONCILE_BATCH_SIZE,
                "table": "l2_candidates",
            },
        )
        return _MirrorReconcileResult(succeeded=False)


# ─────────────────────────────────────────────────────────────────────────
# Public API — promote_run + run_periodic.
# (Plan 04 REPLACES Plan 03 stub BODIES; state + getters preserved above.)
# ─────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _PromoteTerminalDraft:
    status: PromoteStatus
    reason_code: str
    selected_count: int
    desired: frozenset[str]
    committed: frozenset[str]
    evidenced: frozenset[str]
    mapping: tuple[dict[str, str], ...]
    ws_generation: int
    added: frozenset[str] = frozenset()
    removed: frozenset[str] = frozenset()
    added_markets: frozenset[str] = frozenset()
    removed_markets: frozenset[str] = frozenset()
    add_succeeded: bool | None = None
    remove_succeeded: bool | None = None
    mirror_succeeded: bool = False


@dataclass(frozen=True)
class _PromoteStagedState:
    """Module state published only after the terminal ledger row is durable."""

    tob_rows: list[dict] | None
    market_token_map: dict[str, tuple[str | None, str | None]] | None
    active_set: frozenset[str]
    mirrored_market_ids: frozenset[str]


async def _finalize_promote_run(
    *,
    draft: _PromoteTerminalDraft,
    started_at: datetime,
    scheduled_at: datetime,
    run_seq: int,
    acceptance_config_hash: str,
    evidence_store: L3EvidenceStore | None,
    evidence_runtime: L3EvidenceRuntime | None,
    apply_mutations: bool,
    staged_state: _PromoteStagedState,
) -> PromoteRunResult:
    """Build and append one terminal row; never manufacture a retry."""
    global _l3_active_set, _last_known_market_token_map, _last_known_tob_rows
    global _last_mirrored_market_ids, _last_promote_at_s

    if not apply_mutations:
        return PromoteRunResult(
            status=draft.status,
            reason_code=draft.reason_code,
            desired=draft.desired,
            committed=draft.committed,
            evidenced=draft.evidenced,
            run_seq=run_seq,
            scheduled_at=scheduled_at,
            added=draft.added,
            removed=draft.removed,
            added_markets=draft.added_markets,
            removed_markets=draft.removed_markets,
            dry_run=True,
            persisted=False,
        )
    if evidence_store is None or evidence_runtime is None:  # pragma: no cover - guard
        raise ValueError("evidence_store and evidence_runtime are required")

    finished_at = datetime.now(UTC)
    record = PromoteRunRecord(
        boot_id=evidence_runtime.snapshot().boot_id,
        run_seq=run_seq,
        scheduled_at=scheduled_at,
        started_at=started_at,
        finished_at=finished_at,
        status=draft.status,
        reason_code=draft.reason_code,
        selected_count=draft.selected_count,
        desired_count=len(draft.desired),
        committed_count=len(draft.committed),
        evidenced_count=len(draft.evidenced),
        add_count=len(draft.added),
        remove_count=len(draft.removed),
        mapping_hash=stable_sha256(list(draft.mapping)),
        desired_hash=stable_sha256(sorted(draft.desired)),
        committed_hash=stable_sha256(sorted(draft.committed)),
        acceptance_config_hash=acceptance_config_hash,
        ws_generation=draft.ws_generation,
        add_succeeded=draft.add_succeeded,
        remove_succeeded=draft.remove_succeeded,
        mirror_succeeded=draft.mirror_succeeded,
        duration_ms=max(0, int((finished_at - started_at).total_seconds() * 1000)),
    )
    try:
        persisted = await evidence_store.append_promote_run(record)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - custom stores may raise
        logger.warning("l3-promote: terminal evidence append raised type={}", type(exc).__name__)
        persisted = False

    evidence_runtime.note_writer_result(
        persisted,
        finished_at,
        "ok" if persisted else "promote_append_failed",
    )
    if persisted:
        evidence_runtime.mark_promote_persisted(finished_at)
        _last_known_tob_rows = staged_state.tob_rows
        _last_known_market_token_map = staged_state.market_token_map
        _l3_active_set = set(staged_state.active_set)
        _last_mirrored_market_ids = staged_state.mirrored_market_ids
        _last_promote_at_s = finished_at.timestamp()
    return PromoteRunResult(
        status=draft.status,
        reason_code=draft.reason_code,
        desired=draft.desired,
        committed=draft.committed,
        evidenced=draft.evidenced,
        run_seq=run_seq,
        scheduled_at=scheduled_at,
        added=draft.added,
        removed=draft.removed,
        added_markets=draft.added_markets,
        removed_markets=draft.removed_markets,
        dry_run=False,
        persisted=persisted,
    )


def _mapping_rows(
    market_ids: set[str], token_map: dict[str, tuple[str | None, str | None]]
) -> tuple[dict[str, str], ...]:
    rows: list[dict[str, str]] = []
    for market_id in sorted(market_ids):
        yes_token, no_token = token_map[market_id]
        rows.append(
            {
                "market_id": market_id,
                "yes_token_id": str(yes_token),
                "no_token_id": str(no_token),
            }
        )
    return tuple(rows)


async def promote_run(
    *,
    settings: Any,
    ws_consumer: Any,
    recipe_yaml_path: Path,
    evidence_store: L3EvidenceStore | None = None,
    evidence_runtime: L3EvidenceRuntime | None = None,
    scheduled_at: datetime | None = None,
    run_seq: int | None = None,
    apply_mutations: bool = True,
) -> PromoteRunResult:
    """Execute one proposal/control/mirror transaction and finalize it once."""
    started_at = datetime.now(UTC)
    if apply_mutations and (evidence_store is None or evidence_runtime is None):
        raise ValueError(
            "evidence_store and evidence_runtime are required when apply_mutations=True"
        )

    runtime_status = evidence_runtime.snapshot() if evidence_runtime is not None else None
    effective_run_seq = (
        evidence_runtime.next_run_seq()
        if run_seq is None and evidence_runtime is not None
        else (0 if run_seq is None else run_seq)
    )
    effective_scheduled_at = started_at if scheduled_at is None else scheduled_at
    if effective_scheduled_at.tzinfo is None or effective_scheduled_at.utcoffset() != UTC.utcoffset(
        effective_scheduled_at
    ):
        raise ValueError("scheduled_at must be timezone-aware UTC")

    if runtime_status is not None:
        initial = WsMembershipSnapshot(
            generation=runtime_status.ws_generation,
            desired=runtime_status.desired,
            committed=runtime_status.committed,
            evidenced=runtime_status.evidenced,
            evidenced_at=runtime_status.evidenced_at,
        )
        try:
            candidate = ws_consumer.l3_membership_snapshot()
            if not isinstance(candidate, WsMembershipSnapshot):
                raise TypeError("consumer returned an invalid membership snapshot")
            initial = candidate
        except Exception as exc:  # noqa: BLE001 - still terminalize dependency failure
            logger.warning("l3-promote: membership snapshot failed type={}", type(exc).__name__)
    else:
        initial = WsMembershipSnapshot(
            desired=frozenset(_l3_active_set), committed=frozenset(_l3_active_set)
        )

    staged_tob_rows = _last_known_tob_rows
    # Keep the durable cache by reference until a bounded replacement is
    # ready.  In particular, do not copy an oversized legacy cache merely to
    # terminalize a fail-closed cleanup tick.
    staged_market_token_map = _last_known_market_token_map
    staged_active_set = frozenset(_l3_active_set)
    staged_mirrored_market_ids = _last_mirrored_market_ids

    runtime_hash = runtime_status.acceptance_config_hash if runtime_status is not None else "0" * 64
    acceptance_config_invalid = False
    try:
        code_version = (
            runtime_status.identity.code_version
            if runtime_status is not None
            else "dry-run"
        )
        calculated_hash = AcceptanceConfig.from_settings(
            settings, recipe_yaml_path, code_version
        ).digest()
    except Exception as exc:  # noqa: BLE001
        logger.warning("l3-promote: acceptance config invalid type={}", type(exc).__name__)
        calculated_hash = runtime_hash
        acceptance_config_invalid = True

    async def finish(draft: _PromoteTerminalDraft) -> PromoteRunResult:
        staged_state = _PromoteStagedState(
            tob_rows=staged_tob_rows,
            market_token_map=staged_market_token_map,
            active_set=staged_active_set,
            mirrored_market_ids=staged_mirrored_market_ids,
        )
        return await _finalize_promote_run(
            draft=draft,
            started_at=started_at,
            scheduled_at=effective_scheduled_at,
            run_seq=effective_run_seq,
            acceptance_config_hash=runtime_hash if runtime_status is not None else calculated_hash,
            evidence_store=evidence_store,
            evidence_runtime=evidence_runtime,
            apply_mutations=apply_mutations,
            staged_state=staged_state,
        )

    def early(status: PromoteStatus, reason: str) -> _PromoteTerminalDraft:
        return _PromoteTerminalDraft(
            status=status,
            reason_code=reason,
            selected_count=0,
            desired=initial.desired,
            committed=initial.committed,
            evidenced=initial.evidenced,
            mapping=(),
            ws_generation=initial.generation,
        )

    if acceptance_config_invalid:
        return await finish(early(PromoteStatus.FAILED, "acceptance_config_invalid"))
    if runtime_status is not None and calculated_hash != runtime_hash:
        return await finish(early(PromoteStatus.FAILED, "acceptance_config_mismatch"))

    supabase_url = getattr(settings, "supabase_url", "")
    try:
        service_key = settings.supabase_service_key.get_secret_value()
    except AttributeError:
        service_key = ""
    if not (supabase_url and service_key):
        return await finish(early(PromoteStatus.FROZEN, "no_supabase_creds"))

    try:
        client = create_client(supabase_url, service_key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("l3-promote: create client failed type={}", type(exc).__name__)
        return await finish(early(PromoteStatus.FROZEN, "create_client_failed"))

    try:
        tob_rows = _fetch_latest_tob_rows_from_supabase(client)
        staged_tob_rows = tob_rows
    except Exception as exc:  # noqa: BLE001
        logger.warning("l3-promote: tob fetch failed type={}", type(exc).__name__)
        if _last_known_tob_rows is None:
            return await finish(early(PromoteStatus.FROZEN, "tob_fetch_failed"))
        tob_rows = _last_known_tob_rows

    # Read the durable mapping without copying it.  A deployment upgraded
    # from an older build may already hold an oversized cache; the hard-limit
    # path must not construct another oversized mapping before it can cleanly
    # fail closed.
    prior_market_token_map = staged_market_token_map or {}
    recent_asset_ids = sorted(
        {
            str(row.get("asset_id") or "").strip()
            for row in tob_rows
            if str(row.get("asset_id") or "").strip()
        }
    )
    try:
        token_map = _fetch_market_token_map(client, recent_asset_ids)
        if not token_map:
            return await finish(early(PromoteStatus.FROZEN, "empty_token_map"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("l3-promote: token map failed type={}", type(exc).__name__)
        token_map = staged_market_token_map or {}
        if not token_map:
            return await finish(early(PromoteStatus.FROZEN, "token_map_failed"))

    yes_tob_rows = [
        row for row in tob_rows if str(row.get("asset_id") or "").strip() in token_map
    ]
    try:
        from polyarb.observation.scanner import run_recipe

        recipe = _load_recipe(recipe_yaml_path)
        db_path = _build_tob_temp_db(yes_tob_rows)
        try:
            frame = run_recipe(db_path, recipe)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                logger.warning("l3-promote: temp DB cleanup failed path={}", db_path)

        # DataFrame access and all post-selection normalization belong to the
        # same terminal boundary as scanner execution.  Malformed frames,
        # token values, or mapping construction must still append exactly one
        # bounded failed outcome.
        proposed_tokens: set[str] = set()
        accepted_markets: set[str] = set()
        for market_id in sorted(
            str(value) for value in frame["asset_id"].tolist() if value
        ):
            raw_yes, raw_no = token_map.get(market_id, (None, None))
            yes_token = str(raw_yes).strip() if raw_yes is not None else ""
            no_token = str(raw_no).strip() if raw_no is not None else ""
            pair = {yes_token, no_token}
            if (
                not yes_token
                or not no_token
                or yes_token != market_id
                or len(pair) != 2
                or bool(pair & proposed_tokens)
            ):
                continue
            proposed_tokens.update(pair)
            accepted_markets.add(market_id)

        mapping = _mapping_rows(accepted_markets, token_map)
        desired = frozenset(proposed_tokens)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("l3-promote: selection failed type={}", type(exc).__name__)
        return await finish(early(PromoteStatus.FAILED, "selection_failed"))
    if len(accepted_markets) != 5 or len(desired) != 10:
        return await finish(
            _PromoteTerminalDraft(
                status=PromoteStatus.UNDERFILLED,
                reason_code="underfilled",
                selected_count=len(accepted_markets),
                desired=desired,
                committed=initial.committed,
                evidenced=initial.evidenced,
                mapping=mapping,
                ws_generation=initial.generation,
            )
        )

    added = frozenset(desired - initial.committed)
    removed = frozenset(initial.committed - desired)
    if not apply_mutations:
        return await finish(
            _PromoteTerminalDraft(
                status=PromoteStatus.SUCCESS,
                reason_code="dry_run",
                selected_count=5,
                desired=desired,
                committed=initial.committed,
                evidenced=initial.evidenced,
                mapping=mapping,
                ws_generation=initial.generation,
                added=added,
                removed=removed,
            )
        )

    desired_set_succeeded = True
    try:
        ws_consumer.set_l3_desired(desired)
    except Exception as exc:  # noqa: BLE001 - terminal row still required
        logger.warning("l3-promote: desired update failed type={}", type(exc).__name__)
        desired_set_succeeded = False
    add_succeeded: bool | None = None
    remove_succeeded: bool | None = None
    control_identity_ok = desired_set_succeeded
    if removed and desired_set_succeeded:
        try:
            remove_succeeded = bool(await ws_consumer.remove_subscriptions(sorted(removed)))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("l3-promote: remove failed type={}", type(exc).__name__)
            remove_succeeded = False

    # Removal is the capacity gate.  Publish desired truth immediately, but
    # never grow committed membership until every required removal succeeds
    # and the consumer generation still matches the transaction snapshot.
    if desired_set_succeeded:
        try:
            control_snapshot = ws_consumer.l3_membership_snapshot()
            control_identity_ok = (
                isinstance(control_snapshot, WsMembershipSnapshot)
                and control_snapshot.generation == initial.generation
            )
        except Exception as exc:  # noqa: BLE001 - terminal row still required
            logger.warning(
                "l3-promote: control snapshot failed type={}", type(exc).__name__
            )
            control_identity_ok = False
    if (
        added
        and desired_set_succeeded
        and control_identity_ok
        and (not removed or remove_succeeded is True)
    ):
        try:
            add_succeeded = bool(await ws_consumer.add_subscriptions(sorted(added)))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("l3-promote: add failed type={}", type(exc).__name__)
            add_succeeded = False

    try:
        current = ws_consumer.l3_membership_snapshot()
    except Exception as exc:  # noqa: BLE001 - terminal row still required
        logger.warning("l3-promote: terminal snapshot failed type={}", type(exc).__name__)
        current = initial
        desired_set_succeeded = False
    if not isinstance(current, WsMembershipSnapshot):
        current = initial
        desired_set_succeeded = False
    if not desired_set_succeeded:
        add_succeeded = False if added else add_succeeded
        remove_succeeded = False if removed else remove_succeeded

    def token_pair(market_id: str) -> tuple[str | None, str | None] | None:
        return token_map.get(market_id) or prior_market_token_map.get(market_id)

    def committed_markets(tokens: frozenset[str]) -> set[str]:
        markets: set[str] = set()
        for source in (prior_market_token_map, token_map):
            for market_id, (yes_token, no_token) in source.items():
                if yes_token and no_token and {str(yes_token), str(no_token)} <= tokens:
                    markets.add(market_id)
        return markets

    prior_markets = committed_markets(initial.committed)
    current_markets = committed_markets(current.committed)
    added_markets = frozenset(current_markets - prior_markets)
    removed_markets = frozenset(prior_markets - current_markets)

    # The complete post-control committed target is mirrored every tick.  Stale
    # badges are discovered from bounded database pages inside the mirror
    # helper; process memory is never a cleanup source of truth.
    identity_limit_exceeded = len(current_markets) > _MIRROR_RECONCILE_BATCH_SIZE
    mirror_succeeded = False
    mirror_cleanup_pending = False
    if not identity_limit_exceeded:
        raw_mirror_result = _mirror_l3_promoted_at_ts(client, sorted(current_markets))
        if isinstance(raw_mirror_result, _MirrorReconcileResult):
            mirror_succeeded = raw_mirror_result.succeeded
            mirror_cleanup_pending = raw_mirror_result.cleanup_pending
        else:
            # Preserve compatibility with focused tests and injected fail-soft
            # mirrors that predate the richer bounded-reconciliation result.
            mirror_succeeded = bool(raw_mirror_result)

        # A successful bounded DB reconciliation (complete or pending) makes
        # legacy cleanup memory disposable.  Publish the pruned cache only
        # after this tick's terminal evidence row is durable.
        essential_market_ids = set(current_markets)
        cache_market_ids = set(token_map) | essential_market_ids
        if len(cache_market_ids) > _MAX_TOKEN_MAP_CACHE:
            optional = sorted(set(token_map) - essential_market_ids)
            available = _MAX_TOKEN_MAP_CACHE - len(essential_market_ids)
            cache_market_ids = essential_market_ids | set(optional[:available])
        if mirror_succeeded:
            staged_market_token_map = {
                market_id: pair
                for market_id in sorted(cache_market_ids)
                if (pair := token_pair(market_id)) is not None
            }
    staged_active_set = current.committed
    if mirror_succeeded and not identity_limit_exceeded:
        staged_mirrored_market_ids = frozenset(current_markets)
    controls_ok = (not added or add_succeeded is True) and (
        not removed or remove_succeeded is True
    )
    if not desired_set_succeeded:
        status, reason = PromoteStatus.FAILED, "desired_update_failed"
    elif not control_identity_ok or current.generation != initial.generation:
        status, reason = PromoteStatus.FAILED, "generation_changed"
    elif removed and remove_succeeded is not True:
        status, reason = PromoteStatus.FAILED, "remove_failed"
    elif added and add_succeeded is not True:
        status, reason = PromoteStatus.FAILED, "add_failed"
    elif current.desired != desired or current.committed != desired:
        status, reason = PromoteStatus.FAILED, "membership_mismatch"
    elif identity_limit_exceeded:
        status, reason = PromoteStatus.FAILED, "identity_limit_exceeded"
    elif not mirror_succeeded:
        status, reason = PromoteStatus.FAILED, "mirror_failed"
    elif mirror_cleanup_pending:
        status, reason = PromoteStatus.FAILED, "mirror_cleanup_pending"
    elif not controls_ok:
        status, reason = PromoteStatus.FAILED, "control_failed"
    else:
        status, reason = PromoteStatus.SUCCESS, "ok"

    return await finish(
        _PromoteTerminalDraft(
            status=status,
            reason_code=reason,
            selected_count=len(accepted_markets),
            desired=current.desired,
            committed=current.committed,
            evidenced=current.evidenced,
            mapping=mapping,
            ws_generation=current.generation,
            added=added,
            removed=removed,
            added_markets=added_markets,
            removed_markets=removed_markets,
            add_succeeded=add_succeeded,
            remove_succeeded=remove_succeeded,
            mirror_succeeded=mirror_succeeded,
        )
    )


async def run_periodic(
    *,
    stop_event: asyncio.Event,
    settings: Any,
    ws_consumer: Any,
    recipe_yaml_path: Path,
    evidence_store: L3EvidenceStore,
    evidence_runtime: L3EvidenceRuntime,
) -> None:
    """Run one promoter tick per boot-anchored schedule sequence.

    The grid is derived from immutable boot truth, never from the prior run's
    finish time.  A late/slow run therefore makes the next contiguous sequence
    immediately eligible instead of silently skipping it or accumulating drift.
    """
    interval_s = settings.l3_promote_interval_s
    if isinstance(interval_s, bool) or not isinstance(interval_s, (int, float)):
        raise TypeError("l3_promote_interval_s must be numeric")
    if interval_s <= 0:
        raise ValueError("l3_promote_interval_s must be positive")
    boot_started_at = evidence_runtime.snapshot().started_at
    logger.info("l3-promote: run_periodic started interval_s={}", interval_s)
    while not stop_event.is_set():
        run_seq = evidence_runtime.next_run_seq()
        scheduled_at = boot_started_at + timedelta(seconds=run_seq * interval_s)
        delay_s = max(0.0, (scheduled_at - _utc_now()).total_seconds())
        if delay_s > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay_s)
                break
            except TimeoutError:
                pass
        if stop_event.is_set():
            break
        try:
            await promote_run(
                settings=settings,
                ws_consumer=ws_consumer,
                recipe_yaml_path=recipe_yaml_path,
                evidence_store=evidence_store,
                evidence_runtime=evidence_runtime,
                scheduled_at=scheduled_at,
                run_seq=run_seq,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — terminalizer is defense-in-depth
            logger.error(
                "l3-promote tick raised run_seq={} error_type={}",
                run_seq,
                type(exc).__name__,
            )
    logger.info("l3-promote: run_periodic stopped")
