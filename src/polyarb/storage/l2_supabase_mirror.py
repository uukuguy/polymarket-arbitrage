"""L2 Supabase mirror — fail-soft write client for the L2 dashboard surface.

Phase 03 Plan 06 — D-07 / D-08 / D-12 amendment.

Three public methods, all fail-soft (D-12):
- push_top_of_book(rows) — bulk insert into l2_top_of_book in 1000-row chunks
- push_trades(rows)      — upsert into l2_trades with ON CONFLICT (trade_hash)
                           DO NOTHING (D-08 idempotent backfill)
- upsert_candidates(rows)— upsert into l2_candidates (recipe ∪ watchlist union
                           from candidate_refresh)
- mark_candidates_removed(asset_ids) — companion: bulk UPDATE removed_at_ts
                           when an asset drops out of the active candidate set

Dual-anchor breadcrumb (Phase 02.2 preemptive — RESEARCH Open Q 9):
- Every fail-soft path emits a Sentry breadcrumb on BOTH success and failure.
- Success: category='l2-mirror', level='info' — protects against
  "design-unreachable" breadcrumb-buffer evaporation under steady-state load.
- Failure: category='l2-mirror', level='warning' — co-located with the
  loguru error so on-call has matching anchors in Sentry + Axiom.
- category='l2-mirror' is INTENTIONALLY distinct from L1's 'mirror' so the
  Sentry dashboard can filter the two streams independently (Phase 02.1 P2).

Architecture (REUSE Phase 02 supabase_mirror envelope verbatim):
- Long-lived supabase client (one per L2SupabaseMirror instance)
- Constructor takes REST URL (https://...supabase.co) and the service_role
  JWT. Service role bypasses RLS — no policy work needed for writes.
- DO NOT pass the Postgres DSN here — that's used only by alembic / asyncpg
  listener. See W6 fix in Phase 02 Plan 03 / config.py docstring.
- Chunked writes at _CHUNK_SIZE=1000 rows (postgrest body-size headroom).
- Narrow column projection: drop incidental fields before write so a
  caller can pass enriched dicts without polluting the schema.

Threat model carry-over (T-03-06-04/-05):
- _project drops keys not in the narrow column tuple → schema drift safety.
- push_trades uses on_conflict='trade_hash' so duplicate inserts during
  backfill replay are silently dropped (no log spam, no UniqueViolation).
"""
from __future__ import annotations

from typing import Iterator

import sentry_sdk
from loguru import logger
from supabase import Client, create_client

# ── Narrow column projections ────────────────────────────────────────────────

_NARROW_TOB_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "ts",
    "best_bid",
    "best_ask",
    "spread",
    "mid_price",
    "depth_yes_usd",
    "depth_no_usd",
    "source_event",
)

_NARROW_TRADE_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "market_id",
    "ts",
    "price",
    "size",
    "side",
    "taker_address",
    "trade_hash",
    "source",
)

_NARROW_CANDIDATE_COLUMNS: tuple[str, ...] = (
    "snapshot_id",
    "recipe_name",
    "asset_id",
    "market_id",
    "event_id",
    "included_at_ts",
    "removed_at_ts",
    "ranking_score",
    "source",
)

# Chunk size — postgrest body-size headroom; matches Phase 02 supabase_mirror.
_CHUNK_SIZE: int = 1000


def _chunk(items: list, size: int) -> Iterator[list]:
    """Yield successive fixed-size chunks from items (verbatim from Phase 02)."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _project(rows: list[dict], columns: tuple[str, ...]) -> list[dict]:
    """Project row dicts to the specified column tuple, preserving order.

    Keys not in `columns` are dropped (schema-drift safety). Missing keys
    map to None — supabase-py inserts NULL.
    """
    return [{c: r.get(c) for c in columns} for r in rows]


class L2SupabaseMirror:
    """Fail-soft write client for the L2 dashboard mirror tables.

    Constructed once per L2 daemon. Thread-safety: not thread-safe by design;
    the L2 daemon runs single-event-loop. All methods are synchronous and
    BLOCK on the supabase-py REST call (acceptable because Plan 04 batches
    WS frames at ≤5s debounce — well outside the asyncio loop's tight inner
    iteration budget).
    """

    def __init__(self, url: str, service_key: str) -> None:
        """Create supabase client. Exactly ONE client per instance.

        Args:
            url: Supabase REST URL (https://<ref>.supabase.co) — POLYARB_SUPABASE_URL
            service_key: Supabase service_role JWT — POLYARB_SUPABASE_SERVICE_KEY

        DO NOT pass the Postgres DSN — that's reserved for alembic + asyncpg.
        """
        self._client: Client = create_client(url, service_key)

    # ── push_top_of_book ─────────────────────────────────────────────────────

    def push_top_of_book(self, rows: list[dict]) -> bool:
        """Bulk insert top-of-book rows. Fail-soft — never raises.

        Returns True on success, False on any exception. Each chunk runs in
        its own .insert() call (postgrest body limit headroom).
        """
        try:
            narrow = _project(rows, _NARROW_TOB_COLUMNS)
            for chunk in _chunk(narrow, _CHUNK_SIZE):
                self._client.table("l2_top_of_book").insert(chunk).execute()
            # Phase 02.2 preemptive — success-path breadcrumb so on-call has a
            # positive anchor even when nothing fails (dual-anchor pattern).
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="info",
                message=f"push_top_of_book ok rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_top_of_book"},
            )
            logger.info(f"l2-mirror: pushed {len(rows)} top_of_book rows")
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12
            logger.error(
                f"l2-mirror push_top_of_book failed rows={len(rows)}: {str(e)[:200]}"
            )
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message=f"push_top_of_book failed rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_top_of_book", "error": str(e)[:200]},
            )
            return False

    # ── push_trades ──────────────────────────────────────────────────────────

    def push_trades(self, rows: list[dict]) -> bool:
        """Upsert trade rows with ON CONFLICT (trade_hash) DO NOTHING.

        Idempotent backfill: replaying the Data API backfill is safe; duplicate
        trade_hash values are silently dropped by Postgres.

        Fail-soft — never raises.
        """
        try:
            narrow = _project(rows, _NARROW_TRADE_COLUMNS)
            for chunk in _chunk(narrow, _CHUNK_SIZE):
                self._client.table("l2_trades").upsert(
                    chunk, on_conflict="trade_hash"
                ).execute()
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="info",
                message=f"push_trades ok rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_trades"},
            )
            logger.info(f"l2-mirror: upserted {len(rows)} trade rows")
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12
            logger.error(
                f"l2-mirror push_trades failed rows={len(rows)}: {str(e)[:200]}"
            )
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message=f"push_trades failed rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_trades", "error": str(e)[:200]},
            )
            return False

    # ── upsert_candidates ────────────────────────────────────────────────────

    def upsert_candidates(self, rows: list[dict]) -> bool:
        """Upsert candidate rows into l2_candidates (recipe ∪ watchlist union).

        Composite uniqueness is enforced by application logic, not a DB
        constraint (the schema deliberately allows multiple included/removed
        cycles for the same asset_id under the same recipe_name — diff-aware
        history). We therefore use plain insert here; mark_candidates_removed
        handles the removed_at_ts mark separately on diff-out.

        Fail-soft — never raises.
        """
        try:
            narrow = _project(rows, _NARROW_CANDIDATE_COLUMNS)
            for chunk in _chunk(narrow, _CHUNK_SIZE):
                self._client.table("l2_candidates").insert(chunk).execute()
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="info",
                message=f"upsert_candidates ok rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_candidates"},
            )
            logger.info(f"l2-mirror: inserted {len(rows)} candidate rows")
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12
            logger.error(
                f"l2-mirror upsert_candidates failed rows={len(rows)}: {str(e)[:200]}"
            )
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message=f"upsert_candidates failed rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_candidates", "error": str(e)[:200]},
            )
            return False

    # ── mark_candidates_removed ──────────────────────────────────────────────

    def mark_candidates_removed(self, asset_ids: list[str]) -> bool:
        """Set removed_at_ts=now() on currently-active rows for these asset_ids.

        Companion to upsert_candidates: when an asset drops out of the active
        candidate set (diff_candidate_sets returned it in the `removed` set),
        the daemon calls this to close out its history row.

        Filters on removed_at_ts IS NULL so re-running with the same asset_ids
        doesn't keep updating already-closed rows.

        Fail-soft — never raises.
        """
        if not asset_ids:
            return True
        try:
            from datetime import datetime, timezone

            now_iso = datetime.now(timezone.utc).isoformat()
            (
                self._client.table("l2_candidates")
                .update({"removed_at_ts": now_iso})
                .in_("asset_id", asset_ids)
                .is_("removed_at_ts", None)
                .execute()
            )
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="info",
                message=f"mark_candidates_removed ok ids={len(asset_ids)}",
                data={"ids": len(asset_ids), "table": "l2_candidates"},
            )
            logger.info(f"l2-mirror: marked {len(asset_ids)} candidates removed")
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12
            logger.error(
                f"l2-mirror mark_candidates_removed failed ids={len(asset_ids)}: {str(e)[:200]}"
            )
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message=f"mark_candidates_removed failed ids={len(asset_ids)}",
                data={"ids": len(asset_ids), "table": "l2_candidates", "error": str(e)[:200]},
            )
            return False
