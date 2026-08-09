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

import time as _time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import sentry_sdk
from loguru import logger
from supabase import Client, ClientOptions, create_client

if TYPE_CHECKING:
    # Forward-reference only — avoid runtime circular import in the
    # storage layer (sqlite_store and l2_supabase_mirror both live in
    # polyarb.storage and are sibling modules).
    from polyarb.storage.sqlite_store import SQLiteStore

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

# Phase 05 D-04 + D-07: l2_book_levels writes — top-10 per side per book event.
_NARROW_BOOK_LEVELS_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "ts",
    "side",
    "level",
    "price",
    "size",
)

# Chunk size — postgrest body-size headroom; matches Phase 02 supabase_mirror.
_CHUNK_SIZE: int = 1000
_POSTGREST_TIMEOUT_S: float = 5.0


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

    Constructed once per L2 daemon. All public methods are synchronous and
    block on supabase-py REST calls, so the production dispatcher runs them
    through ``asyncio.to_thread`` and awaits them sequentially. The five-second
    PostgREST timeout bounds one off-loop write without allowing concurrent
    access to this client.
    """

    def __init__(
        self,
        url: str,
        service_key: str,
        store: SQLiteStore | None = None,
    ) -> None:
        """Create supabase client. Exactly ONE client per instance.

        Args:
            url: Supabase REST URL (https://<ref>.supabase.co) — POLYARB_SUPABASE_URL
            service_key: Supabase service_role JWT — POLYARB_SUPABASE_SERVICE_KEY
            store: Optional SQLiteStore for the freshness-cache write-through
                (Phase 03.1 Plan 01 GAP-3). When provided, push_top_of_book and
                push_trades success branches call
                `store.upsert_l2_tob_mirror_state(int(time.time()))` so /health
                has a freshness anchor. When None (legacy callers), success
                paths skip the cache write — backwards-compatible.

        DO NOT pass the Postgres DSN — that's reserved for alembic + asyncpg.
        """
        self._client: Client = create_client(
            url,
            service_key,
            ClientOptions(postgrest_client_timeout=_POSTGREST_TIMEOUT_S),
        )
        # Phase 03.1 Plan 01: optional freshness-cache writer. Daemon wires this
        # in Plan 02; legacy / direct callers may pass None.
        self._store: SQLiteStore | None = store

    # ── _refresh_freshness_cache (Phase 03.1 Plan 01) ────────────────────────

    def _refresh_freshness_cache(self) -> None:
        """Best-effort write of the wall-clock freshness anchor to local SQLite.

        Called from push_top_of_book / push_trades success branches ONLY.
        Failure of the cache write must NEVER break the mirror's actual write
        path — wrapped in try/except, emits a breadcrumb on failure.
        """
        if self._store is None:
            return
        try:
            self._store.upsert_l2_tob_mirror_state(int(_time.time()))
        except Exception as e:  # noqa: BLE001 — freshness cache is non-critical
            logger.warning(f"l2-mirror: freshness-cache write failed: {e!r}")
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message="freshness-cache write failed",
                data={"error": str(e)[:200]},
            )

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
            # Phase 03.1 Plan 01 (GAP-3): refresh local freshness anchor so
            # /health l2_tob_age_seconds has a sub-second readable value.
            self._refresh_freshness_cache()
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12
            logger.error(f"l2-mirror push_top_of_book failed rows={len(rows)}: {str(e)[:200]}")
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message=f"push_top_of_book failed rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_top_of_book", "error": str(e)[:200]},
            )
            return False

    # ── push_book_levels (Phase 05 D-04 + D-07) ──────────────────────────────

    def push_book_levels(self, rows: list[dict]) -> bool:
        """Bulk insert ``l2_book_levels`` rows. Fail-soft per D-12 envelope.

        Phase 05 D-04 / D-07. Mirrors :meth:`push_top_of_book` envelope verbatim:
        narrow projection + 1000-row chunks + dual-anchor Sentry breadcrumb
        + loguru. On SUCCESS, mutates ``l3_promote._last_book_levels_write_at_s``
        — the chain-truth anchor read by /health ``l3:last_book_levels_write_at_s``
        (Plan 05-04). On FAILURE the anchor is intentionally left untouched
        (chain-truth: only the real success path advances freshness).

        Args:
            rows: List of dicts with keys ``asset_id, ts, side, level, price,
                size`` (see ``_NARROW_BOOK_LEVELS_COLUMNS``). Extra keys are
                silently dropped by ``_project``.

        Returns:
            True on success; False on any exception (never raises).
        """
        try:
            narrow = _project(rows, _NARROW_BOOK_LEVELS_COLUMNS)
            for chunk in _chunk(narrow, _CHUNK_SIZE):
                self._client.table("l2_book_levels").upsert(
                    chunk,
                    on_conflict="asset_id,ts,side,level",
                ).execute()
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="info",
                message=f"push_book_levels ok rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_book_levels"},
            )
            logger.info(f"l2-mirror: pushed {len(rows)} book_levels rows")
            # Chain-truth anchor — /health reads via
            # l3_promote.get_last_book_levels_write_at_s. Local import avoids
            # a module-init cycle (l3_promote may itself import storage in a
            # future plan; deferring to call-time keeps this resilient).
            from polyarb.observation import l3_promote

            l3_promote._last_book_levels_write_at_s = _time.time()
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12
            logger.error(f"l2-mirror push_book_levels failed rows={len(rows)}: {str(e)[:200]}")
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message=f"push_book_levels failed rows={len(rows)}",
                data={
                    "rows": len(rows),
                    "table": "l2_book_levels",
                    "error": str(e)[:200],
                },
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
                self._client.table("l2_trades").upsert(chunk, on_conflict="trade_hash").execute()
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="info",
                message=f"push_trades ok rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_trades"},
            )
            logger.info(f"l2-mirror: upserted {len(rows)} trade rows")
            # Phase 03.1 Plan 01 (GAP-3): refresh local freshness anchor (any
            # successful write to the L2 mirror surface counts).
            self._refresh_freshness_cache()
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12
            logger.error(f"l2-mirror push_trades failed rows={len(rows)}: {str(e)[:200]}")
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
            logger.error(f"l2-mirror upsert_candidates failed rows={len(rows)}: {str(e)[:200]}")
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message=f"upsert_candidates failed rows={len(rows)}",
                data={"rows": len(rows), "table": "l2_candidates", "error": str(e)[:200]},
            )
            return False

    def fetch_active_candidates(self) -> list[dict] | None:
        """Return active candidate identities, or ``None`` on REST failure."""
        try:
            response = (
                self._client.table("l2_candidates")
                .select("asset_id,recipe_name")
                .is_("removed_at_ts", None)
                .execute()
            )
            return list(response.data or [])
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12
            logger.error(f"l2-mirror fetch_active_candidates failed: {str(e)[:200]}")
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message="fetch_active_candidates failed",
                data={"table": "l2_candidates", "error": str(e)[:200]},
            )
            return None

    def reconcile_candidates(self, desired_rows: list[dict]) -> bool:
        """Converge active history to desired ``(asset_id, recipe_name)`` keys.

        Unchanged active rows remain untouched. Stale keys are closed with both
        identity filters, while missing keys are inserted as a new history
        cycle. Any REST failure returns ``False`` so the durable event cursor
        remains retryable.
        """
        active_rows = self.fetch_active_candidates()
        if active_rows is None:
            return False

        desired_by_key: dict[tuple[str, str], dict] = {}
        for row in desired_rows:
            key = (str(row.get("asset_id", "")), str(row.get("recipe_name", "")))
            if not all(key):
                logger.error("l2-mirror reconcile_candidates invalid candidate identity")
                return False
            desired_by_key[key] = row
        active_keys = {
            (str(row.get("asset_id", "")), str(row.get("recipe_name", ""))) for row in active_rows
        }

        try:
            now_iso = datetime.now(UTC).isoformat()
            stale_keys = sorted(active_keys - desired_by_key.keys())
            for asset_id, recipe_name in stale_keys:
                (
                    self._client.table("l2_candidates")
                    .update({"removed_at_ts": now_iso})
                    .eq("asset_id", asset_id)
                    .eq("recipe_name", recipe_name)
                    .is_("removed_at_ts", None)
                    .execute()
                )

            missing_keys = sorted(desired_by_key.keys() - active_keys)
            missing_rows = [desired_by_key[key] for key in missing_keys]
            if missing_rows and not self.upsert_candidates(missing_rows):
                return False

            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="info",
                message="reconcile_candidates ok",
                data={
                    "active": len(desired_by_key),
                    "closed": len(stale_keys),
                    "inserted": len(missing_rows),
                    "table": "l2_candidates",
                },
            )
            logger.info(
                "l2-mirror: candidate projection converged "
                f"active={len(desired_by_key)} closed={len(stale_keys)} "
                f"inserted={len(missing_rows)}"
            )
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per D-12
            logger.error(f"l2-mirror reconcile_candidates failed: {str(e)[:200]}")
            sentry_sdk.add_breadcrumb(
                category="l2-mirror",
                level="warning",
                message="reconcile_candidates failed",
                data={"table": "l2_candidates", "error": str(e)[:200]},
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
            now_iso = datetime.now(UTC).isoformat()
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
