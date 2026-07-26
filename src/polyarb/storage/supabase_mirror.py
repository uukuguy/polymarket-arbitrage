"""Supabase mirror — post-write fail-soft adapter.

Phase 02 Plan 03 — D-02 / D-19 / D-12 amendment.

Architecture: SQLite remains source of truth (RESEARCH §3 方案 B). After a
successful local write, the orchestrator calls SupabaseMirror.push_snapshot as a
fire-and-forget adapter. Mirror failure is fail-soft: returns False + logs error.
Never raises out of push_snapshot (orchestrator will add an Issue and continue).

Design decisions:
- Long-lived client (singleton pattern per GammaClient precedent, Phase 01.1)
- Idempotent: upsert on snapshots PK; markets_latest is DELETE+INSERT full-overwrite
- Chunked insert: ≤1000 rows per request (postgrest body limit, RESEARCH §3)
- Narrow columns: only 10 fields from markets table (dashboard-relevant only)
- Reconcile: compares last SQLite snapshot_id vs last Supabase snapshot_id,
  re-pushes any missing IDs
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from loguru import logger
from supabase import Client, create_client

if TYPE_CHECKING:
    from polyarb.storage.sqlite_store import SQLiteStore

# Narrow market row columns mirrored to Supabase (dashboard-relevant only).
# Does NOT include all ~23 SQLite markets columns — Supabase is read-only dashboard.
#
# Phase 04 D-07 widened from 10 to 11 columns for yes_token_id. Phase 05.3
# widens the projection to 12 columns so L3 can resolve the complete binary
# outcome pair without querying a nonexistent market-level asset_id.
# yes_token_id is needed by the
# L2 candidate-refresh watchlist path (Plan 02) — temp DB does
# `SELECT yes_token_id FROM markets WHERE slug=?` and treats the value as the
# Polymarket WS subscription asset_id. The narrow_market_row() default branch
# (`out[col] = full_row.get(col)`) handles nullable passthrough correctly
# without a special-case branch — yes_token_id is None when source row lacks it
# (normalizer.py:107: `else None` for empty clobTokenIds list).
_NARROW_MARKET_COLUMNS = (
    "market_id",
    "question",
    "slug",
    "event_slug",
    "mid_price",
    "liquidity_usd",
    "volume_usd",
    "end_time_ms",
    "snapshot_id",
    "question_zh",
    "yes_token_id",  # D-07: nullable; source = normalizer.py:107 clobTokenIds[0]
    "no_token_id",  # Phase 05.3: nullable; source = normalizer clobTokenIds[1]
)


def narrow_market_row(full_row: dict, snapshot_id: int) -> dict:
    """Project a Phase 01.1 normalized market dict to the narrow Supabase row shape.

    Selects only _NARROW_MARKET_COLUMNS. Fields not in the full_row default to None.
    event_slug is mapped from the SQLite event_id column (we store event_id as FK;
    Supabase dashboard wants event_slug for display).
    """
    out: dict = {}
    for col in _NARROW_MARKET_COLUMNS:
        if col == "snapshot_id":
            out[col] = snapshot_id
        elif col == "event_slug":
            # SQLite stores event_id (TEXT FK); mirror uses event_slug name for display
            out[col] = full_row.get("event_slug") or full_row.get("event_id")
        else:
            out[col] = full_row.get(col)
    return out


def _chunk(items: list, size: int) -> Iterator[list]:
    """Yield successive fixed-size chunks from items."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


class SupabaseMirror:
    """Long-lived Supabase client for post-write mirror operations.

    Instantiate once per daemon lifetime (or once per orchestrator run if not
    in daemon mode). Uses the Supabase service_role key for write access;
    the dashboard uses anon_key + RLS for read access (principle of least privilege).

    Thread safety: not thread-safe by design — the snapshot pipeline is
    single-threaded (one run at a time per RESEARCH §3 failure handling).
    """

    def __init__(self, url: str, service_key: str) -> None:
        """Create Supabase client. Exactly ONE client per SupabaseMirror instance.

        Args:
            url: Supabase REST URL (https://<ref>.supabase.co) — env POLYARB_SUPABASE_URL
            service_key: Supabase service_role key — env POLYARB_SUPABASE_SERVICE_KEY
        """
        self._client: Client = create_client(url, service_key)

    def push_snapshot(
        self,
        snapshot_id: int,
        snapshot_meta: dict,
        market_rows: list[dict],
    ) -> bool:
        """Mirror one snapshot to Supabase. Fail-soft — never raises.

        Steps:
          1. Upsert snapshot_meta into snapshots table (idempotent on PK id)
          2. DELETE all current markets_latest rows
          3. INSERT market_rows in ≤1000-row chunks

        Returns:
            True if all operations succeeded, False if any raised an exception.

        On failure: logs error with snapshot_id context, returns False.
        The orchestrator step 7.5 adds an Issue(DEGRADED) and continues normally.
        """
        if not market_rows:
            # DELETE+INSERT is intentionally non-transactional over PostgREST.
            # Treat an empty projection as invalid input before *any* remote
            # mutation; otherwise this method degenerates into DELETE-only and
            # erases the last known-good market universe.
            logger.error(
                f"Supabase mirror rejected empty market projection snapshot_id={snapshot_id}"
            )
            return False
        try:
            self._client.table("snapshots").upsert(snapshot_meta).execute()
            self._client.table("markets_latest").delete().neq("market_id", "").execute()
            for chunk in _chunk(market_rows, 1000):
                self._client.table("markets_latest").insert(chunk).execute()
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per RESEARCH §3
            logger.error(f"Supabase mirror failed snapshot_id={snapshot_id}: {str(e)[:200]}")
            return False

    def update_parquet_url(self, snapshot_id: int, parquet_url: str) -> bool:
        """Update parquet_url field on an existing snapshot row. Fail-soft.

        Called by orchestrator step 7.6 after a successful R2 upload.

        F-02 fix (Plan 02-08): pure UPDATE — never upsert. The pre-fix code
        used upsert(id, parquet_url) which, when the snapshot row didn't
        exist remotely (e.g. step 7.5 push_snapshot had earlier failed),
        would INSERT a new row containing only id+parquet_url. That insert
        violates NOT NULL on taken_at_ms / finished_at_ms / mode / status /
        market_count, blowing up the post-write tail with a misleading
        constraint error. The correct behaviour for "row missing" is to log
        and return False — the next snapshot's push_snapshot will create the
        row properly.

        Returns:
            True if exactly one row was updated.
            False if no row matched (snapshot_id missing remotely) OR an
                  exception was raised (fail-soft per Plan 03 contract).
        """
        try:
            resp = (
                self._client.table("snapshots")
                .update({"parquet_url": parquet_url})
                .eq("id", snapshot_id)
                .execute()
            )
            # supabase-py returns .data as the list of updated rows. Empty
            # list means no row matched the .eq filter — i.e. the snapshot
            # row hasn't been mirrored yet. Do NOT fall back to insert.
            if not getattr(resp, "data", None):
                logger.warning(
                    f"update_parquet_url: snapshot_id={snapshot_id} not found in mirror; "
                    f"skipping (parquet_url={parquet_url!r}). Next push_snapshot will "
                    f"include this URL via the regular mirror path."
                )
                return False
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per RESEARCH §3
            logger.warning(f"update_parquet_url snapshot_id={snapshot_id} failed: {str(e)[:200]}")
            return False

    def get_latest_remote_snapshot_id(self) -> int | None:
        """Return the most-recent snapshot_id from Supabase, or None if table is empty.

        Used by reconcile to identify the gap between local SQLite and remote Supabase.
        """
        try:
            result = (
                self._client.table("snapshots")
                .select("id")
                .order("id", desc=True)
                .limit(1)
                .execute()
            )
            data = result.data
            if not data:
                return None
            return data[0].get("id")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"get_latest_remote_snapshot_id failed: {str(e)[:200]}")
            return None

    def reconcile(self, sqlite_store: SQLiteStore) -> list[int]:
        """Find local snapshot_ids missing on Supabase and push them.

        Compares latest SQLite snapshot_id vs latest Supabase snapshot_id.
        Pushes any IDs in the gap (remote_id+1 .. local_id inclusive).

        Returns list of snapshot_ids that were pushed (empty if no gap).

        This is a best-effort reconcile — individual push failures are logged
        but do not halt the loop. The caller sees the full list of IDs attempted
        (not just successful ones).
        """
        latest_local = sqlite_store.get_latest_snapshot()
        if latest_local is None:
            logger.info("reconcile: no local snapshots, nothing to push")
            return []

        local_id = latest_local["id"]
        remote_id = self.get_latest_remote_snapshot_id()

        if remote_id is None:
            # Supabase has no snapshots — push all from 1 to local_id
            remote_id = 0

        if local_id <= remote_id:
            logger.info(f"reconcile: up-to-date (local={local_id}, remote={remote_id})")
            return []

        gap_ids = list(range(remote_id + 1, local_id + 1))
        logger.info(f"reconcile: gap found — pushing snapshot_ids {gap_ids[0]}..{gap_ids[-1]}")

        pushed: list[int] = []
        for sid in gap_ids:
            snapshot = sqlite_store.get_snapshot(sid)
            if snapshot is None:
                logger.warning(f"reconcile: snapshot_id={sid} not found in SQLite, skipping")
                continue

            market_rows = sqlite_store.get_markets_for_snapshot(sid)
            narrow_rows = [narrow_market_row(m, sid) for m in market_rows]

            # Build snapshot_meta from SQLite row
            snapshot_meta = {
                "id": snapshot["id"],
                "taken_at_ms": snapshot["taken_at_ms"],
                "finished_at_ms": snapshot["finished_at_ms"],
                "mode": snapshot["mode"],
                "status": "ok" if snapshot["is_valid"] else "fail",
                "market_count": snapshot["market_count"],
                "parquet_url": snapshot.get("parquet_r2_url"),
            }

            ok = self.push_snapshot(sid, snapshot_meta, narrow_rows)
            if ok:
                pushed.append(sid)
            else:
                logger.warning(f"reconcile: failed to push snapshot_id={sid}")

        logger.info(f"reconcile: pushed {len(pushed)}/{len(gap_ids)} snapshots")
        return gap_ids  # return all IDs in gap (not just successful ones)
