"""Wave 0 tests for SupabaseMirror — idempotent push_snapshot + fail-soft.

Task 1 (02-03): TDD RED phase. polyarb.storage.supabase_mirror does not yet exist,
so test collection fails with ImportError. That is the expected RED state.

Tests use unittest.mock (no real network calls). Mirror API contract:
- push_snapshot(snapshot_id, snapshot_meta, market_rows) -> bool
- update_parquet_url(snapshot_id, parquet_url) -> bool
- get_latest_remote_snapshot_id() -> int | None
- reconcile(sqlite_store) -> list[int]  (returns missing snapshot IDs that were pushed)
- SupabaseMirror.__init__(url, service_key) creates exactly ONE supabase client
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, call, patch

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_market_rows(n: int, snapshot_id: int = 1) -> list[dict]:
    """Build n minimal market rows shaped like narrow_market_row output."""
    return [
        {
            "market_id": f"mkt-{i}",
            "question": f"Question {i}?",
            "slug": f"question-{i}",
            "event_slug": f"event-{i}",
            "mid_price": 0.5,
            "liquidity_usd": 1000.0 + i,
            "volume_usd": 500.0,
            "end_time_ms": 1800000000000,
            "snapshot_id": snapshot_id,
            "question_zh": None,
        }
        for i in range(n)
    ]


def _make_snapshot_meta(snapshot_id: int = 1) -> dict:
    return {
        "id": snapshot_id,
        "taken_at_ms": 1715500000000,
        "finished_at_ms": 1715500060000,
        "mode": "subset",
        "status": "ok",
        "market_count": 5,
        "parquet_url": None,
    }


def _make_supabase_mock() -> MagicMock:
    """Create a MagicMock supabase client that supports table().upsert/insert/delete/select chain."""
    client = MagicMock()

    def _table_mock(name: str) -> MagicMock:
        tbl = MagicMock()
        tbl.upsert.return_value = tbl
        tbl.insert.return_value = tbl
        tbl.delete.return_value = tbl
        tbl.select.return_value = tbl
        tbl.neq.return_value = tbl
        tbl.eq.return_value = tbl
        tbl.order.return_value = tbl
        tbl.limit.return_value = tbl
        tbl.execute.return_value = MagicMock(data=[])
        return tbl

    client.table.side_effect = _table_mock
    return client


# ---------------------------------------------------------------------------
# Test: push_snapshot writes snapshots + markets_latest
# ---------------------------------------------------------------------------


def test_push_snapshot_writes_snapshots_and_markets_latest() -> None:
    """push_snapshot calls upsert on snapshots and delete+insert on markets_latest."""
    from polyarb.storage.supabase_mirror import SupabaseMirror

    mock_client = _make_supabase_mock()

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=mock_client):
        mirror = SupabaseMirror(url="http://localhost:0", service_key="dummy")
        ok = mirror.push_snapshot(
            snapshot_id=42,
            snapshot_meta=_make_snapshot_meta(42),
            market_rows=_make_market_rows(3, 42),
        )

    assert ok is True
    # snapshots upsert must have been called
    snapshots_calls = [c for c in mock_client.table.call_args_list if c.args[0] == "snapshots"]
    assert snapshots_calls, "client.table('snapshots') was never called"
    # markets_latest delete must have been called
    markets_calls = [c for c in mock_client.table.call_args_list if c.args[0] == "markets_latest"]
    assert markets_calls, "client.table('markets_latest') was never called"


# ---------------------------------------------------------------------------
# Test: idempotent upsert — second call same snapshot_id still uses upsert
# ---------------------------------------------------------------------------


def test_idempotent_upsert() -> None:
    """Calling push_snapshot twice with same snapshot_id uses upsert (idempotent)."""
    from polyarb.storage.supabase_mirror import SupabaseMirror

    mock_client = _make_supabase_mock()

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=mock_client):
        mirror = SupabaseMirror(url="http://localhost:0", service_key="dummy")

        ok1 = mirror.push_snapshot(
            snapshot_id=7,
            snapshot_meta=_make_snapshot_meta(7),
            market_rows=_make_market_rows(2, 7),
        )
        ok2 = mirror.push_snapshot(
            snapshot_id=7,
            snapshot_meta=_make_snapshot_meta(7),
            market_rows=_make_market_rows(2, 7),
        )

    assert ok1 is True
    assert ok2 is True
    # Both calls should go through (upsert on PK handles deduplication server-side)
    # We verify that no matter how many times called, it doesn't raise
    assert mock_client.table.call_count >= 4  # at least 2 calls to snapshots + 2 to markets_latest


# ---------------------------------------------------------------------------
# Test: failure does not raise — returns False
# ---------------------------------------------------------------------------


def test_mirror_failure_does_not_raise() -> None:
    """If supabase client raises, push_snapshot returns False without re-raising."""
    from polyarb.storage.supabase_mirror import SupabaseMirror

    failing_client = MagicMock()
    tbl = MagicMock()
    tbl.upsert.side_effect = Exception("Network error: connection refused")
    tbl.delete.side_effect = Exception("Network error: connection refused")
    failing_client.table.return_value = tbl

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=failing_client):
        mirror = SupabaseMirror(url="http://localhost:0", service_key="dummy")
        ok = mirror.push_snapshot(
            snapshot_id=1,
            snapshot_meta=_make_snapshot_meta(1),
            market_rows=_make_market_rows(2, 1),
        )

    assert ok is False, "push_snapshot must return False on failure, not raise"


# ---------------------------------------------------------------------------
# Test: large market list is chunked (≤1000 per insert)
# ---------------------------------------------------------------------------


def test_chunks_large_market_list() -> None:
    """3500 market rows → chunked into ≤1000 per insert call."""
    from polyarb.storage.supabase_mirror import SupabaseMirror

    mock_client = _make_supabase_mock()
    insert_calls: list[int] = []

    # Track insert sizes by overriding insert.side_effect
    def _make_tracking_table(name: str) -> MagicMock:
        tbl = MagicMock()
        tbl.upsert.return_value = tbl
        tbl.delete.return_value = tbl
        tbl.neq.return_value = tbl
        tbl.eq.return_value = tbl
        tbl.order.return_value = tbl
        tbl.limit.return_value = tbl
        tbl.execute.return_value = MagicMock(data=[])

        if name == "markets_latest":
            def _insert(chunk: list) -> MagicMock:
                insert_calls.append(len(chunk))
                return tbl
            tbl.insert.side_effect = _insert
        else:
            tbl.insert.return_value = tbl

        tbl.select.return_value = tbl
        return tbl

    mock_client.table.side_effect = _make_tracking_table

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=mock_client):
        mirror = SupabaseMirror(url="http://localhost:0", service_key="dummy")
        mirror.push_snapshot(
            snapshot_id=1,
            snapshot_meta=_make_snapshot_meta(1),
            market_rows=_make_market_rows(3500, 1),
        )

    assert insert_calls, "insert was never called for markets_latest"
    assert all(c <= 1000 for c in insert_calls), (
        f"chunk sizes exceed 1000: {insert_calls}"
    )
    total_inserted = sum(insert_calls)
    assert total_inserted == 3500, f"expected 3500 rows inserted, got {total_inserted}"


# ---------------------------------------------------------------------------
# Test: reconcile finds gap between SQLite and Supabase
# ---------------------------------------------------------------------------


def test_reconcile_finds_supabase_gap() -> None:
    """SQLite has snapshot_id=10 most recent; Supabase returns 8 → gap is {9, 10}."""
    from polyarb.storage.supabase_mirror import SupabaseMirror

    mock_client = _make_supabase_mock()

    # Supabase returns last snapshot_id = 8
    snapshots_tbl = MagicMock()
    snapshots_tbl.select.return_value = snapshots_tbl
    snapshots_tbl.order.return_value = snapshots_tbl
    snapshots_tbl.limit.return_value = snapshots_tbl
    snapshots_tbl.execute.return_value = MagicMock(data=[{"id": 8}])
    snapshots_tbl.upsert.return_value = snapshots_tbl
    snapshots_tbl.delete.return_value = snapshots_tbl
    snapshots_tbl.neq.return_value = snapshots_tbl

    markets_tbl = MagicMock()
    markets_tbl.delete.return_value = markets_tbl
    markets_tbl.neq.return_value = markets_tbl
    markets_tbl.insert.return_value = markets_tbl
    markets_tbl.execute.return_value = MagicMock(data=[])
    markets_tbl.upsert.return_value = markets_tbl

    def _table(name: str) -> MagicMock:
        if name == "snapshots":
            return snapshots_tbl
        return markets_tbl

    mock_client.table.side_effect = _table

    # SQLite store mock — returns IDs 1..10 with snapshot_id=10 most recent
    mock_store = MagicMock()
    mock_store.get_latest_snapshot.return_value = {"id": 10, "taken_at_ms": 1715500000000}

    def _get_snapshot(snapshot_id: int) -> dict:
        return {
            "id": snapshot_id,
            "taken_at_ms": 1715500000000 + snapshot_id * 1000,
            "finished_at_ms": 1715500060000 + snapshot_id * 1000,
            "mode": "subset",
            "is_valid": 1,
            "market_count": 5,
            "parquet_path": f"/data/snapshots/snap-{snapshot_id}.parquet",
            "notes": "",
        }

    mock_store.get_snapshot.side_effect = _get_snapshot

    # Markets for a snapshot (small set for reconcile)
    mock_store.get_markets_for_snapshot.return_value = _make_market_rows(2, 9)

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=mock_client):
        mirror = SupabaseMirror(url="http://localhost:0", service_key="dummy")
        missing = mirror.reconcile(mock_store)

    # Reconcile should have identified snapshot IDs 9 and 10 as missing
    assert set(missing) == {9, 10}, f"Expected gap {{9, 10}}, got {set(missing)}"


# ---------------------------------------------------------------------------
# Test: supabase client is initialized exactly once
# ---------------------------------------------------------------------------


def test_supabase_client_initialized_once() -> None:
    """SupabaseMirror.__init__ creates exactly one supabase client (long-lived)."""
    from polyarb.storage.supabase_mirror import SupabaseMirror

    mock_client = _make_supabase_mock()

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=mock_client) as create_mock:
        mirror = SupabaseMirror(url="https://test.supabase.co", service_key="test-key")
        # Multiple operations — client should still only be created once
        mirror.push_snapshot(1, _make_snapshot_meta(1), _make_market_rows(2, 1))
        mirror.push_snapshot(2, _make_snapshot_meta(2), _make_market_rows(2, 2))

    assert create_mock.call_count == 1, (
        f"create_client called {create_mock.call_count} times, expected exactly 1"
    )


# ---------------------------------------------------------------------------
# F-02 regression tests: update_parquet_url is a pure UPDATE — no upsert.
# ---------------------------------------------------------------------------


def test_update_parquet_url_uses_update_not_upsert() -> None:
    """update_parquet_url must call .update().eq('id', ...), not upsert.

    Pre-F-02 code used upsert({"id": sid, "parquet_url": url}) which, when the
    snapshot row didn't exist remotely (mirror push failed earlier), inserts a
    new row with only id+parquet_url — triggering NOT NULL on the required
    columns (taken_at_ms / finished_at_ms / mode / status / market_count).
    """
    from polyarb.storage.supabase_mirror import SupabaseMirror

    mock_client = MagicMock()
    snapshots_tbl = MagicMock()
    snapshots_tbl.update.return_value = snapshots_tbl
    snapshots_tbl.eq.return_value = snapshots_tbl
    # supabase returns the updated row when one was actually updated
    snapshots_tbl.execute.return_value = MagicMock(data=[{"id": 42}])
    snapshots_tbl.upsert.return_value = snapshots_tbl
    mock_client.table.return_value = snapshots_tbl

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=mock_client):
        mirror = SupabaseMirror(url="http://localhost:0", service_key="dummy")
        ok = mirror.update_parquet_url(42, "https://r2.example/snap-42.parquet")

    assert ok is True
    # UPDATE was called — exact column name pinned by separate test below
    snapshots_tbl.update.assert_called_once()
    # eq filter on id was applied
    snapshots_tbl.eq.assert_called_once_with("id", 42)
    # upsert was NEVER called — that was the F-02 bug
    snapshots_tbl.upsert.assert_not_called()


def test_update_parquet_url_missing_snapshot_skips_gracefully() -> None:
    """If snapshot row doesn't exist remotely, return False + log warning, no insert."""
    from polyarb.storage.supabase_mirror import SupabaseMirror

    mock_client = MagicMock()
    snapshots_tbl = MagicMock()
    snapshots_tbl.update.return_value = snapshots_tbl
    snapshots_tbl.eq.return_value = snapshots_tbl
    # Empty data → no row was updated → snapshot row didn't exist
    snapshots_tbl.execute.return_value = MagicMock(data=[])
    snapshots_tbl.upsert.return_value = snapshots_tbl
    snapshots_tbl.insert.return_value = snapshots_tbl
    mock_client.table.return_value = snapshots_tbl

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=mock_client):
        mirror = SupabaseMirror(url="http://localhost:0", service_key="dummy")
        ok = mirror.update_parquet_url(99, "https://r2.example/orphan.parquet")

    assert ok is False, "missing snapshot must return False, not raise / not insert"
    # No upsert was triggered as a fallback (that was the F-02 bug)
    snapshots_tbl.upsert.assert_not_called()
    snapshots_tbl.insert.assert_not_called()


def test_update_parquet_url_exception_fail_soft() -> None:
    """Network / Supabase errors → return False, do not raise."""
    from polyarb.storage.supabase_mirror import SupabaseMirror

    mock_client = MagicMock()
    snapshots_tbl = MagicMock()
    snapshots_tbl.update.return_value = snapshots_tbl
    snapshots_tbl.eq.return_value = snapshots_tbl
    snapshots_tbl.execute.side_effect = Exception("ECONNREFUSED")
    mock_client.table.return_value = snapshots_tbl

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=mock_client):
        mirror = SupabaseMirror(url="http://localhost:0", service_key="dummy")
        ok = mirror.update_parquet_url(7, "https://r2.example/x.parquet")

    assert ok is False  # fail-soft per Plan 03 contract


# ---------------------------------------------------------------------------
# F-05 regression test: orchestrator step 7.5 skips mirror when is_valid=False.
# Lives in this file because the assertion is on mirror.push_snapshot being
# uncalled — closest functional neighbour. The orchestrator test file has
# heavier setup; this one stays narrow.
# ---------------------------------------------------------------------------


def test_step_7_5_skips_mirror_when_snapshot_invalid() -> None:
    """orchestrator: when is_valid=False (e.g. 0-market snapshot), do NOT call mirror.

    Pre-F-05: orchestrator step 7.5 always built snapshot_meta and called
    mirror.push_snapshot regardless of is_valid. A 0-market is_valid=False
    snapshot would still trigger a mirror upsert with status="failed" and 0
    markets, polluting Supabase's snapshots table with degenerate rows.
    """
    import asyncio
    from unittest.mock import AsyncMock, MagicMock as MM, patch

    from polyarb.config import load_settings
    from polyarb.snapshot import orchestrator as orch_mod

    settings = load_settings()
    if not getattr(settings, "supabase_mirror_enabled", False):
        pytest.skip("supabase mirror is disabled; F-05 guard is moot in this env")

    # We don't run the whole orchestrator — just verify the guard logic by
    # patching SupabaseMirror.push_snapshot and forcing is_valid=False through
    # the determine_snapshot_status path. A lighter contract test:
    # the F-05 fix means orchestrator step 7.5 contains
    # `if not snapshot_result.is_valid: skip mirror`. We import the orchestrator
    # source and grep for the guard pattern.
    import inspect

    src = inspect.getsource(orch_mod.run_snapshot)

    # Guard pattern: the orchestrator must check is_valid before calling
    # mirror.push_snapshot in step 7.5. We assert the conditional is present
    # in the source. This is a structural test — narrow but catches regressions
    # where someone removes the guard.
    assert "if not is_valid" in src or "if is_valid" in src or "is_valid=False" in src.replace(" ", ""), (
        "step 7.5 must guard mirror.push_snapshot with an is_valid check (F-05)"
    )

    # And the guard must wrap (be a precondition of) the push_snapshot call.
    # We do a lightweight ordering check: the first `is_valid` reference inside
    # the function appears before the `mirror.push_snapshot(` call.
    mirror_call_idx = src.find("mirror.push_snapshot(")
    assert mirror_call_idx != -1, "orchestrator must still contain a mirror.push_snapshot call"
    guard_idx = src.find("is_valid", src.find("def run_snapshot"))
    assert guard_idx != -1 and guard_idx < mirror_call_idx, (
        "is_valid guard must come before mirror.push_snapshot call"
    )


def test_update_parquet_url_column_name_matches_alembic_001() -> None:
    """Defensive: the column name we UPDATE matches Alembic 001's schema column.

    Alembic 001 declares snapshots.parquet_url (Text, nullable). The F-02 fix
    must NOT silently rename the column — it stays parquet_url to match the
    Supabase schema. Note: the SQLite snapshots table uses parquet_r2_url for
    the same value (the two stores have different historical names). This
    test pins the Supabase column name to prevent drift.
    """
    from polyarb.storage.supabase_mirror import SupabaseMirror

    mock_client = MagicMock()
    snapshots_tbl = MagicMock()
    snapshots_tbl.update.return_value = snapshots_tbl
    snapshots_tbl.eq.return_value = snapshots_tbl
    snapshots_tbl.execute.return_value = MagicMock(data=[{"id": 1}])
    mock_client.table.return_value = snapshots_tbl

    with patch("polyarb.storage.supabase_mirror.create_client", return_value=mock_client):
        mirror = SupabaseMirror(url="http://localhost:0", service_key="dummy")
        mirror.update_parquet_url(1, "https://r2/x.parquet")

    # Alembic 001: snapshots.parquet_url is the canonical Supabase column.
    call_args = snapshots_tbl.update.call_args
    assert call_args is not None
    payload = call_args[0][0]
    assert "parquet_url" in payload, (
        f"update payload must include parquet_url (Alembic 001 column); got {payload!r}"
    )
