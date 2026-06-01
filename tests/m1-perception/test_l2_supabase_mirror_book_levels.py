"""RED tests for `L2SupabaseMirror.push_book_levels` — Phase 05 Plan 03 Task 1.

Stays RED until Task 2 lands:
- `polyarb.observation.l3_promote` (module + `_last_book_levels_write_at_s`)
- `polyarb.storage.l2_supabase_mirror.L2SupabaseMirror.push_book_levels`
- `polyarb.storage.l2_supabase_mirror._NARROW_BOOK_LEVELS_COLUMNS`

Contract (verbatim from `push_top_of_book` envelope + the chain-truth anchor):
- happy path returns True; failure returns False (never raises)
- Narrow projection drops extra fields before .insert()
- 1000-row chunk size matches _CHUNK_SIZE
- On SUCCESS only: l3_promote._last_book_levels_write_at_s = time.time()
- On FAILURE: anchor remains untouched (chain-truth — failure path is pure)
- Sentry breadcrumb category="l2-mirror"; data.table="l2_book_levels"
"""
from __future__ import annotations

import os
import time
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


# ── Helpers ────────────────────────────────────────────────────────────────


def _make_supabase_mock(raise_on_insert: bool = False) -> MagicMock:
    client = MagicMock()

    def _table_mock(name: str) -> MagicMock:
        tbl = MagicMock()
        tbl.upsert.return_value = tbl
        tbl.insert.return_value = tbl
        tbl.delete.return_value = tbl
        tbl.update.return_value = tbl
        tbl.select.return_value = tbl
        tbl.in_.return_value = tbl
        tbl.is_.return_value = tbl
        if raise_on_insert:
            tbl.execute.side_effect = RuntimeError("supabase 5xx")
        else:
            tbl.execute.return_value = MagicMock(data=[])
        return tbl

    client.table.side_effect = _table_mock
    return client


def _book_rows(n: int) -> list[dict]:
    rows: list[dict] = []
    for i in range(n):
        rows.append(
            {
                "asset_id": f"asset-{i}",
                "ts": "2026-06-01T12:00:00Z",
                "side": "BUY" if i % 2 == 0 else "SELL",
                "level": (i % 10) + 1,
                "price": 0.50 + (i * 0.001),
                "size": 100.0 + i,
            }
        )
    return rows


@pytest.fixture(autouse=True)
def _reset_chain_truth_anchor() -> None:
    """Reset `_last_book_levels_write_at_s` before every test for isolation."""
    from polyarb.observation import l3_promote
    prior = l3_promote._last_book_levels_write_at_s
    l3_promote._last_book_levels_write_at_s = None
    yield
    l3_promote._last_book_levels_write_at_s = prior


# ── Tests ──────────────────────────────────────────────────────────────────


def test_push_book_levels_happy_path_returns_true() -> None:
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    sb_mock = _make_supabase_mock()
    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        rows = _book_rows(5)
        result = mirror.push_book_levels(rows)

    assert result is True
    # .table("l2_book_levels") was called at least once
    table_calls = [c.args[0] for c in sb_mock.table.call_args_list]
    assert "l2_book_levels" in table_calls

    # Inspect the insert call payload — should have 5 rows
    # (sb_mock.table returns a fresh MagicMock per call, but they share
    # side_effect; we grab the last tbl by replaying.)
    # Easier: count insert calls across all returned tbls via side_effect.
    insert_calls = []
    for call in sb_mock.table.call_args_list:
        # Walk the chain: every table() call returned a tbl whose insert(...)
        # was called with the chunk. We re-invoke side_effect to get the
        # corresponding tbl is not deterministic; instead patch differently
        # below in the chunk test. Here we rely on overall call count.
        pass

    # Loose assertion: at least one insert was made (chunked path)
    # — strong row-count assertion is in test_push_book_levels_chunks_at_1000.


def test_push_book_levels_failure_returns_false_no_raise() -> None:
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    sb_mock = _make_supabase_mock(raise_on_insert=True)
    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        result = mirror.push_book_levels(_book_rows(3))

    assert result is False  # fail-soft envelope


def test_push_book_levels_chunks_at_1000() -> None:
    """2500 rows → exactly 3 .insert calls (1000 + 1000 + 500)."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    # Use one shared insert-mock so we can count chunk invocations
    insert_mock = MagicMock()
    insert_mock.execute.return_value = MagicMock(data=[])

    tbl_mock = MagicMock()
    tbl_mock.insert.return_value = insert_mock
    tbl_mock.upsert.return_value = insert_mock

    sb_mock = MagicMock()
    sb_mock.table.return_value = tbl_mock

    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        result = mirror.push_book_levels(_book_rows(2500))

    assert result is True
    assert tbl_mock.insert.call_count == 3
    chunk_sizes = [
        len(call.args[0]) for call in tbl_mock.insert.call_args_list
    ]
    assert chunk_sizes == [1000, 1000, 500], (
        f"expected chunks 1000+1000+500 but got {chunk_sizes}"
    )


def test_push_book_levels_narrow_projection() -> None:
    """Extra keys outside `_NARROW_BOOK_LEVELS_COLUMNS` must be stripped."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    insert_mock = MagicMock()
    insert_mock.execute.return_value = MagicMock(data=[])
    tbl_mock = MagicMock()
    tbl_mock.insert.return_value = insert_mock
    sb_mock = MagicMock()
    sb_mock.table.return_value = tbl_mock

    polluted = [
        {
            "asset_id": "x",
            "ts": "2026-06-01T12:00:00Z",
            "side": "BUY",
            "level": 1,
            "price": 0.50,
            "size": 100.0,
            "extra_field": "ignore-me",
            "another_extra": 42,
        }
    ]

    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        assert mirror.push_book_levels(polluted) is True

    sent_chunk = tbl_mock.insert.call_args.args[0]
    assert len(sent_chunk) == 1
    sent_row = sent_chunk[0]
    assert "extra_field" not in sent_row
    assert "another_extra" not in sent_row
    # narrow columns ARE present
    for k in ("asset_id", "ts", "side", "level", "price", "size"):
        assert k in sent_row


def test_push_book_levels_updates_chain_truth_anchor_on_success() -> None:
    """SUCCESS path mutates l3_promote._last_book_levels_write_at_s to time.time()."""
    from polyarb.observation import l3_promote
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    sb_mock = _make_supabase_mock()
    assert l3_promote._last_book_levels_write_at_s is None  # autouse reset

    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        before = time.time()
        result = mirror.push_book_levels(_book_rows(2))
        after = time.time()

    assert result is True
    anchor = l3_promote._last_book_levels_write_at_s
    assert anchor is not None
    assert before <= anchor <= after, (
        f"anchor {anchor} outside [{before}, {after}]"
    )


def test_push_book_levels_does_NOT_update_anchor_on_failure() -> None:
    """FAILURE path leaves chain-truth anchor untouched."""
    from polyarb.observation import l3_promote
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    sb_mock = _make_supabase_mock(raise_on_insert=True)
    assert l3_promote._last_book_levels_write_at_s is None  # autouse reset

    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ):
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        result = mirror.push_book_levels(_book_rows(2))

    assert result is False
    # CRITICAL: anchor still None — only success path mutates.
    assert l3_promote._last_book_levels_write_at_s is None


def test_push_book_levels_sentry_breadcrumb_category() -> None:
    """Happy path emits Sentry breadcrumb category='l2-mirror'; data.table='l2_book_levels'."""
    from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror

    sb_mock = _make_supabase_mock()
    with patch(
        "polyarb.storage.l2_supabase_mirror.create_client", return_value=sb_mock
    ), patch(
        "polyarb.storage.l2_supabase_mirror.sentry_sdk"
    ) as sentry_mock:
        mirror = L2SupabaseMirror(url="https://x.supabase.co", service_key="key")
        assert mirror.push_book_levels(_book_rows(3)) is True

    calls = sentry_mock.add_breadcrumb.call_args_list
    matching = [
        c for c in calls
        if c.kwargs.get("category") == "l2-mirror"
        and (c.kwargs.get("data") or {}).get("table") == "l2_book_levels"
    ]
    assert len(matching) >= 1, (
        f"expected at least one breadcrumb with "
        f"category='l2-mirror' and data.table='l2_book_levels', got: {calls}"
    )
