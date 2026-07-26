"""Unit tests for polyarb.storage.sqlite_store.SQLiteStore.

Verifies:
- WAL pragma + 3-table schema creation (idempotent)
- BEGIN IMMEDIATE + DELETE FROM markets overwrite semantics (anti-pattern #1 NOT used)
- snapshots table is append-only across multiple write_snapshot calls
- validation_issues records category + layer correctly
- is_valid=False still persists (D-D3)
- write_snapshot returns the new snapshots.id (FK matches markets.snapshot_id)
- ValueError on invalid mode
- uint256-style 70-char token IDs round-trip as exact strings (Pitfall 3)
- ROLLBACK on executemany failure leaves the markets table empty
"""

from __future__ import annotations

# Belt-and-suspenders for F-3 path validator (this test does not import Settings,
# but if conftest is added later in Plan 01-5 we want this to keep working).
import os

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

import sqlite3
import time
from dataclasses import replace
from pathlib import Path

import pytest

from polyarb.perception.market_truth import (
    EventMember,
    GroupTruth,
    SourceCoverage,
    membership_hash,
)
from polyarb.storage.sqlite_store import SQLiteStore
from polyarb.validator.category import Category, Issue


def make_market(market_id: str, **overrides) -> dict:
    """Build a fully-populated market dict suitable for write_snapshot."""
    base = dict(
        market_id=market_id,
        condition_id=f"c-{market_id}",
        slug=None,
        question=None,
        yes_token_id="1" * 70,
        no_token_id="2" * 70,
        mid_price=0.5,
        liquidity_usd=1000.0,
        volume_usd=100.0,
        best_bid_price=0.49,
        best_bid_size=100.0,
        best_ask_price=0.51,
        best_ask_size=100.0,
        end_time_ms=2_000_000_000_000,
        active=1,
        closed=0,
        neg_risk=0,
        neg_risk_market_id=None,
        fetched_at_ms=1_714_435_200_000,
        snapshot_id=0,  # placeholder; write_snapshot overrides via _row_to_tuple
        incomplete=0,
    )
    base.update(overrides)
    return base


def _complete_publication() -> dict:
    return {
        "source_coverage": SourceCoverage.complete(0, 0),
        "event_members": [],
        "group_truths": [],
        "publish_markets": True,
    }


def _valid_truth() -> tuple[list[EventMember], GroupTruth]:
    members = [
        EventMember("e1", "g1", "m1", "named", True, False),
        EventMember("e1", "g1", "m2", "named", True, False),
    ]
    return members, GroupTruth(
        event_id="e1",
        group_id="g1",
        neg_risk_type="standard",
        expected_member_count=2,
        active_named_count=2,
        membership_hash=membership_hash("e1", "g1", members),
        quality="complete-supported",
        reason=None,
    )


def _truth_market(member: EventMember, **overrides) -> dict:
    row = make_market(
        member.market_id,
        event_id=member.event_id,
        neg_risk_market_id=member.group_id,
        neg_risk=True,
        active=member.active,
        closed=member.closed,
    )
    row.update(overrides)
    return row


def _write_with_mode(
    store: SQLiteStore,
    *,
    streaming: bool,
    market_rows: list[dict],
    **kwargs,
) -> int:
    common = {
        "taken_at_ms": 10,
        "finished_at_ms": 20,
        "mode": "subset",
        "parquet_path": "candidate.parquet",
        "market_rows": iter(market_rows) if streaming else market_rows,
        **kwargs,
    }
    if streaming:
        snapshot_id, _ = store.write_snapshot_streaming(**common)
        return snapshot_id
    return store.write_snapshot(**common)


@pytest.fixture
def store(tmp_path: Path) -> SQLiteStore:
    s = SQLiteStore(tmp_path / "t.db")
    s.init_schema()
    return s


# ---------- 1. init_schema ----------------------------------------------------


def test_init_schema_creates_three_tables(store: SQLiteStore) -> None:
    con = sqlite3.connect(store.db_path)
    try:
        tables = {
            r[0]
            for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert {"snapshots", "markets", "validation_issues"} <= tables
        # WAL pragma is persistent at the DB level; should report 'wal'.
        mode = con.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"
    finally:
        con.close()


@pytest.mark.parametrize("streaming", [False, True])
def test_snapshot_records_explicit_market_view_publication_marker(
    store: SQLiteStore,
    streaming: bool,
) -> None:
    common = {
        "is_valid": True,
        "issues": [],
        "source_coverage": SourceCoverage.complete(0, 0),
        "event_members": [],
        "group_truths": [],
        "publish_markets": True,
    }
    published_id = _write_with_mode(
        store,
        streaming=streaming,
        market_rows=[],
        **common,
    )
    diagnostic_id = _write_with_mode(
        store,
        streaming=streaming,
        market_rows=[],
        **{**common, "is_valid": False, "publish_markets": False},
    )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT id,market_view_published FROM snapshots "
            "WHERE id IN (?,?) ORDER BY id",
            (published_id, diagnostic_id),
        ).fetchall() == [(published_id, 1), (diagnostic_id, 0)]


def test_purge_old_snapshots_bounds_each_transaction(store: SQLiteStore) -> None:
    old_ms = int((time.time() - 8 * 86_400) * 1000)
    for offset in range(35):
        store.write_snapshot(
            taken_at_ms=old_ms + offset,
            finished_at_ms=old_ms + offset,
            mode="subset",
            parquet_path="",
            is_valid=True,
            market_rows=[],
            issues=[],
            **_complete_publication(),
        )

    deleted, deleted_ids = store.purge_old_snapshots(
        older_than_days=7,
        keep_last=5,
        max_snapshots_per_run=10,
    )

    assert deleted == 10
    assert len(deleted_ids) == 10
    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 25


def test_purge_old_snapshots_removes_market_truth_rows(store: SQLiteStore) -> None:
    old_ms = int((time.time() - 8 * 86_400) * 1000)
    snapshot_ids: list[int] = []
    for offset in range(4):
        member = EventMember(
            f"e{offset}",
            f"g{offset}",
            f"m{offset}",
            "named",
            True,
            False,
        )
        truth = GroupTruth(
            event_id=member.event_id,
            group_id=member.group_id,
            neg_risk_type="standard",
            expected_member_count=1,
            active_named_count=1,
            membership_hash=membership_hash(
                member.event_id,
                member.group_id,
                [member],
            ),
            quality="complete-supported",
            reason=None,
        )
        snapshot_ids.append(
            store.write_snapshot(
                taken_at_ms=old_ms + offset,
                finished_at_ms=old_ms + offset,
                mode="subset",
                parquet_path="",
                is_valid=True,
                market_rows=[_truth_market(member)],
                issues=[],
                source_coverage=SourceCoverage.complete(1, 1),
                event_members=[member],
                group_truths=[truth],
                publish_markets=True,
            )
        )

    deleted, deleted_ids = store.purge_old_snapshots(
        older_than_days=7,
        keep_last=1,
        max_snapshots_per_run=2,
    )

    assert deleted == 2
    assert deleted_ids == snapshot_ids[:2]
    with sqlite3.connect(store.db_path) as con:
        placeholders = ",".join("?" for _ in deleted_ids)
        for table in (
            "snapshot_source_coverage",
            "event_market_memberships",
            "neg_risk_group_truth",
        ):
            assert con.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE snapshot_id IN ({placeholders})",
                deleted_ids,
            ).fetchone() == (0,)


def test_streaming_disk_full_preserves_original_error_when_sqlite_auto_rolls_back(
    store: SQLiteStore, monkeypatch
) -> None:
    """A failed ROLLBACK must never mask the actionable disk-full cause."""
    real_connect = sqlite3.connect

    class _DiskFullProxy:
        def __init__(self, real_con):
            self._real = real_con

        def execute(self, sql, *args, **kwargs):
            normalized = sql.strip().upper() if isinstance(sql, str) else ""
            if normalized == "DELETE FROM MARKETS":
                self._real.execute("ROLLBACK")
                raise sqlite3.OperationalError("database or disk is full")
            return self._real.execute(sql, *args, **kwargs)

        def close(self):
            return self._real.close()

        def __getattr__(self, name):
            return getattr(self._real, name)

    wrapped = False

    def _connect_proxy(*args, **kwargs):
        nonlocal wrapped
        connection = real_connect(*args, **kwargs)
        if kwargs.get("isolation_level", "deferred") is None and not wrapped:
            wrapped = True
            return _DiskFullProxy(connection)
        return connection

    monkeypatch.setattr("polyarb.storage.sqlite_store.sqlite3.connect", _connect_proxy)

    with pytest.raises(sqlite3.OperationalError, match="database or disk is full"):
        store.write_snapshot_streaming(
            taken_at_ms=90,
            finished_at_ms=100,
            mode="subset",
            parquet_path="disk-full.parquet",
            is_valid=True,
            market_rows=[],
            issues=[],
            **_complete_publication(),
        )


def test_init_schema_idempotent(tmp_path: Path) -> None:
    s = SQLiteStore(tmp_path / "t.db")
    s.init_schema()
    s.init_schema()  # second call must not raise
    con = sqlite3.connect(s.db_path)
    try:
        tables = {
            r[0]
            for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert {"snapshots", "markets", "validation_issues"} <= tables
    finally:
        con.close()


# ---------- 2. overwrite semantics (anti-pattern #1) -------------------------


def test_write_snapshot_overwrites_markets(store: SQLiteStore) -> None:
    """The second snapshot must REPLACE markets — never accumulate rows from snapshot 1."""
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("a"), make_market("b")],
        issues=[],
        **_complete_publication(),
    )
    store.write_snapshot(
        taken_at_ms=2_000_000,
        finished_at_ms=2_000_100,
        mode="subset",
        parquet_path="x/2.parquet",
        is_valid=True,
        market_rows=[make_market("c")],
        issues=[],
        **_complete_publication(),
    )

    con = sqlite3.connect(store.db_path)
    try:
        rows = con.execute("SELECT market_id FROM markets").fetchall()
    finally:
        con.close()
    assert rows == [("c",)], (
        "Expected only the latest snapshot's rows. INSERT OR REPLACE alone "
        "would leak 'a' and 'b' from the first snapshot."
    )


def test_incomplete_source_does_not_replace_last_complete_markets(store: SQLiteStore) -> None:
    first = store.write_snapshot(
        taken_at_ms=1,
        finished_at_ms=2,
        mode="subset",
        parquet_path="complete.parquet",
        is_valid=True,
        market_rows=[make_market("complete-market")],
        issues=[],
        source_coverage=SourceCoverage.complete(10, 3),
        event_members=[],
        group_truths=[],
        publish_markets=True,
    )
    second = store.write_snapshot(
        taken_at_ms=3,
        finished_at_ms=4,
        mode="subset",
        parquet_path="partial.parquet",
        is_valid=False,
        market_rows=[make_market("partial-market")],
        issues=[],
        source_coverage=SourceCoverage.incomplete("markets", 2, 100, "http-422"),
        event_members=[],
        group_truths=[],
        publish_markets=False,
    )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT market_id, snapshot_id FROM markets").fetchall() == [
            ("complete-market", first)
        ]
        assert con.execute(
            "SELECT completed, failure_source, failure_reason "
            "FROM snapshot_source_coverage WHERE snapshot_id=?",
            (second,),
        ).fetchone() == (0, "markets", "http-422")


def test_group_truth_and_membership_are_same_snapshot_transaction(
    store: SQLiteStore,
) -> None:
    members = [
        EventMember("e1", "g1", "m1", "named", True, False),
        EventMember("e1", "g1", "m2", "named", True, False),
    ]
    truth = GroupTruth(
        event_id="e1",
        group_id="g1",
        neg_risk_type="standard",
        expected_member_count=2,
        active_named_count=2,
        membership_hash=membership_hash("e1", "g1", members),
        quality="complete-supported",
        reason=None,
    )

    snapshot_id = store.write_snapshot(
        taken_at_ms=1,
        finished_at_ms=2,
        mode="subset",
        parquet_path="truth.parquet",
        is_valid=True,
        market_rows=[_truth_market(member) for member in members],
        issues=[],
        source_coverage=SourceCoverage.complete(2, 1),
        event_members=members,
        group_truths=[truth],
        publish_markets=True,
    )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT market_id FROM event_market_memberships "
            "WHERE snapshot_id=? ORDER BY market_id",
            (snapshot_id,),
        ).fetchall() == [("m1",), ("m2",)]
        assert con.execute(
            "SELECT membership_hash FROM neg_risk_group_truth WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone() == (truth.membership_hash,)


@pytest.mark.parametrize(
    "quality",
    ["complete-supported", "complete-unsupported", "incomplete-quotes"],
)
def test_zero_member_truth_is_rejected_unless_source_is_incomplete(
    store: SQLiteStore,
    quality: str,
) -> None:
    truth = GroupTruth(
        event_id="e1",
        group_id="g1",
        neg_risk_type="standard",
        expected_member_count=0,
        active_named_count=0,
        membership_hash=membership_hash("e1", "g1", []),
        quality=quality,  # type: ignore[arg-type]
        reason="diagnostic",
    )

    with pytest.raises(ValueError, match="expected_member_count"):
        store.write_snapshot(
            taken_at_ms=1,
            finished_at_ms=2,
            mode="subset",
            parquet_path="invalid-truth.parquet",
            is_valid=False,
            market_rows=[],
            issues=[],
            source_coverage=SourceCoverage.complete(0, 1),
            event_members=[],
            group_truths=[truth],
            publish_markets=False,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM snapshots").fetchone() == (0,)


def test_zero_member_incomplete_source_truth_is_persisted(store: SQLiteStore) -> None:
    truth = GroupTruth(
        event_id="e1",
        group_id="g1",
        neg_risk_type="standard",
        expected_member_count=0,
        active_named_count=0,
        membership_hash=membership_hash("e1", "g1", []),
        quality="incomplete-source",
        reason="event-membership-missing-or-empty",
    )

    snapshot_id = store.write_snapshot(
        taken_at_ms=1,
        finished_at_ms=2,
        mode="subset",
        parquet_path="incomplete-truth.parquet",
        is_valid=False,
        market_rows=[],
        issues=[],
        source_coverage=SourceCoverage.incomplete("events", 0, 1, "missing-members"),
        event_members=[],
        group_truths=[truth],
        publish_markets=False,
    )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT expected_member_count, quality FROM neg_risk_group_truth "
            "WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone() == (0, "incomplete-source")


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize(
    "case",
    ["source-incomplete", "invalid", "api-unreachable", "incomplete-truth"],
)
def test_publish_boundary_rejects_contradictory_truth_before_replacement(
    store: SQLiteStore,
    streaming: bool,
    case: str,
) -> None:
    baseline_id = store.write_snapshot(
        taken_at_ms=1,
        finished_at_ms=2,
        mode="subset",
        parquet_path="baseline.parquet",
        is_valid=True,
        market_rows=[make_market("baseline")],
        issues=[],
        **_complete_publication(),
    )
    members, truth = _valid_truth()
    source_coverage = SourceCoverage.complete(2, 1)
    is_valid = True
    issues: list[Issue] = []
    if case == "source-incomplete":
        source_coverage = SourceCoverage.incomplete("events", 2, 1, "mismatch")
    elif case == "invalid":
        is_valid = False
    elif case == "api-unreachable":
        issues = [
            Issue(
                layer=1,
                category=Category.API_UNREACHABLE,
                market_id=None,
                detail="source unavailable",
            )
        ]
    else:
        truth = replace(
            truth,
            quality="incomplete-source",
            reason="membership-mismatch",
        )

    with pytest.raises(ValueError, match="market-truth-publication-rejected"):
        _write_with_mode(
            store,
            streaming=streaming,
            market_rows=[_truth_market(member) for member in members],
            is_valid=is_valid,
            issues=issues,
            source_coverage=source_coverage,
            event_members=members,
            group_truths=[truth],
            publish_markets=True,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT market_id, snapshot_id FROM markets").fetchall() == [
            ("baseline", baseline_id)
        ]
        assert con.execute("SELECT COUNT(*) FROM snapshots").fetchone() == (1,)


@pytest.mark.parametrize(
    "case",
    [
        "member-event",
        "member-group",
        "expected-count",
        "active-count",
        "membership-hash",
        "duplicate-member",
        "duplicate-truth",
        "member-without-truth",
    ],
)
@pytest.mark.parametrize("streaming", [False, True])
def test_truth_consistency_rejects_invalid_projection(
    store: SQLiteStore,
    case: str,
    streaming: bool,
) -> None:
    members, truth = _valid_truth()
    truths = [truth]
    if case == "member-event":
        members[0] = replace(members[0], event_id="other-event")
    elif case == "member-group":
        members[0] = replace(members[0], group_id="other-group")
    elif case == "expected-count":
        truth = replace(truth, expected_member_count=3)
        truths = [truth]
    elif case == "active-count":
        truth = replace(truth, active_named_count=1)
        truths = [truth]
    elif case == "membership-hash":
        truth = replace(truth, membership_hash="not-the-canonical-hash")
        truths = [truth]
    elif case == "duplicate-member":
        members.append(members[0])
    elif case == "duplicate-truth":
        truths.append(truth)
    else:
        truths = []

    with pytest.raises(ValueError, match="market-truth-invalid"):
        _write_with_mode(
            store,
            streaming=streaming,
            market_rows=[],
            is_valid=False,
            issues=[],
            source_coverage=SourceCoverage.incomplete("events", 0, 1, "diagnostic"),
            event_members=members,
            group_truths=truths,
            publish_markets=False,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM snapshots").fetchone() == (0,)


@pytest.mark.parametrize("streaming", [False, True])
def test_publish_rejects_missing_authoritative_member_and_restores_last_view(
    store: SQLiteStore,
    streaming: bool,
) -> None:
    baseline_id = store.write_snapshot(
        taken_at_ms=1,
        finished_at_ms=2,
        mode="subset",
        parquet_path="baseline.parquet",
        is_valid=True,
        market_rows=[make_market("baseline")],
        issues=[],
        **_complete_publication(),
    )
    members, truth = _valid_truth()

    with pytest.raises(ValueError, match="published-members-missing"):
        _write_with_mode(
            store,
            streaming=streaming,
            market_rows=[_truth_market(members[0])],
            is_valid=True,
            issues=[],
            source_coverage=SourceCoverage.complete(1, 1),
            event_members=members,
            group_truths=[truth],
            publish_markets=True,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT market_id, snapshot_id FROM markets").fetchall() == [
            ("baseline", baseline_id)
        ]
        assert con.execute("SELECT COUNT(*) FROM snapshots").fetchone() == (1,)


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize(
    ("case", "overrides"),
    [
        ("event-id", {"event_id": "other-event"}),
        ("group-id", {"neg_risk_market_id": "other-group"}),
        ("neg-risk-false", {"neg_risk": False}),
        ("active", {"active": False}),
        ("active-invalid", {"active": "1"}),
        ("closed", {"closed": True}),
    ],
)
def test_publish_rejects_member_market_semantic_mismatch_and_restores_last_view(
    store: SQLiteStore,
    streaming: bool,
    case: str,
    overrides: dict,
) -> None:
    baseline_id = store.write_snapshot(
        taken_at_ms=1,
        finished_at_ms=2,
        mode="subset",
        parquet_path="baseline.parquet",
        is_valid=True,
        market_rows=[make_market("baseline")],
        issues=[],
        **_complete_publication(),
    )
    members, truth = _valid_truth()
    candidate_rows = [
        _truth_market(members[0], **overrides),
        _truth_market(members[1]),
    ]

    with pytest.raises(
        ValueError,
        match=f"published-market-truth-mismatch:{case}",
    ):
        _write_with_mode(
            store,
            streaming=streaming,
            market_rows=candidate_rows,
            is_valid=True,
            issues=[],
            source_coverage=SourceCoverage.complete(2, 1),
            event_members=members,
            group_truths=[truth],
            publish_markets=True,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT market_id, snapshot_id FROM markets").fetchall() == [
            ("baseline", baseline_id)
        ]
        assert con.execute("SELECT COUNT(*) FROM snapshots").fetchone() == (1,)


@pytest.mark.parametrize("streaming", [False, True])
def test_publish_rejects_numeric_market_id_before_sqlite_can_stringify_it(
    store: SQLiteStore,
    streaming: bool,
) -> None:
    member = EventMember("e1", "g1", "7", "named", True, False)
    truth = GroupTruth(
        event_id="e1",
        group_id="g1",
        neg_risk_type="standard",
        expected_member_count=1,
        active_named_count=1,
        membership_hash=membership_hash("e1", "g1", [member]),
        quality="complete-supported",
        reason=None,
    )
    malformed_id_row = _truth_market(member)
    malformed_id_row["market_id"] = 7

    with pytest.raises(
        ValueError,
        match="published-market-truth-mismatch:market-id",
    ):
        _write_with_mode(
            store,
            streaming=streaming,
            market_rows=[malformed_id_row],
            is_valid=True,
            issues=[],
            source_coverage=SourceCoverage.complete(1, 1),
            event_members=[member],
            group_truths=[truth],
            publish_markets=True,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM snapshots").fetchone() == (0,)


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("neg_risk", [False, True])
def test_publish_rejects_market_side_neg_risk_group_without_truth(
    store: SQLiteStore,
    streaming: bool,
    neg_risk: bool,
) -> None:
    orphan = make_market(
        "orphan",
        event_id="event-orphan",
        neg_risk_market_id="group-orphan",
        neg_risk=neg_risk,
        active=True,
        closed=False,
    )

    with pytest.raises(ValueError, match="published-neg-risk-without-truth"):
        _write_with_mode(
            store,
            streaming=streaming,
            market_rows=[orphan],
            is_valid=True,
            issues=[],
            source_coverage=SourceCoverage.complete(1, 1),
            event_members=[],
            group_truths=[],
            publish_markets=True,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM snapshots").fetchone() == (0,)


@pytest.mark.parametrize("streaming", [False, True])
def test_publish_rejects_true_neg_risk_without_group_id(
    store: SQLiteStore,
    streaming: bool,
) -> None:
    orphan = make_market(
        "orphan",
        event_id="event-orphan",
        neg_risk_market_id=None,
        neg_risk=True,
        active=True,
        closed=False,
    )

    with pytest.raises(ValueError, match="published-neg-risk-without-truth"):
        _write_with_mode(
            store,
            streaming=streaming,
            market_rows=[orphan],
            is_valid=True,
            issues=[],
            source_coverage=SourceCoverage.complete(1, 1),
            event_members=[],
            group_truths=[],
            publish_markets=True,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM snapshots").fetchone() == (0,)


def test_write_snapshot_appends_to_snapshots_table(store: SQLiteStore) -> None:
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("a")],
        issues=[],
        **_complete_publication(),
    )
    store.write_snapshot(
        taken_at_ms=2_000_000,
        finished_at_ms=2_000_100,
        mode="full",
        parquet_path="x/2.parquet",
        is_valid=True,
        market_rows=[make_market("b")],
        issues=[],
        **_complete_publication(),
    )
    con = sqlite3.connect(store.db_path)
    try:
        n = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    finally:
        con.close()
    assert n == 2


# ---------- 3. validation_issues ----------------------------------------------


def test_write_snapshot_records_issues_with_category(store: SQLiteStore) -> None:
    issues = [
        Issue(layer=2, category=Category.ZOMBIE_MARKET, market_id="m1", detail="low liq"),
        Issue(layer=4, category=Category.GHOST_BOOK, market_id="m2", detail="fake book"),
    ]
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("m1"), make_market("m2")],
        issues=issues,
        **_complete_publication(),
    )
    con = sqlite3.connect(store.db_path)
    try:
        rows = con.execute(
            "SELECT category, layer FROM validation_issues ORDER BY layer"
        ).fetchall()
    finally:
        con.close()
    assert rows == [("zombie_market", 2), ("ghost_book", 4)]


# ---------- 4. is_valid=False still persists (D-D3) ---------------------------


def test_write_snapshot_invalid_still_persists(store: SQLiteStore) -> None:
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=False,
        market_rows=[make_market("a")],
        issues=[],
        source_coverage=SourceCoverage.complete(1, 0),
        event_members=[],
        group_truths=[],
        publish_markets=False,
    )
    con = sqlite3.connect(store.db_path)
    try:
        row = con.execute("SELECT is_valid, market_count FROM snapshots").fetchone()
        published_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    finally:
        con.close()
    assert row == (0, 1), "D-D3: is_valid=False rows must be queryable"
    assert published_count == 0


# ---------- 5. snapshot_id ----------------------------------------------------


def test_write_snapshot_returns_snapshot_id(store: SQLiteStore) -> None:
    sid = store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("a")],
        issues=[Issue(layer=2, category=Category.UNKNOWN, market_id="a", detail="x")],
        **_complete_publication(),
    )
    assert isinstance(sid, int) and sid >= 1

    con = sqlite3.connect(store.db_path)
    try:
        market_sid = con.execute("SELECT snapshot_id FROM markets WHERE market_id='a'").fetchone()[
            0
        ]
        issue_sid = con.execute("SELECT snapshot_id FROM validation_issues").fetchone()[0]
    finally:
        con.close()
    assert market_sid == sid
    assert issue_sid == sid


# ---------- 6. invalid mode ---------------------------------------------------


def test_write_snapshot_invalid_mode_raises(store: SQLiteStore) -> None:
    with pytest.raises(ValueError):
        store.write_snapshot(
            taken_at_ms=1,
            finished_at_ms=2,
            mode="weekly",  # not in {"subset", "full"}
            parquet_path="x/1.parquet",
            is_valid=True,
            market_rows=[],
            issues=[],
            **_complete_publication(),
        )


# ---------- 7. uint256 token IDs ---------------------------------------------


def test_token_ids_preserve_uint256_string(store: SQLiteStore) -> None:
    """Pitfall 3: 70-char numeric token IDs must round-trip as exact strings."""
    big_token = "1" * 70  # 70 decimal digits — overflows int64
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("a", yes_token_id=big_token)],
        issues=[],
        **_complete_publication(),
    )
    con = sqlite3.connect(store.db_path)
    try:
        got = con.execute("SELECT yes_token_id FROM markets WHERE market_id='a'").fetchone()[0]
    finally:
        con.close()
    assert got == big_token


# ---------- 8. rollback on failure --------------------------------------------


def test_rollback_on_executemany_failure(store: SQLiteStore) -> None:
    """If executemany fails mid-transaction, markets must remain empty (atomicity)."""
    # First, insert a baseline so we can verify DELETE FROM markets ran but then rolled back.
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("baseline")],
        issues=[],
        **_complete_publication(),
    )

    # Build a row missing the NOT NULL `condition_id` to force an integrity error.
    bad = make_market("bad")
    bad["condition_id"] = None  # NOT NULL → constraint violation on insert

    with pytest.raises(sqlite3.IntegrityError):
        store.write_snapshot(
            taken_at_ms=2_000_000,
            finished_at_ms=2_000_100,
            mode="subset",
            parquet_path="x/2.parquet",
            is_valid=True,
            market_rows=[bad],
            issues=[],
            **_complete_publication(),
        )

    # The failed transaction must roll back DELETE FROM markets too — so the
    # baseline row from the first snapshot is still present and 'bad' is absent.
    con = sqlite3.connect(store.db_path)
    try:
        rows = con.execute("SELECT market_id FROM markets").fetchall()
        n_snaps = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    finally:
        con.close()
    assert rows == [("baseline",)], "Rollback must restore prior markets state — got: " + repr(rows)
    assert n_snaps == 1, "Failed snapshot must NOT leave a snapshots row behind"


# ---------- Plan 02-09: streaming snapshot writer ---------------------------


def test_write_snapshot_streaming_basic_parity(store: SQLiteStore) -> None:
    """50 rows via streaming → same snapshots/markets/issues row counts as legacy."""
    rows_a = [make_market(f"m{i}") for i in range(50)]
    rows_b = [make_market(f"m{i}") for i in range(50)]

    # Legacy path
    legacy_id = store.write_snapshot(
        taken_at_ms=1,
        finished_at_ms=2,
        mode="subset",
        parquet_path="x.parquet",
        is_valid=True,
        market_rows=rows_a,
        issues=[Issue(layer=2, category=Category.UNKNOWN, market_id="m0", detail="stale")],
        **_complete_publication(),
    )

    # Need a SECOND store / DB for streaming so write_snapshot's
    # `DELETE FROM markets` doesn't clobber the comparison.
    store2 = SQLiteStore(Path(str(store.db_path).replace(".db", ".2.db")))
    store2.init_schema()
    snap_id, count = store2.write_snapshot_streaming(
        taken_at_ms=1,
        finished_at_ms=2,
        mode="subset",
        parquet_path="x.parquet",
        is_valid=True,
        market_rows=rows_b,
        issues=[Issue(layer=2, category=Category.UNKNOWN, market_id="m0", detail="stale")],
        batch_size=20,
        **_complete_publication(),
    )
    assert count == 50

    # Compare both DBs: same market_count, same issue count
    con_a = sqlite3.connect(store.db_path)
    con_b = sqlite3.connect(store2.db_path)
    try:
        assert con_a.execute("SELECT COUNT(*) FROM markets").fetchone()[0] == 50
        assert con_b.execute("SELECT COUNT(*) FROM markets").fetchone()[0] == 50
        assert (
            con_a.execute("SELECT market_count FROM snapshots WHERE id=?", (legacy_id,)).fetchone()[
                0
            ]
            == 50
        )
        assert (
            con_b.execute("SELECT market_count FROM snapshots WHERE id=?", (snap_id,)).fetchone()[0]
            == 50
        )
        assert con_a.execute("SELECT COUNT(*) FROM validation_issues").fetchone()[0] == 1
        assert con_b.execute("SELECT COUNT(*) FROM validation_issues").fetchone()[0] == 1
    finally:
        con_a.close()
        con_b.close()


def test_write_snapshot_streaming_with_generator(store: SQLiteStore) -> None:
    """1500 rows yielded from a generator, batch_size=500 → all 1500 persisted."""

    def _gen():
        for i in range(1500):
            yield make_market(f"m{i}")

    snap_id, count = store.write_snapshot_streaming(
        taken_at_ms=10,
        finished_at_ms=20,
        mode="full",
        parquet_path="g.parquet",
        is_valid=True,
        market_rows=_gen(),
        issues=[],
        batch_size=500,
        **_complete_publication(),
    )
    assert count == 1500
    con = sqlite3.connect(store.db_path)
    try:
        assert (
            con.execute("SELECT market_count FROM snapshots WHERE id=?", (snap_id,)).fetchone()[0]
            == 1500
        )
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone()[0] == 1500
    finally:
        con.close()


def test_write_snapshot_streaming_atomicity_on_error(store: SQLiteStore) -> None:
    """Generator raises mid-stream → no snapshot row, no market rows, exception propagates."""

    class BoomError(RuntimeError):
        pass

    def _explode():
        for i in range(750):
            yield make_market(f"m{i}")
        raise BoomError("mid-stream failure")

    # Baseline counts before the failing write
    con = sqlite3.connect(store.db_path)
    try:
        snapshots_before = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        markets_before = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    finally:
        con.close()

    with pytest.raises(BoomError):
        store.write_snapshot_streaming(
            taken_at_ms=30,
            finished_at_ms=40,
            mode="subset",
            parquet_path="boom.parquet",
            is_valid=True,
            market_rows=_explode(),
            issues=[],
            batch_size=200,
            **_complete_publication(),
        )

    # ROLLBACK: counts must be unchanged
    con = sqlite3.connect(store.db_path)
    try:
        snapshots_after = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        markets_after = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        assert snapshots_after == snapshots_before, "snapshots row leaked"
        assert markets_after == markets_before, "markets rows leaked"
    finally:
        con.close()


def test_write_snapshot_streaming_empty_markets(store: SQLiteStore) -> None:
    """Empty iterator → snapshots row with market_count=0 persisted, valid."""
    snap_id, count = store.write_snapshot_streaming(
        taken_at_ms=50,
        finished_at_ms=60,
        mode="subset",
        parquet_path="empty.parquet",
        is_valid=True,
        market_rows=iter([]),
        issues=[],
        batch_size=500,
        **_complete_publication(),
    )
    assert count == 0
    con = sqlite3.connect(store.db_path)
    try:
        row = con.execute(
            "SELECT market_count, is_valid FROM snapshots WHERE id=?", (snap_id,)
        ).fetchone()
        assert row == (0, 1)
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone()[0] == 0
    finally:
        con.close()


def test_write_snapshot_streaming_atomicity_on_commit_failure(
    store: SQLiteStore, monkeypatch
) -> None:
    """W-5: COMMIT itself raises → ROLLBACK invoked, no rows visible.

    This guards against the failure mode where every executemany succeeds
    but the final commit dies (disk full, lock timeout, etc.).

    Implementation: sqlite3.Connection is an immutable C type so we can't
    monkey-patch its .execute directly. Instead we wrap sqlite3.connect at
    the module level the SUT imports from, returning a proxy that intercepts
    .execute("COMMIT") and raises, but delegates ROLLBACK to the real connection
    so we can verify the rollback path actually fires.
    """
    real_connect = sqlite3.connect

    class _CommitBomb:
        def __init__(self, real_con):
            self._real = real_con
            self.rollback_invoked = False

        def execute(self, sql, *args, **kwargs):
            if isinstance(sql, str) and sql.strip().upper() == "COMMIT":
                raise sqlite3.OperationalError("synthetic commit failure")
            if isinstance(sql, str) and sql.strip().upper() == "ROLLBACK":
                self.rollback_invoked = True
            return self._real.execute(sql, *args, **kwargs)

        def close(self):
            return self._real.close()

        def __getattr__(self, name):
            return getattr(self._real, name)

    bomb_holder = {}

    def _connect_proxy(*args, **kwargs):
        # Intercept writes against our test DB; allow ad-hoc readers through.
        real_con = real_connect(*args, **kwargs)
        # isolation_level=None identifies the store's explicit writer.
        if kwargs.get("isolation_level", "deferred") is None and "bomb" not in bomb_holder:
            bomb = _CommitBomb(real_con)
            bomb_holder["bomb"] = bomb
            return bomb
        return real_con

    monkeypatch.setattr("polyarb.storage.sqlite_store.sqlite3.connect", _connect_proxy)

    con = sqlite3.connect(store.db_path)
    try:
        snapshots_before = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        markets_before = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    finally:
        con.close()

    rows = [make_market(f"m{i}") for i in range(20)]
    with pytest.raises(sqlite3.OperationalError, match="commit failure"):
        store.write_snapshot_streaming(
            taken_at_ms=70,
            finished_at_ms=80,
            mode="subset",
            parquet_path="cf.parquet",
            is_valid=True,
            market_rows=rows,
            issues=[],
            batch_size=10,
            **_complete_publication(),
        )

    con = sqlite3.connect(store.db_path)
    try:
        assert con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == snapshots_before, (
            "commit-failure leaked a snapshots row"
        )
        assert con.execute("SELECT COUNT(*) FROM markets").fetchone()[0] == markets_before, (
            "commit-failure leaked markets rows"
        )
    finally:
        con.close()
