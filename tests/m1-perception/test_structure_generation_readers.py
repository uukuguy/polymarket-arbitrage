from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest
from pydantic import ValidationError

import polyarb.http.market_map as market_map_module
import polyarb.routing.focused_quote_collector as focused_module
import polyarb.storage.sqlite_store as sqlite_store_module
from polyarb.config import Settings
from polyarb.http.market_map import _read_market_map
from polyarb.routing.focused_quote_collector import SqliteStructureMembershipReader
from polyarb.routing.neg_risk_quote_store import NegRiskQuoteStore, _source_truth_hash
from polyarb.routing.opportunity_scanner import scan_neg_risk_buy_all
from polyarb.storage.sqlite_store import (
    SQLiteStore,
    StructureGenerationReadError,
    structure_read_transaction,
)


def _seed_structure_revision(
    path: Path,
    *,
    snapshot_id: int,
    market_suffix: str,
    point_current: bool,
) -> None:
    store = SQLiteStore(path)
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (?,?,?,'full',2,1,'structure','legacy','ok',1,'')",
            (snapshot_id, snapshot_id * 1_000, snapshot_id * 1_000 + 1),
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage(snapshot_id,completed,market_items,"
            "event_items) VALUES (?,1,2,1)",
            (snapshot_id,),
        )
        con.execute(
            "INSERT INTO events(snapshot_id,id,slug,active,closed,fetched_at_ms) "
            "VALUES (?,'event-1','event-1',1,0,?)",
            (snapshot_id, snapshot_id * 1_000),
        )
        members = []
        markets = []
        for index in (1, 2):
            market_id = f"market-{market_suffix}-{index}"
            members.append((snapshot_id, "event-1", "group-1", market_id))
            markets.append(
                (
                    market_id,
                    f"condition-{market_suffix}-{index}",
                    f"slug-{market_suffix}-{index}",
                    f"token-{market_suffix}-{index}",
                    snapshot_id * 1_000,
                    snapshot_id,
                )
            )
        membership_identity = [
            ("event-1", "group-1", row[3], "named", True, False)
            for row in members
        ]
        membership_hash = hashlib.sha256(
            json.dumps(
                membership_identity,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        con.executemany(
            "INSERT INTO event_market_memberships(snapshot_id,event_id,"
            "neg_risk_market_id,market_id,member_kind,active,closed) "
            "VALUES (?,?,?,?,'named',1,0)",
            members,
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth(snapshot_id,event_id,neg_risk_market_id,"
            "neg_risk_type,expected_member_count,active_named_count,membership_hash,"
            "quality) VALUES (?,'event-1','group-1','standard',2,2,?,"
            "'complete-supported')",
            (snapshot_id, membership_hash),
        )
        con.executemany(
            "INSERT INTO markets(market_id,condition_id,slug,yes_token_id,active,closed,"
            "best_ask_price,best_ask_size,neg_risk,neg_risk_market_id,fetched_at_ms,"
            "snapshot_id,incomplete,event_id) VALUES (?,?,?, ?,1,0,?,10,1,'group-1',"
            "?,?,0,'event-1')",
            [(*row[:4], 0.4 if index == 0 else 0.5, *row[4:]) for index, row in enumerate(markets)],
        )
        event_columns = (
            "snapshot_id,id,slug,title,ticker,active,closed,liquidity_usd,volume_usd,"
            "end_time_ms,fetched_at_ms,page_fetched_at_ms"
        )
        con.execute(
            f"INSERT INTO structure_generation_events({event_columns}) "
            f"SELECT {event_columns} FROM events WHERE snapshot_id=?",
            (snapshot_id,),
        )
        con.execute(
            "INSERT INTO structure_generation_memberships SELECT * FROM "
            "event_market_memberships WHERE snapshot_id=?",
            (snapshot_id,),
        )
        con.execute(
            "INSERT INTO structure_generation_group_truth SELECT * FROM "
            "neg_risk_group_truth WHERE snapshot_id=?",
            (snapshot_id,),
        )
        market_columns = (
            "snapshot_id,market_id,condition_id,slug,question,yes_token_id,no_token_id,"
            "mid_price,liquidity_usd,volume_usd,best_bid_price,best_bid_size,"
            "best_ask_price,best_ask_size,end_time_ms,active,closed,neg_risk,"
            "neg_risk_market_id,fetched_at_ms,page_fetched_at_ms,incomplete,event_id"
        )
        con.execute(
            f"INSERT INTO structure_generation_markets({market_columns}) "
            f"SELECT {market_columns} FROM markets WHERE snapshot_id=?",
            (snapshot_id,),
        )
        counts = store._generation_counts(con, snapshot_id)
        generation_hash = store._generation_hash(con, snapshot_id)
        counts_json = json.dumps(counts, sort_keys=True, separators=(",", ":"))
        window_id = f"test-window-{snapshot_id}"
        publication_id = f"test-publication-{snapshot_id}"
        con.execute(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms,"
            "published_snapshot_id) VALUES (?,'published',?,?,?)",
            (window_id, snapshot_id * 1_000, snapshot_id * 1_000 + 1, snapshot_id),
        )
        con.execute(
            "INSERT INTO structure_publications(publication_id,window_id,snapshot_id,"
            "status,expected_counts_json,committed_counts_json,validation_hash,"
            "certification_component,certification_hash,certification_counts_json,created_at_ms,"
            "checkpoint_at_ms,certified_at_ms,published_at_ms) VALUES (?,?,?,"
            "'published',?,?,?,'bounded-complete',?,?,?,?,?,?)",
            (
                publication_id,
                window_id,
                snapshot_id,
                counts_json,
                counts_json,
                generation_hash,
                generation_hash,
                counts_json,
                snapshot_id * 1_000,
                snapshot_id * 1_000 + 1,
                snapshot_id * 1_000 + 1,
                snapshot_id * 1_000 + 1,
            ),
        )
        legacy_universe_hash, legacy_source_truth_hash = (
            sqlite_store_module._structure_universe_hash(
                con,
                snapshot_id=snapshot_id,
                generation=False,
            )
        )
        generation_universe_hash, generation_source_truth_hash = (
            sqlite_store_module._structure_universe_hash(
                con,
                snapshot_id=snapshot_id,
                generation=True,
            )
        )
        receipt_created_at_ms = snapshot_id * 1_000 + 1
        receipt_digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=snapshot_id,
            publication_id=publication_id,
            legacy_snapshot_id=snapshot_id,
            legacy_market_count=2,
            generation_market_count=2,
            legacy_universe_hash=legacy_universe_hash,
            generation_universe_hash=generation_universe_hash,
            legacy_source_truth_hash=legacy_source_truth_hash,
            generation_source_truth_hash=generation_source_truth_hash,
            generation_validation_hash=generation_hash,
            created_at_ms=receipt_created_at_ms,
        )
        con.execute(
            "INSERT INTO structure_generation_comparison_receipts("
            "generation_snapshot_id,publication_id,legacy_snapshot_id,"
            "legacy_market_count,generation_market_count,legacy_universe_hash,"
            "generation_universe_hash,legacy_source_truth_hash,"
            "generation_source_truth_hash,generation_validation_hash,created_at_ms,"
            "receipt_digest) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                snapshot_id,
                publication_id,
                snapshot_id,
                2,
                2,
                legacy_universe_hash,
                generation_universe_hash,
                legacy_source_truth_hash,
                generation_source_truth_hash,
                generation_hash,
                receipt_created_at_ms,
                receipt_digest,
            ),
        )
        if point_current:
            con.execute(
                "INSERT INTO current_structure_generation(id,snapshot_id,publication_id,"
                "validation_hash,counts_json,certification_component,"
                "comparison_receipt_digest,switched_at_ms) "
                "VALUES (1,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "snapshot_id=excluded.snapshot_id,publication_id=excluded.publication_id,"
                "validation_hash=excluded.validation_hash,"
                "counts_json=excluded.counts_json,"
                "certification_component=excluded.certification_component,"
                "comparison_receipt_digest=excluded.comparison_receipt_digest,"
                "switched_at_ms=excluded.switched_at_ms",
                (
                    snapshot_id,
                    publication_id,
                    generation_hash,
                    counts_json,
                    "bounded-complete",
                    receipt_digest,
                    snapshot_id * 1_000 + 1,
                ),
            )


@pytest.fixture
def generation_db(tmp_path: Path) -> Path:
    path = tmp_path / "generation-readers.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="new", point_current=False)
    return path


def test_generation_operations_report_pressure_and_reclaim_one_safe_chain(
    tmp_path: Path,
) -> None:
    path = tmp_path / "generation-operations.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="one", point_current=False)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="two", point_current=False)
    _seed_structure_revision(path, snapshot_id=3, market_suffix="three", point_current=True)
    store = SQLiteStore(path)

    status = store.structure_generation_status(retain_generations=2)
    assert status["pointer_snapshot_id"] == 3
    assert status["retained_generation_count"] == 3
    assert status["reclaimable_generation_count"] == 1
    assert status["generation_count_agrees"] is True
    assert status["generation_hash_agrees"] is True

    steps = []
    while True:
        cleanup = SQLiteStore(path).cleanup_structure_generation_evidence(
            retain_generations=2,
            max_rows=1,
            now_ms=10_000 + len(steps),
        )
        steps.append(cleanup)
        assert cleanup["rows_deleted"] <= 1
        if len(steps) == 1:
            with pytest.raises(
                StructureGenerationReadError,
                match="generation-evidence-cleanup-active",
            ):
                with structure_read_transaction(
                    path, mode="generation", snapshot_id=1
                ):
                    pass
        if cleanup["reclaimed_generation_ids"]:
            break
    assert cleanup["blocked"] is False
    assert cleanup["blocked_reason"] is None
    assert cleanup["reclaimed_generation_ids"] == [1]
    assert cleanup["retained_generation_ids"] == [3, 2]
    assert len(steps) > 1
    replay = store.cleanup_structure_generation_evidence(
        retain_generations=2,
        max_rows=1,
        now_ms=20_000,
    )
    assert replay["blocked"] is False
    assert replay["reclaimed_generation_ids"] == []
    assert replay["retained_generation_ids"] == [3, 2]
    assert replay["rows_deleted"] == 0
    with pytest.raises(
        StructureGenerationReadError,
        match="generation-evidence-reclaimed",
    ):
        with structure_read_transaction(path, mode="generation", snapshot_id=1):
            pass

    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT snapshot_id FROM structure_publications ORDER BY snapshot_id"
        ).fetchall() == [(1,), (2,), (3,)]
        assert con.execute(
            "SELECT DISTINCT snapshot_id FROM structure_generation_markets "
            "ORDER BY snapshot_id"
        ).fetchall() == [(2,), (3,)]
        # Cleanup owns generation evidence only. Legacy truth required by any
        # retained authenticated comparison receipt remains untouched.
        assert con.execute(
            "SELECT id FROM snapshots ORDER BY id"
        ).fetchall() == [(1,), (2,), (3,)]
        assert con.execute(
            "SELECT DISTINCT snapshot_id FROM markets ORDER BY snapshot_id"
        ).fetchall() == [(1,), (2,), (3,)]
        assert con.execute(
            "SELECT generation_snapshot_id FROM structure_generation_cleanup_receipts"
        ).fetchall() == [(1,)]
        assert con.execute(
            "SELECT generation_snapshot_id FROM "
            "structure_generation_comparison_receipts ORDER BY generation_snapshot_id"
        ).fetchall() == [(1,), (2,), (3,)]
        with pytest.raises(sqlite3.IntegrityError, match="cleanup-receipt-sealed"):
            con.execute(
                "UPDATE structure_generation_cleanup_receipts SET reclaimed_at_ms=0 "
                "WHERE generation_snapshot_id=1"
            )


def test_generation_cleanup_fails_closed_on_unauthenticated_candidate(
    tmp_path: Path,
) -> None:
    path = tmp_path / "generation-cleanup-blocked.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="one", point_current=False)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="two", point_current=False)
    _seed_structure_revision(path, snapshot_id=3, market_suffix="three", point_current=True)
    with sqlite3.connect(path) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_update")
        con.execute(
            "UPDATE structure_generation_comparison_receipts SET receipt_digest=? "
            "WHERE generation_snapshot_id=1",
            ("f" * 64,),
        )

    result = SQLiteStore(path).cleanup_structure_generation_evidence(
        retain_generations=2,
        max_rows=1,
        now_ms=10_000,
    )
    assert result["blocked"] is True
    assert result["blocked_reason"] == "comparison-receipt-digest-mismatch"
    assert result["reclaimed_generation_ids"] == []

    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_markets WHERE snapshot_id=1"
        ).fetchone() == (2,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_publications WHERE snapshot_id=1"
        ).fetchone() == (1,)


def test_read_mode_defaults_legacy_and_rejects_unknown(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    assert Settings().structure_generation_read_mode == "legacy"
    with pytest.raises(ValidationError):
        Settings(structure_generation_read_mode="shadow")


def test_existing_pointer_schema_adds_redundant_receipt_columns(tmp_path: Path) -> None:
    path = tmp_path / "legacy-pointer.db"
    with sqlite3.connect(path) as con:
        con.execute(
            "CREATE TABLE current_structure_generation("
            "id INTEGER PRIMARY KEY,snapshot_id INTEGER,publication_id TEXT,"
            "switched_at_ms INTEGER)"
        )

    SQLiteStore(path).init_schema()

    with sqlite3.connect(path) as con:
        columns = {
            row[1]
            for row in con.execute("PRAGMA table_info(current_structure_generation)")
        }
    assert {
        "validation_hash",
        "counts_json",
        "certification_component",
        "comparison_receipt_digest",
    } <= columns


def _downgrade_to_pre_task5_pointer(path: Path) -> None:
    with sqlite3.connect(path) as con:
        con.execute("DROP VIEW current_structure_markets")
        con.execute(
            "ALTER TABLE current_structure_generation "
            "RENAME TO current_structure_generation_task5"
        )
        con.execute(
            "CREATE TABLE current_structure_generation("
            "id INTEGER PRIMARY KEY,snapshot_id INTEGER,publication_id TEXT,"
            "switched_at_ms INTEGER)"
        )
        con.execute(
            "INSERT INTO current_structure_generation "
            "SELECT id,snapshot_id,publication_id,switched_at_ms "
            "FROM current_structure_generation_task5"
        )
        con.execute("DROP TABLE current_structure_generation_task5")


def test_literal_pre_task5_pointer_repairs_and_replays_idempotently(
    tmp_path: Path,
) -> None:
    path = tmp_path / "pre-task5.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _downgrade_to_pre_task5_pointer(path)

    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        first = con.execute(
            "SELECT validation_hash,counts_json,certification_component,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        second = con.execute(
            "SELECT validation_hash,counts_json,certification_component,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()
    assert first == second
    assert all(value is not None for value in first)
    with structure_read_transaction(path, mode="generation") as read:
        assert read.snapshot_id == 1


def test_pre_task5_pointer_without_receipt_repairs_with_bounded_provenance(
    tmp_path: Path,
) -> None:
    path = tmp_path / "pre-task5-no-receipt.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _downgrade_to_pre_task5_pointer(path)
    with sqlite3.connect(path) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_delete")
        con.execute("DELETE FROM structure_generation_comparison_receipts")

    SQLiteStore(path).init_schema()
    with structure_read_transaction(path, mode="generation") as generation:
        assert generation.snapshot_id == 1
    with structure_read_transaction(path, mode="compare") as read:
        assert read.comparison is not None
        assert read.comparison.mismatch_reasons == ("comparison-receipt-missing",)
    with sqlite3.connect(path) as con:
        before = con.execute(
            "SELECT validation_hash,counts_json,certification_component,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()
        progress = con.execute(
            "SELECT phase FROM structure_generation_comparison_progress "
            "WHERE publication_id='test-publication-1'"
        ).fetchone()
    assert all(value is not None for value in before[:3])
    assert before[3] is None
    assert progress is not None and progress[0] != "sealed"
    for _ in range(20):
        SQLiteStore(path).backfill_current_structure_generation(max_rows=1)
        with sqlite3.connect(path) as con:
            digest = con.execute(
                "SELECT comparison_receipt_digest "
                "FROM current_structure_generation WHERE id=1"
            ).fetchone()[0]
        if digest is not None:
            break
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        after = con.execute(
            "SELECT validation_hash,counts_json,certification_component,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()
    assert after[:3] == before[:3]
    assert after[3] is not None
    with structure_read_transaction(path, mode="compare") as repaired:
        assert repaired.comparison is not None
        assert repaired.comparison.matches is True


def test_pre_task5_pointer_unverifiable_publication_remains_fail_closed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "pre-task5-corrupt.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _downgrade_to_pre_task5_pointer(path)
    with sqlite3.connect(path) as con:
        con.execute(
            "UPDATE structure_publications SET certification_hash=? "
            "WHERE publication_id='test-publication-1'",
            ("f" * 64,),
        )

    SQLiteStore(path).init_schema()
    with pytest.raises(StructureGenerationReadError):
        with structure_read_transaction(path, mode="generation"):
            pass


def test_pre_task5_pointer_partial_authentication_is_not_self_healed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "pre-task5-partial.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _downgrade_to_pre_task5_pointer(path)
    with sqlite3.connect(path) as con:
        con.execute(
            "ALTER TABLE current_structure_generation ADD COLUMN validation_hash TEXT"
        )
        con.execute(
            "UPDATE current_structure_generation SET validation_hash=? WHERE id=1",
            ("f" * 64,),
        )

    SQLiteStore(path).init_schema()
    with pytest.raises(StructureGenerationReadError):
        with structure_read_transaction(path, mode="generation"):
            pass


@pytest.mark.parametrize("present_mask", range(1, 15))
def test_pre_task5_pointer_partial_authentication_matrix_never_mutates(
    tmp_path: Path,
    present_mask: int,
) -> None:
    path = tmp_path / f"pre-task5-partial-{present_mask}.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    with sqlite3.connect(path) as con:
        valid = con.execute(
            "SELECT validation_hash,committed_counts_json,certification_component "
            "FROM structure_publications WHERE publication_id='test-publication-1'"
        ).fetchone()
        valid_digest = con.execute(
            "SELECT receipt_digest FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=1"
        ).fetchone()[0]
    _downgrade_to_pre_task5_pointer(path)
    with sqlite3.connect(path) as con:
        for column in (
            "validation_hash",
            "counts_json",
            "certification_component",
            "comparison_receipt_digest",
        ):
            con.execute(
                f"ALTER TABLE current_structure_generation ADD COLUMN {column} TEXT"
            )
        values = (valid[0], valid[1], valid[2], valid_digest)
        selected = tuple(
            value if present_mask & (1 << index) else None
            for index, value in enumerate(values)
        )
        con.execute(
            "UPDATE current_structure_generation SET validation_hash=?,counts_json=?,"
            "certification_component=?,comparison_receipt_digest=? WHERE id=1",
            selected,
        )
        before = con.execute(
            "SELECT validation_hash,counts_json,certification_component,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()

    SQLiteStore(path).init_schema()
    SQLiteStore(path).backfill_current_structure_generation(max_rows=1)
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        after = con.execute(
            "SELECT validation_hash,counts_json,certification_component,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()
    assert after == before
    with pytest.raises(StructureGenerationReadError):
        with structure_read_transaction(path, mode="generation"):
            pass
    with structure_read_transaction(path, mode="compare") as read:
        assert read.comparison is not None
        assert read.comparison.matches is False


def test_current_generation_view_selects_only_pointer_rows(generation_db: Path) -> None:
    with sqlite3.connect(generation_db) as con:
        assert con.execute(
            "SELECT market_id FROM current_structure_markets ORDER BY market_id"
        ).fetchall() == [("market-old-1",), ("market-old-2",)]


def test_generation_read_fails_closed_without_pointer(generation_db: Path) -> None:
    with sqlite3.connect(generation_db) as con:
        con.execute("DELETE FROM current_structure_generation")

    with pytest.raises(StructureGenerationReadError, match="pointer-missing"):
        with structure_read_transaction(generation_db, mode="generation"):
            pass


def test_compare_serves_legacy_and_exposes_deterministic_mismatch(
    generation_db: Path,
) -> None:
    with sqlite3.connect(generation_db) as con:
        con.execute(
            "UPDATE current_structure_generation SET snapshot_id=2,"
            "publication_id='test-publication-2',validation_hash=(SELECT validation_hash "
            "FROM structure_publications WHERE publication_id='test-publication-2'),"
            "counts_json=(SELECT committed_counts_json FROM structure_publications "
            "WHERE publication_id='test-publication-2'),"
            "certification_component='bounded-complete',comparison_receipt_digest="
            "(SELECT receipt_digest FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=2),switched_at_ms=2001 WHERE id=1"
        )

    with structure_read_transaction(generation_db, mode="compare") as read:
        rows = read.connection.execute(
            f"SELECT market_id FROM {read.table('markets')} WHERE snapshot_id=? "
            "ORDER BY market_id",
            (read.snapshot_id,),
        ).fetchall()

    assert rows == [("market-new-1",), ("market-new-2",)]
    assert read.comparison is not None
    assert read.comparison.matches is True
    assert read.comparison.mismatch_reasons == ()

    with sqlite3.connect(generation_db) as con, pytest.raises(
        sqlite3.IntegrityError, match="comparison-receipt-sealed"
    ):
        con.execute(
            "UPDATE structure_generation_comparison_receipts SET "
            "legacy_market_count=999,generation_market_count=999,"
            "legacy_universe_hash=?,generation_universe_hash=? "
            "WHERE generation_snapshot_id=2",
            ("d" * 64, "d" * 64),
        )


def test_generation_read_fails_closed_on_receipt_count_mismatch(
    generation_db: Path,
) -> None:
    with sqlite3.connect(generation_db) as con:
        con.execute(
            "UPDATE structure_publications SET committed_counts_json="
            "replace(committed_counts_json,'\"markets\":2','\"markets\":3') "
            "WHERE publication_id='test-publication-1'"
        )
    with pytest.raises(StructureGenerationReadError, match="count-mismatch"):
        with structure_read_transaction(generation_db, mode="generation"):
            pass


def test_historical_generation_read_never_resolves_current_pointer(
    generation_db: Path,
) -> None:
    with structure_read_transaction(
        generation_db,
        mode="generation",
        snapshot_id=2,
    ) as read:
        ids = read.connection.execute(
            f"SELECT market_id FROM {read.table('markets')} WHERE snapshot_id=? "
            "ORDER BY market_id",
            (read.snapshot_id,),
        ).fetchall()
    assert read.snapshot_id == 2
    assert ids == [("market-new-1",), ("market-new-2",)]


def test_read_transaction_stays_on_old_generation_across_pointer_switch(
    generation_db: Path,
) -> None:
    with structure_read_transaction(generation_db, mode="generation") as old:
        assert old.snapshot_id == 1
        with sqlite3.connect(generation_db) as writer:
            writer.execute(
                "UPDATE current_structure_generation SET snapshot_id=2,"
                "publication_id='test-publication-2',validation_hash=(SELECT validation_hash "
                "FROM structure_publications WHERE publication_id='test-publication-2'),"
                "counts_json=(SELECT committed_counts_json FROM structure_publications "
                "WHERE publication_id='test-publication-2'),"
                "certification_component='bounded-complete',comparison_receipt_digest="
                "(SELECT receipt_digest FROM structure_generation_comparison_receipts "
                "WHERE generation_snapshot_id=2),switched_at_ms=2001 WHERE id=1"
            )
        old_ids = old.connection.execute(
            f"SELECT market_id FROM {old.table('markets')} WHERE snapshot_id=? "
            "ORDER BY market_id",
            (old.snapshot_id,),
        ).fetchall()

    with structure_read_transaction(generation_db, mode="generation") as new:
        new_ids = new.connection.execute(
            f"SELECT market_id FROM {new.table('markets')} WHERE snapshot_id=? "
            "ORDER BY market_id",
            (new.snapshot_id,),
        ).fetchall()

    assert old_ids == [("market-old-1",), ("market-old-2",)]
    assert new.snapshot_id == 2
    assert new_ids == [("market-new-1",), ("market-new-2",)]


def test_opportunity_scanner_reads_generation_tables(generation_db: Path) -> None:
    opportunities = scan_neg_risk_buy_all(
        generation_db,
        min_edge_bps=0,
        structure_generation_read_mode="generation",
    )
    assert len(opportunities) == 1
    assert opportunities[0].snapshot_id == 1
    assert [leg.market_id for leg in opportunities[0].legs] == [
        "market-old-1",
        "market-old-2",
    ]


def test_market_map_reads_generation_truth(generation_db: Path) -> None:
    payload = _read_market_map(
        generation_db,
        event_id=None,
        now_ms=1_001,
        max_age_s=60,
        quote_max_age_s=60,
        structure_generation_read_mode="generation",
    )
    assert payload["structure_revision"] == 1
    assert payload["scannable_groups"][0]["group_id"] == "group-1"


def test_supabase_projection_resolves_generation_once(generation_db: Path) -> None:
    snapshot, rows = SQLiteStore(generation_db).read_structure_mirror_projection(
        structure_generation_read_mode="generation"
    )
    assert snapshot["id"] == 1
    assert [row["market_id"] for row in rows] == ["market-old-1", "market-old-2"]


def test_generation_hot_resolution_never_calls_full_scan_helpers(
    generation_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("hot generation read invoked full-universe helper")

    monkeypatch.setattr(SQLiteStore, "_generation_counts", forbidden)
    monkeypatch.setattr(SQLiteStore, "_generation_hash", forbidden)
    monkeypatch.setattr(sqlite_store_module, "_structure_universe_hash", forbidden)

    with structure_read_transaction(generation_db, mode="generation") as read:
        assert read.snapshot_id == 1


def test_compare_hot_resolution_consumes_receipt_without_full_scan_helpers(
    generation_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args, **_kwargs):
        raise AssertionError("hot compare read invoked full-universe helper")

    monkeypatch.setattr(SQLiteStore, "_generation_counts", forbidden)
    monkeypatch.setattr(SQLiteStore, "_generation_hash", forbidden)
    monkeypatch.setattr(sqlite_store_module, "_structure_universe_hash", forbidden)

    with structure_read_transaction(generation_db, mode="compare") as read:
        assert read.comparison is not None
        assert read.comparison.mismatch_reasons == (
            "comparison-receipt-identity-mismatch",
        )


def test_backfill_receipt_hashing_streams_without_fetchall_materialization() -> None:
    source = inspect.getsource(sqlite_store_module._structure_universe_hash)
    assert ".fetchall(" not in source


def test_comparison_receipt_hashes_match_quote_universe_contract(
    generation_db: Path,
) -> None:
    universe = NegRiskQuoteStore(generation_db).latest_verified_universe()
    with sqlite3.connect(generation_db) as con:
        receipt = con.execute(
            "SELECT legacy_universe_hash,legacy_source_truth_hash FROM "
            "structure_generation_comparison_receipts WHERE generation_snapshot_id=2"
        ).fetchone()
    assert receipt == (universe.universe_hash, _source_truth_hash(universe))


def test_focused_generation_read_traces_only_requested_group(
    generation_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    original = sqlite_store_module.structure_read_transaction

    @contextmanager
    def traced(*args, **kwargs):
        kwargs["trace_callback"] = statements.append
        with original(*args, **kwargs) as read:
            yield read

    monkeypatch.setattr(focused_module, "structure_read_transaction", traced)

    group = SqliteStructureMembershipReader(
        generation_db,
        structure_generation_read_mode="generation",
    ).current_group("event-1", "group-1")

    assert group is not None
    sql = "\n".join(statements).lower()
    assert "count(" not in sql
    assert "structure_generation_issues" not in sql
    assert "where snapshot_id=1 and event_id='event-1'" in sql
    assert "neg_risk_market_id='group-1'" in sql


def test_http_generation_read_does_not_hash_or_scan_market_rows(
    generation_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    original = sqlite_store_module.structure_read_transaction

    @contextmanager
    def traced(*args, **kwargs):
        kwargs["trace_callback"] = statements.append
        with original(*args, **kwargs) as read:
            yield read

    monkeypatch.setattr(market_map_module, "structure_read_transaction", traced)

    payload = _read_market_map(
        generation_db,
        event_id="event-1",
        now_ms=1_001,
        max_age_s=60,
        quote_max_age_s=60,
        structure_generation_read_mode="generation",
    )

    assert payload["structure_revision"] == 1
    sql = "\n".join(statements).lower()
    assert "count(" not in sql
    assert "from structure_generation_markets" not in sql
    assert "where snapshot_id=1 and event_id='event-1'" in sql


def test_generation_pointer_publication_identity_corruption_fails_closed(
    generation_db: Path,
) -> None:
    with sqlite3.connect(generation_db) as con:
        con.execute(
            "UPDATE current_structure_generation SET publication_id="
            "'test-publication-2' WHERE id=1"
        )
    with pytest.raises(StructureGenerationReadError, match="identity-mismatch"):
        with structure_read_transaction(generation_db, mode="generation"):
            pass


def test_generation_pointer_validation_receipt_corruption_fails_closed(
    generation_db: Path,
) -> None:
    with sqlite3.connect(generation_db) as con:
        con.execute(
            "UPDATE current_structure_generation SET validation_hash=? WHERE id=1",
            ("f" * 64,),
        )
    with pytest.raises(StructureGenerationReadError, match="validation-hash-mismatch"):
        with structure_read_transaction(generation_db, mode="generation"):
            pass


def test_generation_snapshot_identity_corruption_fails_closed(
    generation_db: Path,
) -> None:
    with sqlite3.connect(generation_db) as con:
        con.execute("UPDATE snapshots SET data_product='archive' WHERE id=1")
    with pytest.raises(StructureGenerationReadError, match="identity-mismatch"):
        with structure_read_transaction(generation_db, mode="generation"):
            pass


def test_compare_receipt_identity_and_hash_corruption_are_deterministic(
    generation_db: Path,
) -> None:
    with sqlite3.connect(generation_db) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_update")
        con.execute(
            "UPDATE structure_generation_comparison_receipts SET legacy_snapshot_id=999 "
            "WHERE generation_snapshot_id=1"
        )
    with structure_read_transaction(generation_db, mode="compare") as identity:
        pass
    assert identity.comparison is not None
    assert identity.comparison.mismatch_reasons == (
        "comparison-receipt-digest-mismatch",
    )

    with sqlite3.connect(generation_db) as con:
        fields = con.execute(
            "SELECT publication_id,legacy_market_count,generation_market_count,"
            "legacy_universe_hash,generation_universe_hash,legacy_source_truth_hash,"
            "generation_source_truth_hash,generation_validation_hash,created_at_ms "
            "FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=1"
        ).fetchone()
        swapped_digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=1,
            publication_id=str(fields[0]),
            legacy_snapshot_id=999,
            legacy_market_count=int(fields[1]),
            generation_market_count=int(fields[2]),
            legacy_universe_hash=str(fields[3]),
            generation_universe_hash=str(fields[4]),
            legacy_source_truth_hash=str(fields[5]),
            generation_source_truth_hash=str(fields[6]),
            generation_validation_hash=str(fields[7]),
            created_at_ms=int(fields[8]),
        )
        con.execute(
            "UPDATE structure_generation_comparison_receipts SET receipt_digest=? "
            "WHERE generation_snapshot_id=1",
            (swapped_digest,),
        )
        con.execute(
            "UPDATE current_structure_generation SET comparison_receipt_digest=? "
            "WHERE id=1",
            (swapped_digest,),
        )
    with structure_read_transaction(generation_db, mode="compare") as swapped:
        pass
    assert swapped.comparison is not None
    assert swapped.comparison.mismatch_reasons == (
        "comparison-receipt-identity-mismatch",
    )

    with sqlite3.connect(generation_db) as con:
        con.execute(
            "UPDATE structure_generation_comparison_receipts SET receipt_digest=? "
            "WHERE generation_snapshot_id=1",
            ("e" * 64,),
        )
    with structure_read_transaction(generation_db, mode="compare") as tampered:
        pass
    assert tampered.comparison is not None
    assert tampered.comparison.mismatch_reasons == (
        "comparison-receipt-digest-mismatch",
    )


def test_compare_missing_receipt_is_deterministic_mismatch(generation_db: Path) -> None:
    with sqlite3.connect(generation_db) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_delete")
        con.execute(
            "DELETE FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=1"
        )
    with structure_read_transaction(generation_db, mode="compare") as read:
        pass
    assert read.comparison is not None
    assert read.comparison.mismatch_reasons == ("comparison-receipt-missing",)


def test_legacy_exact_mirror_accepts_invalid_non_structure_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "legacy-exact.db"
    store = SQLiteStore(path)
    store.init_schema()
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "data_product,is_valid,market_view_published,parquet_path) "
            "VALUES (7,1,2,'full',1,'archive',0,0,'archive.parquet')"
        )
        con.execute(
            "INSERT INTO markets(market_id,condition_id,fetched_at_ms,snapshot_id) "
            "VALUES ('legacy-market','legacy-condition',1,7)"
        )

    snapshot, rows = store.read_structure_mirror_projection(
        structure_generation_read_mode="legacy",
        snapshot_id=7,
    )

    assert snapshot["id"] == 7
    assert [row["market_id"] for row in rows] == ["legacy-market"]
