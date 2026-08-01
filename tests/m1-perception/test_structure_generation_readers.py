from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
from pydantic import ValidationError

from polyarb.config import Settings
from polyarb.http.market_map import _read_market_map
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
            "certification_component,certification_counts_json,created_at_ms,"
            "checkpoint_at_ms,certified_at_ms,published_at_ms) VALUES (?,?,?,"
            "'published',?,?,?,'bounded-complete',?,?,?,?,?)",
            (
                publication_id,
                window_id,
                snapshot_id,
                counts_json,
                counts_json,
                generation_hash,
                counts_json,
                snapshot_id * 1_000,
                snapshot_id * 1_000 + 1,
                snapshot_id * 1_000 + 1,
                snapshot_id * 1_000 + 1,
            ),
        )
        if point_current:
            con.execute(
                "INSERT INTO current_structure_generation(id,snapshot_id,publication_id,"
                "switched_at_ms) VALUES (1,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "snapshot_id=excluded.snapshot_id,publication_id=excluded.publication_id,"
                "switched_at_ms=excluded.switched_at_ms",
                (snapshot_id, publication_id, snapshot_id * 1_000 + 1),
            )


@pytest.fixture
def generation_db(tmp_path: Path) -> Path:
    path = tmp_path / "generation-readers.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="new", point_current=False)
    return path


def test_read_mode_defaults_legacy_and_rejects_unknown(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_ALLOW_EMPTY_SECRET", "1")
    assert Settings().structure_generation_read_mode == "legacy"
    with pytest.raises(ValidationError):
        Settings(structure_generation_read_mode="shadow")


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
            "publication_id='test-publication-2',switched_at_ms=2001 WHERE id=1"
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

    with sqlite3.connect(generation_db) as con:
        con.execute(
            "UPDATE markets SET yes_token_id='legacy-drift' "
            "WHERE snapshot_id=2 AND market_id='market-new-1'"
        )
    with structure_read_transaction(generation_db, mode="compare") as drifted:
        pass
    assert drifted.comparison is not None
    assert drifted.comparison.matches is False
    assert drifted.comparison.mismatch_reasons == (
        "universe-hash-mismatch",
        "source-truth-hash-mismatch",
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
                "publication_id='test-publication-2',switched_at_ms=2001 WHERE id=1"
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
