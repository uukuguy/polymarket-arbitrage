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
    quarantine_issue: bool = False,
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
        if quarantine_issue:
            evidence = (
                "active-open-neg-risk-market-parent-absent-from-active-event-catalogue:"
                + "0" * 64
            )
            issue_values = (
                snapshot_id,
                1,
                "api_jitter",
                "quarantined-market",
                "quarantined",
                evidence,
            )
            con.execute(
                "INSERT INTO validation_issues(snapshot_id,layer,category,market_id,"
                "detail,raw_payload) VALUES (?,?,?,?,?,?)",
                issue_values,
            )
            con.execute(
                "INSERT INTO structure_generation_issues(snapshot_id,issue_index,layer,"
                "category,market_id,detail,raw_payload) VALUES (?,1,?,?,?,?,?)",
                issue_values,
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
        # This fixture seeds the immutable result of a completed legacy
        # publication directly instead of going through write_snapshot().
        con.execute("DELETE FROM legacy_structure_revision_dirty WHERE id=1")


@pytest.fixture
def generation_db(tmp_path: Path) -> Path:
    path = tmp_path / "generation-readers.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="new", point_current=False)
    return path


def test_drift_receipt_is_append_only(generation_db: Path) -> None:
    digest = "a" * 64
    with sqlite3.connect(generation_db) as con:
        con.execute(
            "INSERT INTO structure_generation_drift_receipts("
            "comparison_id,legacy_snapshot_id,legacy_taken_at_ms,"
            "legacy_finished_at_ms,legacy_market_count,legacy_universe_hash,"
            "legacy_source_truth_hash,generation_snapshot_id,publication_id,window_id,"
            "published_snapshot_id,normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash,source_event_count,"
            "source_market_count,source_event_hash,source_market_hash,source_identity_hash,"
            "projection_universe_hash,"
            "projection_group_truth_hash,generation_universe_hash,"
            "generation_group_truth_hash,class_counts_json,class_digests_json,"
            "legacy_reconstruction_root,generation_reconstruction_root,"
            "overlap_conflict_count,unclassified_count,created_at_ms,receipt_digest) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "drift-1",
                1,
                1000,
                1001,
                2,
                digest,
                digest,
                1,
                "test-publication-1",
                "test-window-1",
                1,
                "contract-v1",
                digest,
                digest,
                digest,
                1,
                2,
                digest,
                digest,
                digest,
                digest,
                digest,
                digest,
                digest,
                "{}",
                "{}",
                digest,
                digest,
                0,
                0,
                1002,
                digest,
            ),
        )
        with pytest.raises(sqlite3.IntegrityError, match="structure-drift-receipt-sealed"):
            con.execute(
                "UPDATE structure_generation_drift_receipts SET created_at_ms=1003 "
                "WHERE comparison_id='drift-1'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="structure-drift-receipt-sealed"):
            con.execute(
                "DELETE FROM structure_generation_drift_receipts "
                "WHERE comparison_id='drift-1'"
            )
    deleted, window_ids = SQLiteStore(
        generation_db
    ).purge_published_structure_sync_windows(keep_last=1, max_windows_per_run=1)
    assert deleted == 0
    assert window_ids == []


def test_drift_schema_initialization_does_not_create_progress(
    generation_db: Path,
) -> None:
    SQLiteStore(generation_db).init_structure_sync_schema()
    with sqlite3.connect(generation_db) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress"
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT snapshot_id,publication_id FROM current_structure_generation"
        ).fetchall() == [(1, "test-publication-1")]


def test_drift_receipt_digest_authenticates_every_field() -> None:
    fields = sqlite_store_module._STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS
    payload = {field: index for index, field in enumerate(fields)}
    original = sqlite_store_module._structure_drift_receipt_digest(payload)
    assert len(original) == 64
    for field in fields:
        changed = dict(payload)
        changed[field] = f"changed-{field}"
        assert sqlite_store_module._structure_drift_receipt_digest(changed) != original
    with pytest.raises(ValueError, match="invalid-structure-drift-receipt-fields"):
        sqlite_store_module._structure_drift_receipt_digest(
            {**payload, "unexpected": "field"}
        )


def test_drift_progress_protects_published_source_window_from_retention(
    generation_db: Path,
) -> None:
    digest = "a" * 64
    with sqlite3.connect(generation_db) as con:
        con.execute(
            "INSERT INTO structure_generation_drift_progress("
            "comparison_id,legacy_snapshot_id,generation_snapshot_id,publication_id,"
            "window_id,normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash,source_event_count,"
            "source_market_count,source_event_hash,source_market_hash,source_identity_hash,"
            "phase,row_cursor_json,"
            "digest_state_json,class_counts_json,class_digests_json,created_at_ms,"
            "checkpoint_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'source-events',NULL,"
            "'{}','{}','{}',?,?)",
            (
                "drift-progress-1",
                1,
                1,
                "test-publication-1",
                "test-window-1",
                "contract-v1",
                digest,
                digest,
                digest,
                1,
                2,
                digest,
                digest,
                digest,
                1002,
                1002,
            ),
        )

    deleted, window_ids = SQLiteStore(
        generation_db
    ).purge_published_structure_sync_windows(keep_last=1, max_windows_per_run=1)

    assert deleted == 0
    assert window_ids == []
    with sqlite3.connect(generation_db) as con:
        assert con.execute(
            "SELECT status,published_snapshot_id FROM structure_sync_windows "
            "WHERE id='test-window-1'"
        ).fetchone() == ("published", 1)


def test_drift_initialization_pins_current_authenticated_identity_once(
    generation_db: Path,
) -> None:
    with sqlite3.connect(generation_db) as con:
        # Snapshot 2 is the fresh generation; snapshot 1 remains the latest
        # complete legacy identity because generation 2 owns no legacy coverage.
        con.execute("DELETE FROM snapshot_source_coverage WHERE snapshot_id=2")
        con.execute(
            "UPDATE structure_publications SET normalization_contract_version='contract-v1',"
            "certification_counts_json=? WHERE publication_id='test-publication-2'",
            (
                json.dumps(
                    {
                        "events": 1,
                        "event_tags": 0,
                        "memberships": 2,
                        "group_truth": 1,
                        "markets": 2,
                        "issues": 0,
                        "source_events": 0,
                        "source_markets": 0,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        legacy_universe, legacy_truth = sqlite_store_module._structure_universe_hash(
            con,
            snapshot_id=1,
            generation=False,
        )
        generation_universe, generation_truth = (
            sqlite_store_module._structure_universe_hash(
                con,
                snapshot_id=2,
                generation=True,
            )
        )
        validation_hash = str(
            con.execute(
                "SELECT validation_hash FROM structure_publications "
                "WHERE publication_id='test-publication-2'"
            ).fetchone()[0]
        )
        created_at_ms = 2_001
        receipt_digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=2,
            publication_id="test-publication-2",
            legacy_snapshot_id=1,
            legacy_market_count=2,
            generation_market_count=2,
            legacy_universe_hash=legacy_universe,
            generation_universe_hash=generation_universe,
            legacy_source_truth_hash=legacy_truth,
            generation_source_truth_hash=generation_truth,
            generation_validation_hash=validation_hash,
            created_at_ms=created_at_ms,
        )
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_delete")
        con.execute(
            "DELETE FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=2"
        )
        con.execute(
            "INSERT INTO structure_generation_comparison_receipts("
            "generation_snapshot_id,publication_id,legacy_snapshot_id,"
            "legacy_market_count,generation_market_count,legacy_universe_hash,"
            "generation_universe_hash,legacy_source_truth_hash,"
            "generation_source_truth_hash,generation_validation_hash,created_at_ms,"
            "receipt_digest) VALUES (2,'test-publication-2',1,2,2,?,?,?,?,?,?,?)",
            (
                legacy_universe,
                generation_universe,
                legacy_truth,
                generation_truth,
                validation_hash,
                created_at_ms,
                receipt_digest,
            ),
        )
        counts_json = str(
            con.execute(
                "SELECT committed_counts_json FROM structure_publications "
                "WHERE publication_id='test-publication-2'"
            ).fetchone()[0]
        )
        con.execute(
            "UPDATE current_structure_generation SET snapshot_id=2,"
            "publication_id='test-publication-2',validation_hash=?,counts_json=?,"
            "certification_component='bounded-complete',comparison_receipt_digest=?,"
            "switched_at_ms=2001 WHERE id=1",
            (validation_hash, counts_json, receipt_digest),
        )

    store = SQLiteStore(generation_db)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    assert store.initialize_structure_drift_comparison(now_ms=3_001) == comparison_id
    with sqlite3.connect(generation_db) as con:
        row = con.execute(
            "SELECT legacy_snapshot_id,generation_snapshot_id,publication_id,window_id,"
            "normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash,source_event_count,"
            "source_market_count,source_event_hash,source_market_hash,"
            "source_identity_hash,phase,created_at_ms FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        assert row == (
            1,
            2,
            "test-publication-2",
            "test-window-2",
            "contract-v1",
            receipt_digest,
            validation_hash,
            validation_hash,
            0,
            0,
            None,
            None,
            None,
            "source-events",
            3_000,
        )
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress"
        ).fetchone() == (1,)

    events = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=500,
        now_ms=3_002,
    )
    assert events.component == "source-markets"
    assert events.rows_processed == 0
    markets = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=500,
        now_ms=3_003,
    )
    assert markets.component == "generation-members"
    assert markets.rows_processed == 0
    with sqlite3.connect(generation_db) as con:
        source = con.execute(
            "SELECT source_event_hash,source_market_hash,source_identity_hash,phase "
            "FROM structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
    empty_hash = hashlib.sha256(b"[]").hexdigest()
    assert source[0:2] == (empty_hash, empty_hash)
    assert len(str(source[2])) == 64
    assert source[3] == "generation-members"

    generated = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=1,
        now_ms=3_004,
    )
    assert generated.component == "generation-members"
    assert generated.rows_processed == 1
    generated_again = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=1,
        now_ms=3_005,
    )
    assert generated_again.component == "generation-members"
    assert generated_again.rows_processed == 1
    generation_done = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=1,
        now_ms=3_006,
    )
    assert generation_done.component == "legacy-members"
    legacy = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=1,
        now_ms=3_007,
    )
    assert legacy.component == "legacy-members"
    assert legacy.rows_processed == 1
    legacy_again = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=1,
        now_ms=3_008,
    )
    assert legacy_again.component == "legacy-members"
    assert legacy_again.rows_processed == 1
    legacy_done = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=1,
        now_ms=3_009,
    )
    assert legacy_done.component == "fresh-group-truth"
    with sqlite3.connect(generation_db) as con:
        class_counts_json, class_digests_json, checkpoint_at_ms = con.execute(
            "SELECT class_counts_json,class_digests_json,checkpoint_at_ms FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        class_counts = json.loads(class_counts_json)
        class_digests = json.loads(class_digests_json)
    assert class_counts["class_count:unclassified"] == 2
    assert class_counts["class_count:fresh-source-absent"] == 2
    assert sqlite_store_module.SerializableSHA256.from_json(
        class_digests["class_state:unclassified"]
    )
    assert sqlite_store_module.SerializableSHA256.from_json(
        class_digests["class_state:fresh-source-absent"]
    )
    assert checkpoint_at_ms == 3_009

    fresh_truth = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=1,
        now_ms=3_010,
    )
    assert fresh_truth.component == "fresh-group-truth"
    assert fresh_truth.rows_processed == 1
    truth_done = store.advance_structure_drift_comparison_chunk(
        comparison_id,
        max_rows=1,
        now_ms=3_011,
    )
    assert truth_done.component == "stale"
    with sqlite3.connect(generation_db) as con:
        phase, class_digests_json = con.execute(
            "SELECT phase,class_digests_json FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
    final_digests = json.loads(class_digests_json)
    assert phase == "stale"
    assert len(final_digests["generation_group_truth_hash"]) == 64
    assert (
        final_digests["generation_group_truth_hash"]
        != final_digests["source_group_truth_hash"]
    )


def test_drift_group_truth_reader_is_bounded_and_keyset_stable(
    generation_db: Path,
) -> None:
    store = SQLiteStore(generation_db)
    statements: list[str] = []
    first = store.fetch_structure_drift_group_truth_chunk(
        publication_id="test-publication-2",
        generation_snapshot_id=2,
        after_key=None,
        limit=1,
        trace_callback=statements.append,
    )
    assert len(first) == 1
    assert sum(
        statement.lstrip().upper().startswith("SELECT") for statement in statements
    ) == 2
    assert store.fetch_structure_drift_group_truth_chunk(
        publication_id="test-publication-2",
        generation_snapshot_id=2,
        after_key=(str(first[0][0]), str(first[0][1])),
        limit=1,
    ) == []


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
    assert status["retained_generation_count_lower_bound"] == 3
    assert status["retained_generation_count_is_exact"] is True
    assert status["reclaimable_generation_count_lower_bound"] == 1
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


def test_generation_status_and_compare_execute_only_read_statements(
    generation_db: Path,
) -> None:
    statements: list[str] = []
    status = SQLiteStore(generation_db).structure_generation_status(
        retain_generations=2,
        pressure_probe_limit=8,
        trace_callback=statements.append,
    )
    comparison = sqlite_store_module.compare_current_structure_generation(
        generation_db,
        trace_callback=statements.append,
    )
    assert status["pointer_snapshot_id"] == 1
    assert comparison.generation_snapshot_id == 1
    forbidden = ("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "REPLACE")
    assert not [sql for sql in statements if sql.lstrip().upper().startswith(forbidden)]


def test_generation_status_prefers_next_active_comparison_over_current_receipt(
    tmp_path: Path,
) -> None:
    path = tmp_path / "next-comparison.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="one", point_current=False)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="two", point_current=True)
    with sqlite3.connect(path) as con:
        state = sqlite_store_module.SerializableSHA256.new()
        state.update(b"[")
        con.execute(
            "INSERT INTO structure_generation_comparison_progress("
            "publication_id,generation_snapshot_id,legacy_snapshot_id,legacy_taken_at_ms,"
            "legacy_finished_at_ms,legacy_market_count,phase,row_cursor_json,"
            "digest_state_json,phase_row_count,created_at_ms,checkpoint_at_ms) "
            "VALUES ('test-publication-1',1,1,1000,1001,2,'generation-universe',"
            "NULL,?,0,1000,1001)",
            (state.to_json(),),
        )
    status = SQLiteStore(path).structure_generation_status(retain_generations=2)
    assert status["comparison"]["generation_snapshot_id"] == 1
    assert status["comparison"]["phase"] == "generation-universe"


def test_generation_status_authenticates_pointer_bound_comparison_digest(
    generation_db: Path,
) -> None:
    with sqlite3.connect(generation_db) as con:
        con.execute(
            "UPDATE current_structure_generation SET comparison_receipt_digest=?",
            ("f" * 64,),
        )
    status = SQLiteStore(generation_db).structure_generation_status(retain_generations=2)
    assert status["comparison_authenticated"] is False
    assert "comparison-receipt-digest-mismatch" in status["comparison_mismatch_reasons"]
    from polyarb.http.health import _structure_generation_health_checks

    checks = _structure_generation_health_checks(
        status,
        now_ms=10_000,
        read_mode="legacy",
        publication_sla_s=100,
        pressure_warn_count=4,
        pressure_fail_count=8,
    )
    assert checks["snapshot:structure_generation_comparison"][0]["status"] == "fail"


def test_cleanup_blocked_authentication_is_append_only_and_health_visible(
    tmp_path: Path,
) -> None:
    path = tmp_path / "blocked-observation.db"
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
    SQLiteStore(path).cleanup_structure_generation_evidence(
        retain_generations=2, max_rows=1, now_ms=10_000
    )
    status = SQLiteStore(path).structure_generation_status(retain_generations=2)
    assert status["cleanup_blocked_reason"] == "comparison-receipt-digest-mismatch"
    with sqlite3.connect(path) as con:
        with pytest.raises(sqlite3.IntegrityError, match="cleanup-observation-sealed"):
            con.execute("DELETE FROM structure_generation_cleanup_observations")


@pytest.mark.parametrize(
    ("publication_id", "phase", "blocked_reason"),
    [
        ("test-publication-2", "events", None),
        ("test-publication-1", "markets", None),
        ("test-publication-1", "events", "forged-block"),
    ],
)
def test_cleanup_trigger_rejects_forged_wrong_phase_or_blocked_progress(
    tmp_path: Path,
    publication_id: str,
    phase: str,
    blocked_reason: str | None,
) -> None:
    path = tmp_path / f"forged-{phase}-{blocked_reason}.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="one", point_current=False)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="two", point_current=True)
    with sqlite3.connect(path) as con:
        receipt_digest = con.execute(
            "SELECT receipt_digest FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=1"
        ).fetchone()[0]
        try:
            con.execute(
                "INSERT INTO structure_generation_cleanup_progress("
                "generation_snapshot_id,publication_id,phase,rows_deleted,started_at_ms,"
                "checkpoint_at_ms,blocked_reason,authorization_digest) "
                "VALUES (1,?,?,0,1000,1000,?,?)",
                (publication_id, phase, blocked_reason, receipt_digest),
            )
        except sqlite3.IntegrityError:
            pass
        with pytest.raises(sqlite3.IntegrityError, match="structure-generation-frozen"):
            con.execute(
                "DELETE FROM structure_generation_events WHERE snapshot_id=1"
            )


def test_generation_history_queries_are_bounded_and_indexed(tmp_path: Path) -> None:
    path = tmp_path / "query-plan.db"
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        indexes = {
            row[0] for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        assert "idx_structure_publications_published_history" in indexes
        plans = SQLiteStore(path).structure_generation_query_plans(
            retain_generations=2,
            pressure_probe_limit=8,
        )
        assert "active_comparison" in plans
        assert "pointer_repair" in plans
        assert "active_bootstrap" in plans
    for plan in plans.values():
        detail = " ".join(plan).upper()
        assert "SCAN " not in detail
        assert "USE TEMP B-TREE" not in detail


def test_backfill_uses_bounded_cursors_without_full_table_counts(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bounded-backfill.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="one", point_current=False)
    with sqlite3.connect(path) as con:
        con.execute("DELETE FROM current_structure_generation")
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_delete")
        con.execute("DELETE FROM structure_generation_comparison_receipts")
        con.execute("DELETE FROM structure_publications")
        con.execute("DELETE FROM structure_sync_windows")
        for component in sqlite_store_module._STRUCTURE_COMPONENTS:
            con.execute(f"DELETE FROM structure_generation_{component}")
    statements: list[str] = []
    for _ in range(100):
        checkpoint = SQLiteStore(path).backfill_current_structure_generation(
            max_rows=1,
            trace_callback=statements.append,
        )
        assert checkpoint.copied_rows <= 1
        if checkpoint.complete:
            break
    else:
        raise AssertionError("bounded backfill did not complete")
    assert not [sql for sql in statements if "COUNT(" in sql.upper()]


def test_cleanup_progress_v1_migrates_to_composite_single_slot_authority(
    tmp_path: Path,
) -> None:
    path = tmp_path / "cleanup-progress-v1.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="one", point_current=False)
    with sqlite3.connect(path) as con:
        for component in sqlite_store_module._STRUCTURE_COMPONENTS:
            con.execute(
                f"DROP TRIGGER trg_structure_generation_{component}_frozen_delete"
            )
        con.execute("DROP TABLE structure_generation_cleanup_progress")
        con.execute(
            "CREATE TABLE structure_generation_cleanup_progress("
            "generation_snapshot_id INTEGER PRIMARY KEY,publication_id TEXT UNIQUE,"
            "phase TEXT,rows_deleted INTEGER,started_at_ms INTEGER,"
            "checkpoint_at_ms INTEGER,blocked_reason TEXT)"
        )
        con.execute(
            "INSERT INTO structure_generation_cleanup_progress VALUES "
            "(1,'test-publication-1','events',0,1000,1001,NULL)"
        )

    SQLiteStore(path).init_schema()

    with sqlite3.connect(path) as con:
        columns = {
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_cleanup_progress)"
            )
        }
        assert {"slot", "authorization_digest"} <= columns
        row = con.execute(
            "SELECT slot,generation_snapshot_id,publication_id,authorization_digest "
            "FROM structure_generation_cleanup_progress"
        ).fetchone()
        assert row[:3] == (1, 1, "test-publication-1")
        assert row[3] == con.execute(
            "SELECT receipt_digest FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=1"
        ).fetchone()[0]


def test_pre_task5_missing_receipt_health_is_warn_only_for_fresh_active_repair(
    tmp_path: Path,
) -> None:
    from polyarb.http.health import _structure_generation_health_checks

    path = tmp_path / "pre-task5-health.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _downgrade_to_pre_task5_pointer(path)
    with sqlite3.connect(path) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_delete")
        con.execute("DELETE FROM structure_generation_comparison_receipts")
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        checkpoint_ms = int(
            con.execute(
                "SELECT checkpoint_at_ms FROM structure_generation_comparison_progress"
            ).fetchone()[0]
        )
    status = SQLiteStore(path).structure_generation_status(retain_generations=2)
    assert status["comparison_recoverable_missing_receipt"] is True
    fresh = _structure_generation_health_checks(
        status,
        now_ms=checkpoint_ms + 99_000,
        read_mode="generation",
        publication_sla_s=100,
        pressure_warn_count=4,
        pressure_fail_count=8,
    )
    stale = _structure_generation_health_checks(
        status,
        now_ms=checkpoint_ms + 101_000,
        read_mode="generation",
        publication_sla_s=100,
        pressure_warn_count=4,
        pressure_fail_count=8,
    )
    assert fresh["snapshot:structure_generation_comparison"][0]["status"] == "warn"
    assert stale["snapshot:structure_generation_comparison"][0]["status"] == "fail"


def test_pre_task5_missing_receipt_rejects_wrong_publication_progress_identity(
    tmp_path: Path,
) -> None:
    from polyarb.http.health import _structure_generation_health_checks

    path = tmp_path / "pre-task5-wrong-progress-publication.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="new", point_current=False)
    _downgrade_to_pre_task5_pointer(path)
    with sqlite3.connect(path) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_delete")
        con.execute(
            "DELETE FROM structure_generation_comparison_receipts "
            "WHERE generation_snapshot_id=1"
        )
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        con.execute(
            "UPDATE structure_generation_comparison_progress SET publication_id="
            "'test-publication-2' WHERE generation_snapshot_id=1"
        )

    status = SQLiteStore(path).structure_generation_status(retain_generations=2)
    checks = _structure_generation_health_checks(
        status,
        now_ms=1_001,
        read_mode="generation",
        publication_sla_s=100,
        pressure_warn_count=4,
        pressure_fail_count=8,
    )
    assert status["generation_count_agrees"] is True
    assert status["generation_hash_agrees"] is True
    assert status["comparison_recoverable_missing_receipt"] is False
    assert checks["snapshot:structure_generation_comparison"][0]["status"] == "fail"


@pytest.mark.parametrize(
    ("column", "value"),
    [("digest_state_json", "{}"), ("row_cursor_json", "{}")],
)
def test_pre_task5_missing_receipt_rejects_unresumable_progress_state(
    tmp_path: Path,
    column: str,
    value: str,
) -> None:
    path = tmp_path / f"pre-task5-unresumable-{column}.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _downgrade_to_pre_task5_pointer(path)
    with sqlite3.connect(path) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_delete")
        con.execute("DELETE FROM structure_generation_comparison_receipts")
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        con.execute(
            f"UPDATE structure_generation_comparison_progress SET {column}=?",  # noqa: S608
            (value,),
        )

    status = SQLiteStore(path).structure_generation_status(retain_generations=2)
    assert status["comparison_recoverable_missing_receipt"] is False


@pytest.mark.parametrize(
    ("column", "value", "backfill_error"),
    [
        ("phase_row_count", "oops", None),
        ("legacy_taken_at_ms", 9_999, "structure-comparison-legacy-drift"),
    ],
)
def test_pre_task5_missing_receipt_rejects_invalid_count_or_legacy_pin(
    tmp_path: Path,
    column: str,
    value: object,
    backfill_error: str | None,
) -> None:
    from polyarb.http.health import _structure_generation_health_checks

    path = tmp_path / f"pre-task5-invalid-{column}.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="old", point_current=True)
    _downgrade_to_pre_task5_pointer(path)
    with sqlite3.connect(path) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_delete")
        con.execute("DELETE FROM structure_generation_comparison_receipts")
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        con.execute(
            f"UPDATE structure_generation_comparison_progress SET {column}=?",  # noqa: S608
            (value,),
        )

    status = SQLiteStore(path).structure_generation_status(retain_generations=2)
    checks = _structure_generation_health_checks(
        status,
        now_ms=1_001,
        read_mode="generation",
        publication_sla_s=100,
        pressure_warn_count=4,
        pressure_fail_count=8,
    )
    assert status["comparison_recoverable_missing_receipt"] is False
    assert checks["snapshot:structure_generation_comparison"][0]["status"] == "fail"
    if backfill_error is None:
        with pytest.raises(ValueError):
            SQLiteStore(path).backfill_current_structure_generation(max_rows=1)
    else:
        with pytest.raises(ValueError, match=backfill_error):
            SQLiteStore(path).backfill_current_structure_generation(max_rows=1)


@pytest.mark.parametrize("invalid_count", [True, 1.0, "1", "oops", -1])
def test_comparison_progress_resumability_requires_nonnegative_integer_count(
    invalid_count: object,
) -> None:
    digest = sqlite_store_module.SerializableSHA256.new().to_json()
    progress = (
        "legacy-universe",
        None,
        digest,
        invalid_count,
        1_000,
        None,
        None,
        None,
        1,
        1_000,
        1_001,
        2,
    )
    assert not sqlite_store_module._structure_comparison_progress_is_resumable(
        progress,
        (1, 1_000, 1_001, 2),
    )


@pytest.mark.parametrize("protected_snapshot_id", [3, 2])
def test_cleanup_database_authority_rejects_current_and_rollback_floor(
    tmp_path: Path,
    protected_snapshot_id: int,
) -> None:
    path = tmp_path / f"protected-{protected_snapshot_id}.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="one", point_current=False)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="two", point_current=False)
    _seed_structure_revision(path, snapshot_id=3, market_suffix="three", point_current=True)
    with sqlite3.connect(path) as con:
        con.execute("PRAGMA foreign_keys=ON")
        publication_id, digest = con.execute(
            "SELECT publication_id,receipt_digest FROM "
            "structure_generation_comparison_receipts WHERE generation_snapshot_id=?",
            (protected_snapshot_id,),
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError, match="cleanup-retention-floor"):
            con.execute(
                "INSERT INTO structure_generation_cleanup_progress("
                "generation_snapshot_id,publication_id,phase,rows_deleted,started_at_ms,"
                "checkpoint_at_ms,blocked_reason,authorization_digest) "
                "VALUES (?,?,'events',0,1000,1000,NULL,?)",
                (protected_snapshot_id, publication_id, digest),
            )
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_events WHERE snapshot_id=?",
            (protected_snapshot_id,),
        ).fetchone() == (1,)


def test_cleanup_delete_rechecks_floor_after_authorization(tmp_path: Path) -> None:
    path = tmp_path / "cleanup-floor-recheck.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="one", point_current=False)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="two", point_current=False)
    _seed_structure_revision(path, snapshot_id=3, market_suffix="three", point_current=True)
    with sqlite3.connect(path) as con:
        pub1, digest1 = con.execute(
            "SELECT publication_id,receipt_digest FROM "
            "structure_generation_comparison_receipts WHERE generation_snapshot_id=1"
        ).fetchone()
        con.execute(
            "INSERT INTO structure_generation_cleanup_progress("
            "generation_snapshot_id,publication_id,phase,rows_deleted,started_at_ms,"
            "checkpoint_at_ms,blocked_reason,authorization_digest) "
            "VALUES (1,?,'events',0,1000,1000,NULL,?)",
            (pub1, digest1),
        )
        pub2, counts2, hash2 = con.execute(
            "SELECT publication_id,committed_counts_json,validation_hash FROM "
            "structure_publications WHERE snapshot_id=2"
        ).fetchone()
        cleanup_digest = sqlite_store_module._generation_cleanup_digest(
            generation_snapshot_id=2,
            publication_id=str(pub2),
            component_counts_json=str(counts2),
            generation_validation_hash=str(hash2),
            reclaimed_at_ms=2_000,
        )
        con.execute(
            "INSERT INTO structure_generation_cleanup_receipts VALUES (?,?,?,?,?,?)",
            (2, pub2, counts2, hash2, 2_000, cleanup_digest),
        )
        with pytest.raises(sqlite3.IntegrityError, match="structure-generation-frozen"):
            con.execute("DELETE FROM structure_generation_events WHERE snapshot_id=1")


@pytest.mark.parametrize(
    ("old_publication_id", "old_blocked_reason", "expected_reason"),
    [
        ("test-publication-1", "prior-auth-failure", "prior-auth-failure"),
        (
            "test-publication-2",
            None,
            "cleanup-progress-migration-invalid-binding",
        ),
    ],
)
def test_cleanup_progress_v1_migration_preserves_blocked_and_invalid_diagnostics(
    tmp_path: Path,
    old_publication_id: str,
    old_blocked_reason: str | None,
    expected_reason: str,
) -> None:
    path = tmp_path / f"cleanup-v1-blocked-{old_publication_id}.db"
    SQLiteStore(path).init_schema()
    _seed_structure_revision(path, snapshot_id=1, market_suffix="one", point_current=False)
    _seed_structure_revision(path, snapshot_id=2, market_suffix="two", point_current=True)
    with sqlite3.connect(path) as con:
        for component in sqlite_store_module._STRUCTURE_COMPONENTS:
            con.execute(
                f"DROP TRIGGER trg_structure_generation_{component}_frozen_delete"
            )
        con.execute("DROP TABLE structure_generation_cleanup_progress")
        con.execute(
            "CREATE TABLE structure_generation_cleanup_progress("
            "generation_snapshot_id INTEGER PRIMARY KEY,publication_id TEXT UNIQUE,"
            "phase TEXT,rows_deleted INTEGER,started_at_ms INTEGER,"
            "checkpoint_at_ms INTEGER,blocked_reason TEXT)"
        )
        con.execute(
            "INSERT INTO structure_generation_cleanup_progress VALUES "
            "(1,?,'events',0,1000,1001,?)",
            (old_publication_id, old_blocked_reason),
        )
    SQLiteStore(path).init_schema()
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_cleanup_progress"
        ).fetchone() == (0,)
        observation = con.execute(
            "SELECT state,reason FROM structure_generation_cleanup_observations "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert observation == ("blocked", expected_reason)


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


def test_quarantined_generation_market_never_enters_opportunity_views(
    tmp_path: Path,
) -> None:
    generation_db = tmp_path / "quarantine-opportunity.db"
    SQLiteStore(generation_db).init_schema()
    _seed_structure_revision(
        generation_db,
        snapshot_id=1,
        market_suffix="visible",
        point_current=True,
        quarantine_issue=True,
    )

    opportunities = scan_neg_risk_buy_all(
        generation_db,
        min_edge_bps=0,
        structure_generation_read_mode="generation",
    )
    payload = _read_market_map(
        generation_db,
        event_id=None,
        now_ms=1_001,
        max_age_s=60,
        quote_max_age_s=60,
        structure_generation_read_mode="generation",
    )

    assert all(
        leg.market_id != "quarantined-market"
        for opportunity in opportunities
        for leg in opportunity.legs
    )
    assert "quarantined-market" not in json.dumps(payload)


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
