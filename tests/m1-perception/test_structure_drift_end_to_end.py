from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import polyarb.storage.sqlite_store as sqlite_store_module
from polyarb.perception.structure_drift import (
    project_legacy_compatible_event,
    project_legacy_compatible_market,
)
from polyarb.storage.sqlite_store import SQLiteStore


def _raw_market(
    market_id: str, *, group_id: str, active: bool = True
) -> dict[str, object]:
    return {
        "id": market_id,
        "conditionId": f"condition-{market_id}",
        "clobTokenIds": json.dumps([f"yes-{market_id}", f"no-{market_id}"]),
        "active": active,
        "closed": False,
        "negRisk": True,
        "negRiskMarketID": group_id,
    }


def _drift_store(tmp_path: Path) -> SQLiteStore:
    store = SQLiteStore(tmp_path / "drift-e2e.db")
    store.init_schema()
    main_members = (("shared", True), ("addition", True))
    raw_main = {
        "id": "event-main",
        "slug": "event-main",
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": False,
        "negRiskMarketID": "group-main",
        "markets": [
            {
                "id": market_id,
                "active": active,
                "closed": False,
                "negRiskOther": False,
            }
            for market_id, active in main_members
        ],
    }

    def single_event(
        event_id: str, group_id: str, market_id: str, *, active: bool = True
    ) -> dict[str, object]:
        return {
            "id": event_id,
            "slug": event_id,
            "active": True,
            "closed": False,
            "negRisk": True,
            "enableNegRisk": True,
            "negRiskAugmented": False,
            "negRiskMarketID": group_id,
            "markets": [
                {
                    "id": market_id,
                    "active": active,
                    "closed": False,
                    "negRiskOther": False,
                }
            ],
        }

    raw_events = (
        raw_main,
        single_event(
            "event-current",
            "group-current",
            "current-nontradable",
            active=False,
        ),
        single_event("event-event-only", "group-event-only", "event-only"),
    )
    raw_markets = {
        "shared": _raw_market("shared", group_id="group-main"),
        "addition": _raw_market("addition", group_id="group-main"),
        "current-nontradable": _raw_market(
            "current-nontradable", group_id="group-current", active=False
        ),
        "market-side": _raw_market("market-side", group_id="group-market-a"),
    }
    complete_ids = frozenset(raw_markets)
    event_projections = tuple(
        project_legacy_compatible_event(
            raw_event,
            event_source_ordinal=ordinal,
            complete_market_ids=complete_ids,
        )
        for ordinal, raw_event in enumerate(raw_events, 1)
    )
    market_projections = {
        market_id: project_legacy_compatible_market(
            raw,
            event_ids=(
                ()
                if market_id == "market-side"
                else ("event-current",)
                if market_id == "current-nontradable"
                else ("event-main",)
            ),
            taken_at_ms=2_000,
        )
        for market_id, raw in raw_markets.items()
    }
    with sqlite3.connect(store.db_path) as con:
        con.executemany(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (?,?,?,'full',?,1,'structure','legacy','ok',1,'')",
            ((1, 1_000, 1_001, 5), (2, 2_000, 2_001, 3)),
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage(snapshot_id,completed,market_items,"
            "event_items) VALUES (1,1,5,1)"
        )
        legacy_members = (
            ("event-main", "group-main", "shared"),
            ("event-current", "group-current", "current-nontradable"),
            ("event-event-only", "group-event-only", "event-only"),
            ("event-market-a", "group-market-a", "market-side"),
            ("event-fresh", "group-fresh", "fresh-absent"),
        )
        con.executemany(
            "INSERT INTO event_market_memberships(snapshot_id,event_id,"
            "neg_risk_market_id,market_id,member_kind,active,closed) "
            "VALUES (1,?,?,?,'named',1,0)",
            legacy_members,
        )
        for event_id, group_id, market_id in legacy_members:
            legacy_hash = hashlib.sha256(
                json.dumps(
                    [(event_id, group_id, market_id, "named", True, False)],
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            con.execute(
                "INSERT INTO neg_risk_group_truth(snapshot_id,event_id,neg_risk_market_id,"
                "neg_risk_type,expected_member_count,active_named_count,membership_hash,"
                "quality) VALUES (1,?,?,'standard',1,1,?,"
                "'complete-supported')",
                (event_id, group_id, legacy_hash),
            )
        con.executemany(
            "INSERT INTO markets(snapshot_id,market_id,condition_id,yes_token_id,"
            "no_token_id,active,closed,neg_risk,neg_risk_market_id,fetched_at_ms,"
            "incomplete,event_id) VALUES (1,?,?,?, ?,1,0,1,?,1000,0,?)",
            (
                (
                    market_id,
                    f"condition-{market_id}",
                    f"yes-{market_id}",
                    f"no-{market_id}",
                    group_id,
                    event_id,
                )
                for event_id, group_id, market_id in legacy_members
            ),
        )
        for projection in event_projections:
            con.executemany(
                "INSERT INTO structure_generation_memberships(snapshot_id,event_id,"
                "neg_risk_market_id,market_id,member_kind,active,closed) "
                "VALUES (2,?,?,?,?,?,?)",
                (
                    (
                        member.event_id,
                        member.group_id,
                        member.market_id,
                        member.member_kind,
                        int(member.active),
                        int(member.closed),
                    )
                    for member in projection.members
                ),
            )
            con.executemany(
                "INSERT INTO structure_generation_group_truth(snapshot_id,event_id,"
                "neg_risk_market_id,neg_risk_type,expected_member_count,"
                "active_named_count,membership_hash,quality,reason) "
                "VALUES (2,?,?,?,?,?,?,?,?)",
                (
                    (
                        truth.event_id,
                        truth.group_id,
                        truth.neg_risk_type,
                        truth.expected_member_count,
                        truth.active_named_count,
                        truth.membership_hash,
                        truth.quality,
                        truth.reason,
                    )
                    for truth in projection.truths
                ),
            )
        market_columns = (
            "market_id,condition_id,slug,question,yes_token_id,no_token_id,mid_price,"
            "liquidity_usd,volume_usd,best_bid_price,best_bid_size,best_ask_price,"
            "best_ask_size,end_time_ms,active,closed,neg_risk,neg_risk_market_id,"
            "fetched_at_ms,page_fetched_at_ms,incomplete,event_id"
        )
        for projection in market_projections.values():
            if projection.row is None:
                continue
            con.execute(
                "INSERT INTO structure_generation_markets(snapshot_id,"
                + market_columns
                + ") VALUES (2,"
                + ",".join("?" for _ in projection.row)
                + ")",
                tuple(projection.row.values()),
            )
        con.execute(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms,"
            "published_snapshot_id) VALUES ('window-2','open',2000,2001,NULL)"
        )
        con.executemany(
            "INSERT INTO structure_sync_event_staging(window_id,event_id,payload_json,"
            "source_ordinal) VALUES ('window-2',?,?,?)",
            (
                (str(raw_event["id"]), json.dumps(raw_event), ordinal)
                for ordinal, raw_event in enumerate(raw_events, 1)
            ),
        )
        relations = [
            (str(member["id"]), str(raw_event["id"]), ordinal)
            for ordinal, raw_event in enumerate(raw_events, 1)
            for member in raw_event["markets"]
        ]
        con.executemany(
            "INSERT INTO structure_sync_event_market_staging(window_id,market_id,"
            "event_id,source_ordinal) VALUES ('window-2',?,?,?)",
            relations,
        )
        con.execute(
            "UPDATE structure_sync_windows SET status='events_complete' WHERE id='window-2'"
        )
        con.executemany(
            "INSERT INTO structure_sync_market_staging(window_id,market_id,payload_json,"
            "source_ordinal) VALUES ('window-2',?,?,?)",
            (
                (market_id, json.dumps(raw), ordinal)
                for ordinal, (market_id, raw) in enumerate(raw_markets.items(), 1)
            ),
        )
        con.execute(
            "UPDATE structure_sync_windows SET status='published',"
            "published_snapshot_id=2 WHERE id='window-2'"
        )
        cert = "a" * 64
        cert_counts = json.dumps(
            {"source_events": 3, "source_markets": 4},
            sort_keys=True,
            separators=(",", ":"),
        )
        con.execute(
            "INSERT INTO structure_publications(publication_id,window_id,snapshot_id,"
            "status,normalization_contract_version,expected_counts_json,"
            "committed_counts_json,validation_hash,certification_component,"
            "certification_hash,certification_counts_json,created_at_ms,checkpoint_at_ms) "
            "VALUES ('publication-2','window-2',2,'published','contract-v1','{}','{}',"
            "?,'bounded-complete',?,?,2000,2001)",
            (cert, cert, cert_counts),
        )
        legacy_universe, legacy_truth = sqlite_store_module._structure_universe_hash(
            con, snapshot_id=1, generation=False
        )
        generation_universe, generation_truth = (
            sqlite_store_module._structure_universe_hash(
                con, snapshot_id=2, generation=True
            )
        )
        exact_digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=2,
            publication_id="publication-2",
            legacy_snapshot_id=1,
            legacy_market_count=5,
            generation_market_count=3,
            legacy_universe_hash=legacy_universe,
            generation_universe_hash=generation_universe,
            legacy_source_truth_hash=legacy_truth,
            generation_source_truth_hash=generation_truth,
            generation_validation_hash=cert,
            created_at_ms=2_001,
        )
        con.execute(
            "INSERT INTO structure_generation_comparison_receipts("
            "generation_snapshot_id,publication_id,legacy_snapshot_id,"
            "legacy_market_count,generation_market_count,legacy_universe_hash,"
            "generation_universe_hash,legacy_source_truth_hash,"
            "generation_source_truth_hash,generation_validation_hash,created_at_ms,"
            "receipt_digest) VALUES (2,'publication-2',1,5,3,?,?,?,?,?,?,?)",
            (
                legacy_universe,
                generation_universe,
                legacy_truth,
                generation_truth,
                cert,
                2_001,
                exact_digest,
            ),
        )
        con.execute(
            "INSERT INTO current_structure_generation(id,snapshot_id,publication_id,"
            "validation_hash,counts_json,certification_component,"
            "comparison_receipt_digest,switched_at_ms) VALUES "
            "(1,2,'publication-2',?,'{}','bounded-complete',?,2001)",
            (cert, exact_digest),
        )
    return store


def _reshape_as_production_845_848(store: SQLiteStore) -> None:
    """Retain fixture semantics while matching the production identity topology."""
    with sqlite3.connect(store.db_path) as con:
        con.execute("PRAGMA foreign_keys=OFF")
        for (trigger_name,) in con.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall():
            con.execute(f'DROP TRIGGER "{trigger_name}"')
        con.execute("UPDATE snapshots SET id=845 WHERE id=1")
        con.execute("UPDATE snapshots SET id=848 WHERE id=2")
        con.execute("UPDATE snapshot_source_coverage SET snapshot_id=845")
        for table in (
            "event_market_memberships",
            "neg_risk_group_truth",
            "markets",
        ):
            con.execute(f"UPDATE {table} SET snapshot_id=845 WHERE snapshot_id=1")
        for table in (
            "structure_generation_memberships",
            "structure_generation_group_truth",
            "structure_generation_markets",
        ):
            con.execute(f"UPDATE {table} SET snapshot_id=848 WHERE snapshot_id=2")
        con.execute(
            "UPDATE structure_sync_windows SET id='window-97b',"
            "published_snapshot_id=848 WHERE id='window-2'"
        )
        for table in (
            "structure_sync_event_staging",
            "structure_sync_event_market_staging",
            "structure_sync_market_staging",
        ):
            con.execute(
                f"UPDATE {table} SET window_id='window-97b' WHERE window_id='window-2'"
            )
        con.execute(
            "UPDATE structure_publications SET publication_id='publication-848',"
            "window_id='window-97b',snapshot_id=848 WHERE "
            "publication_id='publication-2'"
        )
        receipt = con.execute(
            "SELECT legacy_market_count,generation_market_count,legacy_universe_hash,"
            "generation_universe_hash,legacy_source_truth_hash,"
            "generation_source_truth_hash,generation_validation_hash,created_at_ms "
            "FROM structure_generation_comparison_receipts"
        ).fetchone()
        exact_digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=848,
            publication_id="publication-848",
            legacy_snapshot_id=845,
            legacy_market_count=int(receipt[0]),
            generation_market_count=int(receipt[1]),
            legacy_universe_hash=str(receipt[2]),
            generation_universe_hash=str(receipt[3]),
            legacy_source_truth_hash=str(receipt[4]),
            generation_source_truth_hash=str(receipt[5]),
            generation_validation_hash=str(receipt[6]),
            created_at_ms=int(receipt[7]),
        )
        con.execute(
            "UPDATE structure_generation_comparison_receipts SET "
            "generation_snapshot_id=848,publication_id='publication-848',"
            "legacy_snapshot_id=845,receipt_digest=?",
            (exact_digest,),
        )
        con.execute(
            "UPDATE current_structure_generation SET snapshot_id=848,"
            "publication_id='publication-848',comparison_receipt_digest=? WHERE id=1",
            (exact_digest,),
        )
    store.init_schema()


def test_drift_v2_schema_binds_algorithm_reason_and_member_scan_indexes(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "drift-v2-schema.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        progress_columns = {
            str(row[1]): (int(row[3]), row[4])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        }
        receipt_columns = {
            str(row[1]): (int(row[3]), row[4])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_receipts)"
            )
        }
        indexes = {
            str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }

    assert {"hash_algorithm", "terminal_reason"} <= set(progress_columns)
    assert "hash_algorithm" in receipt_columns
    assert progress_columns["hash_algorithm"] == (
        1,
        "'serializable-sha256-v1'",
    )
    assert receipt_columns["hash_algorithm"] == (
        1,
        "'serializable-sha256-v1'",
    )
    assert "idx_structure_generation_memberships_drift_scan" in indexes
    assert "idx_event_market_memberships_drift_scan" in indexes


@pytest.mark.parametrize("failure_point", ("index", "analyze"))
def test_drift_v2_schema_startup_failure_reinitializes_without_business_row_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    store = _drift_store(tmp_path)
    business_tables = (
        "snapshots",
        "structure_publications",
        "structure_generation_memberships",
        "event_market_memberships",
        "markets",
    )
    with sqlite3.connect(store.db_path) as con:
        business_before = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in business_tables
        }
        if failure_point == "index":
            con.execute(
                "DROP INDEX idx_structure_generation_memberships_drift_scan"
            )

    original_connect = store._connect_writer

    def connect_with_startup_fault(
        *, timeout_s: float | None = None
    ) -> sqlite3.Connection:
        con = original_connect(timeout_s=timeout_s)

        def deny_selected_operation(
            action: int,
            arg1: str | None,
            _arg2: str | None,
            _database: str | None,
            _trigger: str | None,
        ) -> int:
            index_failure = (
                failure_point == "index"
                and action == sqlite3.SQLITE_CREATE_INDEX
                and arg1 == "idx_structure_generation_memberships_drift_scan"
            )
            analyze_failure = (
                failure_point == "analyze" and action == sqlite3.SQLITE_ANALYZE
            )
            return sqlite3.SQLITE_DENY if index_failure or analyze_failure else sqlite3.SQLITE_OK

        con.set_authorizer(deny_selected_operation)
        return con

    monkeypatch.setattr(store, "_connect_writer", connect_with_startup_fault)
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        store.init_schema()
    monkeypatch.setattr(store, "_connect_writer", original_connect)

    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        business_after = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in business_tables
        }
        indexes = {
            str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        analyzed_indexes = {
            str(row[0])
            for row in con.execute(
                "SELECT idx FROM sqlite_stat1 WHERE idx IN (?,?)",
                (
                    "idx_structure_generation_memberships_drift_scan",
                    "idx_event_market_memberships_drift_scan",
                ),
            )
        }

    assert business_after == business_before
    assert {
        "idx_structure_generation_memberships_drift_scan",
        "idx_event_market_memberships_drift_scan",
    } <= indexes
    assert {
        "idx_structure_generation_memberships_drift_scan",
        "idx_event_market_memberships_drift_scan",
    } <= analyzed_indexes


_V1_PROGRESS_COLUMNS = (
    "comparison_id",
    "legacy_snapshot_id",
    "generation_snapshot_id",
    "publication_id",
    "window_id",
    "normalization_contract_version",
    "exact_receipt_digest",
    "pointer_validation_hash",
    "generation_certification_hash",
    "source_event_count",
    "source_market_count",
    "source_event_hash",
    "source_market_hash",
    "source_identity_hash",
    "phase",
    "row_cursor_json",
    "digest_state_json",
    "class_counts_json",
    "class_digests_json",
    "created_at_ms",
    "checkpoint_at_ms",
)
_V1_RECEIPT_COLUMNS = tuple(
    field
    for field in sqlite_store_module._STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS
    if field != "hash_algorithm"
) + ("receipt_digest",)


def _downgrade_drift_tables_to_v1_shape(store: SQLiteStore) -> None:
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP INDEX IF EXISTS idx_structure_drift_progress_active")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_update")
        con.execute("DROP TRIGGER IF EXISTS trg_structure_drift_receipt_delete")
        progress_columns = ",".join(_V1_PROGRESS_COLUMNS)
        receipt_columns = ",".join(_V1_RECEIPT_COLUMNS)
        con.execute(
            "CREATE TABLE structure_generation_drift_progress_v1 AS SELECT "
            f"{progress_columns} FROM structure_generation_drift_progress"
        )
        con.execute("DROP TABLE structure_generation_drift_progress")
        con.execute(
            "ALTER TABLE structure_generation_drift_progress_v1 "
            "RENAME TO structure_generation_drift_progress"
        )
        con.execute(
            "CREATE TABLE structure_generation_drift_receipts_v1 AS SELECT "
            f"{receipt_columns} FROM structure_generation_drift_receipts"
        )
        con.execute("DROP TABLE structure_generation_drift_receipts")
        con.execute(
            "ALTER TABLE structure_generation_drift_receipts_v1 "
            "RENAME TO structure_generation_drift_receipts"
        )


def test_drift_v2_migration_rolls_back_injected_crash_and_reinitializes(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _downgrade_drift_tables_to_v1_shape(store)
    with sqlite3.connect(store.db_path) as con:
        immutable_before = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "snapshots",
                "structure_publications",
                "current_structure_generation",
                "structure_sync_event_staging",
                "structure_sync_market_staging",
                "markets",
            )
        }
        migrate = getattr(
            sqlite_store_module, "_migrate_structure_drift_hash_v2"
        )

        def fail_after_progress_rename(step: str) -> None:
            if step == "after-progress-rename":
                raise RuntimeError("injected-after-progress-rename")

        with pytest.raises(RuntimeError, match="injected-after-progress-rename"):
            migrate(con, fault_hook=fail_after_progress_rename)
        assert "hash_algorithm" not in {
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        }

    store.init_schema()
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        defaults = {
            table: {
                str(row[1]): (int(row[3]), row[4])
                for row in con.execute(f"PRAGMA table_info({table})")
            }["hash_algorithm"]
            for table in (
                "structure_generation_drift_progress",
                "structure_generation_drift_receipts",
            )
        }
        migrated = con.execute(
            "SELECT comparison_id,hash_algorithm,phase,terminal_reason "
            "FROM structure_generation_drift_progress"
        ).fetchall()
        immutable_after = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in immutable_before
        }
        old_shape_row = con.execute(
            "SELECT " + ",".join(_V1_PROGRESS_COLUMNS) + " FROM "
            "structure_generation_drift_progress"
        ).fetchone()
        con.execute("DELETE FROM structure_generation_drift_progress")
        con.execute(
            "INSERT INTO structure_generation_drift_progress("
            + ",".join(_V1_PROGRESS_COLUMNS)
            + ") VALUES ("
            + ",".join("?" for _ in _V1_PROGRESS_COLUMNS)
            + ")",
            old_shape_row,
        )
        omitted_column_algorithm = con.execute(
            "SELECT hash_algorithm FROM structure_generation_drift_progress"
        ).fetchone()
    assert migrated == [
        (comparison_id, "serializable-sha256-v1", "source-events", None)
    ]
    assert immutable_after == immutable_before
    assert set(defaults.values()) == {(1, "'serializable-sha256-v1'")}
    assert omitted_column_algorithm == ("serializable-sha256-v1",)


def test_drift_v2_migration_writer_lock_leaves_v1_schema_reinitializable(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    store.initialize_structure_drift_comparison(now_ms=3_000)
    _downgrade_drift_tables_to_v1_shape(store)
    blocker = sqlite3.connect(store.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            SQLiteStore(store.db_path, writer_timeout_s=0.01).init_schema()
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    with sqlite3.connect(store.db_path) as con:
        assert "hash_algorithm" not in {
            str(row[1])
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        }
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress"
        ).fetchone() == (1,)
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT hash_algorithm FROM structure_generation_drift_progress"
        ).fetchone() == ("serializable-sha256-v1",)


def test_active_v1_progress_is_atomically_superseded_by_cursor_zero_v2(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    store.initialize_structure_drift_comparison(now_ms=3_000)
    v1_comparison_id = "b" * 64
    with sqlite3.connect(store.db_path) as con:
        v1_state = sqlite_store_module.SerializableSHA256.new()
        v1_state.update(b"[")
        con.execute(
            "UPDATE structure_generation_drift_progress SET comparison_id=?,"
            "hash_algorithm='serializable-sha256-v1',digest_state_json=?",
            (v1_comparison_id, v1_state.to_json()),
        )
        data_plane_before = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "current_structure_generation",
                "structure_publications",
                "structure_sync_event_staging",
                "structure_sync_market_staging",
                "events",
                "event_market_memberships",
                "neg_risk_group_truth",
                "markets",
            )
        }

    v2_comparison_id = store.initialize_structure_drift_comparison(now_ms=3_001)

    with sqlite3.connect(store.db_path) as con:
        progress = {
            str(row[1]): row
            for row in con.execute(
            "SELECT comparison_id,hash_algorithm,phase,terminal_reason,"
            "row_cursor_json,digest_state_json FROM "
            "structure_generation_drift_progress"
            )
        }
        data_plane_after = {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in data_plane_before
        }
    assert v2_comparison_id != v1_comparison_id
    assert progress["serializable-sha256-v1"][:5] == (
        v1_comparison_id,
        "serializable-sha256-v1",
        "stale",
        "drift-hash-algorithm-superseded",
        None,
    )
    assert progress["row-chain-sha256-v2"][:5] == (
        v2_comparison_id,
        "row-chain-sha256-v2",
        "source-events",
        None,
        None,
    )
    v2_state = json.loads(str(progress["row-chain-sha256-v2"][5]))
    assert v2_state["algorithm"] == "row-chain-sha256-v2"
    assert v2_state["domain"] == "source-event"
    assert v2_state["count"] == 0
    assert data_plane_after == data_plane_before
    status = store.structure_generation_drift_status()
    assert status["progress_id"] == v2_comparison_id
    assert status["phase"] == "source-events"
    assert status["hash_algorithm"] == "row-chain-sha256-v2"


def test_v2_insert_failure_rolls_back_v1_supersession(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    store.initialize_structure_drift_comparison(now_ms=3_000)
    v1_comparison_id = "c" * 64
    with sqlite3.connect(store.db_path) as con:
        v1_state = sqlite_store_module.SerializableSHA256.new()
        v1_state.update(b"[")
        con.execute(
            "UPDATE structure_generation_drift_progress SET comparison_id=?,"
            "hash_algorithm='serializable-sha256-v1',digest_state_json=?",
            (v1_comparison_id, v1_state.to_json()),
        )
        con.execute(
            "CREATE TRIGGER reject_v2_progress BEFORE INSERT ON "
            "structure_generation_drift_progress WHEN "
            "NEW.hash_algorithm='row-chain-sha256-v2' BEGIN SELECT "
            "RAISE(ABORT,'injected-v2-insert-failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected-v2-insert-failure"):
        store.initialize_structure_drift_comparison(now_ms=3_001)

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT comparison_id,hash_algorithm,phase,terminal_reason FROM "
            "structure_generation_drift_progress"
        ).fetchall() == [
            (
                v1_comparison_id,
                "serializable-sha256-v1",
                "source-events",
                None,
            )
        ]


def _install_sealed_drift_authority(store: SQLiteStore, comparison_id: str) -> None:
    digest_fields = sqlite_store_module._STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS
    with sqlite3.connect(store.db_path) as con:
        progress = con.execute(
            "SELECT legacy_snapshot_id,generation_snapshot_id,publication_id,"
            "window_id,normalization_contract_version,exact_receipt_digest,"
            "pointer_validation_hash,generation_certification_hash FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        exact = con.execute(
            "SELECT snapshot.taken_at_ms,snapshot.finished_at_ms,"
            "receipt.legacy_market_count,receipt.legacy_universe_hash,"
            "receipt.legacy_source_truth_hash FROM "
            "structure_generation_comparison_receipts receipt JOIN snapshots snapshot "
            "ON snapshot.id=receipt.legacy_snapshot_id WHERE "
            "receipt.generation_snapshot_id=? AND receipt.publication_id=?",
            (progress[1], progress[2]),
        ).fetchone()
        source_hashes = ("1" * 64, "2" * 64, "3" * 64)
        class_counts_json = json.dumps(
            {"overlap-conflict": 0, "unclassified": 0},
            sort_keys=True,
            separators=(",", ":"),
        )
        payload: dict[str, object] = {
            "comparison_id": comparison_id,
            "hash_algorithm": "row-chain-sha256-v2",
            "legacy_snapshot_id": int(progress[0]),
            "legacy_taken_at_ms": int(exact[0]),
            "legacy_finished_at_ms": int(exact[1]),
            "legacy_market_count": int(exact[2]),
            "legacy_universe_hash": str(exact[3]),
            "legacy_source_truth_hash": str(exact[4]),
            "generation_snapshot_id": int(progress[1]),
            "publication_id": str(progress[2]),
            "window_id": str(progress[3]),
            "published_snapshot_id": int(progress[1]),
            "normalization_contract_version": str(progress[4]),
            "exact_receipt_digest": str(progress[5]),
            "pointer_validation_hash": str(progress[6]),
            "generation_certification_hash": str(progress[7]),
            "source_event_count": 3,
            "source_market_count": 4,
            "source_event_hash": source_hashes[0],
            "source_market_hash": source_hashes[1],
            "source_identity_hash": source_hashes[2],
            "projection_universe_hash": "4" * 64,
            "projection_group_truth_hash": "5" * 64,
            "generation_universe_hash": "6" * 64,
            "generation_group_truth_hash": "7" * 64,
            "class_counts_json": class_counts_json,
            "class_digests_json": "{}",
            "legacy_reconstruction_root": "8" * 64,
            "generation_reconstruction_root": "9" * 64,
            "overlap_conflict_count": 0,
            "unclassified_count": 0,
            "created_at_ms": 3_001,
        }
        receipt_digest = sqlite_store_module._structure_drift_receipt_digest(
            {field: payload[field] for field in digest_fields}
        )
        insert_fields = (
            digest_fields
            if "hash_algorithm" in digest_fields
            else ("hash_algorithm", *digest_fields)
        )
        con.execute(
            "INSERT INTO structure_generation_drift_receipts("
            + ",".join(insert_fields)
            + ",receipt_digest) VALUES ("
            + ",".join("?" for _ in range(len(insert_fields) + 1))
            + ")",
            (*(payload[field] for field in insert_fields), receipt_digest),
        )
        con.execute(
            "UPDATE structure_generation_drift_progress SET phase='sealed',"
            "source_event_hash=?,source_market_hash=?,source_identity_hash=?,"
            "class_counts_json=?,class_digests_json=? WHERE comparison_id=?",
            (
                *source_hashes,
                json.dumps(
                    {
                        "class_count:overlap-conflict": 0,
                        "class_count:unclassified": 0,
                    }
                ),
                json.dumps({"receipt_digest": receipt_digest}),
                comparison_id,
            ),
        )


def _rewrite_drift_receipt(
    store: SQLiteStore,
    comparison_id: str,
    **changes: object,
) -> None:
    digest_fields = sqlite_store_module._STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_drift_receipt_update")
        row = con.execute(
            "SELECT " + ",".join(digest_fields) + " FROM "
            "structure_generation_drift_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
        assert row is not None
        payload = dict(zip(digest_fields, row, strict=True))
        payload.update({key: value for key, value in changes.items() if key in payload})
        receipt_digest = hashlib.sha256(
            json.dumps(
                tuple(payload[field] for field in digest_fields),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        assignments = [f"{key}=?" for key in changes]
        con.execute(
            "UPDATE structure_generation_drift_receipts SET "
            + ",".join((*assignments, "receipt_digest=?"))
            + " WHERE comparison_id=?",
            (*changes.values(), receipt_digest, comparison_id),
        )
        con.execute(
            "UPDATE structure_generation_drift_progress SET class_digests_json="
            "json_set(class_digests_json,'$.receipt_digest',?) "
            "WHERE comparison_id=?",
            (receipt_digest, comparison_id),
        )


def _make_exact_receipt_authoritative(store: SQLiteStore) -> None:
    with sqlite3.connect(store.db_path) as con:
        con.execute("DROP TRIGGER trg_structure_comparison_receipt_update")
        row = con.execute(
            "SELECT generation_snapshot_id,publication_id,legacy_snapshot_id,"
            "legacy_market_count,legacy_universe_hash,legacy_source_truth_hash,"
            "generation_validation_hash,created_at_ms FROM "
            "structure_generation_comparison_receipts"
        ).fetchone()
        assert row is not None
        digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=int(row[0]),
            publication_id=str(row[1]),
            legacy_snapshot_id=int(row[2]),
            legacy_market_count=int(row[3]),
            generation_market_count=int(row[3]),
            legacy_universe_hash=str(row[4]),
            generation_universe_hash=str(row[4]),
            legacy_source_truth_hash=str(row[5]),
            generation_source_truth_hash=str(row[5]),
            generation_validation_hash=str(row[6]),
            created_at_ms=int(row[7]),
        )
        con.execute(
            "UPDATE structure_generation_comparison_receipts SET "
            "generation_market_count=legacy_market_count,"
            "generation_universe_hash=legacy_universe_hash,"
            "generation_source_truth_hash=legacy_source_truth_hash,receipt_digest=?",
            (digest,),
        )
        con.execute(
            "UPDATE current_structure_generation SET comparison_receipt_digest=? "
            "WHERE id=1",
            (digest,),
        )


def test_sealed_v1_receipt_cannot_authorize_v2_progress(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)
    assert store.structure_generation_drift_status()["authorized"] is True

    _rewrite_drift_receipt(
        store,
        comparison_id,
        hash_algorithm="serializable-sha256-v1",
    )

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"


def test_sealed_v1_progress_and_receipt_are_not_v2_authority(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)
    _rewrite_drift_receipt(
        store,
        comparison_id,
        hash_algorithm="serializable-sha256-v1",
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE structure_generation_drift_progress SET "
            "hash_algorithm='serializable-sha256-v1' WHERE comparison_id=?",
            (comparison_id,),
        )

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["authorization_mode"] == "none"
    assert status["reason"] == "structure-drift-progress-missing"


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("pointer_validation_hash", "b" * 64),
        ("source_identity_hash", "c" * 64),
    ),
)
def test_sealed_receipt_pointer_and_source_identity_drift_fail_closed(
    tmp_path: Path,
    field: str,
    replacement: str,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)

    _rewrite_drift_receipt(store, comparison_id, **{field: replacement})

    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["reason"] == "structure-drift-receipt-invalid"


def test_exact_authorization_is_independent_of_drift_receipt_algorithm(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    _install_sealed_drift_authority(store, comparison_id)
    _rewrite_drift_receipt(
        store,
        comparison_id,
        hash_algorithm="serializable-sha256-v1",
    )
    _make_exact_receipt_authoritative(store)

    status = store.structure_generation_drift_status()
    assert status["authorized"] is True
    assert status["authorization_mode"] == "exact"
    assert status["phase"] == "exact"


def test_nonempty_drift_state_machine_seals_all_partitions_and_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    observed_phases: set[str] = set()
    for now_ms in range(3_001, 3_100):
        chunk = store.advance_structure_drift_comparison_chunk(
            comparison_id, max_rows=1, now_ms=now_ms
        )
        assert chunk.rows_processed <= 1
        observed_phases.add(str(chunk.component))
        if chunk.component in {"sealed", "stale"}:
            break
    else:
        pytest.fail("drift comparison did not seal")
    with sqlite3.connect(store.db_path) as con:
        debug_row = con.execute(
            "SELECT class_counts_json,class_digests_json FROM "
            "structure_generation_drift_progress WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
    debug_counts = json.loads(debug_row[0])
    debug_digests = json.loads(debug_row[1])
    assert debug_counts.get("projection_member_count") == debug_counts.get(
        "generation_member_count"
    )
    assert debug_digests.get("projection_member_root") == debug_digests.get(
        "generation_member_root"
    )
    assert debug_digests.get("source_group_truth_hash") == debug_digests.get(
        "generation_group_truth_hash"
    )
    assert debug_counts.get("class_count:overlap-conflict", 0) == 0
    assert debug_counts.get("class_count:unclassified", 0) == 0
    assert chunk.component == "sealed"
    assert {
        "source-events",
        "source-markets",
        "generation-members",
        "legacy-members",
        "fresh-group-truth",
        "sealed",
    } <= observed_phases
    with sqlite3.connect(store.db_path) as con:
        row = con.execute(
            "SELECT "
            + ",".join(sqlite_store_module._STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS)
            + ",receipt_digest,class_counts_json FROM "
            "structure_generation_drift_receipts WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone()
    assert row is not None
    field_count = len(sqlite_store_module._STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS)
    payload = dict(
        zip(
            sqlite_store_module._STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS,
            row[:field_count],
            strict=True,
        )
    )
    assert row[field_count] == sqlite_store_module._structure_drift_receipt_digest(
        payload
    )
    classes = json.loads(row[field_count + 1])
    assert classes == {
        "current-nontradable": 1,
        "event-only-quarantine": 1,
        "fresh-addition": 1,
        "fresh-source-absent": 1,
        "market-side-quarantine": 1,
        "overlap-conflict": 0,
        "shared": 1,
        "unclassified": 0,
    }
    status = store.structure_generation_drift_status()
    assert status["authorized"] is True
    assert status["authorization_mode"] == "drift-safe-sealed"
    assert status["phase"] == "sealed"
    assert status["receipt_digest"] == row[field_count]
    from polyarb.snapshot import cli as cli_module

    monkeypatch.setattr(
        cli_module,
        "load_settings",
        lambda: SimpleNamespace(db_path=store.db_path),
    )
    cli_result = CliRunner().invoke(
        cli_module.app, ["structure-generation-drift-compare"]
    )
    assert cli_result.exit_code == 0, cli_result.stdout
    assert json.loads(cli_result.stdout)["authorization_mode"] == "drift-safe-sealed"
    substituted = dict(payload)
    substituted["projection_universe_hash"] = "f" * 64
    assert (
        sqlite_store_module._structure_drift_receipt_digest(substituted)
        != row[field_count]
    )
    with sqlite3.connect(store.db_path) as con:
        with pytest.raises(sqlite3.IntegrityError, match="receipt-sealed"):
            con.execute(
                "UPDATE structure_generation_drift_receipts SET created_at_ms=9999 "
                "WHERE comparison_id=?",
                (comparison_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="receipt-sealed"):
            con.execute(
                "DELETE FROM structure_generation_drift_receipts WHERE comparison_id=?",
                (comparison_id,),
            )
        con.execute("DROP TRIGGER trg_structure_drift_receipt_update")
        con.execute(
            "UPDATE structure_generation_drift_receipts SET "
            "projection_universe_hash=? WHERE comparison_id=?",
            ("f" * 64, comparison_id),
        )
    tampered_status = store.structure_generation_drift_status()
    assert tampered_status["authorized"] is False
    assert tampered_status["reason"] == "structure-drift-receipt-invalid"


def test_drift_pointer_race_fails_before_next_checkpoint(tmp_path: Path) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE current_structure_generation SET validation_hash=? WHERE id=1",
            ("b" * 64,),
        )
    with pytest.raises(ValueError, match="structure-drift-current-identity-invalid"):
        store.advance_structure_drift_comparison_chunk(
            comparison_id, max_rows=1, now_ms=3_001
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT phase,checkpoint_at_ms FROM structure_generation_drift_progress "
            "WHERE comparison_id=?",
            (comparison_id,),
        ).fetchone() == ("source-events", 3_000)
    status = store.structure_generation_drift_status()
    assert status["authorized"] is False
    assert status["authorization_mode"] == "none"


@pytest.mark.asyncio
async def test_actual_drift_child_parser_resumes_committed_chunk(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import run_structure_drift_in_subprocess

    store = _drift_store(tmp_path)
    first = await run_structure_drift_in_subprocess(
        db_path=store.db_path,
        max_rows=1,
        max_chunks=1,
        max_elapsed_s=5.0,
        timeout_s=10.0,
    )
    second = await run_structure_drift_in_subprocess(
        db_path=store.db_path,
        max_rows=1,
        max_chunks=1,
        max_elapsed_s=5.0,
        timeout_s=10.0,
    )

    assert first.chunks_processed == 1
    assert second.chunks_processed == 1
    with sqlite3.connect(store.db_path) as con:
        phase, counts_json = con.execute(
            "SELECT phase,class_counts_json FROM structure_generation_drift_progress"
        ).fetchone()
    assert phase == "source-events"
    assert json.loads(counts_json)["phase_row_count"] == 2


def test_source_event_phase_adapts_global_500_row_budget(tmp_path: Path) -> None:
    dense_rows = []
    for ordinal in range(1, 101):
        event_id = f"dense-event-{ordinal:04d}"
        member_ids = [f"dense-market-{ordinal:04d}-{index:03d}" for index in range(50)]
        dense_rows.append(
            (
                ordinal,
                event_id,
                {
                    "id": event_id,
                    "active": True,
                    "closed": False,
                    "negRisk": True,
                    "enableNegRisk": True,
                    "negRiskMarketID": f"dense-group-{ordinal:04d}",
                    "markets": [
                        {"id": market_id, "active": True, "closed": False}
                        for market_id in member_ids
                    ],
                },
                frozenset(member_ids),
            )
        )

    capped = _drift_store(tmp_path / "capped")
    chunked = _drift_store(tmp_path / "chunked")
    observed_limits: list[int] = []

    def observed_fetch(**kwargs):
        observed_limits.append(int(kwargs["limit"]))
        after = kwargs["after_event_id"]
        eligible = [row for row in dense_rows if after is None or row[1] > after]
        candidates = eligible[: int(kwargs["limit"])]
        workloads = [
            (
                len(json.dumps(row[2]).encode()),
                len(row[2]["markets"]),
                len(row[3]),
            )
            for row in candidates
        ]
        prefix = sqlite_store_module._structure_drift_event_prefix_size(workloads)
        return candidates[:prefix]

    capped.fetch_structure_drift_event_source_chunk = observed_fetch  # type: ignore[method-assign]
    chunked.fetch_structure_drift_event_source_chunk = observed_fetch  # type: ignore[method-assign]
    capped_id = capped.initialize_structure_drift_comparison(now_ms=3_000)
    chunked_id = chunked.initialize_structure_drift_comparison(now_ms=3_000)
    started = time.monotonic()
    chunk = capped.advance_structure_drift_comparison_chunk(
        capped_id,
        max_rows=500,
        now_ms=3_001,
    )
    elapsed_s = time.monotonic() - started
    for index in range(5):
        chunked.advance_structure_drift_comparison_chunk(
            chunked_id,
            max_rows=2,
            now_ms=3_001 + index,
        )

    assert observed_limits[0] == 100
    assert chunk.rows_processed == 10
    assert elapsed_s < 15.0
    with sqlite3.connect(capped.db_path) as capped_con, sqlite3.connect(
        chunked.db_path
    ) as chunked_con:
        query = (
            "SELECT row_cursor_json,digest_state_json,class_counts_json,"
            "class_digests_json FROM structure_generation_drift_progress"
        )
        assert capped_con.execute(query).fetchone() == chunked_con.execute(
            query
        ).fetchone()


def test_source_event_workload_prefix_bounds_normal_and_rejects_oversized() -> None:
    select = sqlite_store_module._structure_drift_event_prefix_size

    assert select([(12_000, 23, 23)] * 100) == 21
    assert select([(300_000, 1, 1), (300_000, 1, 1)]) == 1
    with pytest.raises(
        ValueError, match="structure-drift-source-event-workload-oversized"
    ):
        select([(2_000_000, 50_000, 50_000), (1, 1, 1)])


def test_source_event_fetch_selects_metadata_before_payload_materialization(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    statements: list[str] = []

    rows = store.fetch_structure_drift_event_source_chunk(
        publication_id="publication-2",
        generation_snapshot_id=2,
        after_event_id=None,
        limit=100,
        trace_callback=statements.append,
    )

    metadata_index = next(
        index
        for index, statement in enumerate(statements)
        if "length(CAST(payload_json AS BLOB))" in statement
    )
    payload_index = next(
        index
        for index, statement in enumerate(statements)
        if "SELECT event_id,payload_json" in statement
    )
    assert metadata_index < payload_index
    assert "SELECT COALESCE(source_ordinal,rowid),event_id,payload_json" not in statements[
        metadata_index
    ]
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_actual_drift_child_defers_on_real_sqlite_writer_contention(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import run_structure_drift_in_subprocess

    store = _drift_store(tmp_path)
    blocker = sqlite3.connect(store.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        checkpoint = await run_structure_drift_in_subprocess(
            db_path=store.db_path,
            max_rows=1,
            max_chunks=100,
            max_elapsed_s=45.0,
            timeout_s=10.0,
        )
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    assert checkpoint.deferred is True
    assert checkpoint.defer_reason == "writer-busy"
    assert checkpoint.chunks_processed == 0
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress"
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_scheduler_records_actual_drift_child_checkpoint(tmp_path: Path) -> None:
    from polyarb.daemon.scheduler import SnapshotScheduler

    store = _drift_store(tmp_path)
    settings = SimpleNamespace(
        db_path=store.db_path,
        scheduler_interval_s=3600,
        structure_generation_drift_compare_enabled=True,
        structure_generation_drift_max_rows=1,
        structure_generation_drift_max_chunks_per_tick=1,
        structure_generation_drift_slice_s=5.0,
    )
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
    )

    assert await scheduler._maybe_advance_structure_drift(queued_at_ms=1_000) is True

    attempt = store.get_latest_structure_drift_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "checkpointed"
    assert attempt["chunks_processed"] == 1
    assert attempt["rows_processed"] == 1
    assert attempt["stderr_safe_marker"].startswith("structure-drift stage=")


@pytest.mark.asyncio
async def test_scheduler_never_spawns_unledgered_child_when_attempt_db_is_busy(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import SnapshotScheduler

    seeded = _drift_store(tmp_path)
    store = SQLiteStore(seeded.db_path, writer_timeout_s=0.01)
    settings = SimpleNamespace(
        db_path=store.db_path,
        scheduler_interval_s=3600,
        structure_generation_drift_compare_enabled=True,
        structure_generation_drift_max_rows=1,
        structure_generation_drift_max_chunks_per_tick=1,
        structure_generation_drift_slice_s=5.0,
    )
    scheduler = SnapshotScheduler(
        settings=settings,
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
    )
    blocker = sqlite3.connect(store.db_path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        assert (
            await scheduler._maybe_advance_structure_drift(queued_at_ms=1_000) is True
        )
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM structure_drift_attempts"
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_progress"
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_production_shaped_845_848_children_resume_to_sealed(
    tmp_path: Path,
) -> None:
    from polyarb.daemon.scheduler import run_structure_drift_in_subprocess

    store = _drift_store(tmp_path)
    _reshape_as_production_845_848(store)
    with sqlite3.connect(store.db_path) as con:
        immutable_before = con.execute(
            "SELECT snapshot_id,publication_id,validation_hash,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()
    process_count = 0
    total_chunks = 0
    while process_count < 20:
        checkpoint = await run_structure_drift_in_subprocess(
            db_path=store.db_path,
            max_rows=1,
            max_chunks=3,
            max_elapsed_s=5.0,
            timeout_s=10.0,
        )
        process_count += 1
        total_chunks += checkpoint.chunks_processed
        if checkpoint.ready:
            break
    else:
        pytest.fail("production-shaped drift children did not seal")

    status = store.structure_generation_drift_status()
    assert process_count > 1
    assert total_chunks > 3
    assert status["authorized"] is True
    assert status["authorization_mode"] == "drift-safe-sealed"
    assert status["legacy_snapshot_id"] == 845
    assert status["generation_snapshot_id"] == 848
    assert status["window_id"] == "window-97b"
    with sqlite3.connect(store.db_path) as con:
        immutable_after = con.execute(
            "SELECT snapshot_id,publication_id,validation_hash,"
            "comparison_receipt_digest FROM current_structure_generation WHERE id=1"
        ).fetchone()
        receipt_count = con.execute(
            "SELECT COUNT(*) FROM structure_generation_drift_receipts"
        ).fetchone()[0]
    assert immutable_after == immutable_before
    assert receipt_count == 1
