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
