from __future__ import annotations

import json
import sqlite3
import statistics
import time
from collections.abc import Callable, Sequence
from pathlib import Path

import pytest

from polyarb.perception.structure_drift import (
    StructuralMemberIdentity,
    _member_tuple,
    project_legacy_compatible_market,
)
from polyarb.snapshot.normalizer import normalize_events
from polyarb.storage.row_chain_sha256 import RowChainSHA256
from polyarb.storage.serializable_sha256 import SerializableSHA256
from polyarb.storage.sqlite_store import SQLiteStore


def _median_seconds(operation: Callable[[], object], *, repeats: int = 5) -> float:
    operation()
    samples: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        operation()
        samples.append(time.perf_counter() - started)
    return statistics.median(samples)


def _v1_root(rows: Sequence[object], *, ensure_ascii: bool) -> str:
    digest = SerializableSHA256.new()
    digest.update(b"[")
    for index, row in enumerate(rows):
        if index:
            digest.update(b",")
        digest.update(
            json.dumps(
                row,
                ensure_ascii=ensure_ascii,
                separators=(",", ":"),
            ).encode()
        )
    digest.update(b"]")
    return digest.hexdigest()


def _v2_root(rows: Sequence[object], *, domain: str) -> str:
    digest = RowChainSHA256.new(domain)
    for row in rows:
        digest.update(row)
    return digest.hexdigest()


def test_projection_row_chain_v2_root_work_is_at_least_twice_as_fast_as_v1() -> None:
    event_rows = tuple(
        (
            index,
            f"event-{index:06d}",
            {
                "id": f"event-{index:06d}",
                "title": f"Production-shaped event {index}",
                "markets": [
                    {
                        "id": f"market-{index:06d}-{member:02d}",
                        "active": True,
                        "closed": False,
                    }
                    for member in range(4)
                ],
            },
        )
        for index in range(500)
    )
    market_rows = tuple(
        (
            f"market-{index:06d}",
            {
                "id": f"market-{index:06d}",
                "conditionId": f"condition-{index:06d}",
                "clobTokenIds": json.dumps(
                    [f"yes-{index:06d}", f"no-{index:06d}"]
                ),
                "active": True,
                "closed": False,
                "negRisk": True,
            },
            (f"event-{index // 24:06d}",),
        )
        for index in range(500)
    )
    member_rows = tuple(
        (
            f"event-{index // 24:06d}",
            f"group-{index // 24:06d}",
            f"market-{index:06d}",
            "named",
            True,
            False,
            f"condition-{index:06d}",
            f"yes-{index:06d}",
            f"no-{index:06d}",
            True,
            False,
        )
        for index in range(500)
    )
    shared_class_rows = tuple(("shared", *row) for row in member_rows)
    cases = (
        ("source-events", "source-event", event_rows, True),
        ("source-markets", "source-market", market_rows, True),
        ("projection-members", "projection-member", member_rows, False),
        ("generation-members", "generation-member", member_rows, False),
        ("shared-class", "class/shared", shared_class_rows, False),
    )

    ratios: dict[str, float] = {}
    for name, domain, rows, ensure_ascii in cases:
        v1_median = _median_seconds(
            lambda rows=rows, ensure_ascii=ensure_ascii: _v1_root(
                rows,
                ensure_ascii=ensure_ascii,
            ),
            repeats=3,
        )
        v2_median = _median_seconds(
            lambda rows=rows, domain=domain: _v2_root(rows, domain=domain),
            repeats=5,
        )
        ratios[name] = v1_median / v2_median

    v1_pair_median = _median_seconds(
        lambda: (
            _v1_root(member_rows, ensure_ascii=False),
            _v1_root(member_rows, ensure_ascii=False),
        ),
        repeats=3,
    )
    v2_pair_median = _median_seconds(
        lambda: (
            _v2_root(member_rows, domain="generation-member"),
            _v2_root(member_rows, domain="projection-member"),
        ),
        repeats=5,
    )
    ratios["generation-audit-plus-comparison-mirror"] = (
        v1_pair_median / v2_pair_median
    )

    assert set(ratios) == {
        "source-events",
        "source-markets",
        "projection-members",
        "generation-members",
        "shared-class",
        "generation-audit-plus-comparison-mirror",
    }
    assert all(ratio >= 2.0 for ratio in ratios.values()), ratios


def _seed_projection_gate_database(store: SQLiteStore) -> int:
    event_count = 50
    members_per_event = 24
    row_count = event_count * members_per_event
    store.init_schema()
    events = []
    markets = []
    relations = []
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (1,1000,1001,'full',?,1,'structure','legacy','ok',1,'')",
            (row_count,),
        )
        con.execute(
            "INSERT INTO structure_sync_windows(id,status,started_at_ms,checkpoint_at_ms) "
            "VALUES ('window-perf','open',1000,1000)"
        )
        con.execute(
            "INSERT INTO structure_sync_event_source_progress VALUES ('window-perf',0,?,1000)",
            (RowChainSHA256.new("source-event").to_json(),),
        )
        for event_index in range(event_count):
            event_id = f"event-{event_index:04d}"
            group_id = f"group-{event_index:04d}"
            members = []
            for member_index in range(members_per_event):
                market_index = event_index * members_per_event + member_index
                market_id = f"market-{market_index:06d}"
                members.append(
                    {
                        "id": market_id,
                        "active": True,
                        "closed": False,
                        "negRiskOther": False,
                    }
                )
                markets.append(
                    (
                        "window-perf",
                        market_id,
                        json.dumps(
                            {
                                "id": market_id,
                                "conditionId": f"condition-{market_index:06d}",
                                "clobTokenIds": json.dumps(
                                    [
                                        f"yes-{market_index:06d}",
                                        f"no-{market_index:06d}",
                                    ]
                                ),
                                "active": True,
                                "closed": False,
                                "negRisk": True,
                                "negRiskMarketID": group_id,
                            }
                        ),
                        market_index + 1,
                    )
                )
                relations.append(
                    ("window-perf", market_id, event_id, event_index + 1)
                )
            events.append(
                {
                    "id": event_id,
                    "slug": event_id,
                    "active": True,
                    "closed": False,
                    "negRisk": True,
                    "enableNegRisk": True,
                    "negRiskAugmented": False,
                    "negRiskMarketID": group_id,
                    "markets": members,
                }
            )
        con.executemany(
            "INSERT INTO structure_sync_event_market_staging VALUES (?,?,?,?)",
            relations,
        )
    store.commit_structure_event_page(
        window_id="window-perf",
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=events,
        finished_at_ms=1001,
    )
    with sqlite3.connect(store.db_path) as con:
        con.executemany(
            "INSERT INTO structure_sync_market_staging(window_id,market_id,payload_json,"
            "source_ordinal) VALUES (?,?,?,?)",
            markets,
        )
        con.execute(
            "UPDATE structure_sync_windows SET status='complete' WHERE id='window-perf'"
        )
    while store.structure_event_member_status(window_id="window-perf").get("sealed") is not True:
        result = store.advance_structure_event_member_staging_chunk(
            window_id="window-perf", limit=500
        )
        assert result.get("reason") is None and result.get("failure_reason") is None
    with sqlite3.connect(store.db_path) as con:
        digest = "a" * 64
        con.execute(
            "INSERT INTO structure_publications(publication_id,window_id,snapshot_id,status,"
            "normalization_contract_version,expected_counts_json,committed_counts_json,"
            "validation_hash,certification_component,certification_hash,created_at_ms,"
            "checkpoint_at_ms) VALUES ('publication-perf','window-perf',1,'published',"
            "'contract-v1','{}','{}',?,'bounded-complete',?,1000,1001)",
            (digest, digest),
        )
        con.execute(
            "UPDATE structure_sync_windows SET status='published',published_snapshot_id=1 "
            "WHERE id='window-perf'"
        )
    return row_count


def test_complete_sealed_sidecar_gate_is_twice_as_fast_as_old_raw_projection(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "projection-gate-performance.db")
    row_count = _seed_projection_gate_database(store)

    def raw_v1_projection() -> tuple[int, str]:
        raw_events: dict[str, dict[str, object]] = {}
        after_event_id = None
        while True:
            rows = store.fetch_structure_drift_event_source_chunk(
                publication_id="publication-perf",
                generation_snapshot_id=1,
                after_event_id=after_event_id,
                limit=500,
            )
            if not rows:
                break
            raw_events.update((str(row[1]), row[2]) for row in rows)
            after_event_id = str(rows[-1][1])
        digest = RowChainSHA256.new("projection-member")
        count = 0
        after_market_id = None
        while True:
            rows = store.fetch_structure_drift_market_source_chunk(
                publication_id="publication-perf",
                generation_snapshot_id=1,
                after_market_id=after_market_id,
                limit=500,
            )
            if not rows:
                break
            for market_id, raw_market, event_ids, _taken_at_ms in rows:
                # This is the rejected v1 shape: normalize the whole parent
                # sibling array once per market candidate.
                _events, _tags, _mapping, source_members, _truths = normalize_events(
                    [raw_events[str(event_ids[0])]]
                )
                source_member = next(
                    member for member in source_members if member.market_id == market_id
                )
                projected = project_legacy_compatible_market(
                    raw_market, event_ids=event_ids, taken_at_ms=0
                )
                assert projected.row is not None
                row = projected.row
                member = StructuralMemberIdentity(
                    event_id=source_member.event_id,
                    group_id=source_member.group_id,
                    market_id=source_member.market_id,
                    member_kind=source_member.member_kind,
                    active=source_member.active,
                    closed=source_member.closed,
                    condition_id=str(row["condition_id"]),
                    yes_token_id=str(row["yes_token_id"]),
                    no_token_id=str(row["no_token_id"]),
                    neg_risk=bool(row["neg_risk"]),
                    incomplete=bool(row["incomplete"]),
                )
                digest.update(_member_tuple(member))
                count += 1
            after_market_id = str(rows[-1][0])
        return count, digest.hexdigest()

    query_statements: list[str] = []

    def sidecar_v2_projection(*, trace: bool = False) -> tuple[int, str]:
        commitment = None
        started = time.perf_counter()
        while commitment is None or not commitment.complete:
            commitment = store.advance_structure_drift_fresh_projection_commitment(
                publication_id="publication-perf",
                generation_snapshot_id=1,
                commitment=commitment,
                limit=500,
                trace_callback=query_statements.append if trace else None,
            )
        assert time.perf_counter() - started < 45.0
        return commitment.member_count, commitment.root

    expected = raw_v1_projection()
    assert expected == sidecar_v2_projection()
    assert expected[0] == row_count
    query_statements.clear()
    assert sidecar_v2_projection(trace=True) == expected
    select_count = sum(
        statement.lstrip().upper().startswith("SELECT")
        for statement in query_statements
    )
    assert select_count > 0
    assert not any("json_each" in statement.lower() for statement in query_statements)

    raw_median = _median_seconds(raw_v1_projection, repeats=3)
    sidecar_median = _median_seconds(sidecar_v2_projection, repeats=5)
    print(
        "projection-gate-performance "
        f"rows={row_count} raw_median_s={raw_median:.6f} "
        f"sidecar_median_s={sidecar_median:.6f} "
        f"ratio={raw_median / sidecar_median:.2f} "
        f"v2_selects={select_count}"
    )
    assert raw_median / sidecar_median >= 2.0, {
        "rows": row_count,
        "raw_seconds": raw_median,
        "sidecar_seconds": sidecar_median,
        "ratio": raw_median / sidecar_median,
        "v2_select_count": select_count,
    }


def _seed_member_scan_database(store: SQLiteStore, *, row_count: int) -> None:
    members_per_group = 24
    group_count = row_count // members_per_group
    with sqlite3.connect(store.db_path) as con:
        con.execute("PRAGMA synchronous=OFF")
        for generation in (False, True):
            membership_table = (
                "structure_generation_memberships"
                if generation
                else "event_market_memberships"
            )
            truth_table = (
                "structure_generation_group_truth"
                if generation
                else "neg_risk_group_truth"
            )
            market_table = "structure_generation_markets" if generation else "markets"
            snapshot_id = 2 if generation else 1
            con.executemany(
                f"INSERT INTO {membership_table}(snapshot_id,event_id,"
                "neg_risk_market_id,market_id,member_kind,active,closed) "
                "VALUES (?,?,?,?,'named',1,0)",
                (
                    (
                        snapshot_id,
                        f"event-{index // members_per_group:06d}",
                        f"group-{index // members_per_group:06d}",
                        f"market-{index:06d}",
                    )
                    for index in range(row_count)
                ),
            )
            con.executemany(
                f"INSERT INTO {truth_table}(snapshot_id,event_id,"
                "neg_risk_market_id,neg_risk_type,expected_member_count,"
                "active_named_count,membership_hash,quality) "
                "VALUES (?,?,?,'standard',24,24,?,'complete-supported')",
                (
                    (
                        snapshot_id,
                        f"event-{index:06d}",
                        f"group-{index:06d}",
                        "a" * 64,
                    )
                    for index in range(group_count)
                ),
            )
            con.executemany(
                f"INSERT INTO {market_table}(snapshot_id,market_id,condition_id,"
                "yes_token_id,no_token_id,active,closed,neg_risk,"
                "neg_risk_market_id,fetched_at_ms,incomplete,event_id) "
                "VALUES (?,?,?,?,?,1,0,1,?,1000,0,?)",
                (
                    (
                        snapshot_id,
                        f"market-{index:06d}",
                        f"condition-{index:06d}",
                        f"yes-{index:06d}",
                        f"no-{index:06d}",
                        f"group-{index // members_per_group:06d}",
                        f"event-{index // members_per_group:06d}",
                    )
                    for index in range(row_count)
                ),
            )


def _member_scan_sql(*, generation: bool, indexed: bool) -> str:
    membership_table = (
        "structure_generation_memberships"
        if generation
        else "event_market_memberships"
    )
    truth_table = (
        "structure_generation_group_truth" if generation else "neg_risk_group_truth"
    )
    market_table = "structure_generation_markets" if generation else "markets"
    index_clause = (
        " INDEXED BY idx_structure_generation_memberships_drift_scan"
        if generation and indexed
        else " INDEXED BY idx_event_market_memberships_drift_scan"
        if indexed
        else ""
    )
    return (
        "SELECT m.event_id,m.neg_risk_market_id,m.market_id,m.member_kind,"
        "m.active,m.closed,k.condition_id,k.yes_token_id,k.no_token_id,"
        f"k.neg_risk,k.incomplete FROM {membership_table} m{index_clause} "
        f"CROSS JOIN {truth_table} t ON m.snapshot_id=t.snapshot_id AND "
        "m.event_id=t.event_id AND m.neg_risk_market_id=t.neg_risk_market_id "
        f"CROSS JOIN {market_table} k ON k.snapshot_id=m.snapshot_id AND "
        "k.market_id=m.market_id AND k.event_id=m.event_id AND "
        "k.neg_risk_market_id=m.neg_risk_market_id WHERE t.snapshot_id=? "
        "AND t.neg_risk_type='standard' AND t.quality='complete-supported' "
        "AND m.market_id>? ORDER BY m.market_id LIMIT ?"
    )


def _execute_scan(db_path: Path, sql: str, snapshot_id: int) -> list[tuple[object, ...]]:
    with sqlite3.connect(db_path) as con:
        return con.execute(sql, (snapshot_id, "market-059999", 500)).fetchall()


@pytest.fixture(scope="module")
def member_scan_store(tmp_path_factory: pytest.TempPathFactory) -> SQLiteStore:
    store = SQLiteStore(
        tmp_path_factory.mktemp("drift-performance") / "member-scan-performance.db"
    )
    store.init_schema()
    _seed_member_scan_database(store, row_count=120_000)
    return store


@pytest.mark.parametrize("generation", (False, True), ids=("legacy", "generation"))
def test_120k_member_scan_covering_index_is_at_least_twice_as_fast(
    member_scan_store: SQLiteStore,
    generation: bool,
) -> None:
    store = member_scan_store
    target_index = (
        "idx_structure_generation_memberships_drift_scan"
        if generation
        else "idx_event_market_memberships_drift_scan"
    )
    snapshot_id = 2 if generation else 1
    baseline_sql = _member_scan_sql(generation=generation, indexed=False)
    indexed_sql = _member_scan_sql(generation=generation, indexed=True)

    with sqlite3.connect(store.db_path) as con:
        con.execute(f"DROP INDEX {target_index}")
        baseline_plan = "\n".join(
            str(row[3])
            for row in con.execute(
                "EXPLAIN QUERY PLAN " + baseline_sql,
                (snapshot_id, "market-059999", 500),
            )
        )
    baseline_median = _median_seconds(
        lambda: _execute_scan(store.db_path, baseline_sql, snapshot_id),
        repeats=5,
    )

    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        indexed_plan = "\n".join(
            str(row[3])
            for row in con.execute(
                "EXPLAIN QUERY PLAN " + indexed_sql,
                (snapshot_id, "market-059999", 500),
            )
        )
    indexed_rows = _execute_scan(store.db_path, indexed_sql, snapshot_id)
    indexed_median = _median_seconds(
        lambda: _execute_scan(store.db_path, indexed_sql, snapshot_id),
        repeats=5,
    )

    assert len(indexed_rows) == 500
    assert target_index not in baseline_plan
    assert "USE TEMP B-TREE FOR ORDER BY" in baseline_plan
    assert target_index in indexed_plan
    assert "USE TEMP B-TREE FOR ORDER BY" not in indexed_plan
    assert baseline_median / indexed_median >= 2.0, {
        "baseline_median_s": baseline_median,
        "indexed_median_s": indexed_median,
        "ratio": baseline_median / indexed_median,
    }
