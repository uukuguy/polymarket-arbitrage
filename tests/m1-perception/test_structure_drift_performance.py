from __future__ import annotations

import json
import sqlite3
import statistics
import time
from collections.abc import Callable, Sequence
from pathlib import Path

import pytest

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


def test_row_chain_v2_root_work_is_at_least_twice_as_fast_as_v1() -> None:
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
