from __future__ import annotations

import hashlib
import json
import sqlite3
import statistics
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import pytest

from polyarb.perception import structure_drift as structure_drift_module
from polyarb.perception.structure_contract import STRUCTURE_DRIFT_CLASSIFIER_V3
from polyarb.perception.structure_drift import (
    FreshGroupEvidence,
    FreshProjectionChunk,
    FreshProjectionCommitment,
    FreshProjectionCursor,
    FreshProjectionExclusion,
    StructuralMemberIdentity,
    StructureDriftCandidateEnvelope,
    _member_tuple,
    advance_fresh_projection_commitment,
    project_legacy_compatible_market,
)
from polyarb.snapshot.normalizer import normalize_events
from polyarb.storage import sqlite_store as sqlite_store_module
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


def test_paginated_drift_reader_closes_each_sqlite_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SQLiteStore(tmp_path / "drift-reader-lifecycle.db")
    store.init_schema()
    real_connect = sqlite_store_module.sqlite3.connect
    opened = []

    class TrackedConnection:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self._connection = real_connect(*args, **kwargs)
            self.closed = False
            opened.append(self)

        def __getattr__(self, name: str) -> object:
            return getattr(self._connection, name)

        def __enter__(self):
            self._connection.__enter__()
            return self

        def __exit__(self, *args: object) -> object:
            return self._connection.__exit__(*args)

        def close(self) -> None:
            self.closed = True
            self._connection.close()

    monkeypatch.setattr(sqlite_store_module.sqlite3, "connect", TrackedConnection)

    for _ in range(10):
        assert store.fetch_structure_drift_member_chunk(
            snapshot_id=1,
            generation=False,
            after_market_id=None,
            limit=500,
        ) == []

    assert len(opened) == 10
    assert all(connection.closed for connection in opened)


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


def _seed_projection_gate_database(
    store: SQLiteStore,
    *,
    no_conflict_event_siblings: int | None = None,
    event_count: int = 50,
    event_only_market_index: int | None = None,
    global_conflict_market_index: int | None = None,
    add_orphan_market: bool = False,
    publish: bool = True,
) -> int:
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
                if market_index != event_only_market_index:
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
        if no_conflict_event_siblings is not None:
            events[0]["markets"].append(
                {
                    "id": "event-only-vm-sentinel",
                    "active": False,
                    "closed": True,
                    "negRiskOther": False,
                }
            )
            relations.extend(
                (
                    "window-perf",
                    f"no-conflict-sibling-{index:06d}",
                    "event-0000",
                    1,
                )
                for index in range(no_conflict_event_siblings)
            )
        if global_conflict_market_index is not None:
            relations.append(
                (
                    "window-perf",
                    f"market-{global_conflict_market_index:06d}",
                    "event-0001",
                    2,
                )
            )
        if add_orphan_market:
            markets.append(
                (
                    "window-perf",
                    "market-orphan",
                    json.dumps(
                        {
                            "id": "market-orphan",
                            "conditionId": "condition-orphan",
                            "clobTokenIds": json.dumps(["yes-orphan", "no-orphan"]),
                            "active": True,
                            "closed": False,
                            "negRisk": True,
                            "negRiskMarketID": "group-orphan",
                        }
                    ),
                    row_count + 1,
                )
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
        con.execute(
            "INSERT INTO structure_sync_event_market_backfill_progress("
            "window_id,window_checkpoint_at_ms,checkpoint_at_ms,completed_at_ms) "
            "VALUES ('window-perf',1001,1001,1001)"
        )
    while store.structure_event_member_status(window_id="window-perf").get("sealed") is not True:
        result = store.advance_structure_event_member_staging_chunk(
            window_id="window-perf", limit=500
        )
        assert result.get("reason") is None and result.get("failure_reason") is None
    if publish:
        with sqlite3.connect(store.db_path) as con:
            digest = "a" * 64
            con.execute(
                "INSERT INTO structure_publications(publication_id,window_id,snapshot_id,"
                "status,normalization_contract_version,expected_counts_json,"
                "committed_counts_json,validation_hash,certification_component,"
                "certification_hash,created_at_ms,checkpoint_at_ms) VALUES "
                "('publication-perf','window-perf',1,'published','contract-v1','{}',"
                "'{}',?,'bounded-complete',?,1000,1001)",
                (digest, digest),
            )
            con.execute(
                "UPDATE structure_sync_windows SET status='published',"
                "published_snapshot_id=1 WHERE id='window-perf'"
            )
    return row_count


def test_event_conflict_projection_vm_steps_do_not_scale_with_relation_siblings(
    tmp_path: Path,
) -> None:
    stores = {
        sibling_count: SQLiteStore(tmp_path / f"siblings-{sibling_count}.db")
        for sibling_count in (100, 50_000)
    }
    for sibling_count, store in stores.items():
        _seed_projection_gate_database(
            store,
            no_conflict_event_siblings=sibling_count,
        )

    def project_vm_steps(store: SQLiteStore) -> tuple[int, int, int]:
        vm_steps = 0

        def progress() -> int:
            nonlocal vm_steps
            vm_steps += 1
            return 0

        cursor = None
        member_count = diagnostic_count = 0
        while True:
            chunk = store.fetch_structure_drift_fresh_projection_chunk(
                publication_id="publication-perf",
                generation_snapshot_id=1,
                cursor=cursor,
                limit=500,
                sqlite_progress_callback=progress,
            )
            member_count += len(chunk.members)
            diagnostic_count += len(chunk.diagnostics)
            if chunk.cursor is None:
                break
            cursor = chunk.cursor
        return vm_steps, member_count, diagnostic_count

    small = project_vm_steps(stores[100])
    large = project_vm_steps(stores[50_000])
    print(
        "event-conflict-summary-vm-steps "
        f"siblings_100={small[0]} siblings_50000={large[0]} "
        f"ratio={large[0] / small[0]:.3f}"
    )
    # The source-authenticated group truth rejects all 24 active siblings in
    # the group containing one closed event-only member, plus the sentinel.
    assert small[1:] == large[1:] == (1_176, 25)
    assert large[0] <= small[0] * 1.05, {"small": small, "large": large}


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


def _copy_sqlite_database(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source) as source_con, sqlite3.connect(destination) as target:
        source_con.backup(target)


def _seed_production_shaped_classifier_database(store: SQLiteStore) -> dict[str, int]:
    event_count = 5_000
    members_per_group = 24
    market_count = event_count * members_per_group
    event_only_index = members_per_group * 2
    global_conflict_index = 0
    _seed_projection_gate_database(
        store,
        event_count=event_count,
        event_only_market_index=event_only_index,
        global_conflict_market_index=global_conflict_index,
        add_orphan_market=True,
        publish=False,
    )
    eligible_indexes = range(members_per_group * 2 + 1, market_count)
    eligible_count = market_count - members_per_group * 2 - 1
    legacy_count = eligible_count + 1
    cert = "a" * 64
    certification_counts = json.dumps(
        {"source_events": event_count, "source_markets": market_count},
        sort_keys=True,
        separators=(",", ":"),
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute("PRAGMA synchronous=OFF")
        con.execute("UPDATE snapshots SET market_count=? WHERE id=1", (eligible_count,))
        con.execute(
            "INSERT INTO snapshots(id,taken_at_ms,finished_at_ms,mode,market_count,"
            "market_view_published,data_product,archive_status,snapshot_status,is_valid,"
            "parquet_path) VALUES (2,900,901,'full',?,1,'structure','legacy','ok',1,'')",
            (legacy_count,),
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage(snapshot_id,completed,market_items,"
            "event_items) VALUES (2,1,?,?)",
            (legacy_count, event_count),
        )
        membership_rows = (
            (
                index // members_per_group,
                index,
            )
            for index in eligible_indexes
        )
        materialized = tuple(membership_rows)
        for generation in (False, True):
            snapshot_id = 1 if generation else 2
            membership_table = (
                "structure_generation_memberships"
                if generation
                else "event_market_memberships"
            )
            market_table = "structure_generation_markets" if generation else "markets"
            con.executemany(
                f"INSERT INTO {membership_table}(snapshot_id,event_id,"
                "neg_risk_market_id,market_id,member_kind,active,closed) "
                "VALUES (?,?,?,?,'named',1,0)",
                (
                    (
                        snapshot_id,
                        f"event-{event_index:04d}",
                        f"group-{event_index:04d}",
                        f"market-{market_index:06d}",
                    )
                    for event_index, market_index in materialized
                ),
            )
            con.executemany(
                f"INSERT INTO {market_table}(snapshot_id,market_id,condition_id,"
                "yes_token_id,no_token_id,active,closed,neg_risk,neg_risk_market_id,"
                "fetched_at_ms,incomplete,event_id) VALUES (?,?,?,?,?,1,0,1,?,1000,0,?)",
                (
                    (
                        snapshot_id,
                        f"market-{market_index:06d}",
                        f"condition-{market_index:06d}",
                        f"yes-{market_index:06d}",
                        f"no-{market_index:06d}",
                        f"group-{event_index:04d}",
                        f"event-{event_index:04d}",
                    )
                    for event_index, market_index in materialized
                ),
            )
        truths = con.execute(
            "SELECT event_id,group_id,neg_risk_type,expected_member_count,"
            "active_named_count,membership_hash,quality,reason FROM "
            "structure_sync_event_group_truth_staging WHERE window_id='window-perf' "
            "ORDER BY event_id,group_id"
        ).fetchall()
        assert len(truths) == event_count
        for generation in (False, True):
            snapshot_id = 1 if generation else 2
            truth_table = (
                "structure_generation_group_truth" if generation else "neg_risk_group_truth"
            )
            con.executemany(
                f"INSERT INTO {truth_table}(snapshot_id,event_id,neg_risk_market_id,"
                "neg_risk_type,expected_member_count,active_named_count,membership_hash,"
                "quality,reason) VALUES (?,?,?,?,?,?,?,?,?)",
                ((snapshot_id, *truth) for truth in truths),
            )
        legacy_only_hash = hashlib.sha256(
            json.dumps(
                [
                    (
                        "event-legacy-only",
                        "group-legacy-only",
                        "market-legacy-only",
                        "named",
                        True,
                        False,
                    )
                ],
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        con.execute(
            "INSERT INTO event_market_memberships(snapshot_id,event_id,"
            "neg_risk_market_id,market_id,member_kind,active,closed) VALUES "
            "(2,'event-legacy-only','group-legacy-only','market-legacy-only','named',1,0)"
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth(snapshot_id,event_id,neg_risk_market_id,"
            "neg_risk_type,expected_member_count,active_named_count,membership_hash,"
            "quality) VALUES (2,'event-legacy-only','group-legacy-only','standard',"
            "1,1,?,'complete-supported')",
            (legacy_only_hash,),
        )
        con.execute(
            "INSERT INTO markets(snapshot_id,market_id,condition_id,yes_token_id,"
            "no_token_id,active,closed,neg_risk,neg_risk_market_id,fetched_at_ms,"
            "incomplete,event_id) VALUES (2,'market-legacy-only',"
            "'condition-legacy-only','yes-legacy-only','no-legacy-only',1,0,1,"
            "'group-legacy-only',900,0,'event-legacy-only')"
        )
        con.execute(
            "INSERT INTO structure_publications(publication_id,window_id,snapshot_id,status,"
            "normalization_contract_version,expected_counts_json,committed_counts_json,"
            "validation_hash,certification_component,certification_hash,"
            "certification_counts_json,created_at_ms,checkpoint_at_ms) VALUES "
            "('publication-perf','window-perf',1,'published','contract-v1','{}','{}',"
            "?,'bounded-complete',?,?,1000,1001)",
            (cert, cert, certification_counts),
        )
        con.execute(
            "UPDATE structure_sync_windows SET status='published',published_snapshot_id=1 "
            "WHERE id='window-perf'"
        )
        legacy_universe, legacy_truth = sqlite_store_module._structure_universe_hash(
            con, snapshot_id=2, generation=False
        )
        generation_universe, generation_truth = (
            sqlite_store_module._structure_universe_hash(
                con, snapshot_id=1, generation=True
            )
        )
        receipt_digest = sqlite_store_module._comparison_receipt_digest(
            generation_snapshot_id=1,
            publication_id="publication-perf",
            legacy_snapshot_id=2,
            legacy_market_count=legacy_count,
            generation_market_count=eligible_count,
            legacy_universe_hash=legacy_universe,
            generation_universe_hash=generation_universe,
            legacy_source_truth_hash=legacy_truth,
            generation_source_truth_hash=generation_truth,
            generation_validation_hash=cert,
            created_at_ms=1_001,
        )
        con.execute(
            "INSERT INTO structure_generation_comparison_receipts("
            "generation_snapshot_id,publication_id,legacy_snapshot_id,"
            "legacy_market_count,generation_market_count,legacy_universe_hash,"
            "generation_universe_hash,legacy_source_truth_hash,"
            "generation_source_truth_hash,generation_validation_hash,created_at_ms,"
            "receipt_digest) VALUES (1,'publication-perf',2,?,?,?,?,?,?,?,?,?)",
            (
                legacy_count,
                eligible_count,
                legacy_universe,
                generation_universe,
                legacy_truth,
                generation_truth,
                cert,
                1_001,
                receipt_digest,
            ),
        )
        con.execute(
            "INSERT INTO current_structure_generation(id,snapshot_id,publication_id,"
            "validation_hash,counts_json,certification_component,"
            "comparison_receipt_digest,switched_at_ms) VALUES "
            "(1,1,'publication-perf',?,'{}','bounded-complete',?,1001)",
            (cert, receipt_digest),
        )
    return {
        "market_count": market_count,
        "event_count": event_count,
        "members_per_group": members_per_group,
        "global_conflict_count": 2,
        "event_only_candidate_count": 1,
    }


def _run_production_shaped_classifier_benchmark(
    tmp_path: Path,
) -> dict[str, float | int | bool]:
    template = SQLiteStore(tmp_path / "classifier-template.db")
    shape = _seed_production_shaped_classifier_database(template)

    def old_complete_gate() -> tuple[int, str]:
        raw_events: dict[str, dict[str, object]] = {}
        event_cursor = None
        source_event_digest = SerializableSHA256.new()
        source_event_digest.update(b"[")
        source_event_count = 0
        while True:
            rows = template.fetch_structure_drift_event_source_chunk(
                publication_id="publication-perf",
                generation_snapshot_id=1,
                after_event_id=event_cursor,
                limit=100,
            )
            if not rows:
                break
            for ordinal, event_id, raw, _market_ids in rows:
                if source_event_count:
                    source_event_digest.update(b",")
                source_event_digest.update(
                    json.dumps(
                        (ordinal, event_id, raw),
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ).encode()
                )
                source_event_count += 1
                raw_events[str(event_id)] = raw
            event_cursor = str(rows[-1][1])
        source_event_digest.update(b"]")
        source_market_digest = SerializableSHA256.new()
        source_market_digest.update(b"[")
        projection_digest = SerializableSHA256.new()
        projection_digest.update(b"[")
        emitted = 0
        source_market_count = 0
        market_cursor = None
        while True:
            rows = template.fetch_structure_drift_market_source_chunk(
                publication_id="publication-perf",
                generation_snapshot_id=1,
                after_market_id=market_cursor,
                limit=500,
            )
            if not rows:
                break
            for market_id, raw_market, event_ids, _taken_at_ms in rows:
                if source_market_count:
                    source_market_digest.update(b",")
                source_market_digest.update(
                    json.dumps(
                        (market_id, raw_market, event_ids),
                        ensure_ascii=True,
                        separators=(",", ":"),
                    ).encode()
                )
                source_market_count += 1
                if not event_ids:
                    continue
                # Reproduce the rejected gate: every candidate reparses and
                # normalizes its complete 24-member parent event.
                _events, _tags, _mapping, source_members, truths = normalize_events(
                    [raw_events[str(event_ids[0])]]
                )
                source_member = next(
                    (member for member in source_members if member.market_id == market_id),
                    None,
                )
                if (
                    source_member is None
                    or not truths
                    or truths[0].quality != "complete-supported"
                ):
                    continue
                projected = project_legacy_compatible_market(
                    raw_market, event_ids=event_ids, taken_at_ms=0
                )
                if projected.row is None:
                    continue
                if emitted:
                    projection_digest.update(b",")
                projection_digest.update(
                    json.dumps(
                        (
                            source_member.event_id,
                            source_member.group_id,
                            source_member.market_id,
                            source_member.member_kind,
                            source_member.active,
                            source_member.closed,
                            projected.row["condition_id"],
                            projected.row["yes_token_id"],
                            projected.row["no_token_id"],
                            projected.row["neg_risk"],
                            projected.row["incomplete"],
                        ),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode()
                )
                emitted += 1
            market_cursor = str(rows[-1][0])
        source_market_digest.update(b"]")
        projection_digest.update(b"]")
        audit_roots = []
        for generation, snapshot_id in ((True, 1), (False, 2)):
            audit = SerializableSHA256.new()
            audit.update(b"[")
            audit_count = 0
            member_cursor = None
            while True:
                rows = template.fetch_structure_drift_member_chunk(
                    snapshot_id=snapshot_id,
                    generation=generation,
                    after_market_id=member_cursor,
                    limit=500,
                )
                if not rows:
                    break
                for member in rows:
                    if audit_count:
                        audit.update(b",")
                    audit.update(
                        json.dumps(
                            _member_tuple(member),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ).encode()
                    )
                    audit_count += 1
                member_cursor = rows[-1].market_id
            audit.update(b"]")
            audit_roots.append(audit.hexdigest())
        terminal = hashlib.sha256(
            json.dumps(
                (
                    source_event_digest.hexdigest(),
                    source_market_digest.hexdigest(),
                    projection_digest.hexdigest(),
                    *audit_roots,
                ),
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return emitted, terminal

    def projection_query_budget() -> tuple[int, int]:
        commitment = None
        max_selects = 0
        calls = 0
        while commitment is None or not commitment.complete:
            statements: list[str] = []
            commitment = template.advance_structure_drift_fresh_projection_commitment(
                publication_id="publication-perf",
                generation_snapshot_id=1,
                commitment=commitment,
                limit=500,
                trace_callback=statements.append,
            )
            calls += 1
            max_selects = max(
                max_selects,
                sum(
                    statement.lstrip().upper().startswith("SELECT")
                    for statement in statements
                ),
            )
        assert commitment.member_count > 0
        return max_selects, calls

    def projection_page_query_shape(limit: int) -> tuple[int, int]:
        statements: list[str] = []
        inspected: dict[str, int] = {}
        chunk = template.fetch_structure_drift_fresh_projection_chunk(
            publication_id="publication-perf",
            generation_snapshot_id=1,
            cursor=None,
            limit=limit,
            classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3,
            trace_callback=statements.append,
            inspection_callback=inspected.__setitem__,
        )
        assert chunk.cursor is not None
        assert chunk.candidates_processed == limit
        assert inspected["candidates"] == limit
        selects = sum(
            statement.lstrip().upper().startswith("SELECT")
            for statement in statements
        )
        return selects, chunk.candidates_processed

    def projection_event_only_page_query_shape(limit: int) -> tuple[int, int]:
        statements: list[str] = []
        inspected: dict[str, int] = {}
        chunk = template.fetch_structure_drift_fresh_projection_chunk(
            publication_id="publication-perf",
            generation_snapshot_id=1,
            cursor=FreshProjectionCursor(
                stream="market",
                market_id="market-orphan",
                event_id=None,
                source_ordinal=None,
                member_ordinal=None,
            ),
            limit=limit,
            classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3,
            trace_callback=statements.append,
            inspection_callback=inspected.__setitem__,
        )
        assert chunk.cursor is None
        assert chunk.candidates_processed == 1
        assert inspected["candidates"] == 1
        selects = sum(
            statement.lstrip().upper().startswith("SELECT")
            for statement in statements
        )
        return selects, chunk.candidates_processed

    def classifier_v2_complete_gate(
        sample: int,
    ) -> tuple[float, dict[str, list[float]], float, str, int]:
        sample_path = tmp_path / f"classifier-v2-{sample}.db"
        _copy_sqlite_database(template.db_path, sample_path)
        store = SQLiteStore(sample_path)
        comparison_id = store.initialize_structure_drift_comparison(now_ms=2_000)
        stage_timings: dict[str, list[float]] = {
            "fresh-projection-members": [],
            "generation-members": [],
            "legacy-members": [],
            "terminal-receipt": [],
        }
        child_slice_elapsed = 0.0
        max_child_slice = 0.0
        chunks_in_slice = 0
        started_total = time.perf_counter()
        terminal = ""
        for chunk_index in range(2_000):
            # The phase probe is an observer, not part of the classifier
            # transaction. sqlite3's context manager does not close, so this
            # 2,000-turn loop needs an explicit connection owner too.
            with closing(sqlite3.connect(sample_path)) as con:
                phase = str(
                    con.execute(
                        "SELECT phase FROM structure_generation_drift_progress "
                        "WHERE comparison_id=?",
                        (comparison_id,),
                    ).fetchone()[0]
                )
            started = time.perf_counter()
            result = store.advance_structure_drift_comparison_chunk(
                comparison_id,
                max_rows=500,
                now_ms=2_001 + chunk_index,
            )
            elapsed = time.perf_counter() - started
            child_slice_elapsed += elapsed
            chunks_in_slice += 1
            if phase in stage_timings:
                stage_timings[phase].append(elapsed)
            if result.component in {"sealed", "stale"}:
                terminal = str(result.component)
                stage_timings["terminal-receipt"].append(elapsed)
                max_child_slice = max(max_child_slice, child_slice_elapsed)
                break
            if chunks_in_slice == 100:
                max_child_slice = max(max_child_slice, child_slice_elapsed)
                child_slice_elapsed = 0.0
                chunks_in_slice = 0
        else:
            pytest.fail("120k classifier-v2 benchmark did not reach terminal state")
        total = time.perf_counter() - started_total
        with closing(sqlite3.connect(sample_path)) as con:
            terminal_receipts = int(
                con.execute(
                    "SELECT COUNT(*) FROM structure_generation_drift_terminal_receipts "
                    "WHERE comparison_id=?",
                    (comparison_id,),
                ).fetchone()[0]
            )
        return total, stage_timings, max_child_slice, terminal, terminal_receipts

    # Seed and database copies stay outside every timed operation. Each path is
    # warmed once before the three samples used for medians.
    old_warm_started = time.perf_counter()
    old_complete_gate()
    print(
        "classifier-v1-complete-gate-sample "
        f"kind=warm elapsed_s={time.perf_counter() - old_warm_started:.6f}",
        flush=True,
    )
    old_samples = []
    for sample_index in range(3):
        started = time.perf_counter()
        old_complete_gate()
        elapsed = time.perf_counter() - started
        old_samples.append(elapsed)
        print(
            "classifier-v1-complete-gate-sample "
            f"kind=timed index={sample_index} elapsed_s={elapsed:.6f}",
            flush=True,
        )
    v2_warm = classifier_v2_complete_gate(-1)
    print(
        "classifier-v2-complete-gate-sample "
        f"kind=warm elapsed_s={v2_warm[0]:.6f}",
        flush=True,
    )
    v2_samples = []
    for sample_index in range(3):
        sample = classifier_v2_complete_gate(sample_index)
        v2_samples.append(sample)
        print(
            "classifier-v2-complete-gate-sample "
            f"kind=timed index={sample_index} elapsed_s={sample[0]:.6f}",
            flush=True,
        )
    projection_selects, projection_calls = projection_query_budget()
    page_shapes = {
        limit: projection_page_query_shape(limit) for limit in (1, 17, 500)
    }
    page_select_counts = {shape[0] for shape in page_shapes.values()}
    assert page_select_counts == {17}, page_shapes
    event_only_page_shapes = {
        limit: projection_event_only_page_query_shape(limit)
        for limit in (1, 17, 500)
    }
    event_only_select_counts = {
        shape[0] for shape in event_only_page_shapes.values()
    }
    assert event_only_select_counts == {20}, event_only_page_shapes
    query_plan_evidence = _fresh_projection_query_plan_evidence(template.db_path)
    false_plan_evidence = {
        "candidate_count_scans_market",
        "event_only_page_scans_member",
        "event_only_page_uses_temp_sort",
    }
    assert all(
        value is expected
        for key, value in query_plan_evidence.items()
        for expected in (key not in false_plan_evidence,)
    ), query_plan_evidence
    candidate_count_antijoin_indexed = (
        query_plan_evidence["candidate_count_uses_sidecar_index"]
        and query_plan_evidence["candidate_count_uses_market_index"]
        and not query_plan_evidence["candidate_count_scans_market"]
    )
    stage_medians: dict[str, float] = {}
    for stage in (
        "fresh-projection-members",
        "generation-members",
        "legacy-members",
        "terminal-receipt",
    ):
        samples = [
            sum(timings[stage])
            for _total, timings, _slice, _terminal, _receipts in v2_samples
        ]
        assert all(samples), stage
        stage_medians[stage] = statistics.median(samples)
    terminals = {sample[3] for sample in v2_samples}
    terminal_receipts = {sample[4] for sample in v2_samples}
    assert terminals == {"stale"}
    assert terminal_receipts == {1}
    evidence: dict[str, float | int | bool] = {
        **shape,
        "old_complete_gate_median_s": statistics.median(old_samples),
        "classifier_v2_complete_gate_median_s": statistics.median(
            sample[0] for sample in v2_samples
        ),
        "complete_projection_median_s": stage_medians["fresh-projection-members"],
        "classification_diagnostics_median_s": stage_medians["generation-members"],
        # The production generation phase advances the comparison mirror in
        # the same CAS as classification; this records that integrated wall time.
        "generation_mirror_median_s": stage_medians["generation-members"],
        "legacy_scan_median_s": stage_medians["legacy-members"],
        "terminal_receipt_median_s": stage_medians["terminal-receipt"],
        "max_child_slice_s": max(sample[2] for sample in v2_samples),
        "projection_query_count": projection_selects,
        "bounded_chunk_query_budget": 17,
        "projection_call_count": projection_calls,
        "projection_max_candidates": max(
            candidate_count for _select_count, candidate_count in page_shapes.values()
        ),
        "projection_page_query_count_min": min(page_select_counts),
        "projection_page_query_count_max": max(page_select_counts),
        "projection_event_only_query_count_min": min(event_only_select_counts),
        "projection_event_only_query_count_max": max(event_only_select_counts),
        "candidate_count_antijoin_indexed": candidate_count_antijoin_indexed,
    }
    print(
        "classifier-v2-production-gate "
        + json.dumps(evidence, sort_keys=True, separators=(",", ":"))
    )
    return evidence


@pytest.mark.slow
def test_120k_production_shaped_complete_classifier_gate(tmp_path: Path) -> None:
    evidence = _run_production_shaped_classifier_benchmark(tmp_path)

    assert evidence["market_count"] == 120_000
    assert evidence["event_count"] == 5_000
    assert evidence["members_per_group"] == 24
    assert evidence["global_conflict_count"] > 0
    assert evidence["event_only_candidate_count"] > 0
    assert evidence["old_complete_gate_median_s"] / evidence[
        "classifier_v2_complete_gate_median_s"
    ] >= 2.0
    assert evidence["max_child_slice_s"] < 45.0
    assert evidence["projection_query_count"] <= evidence[
        "bounded_chunk_query_budget"
    ]
    assert evidence["projection_max_candidates"] <= 500
    assert evidence["projection_page_query_count_min"] == evidence[
        "projection_page_query_count_max"
    ]
    assert evidence["projection_page_query_count_max"] == 17
    assert evidence["projection_event_only_query_count_min"] == evidence[
        "projection_event_only_query_count_max"
    ]
    assert evidence["projection_event_only_query_count_max"] == 20
    assert evidence["candidate_count_antijoin_indexed"] is True


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


PRODUCTION_V3_PARTITION = {
    "non-neg-risk-market": 82_346,
    "market-side-quarantine": 193,
    "augmented-group": 11_069,
    "fresh-group-ineligible": 312,
    "non-neg-risk-event-member": 13_655,
    "current-nontradable-event-member": 17_515,
    "event-only-quarantine": 68,
}
PRODUCTION_V3_ELIGIBLE = 41_768
PRODUCTION_V3_CANDIDATES = 166_926
PRODUCTION_V3_EVENT_ONLY_START = PRODUCTION_V3_ELIGIBLE + sum(
    PRODUCTION_V3_PARTITION[reason]
    for reason in (
        "non-neg-risk-market",
        "market-side-quarantine",
        "augmented-group",
        "fresh-group-ineligible",
    )
)
EXPECTED_PRODUCTION_MEMBER_ROOT = (
    "af94bd2afee226e0ad45f3ef5df35b270e2367dc06f272516f32dcb5efe38f28"
)
EXPECTED_PRODUCTION_EXCLUSION_ROOTS = {
    "non-neg-risk-market": (
        "7dededf83da7c7aa9a0a5705eed3f8670279822c68ad3d785bfbfef2db46670a"
    ),
    "market-side-quarantine": (
        "0336420644307f8b71089704c5e56d5dd14fa379c4a4884615ed653dfb1b2bd5"
    ),
    "augmented-group": (
        "31490bdc42043375f33d393a83be590353baf79180358ec6bd73b344a475b912"
    ),
    "fresh-group-ineligible": (
        "5fb541a71d4d6412802dbb6662b8422667c3f2ff4a9b87cfb44516da84f9f1af"
    ),
    "non-neg-risk-event-member": (
        "43a4314c4c2cbad9d7f7bf319660a3cfb2d2541fc19dc2e4b074ad40ef3a969f"
    ),
    "current-nontradable-event-member": (
        "72a6d8ffae143939cada101aec162dfdb6522080d33419b83ce893988e41d697"
    ),
    "event-only-quarantine": (
        "1752753f4acf5772387e35bd95b640b271170cc919cafedea4e383544827595e"
    ),
}

_INDEPENDENT_ROW_CHAIN_PREFIX = b"polyarb.structure-drift.row-chain-sha256-v2\x00"


def _independent_row_chain_frame(operation: str, domain: str) -> bytes:
    operation_bytes = operation.encode("ascii")
    domain_bytes = domain.encode("ascii")
    return (
        _INDEPENDENT_ROW_CHAIN_PREFIX
        + len(operation_bytes).to_bytes(2, "big")
        + operation_bytes
        + len(domain_bytes).to_bytes(2, "big")
        + domain_bytes
    )


def _independent_row_chain_root(
    domain: str,
    rows: Iterable[object],
) -> str:
    state = hashlib.sha256(_independent_row_chain_frame("init", domain)).digest()
    count = 0
    for row in rows:
        canonical = json.dumps(
            row,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode()
        leaf = hashlib.sha256(
            _independent_row_chain_frame("leaf", domain)
            + len(canonical).to_bytes(8, "big")
            + canonical
        ).digest()
        state = hashlib.sha256(
            _independent_row_chain_frame("chain", domain) + state + leaf
        ).digest()
        count += 1
    return hashlib.sha256(
        _independent_row_chain_frame("root", domain)
        + count.to_bytes(8, "big")
        + state
    ).hexdigest()


def _production_v3_member(index: int) -> StructuralMemberIdentity:
    group_index = index // 24
    return StructuralMemberIdentity(
        event_id=f"production-event-{group_index:05d}",
        group_id=f"production-group-{group_index:05d}",
        market_id=f"production-market-{index:06d}",
        member_kind="named",
        active=True,
        closed=False,
        condition_id=f"production-condition-{index:06d}",
        yes_token_id=f"production-yes-{index:06d}",
        no_token_id=f"production-no-{index:06d}",
        neg_risk=True,
        incomplete=False,
    )


def _production_v3_reason(index: int) -> tuple[str, int]:
    reason_index = index - PRODUCTION_V3_ELIGIBLE
    for reason, count in PRODUCTION_V3_PARTITION.items():
        if reason_index < count:
            return reason, reason_index
        reason_index -= count
    raise AssertionError(f"candidate index outside production partition: {index}")


def _production_v3_exclusion(index: int) -> FreshProjectionExclusion:
    reason, reason_index = _production_v3_reason(index)
    event_only = reason in {
        "non-neg-risk-event-member",
        "current-nontradable-event-member",
        "event-only-quarantine",
    }
    event_only_index = index - PRODUCTION_V3_EVENT_ONLY_START
    group_index = event_only_index // 24 if event_only else index // 24
    event_id = (
        f"production-event-only-{group_index:05d}"
        if event_only
        else f"production-event-{group_index:05d}"
    )
    group_id = (
        f"production-group-event-only-{group_index:05d}"
        if event_only
        else f"production-group-{group_index:05d}"
    )
    raw_hash = hashlib.sha256(f"{reason}:{reason_index}".encode()).hexdigest()
    envelope = StructureDriftCandidateEnvelope(
        side="generation-only",
        event_id=event_id,
        group_id=group_id,
        market_id=f"production-market-{index:06d}",
        member_kind="named",
        active=reason != "current-nontradable-event-member",
        closed=reason == "current-nontradable-event-member",
        condition_id=None if event_only else f"production-condition-{index:06d}",
        yes_token_id=None if event_only else f"production-yes-{index:06d}",
        no_token_id=None if event_only else f"production-no-{index:06d}",
        neg_risk=False if reason == "non-neg-risk-market" else None if event_only else True,
        incomplete=None if event_only else False,
        source_ordinal=group_index if event_only else None,
        member_ordinal=event_only_index % 24 if event_only else None,
        raw_event_hash=raw_hash if event_only else None,
        raw_market_hash=raw_hash,
    )
    group_truth = None
    if reason in {"augmented-group", "fresh-group-ineligible"}:
        augmented = reason == "augmented-group"
        group_truth = FreshGroupEvidence(
            event_id=event_id,
            group_id=group_id,
            neg_risk_type="augmented" if augmented else "standard",
            quality="complete-unsupported",
            reason=(
                "augmented-neg-risk-not-supported"
                if augmented
                else "standard-neg-risk-has-non-tradable-members"
            ),
            membership_hash=raw_hash,
            global_relation_conflict=False,
        )
    return FreshProjectionExclusion(
        reason=reason,
        stream="event-only" if event_only else "market",
        envelope=envelope,
        group_truth=group_truth,
    )


def _independent_production_v3_member_tuple(index: int) -> tuple[object, ...]:
    group_index = index // 24
    return (
        f"production-event-{group_index:05d}",
        f"production-group-{group_index:05d}",
        f"production-market-{index:06d}",
        "named",
        True,
        False,
        f"production-condition-{index:06d}",
        f"production-yes-{index:06d}",
        f"production-no-{index:06d}",
        True,
        False,
    )


def _independent_production_v3_reason(index: int) -> tuple[str, int]:
    reason_index = index - PRODUCTION_V3_ELIGIBLE
    for reason, count in PRODUCTION_V3_PARTITION.items():
        if reason_index < count:
            return reason, reason_index
        reason_index -= count
    raise AssertionError(f"candidate index outside independent partition: {index}")


def _independent_production_v3_exclusion_tuple(
    index: int,
) -> tuple[object, ...]:
    reason, reason_index = _independent_production_v3_reason(index)
    event_only = reason in {
        "non-neg-risk-event-member",
        "current-nontradable-event-member",
        "event-only-quarantine",
    }
    event_only_index = index - PRODUCTION_V3_EVENT_ONLY_START
    group_index = event_only_index // 24 if event_only else index // 24
    event_id = (
        f"production-event-only-{group_index:05d}"
        if event_only
        else f"production-event-{group_index:05d}"
    )
    group_id = (
        f"production-group-event-only-{group_index:05d}"
        if event_only
        else f"production-group-{group_index:05d}"
    )
    market_id = f"production-market-{index:06d}"
    raw_hash = hashlib.sha256(f"{reason}:{reason_index}".encode()).hexdigest()
    augmented = reason == "augmented-group"
    group_ineligible = reason == "fresh-group-ineligible"
    return (
        reason,
        "event-only" if event_only else "market",
        event_id,
        group_id,
        market_id,
        "named",
        reason != "current-nontradable-event-member",
        reason == "current-nontradable-event-member",
        None if event_only else f"production-condition-{index:06d}",
        None if event_only else f"production-yes-{index:06d}",
        None if event_only else f"production-no-{index:06d}",
        False if reason == "non-neg-risk-market" else None if event_only else True,
        None if event_only else False,
        group_index if event_only else None,
        event_only_index % 24 if event_only else None,
        raw_hash if event_only else None,
        raw_hash,
        event_id if augmented or group_ineligible else None,
        group_id if augmented or group_ineligible else None,
        "augmented" if augmented else "standard" if group_ineligible else None,
        "complete-unsupported" if augmented or group_ineligible else None,
        (
            "augmented-neg-risk-not-supported"
            if augmented
            else "standard-neg-risk-has-non-tradable-members"
            if group_ineligible
            else None
        ),
        raw_hash if augmented or group_ineligible else None,
    )


def _independent_production_v3_roots() -> tuple[str, dict[str, str]]:
    member_root = _independent_row_chain_root(
        "projection-member",
        (
            _independent_production_v3_member_tuple(index)
            for index in range(PRODUCTION_V3_ELIGIBLE)
        ),
    )
    exclusion_roots: dict[str, str] = {}
    start = PRODUCTION_V3_ELIGIBLE
    for reason, count in PRODUCTION_V3_PARTITION.items():
        exclusion_roots[reason] = _independent_row_chain_root(
            f"projection-exclusion/{reason}",
            (
                _independent_production_v3_exclusion_tuple(index)
                for index in range(start, start + count)
            ),
        )
        start += count
    assert start == PRODUCTION_V3_CANDIDATES
    return member_root, exclusion_roots


def _cursor_position(cursor: FreshProjectionCursor) -> int:
    prefix = "production-market-"
    if (
        cursor.market_id is None
        or not cursor.market_id.startswith(prefix)
    ):
        raise AssertionError(f"invalid synthetic production cursor: {cursor!r}")
    if cursor.stream == "market" and (
        cursor.event_id is not None
        or cursor.source_ordinal is not None
        or cursor.member_ordinal is not None
    ):
        raise AssertionError(f"invalid synthetic production cursor: {cursor!r}")
    if cursor.stream == "event-only" and (
        cursor.event_id is None
        or cursor.source_ordinal is None
        or cursor.member_ordinal is None
    ):
        raise AssertionError(f"invalid synthetic production cursor: {cursor!r}")
    return int(cursor.market_id.removeprefix(prefix))


def _fetch_production_v3_partition_chunk(
    *, cursor: FreshProjectionCursor | None, limit: int
) -> FreshProjectionChunk:
    assert 1 <= limit <= 500
    start = 0 if cursor is None else _cursor_position(cursor) + 1
    stop = min(start + limit, PRODUCTION_V3_CANDIDATES)
    members = []
    exclusions = []
    for index in range(start, stop):
        if index < PRODUCTION_V3_ELIGIBLE:
            members.append(_production_v3_member(index))
        else:
            exclusions.append(_production_v3_exclusion(index))
    next_cursor = None
    if stop != PRODUCTION_V3_CANDIDATES:
        last_index = stop - 1
        if last_index < PRODUCTION_V3_ELIGIBLE:
            next_cursor = FreshProjectionCursor(
                stream="market",
                market_id=f"production-market-{last_index:06d}",
                event_id=None,
                source_ordinal=None,
                member_ordinal=None,
            )
        else:
            exclusion = _production_v3_exclusion(last_index)
            event_only = exclusion.stream == "event-only"
            next_cursor = FreshProjectionCursor(
                stream="event-only" if event_only else "market",
                market_id=f"production-market-{last_index:06d}",
                event_id=exclusion.envelope.event_id if event_only else None,
                source_ordinal=(
                    exclusion.envelope.source_ordinal if event_only else None
                ),
                member_ordinal=(
                    exclusion.envelope.member_ordinal if event_only else None
                ),
            )
    return FreshProjectionChunk(
        cursor=next_cursor,
        members=tuple(members),
        diagnostics=(),
        candidates_processed=stop - start,
        exclusions=tuple(exclusions),
    )


@dataclass(frozen=True)
class _ProductionPartitionEvidence:
    commitment: FreshProjectionCommitment
    page_count: int
    nonterminal_page_count: int
    max_page_size: int


def _commit_production_v3_partition(*, limit: int) -> _ProductionPartitionEvidence:
    commitment = FreshProjectionCommitment.initial(
        publication_id="production-shaped-v3",
        generation_snapshot_id=1,
        member_receipt_digest="a" * 64,
        classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V3,
    )
    page_count = nonterminal_page_count = max_page_size = 0
    while not commitment.complete:
        before = commitment.candidates_processed
        previous_cursor = commitment.cursor
        chunk = _fetch_production_v3_partition_chunk(
            cursor=previous_cursor,
            limit=limit,
        )
        expected_page_size = min(limit, PRODUCTION_V3_CANDIDATES - before)
        assert chunk.candidates_processed == expected_page_size
        assert (
            len(chunk.members) + len(chunk.exclusions) + len(chunk.diagnostics)
            == chunk.candidates_processed
        )
        if chunk.cursor is not None:
            assert _cursor_position(chunk.cursor) == before + expected_page_size - 1
            assert chunk.cursor != previous_cursor
            nonterminal_page_count += 1
        commitment = advance_fresh_projection_commitment(commitment, chunk)
        assert commitment.candidates_processed == before + expected_page_size
        page_count += 1
        max_page_size = max(max_page_size, chunk.candidates_processed)
    return _ProductionPartitionEvidence(
        commitment=commitment,
        page_count=page_count,
        nonterminal_page_count=nonterminal_page_count,
        max_page_size=max_page_size,
    )


def _explain_query_plan(
    con: sqlite3.Connection,
    statement: str,
) -> str:
    return "\n".join(
        str(row[3])
        for row in con.execute("EXPLAIN QUERY PLAN " + statement)
    )


def _captured_select(
    statements: Sequence[str],
    *,
    starts_with: str,
    contains: tuple[str, ...] = (),
) -> str:
    matches = []
    for statement in statements:
        normalized = " ".join(statement.split())
        if normalized.startswith(starts_with) and all(
            fragment in normalized for fragment in contains
        ):
            matches.append(statement)
    assert len(matches) == 1, {
        "contains": contains,
        "matches": matches,
        "starts_with": starts_with,
    }
    return matches[0]


def _fresh_projection_query_plan_evidence(db_path: Path) -> dict[str, bool]:
    store = SQLiteStore(db_path)
    market_trace: list[str] = []
    market_chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-perf",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3,
        trace_callback=market_trace.append,
    )
    event_only_trace: list[str] = []
    event_only_chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-perf",
        generation_snapshot_id=1,
        cursor=FreshProjectionCursor(
            stream="event-only",
            market_id="market-before-event-only",
            event_id="event-0001",
            source_ordinal=2,
            member_ordinal=0,
        ),
        limit=500,
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3,
        trace_callback=event_only_trace.append,
    )
    count_trace: list[str] = []
    with sqlite3.connect(db_path) as con:
        con.set_trace_callback(count_trace.append)
        expected_candidates = (
            sqlite_store_module._fresh_projection_expected_candidate_count(
                con,
                window_id="window-perf",
            )
        )
        con.set_trace_callback(None)
    assert 0 < market_chunk.candidates_processed <= 500
    assert event_only_chunk.candidates_processed == 1
    assert expected_candidates > 0
    assert 0 < len(market_trace) <= 64
    assert 0 < len(event_only_trace) <= 64
    assert len(count_trace) == 2

    market_page_statement = _captured_select(
        market_trace,
        starts_with=(
            "SELECT market_id,payload_json FROM structure_sync_market_staging"
        ),
        contains=("ORDER BY market_id LIMIT",),
    )
    event_only_page_statement = _captured_select(
        event_only_trace,
        starts_with=(
            "SELECT member.market_sort_key,member.event_id,member.event_ordinal,"
            "member.member_ordinal"
        ),
        contains=(
            "structure_sync_event_member_staging member",
            "(member.event_id,member.member_ordinal,member.event_ordinal)>",
            "market.market_id IS NULL",
            "ORDER BY member.event_id,member.member_ordinal,member.event_ordinal",
        ),
    )
    market_count_statement = _captured_select(
        count_trace,
        starts_with="SELECT COUNT(*) FROM structure_sync_market_staging",
    )
    candidate_count_statement = _captured_select(
        count_trace,
        starts_with=(
            "SELECT COUNT(*) FROM structure_sync_event_member_staging member"
        ),
        contains=("NOT EXISTS", "structure_sync_market_staging market"),
    )
    with sqlite3.connect(db_path) as con:
        market_plan = _explain_query_plan(con, market_page_statement)
        event_only_plan = _explain_query_plan(con, event_only_page_statement)
        market_count_plan = _explain_query_plan(con, market_count_statement)
        candidate_count_plan = _explain_query_plan(con, candidate_count_statement)
    normalized_count_plan = candidate_count_plan.upper()
    normalized_event_only_plan = event_only_plan.upper()
    event_only_searches = [
        line
        for line in event_only_plan.splitlines()
        if line.upper().startswith("SEARCH MEMBER USING ")
    ]
    event_only_keyset_indexes = (
        "idx_structure_event_member_resume",
        "sqlite_autoindex_structure_sync_event_member_staging_1",
    )
    approved_market_count_indexes = (
        "idx_structure_sync_market_ordinal",
        "sqlite_autoindex_structure_sync_market_staging_1",
    )
    print(
        "fresh-projection-query-plans "
        + json.dumps(
            {
                "candidate_count": candidate_count_plan.splitlines(),
                "event_only_page": event_only_plan.splitlines(),
                "market_count": market_count_plan.splitlines(),
                "market_page": market_plan.splitlines(),
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )
    return {
        "market_page_uses_staging_index": (
            "sqlite_autoindex_structure_sync_market_staging_1" in market_plan
        ),
        "event_only_page_uses_keyset_index": any(
            any(index_name in line for index_name in event_only_keyset_indexes)
            for line in event_only_searches
        ),
        "event_only_page_scans_member": "SCAN MEMBER" in normalized_event_only_plan,
        "event_only_page_uses_temp_sort": (
            "USE TEMP B-TREE" in normalized_event_only_plan
            or "SORT" in normalized_event_only_plan
        ),
        "event_only_page_uses_market_index": (
            "sqlite_autoindex_structure_sync_market_staging_1" in event_only_plan
        ),
        "market_count_uses_approved_covering_index": (
            any(
                index_name in market_count_plan
                for index_name in approved_market_count_indexes
            )
        ),
        "candidate_count_uses_sidecar_index": (
            "idx_structure_event_member_market" in candidate_count_plan
        ),
        "candidate_count_uses_market_index": (
            "sqlite_autoindex_structure_sync_market_staging_1"
            in candidate_count_plan
        ),
        "candidate_count_scans_market": "SCAN MARKET" in normalized_count_plan,
    }


def test_166926_production_shaped_v3_goldens_use_independent_oracle() -> None:
    member_root, exclusion_roots = _independent_production_v3_roots()

    assert member_root == EXPECTED_PRODUCTION_MEMBER_ROOT
    assert exclusion_roots == EXPECTED_PRODUCTION_EXCLUSION_ROOTS


def test_166926_production_shaped_v3_uses_canonical_stream_order() -> None:
    expected_reason_order = (
        "non-neg-risk-market",
        "market-side-quarantine",
        "augmented-group",
        "fresh-group-ineligible",
        "non-neg-risk-event-member",
        "current-nontradable-event-member",
        "event-only-quarantine",
    )
    observed_reason_order = []
    observed_stream_order = []
    index = PRODUCTION_V3_ELIGIBLE
    while index < PRODUCTION_V3_CANDIDATES:
        reason, _reason_index = _production_v3_reason(index)
        observed_reason_order.append(reason)
        observed_stream_order.append(_production_v3_exclusion(index).stream)
        index += PRODUCTION_V3_PARTITION[reason]

    assert tuple(observed_reason_order) == expected_reason_order
    assert tuple(observed_stream_order) == (
        "market",
        "market",
        "market",
        "market",
        "event-only",
        "event-only",
        "event-only",
    )
    assert _production_v3_member(PRODUCTION_V3_ELIGIBLE - 1).market_id.endswith(
        f"{PRODUCTION_V3_ELIGIBLE - 1:06d}"
    )
    assert _production_v3_exclusion(PRODUCTION_V3_EVENT_ONLY_START - 1).stream == (
        "market"
    )
    assert _production_v3_exclusion(PRODUCTION_V3_EVENT_ONLY_START).stream == (
        "event-only"
    )
    assert index == PRODUCTION_V3_CANDIDATES


@pytest.mark.parametrize("limit", [1, 17, 500])
def test_166926_production_shaped_v3_partition_is_chunk_invariant(
    limit: int,
) -> None:
    evidence = _commit_production_v3_partition(limit=limit)
    result = evidence.commitment

    assert result.candidates_processed == PRODUCTION_V3_CANDIDATES
    assert result.member_count == PRODUCTION_V3_ELIGIBLE
    assert result.exclusion_count == 125_158
    assert result.diagnostic_count == 0
    assert result.exclusion_counts == PRODUCTION_V3_PARTITION
    assert result.root == EXPECTED_PRODUCTION_MEMBER_ROOT
    assert result.exclusion_roots == EXPECTED_PRODUCTION_EXCLUSION_ROOTS
    assert evidence.page_count == (
        PRODUCTION_V3_CANDIDATES + limit - 1
    ) // limit
    assert evidence.nonterminal_page_count == evidence.page_count - 1
    assert evidence.max_page_size <= limit
    assert evidence.max_page_size <= 500


def test_120k_production_shaped_candidate_count_antijoin_uses_indexes(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "candidate-count-plan.db")
    _seed_projection_gate_database(
        store,
        event_count=3,
        event_only_market_index=48,
        add_orphan_market=True,
    )

    evidence = _fresh_projection_query_plan_evidence(store.db_path)

    assert evidence["market_page_uses_staging_index"] is True
    assert evidence["event_only_page_uses_keyset_index"] is True
    assert evidence["event_only_page_scans_member"] is False
    assert evidence["event_only_page_uses_temp_sort"] is False
    assert evidence["event_only_page_uses_market_index"] is True
    assert evidence["market_count_uses_approved_covering_index"] is True
    assert evidence["candidate_count_uses_sidecar_index"] is True
    assert evidence["candidate_count_uses_market_index"] is True
    assert evidence["candidate_count_scans_market"] is False


def test_166926_member_golden_is_independent_of_production_tuple_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = structure_drift_module._member_tuple

    def reordered(member: StructuralMemberIdentity) -> tuple[object, ...]:
        row = original(member)
        return (*row[1:], row[0])

    monkeypatch.setattr(structure_drift_module, "_member_tuple", reordered)
    monkeypatch.setattr(sys.modules[__name__], "_member_tuple", reordered)

    member_root, _exclusion_roots = _independent_production_v3_roots()
    incremental = _commit_production_v3_partition(limit=500).commitment

    assert member_root == EXPECTED_PRODUCTION_MEMBER_ROOT
    assert incremental.root != EXPECTED_PRODUCTION_MEMBER_ROOT


def test_166926_exclusion_goldens_are_independent_of_production_tuple_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = structure_drift_module.structure_projection_exclusion_tuple

    def reordered(exclusion: FreshProjectionExclusion) -> tuple[object, ...]:
        row = original(exclusion)
        return (row[1], row[0], *row[2:])

    monkeypatch.setattr(
        structure_drift_module,
        "structure_projection_exclusion_tuple",
        reordered,
    )
    monkeypatch.setattr(
        sys.modules[__name__],
        "structure_projection_exclusion_tuple",
        reordered,
        raising=False,
    )

    _member_root, exclusion_roots = _independent_production_v3_roots()
    incremental = _commit_production_v3_partition(limit=500).commitment

    assert exclusion_roots == EXPECTED_PRODUCTION_EXCLUSION_ROOTS
    assert incremental.exclusion_roots != EXPECTED_PRODUCTION_EXCLUSION_ROOTS


def test_120k_query_plans_are_captured_from_production_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SQLiteStore(tmp_path / "captured-query-plan.db")
    _seed_projection_gate_database(
        store,
        event_count=3,
        event_only_market_index=48,
        add_orphan_market=True,
    )
    reader_calls: list[FreshProjectionCursor | None] = []
    count_calls: list[str] = []
    original_reader = SQLiteStore.fetch_structure_drift_fresh_projection_chunk
    original_count = sqlite_store_module._fresh_projection_expected_candidate_count

    def traced_reader(
        self: SQLiteStore, **kwargs: object
    ) -> FreshProjectionChunk:
        cursor = kwargs.get("cursor")
        assert cursor is None or isinstance(cursor, FreshProjectionCursor)
        reader_calls.append(cursor)
        return original_reader(self, **kwargs)

    def traced_count(con: sqlite3.Connection, *, window_id: str) -> int:
        count_calls.append(window_id)
        return original_count(con, window_id=window_id)

    monkeypatch.setattr(
        SQLiteStore,
        "fetch_structure_drift_fresh_projection_chunk",
        traced_reader,
    )
    monkeypatch.setattr(
        sqlite_store_module,
        "_fresh_projection_expected_candidate_count",
        traced_count,
    )

    _fresh_projection_query_plan_evidence(store.db_path)

    assert len(reader_calls) == 2
    assert reader_calls[0] is None
    assert reader_calls[1] is not None
    assert count_calls == ["window-perf"]
