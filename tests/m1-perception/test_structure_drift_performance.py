from __future__ import annotations

import hashlib
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


def _run_production_shaped_classifier_benchmark(tmp_path: Path) -> dict[str, float]:
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
            with sqlite3.connect(sample_path) as con:
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
        with sqlite3.connect(sample_path) as con:
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
    evidence: dict[str, float] = {
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
