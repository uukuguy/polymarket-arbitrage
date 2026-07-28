from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

import polyarb.perception.store as store_module
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.storage.schemas import (
    OWNER_JOURNAL_TRIGGER_NAMES,
    OWNER_TRIGGER_TABLES,
    V2_CANDIDATE_CURRENT_AGGREGATE_DDL,
    V2_OWNER_JOURNAL_TRIGGER_DDL,
    V2_OWNER_MUTATION_GUARD_DDL,
    V3_OWNER_JOURNAL_TRIGGER_NAMES,
    V3_OWNER_MUTATION_GUARD_DDL,
)
from polyarb.storage.sqlite_store import SQLiteStore


def revision(
    *,
    group_id: str,
    revision: int,
    token_suffix: str,
    observed_at_ms: int = 2_000,
) -> GroupRevision:
    return GroupRevision.certified(
        group_id=group_id,
        event_id=f"event-{group_id}",
        revision=revision,
        started_at_ms=1_000,
        observed_at_ms=observed_at_ms,
        source_cursor=f"cursor-{revision}",
        legs=(
            GroupLeg(
                f"market-1-{token_suffix}",
                f"condition-1-{token_suffix}",
                f"yes-1-{token_suffix}",
                "First",
            ),
            GroupLeg(
                f"market-2-{token_suffix}",
                f"condition-2-{token_suffix}",
                f"yes-2-{token_suffix}",
                "Second",
            ),
        ),
    )


def batch_for(
    group: GroupRevision,
    *,
    quote_batch_id: str,
    quoted_at_ms: int = 3_100,
) -> GroupQuoteBatch:
    return GroupQuoteBatch.complete(
        group_id=group.group_id,
        membership_hash=group.membership_hash,
        quote_batch_id=quote_batch_id,
        started_at_ms=3_000,
        quoted_at_ms=quoted_at_ms,
        legs=tuple(
            GroupQuoteLeg(
                leg.yes_token_id,
                group.membership_hash,
                0.40 + index * 0.10,
                10.0 + index,
                "executable",
            )
            for index, leg in enumerate(group.legs)
        ),
    )


def _publish_candidate_result(
    store: OpportunityPerceptionStore,
    *,
    group_id: str,
    result: str,
    gross_edge_bps: float | None,
    quoted_at_ms: int,
) -> GroupRevision:
    group = revision(
        group_id=group_id,
        revision=1,
        token_suffix=group_id,
        observed_at_ms=quoted_at_ms - 1_000,
    )
    store.publish_group_revision(group)
    if result == "unavailable":
        store.record_candidate_watch_fact(
            group_id=group_id,
            membership_hash=group.membership_hash,
            quote_batch_id=None,
            observed_at_ms=quoted_at_ms,
            last_result="unavailable",
            reason="quote-unavailable",
            bundle_cost=None,
            gross_edge_bps=None,
            max_bundle_size=None,
            priority_class="normal",
            consecutive_failures=1,
            effective_interval_s=30,
            schedule_reason="retry",
            next_due_at_ms=quoted_at_ms + 30_000,
        )
        return group
    batch = batch_for(
        group,
        quote_batch_id=f"qb-{group_id}",
        quoted_at_ms=quoted_at_ms,
    )
    store.publish_candidate_success(
        batch,
        observed_at_ms=quoted_at_ms,
        last_result=result,
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=gross_edge_bps,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="current",
        next_due_at_ms=quoted_at_ms + 15_000,
    )
    return group


def test_current_opportunities_and_summary_are_authenticated_and_page_bounded(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    first = _publish_candidate_result(
        store,
        group_id="g-a",
        result="watching",
        gross_edge_bps=1_000,
        quoted_at_ms=3_100,
    )
    _publish_candidate_result(
        store,
        group_id="g-b",
        result="watching",
        gross_edge_bps=500,
        quoted_at_ms=3_200,
    )
    _publish_candidate_result(
        store,
        group_id="g-c",
        result="no-edge",
        gross_edge_bps=0,
        quoted_at_ms=3_300,
    )
    _publish_candidate_result(
        store,
        group_id="g-d",
        result="unavailable",
        gross_edge_bps=None,
        quoted_at_ms=3_400,
    )

    page, next_after = store.current_opportunities(
        after_group_id="",
        limit=1,
    )
    summary = store.candidate_current_summary()

    assert len(page) == 1
    assert page[0].group_id == "g-a"
    assert page[0].event_id == first.event_id
    assert page[0].group_revision == 1
    assert page[0].membership_hash == first.membership_hash
    assert page[0].quote_batch_id == "qb-g-a"
    assert page[0].bundle_cost == Decimal("0.9")
    assert page[0].gross_edge_bps == Decimal("1000.0")
    assert page[0].max_bundle_size == Decimal("10.0")
    assert page[0].structure_observed_at_ms == 2_100
    assert page[0].quote_started_at_ms == 3_000
    assert page[0].quote_quoted_at_ms == 3_100
    assert next_after == "g-a"
    second_page, final_after = store.current_opportunities(
        after_group_id=next_after,
        limit=1,
    )
    assert [item.group_id for item in second_page] == ["g-b"]
    assert final_after is None
    assert summary.current_group_count == 4
    assert summary.opportunity_count == 2
    assert summary.state_counts == {
        "watching": 2,
        "no-edge": 1,
        "unavailable": 1,
    }


_V4_EVIDENCE_OWNER_TABLES = (
    "neg_risk_incident_authority_checkpoint",
    "neg_risk_incident_open_authority",
    "neg_risk_incident_open_aggregate",
    "neg_risk_incident_scope_floors",
    "neg_risk_incident_replay_anchors",
    "neg_risk_resource_authority_checkpoint",
    "neg_risk_evidence_failures",
)


def _downgrade_owner_v4_fixture_to_v3(
    store: OpportunityPerceptionStore,
) -> None:
    with store._connect() as con:
        con.execute("BEGIN IMMEDIATE")
        for name in (
            set(OWNER_JOURNAL_TRIGGER_NAMES)
            - set(V3_OWNER_JOURNAL_TRIGGER_NAMES)
        ):
            con.execute(f'DROP TRIGGER "{name}"')
        for table in reversed(_V4_EVIDENCE_OWNER_TABLES):
            con.execute(f'DROP TABLE "{table}"')
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        con.execute("DROP TABLE neg_risk_owner_mutation_guard")
        con.execute(V3_OWNER_MUTATION_GUARD_DDL)
        con.execute(
            "INSERT INTO neg_risk_owner_mutation_guard("
            "id,consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,authority_version,migration_state"
            ") VALUES(1,?,?,?,?,?,?,3,?)",
            tuple(guard),
        )
        con.execute("COMMIT")


def _downgrade_owner_v3_fixture_to_v2(
    store: OpportunityPerceptionStore,
) -> None:
    _downgrade_owner_v4_fixture_to_v3(store)
    aggregate_trigger_names = tuple(
        name
        for name in OWNER_JOURNAL_TRIGGER_NAMES
        if "candidate_current_aggregate" in name
    )
    migration_trigger_names = tuple(
        name
        for name in OWNER_JOURNAL_TRIGGER_NAMES
        if (
            "candidate_current_aggregate" in name
            or "candidate_current_authority" in name
        )
    )
    with store._connect() as con:
        con.execute("BEGIN IMMEDIATE")
        for name in migration_trigger_names:
            con.execute(f'DROP TRIGGER "{name}"')
        row_hashes: list[str] = []
        for row in con.execute(
            "SELECT group_id,canonical_json FROM "
            "neg_risk_candidate_current_authority ORDER BY group_id"
        ).fetchall():
            canonical = json.loads(str(row["canonical_json"]))
            for field in (
                "bundle_cost",
                "fact_observed_at_ms",
                "gross_edge_bps",
                "max_bundle_size",
                "structure_observed_at_ms",
            ):
                canonical.pop(field)
            canonical_json = json.dumps(
                canonical,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            row_hash = f"sha256:{hashlib.sha256(canonical_json.encode()).hexdigest()}"
            con.execute(
                "UPDATE neg_risk_candidate_current_authority "
                "SET canonical_json=?,row_hash=? WHERE group_id=?",
                (canonical_json, row_hash, row["group_id"]),
            )
            row_hashes.append(row_hash)
        aggregate_digest = 0
        for row_hash in row_hashes:
            aggregate_digest ^= int(row_hash.removeprefix("sha256:"), 16)
        aggregate = con.execute(
            "SELECT current_group_count,opportunity_count "
            "FROM neg_risk_candidate_current_aggregate WHERE id=1"
        ).fetchone()
        con.execute(
            "ALTER TABLE neg_risk_candidate_current_aggregate "
            "RENAME TO neg_risk_candidate_current_aggregate_v3"
        )
        con.execute(V2_CANDIDATE_CURRENT_AGGREGATE_DDL)
        con.execute(
            "INSERT INTO neg_risk_candidate_current_aggregate("
            "id,current_group_count,opportunity_count,aggregate_digest"
            ") VALUES(1,?,?,?)",
            (
                aggregate["current_group_count"],
                aggregate["opportunity_count"],
                f"{aggregate_digest:064x}",
            ),
        )
        con.execute("DROP TABLE neg_risk_candidate_current_aggregate_v3")
        con.execute(
            "DROP INDEX idx_neg_risk_candidate_current_opportunity_page"
        )
        old_guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,discovery_aggregate_hash,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        con.execute("DROP TABLE neg_risk_owner_mutation_guard")
        con.execute(V2_OWNER_MUTATION_GUARD_DDL)
        candidate_hash, _ = store._owner_aggregate_hashes_v2(con)
        con.execute(
            "INSERT INTO neg_risk_owner_mutation_guard("
            "id,consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,authority_version,migration_state"
            ") VALUES(1,?,?,?,?,?,?,2,?)",
            (
                old_guard["consumed_journal_id"],
                old_guard["consumed_hash"],
                old_guard["retained_base_id"],
                old_guard["retained_base_hash"],
                candidate_hash,
                old_guard["discovery_aggregate_hash"],
                old_guard["migration_state"],
            ),
        )
        con.execute("COMMIT")
        con.executescript(V2_OWNER_JOURNAL_TRIGGER_DDL)
    assert aggregate_trigger_names


def test_existing_v2_owner_authority_migrates_atomically_through_v3_to_v4(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    _publish_candidate_result(
        store,
        group_id="g-migrate",
        result="watching",
        gross_edge_bps=1_000,
        quoted_at_ms=3_100,
    )
    _downgrade_owner_v3_fixture_to_v2(store)

    store.init_schema()
    store.init_schema()

    with store._connect() as con:
        columns = {
            str(row["name"])
            for row in con.execute(
                "PRAGMA table_info(neg_risk_candidate_current_aggregate)"
            )
        }
        guard = con.execute(
            "SELECT authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
    assert {
        "watching_count",
        "no_edge_count",
        "unavailable_count",
    } <= columns
    assert tuple(guard) == (4, "complete")
    assert store.candidate_current_summary().state_counts == {
        "watching": 1,
        "no-edge": 0,
        "unavailable": 0,
    }
    opportunities, next_after = store.current_opportunities(
        after_group_id="",
        limit=100,
    )
    assert len(opportunities) == 1
    assert opportunities[0].gross_edge_bps == Decimal("1000.0")
    assert next_after is None


def test_fresh_owner_v4_manifest_covers_bounded_evidence_authorities(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()

    expected_tables = {
        "neg_risk_incident_authority_checkpoint",
        "neg_risk_incident_open_authority",
        "neg_risk_incident_open_aggregate",
        "neg_risk_incident_scope_floors",
        "neg_risk_incident_replay_anchors",
        "neg_risk_resource_authority_checkpoint",
        "neg_risk_evidence_failures",
    }
    with store._connect() as con:
        guard = con.execute(
            "SELECT authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        covered_tables = {
            str(row["tbl_name"])
            for row in con.execute(
                "SELECT tbl_name FROM sqlite_master WHERE type='trigger' "
                "AND name LIKE 'trg_owner_%'"
            )
        }

    assert tuple(guard) == (4, "complete")
    assert expected_tables <= covered_tables


def test_existing_v3_owner_authority_migrates_atomically_to_v4(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    _publish_candidate_result(
        store,
        group_id="g-v3-v4",
        result="watching",
        gross_edge_bps=1_000,
        quoted_at_ms=3_100,
    )
    _downgrade_owner_v4_fixture_to_v3(store)

    store.init_schema()
    store.init_schema()

    with store._connect() as con:
        guard = con.execute(
            "SELECT authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        tables = {
            str(row["name"])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert tuple(guard) == (4, "complete")
    assert set(_V4_EVIDENCE_OWNER_TABLES) <= tables
    assert store.candidate_current_summary().opportunity_count == 1


def test_v3_owner_migration_backfills_existing_open_incident_authority(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    incident = IncidentManager(store, clock_ms=lambda: 2_000).detect(
        "discovery",
        "timeout",
        {"cursor": "legacy-v3"},
    )
    _downgrade_owner_v4_fixture_to_v3(store)

    store.init_schema()

    assert [item.id for item in store.open_incidents()] == [incident.id]
    with store._connect() as con:
        aggregate = con.execute(
            "SELECT open_count FROM neg_risk_incident_open_aggregate WHERE id=1"
        ).fetchone()
    assert aggregate["open_count"] == 1


def test_v3_owner_migration_deadline_rolls_back_as_exact_v3(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    _downgrade_owner_v4_fixture_to_v3(store)

    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        OpportunityPerceptionStore(
            db_path,
            deadline_monotonic=0,
        ).init_schema()

    with store._connect() as con:
        guard = con.execute(
            "SELECT authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        tables = {
            str(row["name"])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert tuple(guard) == (3, "complete")
    assert set(_V4_EVIDENCE_OWNER_TABLES).isdisjoint(tables)


def test_candidate_summary_is_constant_projection_read_and_opportunity_page_is_indexed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()

    monkeypatch.setattr(
        store,
        "_validated_candidate_checkpoint",
        lambda _con: (_ for _ in ()).throw(
            AssertionError("summary-must-not-replay-checkpoint")
        ),
    )
    assert store.candidate_current_summary().current_group_count == 0
    with store._connect() as con:
        plan = tuple(
            str(row["detail"])
            for row in con.execute(
                "EXPLAIN QUERY PLAN "
                "SELECT * FROM neg_risk_candidate_current_authority "
                "WHERE opportunity=1 AND group_id>? "
                "ORDER BY group_id LIMIT ?",
                ("", 101),
            )
        )
    assert any(
        "idx_neg_risk_candidate_current_opportunity_page" in detail
        for detail in plan
    )


def test_corrupt_v2_owner_authority_rolls_back_as_exact_v2(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    _publish_candidate_result(
        store,
        group_id="g-corrupt-v2",
        result="watching",
        gross_edge_bps=1_000,
        quoted_at_ms=3_100,
    )
    _downgrade_owner_v3_fixture_to_v2(store)
    current_trigger_names = tuple(
        name
        for name in OWNER_JOURNAL_TRIGGER_NAMES
        if "candidate_current_authority" in name
    )
    with store._connect() as con:
        for name in current_trigger_names:
            con.execute(f'DROP TRIGGER "{name}"')
        con.execute(
            "UPDATE neg_risk_candidate_current_authority "
            "SET canonical_json='{}' WHERE group_id='g-corrupt-v2'"
        )
        con.executescript(V2_OWNER_JOURNAL_TRIGGER_DDL)

    with pytest.raises(
        ValueError,
        match="invalid-candidate-current-authority",
    ):
        store.init_schema()

    with store._connect() as con:
        columns = {
            str(row["name"])
            for row in con.execute(
                "PRAGMA table_info(neg_risk_candidate_current_aggregate)"
            )
        }
        guard = con.execute(
            "SELECT authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
    assert "watching_count" not in columns
    assert tuple(guard) == (2, "complete")


def test_coherent_but_stale_v2_current_row_is_not_migrated(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-stale-v2", revision=1, token_suffix="stale")
    store.publish_group_revision(group)
    for sequence in range(2):
        quote = batch_for(
            group,
            quote_batch_id=f"stale-v2-{sequence}",
            quoted_at_ms=3_100 + sequence,
        )
        store.publish_candidate_success(
            quote,
            observed_at_ms=quote.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.8,
            gross_edge_bps=2_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="stale-v2",
            next_due_at_ms=quote.quoted_at_ms + 15_000,
        )
    _downgrade_owner_v3_fixture_to_v2(store)
    trigger_names = tuple(
        name
        for name in OWNER_JOURNAL_TRIGGER_NAMES
        if (
            "candidate_current_authority" in name
            or "candidate_current_aggregate" in name
        )
    )
    with store._connect() as con:
        for name in trigger_names:
            con.execute(f'DROP TRIGGER "{name}"')
        fact = con.execute(
            "SELECT * FROM neg_risk_candidate_watch_facts "
            "WHERE group_id=? ORDER BY id LIMIT 1",
            (group.group_id,),
        ).fetchone()
        quote = con.execute(
            "SELECT * FROM neg_risk_group_quote_batches WHERE id=?",
            (fact["quote_batch_id"],),
        ).fetchone()
        payload = {
            "event_id": group.event_id,
            "fact_id": int(fact["id"]),
            "group_id": group.group_id,
            "group_revision": group.revision,
            "last_result": "watching",
            "legs": json.loads(str(quote["legs_json"])),
            "membership_hash": group.membership_hash,
            "opportunity": 1,
            "quote_started_at_ms": int(quote["started_at_ms"]),
            "quote_quoted_at_ms": int(quote["quoted_at_ms"]),
            "quote_batch_id": fact["quote_batch_id"],
        }
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        row_hash = (
            "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()
        )
        con.execute(
            "UPDATE neg_risk_candidate_current_authority SET "
            "quote_batch_id=?,fact_id=?,legs_json=?,canonical_json=?,"
            "row_hash=? WHERE group_id=?",
            (
                fact["quote_batch_id"],
                fact["id"],
                quote["legs_json"],
                canonical,
                row_hash,
                group.group_id,
            ),
        )
        con.execute(
            "UPDATE neg_risk_candidate_current_aggregate "
            "SET aggregate_digest=? WHERE id=1",
            (row_hash.removeprefix("sha256:"),),
        )
        candidate_hash, _ = store._owner_aggregate_hashes_v2(con)
        con.execute(
            "UPDATE neg_risk_owner_mutation_guard "
            "SET candidate_aggregate_hash=? WHERE id=1",
            (candidate_hash,),
        )
        con.executescript(V2_OWNER_JOURNAL_TRIGGER_DDL)

    with pytest.raises(
        ValueError,
        match="invalid-candidate-current-authority",
    ):
        store.init_schema()


def test_v2_owner_migration_deadline_rolls_back_as_exact_v2(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    _publish_candidate_result(
        store,
        group_id="g-deadline-v2",
        result="watching",
        gross_edge_bps=1_000,
        quoted_at_ms=3_100,
    )
    _downgrade_owner_v3_fixture_to_v2(store)

    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        OpportunityPerceptionStore(
            db_path,
            deadline_monotonic=0,
        ).init_schema()

    with store._connect() as con:
        columns = {
            str(row["name"])
            for row in con.execute(
                "PRAGMA table_info(neg_risk_candidate_current_aggregate)"
            )
        }
        version = con.execute(
            "SELECT authority_version FROM "
            "neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()[0]
    assert "watching_count" not in columns
    assert version == 2


def test_concurrent_v2_owner_migrations_converge_to_one_v4(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    _publish_candidate_result(
        store,
        group_id="g-race-v2",
        result="watching",
        gross_edge_bps=1_000,
        quoted_at_ms=3_100,
    )
    _downgrade_owner_v3_fixture_to_v2(store)
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def migrate() -> None:
        try:
            barrier.wait(timeout=2)
            OpportunityPerceptionStore(db_path).init_schema()
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=migrate) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    with store._connect() as con:
        guard = con.execute(
            "SELECT authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
    assert tuple(guard) == (4, "complete")


def test_v2_owner_migration_backfills_from_compacted_retained_seed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        100,
    )
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_BYTES",
        1,
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-seed-v2", revision=1, token_suffix="seed")
    store.publish_group_revision(group)
    for sequence in range(3):
        quote = batch_for(
            group,
            quote_batch_id=f"seed-v2-{sequence}",
            quoted_at_ms=3_100 + sequence,
        )
        store.publish_candidate_success(
            quote,
            observed_at_ms=quote.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=(0.8, 0.81, 0.82)[sequence],
            gross_edge_bps=2_000 - sequence,
            max_bundle_size=10 + sequence,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="seed-v2",
            next_due_at_ms=quote.quoted_at_ms + 15_000,
        )
    with store._connect() as con:
        assert con.execute(
            "SELECT generation FROM "
            "neg_risk_candidate_authority_checkpoints WHERE id=1"
        ).fetchone() is not None
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_group_quote_batches"
        ).fetchone()[0] == 1
    _downgrade_owner_v3_fixture_to_v2(store)

    store.init_schema()

    opportunities, next_after = store.current_opportunities(
        after_group_id="",
        limit=100,
    )
    assert next_after is None
    assert len(opportunities) == 1
    assert opportunities[0].quote_batch_id == "seed-v2-2"
    assert opportunities[0].bundle_cost == Decimal("0.82")
    assert opportunities[0].gross_edge_bps == Decimal("1998.0")
    assert opportunities[0].max_bundle_size == Decimal("12.0")


def test_sqlite_schema_initialization_adds_perception_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"

    SQLiteStore(db_path).init_schema()

    with sqlite3.connect(db_path) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "neg_risk_group_revisions" in tables
    assert "neg_risk_group_quote_batches" in tables
    assert "neg_risk_candidate_success_receipts" in tables


def test_only_atomic_candidate_publish_creates_success_receipt(tmp_path: Path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    split_batch = batch_for(group, quote_batch_id="qb-split")
    store.publish_quote_batch(split_batch)
    store.record_candidate_watch_fact(
        group_id=group.group_id,
        membership_hash=group.membership_hash,
        quote_batch_id=split_batch.quote_batch_id,
        observed_at_ms=split_batch.quoted_at_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="split",
        next_due_at_ms=split_batch.quoted_at_ms + 15_000,
    )

    atomic_batch = batch_for(group, quote_batch_id="qb-atomic", quoted_at_ms=3_200)
    fact = store.publish_candidate_success(
        atomic_batch,
        observed_at_ms=atomic_batch.quoted_at_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="atomic",
        next_due_at_ms=atomic_batch.quoted_at_ms + 15_000,
    )

    with sqlite3.connect(store.db_path) as con:
        receipts = con.execute(
            "SELECT quote_batch_id,candidate_fact_row_id "
            "FROM neg_risk_candidate_success_receipts"
        ).fetchall()
    assert receipts == [(atomic_batch.quote_batch_id, fact.id)]


def test_candidate_success_uses_one_owner_token_and_one_begin_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-token", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    clean_checks = 0
    original = store._assert_owner_journal_clean

    def counted_clean_check(con: sqlite3.Connection) -> None:
        nonlocal clean_checks
        clean_checks += 1
        original(con)

    monkeypatch.setattr(store, "_assert_owner_journal_clean", counted_clean_check)
    batch = batch_for(group, quote_batch_id="qb-one-token")
    store.publish_candidate_success(
        batch,
        observed_at_ms=batch.quoted_at_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="one-token",
        next_due_at_ms=batch.quoted_at_ms + 15_000,
    )

    with store._connect() as con:
        receipt_token = str(
            con.execute(
                "SELECT writer_token FROM neg_risk_owner_mutation_journal "
                "WHERE table_name='neg_risk_candidate_success_receipts' "
                "AND row_key=? ORDER BY id DESC LIMIT 1",
                (group.group_id,),
            ).fetchone()[0]
        )
        token_tables = {
            str(row[0])
            for row in con.execute(
                "SELECT DISTINCT table_name FROM neg_risk_owner_mutation_journal "
                "WHERE writer_token=?",
                (receipt_token,),
            )
        }
    assert {
        "neg_risk_group_quote_batches",
        "neg_risk_candidate_watch_facts",
        "neg_risk_candidate_current_authority",
        "neg_risk_candidate_current_aggregate",
        "neg_risk_discovery_status_projection",
        "neg_risk_candidate_success_receipts",
    } <= token_tables
    assert clean_checks == 2  # one begin validation plus one post-finalize proof


def test_concurrent_candidate_writers_leave_owner_chain_clean(tmp_path: Path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-concurrent", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    errors: list[BaseException] = []

    def write_fact(sequence: int) -> None:
        try:
            store.record_candidate_watch_fact(
                group_id=group.group_id,
                membership_hash=group.membership_hash,
                quote_batch_id=None,
                observed_at_ms=4_000 + sequence,
                last_result="unavailable",
                reason=f"concurrent-{sequence}",
                bundle_cost=None,
                gross_edge_bps=None,
                max_bundle_size=None,
                priority_class="normal",
                consecutive_failures=sequence,
                effective_interval_s=60,
                schedule_reason="concurrent",
                next_due_at_ms=64_000 + sequence,
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=write_fact, args=(sequence,)) for sequence in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(store.candidate_watch_facts(group.group_id)) == 2
    assert store.validated_candidate_opportunity_count() == 0


@pytest.mark.parametrize(
    "layer",
    ("raw", "candidate-derived", "discovery-derived"),
)
@pytest.mark.parametrize("surface", ("read", "init", "next-writer"))
def test_deleted_pending_owner_event_is_detected_by_sqlite_sequence(
    tmp_path: Path,
    layer: str,
    surface: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-sequence", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    mutation = {
        "raw": (
            "UPDATE neg_risk_group_revisions SET source_cursor='ghost' "
            "WHERE group_id='g-sequence'"
        ),
        "candidate-derived": (
            "UPDATE neg_risk_candidate_current_aggregate "
            "SET current_group_count=current_group_count+1 WHERE id=1"
        ),
        "discovery-derived": (
            "UPDATE neg_risk_discovery_status_projection "
            "SET generation=generation+1 WHERE id=1"
        ),
    }[layer]
    with store._connect() as con:
        con.execute(mutation)
        con.execute(
            "DELETE FROM neg_risk_owner_mutation_journal WHERE id>("
            "SELECT consumed_journal_id FROM neg_risk_owner_mutation_guard WHERE id=1)"
        )

    with pytest.raises(ValueError, match="invalid-owner-mutation-sequence"):
        if surface == "read":
            store.current_group(group.group_id)
        elif surface == "init":
            store.init_schema()
        else:
            store.record_discovery_load_decision(
                degraded_reason=None,
                probe_every_cycles=10,
                now_ms=5_000,
            )


@pytest.mark.parametrize(
    "column",
    ("candidate_aggregate_hash", "discovery_aggregate_hash"),
)
@pytest.mark.parametrize("surface", ("read", "init", "next-writer"))
def test_completed_owner_guard_rejects_null_authenticated_hash(
    tmp_path: Path,
    column: str,
    surface: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        con.execute(
            f"UPDATE neg_risk_owner_mutation_guard SET {column}=NULL WHERE id=1"
        )

    with pytest.raises(ValueError, match="invalid-owner-guard-state"):
        if surface == "read":
            store.validated_candidate_opportunity_count()
        elif surface == "init":
            store.init_schema()
        else:
            store.record_discovery_load_decision(
                degraded_reason=None,
                probe_every_cycles=10,
                now_ms=5_000,
            )


@pytest.mark.parametrize("mutation", ("trigger-subset", "table-subset", "unknown"))
def test_unknown_or_partial_owner_manifest_fails_before_ddl(
    tmp_path: Path,
    mutation: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        if mutation == "trigger-subset":
            con.execute("DROP TRIGGER trg_owner_group_revisions_insert")
        elif mutation == "table-subset":
            con.execute("DROP TABLE neg_risk_candidate_current_authority")
        else:
            con.execute(
                "ALTER TABLE neg_risk_owner_mutation_guard "
                "ADD COLUMN unknown_owner_state TEXT"
            )

    with pytest.raises(ValueError, match="invalid-owner-authority-manifest"):
        store.init_schema()

    with store._connect() as con:
        if mutation == "trigger-subset":
            assert con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' "
                "AND name='trg_owner_group_revisions_insert'"
            ).fetchone() is None
        elif mutation == "table-subset":
            assert con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='neg_risk_candidate_current_authority'"
            ).fetchone() is None
        else:
            assert "unknown_owner_state" in {
                str(row["name"])
                for row in con.execute(
                    "PRAGMA table_info(neg_risk_owner_mutation_guard)"
                )
            }


@pytest.mark.parametrize("trigger_schema", ("main", "temp"))
@pytest.mark.parametrize("table_name", OWNER_TRIGGER_TABLES)
@pytest.mark.parametrize("surface", ("read", "init", "next-writer"))
def test_arbitrary_name_trigger_on_owner_table_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    trigger_schema: str,
    table_name: str,
    surface: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    trigger_sql = (
        f"CREATE {'TEMP ' if trigger_schema == 'temp' else ''}"
        "TRIGGER arbitrary_extra_authority_trigger "
        f"AFTER INSERT ON {table_name} BEGIN SELECT 1; END"
    )
    if trigger_schema == "main":
        with store._connect() as con:
            con.execute(trigger_sql)
    else:
        original_connect = store._connect

        def connect_with_temp_trigger(
            *args: object,
            **kwargs: object,
        ) -> sqlite3.Connection:
            con = original_connect(*args, **kwargs)
            con.execute(trigger_sql)
            return con

        monkeypatch.setattr(store, "_connect", connect_with_temp_trigger)

    with pytest.raises(ValueError, match="invalid-owner-authority-manifest"):
        if surface == "read":
            store.validated_candidate_opportunity_count()
        elif surface == "init":
            store.init_schema()
        else:
            store.record_discovery_load_decision(
                degraded_reason=None,
                probe_every_cycles=10,
                now_ms=5_000,
            )


_PROTECTED_TRIGGER_MUTATIONS = (
    (
        "raw",
        "UPDATE neg_risk_group_revisions SET source_cursor=source_cursor",
    ),
    (
        "derived",
        "UPDATE neg_risk_candidate_current_aggregate "
        "SET current_group_count=current_group_count",
    ),
    (
        "journal",
        "DELETE FROM neg_risk_owner_mutation_journal WHERE 0",
    ),
    (
        "guard",
        "UPDATE neg_risk_owner_mutation_guard "
        "SET consumed_journal_id=consumed_journal_id",
    ),
    (
        "context",
        "DELETE FROM neg_risk_owner_write_context WHERE 0",
    ),
)


@pytest.mark.parametrize("trigger_schema", ("main", "temp"))
@pytest.mark.parametrize(
    ("protected_class", "protected_mutation"),
    _PROTECTED_TRIGGER_MUTATIONS,
)
def test_non_owner_trigger_cannot_write_protected_owner_table(
    tmp_path: Path,
    trigger_schema: str,
    protected_class: str,
    protected_mutation: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        con.execute("CREATE TABLE unrelated_trigger_source(id INTEGER)")
        con.execute(
            f"CREATE {'TEMP ' if trigger_schema == 'temp' else ''}"
            f"TRIGGER arbitrary_{protected_class}_writer "
            "AFTER INSERT ON unrelated_trigger_source BEGIN "
            f"{protected_mutation}; END"
        )
        con.execute("BEGIN")
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            con.execute("INSERT INTO unrelated_trigger_source VALUES(1)")
        con.execute("ROLLBACK")
        assert con.execute(
            "SELECT COUNT(*) FROM unrelated_trigger_source"
        ).fetchone()[0] == 0


@pytest.mark.parametrize("trigger_schema", ("main", "temp"))
@pytest.mark.parametrize("effect", ("select", "log"))
def test_non_owner_benign_trigger_remains_allowed(
    tmp_path: Path,
    trigger_schema: str,
    effect: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        con.execute("CREATE TABLE unrelated_trigger_source(id INTEGER)")
        con.execute("CREATE TABLE unrelated_trigger_log(id INTEGER)")
        body = (
            "SELECT NEW.id"
            if effect == "select"
            else "INSERT INTO unrelated_trigger_log VALUES(NEW.id)"
        )
        con.execute(
            f"CREATE {'TEMP ' if trigger_schema == 'temp' else ''}"
            "TRIGGER benign_unrelated_trigger "
            "AFTER INSERT ON unrelated_trigger_source BEGIN "
            f"{body}; END"
        )
        con.execute("BEGIN")
        con.execute("INSERT INTO unrelated_trigger_source VALUES(1)")
        con.execute("COMMIT")
        assert con.execute(
            "SELECT COUNT(*) FROM unrelated_trigger_source"
        ).fetchone()[0] == 1
        assert con.execute(
            "SELECT COUNT(*) FROM unrelated_trigger_log"
        ).fetchone()[0] == (1 if effect == "log" else 0)


@pytest.mark.parametrize(
    "read_entry",
    ("candidate", "discovery", "reconciliation"),
)
def test_internal_read_connection_installs_owner_write_authorizer(
    tmp_path: Path,
    read_entry: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path, isolation_level=None)
    con.row_factory = sqlite3.Row
    try:
        con.execute("CREATE TABLE unrelated_trigger_source(id INTEGER)")
        con.execute(
            "CREATE TRIGGER indirect_guard_writer "
            "AFTER INSERT ON unrelated_trigger_source BEGIN "
            "UPDATE neg_risk_owner_mutation_guard "
            "SET consumed_journal_id=consumed_journal_id; END"
        )
        if read_entry == "candidate":
            store.validated_candidate_opportunity_count(_connection=con)
        elif read_entry == "discovery":
            store.discovery_status(now_ms=5_000, _connection=con)
        else:
            assert store.current_reconciliation(_connection=con) is None

        con.execute("BEGIN")
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            con.execute("INSERT INTO unrelated_trigger_source VALUES(1)")
        con.execute("ROLLBACK")
    finally:
        con.close()


def test_internal_connection_rejects_existing_temp_canonical_trigger_shadow(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path, isolation_level=None)
    con.row_factory = sqlite3.Row
    try:
        con.execute("CREATE TABLE unrelated_trigger_source(id INTEGER)")
        con.execute(
            f"CREATE TEMP TRIGGER {OWNER_JOURNAL_TRIGGER_NAMES[0]} "
            "AFTER INSERT ON unrelated_trigger_source BEGIN "
            "UPDATE neg_risk_owner_mutation_guard "
            "SET consumed_journal_id=consumed_journal_id; END"
        )

        with pytest.raises(
            ValueError,
            match="invalid-owner-authority-manifest",
        ):
            store.current_reconciliation(_connection=con)
    finally:
        con.close()


@pytest.mark.parametrize("trigger_name", OWNER_JOURNAL_TRIGGER_NAMES)
def test_store_connection_rejects_temp_canonical_trigger_shadow(
    tmp_path: Path,
    trigger_name: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        con.execute("CREATE TABLE unrelated_trigger_source(id INTEGER)")
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            con.execute(
                f"CREATE TEMP TRIGGER {trigger_name} "
                "AFTER INSERT ON unrelated_trigger_source BEGIN SELECT 1; END"
            )


@pytest.mark.parametrize("trigger_schema", ("main", "temp"))
def test_non_owner_indirect_owner_write_rolls_back_store_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    trigger_schema: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    trigger_sql = (
        f"CREATE {'TEMP ' if trigger_schema == 'temp' else ''}"
        "TRIGGER indirect_load_state_owner_writer "
        "AFTER INSERT ON neg_risk_discovery_load_state BEGIN "
        "UPDATE neg_risk_owner_mutation_guard "
        "SET consumed_journal_id=consumed_journal_id; END"
    )
    if trigger_schema == "main":
        with store._connect() as con:
            con.execute(trigger_sql)
    else:
        original_connect = store._connect

        def connect_with_temp_trigger(
            *args: object,
            **kwargs: object,
        ) -> sqlite3.Connection:
            con = original_connect(*args, **kwargs)
            con.execute(trigger_sql)
            return con

        monkeypatch.setattr(store, "_connect", connect_with_temp_trigger)

    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        store.record_discovery_load_decision(
            degraded_reason=None,
            probe_every_cycles=10,
            now_ms=5_000,
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_discovery_load_state"
        ).fetchone()[0] == 0


def test_sqlite_authorizer_exposes_main_and_temp_trigger_source() -> None:
    for trigger_schema in ("main", "temp"):
        con = sqlite3.connect(":memory:")
        try:
            con.execute("CREATE TABLE protected(id INTEGER)")
            con.execute("CREATE TABLE source(id INTEGER)")
            observed_sources: list[str | None] = []

            def authorizer(
                action: int,
                table_name: str | None,
                _column_name: str | None,
                _database: str | None,
                source: str | None,
            ) -> int:
                if action == sqlite3.SQLITE_INSERT and table_name == "protected":
                    observed_sources.append(source)
                return sqlite3.SQLITE_OK

            con.set_authorizer(authorizer)
            con.execute(
                f"CREATE {'TEMP ' if trigger_schema == 'temp' else ''}"
                "TRIGGER source_probe AFTER INSERT ON source BEGIN "
                "INSERT INTO protected VALUES(NEW.id); END"
            )
            con.execute("INSERT INTO source VALUES(1)")
            assert observed_sources == ["source_probe"]
        finally:
            con.close()


def test_trigger_on_non_owner_table_does_not_change_owner_manifest(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        con.execute(
            "CREATE TRIGGER unrelated_market_trigger "
            "AFTER INSERT ON markets BEGIN SELECT 1; END"
        )

    assert store.validated_candidate_opportunity_count() == 0
    store.init_schema()
    store.record_discovery_load_decision(
        degraded_reason=None,
        probe_every_cycles=10,
        now_ms=5_000,
    )


@pytest.mark.parametrize(
    "drift",
    ("type", "notnull", "default", "pk", "check", "order"),
)
def test_owner_table_schema_fingerprint_rejects_semantic_drift(
    tmp_path: Path,
    drift: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        canonical_sql = str(
            con.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' "
                "AND name='neg_risk_owner_mutation_guard'"
            ).fetchone()[0]
        )
        replacements = {
            "type": ("authority_version INTEGER", "authority_version TEXT"),
            "notnull": (
                "migration_state TEXT NOT NULL",
                "migration_state TEXT",
            ),
            "default": (
                "retained_base_id INTEGER NOT NULL DEFAULT 0",
                "retained_base_id INTEGER NOT NULL DEFAULT 1",
            ),
            "pk": (
                "id INTEGER PRIMARY KEY CHECK(id = 1)",
                "id INTEGER CHECK(id = 1)",
                ),
                "check": (
                    "CHECK(authority_version = 4)",
                    "CHECK(authority_version >= 4)",
            ),
            "order": (
                "candidate_aggregate_hash TEXT,\n  discovery_aggregate_hash TEXT",
                "discovery_aggregate_hash TEXT,\n  candidate_aggregate_hash TEXT",
            ),
        }
        old, new = replacements[drift]
        drift_sql = canonical_sql.replace(old, new)
        assert drift_sql != canonical_sql
        columns = tuple(
            str(row["name"])
            for row in con.execute(
                "PRAGMA table_info(neg_risk_owner_mutation_guard)"
            )
        )
        con.execute(
            "ALTER TABLE neg_risk_owner_mutation_guard "
            "RENAME TO neg_risk_owner_mutation_guard_backup"
        )
        con.execute(drift_sql)
        con.execute(
            "INSERT INTO neg_risk_owner_mutation_guard("
            f"{','.join(columns)}) SELECT {','.join(columns)} "
            "FROM neg_risk_owner_mutation_guard_backup"
        )
        con.execute("DROP TABLE neg_risk_owner_mutation_guard_backup")

    with pytest.raises(ValueError, match="invalid-owner-authority-manifest"):
        store.init_schema()


@pytest.mark.parametrize(
    "drift",
    ("missing", "rename", "wrong-columns", "unique", "partial"),
)
def test_owner_index_manifest_rejects_semantic_drift(
    tmp_path: Path,
    drift: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    index_name = "idx_neg_risk_discovery_projection_oldest"
    with store._connect() as con:
        con.execute(f"DROP INDEX {index_name}")
        if drift != "missing":
            replacement = {
                "rename": (
                    "idx_neg_risk_discovery_projection_oldest_renamed",
                    "(visit_anchor_ms,group_id)",
                    "",
                ),
                "wrong-columns": (
                    index_name,
                    "(row_hash,group_id)",
                    "",
                ),
                "unique": (
                    index_name,
                    "(visit_anchor_ms,group_id)",
                    "UNIQUE ",
                ),
                "partial": (
                    index_name,
                    "(visit_anchor_ms,group_id) "
                    "WHERE visit_anchor_ms IS NOT NULL",
                    "",
                ),
            }[drift]
            name, columns, qualifier = replacement
            con.execute(
                f"CREATE {qualifier}INDEX {name} "
                f"ON neg_risk_discovery_group_projection{columns}"
            )

    with pytest.raises(ValueError, match="invalid-owner-authority-manifest"):
        store.init_schema()


def test_discovery_oldest_projection_query_uses_canonical_index(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        plan = tuple(
            str(row["detail"])
            for row in con.execute(
                "EXPLAIN QUERY PLAN SELECT visit_anchor_ms AS oldest "
                "FROM neg_risk_discovery_group_projection "
                "WHERE visit_anchor_ms IS NOT NULL "
                "ORDER BY visit_anchor_ms,group_id LIMIT 1"
            )
        )

    assert any(
        "idx_neg_risk_discovery_projection_oldest" in detail
        for detail in plan
    )
    assert all("TEMP B-TREE" not in detail for detail in plan)


def _drop_v2_guard_columns(con: sqlite3.Connection) -> None:
    columns = {
        str(row["name"])
        for row in con.execute("PRAGMA table_info(neg_risk_owner_mutation_guard)")
    }
    for column in ("migration_state", "authority_version"):
        if column in columns:
            con.execute(
                f"ALTER TABLE neg_risk_owner_mutation_guard DROP COLUMN {column}"
            )


def _grow_a527_owner_window(
    store: OpportunityPerceptionStore,
    group: GroupRevision,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(store_module, "_OWNER_MUTATION_JOURNAL_RETAIN_ROWS", 256)
    for sequence in range(50):
        store.record_candidate_watch_fact(
            group_id=group.group_id,
            membership_hash=group.membership_hash,
            quote_batch_id=None,
            observed_at_ms=4_000 + sequence,
            last_result="unavailable",
            reason="a527-window",
            bundle_cost=None,
            gross_edge_bps=None,
            max_bundle_size=None,
            priority_class="normal",
            consecutive_failures=sequence,
            effective_interval_s=60,
            schedule_reason="a527-window",
            next_due_at_ms=64_000 + sequence,
        )
    monkeypatch.setattr(store_module, "_OWNER_MUTATION_JOURNAL_RETAIN_ROWS", 128)


def test_a527_owner_window_migrates_atomically_to_v4(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-a527", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    _grow_a527_owner_window(store, group, monkeypatch)
    _downgrade_owner_v3_fixture_to_v2(store)
    with store._connect() as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_owner_mutation_journal"
        ).fetchone()[0] > 128
        _drop_v2_guard_columns(con)

    store.init_schema()
    # The migration must produce the exact current fingerprint, not merely
    # values that survive the first transaction.
    store.init_schema()

    with store._connect() as con:
        guard = con.execute(
            "SELECT authority_version,migration_state,candidate_aggregate_hash,"
            "discovery_aggregate_hash FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        assert tuple(guard[:2]) == (4, "complete")
        assert guard["candidate_aggregate_hash"] is not None
        assert guard["discovery_aggregate_hash"] is not None
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_owner_mutation_journal"
        ).fetchone()[0] <= 128


def test_invalid_a527_owner_window_migration_rolls_back_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-a527-bad", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    _grow_a527_owner_window(store, group, monkeypatch)
    _downgrade_owner_v3_fixture_to_v2(store)
    with store._connect() as con:
        _drop_v2_guard_columns(con)
        con.execute(
            "UPDATE neg_risk_owner_mutation_journal SET event_hash='tampered' "
            "WHERE id=(SELECT MIN(id) FROM neg_risk_owner_mutation_journal)"
        )

    with pytest.raises(ValueError, match="invalid-owner-mutation-chain"):
        store.init_schema()

    with store._connect() as con:
        columns = {
            str(row["name"])
            for row in con.execute(
                "PRAGMA table_info(neg_risk_owner_mutation_guard)"
            )
        }
    assert "authority_version" not in columns
    assert "migration_state" not in columns


def test_a527_owner_migration_honors_sqlite_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    group = revision(group_id="g-a527-deadline", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    _grow_a527_owner_window(store, group, monkeypatch)
    _downgrade_owner_v3_fixture_to_v2(store)
    with store._connect() as con:
        _drop_v2_guard_columns(con)

    expired = OpportunityPerceptionStore(db_path, deadline_monotonic=0)
    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        expired.init_schema()

    with store._connect() as con:
        columns = {
            str(row["name"])
            for row in con.execute(
                "PRAGMA table_info(neg_risk_owner_mutation_guard)"
            )
        }
    assert "authority_version" not in columns
    assert "migration_state" not in columns


def test_concurrent_a527_owner_migrations_converge_to_one_v4_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    group = revision(group_id="g-a527-race", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    _grow_a527_owner_window(store, group, monkeypatch)
    _downgrade_owner_v3_fixture_to_v2(store)
    with store._connect() as con:
        _drop_v2_guard_columns(con)
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def migrate() -> None:
        try:
            barrier.wait(timeout=2)
            OpportunityPerceptionStore(db_path).init_schema()
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=migrate) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert errors == []
    with store._connect() as con:
        guard = con.execute(
            "SELECT authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        assert tuple(guard) == (4, "complete")
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_owner_mutation_journal"
        ).fetchone()[0] <= 128


def test_candidate_authority_rolls_checkpoint_beyond_daily_history_bound(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)

    for sequence in range(10_010):
        quoted_at_ms = 3_100 + sequence
        batch = batch_for(
            group,
            quote_batch_id=f"qb-{sequence}",
            quoted_at_ms=quoted_at_ms,
        )
        store.publish_candidate_success(
            batch,
            observed_at_ms=quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="continuous",
            next_due_at_ms=quoted_at_ms + 15_000,
        )

    assert store.validated_candidate_opportunity_count() == 1
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_candidate_authority_checkpoints"
        ).fetchone() == (1,)
        for table in (
            "neg_risk_group_quote_batches",
            "neg_risk_candidate_watch_facts",
            "neg_risk_candidate_success_receipts",
        ):
            assert con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] < 3_000
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_owner_mutation_journal"
        ).fetchone()[0] <= 1_025


def test_candidate_authority_compacts_before_quote_bytes_hit_hard_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        100,
    )
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_BYTES",
        1,
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    for sequence in range(3):
        batch = batch_for(
            group,
            quote_batch_id=f"bytes-{sequence}",
            quoted_at_ms=3_100 + sequence,
        )
        store.publish_candidate_success(
            batch,
            observed_at_ms=batch.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="byte-watermark",
            next_due_at_ms=batch.quoted_at_ms + 15_000,
        )

    assert store.validated_candidate_opportunity_count() == 1
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT generation FROM neg_risk_candidate_authority_checkpoints"
        ).fetchone()[0] >= 1
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_group_quote_batches"
        ).fetchone()[0] == 1


def test_candidate_authority_evicts_inactive_distinct_groups_without_losing_watch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        2,
    )
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_UNCOMPACTED_MAX_ROWS",
        4,
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()

    active = revision(group_id="active", revision=1, token_suffix="active")
    store.publish_group_revision(active)
    active_batch = batch_for(
        active,
        quote_batch_id="active-watch",
        quoted_at_ms=3_100,
    )
    store.publish_candidate_success(
        active_batch,
        observed_at_ms=active_batch.quoted_at_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="active-watch",
        next_due_at_ms=active_batch.quoted_at_ms + 15_000,
    )

    for sequence in range(8):
        group = revision(
            group_id=f"inactive-{sequence}",
            revision=1,
            token_suffix=f"inactive-{sequence}",
            observed_at_ms=4_000 + sequence * 10,
        )
        store.publish_group_revision(group)
        quote = batch_for(
            group,
            quote_batch_id=f"inactive-watch-{sequence}",
            quoted_at_ms=4_001 + sequence * 10,
        )
        store.publish_candidate_success(
            quote,
            observed_at_ms=quote.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="inactive-watch",
            next_due_at_ms=quote.quoted_at_ms + 15_000,
        )
        store.publish_group_revision(
            replace(
                group,
                revision=2,
                observed_at_ms=quote.quoted_at_ms + 1,
                source_cursor=f"closed-{sequence}",
                status="closed",
            )
        )

    refreshed_active_batch = batch_for(
        active,
        quote_batch_id="active-watch-final",
        quoted_at_ms=5_000,
    )
    store.publish_candidate_success(
        refreshed_active_batch,
        observed_at_ms=refreshed_active_batch.quoted_at_ms,
        last_result="watching",
        reason=None,
        bundle_cost=0.9,
        gross_edge_bps=1_000,
        max_bundle_size=10,
        priority_class="high",
        consecutive_failures=0,
        effective_interval_s=15,
        schedule_reason="active-watch-final",
        next_due_at_ms=refreshed_active_batch.quoted_at_ms + 15_000,
    )

    assert store.validated_candidate_opportunity_count() == 1
    current = store.current_quote_batch(
        active.group_id,
        now_ms=10_000,
        max_age_ms=60_000,
    )
    assert current is not None
    assert tuple(leg.yes_token_id for leg in current.legs) == tuple(
        leg.yes_token_id for leg in active.legs
    )
    with sqlite3.connect(store.db_path) as con:
        retained_group_ids = {
            row[0]
            for row in con.execute(
                "SELECT group_id FROM neg_risk_candidate_watch_facts "
                "UNION SELECT group_id FROM neg_risk_group_quote_batches "
                "UNION SELECT group_id FROM neg_risk_candidate_success_receipts"
            )
        }
        assert active.group_id in retained_group_ids
        assert len(retained_group_ids) <= 2
        seeds = con.execute(
            "SELECT seeds_json FROM neg_risk_candidate_authority_checkpoints"
        ).fetchone()
    assert seeds is not None
    assert active.group_id in str(seeds[0])


def test_candidate_authority_supports_more_current_groups_than_history_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        2,
    )
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_UNCOMPACTED_MAX_ROWS",
        4,
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()

    expected_legs: dict[str, tuple[str, ...]] = {}
    for sequence in range(8):
        group = revision(
            group_id=f"active-{sequence}",
            revision=1,
            token_suffix=f"active-{sequence}",
            observed_at_ms=2_000 + sequence * 10,
        )
        store.publish_group_revision(group)
        quote = batch_for(
            group,
            quote_batch_id=f"active-{sequence}-watch",
            quoted_at_ms=3_100 + sequence * 10,
        )
        store.publish_candidate_success(
            quote,
            observed_at_ms=quote.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="active-watch",
            next_due_at_ms=quote.quoted_at_ms + 15_000,
        )
        expected_legs[group.group_id] = tuple(
            leg.yes_token_id for leg in group.legs
        )

    assert store.validated_candidate_opportunity_count() == len(expected_legs)
    with sqlite3.connect(store.db_path) as con:
        for table in (
            "neg_risk_group_quote_batches",
            "neg_risk_candidate_watch_facts",
            "neg_risk_candidate_success_receipts",
        ):
            assert con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] <= 2
    for group_id, token_ids in expected_legs.items():
        current = store.current_quote_batch(
            group_id,
            now_ms=10_000,
            max_age_ms=60_000,
        )
        assert current is not None
        assert tuple(leg.yes_token_id for leg in current.legs) == token_ids


def test_candidate_authority_checkpoint_and_suffix_tampering_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        2,
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    for sequence in range(4):
        batch = batch_for(
            group,
            quote_batch_id=f"qb-{sequence}",
            quoted_at_ms=3_100 + sequence,
        )
        store.publish_candidate_success(
            batch,
            observed_at_ms=batch.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="continuous",
            next_due_at_ms=batch.quoted_at_ms + 15_000,
        )
    assert store.validated_candidate_opportunity_count() == 1

    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE neg_risk_candidate_authority_checkpoints "
            "SET checkpoint_hash='sha256:tampered'"
        )
    with pytest.raises(ValueError, match="invalid-candidate-authority-checkpoint"):
        store.validated_candidate_opportunity_count()

    suffix_store = OpportunityPerceptionStore(tmp_path / "suffix.db")
    suffix_store.init_schema()
    suffix_store.publish_group_revision(group)
    for sequence in range(4):
        batch = batch_for(
            group,
            quote_batch_id=f"suffix-{sequence}",
            quoted_at_ms=4_100 + sequence,
        )
        suffix_store.publish_candidate_success(
            batch,
            observed_at_ms=batch.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="continuous",
            next_due_at_ms=batch.quoted_at_ms + 15_000,
        )
    with sqlite3.connect(suffix_store.db_path) as con:
        con.execute(
            "UPDATE neg_risk_candidate_success_receipts "
            "SET receipt_hash='sha256:tampered' WHERE id=("
            "SELECT MAX(id) FROM neg_risk_candidate_success_receipts)"
        )
    with pytest.raises(ValueError, match="pending-owner-mutation"):
        suffix_store.validated_candidate_opportunity_count()


def _seed_tampered_candidate_checkpoint(
    db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[OpportunityPerceptionStore, GroupRevision]:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        2,
    )
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    for sequence in range(3):
        batch = batch_for(
            group,
            quote_batch_id=f"seed-{sequence}",
            quoted_at_ms=3_100 + sequence,
        )
        store.publish_candidate_success(
            batch,
            observed_at_ms=batch.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="seed",
            next_due_at_ms=batch.quoted_at_ms + 15_000,
        )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE neg_risk_candidate_authority_checkpoints "
            "SET checkpoint_hash='sha256:tampered'"
        )
    return store, group


def test_tampered_candidate_checkpoint_blocks_quote_writer_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, group = _seed_tampered_candidate_checkpoint(
        tmp_path / "quote.db",
        monkeypatch,
    )
    with pytest.raises(ValueError, match="invalid-candidate-authority-checkpoint"):
        store.publish_quote_batch(
            batch_for(group, quote_batch_id="blocked-quote", quoted_at_ms=4_000)
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_group_quote_batches "
            "WHERE id='blocked-quote'"
        ).fetchone() == (0,)


def test_tampered_candidate_checkpoint_blocks_success_writer_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, group = _seed_tampered_candidate_checkpoint(
        tmp_path / "success.db",
        monkeypatch,
    )
    blocked = batch_for(
        group,
        quote_batch_id="blocked-success",
        quoted_at_ms=4_000,
    )
    with pytest.raises(ValueError, match="invalid-candidate-authority-checkpoint"):
        store.publish_candidate_success(
            blocked,
            observed_at_ms=blocked.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="blocked",
            next_due_at_ms=19_000,
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_group_quote_batches "
            "WHERE id='blocked-success'"
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_candidate_watch_facts "
            "WHERE schedule_reason='blocked'"
        ).fetchone() == (0,)


def test_tampered_candidate_checkpoint_blocks_fact_writer_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, group = _seed_tampered_candidate_checkpoint(
        tmp_path / "fact.db",
        monkeypatch,
    )
    with pytest.raises(ValueError, match="invalid-candidate-authority-checkpoint"):
        store.record_candidate_watch_fact(
            group_id=group.group_id,
            membership_hash=group.membership_hash,
            quote_batch_id=None,
            observed_at_ms=4_000,
            last_result="unavailable",
            reason="blocked",
            bundle_cost=None,
            gross_edge_bps=None,
            max_bundle_size=None,
            priority_class="high",
            consecutive_failures=1,
            effective_interval_s=15,
            schedule_reason="blocked",
            next_due_at_ms=19_000,
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_candidate_watch_facts "
            "WHERE schedule_reason='blocked'"
        ).fetchone() == (0,)


def test_candidate_checkpoint_compaction_rolls_back_on_delete_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        2,
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    for sequence in range(2):
        batch = batch_for(group, quote_batch_id=f"qb-{sequence}", quoted_at_ms=3_100 + sequence)
        store.publish_candidate_success(
            batch,
            observed_at_ms=batch.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="continuous",
            next_due_at_ms=batch.quoted_at_ms + 15_000,
        )
    original_consume = store._consume_expected_owner_mutations

    def fail_after_quote_delete(*args: object, **kwargs: object) -> None:
        original_consume(*args, **kwargs)
        if (
            kwargs["table_name"] == "neg_risk_group_quote_batches"
            and kwargs["operation"] == "DELETE"
        ):
            raise sqlite3.IntegrityError("reject compaction")

    monkeypatch.setattr(
        store,
        "_consume_expected_owner_mutations",
        fail_after_quote_delete,
    )
    third = batch_for(group, quote_batch_id="qb-2", quoted_at_ms=3_102)
    with pytest.raises(sqlite3.IntegrityError, match="reject compaction"):
        store.publish_candidate_success(
            third,
            observed_at_ms=third.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="continuous",
            next_due_at_ms=third.quoted_at_ms + 15_000,
        )
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_candidate_authority_checkpoints"
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_candidate_success_receipts"
        ).fetchone() == (2,)
    assert store.validated_candidate_opportunity_count() == 1


def test_owner_journal_consume_crash_rolls_back_business_and_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    with store._connect() as con:
        guard_before = tuple(
            con.execute(
                "SELECT consumed_journal_id,consumed_hash "
                "FROM neg_risk_owner_mutation_guard WHERE id=1"
            ).fetchone()
        )
        journal_before = con.execute(
            "SELECT COUNT(*) FROM neg_risk_owner_mutation_journal"
        ).fetchone()[0]
    original = store._consume_expected_owner_mutation

    def crash_after_consume(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("consume-crash")

    monkeypatch.setattr(store, "_consume_expected_owner_mutation", crash_after_consume)
    with pytest.raises(RuntimeError, match="consume-crash"):
        store.publish_quote_batch(batch_for(group, quote_batch_id="crash-quote"))

    with store._connect() as con:
        assert con.execute(
            "SELECT 1 FROM neg_risk_group_quote_batches WHERE id='crash-quote'"
        ).fetchone() is None
        assert tuple(
            con.execute(
                "SELECT consumed_journal_id,consumed_hash "
                "FROM neg_risk_owner_mutation_guard WHERE id=1"
            ).fetchone()
        ) == guard_before
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_owner_mutation_journal"
        ).fetchone()[0] == journal_before
        assert con.execute(
            "SELECT 1 FROM neg_risk_owner_write_context"
        ).fetchone() is None


@pytest.mark.parametrize(
    "assignment",
    (
        "current_group_count=current_group_count+1",
        "aggregate_digest=lower(hex(randomblob(32)))",
    ),
)
def test_candidate_current_aggregate_direct_mutation_fails_closed(
    tmp_path: Path,
    assignment: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()

    with store._connect() as con:
        con.execute(
            f"UPDATE neg_risk_candidate_current_aggregate SET {assignment} WHERE id=1"
        )

    with pytest.raises(ValueError, match="pending-owner-mutation"):
        store.validated_candidate_opportunity_count()


@pytest.mark.parametrize(
    "tamper_sql",
    (
        "UPDATE neg_risk_owner_mutation_journal "
        "SET event_hash=lower(hex(randomblob(32))) WHERE id=("
        "SELECT MIN(id) FROM neg_risk_owner_mutation_journal)",
        "DELETE FROM neg_risk_owner_mutation_journal WHERE id=("
        "SELECT MAX(id) FROM neg_risk_owner_mutation_journal)",
        "UPDATE neg_risk_owner_mutation_journal "
        "SET previous_hash=lower(hex(randomblob(32))) WHERE id=("
        "SELECT MAX(id) FROM neg_risk_owner_mutation_journal)",
    ),
    ids=("event-hash", "delete-tail", "break-chain"),
)
def test_retained_owner_journal_tamper_fails_closed(
    tmp_path: Path,
    tamper_sql: str,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.publish_group_revision(
        revision(group_id="g-journal", revision=1, token_suffix="a")
    )

    with store._connect() as con:
        con.execute(tamper_sql)

    with pytest.raises(
        ValueError,
        match="invalid-owner-mutation-(?:chain|sequence)",
    ):
        store.validated_candidate_opportunity_count()


def test_candidate_checkpoint_tracks_group_supersede_across_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        2,
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    first = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(first)
    for sequence in range(3):
        batch = batch_for(first, quote_batch_id=f"qb-{sequence}", quoted_at_ms=3_100 + sequence)
        store.publish_candidate_success(
            batch,
            observed_at_ms=batch.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="continuous",
            next_due_at_ms=batch.quoted_at_ms + 15_000,
        )

    changed = revision(
        group_id="g-1",
        revision=2,
        token_suffix="b",
        observed_at_ms=4_000,
    )
    store.publish_group_revision(changed)
    store.record_candidate_watch_fact(
        group_id=changed.group_id,
        membership_hash=changed.membership_hash,
        quote_batch_id=None,
        observed_at_ms=4_001,
        last_result="unavailable",
        reason="membership-changed",
        bundle_cost=None,
        gross_edge_bps=None,
        max_bundle_size=None,
        priority_class="high",
        consecutive_failures=1,
        effective_interval_s=15,
        schedule_reason="retry",
        next_due_at_ms=19_001,
    )
    assert store.validated_candidate_opportunity_count() == 0


def test_candidate_checkpoint_does_not_own_group_only_revision_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        2,
    )
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_UNCOMPACTED_MAX_ROWS",
        2,
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    for revision_number in range(1, 4):
        store.publish_group_revision(
            revision(
                group_id="g-1",
                revision=revision_number,
                token_suffix="a",
                observed_at_ms=1_000 + revision_number,
            )
        )

    assert store.validated_candidate_opportunity_count() == 0
    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM neg_risk_group_revisions").fetchone() == (3,)
        assert con.execute(
            "SELECT COUNT(*) "
            "FROM neg_risk_candidate_authority_checkpoints"
        ).fetchone() == (0,)


def test_candidate_compaction_preserves_shared_group_revision_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "polyarb.perception.store._CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS",
        2,
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    first = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(first)
    for sequence in range(3):
        batch = batch_for(
            first,
            quote_batch_id=f"before-{sequence}",
            quoted_at_ms=3_100 + sequence,
        )
        store.publish_candidate_success(
            batch,
            observed_at_ms=batch.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="before-revision",
            next_due_at_ms=batch.quoted_at_ms + 15_000,
        )

    changed = revision(
        group_id="g-1",
        revision=2,
        token_suffix="b",
        observed_at_ms=4_000,
    )
    store.publish_group_revision(changed)
    for sequence in range(2):
        batch = batch_for(
            changed,
            quote_batch_id=f"after-{sequence}",
            quoted_at_ms=4_100 + sequence,
        )
        store.publish_candidate_success(
            batch,
            observed_at_ms=batch.quoted_at_ms,
            last_result="watching",
            reason=None,
            bundle_cost=0.9,
            gross_edge_bps=1_000,
            max_bundle_size=10,
            priority_class="high",
            consecutive_failures=0,
            effective_interval_s=15,
            schedule_reason="after-revision",
            next_due_at_ms=batch.quoted_at_ms + 15_000,
        )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT revision FROM neg_risk_group_revisions "
            "WHERE group_id='g-1' ORDER BY revision"
        ).fetchall() == [(1,), (2,)]
    assert store.validated_candidate_opportunity_count() == 1


def test_membership_change_invalidates_previous_quote_atomically(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    first = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(first)
    store.publish_quote_batch(batch_for(first, quote_batch_id="qb-1"))

    changed = revision(group_id="g-1", revision=2, token_suffix="b")
    store.publish_group_revision(changed)

    assert store.current_group("g-1") == changed
    assert (
        store.current_quote_batch("g-1", now_ms=10_000, max_age_ms=60_000)
        is None
    )
    with sqlite3.connect(db_path) as con:
        status = con.execute(
            "SELECT status FROM neg_risk_group_quote_batches WHERE id='qb-1'"
        ).fetchone()[0]
    assert status == "superseded"


def test_current_quote_batch_requires_current_complete_all_leg_identity(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)

    wrong_group = replace(batch_for(group, quote_batch_id="qb-wrong-group"), group_id="g-2")
    wrong_hash = replace(
        batch_for(group, quote_batch_id="qb-wrong-hash"),
        membership_hash="other-hash",
    )
    missing_leg = replace(
        batch_for(group, quote_batch_id="qb-missing"),
        legs=batch_for(group, quote_batch_id="ignored").legs[:1],
    )

    with pytest.raises(ValueError, match="group-identity-mismatch"):
        store.publish_quote_batch(wrong_group)
    with pytest.raises(ValueError, match="membership-hash-mismatch"):
        store.publish_quote_batch(wrong_hash)
    with pytest.raises(ValueError, match="quote-leg-identity-mismatch"):
        store.publish_quote_batch(missing_leg)

    expected = batch_for(group, quote_batch_id="qb-1")
    assert store.publish_quote_batch(expected) == expected
    assert store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000) == expected


def test_quote_batch_fails_closed_when_no_certified_group_exists(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")

    with pytest.raises(ValueError, match="certified-group-not-found"):
        store.publish_quote_batch(batch_for(group, quote_batch_id="qb-1"))


def test_current_quote_batch_excludes_stale_and_future_observations(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    store.publish_quote_batch(
        batch_for(group, quote_batch_id="qb-1", quoted_at_ms=3_100)
    )

    assert store.current_quote_batch("g-1", now_ms=4_101, max_age_ms=1_000) is None
    assert store.current_quote_batch("g-1", now_ms=3_000, max_age_ms=1_000) is None


def test_same_membership_revision_keeps_current_quote_authority(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    first = revision(group_id="g-1", revision=1, token_suffix="a")
    quote = batch_for(first, quote_batch_id="qb-1")
    store.publish_group_revision(first)
    store.publish_quote_batch(quote)
    unchanged = replace(
        first,
        revision=2,
        observed_at_ms=2_500,
        source_cursor="cursor-2",
    )

    store.publish_group_revision(unchanged)

    assert store.current_group("g-1") == unchanged
    assert store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000) == quote


def test_revision_numbers_are_append_only_and_monotonic(tmp_path: Path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    current = revision(group_id="g-1", revision=2, token_suffix="a")
    store.publish_group_revision(current)

    with pytest.raises(ValueError, match="group-revision-not-monotonic"):
        store.publish_group_revision(
            revision(group_id="g-1", revision=1, token_suffix="b")
        )

    assert store.current_group("g-1") == current


def test_publish_revision_rejects_forged_membership_hash(tmp_path: Path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    first = revision(group_id="g-1", revision=1, token_suffix="a")
    quote = batch_for(first, quote_batch_id="qb-1")
    store.publish_group_revision(first)
    store.publish_quote_batch(quote)
    changed = revision(group_id="g-1", revision=2, token_suffix="b")
    forged = replace(changed, membership_hash=first.membership_hash)

    with pytest.raises(ValueError, match="membership-hash-mismatch"):
        store.publish_group_revision(forged)

    assert store.current_group("g-1") == first
    assert store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000) == quote


def test_publish_quote_batch_revalidates_complete_model_invariants(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    valid = batch_for(group, quote_batch_id="qb-1")
    forged_leg = replace(valid.legs[0], best_ask_price=float("nan"))
    forged = replace(valid, legs=(forged_leg, *valid.legs[1:]))

    with pytest.raises(ValueError, match="invalid-best-ask-price"):
        store.publish_quote_batch(forged)

    assert store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000) is None


def test_publish_certified_revision_revalidates_model_invariants(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    valid = revision(group_id="g-1", revision=1, token_suffix="a")
    one_leg = valid.legs[:1]
    forged = replace(
        valid,
        membership_hash=GroupRevision.membership_digest(one_leg),
        legs=one_leg,
    )

    with pytest.raises(ValueError, match="incomplete-group-membership"):
        store.publish_group_revision(forged)

    assert store.current_group("g-1") is None


def test_current_reads_reject_corrupt_ordered_group_membership(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    store.publish_quote_batch(batch_for(group, quote_batch_id="qb-1"))

    with sqlite3.connect(db_path) as con:
        con.execute(
            "UPDATE neg_risk_group_revisions SET legs_json=? "
            "WHERE group_id='g-1' AND revision=1",
            (OpportunityPerceptionStore._group_legs_json(tuple(reversed(group.legs))),),
        )

    with pytest.raises(ValueError, match="pending-owner-mutation"):
        store.current_group("g-1")
    with pytest.raises(ValueError, match="pending-owner-mutation"):
        store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000)


def test_current_quote_rejects_corrupt_ordered_quote_membership(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    quote = batch_for(group, quote_batch_id="qb-1")
    store.publish_group_revision(group)
    store.publish_quote_batch(quote)

    with sqlite3.connect(db_path) as con:
        con.execute(
            "UPDATE neg_risk_group_quote_batches SET legs_json=? WHERE id='qb-1'",
            (
                OpportunityPerceptionStore._quote_legs_json(
                    tuple(reversed(quote.legs))
                ),
            ),
        )

    with pytest.raises(ValueError, match="pending-owner-mutation"):
        store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000)


class CoordinatedQuoteReadStore(OpportunityPerceptionStore):
    def __init__(
        self,
        db_path: Path,
        *,
        query_ready: threading.Event,
        revision_published: threading.Event,
    ) -> None:
        super().__init__(db_path)
        self._query_ready = query_ready
        self._revision_published = revision_published

    def _current_quote_row(
        self,
        con: sqlite3.Connection,
        group_id: str,
        now_ms: int,
        max_age_ms: int,
    ) -> sqlite3.Row | None:
        self._query_ready.set()
        assert self._revision_published.wait(timeout=1)
        return super()._current_quote_row(con, group_id, now_ms, max_age_ms)


def test_same_hash_revocation_is_observed_before_atomic_quote_read(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    publisher = OpportunityPerceptionStore(db_path)
    publisher.init_schema()
    certified = revision(group_id="g-1", revision=1, token_suffix="a")
    publisher.publish_group_revision(certified)
    publisher.publish_quote_batch(batch_for(certified, quote_batch_id="qb-1"))
    query_ready = threading.Event()
    revision_published = threading.Event()
    reader = CoordinatedQuoteReadStore(
        db_path,
        query_ready=query_ready,
        revision_published=revision_published,
    )
    outcome: list[GroupQuoteBatch | None] = []

    worker = threading.Thread(
        target=lambda: outcome.append(
            reader.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000)
        )
    )
    worker.start()
    assert query_ready.wait(timeout=1)
    publisher.publish_group_revision(
        replace(
            certified,
            revision=2,
            observed_at_ms=2_500,
            source_cursor="cursor-2",
            status="stale",
        )
    )
    revision_published.set()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert outcome == [None]


class TracingQuoteReadStore(OpportunityPerceptionStore):
    def __init__(self, db_path: Path) -> None:
        super().__init__(db_path)
        self.statements: list[str] = []

    def _connect(self) -> sqlite3.Connection:
        con = super()._connect()
        con.set_trace_callback(self.statements.append)
        return con


def test_quote_authority_uses_one_statement_for_group_and_quote(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    publisher = OpportunityPerceptionStore(db_path)
    publisher.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    quote = batch_for(group, quote_batch_id="qb-1")
    publisher.publish_group_revision(group)
    publisher.publish_quote_batch(quote)
    reader = TracingQuoteReadStore(db_path)

    assert reader.current_quote_batch(
        "g-1", now_ms=3_200, max_age_ms=1_000
    ) == quote

    authority_reads = [
        statement
        for statement in reader.statements
        if statement.lstrip().upper().startswith(("SELECT", "WITH"))
        and "sqlite_master" not in statement
        and "sqlite_temp_master" not in statement
        and (
            "neg_risk_group_revisions" in statement
            or "neg_risk_group_quote_batches" in statement
        )
    ]
    assert len(authority_reads) == 1
    assert "neg_risk_group_revisions" in authority_reads[0]
    assert "neg_risk_group_quote_batches" in authority_reads[0]
