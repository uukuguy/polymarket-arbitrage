from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from decimal import Decimal
from pathlib import Path

import pytest

import polyarb.perception.store as store_module
from polyarb.cli_discovery import main as discovery_status_main
from polyarb.clients.gamma_client import EventPage
from polyarb.perception.candidate_watcher import (
    CandidateWatcher,
    CandidateWatcherRuntime,
    CandidateWatcherScheduler,
    IntervalController,
)
from polyarb.perception.discovery import (
    CandidateFreshness,
    DiscoveryLoadController,
    DiscoveryWorker,
    compose_candidate_group_ids,
)
from polyarb.perception.group_structure import (
    GroupStructureReader,
    GroupStructureUnavailableError,
)
from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.store import (
    DiscoveryAdmissionProof,
    OpportunityPerceptionStore,
)


def _event(
    *,
    event_id: str,
    group_id: str,
    liquidity: str = "100",
    volume: str = "200",
    valid: bool = True,
    augmented: bool = False,
) -> dict:
    markets = [
        {
            "id": f"{group_id}-m1",
            "conditionId": f"{group_id}-c1",
            "clobTokenIds": json.dumps([f"{group_id}-yes1", f"{group_id}-no1"]),
            "question": "One?",
            "active": True,
            "closed": False,
            "negRiskOther": False,
            "groupItemTitle": "One",
        },
        {
            "id": f"{group_id}-m2",
            "conditionId": f"{group_id}-c2",
            "clobTokenIds": json.dumps([f"{group_id}-yes2", f"{group_id}-no2"]),
            "question": "Two?",
            "active": True,
            "closed": False,
            "negRiskOther": False,
            "groupItemTitle": "Two",
        },
    ]
    if not valid:
        markets[1]["active"] = "unknown"
    return {
        "id": event_id,
        "slug": event_id,
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": augmented,
        "negRiskMarketID": group_id,
        "liquidity": liquidity,
        "volume": volume,
        "markets": markets,
    }


class FakeGamma:
    def __init__(self, page: EventPage | BaseException) -> None:
        self.page = page
        self.calls: list[tuple[str | None, int]] = []

    async def fetch_active_event_page(
        self,
        cursor: str | None,
        limit: int,
    ) -> EventPage:
        self.calls.append((cursor, limit))
        if isinstance(self.page, BaseException):
            raise self.page
        return self.page


def _page(
    *events: dict,
    requested_cursor: str | None = "c-1",
    next_cursor: str | None = "c-2",
    completed: bool = False,
) -> EventPage:
    return EventPage(
        events=events,
        requested_cursor=requested_cursor,
        next_cursor=next_cursor,
        completed=completed,
        started_at_ms=9_900,
        finished_at_ms=10_000,
    )


def _store(tmp_path: Path) -> OpportunityPerceptionStore:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_discovery_state("
            "id,next_cursor,completed,last_started_at_ms,last_finished_at_ms,"
            "page_event_count,groups_seen,promoted_count"
            ") VALUES (1,'c-1',0,0,0,0,0,0)"
        )
    return store


def _admission_proof() -> DiscoveryAdmissionProof:
    return DiscoveryAdmissionProof(
        effective_capacity=2,
        candidate_max_wait_ms=60_000,
        selection_budget_ms=6_000,
        poll_interval_ms=1_000,
        group_timeout_ms=10_000,
        terminal_write_budget_ms=5_000,
        high_burst_groups=1,
        reserved_non_high_slots=2,
    )


def _publish_empty_discovery_sweep(
    store: OpportunityPerceptionStore,
    *,
    sweep: int,
) -> None:
    proof = store.discovery_admission_proof()
    cursor = f"sweep-{sweep}-page-2"
    store.publish_discovery_batch(
        requested_cursor=None,
        next_cursor=cursor,
        completed=False,
        started_at_ms=sweep * 1_000,
        finished_at_ms=sweep * 1_000 + 10,
        page_event_count=0,
        candidates=(),
        admission_proof=proof,
    )
    store.publish_discovery_batch(
        requested_cursor=cursor,
        next_cursor=None,
        completed=True,
        started_at_ms=sweep * 1_000 + 20,
        finished_at_ms=sweep * 1_000 + 30,
        page_event_count=0,
        candidates=(),
        admission_proof=proof,
    )


def _publish_empty_discovery_page(
    store: OpportunityPerceptionStore,
    *,
    sequence: int,
    requested_cursor: str | None,
    completed: bool = False,
) -> str | None:
    next_cursor = None if completed else f"long-sweep-{sequence + 1}"
    store.publish_discovery_batch(
        requested_cursor=requested_cursor,
        next_cursor=next_cursor,
        completed=completed,
        started_at_ms=sequence * 100,
        finished_at_ms=sequence * 100 + 10,
        page_event_count=0,
        candidates=(),
        admission_proof=store.discovery_admission_proof(),
    )
    return next_cursor


def _publish_quote(
    store: OpportunityPerceptionStore,
    group_id: str,
    *,
    quoted_at_ms: int,
) -> None:
    group = store.current_group(group_id)
    store.publish_quote_batch(
        GroupQuoteBatch.complete(
            group_id=group_id,
            membership_hash=group.membership_hash,
            quote_batch_id=f"qb-{group_id}-{quoted_at_ms}",
            started_at_ms=quoted_at_ms - 1,
            quoted_at_ms=quoted_at_ms,
            legs=tuple(
                GroupQuoteLeg(
                    leg.yes_token_id,
                    group.membership_hash,
                    0.4,
                    10,
                    "executable",
                )
                for leg in group.legs
            ),
        )
    )


def test_discovery_checkpoint_bounds_status_history_reads(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_HIGH_ROWS", 4, raising=False
    )
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_LOW_ROWS", 2, raising=False
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.configure_discovery_admission(_admission_proof(), now_ms=0)
    for sweep in range(1, 6):
        _publish_empty_discovery_sweep(store, sweep=sweep)

    statements: list[str] = []
    with store._connect() as con:
        con.set_trace_callback(statements.append)
        status = store.discovery_status(now_ms=6_000, _connection=con)
        checkpoint = con.execute(
            "SELECT through_batch_id FROM neg_risk_discovery_authority_checkpoints "
            "WHERE id=1"
        ).fetchone()
        retained = con.execute(
            "SELECT COUNT(*) FROM neg_risk_discovery_batches"
        ).fetchone()[0]

    assert status.completed is True
    assert checkpoint is not None
    assert retained <= 4
    history_reads = [
        statement
        for statement in statements
        if "SELECT * FROM neg_risk_discovery_batches" in statement
        and statement.lstrip().upper().startswith("SELECT")
    ]
    assert history_reads
    assert all("WHERE id>" in statement for statement in history_reads)


def test_discovery_status_never_scans_lifecycle_authority_tables(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.configure_discovery_admission(_admission_proof(), now_ms=0)
    _publish_empty_discovery_sweep(store, sweep=1)

    statements: list[str] = []
    with store._connect() as con:
        con.set_trace_callback(statements.append)
        store.discovery_status(now_ms=1_000, _connection=con)

    normalized = [" ".join(statement.lower().split()) for statement in statements]
    forbidden_full_lifecycle_reads = (
        "from neg_risk_group_revisions where status='certified' group by",
        "select distinct group_id from neg_risk_candidate_watch_facts",
        "from neg_risk_candidate_watch_facts where last_result='unavailable'",
        "select * from neg_risk_candidate_attempt_starts order by id",
        "select * from neg_risk_candidate_admissions order by id",
    )
    observed = [
        fragment
        for fragment in forbidden_full_lifecycle_reads
        if any(fragment in statement for statement in normalized)
    ]
    assert observed == []


def test_discovery_status_projection_tamper_fails_closed(tmp_path: Path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        con.execute(
            "UPDATE neg_risk_discovery_status_projection "
            "SET generation=generation+1"
        )

    with pytest.raises(ValueError, match="invalid-discovery-status-projection"):
        store.discovery_status(now_ms=1)


def test_discovery_projection_write_failure_rolls_back_owner_mutation(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    with store._connect() as con:
        con.execute(
            "CREATE TRIGGER reject_discovery_projection BEFORE UPDATE "
            "ON neg_risk_discovery_status_projection BEGIN "
            "SELECT RAISE(ABORT,'reject discovery projection'); END"
        )
    group = GroupRevision.certified(
        group_id="g-rollback",
        event_id="e-rollback",
        revision=1,
        started_at_ms=1,
        observed_at_ms=2,
        source_cursor="rollback",
        legs=(
            GroupLeg("m-1", "c-1", "yes-1", "One"),
            GroupLeg("m-2", "c-2", "yes-2", "Two"),
        ),
    )

    with pytest.raises(sqlite3.IntegrityError, match="reject discovery projection"):
        store.publish_group_revision(group)

    assert store.current_group(group.group_id) is None


def test_discovery_status_projection_honors_deadline(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    OpportunityPerceptionStore(db_path).init_schema()
    expired = OpportunityPerceptionStore(db_path, deadline_monotonic=0)

    with pytest.raises(TimeoutError, match="discovery-status-deadline"):
        expired.discovery_status(now_ms=1)


def test_discovery_checkpoint_segments_one_long_sweep_and_terminal_commits(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_HIGH_ROWS", 3, raising=False
    )
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_LOW_ROWS", 1, raising=False
    )
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_UNCOMPACTED_MAX_ROWS", 4, raising=False
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.configure_discovery_admission(_admission_proof(), now_ms=0)
    cursor = None
    for sequence in range(1, 9):
        cursor = _publish_empty_discovery_page(
            store,
            sequence=sequence,
            requested_cursor=cursor,
        )
        status = store.discovery_status(now_ms=sequence * 100 + 10)
        assert status.completed is False
        assert status.next_cursor == cursor
    with store._connect() as con:
        anchor = json.loads(
            con.execute(
                "SELECT anchor_json FROM neg_risk_discovery_authority_checkpoints "
                "WHERE id=1"
            ).fetchone()[0]
        )
        assert anchor["batch"]["completed"] == 0
        assert anchor["batch"]["sweep_id"] == 1
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_discovery_batches"
        ).fetchone()[0] <= 3

    _publish_empty_discovery_page(
        store,
        sequence=9,
        requested_cursor=cursor,
        completed=True,
    )

    assert store.discovery_status(now_ms=910).completed is True


def test_discovery_incomplete_checkpoint_prune_failure_rolls_back_page(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_HIGH_ROWS", 2, raising=False
    )
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_LOW_ROWS", 1, raising=False
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.configure_discovery_admission(_admission_proof(), now_ms=0)
    cursor = _publish_empty_discovery_page(
        store,
        sequence=1,
        requested_cursor=None,
    )
    cursor = _publish_empty_discovery_page(
        store,
        sequence=2,
        requested_cursor=cursor,
    )
    with store._connect() as con:
        con.execute(
            "CREATE TRIGGER reject_incomplete_discovery_compaction BEFORE DELETE "
            "ON neg_risk_discovery_batches BEGIN "
            "SELECT RAISE(ABORT,'reject incomplete discovery compaction'); END"
        )

    with pytest.raises(
        sqlite3.IntegrityError,
        match="reject incomplete discovery compaction",
    ):
        _publish_empty_discovery_page(
            store,
            sequence=3,
            requested_cursor=cursor,
        )

    status = store.discovery_status(now_ms=210)
    assert status.next_cursor == cursor
    with store._connect() as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_discovery_batches"
        ).fetchone()[0] == 2
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_discovery_authority_checkpoints"
        ).fetchone()[0] == 0


def test_discovery_incomplete_checkpoint_anchor_tamper_fails_closed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_HIGH_ROWS", 2, raising=False
    )
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_LOW_ROWS", 1, raising=False
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.configure_discovery_admission(_admission_proof(), now_ms=0)
    cursor = None
    for sequence in range(1, 4):
        cursor = _publish_empty_discovery_page(
            store,
            sequence=sequence,
            requested_cursor=cursor,
        )
    with store._connect() as con:
        con.execute(
            "UPDATE neg_risk_discovery_authority_checkpoints "
            "SET anchor_json=json_set(anchor_json,'$.batch.next_cursor','tampered') "
            "WHERE id=1"
        )

    with pytest.raises(ValueError, match="invalid-discovery-authority-checkpoint"):
        store.discovery_status(now_ms=310)


def test_discovery_checkpoint_survives_sample_rowid_reuse(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_HIGH_ROWS", 2, raising=False
    )
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_LOW_ROWS", 1, raising=False
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.configure_discovery_admission(_admission_proof(), now_ms=0)
    for sweep in range(1, 5):
        cursor = f"sample-sweep-{sweep}"
        page = EventPage(
            events=(_event(event_id="e-1", group_id="g-1"),),
            requested_cursor=None,
            next_cursor=cursor,
            completed=False,
            started_at_ms=sweep * 1_000,
            finished_at_ms=sweep * 1_000 + 10,
        )
        store.publish_discovery_batch(
            requested_cursor=None,
            next_cursor=cursor,
            completed=False,
            started_at_ms=page.started_at_ms,
            finished_at_ms=page.finished_at_ms,
            page_event_count=1,
            candidates=DiscoveryWorker._normalize_page(page),
            admission_proof=store.discovery_admission_proof(),
        )
        store.publish_discovery_batch(
            requested_cursor=cursor,
            next_cursor=None,
            completed=True,
            started_at_ms=sweep * 1_000 + 20,
            finished_at_ms=sweep * 1_000 + 30,
            page_event_count=0,
            candidates=(),
            admission_proof=store.discovery_admission_proof(),
        )

    assert store.discovery_status(now_ms=5_000).completed is True
    assert store.coverage_windows(now_ms=5_000).by_minutes[15].raw_fraction == Decimal(
        "1"
    )


def test_discovery_checkpoint_tamper_and_prune_failure_fail_closed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_HIGH_ROWS", 2, raising=False
    )
    monkeypatch.setattr(
        store_module, "_DISCOVERY_AUTHORITY_COMPACT_LOW_ROWS", 1, raising=False
    )
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.configure_discovery_admission(_admission_proof(), now_ms=0)
    _publish_empty_discovery_sweep(store, sweep=1)
    with store._connect() as con:
        con.execute(
            "CREATE TRIGGER reject_discovery_compaction BEFORE DELETE "
            "ON neg_risk_discovery_batches BEGIN "
            "SELECT RAISE(ABORT,'reject discovery compaction'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="reject discovery compaction"):
        _publish_empty_discovery_sweep(store, sweep=2)

    with store._connect() as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_discovery_batches"
        ).fetchone()[0] == 2
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_discovery_authority_checkpoints"
        ).fetchone()[0] == 0
        con.execute("DROP TRIGGER reject_discovery_compaction")
    _publish_empty_discovery_sweep(store, sweep=2)
    with store._connect() as con:
        con.execute(
            "UPDATE neg_risk_discovery_authority_checkpoints "
            "SET checkpoint_hash='sha256:tampered' WHERE id=1"
        )
    with pytest.raises(ValueError, match="invalid-discovery-authority-checkpoint"):
        store.discovery_status(now_ms=3_000)


def test_discovery_checkpoint_legacy_schema_upgrade_is_idempotent(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    store.configure_discovery_admission(_admission_proof(), now_ms=0)
    _publish_empty_discovery_sweep(store, sweep=1)
    with store._connect() as con:
        con.execute("DROP TABLE neg_risk_discovery_authority_checkpoints")

    store.init_schema()
    store.init_schema()

    assert store.discovery_status(now_ms=2_000).completed is True
    with store._connect() as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_discovery_authority_checkpoints"
        ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_discovery_commits_rows_promotions_coverage_and_cursor_atomically(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(
        _page(
            _event(event_id="e-1", group_id="g-1", liquidity="100"),
            _event(event_id="e-2", group_id="g-2", liquidity="300"),
        )
    )
    worker = DiscoveryWorker(
        gamma=gamma,
        store=store,
        page_limit=2,
        clock_ms=lambda: 10_000,
    )

    result = await worker.run_batch()

    assert result.groups_seen == 2
    assert result.promoted_group_ids == ("g-2",)
    assert store.discovery_cursor() == "c-2"
    assert store.group_schedule("g-1").last_discovered_at_ms == result.finished_at_ms
    assert store.current_group("g-1").status == "certified"
    assert tuple(
        leg.yes_token_id for leg in store.current_group("g-1").legs
    ) == ("g-1-yes1", "g-1-yes2")
    assert store.promoted_group_ids() == ("g-2",)
    assert store.current_group("g-1").status == "certified"
    assert store.group_schedule("g-1").promoted_at_ms is None
    coverage = store.coverage_windows(now_ms=10_000)
    assert coverage.by_minutes[15].raw_fraction == Decimal("1")
    assert coverage.by_minutes[15].liquidity_weighted_fraction == Decimal("1")


@pytest.mark.asyncio
async def test_discovery_resource_disabled_never_reads_decision(tmp_path: Path) -> None:
    class Store(OpportunityPerceptionStore):
        def latest_resource_decision(self, **_kwargs):
            raise AssertionError("disabled discovery consumed resource decision")

    store = Store(tmp_path / "state.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_discovery_state("
            "id,next_cursor,completed,last_started_at_ms,last_finished_at_ms,"
            "page_event_count,groups_seen,promoted_count"
            ") VALUES (1,'c-1',0,0,0,0,0,0)"
        )
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-1")))
    result = await DiscoveryWorker(
        gamma=gamma,
        store=store,
        clock_ms=lambda: 10_000,
        require_resource_decision=False,
    ).run_batch()

    assert result.groups_seen == 1


@pytest.mark.asyncio
async def test_discovery_rollback_never_advances_cursor_on_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-1")))
    original = store._insert_discovery_schedule

    def fail_after_row(*args, **kwargs):
        original(*args, **kwargs)
        raise sqlite3.OperationalError("injected")

    monkeypatch.setattr(store, "_insert_discovery_schedule", fail_after_row)
    worker = DiscoveryWorker(gamma=gamma, store=store, clock_ms=lambda: 10_000)

    with pytest.raises(sqlite3.OperationalError, match="injected"):
        await worker.run_batch()

    assert store.discovery_cursor() == "c-1"
    assert store.group_schedule("g-1") is None
    assert store.coverage_windows(now_ms=10_000).known_groups == 0


@pytest.mark.asyncio
async def test_upstream_or_normalization_failure_never_advances_cursor(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    upstream = DiscoveryWorker(
        gamma=FakeGamma(RuntimeError("upstream")),
        store=store,
        clock_ms=lambda: 10_000,
    )
    with pytest.raises(RuntimeError, match="upstream"):
        await upstream.run_batch()
    assert store.discovery_cursor() == "c-1"

    invalid = DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-1"),
                _event(event_id="e-1", group_id="g-different"),
            )
        ),
        store=store,
        clock_ms=lambda: 10_000,
    )
    with pytest.raises(RuntimeError, match="conflict"):
        await invalid.run_batch()
    assert store.discovery_cursor() == "c-1"


@pytest.mark.asyncio
async def test_duplicate_group_identity_in_one_page_fails_batch_closed(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-1"),
                _event(event_id="e-2", group_id="g-1"),
            )
        ),
        store=store,
    )

    with pytest.raises(ValueError, match="duplicate-discovery-group"):
        await worker.run_batch()

    assert store.discovery_cursor() == "c-1"
    assert store.group_schedule("g-1") is None


@pytest.mark.asyncio
async def test_same_group_and_membership_cannot_migrate_event_identity(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    before = store.current_group("g-1")
    schedule_before = store.group_schedule("g-1")
    migrated = _event(event_id="e-2", group_id="g-1")
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(migrated,),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=20_000,
                finished_at_ms=20_100,
            )
        ),
        store=store,
    )

    with pytest.raises(ValueError, match="event-identity-conflict"):
        await worker.run_batch()

    assert store.discovery_cursor() == "c-2"
    assert store.current_group("g-1") == before
    assert store.group_schedule("g-1") == schedule_before


@pytest.mark.asyncio
async def test_incomplete_first_sight_binds_event_identity_for_later_recovery(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(_event(event_id="e-1", group_id="g-1", valid=False))
        ),
        store=store,
    ).run_batch()
    schedule_before = store.group_schedule("g-1")

    conflicting = DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(
                    _event(event_id="e-3", group_id="a-new"),
                    _event(event_id="e-2", group_id="g-1"),
                ),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=20_000,
                finished_at_ms=20_100,
            )
        ),
        store=store,
    )

    with pytest.raises(ValueError, match="event-identity-conflict"):
        await conflicting.run_batch()

    assert store.discovery_cursor() == "c-2"
    assert store.group_schedule("g-1") == schedule_before
    assert store.current_group("g-1") is None
    assert store.group_schedule("a-new") is None
    assert store.current_group("a-new") is None


@pytest.mark.asyncio
async def test_incomplete_first_sight_recovers_under_same_event_identity(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(_event(event_id="e-1", group_id="g-1", valid=False))
        ),
        store=store,
    ).run_batch()

    recovered = await DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(_event(event_id="e-1", group_id="g-1"),),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=20_000,
                finished_at_ms=20_100,
            )
        ),
        store=store,
    ).run_batch()

    assert recovered.promoted_group_ids == ("g-1",)
    assert store.group_schedule("g-1").event_id == "e-1"
    assert store.current_group("g-1").event_id == "e-1"


@pytest.mark.asyncio
async def test_restart_uses_durable_cursor_and_terminal_page_restarts_sweep(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    terminal_gamma = FakeGamma(
        _page(
            _event(event_id="e-1", group_id="g-1"),
            requested_cursor="c-1",
            next_cursor=None,
            completed=True,
        )
    )
    await DiscoveryWorker(
        gamma=terminal_gamma,
        store=store,
        clock_ms=lambda: 10_000,
    ).run_batch()

    restart_gamma = FakeGamma(
        _page(requested_cursor=None, next_cursor="new-cursor")
    )
    await DiscoveryWorker(
        gamma=restart_gamma,
        store=store,
        clock_ms=lambda: 20_000,
    ).run_batch()

    assert terminal_gamma.calls == [("c-1", 100)]
    assert restart_gamma.calls == [(None, 100)]
    assert store.discovery_cursor() == "new-cursor"
    assert discovery_status_main(
        ["--db-path", str(tmp_path / "state.db"), "--now-ms", "20000"]
    ) == 0


@pytest.mark.asyncio
async def test_incomplete_and_unsupported_membership_fail_closed(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(
                    event_id="e-1",
                    group_id="g-1",
                    augmented=True,
                )
            )
        ),
        store=store,
        clock_ms=lambda: 10_000,
    )

    result = await worker.run_batch()

    assert result.promoted_group_ids == ()
    assert store.group_schedule("g-1").quality == "complete-unsupported"
    assert store.promoted_group_ids() == ()
    assert store.current_group("g-1") is None
    assert store.discovery_status(now_ms=10_001).groups_seen == 1


@pytest.mark.asyncio
async def test_incomplete_first_sample_without_revision_is_valid_history(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(_event(event_id="e-1", group_id="g-1", valid=False))
        ),
        store=store,
    ).run_batch()

    assert store.current_group("g-1") is None
    assert store.group_schedule("g-1").quality == "incomplete-source"
    assert store.discovery_status(now_ms=10_001).groups_seen == 1


@pytest.mark.asyncio
async def test_status_rejects_forged_nonlatest_incomplete_sample_identity(
    tmp_path: Path,
    capsys,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(_event(event_id="e-1", group_id="g-1", valid=False))
        ),
        store=store,
    ).run_batch()
    await DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(_event(event_id="e-2", group_id="g-2"),),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=20_000,
                finished_at_ms=20_100,
            )
        ),
        store=store,
    ).run_batch()
    with sqlite3.connect(tmp_path / "state.db") as con:
        con.execute(
            "UPDATE neg_risk_discovery_batch_samples "
            "SET group_id='ghost',event_id='ghost-event',"
            "membership_hash='ghost-hash' WHERE batch_id=("
            "SELECT MIN(id) FROM neg_risk_discovery_batches)"
        )

    assert discovery_status_main(
        ["--db-path", str(tmp_path / "state.db"), "--now-ms", "20101"]
    ) == 2
    assert "invalid discovery state" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_status_rejects_attempt_without_real_admission_as_of_identity(
    tmp_path: Path,
    capsys,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    revision = store.current_group("g-1")
    with sqlite3.connect(tmp_path / "state.db") as con:
        con.execute(
            "INSERT INTO neg_risk_candidate_admissions("
            "group_id,event_id,membership_hash,promoted_at_ms,"
            "candidate_start_deadline_at_ms,effective_capacity,"
            "candidate_max_wait_ms,selection_budget_ms,poll_interval_ms,"
            "group_timeout_ms,terminal_write_budget_ms,"
            "attempt_start_write_budget_ms,high_burst_groups,"
            "reserved_non_high_slots,effective_start_bound_ms,recorded_at_ms"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "g-1",
                "e-1",
                revision.membership_hash,
                1,
                60_001,
                1,
                60_000,
                6_000,
                1_000,
                30_000,
                5_000,
                5_000,
                1,
                3,
                47_000,
                2,
            ),
        )
        con.execute(
            "INSERT INTO neg_risk_candidate_attempt_starts("
            "group_id,event_id,membership_hash,promoted_at_ms,"
            "candidate_max_wait_ms,started_at_ms,"
            "candidate_start_deadline_at_ms,deadline_breached"
            ") VALUES (?,?,?,?,?,?,?,0)",
            (
                "g-1",
                "e-1",
                revision.membership_hash,
                1,
                60_000,
                2,
                60_001,
            ),
        )

    assert discovery_status_main(
        ["--db-path", str(tmp_path / "state.db"), "--now-ms", "10001"]
    ) == 2
    assert "invalid discovery state" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_incomplete_rediscovery_revokes_prior_group_and_quote_authority(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    group = store.current_group("g-1")
    quote = GroupQuoteBatch.complete(
        group_id="g-1",
        membership_hash=group.membership_hash,
        quote_batch_id="qb-1",
        started_at_ms=10_001,
        quoted_at_ms=10_002,
        legs=tuple(
            GroupQuoteLeg(
                leg.yes_token_id,
                group.membership_hash,
                0.4,
                10,
                "executable",
            )
            for leg in group.legs
        ),
    )
    store.publish_quote_batch(quote)
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(
                    _event(
                        event_id="e-1",
                        group_id="g-1",
                        augmented=True,
                    ),
                ),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=20_000,
                finished_at_ms=20_100,
            )
        ),
        store=store,
    )

    await worker.run_batch()

    assert store.discovery_cursor() == "c-3"
    assert store.group_schedule("g-1").quality == "complete-unsupported"
    assert store.promoted_group_ids() == ()
    assert store.current_group("g-1").status == "invalidated"
    assert store.current_quote_batch("g-1", 20_100, 60_000) is None
    with pytest.raises(GroupStructureUnavailableError):
        await GroupStructureReader(store).read_group("g-1")


@pytest.mark.asyncio
async def test_failed_revocation_batch_rolls_back_authority_and_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    _publish_quote(store, "g-1", quoted_at_ms=10_100)
    original = store._insert_discovery_schedule

    def fail_after_revocation(*args, **kwargs):
        original(*args, **kwargs)
        raise sqlite3.OperationalError("injected-revocation")

    monkeypatch.setattr(store, "_insert_discovery_schedule", fail_after_revocation)
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(
                    _event(
                        event_id="e-1",
                        group_id="g-1",
                        augmented=True,
                    ),
                ),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=20_000,
                finished_at_ms=20_100,
            )
        ),
        store=store,
    )

    with pytest.raises(sqlite3.OperationalError, match="injected-revocation"):
        await worker.run_batch()

    assert store.discovery_cursor() == "c-2"
    assert store.current_group("g-1").status == "certified"
    assert store.current_quote_batch("g-1", 20_100, 60_000) is not None


@pytest.mark.asyncio
async def test_coverage_windows_use_exact_discovery_samples_and_liquidity_weights(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-light", liquidity="100"),
                _event(event_id="e-2", group_id="g-heavy", liquidity="300"),
            )
        ),
        store=store,
    ).run_batch()
    await DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(
                    _event(
                        event_id="e-2",
                        group_id="g-heavy",
                        liquidity="300",
                    ),
                ),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=999_900,
                finished_at_ms=1_000_000,
            )
        ),
        store=store,
    ).run_batch()

    coverage = store.coverage_windows(now_ms=1_000_000)

    assert coverage.by_minutes[15].visited_groups == 1
    assert coverage.by_minutes[15].raw_fraction == Decimal("0.5")
    assert coverage.by_minutes[15].liquidity_weighted_fraction == Decimal("0.75")
    assert coverage.by_minutes[30].raw_fraction == Decimal("1")
    assert coverage.by_minutes[60].raw_fraction == Decimal("1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    [
        "UPDATE neg_risk_discovery_state SET groups_seen=999",
        "UPDATE neg_risk_group_schedule SET priority_score='999'",
        "UPDATE neg_risk_group_schedule SET first_discovered_at_ms=999999",
        "UPDATE neg_risk_group_schedule SET activity_rank='NaN'",
        "UPDATE neg_risk_group_schedule SET promoted_at_ms=NULL",
        "INSERT INTO neg_risk_discovery_load_state("
        "id,degraded_streak,last_reason,last_decision,probe_every_cycles,updated_at_ms"
        ") VALUES (1,1,'candidate-quote-stale','fresh',3,1)",
        "INSERT INTO neg_risk_discovery_load_state("
        "id,degraded_streak,last_reason,last_decision,probe_every_cycles,updated_at_ms"
        ") VALUES (1,2,'candidate-quote-stale','probe',3,1)",
    ],
)
async def test_status_rejects_direct_semantic_corruption_without_leak(
    tmp_path: Path,
    capsys,
    corruption: str,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    db_path = tmp_path / "state.db"
    with sqlite3.connect(db_path) as con:
        con.execute(corruption)

    assert discovery_status_main(["--db-path", str(db_path)]) == 2
    captured = capsys.readouterr()
    assert str(db_path) not in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.asyncio
async def test_durable_candidate_freshness_covers_all_promoted_certified_groups(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-fresh"),
                _event(event_id="e-2", group_id="g-stale"),
            )
        ),
        store=store,
        promotion_admission_capacity=2,
        candidate_group_timeout_s=10,
    ).run_batch()
    _publish_quote(store, "g-fresh", quoted_at_ms=99_000)
    _publish_quote(store, "g-stale", quoted_at_ms=10_000)
    stale = store.current_group("g-stale")
    store.record_candidate_watch_fact(
        group_id="g-stale",
        membership_hash=stale.membership_hash,
        quote_batch_id=None,
        observed_at_ms=99_500,
        last_result="unavailable",
        reason="fixture",
        bundle_cost=None,
        gross_edge_bps=None,
        max_bundle_size=None,
        priority_class="high",
        consecutive_failures=1,
        effective_interval_s=1,
        schedule_reason="fixture",
        next_due_at_ms=100_500,
    )

    snapshot = store.candidate_freshness_snapshot(now_ms=100_000)
    restarted = OpportunityPerceptionStore(tmp_path / "state.db")

    assert snapshot.candidate_count == 2
    assert snapshot.missing_quote_count == 0
    assert snapshot.quote_p95_age_ms == 90_000
    assert restarted.candidate_freshness_snapshot(now_ms=100_000) == snapshot


def test_empty_durable_candidate_set_allows_discovery_bootstrap(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    snapshot = store.candidate_freshness_snapshot(now_ms=100_000)

    assert snapshot.candidate_count == 0
    assert snapshot.missing_quote_count == 0
    assert snapshot.quote_p95_age_ms is None


@pytest.mark.asyncio
async def test_missing_durable_quote_yields_discovery_but_empty_set_does_not(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    snapshot = store.candidate_freshness_snapshot(now_ms=100_000)
    controller = DiscoveryLoadController(candidate_hard_stale_ms=90_000)

    assert snapshot.missing_quote_count == 1
    assert controller.yield_reason(
        CandidateFreshness(
            candidate_count=snapshot.candidate_count,
            quote_p95_age_ms=snapshot.quote_p95_age_ms,
            missing_quote_count=snapshot.missing_quote_count,
        )
    ) == "candidate-quote-missing"
    assert controller.yield_reason(
        CandidateFreshness(candidate_count=0, quote_p95_age_ms=None)
    ) is None


@pytest.mark.asyncio
async def test_queued_unpromoted_groups_do_not_degrade_admitted_freshness(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-admitted", liquidity="300"),
                _event(event_id="e-2", group_id="g-queued", liquidity="100"),
            )
        ),
        store=store,
        promotion_admission_capacity=1,
    ).run_batch()
    _publish_quote(store, "g-admitted", quoted_at_ms=99_000)

    snapshot = store.candidate_freshness_snapshot(now_ms=100_000)

    assert store.promoted_group_ids() == ("g-admitted",)
    assert snapshot.candidate_count == 1
    assert snapshot.missing_quote_count == 0


@pytest.mark.asyncio
async def test_watched_certified_group_remains_actual_candidate_when_unpromoted(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-admitted", liquidity="300"),
                _event(event_id="e-2", group_id="g-watched", liquidity="100"),
            )
        ),
        store=store,
        promotion_admission_capacity=1,
    ).run_batch()
    watched = store.current_group("g-watched")
    _publish_quote(store, "g-watched", quoted_at_ms=10_100)
    store.record_candidate_watch_fact(
        group_id="g-watched",
        membership_hash=watched.membership_hash,
        quote_batch_id="qb-g-watched-10100",
        observed_at_ms=10_100,
        last_result="no-edge",
        reason=None,
        bundle_cost=1.0,
        gross_edge_bps=0.0,
        max_bundle_size=1.0,
        priority_class="normal",
        consecutive_failures=0,
        effective_interval_s=60,
        schedule_reason="normal-cadence",
        next_due_at_ms=70_100,
    )

    source = compose_candidate_group_ids(lambda: (), store)
    before = store.candidate_freshness_snapshot(now_ms=20_000)
    store.record_candidate_watch_fact(
        group_id="g-watched",
        membership_hash=watched.membership_hash,
        quote_batch_id=None,
        observed_at_ms=20_001,
        last_result="unavailable",
        reason="fixture-unavailable",
        bundle_cost=None,
        gross_edge_bps=None,
        max_bundle_size=None,
        priority_class="normal",
        consecutive_failures=1,
        effective_interval_s=60,
        schedule_reason="bounded-failure-backoff",
        next_due_at_ms=80_001,
    )
    after = store.candidate_freshness_snapshot(now_ms=20_000)

    assert source() == ("g-admitted", "g-watched")
    assert before.candidate_count == 2
    assert before.missing_quote_count == 1
    assert after.quote_p95_age_ms == before.quote_p95_age_ms


@pytest.mark.asyncio
async def test_degraded_duty_cycle_persists_probe_phase_across_restart(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-1")))

    def freshness() -> CandidateFreshness:
        return CandidateFreshness(
            candidate_count=1,
            quote_p95_age_ms=None,
            missing_quote_count=1,
        )

    first = DiscoveryWorker(
        gamma=gamma,
        store=store,
        load_controller=DiscoveryLoadController(candidate_hard_stale_ms=90_000),
        candidate_freshness=freshness,
        degraded_probe_every_cycles=3,
    )
    assert (await first.run_batch()).yielded is True
    assert (await first.run_batch()).yielded is True
    restarted = DiscoveryWorker(
        gamma=gamma,
        store=OpportunityPerceptionStore(tmp_path / "state.db"),
        load_controller=DiscoveryLoadController(candidate_hard_stale_ms=90_000),
        candidate_freshness=freshness,
        degraded_probe_every_cycles=3,
    )

    result = await restarted.run_batch()

    assert result.yielded is False
    assert gamma.calls == [("c-1", 100)]
    assert store.discovery_load_state().degraded_streak == 3
    assert store.discovery_load_state().last_decision == "probe"
    assert store.discovery_load_state().probe_every_cycles == 3
    recovered = store.record_discovery_load_decision(
        degraded_reason=None,
        probe_every_cycles=3,
        now_ms=20_000,
    )
    assert recovered.degraded_streak == 0
    assert recovered.last_decision == "fresh"
    assert recovered.probe_every_cycles == 3
    assert OpportunityPerceptionStore(
        tmp_path / "state.db"
    ).discovery_load_state() == recovered


def test_candidate_source_filters_legacy_seed_through_current_authority(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-new")))
    asyncio.run(
        DiscoveryWorker(
            gamma=gamma,
            store=store,
            clock_ms=lambda: 10_000,
        ).run_batch()
    )
    # A pre-Discovery bootstrap authority has no schedule row; it remains
    # independently actual while an authority-free legacy string is rejected.
    with sqlite3.connect(tmp_path / "state.db") as con:
        con.execute(
            "DELETE FROM neg_risk_group_schedule WHERE group_id='g-new'"
        )

    source = compose_candidate_group_ids(lambda: ("g-legacy", "g-new"), store)

    assert source() == ("g-new",)
    assert store.candidate_freshness_snapshot(now_ms=10_001).candidate_count == 1


def test_status_rejects_forged_historical_sample_with_correct_count(
    tmp_path: Path,
    capsys,
) -> None:
    store = _store(tmp_path)
    asyncio.run(
        DiscoveryWorker(
            gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
            store=store,
        ).run_batch()
    )
    with sqlite3.connect(tmp_path / "state.db") as con:
        con.execute(
            "UPDATE neg_risk_discovery_batch_samples "
            "SET group_id='ghost' WHERE batch_id=("
            "SELECT MIN(id) FROM neg_risk_discovery_batches)"
        )

    assert discovery_status_main(
        ["--db-path", str(tmp_path / "state.db"), "--now-ms", "10001"]
    ) == 2
    assert "invalid discovery state" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_new_promotions_enter_candidate_scheduler_in_discovery_score_order(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="a-low", liquidity="100"),
                _event(event_id="e-2", group_id="z-high", liquidity="300"),
            )
        ),
        store=store,
        clock_ms=lambda: 10_000,
        promotion_admission_capacity=2,
        candidate_group_timeout_s=10,
    ).run_batch()
    calls: list[str] = []

    class Watcher:
        async def run_once(
            self, group_id: str, *, priority_hint: str, admission_context=None
        ) -> None:
            assert admission_context is not None
            breach = store.record_candidate_attempt_start(
                admission=admission_context,
                clock_ms=lambda: 20_000,
            )
            if breach is None:
                calls.append(group_id)

    scheduler = CandidateWatcherScheduler(
        watcher=Watcher(),
        store=store,
        candidate_group_ids=lambda: store.promoted_group_ids(),
        runtime=CandidateWatcherRuntime(),
        clock_ms=lambda: 10_000,
        cycle_max_groups=3,
        reserved_non_high_slots=2,
        group_timeout_s=10,
    )

    await scheduler.run_due_once()

    assert calls == ["z-high", "a-low"]


@pytest.mark.asyncio
async def test_promotion_admission_is_capacity_bounded_and_backfills_after_fact(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    result = await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-low", liquidity="100"),
                _event(event_id="e-2", group_id="g-mid", liquidity="200"),
                _event(event_id="e-3", group_id="g-high", liquidity="300"),
            )
        ),
        store=store,
        promotion_admission_capacity=1,
        candidate_max_wait_s=60,
    ).run_batch()

    assert result.promoted_group_ids == ("g-high",)
    assert store.promoted_group_ids() == ("g-high",)
    assert store.group_schedule("g-mid").promoted_at_ms is None
    assert store.group_schedule("g-low").promoted_at_ms is None
    admitted = store.group_schedule("g-high")
    assert admitted.promoted_at_ms == 10_000
    assert admitted.candidate_start_deadline_at_ms == 70_000
    calls: list[tuple[str, int]] = []
    now = 10_001

    class Watcher:
        async def run_once(
            self, group_id: str, *, priority_hint: str, admission_context=None
        ) -> None:
            calls.append((group_id, now))

    scheduler = CandidateWatcherScheduler(
        watcher=Watcher(),
        store=store,
        candidate_group_ids=lambda: (
            "hot-existing",
            *store.actual_candidate_group_ids(),
        ),
        runtime=CandidateWatcherRuntime(),
        clock_ms=lambda: now,
        cycle_max_groups=3,
        reserved_non_high_slots=1,
        discovery_candidate_max_wait_s=60,
    )
    await scheduler.run_due_once()

    assert calls == [("hot-existing", now), ("g-high", now)]
    assert calls[1][1] <= admitted.candidate_start_deadline_at_ms

    store.record_candidate_watch_fact(
        group_id="g-high",
        membership_hash=store.current_group("g-high").membership_hash,
        quote_batch_id=None,
        observed_at_ms=20_000,
        last_result="no-edge",
        reason=None,
        bundle_cost=1.0,
        gross_edge_bps=0.0,
        max_bundle_size=1.0,
        priority_class="normal",
        consecutive_failures=0,
        effective_interval_s=60,
        schedule_reason="normal-cadence",
        next_due_at_ms=80_000,
    )

    assert store.promoted_group_ids() == ("g-high", "g-mid")
    next_admitted = OpportunityPerceptionStore(
        tmp_path / "state.db"
    ).group_schedule("g-mid")
    assert next_admitted.promoted_at_ms == 20_000
    assert next_admitted.candidate_start_deadline_at_ms == 80_000
    assert store.group_schedule("g-low").promoted_at_ms is None
    with sqlite3.connect(tmp_path / "state.db") as con:
        audits = con.execute(
            "SELECT group_id,promoted_at_ms,candidate_start_deadline_at_ms "
            "FROM neg_risk_candidate_admissions ORDER BY promoted_at_ms,group_id"
        ).fetchall()
    assert audits == [
        ("g-high", 10_000, 70_000),
        ("g-mid", 20_000, 80_000),
    ]
    assert store.discovery_status(now_ms=20_001).candidate_start_ready is True


@pytest.mark.asyncio
async def test_admitted_attempt_start_is_durable_and_within_deadline(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    calls: list[str] = []
    revision = store.current_group("g-1")

    class Structure:
        async def read_group(self, group_id: str):
            calls.append(f"structure:{group_id}")
            return revision

    class Books:
        async def get_books(self, token_ids, *, projection):
            calls.append(f"books:{','.join(token_ids)}")
            return [
                {
                    "asset_id": token_id,
                    "asks": [{"price": "0.40", "size": "10"}],
                }
                for token_id in token_ids
            ]

    times = iter((20_000, 20_001, 20_002))
    runtime = CandidateWatcherRuntime()
    watcher = CandidateWatcher(
        structure_reader=Structure(),
        books_reader=Books(),
        store=store,
        runtime=runtime,
        interval_controller=IntervalController(),
        clock_ms=lambda: next(times),
    )

    scheduler = CandidateWatcherScheduler(
        watcher=watcher,
        store=store,
        candidate_group_ids=lambda: store.actual_candidate_group_ids(),
        runtime=runtime,
        clock_ms=lambda: 20_000,
    )

    await scheduler.run_due_once()

    status = store.discovery_status(now_ms=20_001)
    assert calls[0] == "structure:g-1"
    assert any(call.startswith("books:") for call in calls)
    assert status.candidate_attempt_start_count == 1
    assert status.candidate_start_deadline_breach_count == 0
    assert status.candidate_start_ready is True


@pytest.mark.asyncio
async def test_restart_after_candidate_start_deadline_records_breach_not_call(
    tmp_path: Path,
    capsys,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    calls: list[str] = []
    revision = store.current_group("g-1")

    class Structure:
        async def read_group(self, group_id: str):
            calls.append(f"structure:{group_id}")
            return revision

    class Books:
        async def get_books(self, token_ids, *, projection):
            calls.append("books")
            return []

    runtime = CandidateWatcherRuntime()
    watcher = CandidateWatcher(
        structure_reader=Structure(),
        books_reader=Books(),
        store=OpportunityPerceptionStore(tmp_path / "state.db"),
        runtime=runtime,
        interval_controller=IntervalController(),
        clock_ms=lambda: 70_001,
    )

    restarted = CandidateWatcherScheduler(
        watcher=watcher,
        store=OpportunityPerceptionStore(tmp_path / "state.db"),
        candidate_group_ids=lambda: ("g-1",),
        runtime=runtime,
        clock_ms=lambda: 70_001,
    )

    await restarted.run_due_once()

    fact = store.latest_candidate_watch_fact("g-1")
    status = store.discovery_status(now_ms=70_002)
    assert calls == []
    assert fact.last_result == "unavailable"
    assert fact.reason == "candidate-start-deadline-breached"
    assert status.candidate_start_deadline_breach_count == 1
    assert status.candidate_start_ready is False
    with sqlite3.connect(tmp_path / "state.db") as con:
        con.execute(
            "UPDATE neg_risk_candidate_attempt_starts "
            "SET deadline_breached=0"
        )
    assert discovery_status_main(
        ["--db-path", str(tmp_path / "state.db")]
    ) == 2
    assert "Traceback" not in capsys.readouterr().err


@pytest.mark.asyncio
async def test_discovery_never_promotes_without_capacity_proof(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)

    result = await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
        promotion_admission_capacity=0,
        candidate_max_wait_s=60,
    ).run_batch()

    assert result.promoted_group_ids == ()
    assert store.current_group("g-1").status == "certified"
    assert store.group_schedule("g-1").promoted_at_ms is None
    assert store.promoted_group_ids() == ()


@pytest.mark.asyncio
async def test_generic_init_preserves_legacy_promotions_until_explicit_config(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-low", liquidity="100"),
                _event(event_id="e-2", group_id="g-mid", liquidity="200"),
                _event(event_id="e-3", group_id="g-high", liquidity="300"),
            )
        ),
        store=store,
        promotion_admission_capacity=3,
        candidate_group_timeout_s=7,
    ).run_batch()
    with sqlite3.connect(tmp_path / "state.db") as con:
        audit_count_before_init = con.execute(
            "SELECT COUNT(*) FROM neg_risk_candidate_admissions"
        ).fetchone()[0]
        con.execute("DELETE FROM neg_risk_discovery_admission_state")

    OpportunityPerceptionStore(tmp_path / "state.db").init_schema()

    assert store.promoted_group_ids() == ("g-high", "g-mid", "g-low")
    assert store.discovery_admission_proof() is None
    with sqlite3.connect(tmp_path / "state.db") as con:
        assert (
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_candidate_admissions"
            ).fetchone()[0]
            == audit_count_before_init
        )

    proof = DiscoveryAdmissionProof(
        effective_capacity=2,
        candidate_max_wait_ms=60_000,
        selection_budget_ms=6_000,
        poll_interval_ms=1_000,
        group_timeout_ms=10_000,
        terminal_write_budget_ms=5_000,
        high_burst_groups=1,
        reserved_non_high_slots=3,
    )
    store.configure_discovery_admission(proof, now_ms=20_000)
    store.configure_discovery_admission(proof, now_ms=20_000)

    assert store.promoted_group_ids() == ("g-high", "g-mid")
    assert store.group_schedule("g-low").promoted_at_ms is None
    assert store.discovery_admission_proof() == proof


@pytest.mark.asyncio
async def test_status_rejects_admission_beyond_proven_capacity(
    tmp_path: Path,
    capsys,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-high", liquidity="300"),
                _event(event_id="e-2", group_id="g-low", liquidity="100"),
            )
        ),
        store=store,
        promotion_admission_capacity=1,
    ).run_batch()
    with sqlite3.connect(tmp_path / "state.db") as con:
        con.execute(
            "UPDATE neg_risk_group_schedule SET promoted_at_ms=10000,"
            "candidate_start_deadline_at_ms=70000 WHERE group_id='g-low'"
        )

    assert discovery_status_main(
        ["--db-path", str(tmp_path / "state.db")]
    ) == 2
    captured = capsys.readouterr()
    assert str(tmp_path / "state.db") not in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.asyncio
async def test_unadmitted_factless_group_never_bypasses_capacity(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="a-old", liquidity="1"),
                _event(event_id="e-2", group_id="z-new", liquidity="999"),
            )
        ),
        store=store,
    ).run_batch()
    with sqlite3.connect(tmp_path / "state.db") as con:
        con.execute(
            "UPDATE neg_risk_group_schedule SET first_discovered_at_ms=0,"
            "priority_score='0' WHERE group_id='a-old'"
        )
        con.execute(
            "UPDATE neg_risk_group_schedule SET first_discovered_at_ms=999000,"
            "priority_score='999' WHERE group_id='z-new'"
        )
    calls: list[str] = []

    class Watcher:
        async def run_once(
            self, group_id: str, *, priority_hint: str, admission_context=None
        ) -> None:
            calls.append(group_id)

    def scheduler() -> CandidateWatcherScheduler:
        return CandidateWatcherScheduler(
            watcher=Watcher(),
            store=OpportunityPerceptionStore(tmp_path / "state.db"),
            candidate_group_ids=lambda: ("z-new", "a-old"),
            runtime=CandidateWatcherRuntime(),
            clock_ms=lambda: 20_000,
            cycle_max_groups=2,
            reserved_non_high_slots=1,
            discovery_candidate_max_wait_s=60,
        )

    await scheduler().run_due_once()
    await scheduler().run_due_once()

    assert calls == ["z-new", "z-new"]


def test_overdue_promotions_use_only_reserved_capacity_after_genuine_high(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    scheduler = CandidateWatcherScheduler(
        watcher=object(),
        store=store,
        candidate_group_ids=lambda: (),
        runtime=CandidateWatcherRuntime(),
        cycle_max_groups=5,
        reserved_non_high_slots=2,
    )
    due = [
        (0, 100, "hot-1"),
        (0, 101, "hot-2"),
        (0, 102, "hot-3"),
    ] + [
        (1, -(10**18) + index, f"overdue-{index}")
        for index in range(5)
    ]

    selected = scheduler._select_cycle(due)

    assert selected[0][2] == "hot-1"
    assert sum(item[2].startswith("overdue") for item in selected) == 2
    assert {item[2] for item in selected if item[2].startswith("hot")} == {
        "hot-1",
        "hot-2",
        "hot-3",
    }


@pytest.mark.asyncio
async def test_discovery_yields_before_gamma_when_hot_path_is_stale(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-1")))
    controller = DiscoveryLoadController(candidate_hard_stale_ms=90_000)
    worker = DiscoveryWorker(
        gamma=gamma,
        store=store,
        load_controller=controller,
        candidate_freshness=lambda: CandidateFreshness(
            candidate_count=2,
            quote_p95_age_ms=91_000,
        ),
        clock_ms=lambda: 10_000,
    )

    result = await worker.run_batch()

    assert result.yielded is True
    assert result.yield_reason == "candidate-quote-stale"
    assert gamma.calls == []
    assert store.discovery_cursor() == "c-1"


@pytest.mark.asyncio
async def test_cancellation_during_commit_finishes_one_atomic_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-1")))
    entered = threading.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    entered_async = asyncio.Event()
    original = store.publish_discovery_batch

    def delayed(*args, **kwargs):
        entered.set()
        loop.call_soon_threadsafe(entered_async.set)
        release.wait(timeout=5)
        return original(*args, **kwargs)

    monkeypatch.setattr(store, "publish_discovery_batch", delayed)
    worker = DiscoveryWorker(gamma=gamma, store=store, clock_ms=lambda: 10_000)
    task = asyncio.create_task(worker.run_batch())
    await entered_async.wait()
    task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.discovery_cursor() == "c-2"
    assert store.group_schedule("g-1") is not None
