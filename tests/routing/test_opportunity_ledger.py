"""Durable lifecycle facts for observer-only neg-risk opportunities."""

from __future__ import annotations

from dataclasses import replace

from polyarb.routing.opportunity_ledger import OpportunityLedger
from polyarb.routing.opportunity_scanner import GroupAssessment, OpportunityLeg
from polyarb.storage.sqlite_store import SQLiteStore

NOW_MS = 1_800_000_000_000


def _observe_assessment() -> GroupAssessment:
    return GroupAssessment(
        group_id="group-1",
        event_id="event-1",
        membership_hash="membership-1",
        status="observe",
        reason=None,
        bundle_cost=0.97,
        gross_edge_bps=300.0,
        max_bundle_size=42.0,
        legs=(
            OpportunityLeg("market-a", "condition-a", "alpha", "token-a", 0.45, 50),
            OpportunityLeg("market-b", "condition-b", "beta", "token-b", 0.52, 42),
        ),
        structure_revision=17,
        quote_run_id=42,
        quoted_at_ms=NOW_MS,
    )


def test_first_crossing_persists_master_observation_and_notification(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)

    transition = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)

    assert transition.kind == "entered"
    assert transition.opportunity_id
    assert ledger.current_opportunities() == [
        {
            "id": transition.opportunity_id,
            "status": "observe",
            "event_id": "event-1",
            "group_id": "group-1",
            "membership_hash": "membership-1",
            "bundle_cost": 0.97,
            "gross_edge_bps": 300.0,
            "max_bundle_size": 42.0,
            "structure_revision": 17,
            "quote_run_id": 42,
        }
    ]
    pending = ledger.pending_notifications(now_ms=NOW_MS)
    assert len(pending) == 1
    assert pending[0].reason == "entered-gross-edge-threshold"
    assert pending[0].opportunity_id == transition.opportunity_id


def test_complete_below_threshold_observation_closes_existing_opportunity(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)
    no_edge = replace(
        _observe_assessment(),
        status="no-edge",
        bundle_cost=1.01,
        gross_edge_bps=-100.0,
        max_bundle_size=38.0,
        quote_run_id=43,
    )

    transition = ledger.reconcile_global(no_edge, observed_at_ms=NOW_MS + 120_000)

    assert transition == replace(opened, kind="closed")
    assert ledger.current_opportunities() == []
    pending = ledger.pending_notifications(now_ms=NOW_MS + 120_000)
    assert [item.reason for item in pending] == [
        "entered-gross-edge-threshold",
        "closed-gross-edge-threshold",
    ]


def test_material_edge_change_enqueues_one_follow_up_notification(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)
    changed = replace(
        _observe_assessment(),
        gross_edge_bps=325.0,
        bundle_cost=0.9675,
        quote_run_id=43,
    )

    transition = ledger.reconcile_global(changed, observed_at_ms=NOW_MS + 120_000)

    assert transition == replace(opened, kind="edge-changed")
    assert [item.reason for item in ledger.pending_notifications(now_ms=NOW_MS)] == [
        "entered-gross-edge-threshold",
        "edge-changed",
    ]
