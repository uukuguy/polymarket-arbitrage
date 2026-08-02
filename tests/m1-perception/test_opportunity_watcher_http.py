from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from types import SimpleNamespace

from polyarb.daemon.scheduler import SchedulerState


def _seed_market_map(client) -> int:
    now_ms = int(time.time() * 1000)
    db_path = client.app.state.sqlite_store.db_path
    con = sqlite3.connect(db_path)
    try:
        cursor = con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,archive_status,snapshot_status,is_valid,parquet_path"
            ") VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                now_ms,
                now_ms,
                "full",
                3,
                1,
                "structure",
                "not-requested",
                "ok",
                1,
                "",
            ),
        )
        snapshot_id = int(cursor.lastrowid)
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items,failure_source,failure_reason"
            ") VALUES (?,1,3,1,NULL,NULL)",
            (snapshot_id,),
        )
        con.executemany(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            (
                (
                    snapshot_id,
                    "event-scannable",
                    "group-scannable",
                    "standard",
                    2,
                    2,
                    "membership-scannable",
                    "complete-supported",
                    None,
                ),
                (
                    snapshot_id,
                    "event-rejected",
                    "group-rejected",
                    "augmented",
                    1,
                    1,
                    "membership-rejected",
                    "complete-unsupported",
                    "augmented-neg-risk-not-supported",
                ),
            ),
        )
        con.commit()
        return snapshot_id
    finally:
        con.close()


def test_market_map_exposes_scannable_and_rejected_groups(http_test_client):
    revision = _seed_market_map(http_test_client)

    response = http_test_client.get("/market-map")

    assert response.status_code == 200
    body = response.json()
    assert body["structure_revision"] == revision
    assert body["scannable_groups"][0]["quality"] == "complete-supported"
    assert body["rejected_groups"] == [
        {
            "event_id": "event-rejected",
            "group_id": "group-rejected",
            "quality": "complete-unsupported",
            "reason": "augmented-neg-risk-not-supported",
        }
    ]


def test_market_map_stays_200_and_excludes_quarantine_only_market(http_test_client):
    revision = _seed_market_map(http_test_client)
    with sqlite3.connect(http_test_client.app.state.sqlite_store.db_path) as con:
        con.execute(
            "INSERT INTO validation_issues(snapshot_id,layer,category,market_id,detail,"
            "raw_payload) VALUES (?,1,'api_jitter','quarantined-market',"
            "'quarantined',?)",
            (
                revision,
                "active-open-neg-risk-market-parent-absent-from-active-event-catalogue:"
                + "0" * 64,
            ),
        )

    response = http_test_client.get("/market-map")

    assert response.status_code == 200
    assert "quarantined-market" not in response.text


def test_market_map_event_filter_returns_only_requested_event(http_test_client):
    _seed_market_map(http_test_client)

    response = http_test_client.get("/market-map?event_id=event-scannable")

    assert response.status_code == 200
    assert response.json()["scannable_groups"] == [
        {
            "event_id": "event-scannable",
            "group_id": "group-scannable",
            "quality": "complete-supported",
            "expected_member_count": 2,
            "active_named_count": 2,
            "membership_hash": "membership-scannable",
        }
    ]
    assert response.json()["rejected_groups"] == []


def test_market_map_without_a_fresh_published_structure_is_bounded_503(http_test_client):
    response = http_test_client.get("/market-map")

    assert response.status_code == 503
    assert response.json() == {"error": "market map unavailable"}


def test_new_structure_revision_hides_old_observer_opportunity(http_test_client):
    old_revision = _seed_market_map(http_test_client)
    now_ms = int(time.time() * 1000)
    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_opportunities("
            "id,event_id,group_id,membership_hash,status,bundle_cost,gross_edge_bps,"
            "max_bundle_size,structure_revision,quote_run_id,opened_at_ms,updated_at_ms"
            ") VALUES ('old-observer','event-scannable','group-scannable',"
            "'membership-scannable','observe',0.95,500,1,?,1,?,?)",
            (old_revision, now_ms, now_ms),
        )
        cursor = con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,archive_status,snapshot_status,is_valid,parquet_path"
            ") VALUES (?,?,?,?,?,?,?,?,?,?)",
            (now_ms, now_ms, "full", 2, 1, "structure", "not-requested", "ok", 1, ""),
        )
        new_revision = int(cursor.lastrowid)
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items,failure_source,failure_reason"
            ") VALUES (?,1,2,1,NULL,NULL)",
            (new_revision,),
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            (
                new_revision,
                "event-scannable",
                "group-scannable",
                "standard",
                2,
                2,
                "membership-scannable",
                "complete-supported",
                None,
            ),
        )

    response = http_test_client.get("/market-map")

    assert response.status_code == 200
    assert response.json()["structure_revision"] == new_revision
    assert response.json()["current_opportunities"] == []


async def test_status_reports_the_watcher_used_for_global_reconciliation(
    http_test_client,
    daemon_settings_for_test,
):
    from polyarb.daemon.quote_worker import build_production_quote_worker

    _seed_market_map(http_test_client)

    class GlobalWatcher:
        reconciliation_count = 0

        async def reconcile_global_projection(self, _projection) -> None:
            self.reconciliation_count += 1

        def snapshot(self):
            return SimpleNamespace(
                reconciliation_count=self.reconciliation_count,
                last_reconciled_at_ms=None,
                notification_delivery_count=0,
                notification_failure_count=0,
                last_notification_error_kind=None,
            )

    watcher = GlobalWatcher()
    settings = daemon_settings_for_test.model_copy(
        update={"neg_risk_quote_worker_enabled": True}
    )
    worker = build_production_quote_worker(settings, opportunity_watcher=watcher)
    assert worker is not None
    assert worker._reconcile_global_projection is not None
    await worker._reconcile_global_projection(object())
    http_test_client.app.state.opportunity_watcher = watcher

    response = http_test_client.get("/opportunity-watch/status")

    assert response.status_code == 200
    assert response.json()["watcher"]["reconciliation_count"] == 1


@dataclass
class _QueuedProducer:
    queued: bool = True
    state: SchedulerState = SchedulerState.RUNNING

    def request_now(self) -> bool:
        result = self.queued
        self.queued = False
        return result


def test_market_map_build_requires_hmac(http_test_client):
    response = http_test_client.post("/control/market-map/build", content=b"{}")

    assert response.status_code == 401
    assert response.json() == {"error": "missing X-Signature header"}


def test_hmac_controls_queue_once_and_never_run_the_producer_directly(
    http_test_client, make_signed_request
):
    scheduler = _QueuedProducer()
    quote_worker = _QueuedProducer()
    http_test_client.app.state.scheduler = scheduler
    http_test_client.app.state.quote_worker = quote_worker

    map_first = make_signed_request(http_test_client, "/control/market-map/build", {})
    map_second = make_signed_request(http_test_client, "/control/market-map/build", {})
    quote_first = make_signed_request(http_test_client, "/control/neg-risk/scan", {})
    quote_second = make_signed_request(http_test_client, "/control/neg-risk/scan", {})

    assert (map_first.status_code, map_first.json()) == (202, {"status": "queued"})
    assert (map_second.status_code, map_second.json()) == (200, {"status": "already_queued"})
    assert (quote_first.status_code, quote_first.json()) == (202, {"status": "queued"})
    assert (quote_second.status_code, quote_second.json()) == (200, {"status": "already_queued"})


def test_hmac_controls_refuse_paused_or_disabled_producers(http_test_client, make_signed_request):
    http_test_client.app.state.scheduler = _QueuedProducer(state=SchedulerState.PAUSED)
    http_test_client.app.state.quote_worker = None

    map_response = make_signed_request(http_test_client, "/control/market-map/build", {})
    quote_response = make_signed_request(http_test_client, "/control/neg-risk/scan", {})

    assert (map_response.status_code, map_response.json()) == (409, {"error": "unavailable"})
    assert (quote_response.status_code, quote_response.json()) == (409, {"error": "unavailable"})


def test_cloud_control_cli_fails_before_a_request_without_local_secret(monkeypatch):
    from polyarb import cli_perception

    monkeypatch.delenv("POLYARB_SCAN_SHARED_SECRET", raising=False)
    monkeypatch.setattr(
        cli_perception,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not request")),
    )

    assert cli_perception.main(["build-market-map"]) == 2


def test_public_read_routes_do_not_start_scheduler_or_clob_work(http_test_client):
    _seed_market_map(http_test_client)

    class _Forbidden:
        def __getattr__(self, name):
            raise AssertionError(f"public read invoked producer attribute {name}")

    http_test_client.app.state.scheduler = _Forbidden()
    http_test_client.app.state.quote_worker = _Forbidden()
    response = http_test_client.get("/market-map")
    status = http_test_client.get("/opportunity-watch/status")

    assert response.status_code == 200
    assert status.status_code == 200
