"""Tests for /health IETF三态 endpoint.

Covers D-12 / D-16 — IETF draft-inadarei-api-health-check-06 compliance.
Three-state health: pass (< 14h), warn (14-25h stale), fail (> 25h stale OR no snapshot).
HTTP 200 for pass/warn, 503 for fail.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from starlette.testclient import TestClient

from polyarb.http import health as health_module


def _read_market_truth_health(path: Path, *, now_s: float):
    reader = getattr(health_module, "read_market_truth_health", None)
    assert callable(reader), "read_market_truth_health is not implemented"
    return reader(path, now_s)


@pytest.mark.parametrize("handler_name", ["health", "healthz"])
async def test_health_database_projection_runs_off_event_loop(
    daemon_settings_for_test: Any,
    monkeypatch: pytest.MonkeyPatch,
    handler_name: str,
) -> None:
    event_loop_thread = threading.get_ident()
    projection_threads: list[int] = []

    def build_checks(*_args, **_kwargs):
        projection_threads.append(threading.get_ident())
        return {}, "pass"

    monkeypatch.setattr(health_module, "_build_health_checks", build_checks)
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                sqlite_store=SimpleNamespace(db_path=daemon_settings_for_test.db_path),
                settings=daemon_settings_for_test,
                quote_worker_runtime=None,
                machine_id="machine-test",
                boot_id="boot-test",
            )
        )
    )

    response = await getattr(health_module, handler_name)(request)

    assert response.status_code == 200
    assert projection_threads
    assert projection_threads[0] != event_loop_thread


# ---------------------------------------------------------------------------
# Helper: insert a snapshot row into a tmp SQLite DB
# ---------------------------------------------------------------------------


def _insert_snapshot(
    db_path: Path,
    *,
    taken_at_ms: int,
    status: str = "ok",
    market_count: int = 100,
    coverage_completed: bool = True,
    market_view_published: bool = True,
    data_product: str = "structure",
    archive_status: str = "not_requested",
    snapshot_status: str = "ok",
    event_count: int = 20,
    failure_source: str | None = None,
    failure_reason: str | None = None,
) -> int:
    """Insert a minimal snapshots row so get_latest_snapshot has data."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(db_path)
    store.init_schema()

    # Insert a snapshot row directly
    con = sqlite3.connect(db_path)
    try:
        now_ms = int(time.time() * 1000)
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,archive_status,snapshot_status,is_valid,parquet_path,notes"
            ")"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                taken_at_ms,
                now_ms,
                "subset",
                market_count,
                int(market_view_published),
                data_product,
                archive_status,
                snapshot_status,
                1,
                "/tmp/dummy.parquet",
                status,
            ),
        )
        snapshot_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items,failure_source,failure_reason"
            ") VALUES (?,?,?,?,?,?)",
            (
                snapshot_id,
                int(coverage_completed),
                market_count,
                event_count,
                failure_source,
                failure_reason,
            ),
        )
        con.commit()
    finally:
        con.close()
    return int(snapshot_id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pass_when_fresh(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot taken 1h ago → status=pass, HTTP 200."""
    now_ms = int(time.time() * 1000)
    one_hour_ago_ms = now_ms - int(1 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=one_hour_ago_ms)

    resp = http_test_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "pass"
    ct = resp.headers.get("content-type", "")
    assert "application/health+json" in ct


def test_health_binds_stable_machine_and_boot_identity(
    http_test_client: TestClient,
) -> None:
    strict = http_test_client.get("/health").json()
    probe = http_test_client.get("/healthz").json()

    assert strict["machineId"] == "local"
    assert UUID(strict["bootId"]).version == 4
    assert probe["machineId"] == strict["machineId"]
    assert probe["bootId"] == strict["bootId"]


def test_health_exposes_release_bound_qualification_policy(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    body = http_test_client.get("/healthz").json()

    assert body["qualificationPolicy"] == {
        "candidateQuoteHardStaleS": (
            daemon_settings_for_test.candidate_quote_hard_stale_s
        ),
        "candidateLowerLaneMaxWaitS": (
            daemon_settings_for_test.candidate_lower_lane_max_wait_s
        ),
        "discoveryCandidateMaxWaitS": (
            daemon_settings_for_test.discovery_candidate_max_wait_s
        ),
        "producerStallDetectionS": (
            daemon_settings_for_test.producer_stall_detection_s
        ),
    }


def test_warn_when_stale(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot taken 15h ago → status=warn, HTTP 200."""
    now_ms = int(time.time() * 1000)
    fifteen_hours_ago_ms = now_ms - int(15 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=fifteen_hours_ago_ms)

    resp = http_test_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "warn"


def test_fail_when_very_stale(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot taken 26h ago → status=fail, HTTP 503."""
    now_ms = int(time.time() * 1000)
    twenty_six_hours_ago_ms = now_ms - int(26 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=twenty_six_hours_ago_ms)

    resp = http_test_client.get("/health")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "fail"


def test_checks_structure(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Response body has key 'checks' with expected sub-check keys per IETF schema."""
    now_ms = int(time.time() * 1000)
    one_hour_ago_ms = now_ms - int(1 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=one_hour_ago_ms)

    resp = http_test_client.get("/health")
    body = resp.json()

    assert "checks" in body
    checks = body["checks"]
    assert "snapshot:last_success_age_seconds" in checks
    assert "snapshot:last_status" in checks

    # Each value is a list of dicts per IETF schema
    age_checks = checks["snapshot:last_success_age_seconds"]
    assert isinstance(age_checks, list)
    assert len(age_checks) >= 1
    assert isinstance(age_checks[0], dict)

    status_checks = checks["snapshot:last_status"]
    assert isinstance(status_checks, list)
    assert len(status_checks) >= 1
    assert isinstance(status_checks[0], dict)


def test_no_snapshot_returns_fail(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Empty DB (no snapshots) → status=fail, HTTP 503 (first deploy edge case)."""
    # db_path exists but has no snapshots — init schema only
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    resp = http_test_client.get("/health")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "fail"


def test_market_truth_health_fails_on_latest_incomplete_attempt(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    _insert_snapshot(path, taken_at_ms=1_000)
    _insert_snapshot(
        path,
        taken_at_ms=2_000,
        coverage_completed=False,
        market_view_published=False,
        failure_source="markets",
        failure_reason="http-422",
    )

    result = _read_market_truth_health(path, now_s=3.0)

    assert result.coverage_status == "fail"
    assert result.coverage_value == "incomplete-source"
    assert result.latest_attempt_snapshot_id == 2
    assert result.latest_attempt_market_items == 100
    assert result.latest_attempt_event_items == 20
    assert result.last_complete_snapshot_id == 1
    assert result.last_complete_age_seconds == pytest.approx(2.0)


def test_market_truth_health_does_not_certify_unpublished_complete_attempt(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    _insert_snapshot(
        path,
        taken_at_ms=1_000,
        coverage_completed=True,
        market_view_published=False,
    )

    result = _read_market_truth_health(path, now_s=3.0)

    assert result.coverage_status == "fail"
    assert result.coverage_value == "incomplete-source"
    assert result.last_complete_snapshot_id is None
    assert result.last_complete_age_seconds is None


def test_market_truth_health_rejects_legacy_combined_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    _insert_snapshot(path, taken_at_ms=1_000, data_product="legacy_combined")

    result = _read_market_truth_health(path, now_s=3.0)

    assert result.coverage_status == "fail"
    assert result.last_complete_snapshot_id is None


def test_reconciliation_health_reads_checkpoint_without_gating_hot_path(
    tmp_path: Path,
) -> None:
    from polyarb.http.health import read_reconciliation_health
    from polyarb.perception.store import OpportunityPerceptionStore

    path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    window = store.begin_reconciliation(started_at_ms=1_000)
    with sqlite3.connect(path) as con:
        con.execute(
            "UPDATE neg_risk_reconciliation_windows SET next_cursor='c-2',"
            "checkpoint_at_ms=2_000,pages_completed=1 "
            "WHERE id=?",
            (window.id,),
        )
        con.execute(
            "INSERT INTO neg_risk_reconciliation_batches("
            "window_id,batch_sequence,requested_cursor,next_cursor,completed,"
            "started_at_ms,finished_at_ms,page_event_count,groups_staged,"
            "rejected_count) VALUES (?,1,NULL,'c-2',0,1000,2000,0,0,0)",
            (window.id,),
        )

    result = read_reconciliation_health(path, now_ms=5_000)

    assert result.progress == "open"
    assert result.pages_completed == 1
    assert result.next_cursor == "c-2"
    assert result.checkpoint_age_seconds == 3.0
    assert result.receipt_consistent is True


def test_reconciliation_health_rejects_forged_early_cursor_and_bad_numbers(
    tmp_path: Path,
) -> None:
    from polyarb.http.health import read_reconciliation_health
    from polyarb.perception.store import OpportunityPerceptionStore

    path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    window = store.begin_reconciliation(started_at_ms=1_000)
    with sqlite3.connect(path) as con:
        con.execute(
            "UPDATE neg_risk_reconciliation_windows SET status='complete',"
            "checkpoint_at_ms=3000,finished_at_ms=3000,pages_completed=2 "
            "WHERE id=?",
            (window.id,),
        )
        con.executemany(
            "INSERT INTO neg_risk_reconciliation_batches("
            "window_id,batch_sequence,requested_cursor,next_cursor,completed,"
            "started_at_ms,finished_at_ms,page_event_count,groups_staged,"
            "rejected_count) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                (window.id, 1, None, "c-2", 0, 1000, 2000, 0, 0, 0),
                (window.id, 2, "c-2", None, 1, 2000, 3000, 0, 0, 0),
            ],
        )
        con.execute(
            "UPDATE neg_risk_reconciliation_batches "
            "SET requested_cursor='forged' "
            "WHERE window_id=? AND batch_sequence=1",
            (window.id,),
        )

    forged = read_reconciliation_health(path, now_ms=4_000)
    assert forged.progress == "unavailable"
    assert forged.receipt_consistent is False

    with sqlite3.connect(path) as con:
        con.execute(
            "UPDATE neg_risk_reconciliation_windows SET pages_completed='bad' WHERE id=?",
            (window.id,),
        )
    corrupt = read_reconciliation_health(path, now_ms=4_000)
    assert corrupt.progress == "unavailable"
    assert corrupt.receipt_consistent is False


def test_health_exposes_latest_attempt_and_last_complete_truth_separately(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    now_ms = int(time.time() * 1000)
    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=now_ms - 2_000,
    )
    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=now_ms - 1_000,
        coverage_completed=False,
        market_view_published=False,
        failure_source="events",
        failure_reason="cursor-repeat",
    )

    response = http_test_client.get("/health")

    assert response.status_code == 503
    checks = response.json()["checks"]
    coverage = checks["market_truth:coverage"][0]
    assert coverage["status"] == "fail"
    assert coverage["observedValue"] == "incomplete-source"
    assert coverage["output"] == "markets=100 events=20"
    complete_age = checks["market_truth:last_complete_age_seconds"][0]
    assert complete_age["status"] == "pass"
    assert 2.0 <= complete_age["observedValue"] < 5.0


def test_health_surfaces_failed_scheduler_attempt_while_truth_is_fresh(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """A fresh published revision cannot conceal a later scheduler OOM."""
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1000)
    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=now_ms - 2_000,
    )
    store = SQLiteStore(daemon_settings_for_test.db_path)
    attempt_id = store.begin_snapshot_attempt(started_at_ms=now_ms - 1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="failed",
        finished_at_ms=now_ms - 500,
        snapshot_id=None,
        failure_kind="snapshot-subprocess-signal-sigkill-possible-oom",
        last_stage="gamma-markets",
        elapsed_ms=245_012,
    )

    response = http_test_client.get("/health")

    assert response.status_code == 200
    checks = response.json()["checks"]
    assert checks["market_truth:last_complete_age_seconds"][0]["status"] == "pass"
    assert checks["snapshot:latest_attempt"][0]["observedValue"] == "failed"
    assert checks["snapshot:latest_attempt"][0]["status"] == "warn"
    assert checks["snapshot:latest_attempt"][0]["output"] == (
        "snapshot-subprocess-signal-sigkill-possible-oom stage=gamma-markets elapsed_ms=245012"
    )


def test_health_treats_cooperative_structure_checkpoint_as_healthy_progress(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=now_ms - 2_000)
    store = SQLiteStore(daemon_settings_for_test.db_path)
    attempt_id = store.begin_snapshot_attempt(started_at_ms=now_ms - 1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="cancelled",
        finished_at_ms=now_ms - 500,
        snapshot_id=None,
        failure_kind="structure-checkpoint",
        last_stage="gamma-markets",
        elapsed_ms=40_000,
    )

    attempt = http_test_client.get("/health").json()["checks"][
        "snapshot:latest_attempt"
    ][0]
    assert attempt["observedValue"] == "checkpointed"
    assert attempt["status"] == "pass"
    assert attempt["output"] == "stage=gamma-markets elapsed_ms=40000"


def test_health_omits_stage_for_historical_attempt_without_diagnostics(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Nullable legacy fields must not fabricate a stage label in health."""
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=now_ms - 2_000)
    store = SQLiteStore(daemon_settings_for_test.db_path)
    attempt_id = store.begin_snapshot_attempt(started_at_ms=now_ms - 1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="failed",
        finished_at_ms=now_ms - 500,
        snapshot_id=None,
        failure_kind="snapshot-status-failed",
    )

    attempt = http_test_client.get("/health").json()["checks"]["snapshot:latest_attempt"][0]

    assert attempt["output"] == "snapshot-status-failed"
    assert "stage=" not in attempt["output"]


def test_health_uses_effective_snapshot_timeout_and_surfaces_schedule(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Producer-slot budget caps adaptive timeout and stays health-visible."""
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1000)
    snapshot_id = _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=now_ms - 2_000,
    )
    store = SQLiteStore(daemon_settings_for_test.db_path)
    source_attempt_id = store.begin_snapshot_attempt(started_at_ms=now_ms - 300_000)
    store.finish_snapshot_attempt(
        attempt_id=source_attempt_id,
        outcome="succeeded",
        finished_at_ms=now_ms - 50_000,
        snapshot_id=snapshot_id,
        failure_kind=None,
    )
    store.append_structure_schedule_adjustment(
        source_attempt_id=source_attempt_id,
        decided_at_ms=now_ms - 40_000,
        success_sample_count=10,
        success_p95_s=236,
        previous_timeout_s=240,
        previous_cadence_s=300,
        timeout_s=288,
        cadence_s=348,
        reason="timeout-backoff",
    )
    store.begin_snapshot_attempt(started_at_ms=now_ms - 250_000)

    response = http_test_client.get("/health")

    checks = response.json()["checks"]
    assert checks["snapshot:latest_attempt"][0]["observedValue"] == "running"
    assert checks["snapshot:latest_attempt"][0]["status"] == "fail"
    schedule = checks["snapshot:schedule"][0]
    assert schedule["observedValue"] == "adaptive"
    assert schedule["status"] == "pass"
    assert schedule["output"] == (
        "configured_timeout_s=240 effective_timeout_s=288 "
        "producer_slot_budget_s=120 attempt_timeout_s=120 "
        "configured_cadence_s=3600 effective_cadence_s=348 "
        "success_samples=10 success_p95_s=236 reason=timeout-backoff"
    )


def test_health_fails_a_stalled_snapshot_attempt_while_truth_is_fresh(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """An in-flight child beyond its production deadline is an outage, not pass."""
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1000)
    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=now_ms - 2_000,
    )
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.begin_snapshot_attempt(started_at_ms=now_ms - 241_000)

    response = http_test_client.get("/health")

    assert response.status_code == 503
    attempt = response.json()["checks"]["snapshot:latest_attempt"][0]
    assert attempt["observedValue"] == "running"
    assert attempt["status"] == "fail"
    assert attempt["output"] == "snapshot-subprocess-timeout-exceeded"


def test_health_surfaces_resumable_structure_window_progress(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=now_ms - 2_000)
    store = SQLiteStore(daemon_settings_for_test.db_path)
    window = store.begin_or_resume_structure_sync(started_at_ms=now_ms - 1_000)
    store.commit_structure_event_page(
        window_id=window["id"], requested_cursor=None, next_cursor="opaque-2",
        completed=False, events=[], finished_at_ms=now_ms,
    )

    check = http_test_client.get("/health").json()["checks"]["snapshot:structure_sync"][0]

    assert check["observedValue"] == "open"
    assert check["status"] == "warn"
    assert check["output"] == "stage=events event_pages=1 market_pages=0"


def test_archive_failure_is_visible_but_does_not_fail_structure_health(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Archive is P1 evidence, not a hidden dependency of Structure/Quote."""
    now_ms = int(time.time() * 1000)
    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=now_ms - 2_000,
    )
    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=now_ms - 1_000,
        data_product="archive",
        archive_status="failed",
        market_view_published=False,
    )

    response = http_test_client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pass"
    assert body["checks"]["archive:last_attempt"][0]["observedValue"] == "failed"
    assert body["checks"]["archive:last_attempt"][0]["status"] == "warn"
    assert body["checks"]["archive:last_success_age_seconds"][0]["status"] == "warn"


def test_health_reports_persisted_degraded_structure_status(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """A valid-but-degraded Structure must not be silently relabeled as OK."""
    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=int(time.time() * 1000) - 1_000,
        snapshot_status="degraded",
    )

    response = http_test_client.get("/health")

    assert response.status_code == 200
    check = response.json()["checks"]["snapshot:last_status"][0]
    assert check["observedValue"] == "DEGRADED"
    assert check["status"] == "warn"


def test_resource_evidence_health_has_no_capability_gate(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    from polyarb.perception.store import OpportunityPerceptionStore

    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=int(time.time() * 1000) - 1_000,
    )
    perception = OpportunityPerceptionStore(daemon_settings_for_test.db_path)
    perception.init_schema()
    with perception._connect() as con:
        con.execute(
            "INSERT INTO neg_risk_resource_samples(observed_at_ms,sample_json) "
            "VALUES(1,'not-json')"
        )
    daemon_settings_for_test.opportunity_producer_supervisor_enabled = True
    daemon_settings_for_test.opportunity_first_watcher_enabled = False
    daemon_settings_for_test.opportunity_discovery_enabled = False
    daemon_settings_for_test.opportunity_reconciliation_enabled = False
    daemon_settings_for_test.opportunity_resource_controller_enabled = False

    response = http_test_client.get("/health")

    assert response.status_code == 503
    checks = response.json()["checks"]
    assert checks["perception:open_incidents"][0]["status"] == "pass"
    assert checks["perception:resource_mode"][0]["observedValue"] == "disabled"
    assert checks["perception:resource_evidence"][0]["status"] == "fail"
    assert not any(key.endswith("_producer_liveness") for key in checks)


def test_health_incident_evidence_fails_on_restored_trigger_checkpoint_tamper(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    from polyarb.perception.incidents import IncidentManager
    from polyarb.perception.store import OpportunityPerceptionStore

    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=int(time.time() * 1000) - 1_000,
    )
    store = OpportunityPerceptionStore(daemon_settings_for_test.db_path)
    store.init_schema()
    manager = IncidentManager(store, clock_ms=lambda: 1_000)
    for sequence in range(513):
        manager.detect(
            f"operator:health-tamper-{sequence}",
            "manual-investigation",
            {"sequence": sequence},
        )
    trigger_name = "trg_owner_incident_authority_checkpoint_update"
    with sqlite3.connect(store.db_path) as con:
        trigger_sql = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
            (trigger_name,),
        ).fetchone()[0]
        con.execute(f'DROP TRIGGER "{trigger_name}"')
        con.execute(
            "UPDATE neg_risk_incident_authority_checkpoint "
            "SET prefix_hash='sha256:forged'"
        )
        con.execute(trigger_sql)

    response = http_test_client.get("/health")

    assert response.status_code == 503
    check = response.json()["checks"]["perception:incident_evidence"][0]
    assert check["status"] == "fail"
    assert check["output"] == "scopes= evidence_consistent=False"


def test_health_resource_evidence_fails_on_checkpoint_tamper(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    from polyarb.perception.resource_controller import (
        ResourceController,
        ResourceSample,
    )
    from polyarb.perception.store import OpportunityPerceptionStore

    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=int(time.time() * 1000) - 1_000,
    )
    store = OpportunityPerceptionStore(daemon_settings_for_test.db_path)
    store.init_schema()
    ResourceController(
        store,
        clock_ms=lambda: 2_000,
        _verify_store_authority=False,
    ).decide(
        ResourceSample(
            candidate_count=0,
            candidate_quote_p95_ms=None,
            candidate_missing_quote_count=0,
            candidate_worker_ok=True,
            discovery_worker_ok=True,
            reconciliation_running=False,
            previous_discovery_batch_limit=50,
            observed_at_ms=2_000,
        )
    )
    trigger_name = "trg_owner_resource_authority_checkpoint_update"
    with sqlite3.connect(store.db_path) as con:
        trigger_sql = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
            (trigger_name,),
        ).fetchone()[0]
        con.execute(f'DROP TRIGGER "{trigger_name}"')
        con.execute(
            "UPDATE neg_risk_resource_authority_checkpoint "
            "SET last_decision_digest='sha256:forged'"
        )
        con.execute(trigger_sql)
    daemon_settings_for_test.opportunity_resource_controller_enabled = True

    response = http_test_client.get("/health")

    assert response.status_code == 503
    check = response.json()["checks"]["perception:resource_evidence"][0]
    assert check["status"] == "fail"
    assert check["output"] == "evidence_consistent=False"


@pytest.mark.parametrize(
    ("candidate", "discovery", "reconciliation", "expected"),
    [
        (True, False, False, {"candidate"}),
        (False, True, False, {"discovery"}),
        (False, False, True, {"reconciliation"}),
        (True, True, True, {"candidate", "discovery", "reconciliation"}),
    ],
)
def test_health_only_adds_liveness_for_enabled_producers(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
    candidate: bool,
    discovery: bool,
    reconciliation: bool,
    expected: set[str],
) -> None:
    from polyarb.perception.store import OpportunityPerceptionStore

    _insert_snapshot(
        daemon_settings_for_test.db_path,
        taken_at_ms=int(time.time() * 1000) - 1_000,
    )
    OpportunityPerceptionStore(daemon_settings_for_test.db_path).init_schema()
    daemon_settings_for_test.opportunity_producer_supervisor_enabled = True
    daemon_settings_for_test.opportunity_first_watcher_enabled = candidate
    daemon_settings_for_test.opportunity_discovery_enabled = discovery
    daemon_settings_for_test.opportunity_reconciliation_enabled = reconciliation

    checks = http_test_client.get("/health").json()["checks"]
    actual = {
        component
        for component in ("candidate", "discovery", "reconciliation")
        if f"perception:{component}_producer_liveness" in checks
    }
    assert actual == expected


# ---------------------------------------------------------------------------
# Plan 02.1-03 — /healthz Fly-friendly probe (D-05 / D-06)
#
# /healthz mirrors /health body schema but ALWAYS returns HTTP 200 regardless
# of underlying check status. Fly platform probe reads only HTTP code, so this
# keeps Fly proxy routing traffic even when daemon is PAUSED or
# Supabase/R2 are stale. /health (IETF strict 503) remains for Better Stack.
# ---------------------------------------------------------------------------


def test_healthz_returns_200_when_fresh(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot 1h ago (underlying pass) → /healthz HTTP 200."""
    now_ms = int(time.time() * 1000)
    one_hour_ago_ms = now_ms - int(1 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=one_hour_ago_ms)

    resp = http_test_client.get("/healthz")
    assert resp.status_code == 200


def test_healthz_returns_200_when_stale(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot 15h ago (underlying warn) → /healthz HTTP 200."""
    now_ms = int(time.time() * 1000)
    fifteen_hours_ago_ms = now_ms - int(15 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=fifteen_hours_ago_ms)

    resp = http_test_client.get("/healthz")
    assert resp.status_code == 200


def test_healthz_returns_200_when_failed(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """Snapshot 26h ago (underlying fail) → /healthz STILL HTTP 200 (key D-05 differentiator)."""
    now_ms = int(time.time() * 1000)
    twenty_six_hours_ago_ms = now_ms - int(26 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=twenty_six_hours_ago_ms)

    resp = http_test_client.get("/healthz")
    # KEY: NOT 503 — Fly probe sees 200, proxy keeps routing traffic.
    assert resp.status_code == 200


def test_healthz_body_has_status_field_and_content_type(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """GET /healthz body has status field + application/health+json content-type (D-06)."""
    now_ms = int(time.time() * 1000)
    twenty_six_hours_ago_ms = now_ms - int(26 * 3600 * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=twenty_six_hours_ago_ms)

    resp = http_test_client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert body["status"] in ("pass", "warn", "fail")
    # underlying state is fail; only HTTP wrapping differs from /health
    assert body["status"] == "fail"
    # D-06: same schema as /health
    assert "checks" in body
    # RESEARCH Pitfall 5: must use application/health+json content type
    ct = resp.headers.get("content-type", "")
    assert "health+json" in ct
