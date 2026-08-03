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
from polyarb.http.opportunity_read_health import OpportunityReadHealth


def _read_market_truth_health(path: Path, *, now_s: float):
    reader = getattr(health_module, "read_market_truth_health", None)
    assert callable(reader), "read_market_truth_health is not implemented"
    return reader(path, now_s)


def test_opportunity_authority_read_health_warns_then_fails_and_recovers() -> None:
    registry = OpportunityReadHealth()
    source_token = registry.begin_source_attempt(100.0)
    lifecycle_token = registry.begin_lifecycle_attempt(100.0)
    registry.mark_source_fallback(source_token, 100.0, "timeout")
    registry.mark_lifecycle(
        lifecycle_token,
        100.0,
        "unavailable",
        "timeout",
    )

    transient = health_module._opportunity_read_health_checks(
        registry,
        now_s=101.0,
    )
    assert transient["quote_feed:source_truth_read"][0]["status"] == "warn"
    assert transient["quote_feed:lifecycle_read"][0]["status"] == "warn"
    assert (
        "error_kind=timeout" in transient["quote_feed:source_truth_read"][0]["output"]
    )

    source_token = registry.begin_source_attempt(101.0)
    registry.mark_source_fallback(source_token, 101.0, "timeout")
    source_token = registry.begin_source_attempt(101.0)
    registry.mark_source_fallback(source_token, 101.0, "timeout")
    lifecycle_token = registry.begin_lifecycle_attempt(101.0)
    registry.mark_lifecycle(
        lifecycle_token,
        101.0,
        "unavailable",
        "timeout",
    )
    lifecycle_token = registry.begin_lifecycle_attempt(101.0)
    registry.mark_lifecycle(
        lifecycle_token,
        101.0,
        "unavailable",
        "timeout",
    )
    repeated = health_module._opportunity_read_health_checks(
        registry,
        now_s=101.0,
    )
    assert repeated["quote_feed:source_truth_read"][0]["status"] == "fail"
    assert repeated["quote_feed:lifecycle_read"][0]["status"] == "fail"

    persistent = health_module._opportunity_read_health_checks(
        registry,
        now_s=401.0,
    )
    assert persistent["quote_feed:source_truth_read"][0]["status"] == "fail"
    assert persistent["quote_feed:lifecycle_read"][0]["status"] == "fail"
    assert persistent["quote_feed:source_truth_read"][0]["observedValue"] == 301.0

    source_token = registry.begin_source_attempt(402.0)
    lifecycle_token = registry.begin_lifecycle_attempt(402.0)
    registry.mark_source_live(source_token, 402.0)
    registry.mark_lifecycle(lifecycle_token, 402.0, "available", None)
    recovered = health_module._opportunity_read_health_checks(
        registry,
        now_s=403.0,
    )
    assert recovered["quote_feed:source_truth_read"][0]["status"] == "pass"
    assert recovered["quote_feed:lifecycle_read"][0]["status"] == "pass"


def test_health_endpoint_exposes_opportunity_authority_read_fallback(
    http_test_client: TestClient,
) -> None:
    registry = OpportunityReadHealth()
    now_s = time.time()
    source_token = registry.begin_source_attempt(now_s)
    lifecycle_token = registry.begin_lifecycle_attempt(now_s)
    registry.mark_source_fallback(source_token, now_s, "timeout")
    registry.mark_lifecycle(
        lifecycle_token,
        now_s,
        "unavailable",
        "saturated",
    )
    http_test_client.app.state.opportunity_read_health = registry

    checks = http_test_client.get("/healthz").json()["checks"]

    assert checks["quote_feed:source_truth_read"][0]["status"] == "warn"
    assert checks["quote_feed:lifecycle_read"][0]["status"] == "warn"
    assert (
        "status=last-known-authenticated"
        in checks["quote_feed:source_truth_read"][0]["output"]
    )
    assert "error_kind=saturated" in checks["quote_feed:lifecycle_read"][0]["output"]


@pytest.mark.parametrize(
    ("attempts", "age_s", "expected"),
    (
        (1, 300.0, "warn"),
        (2, 300.0, "warn"),
        (3, 0.0, "fail"),
        (1, 300.001, "fail"),
    ),
)
def test_source_unavailable_health_uses_count_and_strict_age_boundary(
    attempts: int,
    age_s: float,
    expected: str,
) -> None:
    registry = OpportunityReadHealth()
    for sequence in range(attempts):
        token = registry.begin_source_attempt(100.0 + sequence)
        registry.mark_source_unavailable(
            token,
            100.0 + sequence,
            "source-truth-unavailable",
        )

    check = health_module._opportunity_read_health_checks(
        registry,
        now_s=100.0 + age_s,
    )["quote_feed:source_truth_read"][0]

    assert check["status"] == expected


def test_invalid_source_authentication_fails_health_immediately() -> None:
    registry = OpportunityReadHealth()
    token = registry.begin_source_attempt(100.0)
    registry.mark_source_unavailable(
        token,
        100.0,
        "source-binding-invalid",
        authentication_invalid=True,
    )

    check = health_module._opportunity_read_health_checks(
        registry,
        now_s=100.0,
    )["quote_feed:source_truth_read"][0]

    assert check["status"] == "fail"
    assert "status=authentication-invalid" in check["output"]


def test_cold_start_health_registers_opportunity_read_checks(
    http_test_client: TestClient,
) -> None:
    checks = http_test_client.get("/healthz").json()["checks"]

    assert checks["quote_feed:source_truth_read"][0]["status"] == "pass"
    assert checks["quote_feed:lifecycle_read"][0]["status"] == "pass"
    assert (
        "status=never-attempted" in checks["quote_feed:source_truth_read"][0]["output"]
    )


def test_structure_generation_health_exposes_stalled_publication_and_cleanup_pressure() -> (
    None
):
    builder = getattr(health_module, "_structure_generation_health_checks", None)
    assert callable(builder), "generation health builder is not implemented"
    checks = builder(
        {
            "pointer_snapshot_id": 9,
            "generation_count_agrees": True,
            "generation_hash_agrees": True,
            "comparison_authenticated": True,
            "publication": {
                "status": "writing",
                "write_component": "markets",
                "write_cursor": "market-42",
                "checkpoint_at_ms": 1_000,
            },
            "comparison": {
                "phase": "generation-universe",
                "cursor": "market-41",
                "checkpoint_at_ms": 1_000,
                "receipt_present": False,
            },
            "cleanup": {
                "generation_snapshot_id": 1,
                "phase": "markets",
                "rows_deleted": 500,
                "checkpoint_at_ms": 1_000,
                "blocked_reason": "generation-entered-retention-floor",
            },
            "retained_generation_count_lower_bound": 9,
            "retained_generation_count_is_exact": False,
            "reclaimable_generation_count_lower_bound": 7,
            "retention_floor": 2,
        },
        now_ms=101_001,
        read_mode="legacy",
        publication_sla_s=100,
        pressure_warn_count=4,
        pressure_fail_count=8,
    )
    assert checks["snapshot:structure_generation"][0]["status"] == "fail"
    assert "stage=writing" in checks["snapshot:structure_generation"][0]["output"]
    assert checks["snapshot:structure_generation_comparison"][0]["status"] == "fail"
    assert checks["snapshot:structure_generation_evidence"][0]["status"] == "fail"
    assert (
        "blocked_reason=generation-entered-retention-floor"
        in checks["snapshot:structure_generation_evidence"][0]["output"]
    )


def test_structure_generation_health_fails_stale_active_comparison() -> None:
    checks = health_module._structure_generation_health_checks(
        {
            "pointer_snapshot_id": 9,
            "generation_count_agrees": True,
            "generation_hash_agrees": True,
            "comparison_authenticated": True,
            "publication": {"status": "published", "checkpoint_at_ms": 100_000},
            "comparison": {
                "generation_snapshot_id": 10,
                "phase": "generation-universe",
                "cursor": "market-9",
                "checkpoint_at_ms": 1_000,
                "receipt_present": False,
            },
            "retained_generation_count_lower_bound": 2,
            "retained_generation_count_is_exact": True,
            "reclaimable_generation_count_lower_bound": 0,
            "retention_floor": 2,
        },
        now_ms=102_000,
        read_mode="legacy",
        publication_sla_s=100,
        pressure_warn_count=4,
        pressure_fail_count=8,
    )
    assert checks["snapshot:structure_generation_comparison"][0]["status"] == "fail"


@pytest.mark.parametrize("quarantine_count", (184, 62))
def test_structure_generation_health_warns_nonfatally_on_exact_quarantine(
    quarantine_count: int,
) -> None:
    checks = health_module._structure_generation_health_checks(
        {
            "pointer_snapshot_id": 9,
            "generation_count_agrees": True,
            "generation_hash_agrees": True,
            "comparison_authenticated": True,
            "publication": {
                "status": "published",
                "checkpoint_at_ms": 100_000,
                "quarantine_count": quarantine_count,
            },
            "comparison": None,
            "retained_generation_count_lower_bound": 2,
            "retained_generation_count_is_exact": True,
            "reclaimable_generation_count_lower_bound": 0,
            "retention_floor": 2,
        },
        now_ms=102_000,
        read_mode="legacy",
        publication_sla_s=100,
        pressure_warn_count=4,
        pressure_fail_count=8,
    )
    check = checks["snapshot:structure_generation"][0]
    assert check["status"] == "warn"
    assert f"quarantine_count={quarantine_count}" in check["output"]


@pytest.mark.parametrize(
    ("checkpoint_at_ms", "blocked_reason", "expected"),
    ((99_000, None, "warn"), (1_000, None, "fail"), (99_000, "invalid-json", "fail")),
)
def test_structure_generation_health_exposes_bootstrap_progress(
    checkpoint_at_ms: int,
    blocked_reason: str | None,
    expected: str,
) -> None:
    checks = health_module._structure_generation_health_checks(
        {
            "pointer_snapshot_id": None,
            "publication": None,
            "bootstrap": {
                "window_id": "window-1",
                "event_cursor": "event-42",
                "member_offset": 3,
                "checkpoint_at_ms": checkpoint_at_ms,
                "blocked_reason": blocked_reason,
            },
            "comparison": None,
            "retained_generation_count_lower_bound": 0,
            "retained_generation_count_is_exact": True,
            "reclaimable_generation_count_lower_bound": 0,
            "retention_floor": 2,
        },
        now_ms=100_000,
        read_mode="legacy",
        publication_sla_s=50,
        pressure_warn_count=4,
        pressure_fail_count=8,
    )
    check = checks["snapshot:structure_generation"][0]
    assert check["status"] == expected
    assert "stage=event-market-bootstrap" in check["output"]
    assert "event_cursor=event-42" in check["output"]
    assert "member_offset=3" in check["output"]


def test_structure_drift_health_is_disabled_warn_pending_and_fail_stale() -> None:
    disabled = health_module._structure_drift_health_check(
        None,
        enabled=False,
        now_ms=10_000,
        publication_sla_s=100,
    )["snapshot:structure_generation_drift"][0]
    assert disabled["status"] == "pass"
    assert disabled["observedValue"] == "disabled"

    pending = health_module._structure_drift_health_check(
        {
            "authorization_mode": "none",
            "authorized": False,
            "checkpoint_at_ms": 9_000,
            "class_counts": {"shared": 500},
            "generation_snapshot_id": 848,
            "legacy_snapshot_id": 845,
            "phase": "generation-members",
            "publication_id": "publication-848",
            "reason": "structure-drift-incomplete",
            "window_id": "window-97b",
        },
        enabled=True,
        now_ms=10_000,
        publication_sla_s=100,
    )["snapshot:structure_generation_drift"][0]
    assert pending["status"] == "warn"
    assert "generation_snapshot_id=848" in pending["output"]
    assert 'class_counts={"shared": 500}' in pending["output"]

    stale = health_module._structure_drift_health_check(
        {
            "authorization_mode": "none",
            "authorized": False,
            "checkpoint_at_ms": 9_000,
            "class_counts": {"unclassified": 1},
            "generation_snapshot_id": 848,
            "legacy_snapshot_id": 845,
            "phase": "stale",
            "publication_id": "publication-848",
            "reason": "structure-drift-stale",
            "window_id": "window-97b",
        },
        enabled=True,
        now_ms=10_000,
        publication_sla_s=100,
    )["snapshot:structure_generation_drift"][0]
    assert stale["status"] == "fail"

    failed_attempt = health_module._structure_drift_health_check(
        {
            "authorization_mode": "drift-safe-sealed",
            "authorized": True,
            "phase": "sealed",
            "reason": None,
            "latest_attempt": {
                "id": 7,
                "outcome": "failed",
                "failure_kind": "structure-drift-signal-sigkill-possible-oom",
                "started_at_ms": 9_500,
            },
        },
        enabled=True,
        now_ms=10_000,
        publication_sla_s=100,
    )["snapshot:structure_generation_drift"][0]
    assert failed_attempt["status"] == "fail"
    assert "latest_attempt_id=7" in failed_attempt["output"]


@pytest.mark.parametrize(
    ("free", "expected_status"),
    ((25, "pass"), (19, "warn"), (9, "fail")),
)
def test_health_tracks_physical_volume_headroom(
    http_test_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    free: int,
    expected_status: str,
) -> None:
    monkeypatch.setattr(
        health_module.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(total=100, used=100 - free, free=free),
    )

    check = http_test_client.get("/health").json()["checks"][
        "storage:volume_free_percent"
    ][0]

    assert check["observedValue"] == float(free)
    assert check["status"] == expected_status
    assert check["output"] == f"free_bytes={free} total_bytes=100"


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


def test_event_member_health_recovers_after_validated_seal(
    daemon_settings_for_test: Any, http_test_client: TestClient,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    window_id = str(store.begin_or_resume_structure_sync(started_at_ms=1)["id"])
    store.commit_structure_event_page(
        window_id=window_id, requested_cursor=None, next_cursor=None,
        completed=True, events=[{"id": "event-1", "markets": []}],
        finished_at_ms=2,
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute("UPDATE structure_sync_windows SET status='complete'")
    recovering = http_test_client.get("/healthz").json()["checks"][
        "snapshot:structure_event_members"
    ][0]
    assert (recovering["observedValue"], recovering["status"]) == (
        "recovering", "warn",
    )
    store.advance_structure_event_member_staging_chunk(window_id=window_id)
    for path in ("/health", "/healthz"):
        sealed = http_test_client.get(path).json()["checks"][
            "snapshot:structure_event_members"
        ][0]
        assert (sealed["observedValue"], sealed["status"], sealed["output"]) == (
            "sealed", "pass", "rows=0 invalid=0",
        )


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
    _insert_snapshot(
        daemon_settings_for_test.db_path, taken_at_ms=twenty_six_hours_ago_ms
    )

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

    attempt = http_test_client.get("/health").json()["checks"][
        "snapshot:latest_attempt"
    ][0]

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
    window = store.begin_or_resume_structure_sync(started_at_ms=now_ms - 30_000)
    store.commit_structure_event_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[],
        finished_at_ms=now_ms - 20_000,
    )
    store.commit_structure_market_page(
        window_id=window["id"],
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[],
        finished_at_ms=now_ms - 10_000,
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
        "producer_slot_budget_s=75 attempt_timeout_s=75 "
        "generation_checkpoint_budget_s=45 generation_child_hard_limit_s=75 "
        "pointer_switch_hard_deadline_s=15 "
        "configured_cadence_s=3600 effective_cadence_s=348 "
        "success_samples=10 success_p95_s=236 reason=timeout-backoff"
    )


def test_health_surfaces_short_incomplete_structure_slice_budget(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=now_ms - 2_000)
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.begin_or_resume_structure_sync(started_at_ms=now_ms - 1_000)

    response = http_test_client.get("/health")

    schedule = response.json()["checks"]["snapshot:schedule"][0]
    assert "producer_slot_budget_s=75 attempt_timeout_s=75" in schedule["output"]
    assert "generation_checkpoint_budget_s=45" in schedule["output"]
    assert "generation_child_hard_limit_s=75" in schedule["output"]
    assert "pointer_switch_hard_deadline_s=15" in schedule["output"]
    assert "finalizer_slot_budget_s" not in schedule["output"]


def test_health_surfaces_bounded_generation_publication_budgets(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    """A terminal chunk cannot silently inherit the former 180s finalizer slot."""
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1000)
    _insert_snapshot(daemon_settings_for_test.db_path, taken_at_ms=now_ms - 2_000)
    SQLiteStore(daemon_settings_for_test.db_path).init_schema()

    schedule = http_test_client.get("/health").json()["checks"]["snapshot:schedule"][0]

    assert "generation_checkpoint_budget_s=45" in schedule["output"]
    assert "generation_child_hard_limit_s=75" in schedule["output"]
    assert "pointer_switch_hard_deadline_s=15" in schedule["output"]
    assert "finalizer_slot_budget_s" not in schedule["output"]


def test_health_surfaces_restart_visible_quote_priority_defer(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1000)
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.record_structure_defer(
        reason="quote-pipeline-active",
        queued_at_ms=now_ms - 5_000,
        observed_at_ms=now_ms - 1_000,
    )

    defer = http_test_client.get("/health").json()["checks"]["snapshot:producer_defer"][
        0
    ]

    assert defer["observedValue"] == "quote-pipeline-active"
    assert defer["status"] == "warn"
    assert "queued_age_seconds=" in defer["output"]
    assert "observed_age_seconds=" in defer["output"]


def test_health_fails_quote_priority_defer_older_than_structure_sla(
    daemon_settings_for_test: Any,
    http_test_client: TestClient,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    now_ms = int(time.time() * 1_000)
    daemon_settings_for_test.structure_publication_sla_s = 100
    SQLiteStore(daemon_settings_for_test.db_path).record_structure_defer(
        reason="quote-pipeline-active",
        queued_at_ms=now_ms - 101_000,
        observed_at_ms=now_ms - 1_000,
    )
    response = http_test_client.get("/health")
    assert response.status_code == 503
    assert response.json()["checks"]["snapshot:producer_defer"][0]["status"] == "fail"


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
        window_id=window["id"],
        requested_cursor=None,
        next_cursor="opaque-2",
        completed=False,
        events=[],
        finished_at_ms=now_ms,
    )

    check = http_test_client.get("/health").json()["checks"]["snapshot:structure_sync"][
        0
    ]

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
            "INSERT INTO neg_risk_resource_samples(observed_at_ms,sample_json) VALUES(1,'not-json')"
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
            "UPDATE neg_risk_incident_authority_checkpoint SET prefix_hash='sha256:forged'"
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
            "UPDATE neg_risk_resource_authority_checkpoint SET last_decision_digest='sha256:forged'"
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
    _insert_snapshot(
        daemon_settings_for_test.db_path, taken_at_ms=twenty_six_hours_ago_ms
    )

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
    _insert_snapshot(
        daemon_settings_for_test.db_path, taken_at_ms=twenty_six_hours_ago_ms
    )

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
