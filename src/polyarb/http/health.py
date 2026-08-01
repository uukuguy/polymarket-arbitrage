"""IETF draft-inadarei-api-health-check-06 compliant /health endpoint + Fly-friendly /healthz.

Phase 02 Plan 02 — D-12 / D-13 / D-16.
Phase 02.1 Plan 03 — D-05 / D-06: separate /healthz (always 200) from /health (IETF strict).

Two endpoints, ONE underlying check logic (see _build_health_checks()):

- GET /health (IETF strict) — Better Stack external probe target.
    * 200 for pass/warn, 503 for fail.
    * 503 is the alarm signal — Better Stack uptime probe interprets it as down.

- GET /healthz (Fly-friendly) — Fly platform machine probe target (per fly.toml).
    * ALWAYS 200 regardless of underlying check status.
    * Status is reported in the JSON body, but Fly proxy reads only the HTTP code.
    * This prevents Fly from withdrawing the machine from the routing pool when
      daemon is PAUSED / Supabase mirror is stale / R2 upload failed —
      observed during Phase 02 Inj 2 and confirmed in Plan 02.1-02 Inj 4
      ("could not find a good candidate within 40 attempts at load balancing").

Three-state health: pass | warn | fail
- pass  → all checks green
- warn  → at least one check degraded, none failed
- fail  → at least one check failed

Plan 02 checks (Plan 03 adds supabase/r2):
1. snapshot:last_success_age_seconds
   - pass  < 14h  (subset cron interval 12h + 2h buffer)
   - warn  14-25h
   - fail  > 25h  OR no snapshot at all
2. snapshot:last_status
   - maps SnapshotStatus.OK → pass, DEGRADED → warn, FAILED → fail
   - if no snapshot → omitted from checks (age check already reports fail)
3. supabase:mirror_age_seconds (when settings.supabase_mirror_enabled)
4. r2:upload_recent_success (when settings.r2_enabled)

Overall = worst-of all sub-checks (fail > warn > pass).

Security note (T-02-09 / T-02.1-6-01): both /health and /healthz are intentionally
PUBLIC (no HMAC). Better Stack uptime probe + Fly platform probe both need
unauthenticated access. Response exposes only snapshot age + status enum —
no DB schema, no IPs, no secrets. (D-22 amendment + D-06.)

Source: datatracker.ietf.org/doc/html/draft-inadarei-api-health-check-06
        RESEARCH.md §8 / 02.1-RESEARCH.md Area 1-2
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.routing.feed_handoff import decide_feed_availability

HEALTH_CONTENT_TYPE = "application/health+json"

# Age thresholds in seconds
_PASS_AGE_S = 14 * 3600  # < 14h → pass
_WARN_AGE_S = 25 * 3600  # 14-25h → warn; > 25h → fail

# Supabase mirror thresholds
_MIRROR_WARN_S = 25 * 3600
_MIRROR_FAIL_S = 48 * 3600


@dataclass(frozen=True)
class MarketTruthHealth:
    """Latest-attempt coverage plus the last complete published truth anchor."""

    coverage_status: str
    coverage_value: str
    latest_attempt_snapshot_id: int | None
    latest_attempt_market_items: int | None
    latest_attempt_event_items: int | None
    last_complete_snapshot_id: int | None
    last_complete_age_seconds: float | None
    last_complete_finished_age_seconds: float | None


@dataclass(frozen=True)
class ArchiveHealth:
    """Non-blocking evidence about the explicit research archive product."""

    latest_status: str
    latest_snapshot_id: int | None
    last_success_snapshot_id: int | None
    last_success_age_seconds: float | None


@dataclass(frozen=True)
class ReconciliationHealth:
    """Exact durable checkpoint state for background Full Reconciliation."""

    progress: str
    window_id: str | None
    pages_completed: int
    next_cursor: str | None
    checkpoint_age_seconds: float | None
    receipt_consistent: bool


@dataclass(frozen=True)
class PerceptionRecoveryHealth:
    open_count: int | None
    scopes: tuple[str, ...]
    resource_mode: str
    resource_reason: str | None
    evidence_consistent: bool


@dataclass(frozen=True)
class IncidentEvidenceHealth:
    open_count: int | None
    scopes: tuple[str, ...]
    evidence_consistent: bool


@dataclass(frozen=True)
class ResourceEvidenceHealth:
    sequence: int | None
    evidence_consistent: bool


@dataclass(frozen=True)
class ProducerLivenessHealth:
    state: str
    age_seconds: float | None
    evidence_consistent: bool


def read_producer_liveness_health(
    path: Path,
    component: str,
    *,
    now_ms: int,
    stall_timeout_ms: int,
) -> ProducerLivenessHealth:
    unavailable = ProducerLivenessHealth("unavailable", None, False)
    if (
        component not in {"candidate", "discovery", "reconciliation"}
        or type(now_ms) is not int
        or now_ms < 0
        or type(stall_timeout_ms) is not int
        or stall_timeout_ms <= 0
    ):
        return unavailable
    try:
        from polyarb.perception.store import validate_producer_history

        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=0.25)
        con.row_factory = sqlite3.Row
        try:
            history = validate_producer_history(con, component, now_ms=now_ms)
            if history.state == "never-started":
                return ProducerLivenessHealth("never-started", None, True)
            anchor_ms = (
                history.terminal_at_ms
                if history.terminal_at_ms is not None
                else (
                    history.last_progress_at_ms
                    if history.last_progress_at_ms is not None
                    else history.latest_started_at_ms
                )
            )
            assert anchor_ms is not None
            age_ms = now_ms - anchor_ms
            state = history.state
            if state in {"starting", "running"} and age_ms > stall_timeout_ms:
                state = "stalled"
            return ProducerLivenessHealth(state, age_ms / 1_000, True)
        finally:
            con.close()
    except (sqlite3.Error, TypeError, ValueError):
        return unavailable


def read_perception_recovery_health(
    path: Path,
    *,
    now_ms: int | None = None,
    include_resource: bool = True,
) -> PerceptionRecoveryHealth:
    unavailable = PerceptionRecoveryHealth(None, (), "unavailable", None, False)
    try:
        from polyarb.perception.incidents import IncidentManager
        from polyarb.perception.resource_controller import (
            validate_resource_history,
        )
        from polyarb.perception.store import OpportunityPerceptionStore

        store = OpportunityPerceptionStore(path, read_only=True)
        open_count, candidate_open, http_open, other_open = IncidentManager(
            store
        ).open_incident_status()
        scopes = []
        if candidate_open:
            scopes.append("candidate")
        if http_open:
            scopes.append("http")
        if other_open:
            scopes.append("other")
        mode, reason = "disabled", None
        if include_resource:
            con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=0.25)
            con.row_factory = sqlite3.Row
            try:
                con.execute("BEGIN")
                decision = validate_resource_history(con)
                if decision is None:
                    mode, reason = "idle", None
                else:
                    if now_ms is not None and (
                        now_ms < decision.decided_at_ms
                        or now_ms > decision.valid_until_ms
                    ):
                        return unavailable
                    mode, reason = decision.mode, decision.reason
                con.commit()
            finally:
                con.close()
        return PerceptionRecoveryHealth(
            open_count,
            tuple(scopes),
            mode,
            reason,
            True,
        )
    except (sqlite3.Error, TypeError, ValueError, KeyError):
        return unavailable


def read_incident_evidence_health(path: Path) -> IncidentEvidenceHealth:
    unavailable = IncidentEvidenceHealth(None, (), False)
    try:
        from polyarb.perception.incidents import IncidentManager
        from polyarb.perception.store import OpportunityPerceptionStore

        count, candidate_open, http_open, other_open = IncidentManager(
            OpportunityPerceptionStore(path, read_only=True)
        ).open_incident_status()
        scopes = []
        if candidate_open:
            scopes.append("candidate")
        if http_open:
            scopes.append("http")
        if other_open:
            scopes.append("other")
        return IncidentEvidenceHealth(count, tuple(scopes), True)
    except (sqlite3.Error, TypeError, ValueError, KeyError):
        return unavailable


def read_resource_evidence_health(path: Path) -> ResourceEvidenceHealth:
    unavailable = ResourceEvidenceHealth(None, False)
    try:
        from polyarb.perception.resource_controller import (
            validate_resource_evidence_failure,
            validate_resource_history,
        )
        from polyarb.perception.store import OpportunityPerceptionStore

        store = OpportunityPerceptionStore(path, read_only=True)
        con = store._connect()
        try:
            con.execute("BEGIN")
            store._assert_owner_journal_clean(con)
            validate_resource_evidence_failure(con, require_resolved=True)
            decision = validate_resource_history(con)
            con.commit()
        finally:
            con.close()
        return ResourceEvidenceHealth(
            None if decision is None else decision.sequence,
            True,
        )
    except (sqlite3.Error, TypeError, ValueError, KeyError):
        return unavailable


def read_reconciliation_health(path: Path, now_ms: int) -> ReconciliationHealth:
    """Read and validate the exact window/receipt/staging/baseline snapshot."""
    empty = ReconciliationHealth("idle", None, 0, None, None, True)
    unavailable = ReconciliationHealth("unavailable", None, 0, None, None, False)
    try:
        from polyarb.perception.store import OpportunityPerceptionStore

        window = OpportunityPerceptionStore(path, read_only=True).current_reconciliation()
        if window is None:
            return empty
    except (sqlite3.Error, TypeError, ValueError):
        return unavailable
    return ReconciliationHealth(
        progress=window.status,
        window_id=window.id,
        pages_completed=window.pages_completed,
        next_cursor=window.next_cursor,
        checkpoint_age_seconds=max(0.0, (now_ms - window.checkpoint_at_ms) / 1000),
        receipt_consistent=True,
    )


def read_market_truth_health(path: Path, now_s: float) -> MarketTruthHealth:
    """Read durable market-truth health without certifying diagnostic rows."""
    empty = MarketTruthHealth(
        coverage_status="fail",
        coverage_value="incomplete-source",
        latest_attempt_snapshot_id=None,
        latest_attempt_market_items=None,
        latest_attempt_event_items=None,
        last_complete_snapshot_id=None,
        last_complete_age_seconds=None,
        last_complete_finished_age_seconds=None,
    )
    try:
        con = sqlite3.connect(
            f"file:{path}?mode=ro",
            uri=True,
            timeout=0.25,
        )
    except sqlite3.Error:
        return empty
    try:
        con.execute("BEGIN")
        latest = con.execute(
            "SELECT s.id,s.market_view_published,c.completed,"
            "c.market_items,c.event_items "
            "FROM snapshots s "
            "LEFT JOIN snapshot_source_coverage c ON c.snapshot_id=s.id "
            "WHERE s.data_product='structure' "
            "ORDER BY s.id DESC LIMIT 1"
        ).fetchone()
        complete = con.execute(
            "SELECT s.id,s.taken_at_ms,s.finished_at_ms "
            "FROM snapshots s "
            "JOIN snapshot_source_coverage c ON c.snapshot_id=s.id "
            "WHERE s.data_product='structure' AND s.market_view_published=1 "
            "AND s.is_valid=1 AND c.completed=1 "
            "ORDER BY s.id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        return empty
    finally:
        con.close()

    if latest is None:
        return empty
    latest_id, published, completed, market_items, event_items = latest
    coverage_complete = completed == 1 and published == 1
    complete_id = complete[0] if complete is not None else None
    complete_age = max(0.0, now_s - complete[1] / 1000.0) if complete is not None else None
    complete_finished_age = (
        max(0.0, now_s - complete[2] / 1000.0)
        if complete is not None and isinstance(complete[2], (int, float))
        else None
    )
    return MarketTruthHealth(
        coverage_status="pass" if coverage_complete else "fail",
        coverage_value="complete" if coverage_complete else "incomplete-source",
        latest_attempt_snapshot_id=latest_id,
        latest_attempt_market_items=market_items,
        latest_attempt_event_items=event_items,
        last_complete_snapshot_id=complete_id,
        last_complete_age_seconds=complete_age,
        last_complete_finished_age_seconds=complete_finished_age,
    )


def read_archive_health(path: Path, now_s: float) -> ArchiveHealth:
    """Read Archive evidence without allowing it to gate online market truth."""
    empty = ArchiveHealth(
        latest_status="never-run",
        latest_snapshot_id=None,
        last_success_snapshot_id=None,
        last_success_age_seconds=None,
    )
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=0.25)
    except sqlite3.Error:
        return empty
    try:
        latest = con.execute(
            "SELECT id,archive_status FROM snapshots "
            "WHERE data_product='archive' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        success = con.execute(
            "SELECT id,taken_at_ms FROM snapshots "
            "WHERE data_product='archive' AND is_valid=1 "
            "AND archive_status IN ('local_complete','r2_uploaded') "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        return empty
    finally:
        con.close()
    if latest is None:
        return empty
    success_age = max(0.0, now_s - success[1] / 1000.0) if success is not None else None
    return ArchiveHealth(
        latest_status=str(latest[1]),
        latest_snapshot_id=int(latest[0]),
        last_success_snapshot_id=int(success[0]) if success is not None else None,
        last_success_age_seconds=success_age,
    )


def _utc_now_iso() -> str:
    """Current UTC timestamp in ISO 8601 format with Z suffix."""
    return datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _severity(a: str, b: str) -> str:
    """Return worst of two health statuses (fail > warn > pass)."""
    order = {"pass": 0, "warn": 1, "fail": 2}
    return a if order.get(a, 0) >= order.get(b, 0) else b


def _build_health_checks(
    store: Any,
    settings: Any,
    now_s: float,
    quote_worker_runtime: Any | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], str]:
    """Compute all health sub-checks and the overall status.

    Shared by /health (IETF strict — fail → 503) and /healthz (always 200).
    Both endpoints render the same JSON body shape; only the HTTP status code
    differs (per D-06 — full-mirror body schema, do NOT diverge check logic).

    Args:
        store: SQLiteStore instance (read-only).
        settings: Settings instance — drives optional mirror/r2 checks.
        now_s: current epoch seconds (passed in so /health and /healthz agree
               on age values within a single request handler invocation).

    Returns:
        (checks_dict, overall_status) where overall_status in {"pass","warn","fail"}.
    """
    checks: dict[str, list[dict[str, Any]]] = {}
    overall = "pass"

    recovery_enabled = bool(getattr(settings, "opportunity_producer_supervisor_enabled", False))
    resource_enabled = bool(getattr(settings, "opportunity_resource_controller_enabled", False))
    incident_evidence = read_incident_evidence_health(store.db_path)
    incident_evidence_status = (
        "pass" if incident_evidence.evidence_consistent else "fail"
    )
    overall = _severity(overall, incident_evidence_status)
    checks["perception:incident_evidence"] = [
        {
            "componentId": "perception-incident-evidence",
            "componentType": "component",
            "observedValue": incident_evidence.open_count,
            "status": incident_evidence_status,
            "output": (
                f"scopes={','.join(incident_evidence.scopes)} "
                f"evidence_consistent={incident_evidence.evidence_consistent}"
            ),
            "time": _utc_now_iso(),
        }
    ]
    resource_evidence = read_resource_evidence_health(store.db_path)
    resource_evidence_status = (
        "pass" if resource_evidence.evidence_consistent else "fail"
    )
    overall = _severity(overall, resource_evidence_status)
    checks["perception:resource_evidence"] = [
        {
            "componentId": "perception-resource-evidence",
            "componentType": "component",
            "observedValue": resource_evidence.sequence,
            "status": resource_evidence_status,
            "output": (
                "evidence_consistent="
                f"{resource_evidence.evidence_consistent}"
            ),
            "time": _utc_now_iso(),
        }
    ]
    recovery = read_perception_recovery_health(
        store.db_path,
        now_ms=int(now_s * 1_000),
        include_resource=resource_enabled,
    )
    incident_status = "pass"
    if recovery_enabled:
        if not recovery.evidence_consistent:
            incident_status = "fail"
        elif recovery.open_count:
            incident_status = (
                "fail"
                if any(
                    scope == "candidate" or scope == "http" or scope.startswith("candidate:")
                    for scope in recovery.scopes
                )
                else "warn"
            )
    overall = _severity(overall, incident_status)
    checks["perception:open_incidents"] = [
        {
            "componentId": "perception-recovery",
            "componentType": "component",
            "observedValue": (recovery.open_count if recovery_enabled else 0),
            "status": incident_status,
            "output": (
                f"scopes={','.join(recovery.scopes)} "
                f"evidence_consistent={recovery.evidence_consistent}"
                if recovery_enabled
                else "disabled"
            ),
            "time": _utc_now_iso(),
        }
    ]
    liveness_components = []
    if bool(getattr(settings, "opportunity_first_watcher_enabled", False)):
        liveness_components.append("candidate")
    if bool(getattr(settings, "opportunity_discovery_enabled", False)):
        liveness_components.append("discovery")
    if bool(getattr(settings, "opportunity_reconciliation_enabled", False)):
        liveness_components.append("reconciliation")
    stall_timeout_ms = int(
        float(getattr(settings, "producer_stall_timeout_s", 180.0)) * 1_000
    )
    for component in liveness_components:
        liveness = read_producer_liveness_health(
            store.db_path,
            component,
            now_ms=int(now_s * 1_000),
            stall_timeout_ms=stall_timeout_ms,
        )
        unhealthy = (
            not liveness.evidence_consistent
            or liveness.state
            not in {"starting", "running"}
        )
        liveness_status = (
            ("fail" if component == "candidate" else "warn")
            if recovery_enabled and unhealthy
            else "pass"
        )
        overall = _severity(overall, liveness_status)
        checks[f"perception:{component}_producer_liveness"] = [
            {
                "componentId": f"perception-{component}-producer",
                "componentType": "component",
                "observedValue": (
                    liveness.state if recovery_enabled else "disabled"
                ),
                "status": liveness_status,
                "output": (
                    f"age_seconds={liveness.age_seconds} "
                    f"evidence_consistent={liveness.evidence_consistent}"
                ),
                "time": _utc_now_iso(),
            }
        ]
    resource_status = "pass"
    if resource_enabled and (
        not recovery.evidence_consistent
        or recovery.resource_mode
        in {"unavailable", "idle", "protect-hot-path", "empty-candidate-exploration"}
    ):
        resource_status = "fail" if recovery.resource_mode in {"unavailable", "idle"} else "warn"
    overall = _severity(overall, resource_status)
    checks["perception:resource_mode"] = [
        {
            "componentId": "perception-resource-controller",
            "componentType": "component",
            "observedValue": (recovery.resource_mode if resource_enabled else "disabled"),
            "status": resource_status,
            "output": f"reason={recovery.resource_reason}",
            "time": _utc_now_iso(),
        }
    ]

    # Full Reconciliation is calibration evidence, never Candidate availability.
    # Its scoped checks read the same checkpoint rows the worker mutates but do
    # not feed `overall`; Task 5 owns incident escalation for a stalled window.
    reconciliation_enabled = bool(getattr(settings, "opportunity_reconciliation_enabled", False))
    reconciliation = read_reconciliation_health(store.db_path, int(now_s * 1_000))
    progress_value = reconciliation.progress if reconciliation_enabled else "disabled"
    progress_status = (
        "warn"
        if reconciliation_enabled
        and (
            reconciliation.progress in {"idle", "failed", "unavailable"}
            or not reconciliation.receipt_consistent
        )
        else "pass"
    )
    checks["perception:reconciliation_progress"] = [
        {
            "componentId": "perception-reconciliation",
            "componentType": "component",
            "observedValue": progress_value,
            "status": progress_status,
            "output": (
                f"window_id={reconciliation.window_id} "
                f"pages_completed={reconciliation.pages_completed} "
                f"receipt_consistent={reconciliation.receipt_consistent}"
            ),
            "time": _utc_now_iso(),
        }
    ]
    checkpoint_age = reconciliation.checkpoint_age_seconds if reconciliation_enabled else None
    checkpoint_warn_s = float(getattr(settings, "reconciliation_checkpoint_warn_s", 900.0))
    checkpoint_status = (
        "warn" if checkpoint_age is not None and checkpoint_age > checkpoint_warn_s else "pass"
    )
    checks["perception:reconciliation_checkpoint_age_seconds"] = [
        {
            "componentId": "perception-reconciliation",
            "componentType": "datastore",
            "observedValue": (None if checkpoint_age is None else round(checkpoint_age, 1)),
            "observedUnit": "s",
            "status": checkpoint_status,
            "output": f"next_cursor={reconciliation.next_cursor}",
            "time": _utc_now_iso(),
        }
    ]

    # ── Check 0: authoritative market-truth coverage ──────────────────────
    market_truth = read_market_truth_health(store.db_path, now_s)
    overall = _severity(overall, market_truth.coverage_status)
    checks["market_truth:coverage"] = [
        {
            "componentId": "market-truth",
            "componentType": "datastore",
            "observedValue": market_truth.coverage_value,
            "status": market_truth.coverage_status,
            "output": (
                f"markets={market_truth.latest_attempt_market_items} "
                f"events={market_truth.latest_attempt_event_items}"
            ),
            "time": _utc_now_iso(),
        }
    ]

    truth_age = market_truth.last_complete_age_seconds
    if truth_age is None:
        truth_age_status = "fail"
    elif truth_age < _PASS_AGE_S:
        truth_age_status = "pass"
    elif truth_age < _WARN_AGE_S:
        truth_age_status = "warn"
    else:
        truth_age_status = "fail"
    overall = _severity(overall, truth_age_status)
    checks["market_truth:last_complete_age_seconds"] = [
        {
            "componentId": "market-truth",
            "componentType": "datastore",
            "observedValue": round(truth_age, 1) if truth_age is not None else None,
            "observedUnit": "s",
            "status": truth_age_status,
            "output": (
                f"snapshot_id={market_truth.last_complete_snapshot_id}"
                if market_truth.last_complete_snapshot_id is not None
                else "no-complete-published-market-truth"
            ),
            "time": _utc_now_iso(),
        }
    ]

    # ── Check 0.5: explicit Archive evidence (non-blocking by design) ─────
    # Archive is P1 research/audit.  A failure remains visible, but it must
    # not make strict health fail or pause the Structure → Quote → M2 path.
    archive = read_archive_health(store.db_path, now_s)
    archive_attempt_status = "pass" if archive.latest_status == "local_complete" else "warn"
    checks["archive:last_attempt"] = [
        {
            "componentId": "market-archive",
            "componentType": "datastore",
            "observedValue": archive.latest_status,
            "status": archive_attempt_status,
            "output": (
                f"snapshot_id={archive.latest_snapshot_id}"
                if archive.latest_snapshot_id is not None
                else "archive-not-scheduled"
            ),
            "time": _utc_now_iso(),
        }
    ]
    archive_age = archive.last_success_age_seconds
    checks["archive:last_success_age_seconds"] = [
        {
            "componentId": "market-archive",
            "componentType": "datastore",
            "observedValue": round(archive_age, 1) if archive_age is not None else None,
            "observedUnit": "s",
            "status": "pass" if archive_age is not None else "warn",
            "output": (
                f"snapshot_id={archive.last_success_snapshot_id}"
                if archive.last_success_snapshot_id is not None
                else "no-successful-archive"
            ),
            "time": _utc_now_iso(),
        }
    ]

    # ── Check 1: snapshot age ─────────────────────────────────────────────
    last_snapshot = store.get_latest_snapshot(data_product="structure")

    if last_snapshot is None:
        age_s: float | None = None
        age_status = "fail"
        overall = _severity(overall, "fail")
    else:
        taken_at_ms = last_snapshot["taken_at_ms"]
        age_s = now_s - taken_at_ms / 1000.0
        if age_s < _PASS_AGE_S:
            age_status = "pass"
        elif age_s < _WARN_AGE_S:
            age_status = "warn"
        else:
            age_status = "fail"
        overall = _severity(overall, age_status)

    checks["snapshot:last_success_age_seconds"] = [
        {
            "componentId": "scheduler",
            "componentType": "component",
            "observedValue": round(age_s, 1) if age_s is not None else None,
            "observedUnit": "s",
            "status": age_status,
            "time": _utc_now_iso(),
        }
    ]

    # ── Check 2: last snapshot status ─────────────────────────────────────
    if last_snapshot is not None:
        # snapshot_status is written atomically with the result.  `notes` is
        # intentionally only a bounded failure reason and cannot distinguish
        # a valid DEGRADE from a clean OK.
        persisted_status = str(last_snapshot.get("snapshot_status") or "").lower()
        if persisted_status == "degraded":
            last_status_val = "DEGRADED"
            status_check = "warn"
        elif persisted_status == "failed" or not last_snapshot.get("is_valid", True):
            last_status_val = "FAILED"
            status_check = "fail"
        else:
            last_status_val = "OK"
            status_check = "pass"

        overall = _severity(overall, status_check)
        checks["snapshot:last_status"] = [
            {
                "componentId": "orchestrator",
                "observedValue": last_status_val,
                "status": status_check,
                "time": _utc_now_iso(),
            }
        ]

    # ── Check 2.5: parent-observed scheduler attempt truth ───────────────
    from polyarb.daemon.scheduler import (
        SNAPSHOT_SUBPROCESS_TIMEOUT_S,
        STRUCTURE_PRODUCER_SLOT_BUDGET_S,
    )

    schedule_adjustment = store.get_latest_structure_schedule_adjustment()
    configured_timeout_s = int(SNAPSHOT_SUBPROCESS_TIMEOUT_S)
    configured_cadence_s = int(settings.scheduler_interval_s)
    if schedule_adjustment is None:
        effective_timeout_s = configured_timeout_s
        effective_cadence_s = configured_cadence_s
        schedule_value = "configured"
        success_sample_count = 0
        success_p95_s: object = None
        schedule_reason = "configured"
    else:
        effective_timeout_s = int(schedule_adjustment["timeout_s"])
        effective_cadence_s = int(schedule_adjustment["cadence_s"])
        schedule_value = "adaptive"
        success_sample_count = int(schedule_adjustment["success_sample_count"])
        success_p95_s = schedule_adjustment["success_p95_s"]
        schedule_reason = str(schedule_adjustment["reason"])
    producer_slot_budget_s = int(STRUCTURE_PRODUCER_SLOT_BUDGET_S)
    attempt_timeout_s = min(effective_timeout_s, producer_slot_budget_s)

    checks["snapshot:schedule"] = [
        {
            "componentId": "snapshot-scheduler",
            "componentType": "component",
            "observedValue": schedule_value,
            "status": "pass",
            "output": (
                f"configured_timeout_s={configured_timeout_s} "
                f"effective_timeout_s={effective_timeout_s} "
                f"producer_slot_budget_s={producer_slot_budget_s} "
                f"attempt_timeout_s={attempt_timeout_s} "
                f"configured_cadence_s={configured_cadence_s} "
                f"effective_cadence_s={effective_cadence_s} "
                f"success_samples={success_sample_count} "
                f"success_p95_s={success_p95_s} reason={schedule_reason}"
            ),
            "time": _utc_now_iso(),
        }
    ]

    latest_attempt = store.get_latest_snapshot_attempt()
    if latest_attempt is None:
        attempt_value = "never-started"
        # Existing installations can have valid published truth from before
        # attempt recording was introduced. Absence is not a fabricated fault;
        # an explicit failed/cancelled row below is the new alert signal.
        attempt_status = "pass"
        attempt_output = None
    else:
        attempt_value = str(latest_attempt["outcome"])
        diagnostic_parts = []
        failure_kind = latest_attempt["failure_kind"]
        is_structure_checkpoint = (
            attempt_value == "cancelled"
            and failure_kind == "structure-checkpoint"
        )
        if failure_kind is not None and not is_structure_checkpoint:
            diagnostic_parts.append(str(failure_kind))
        last_stage = latest_attempt["last_stage"]
        if last_stage is not None:
            diagnostic_parts.append(f"stage={last_stage}")
        elapsed_ms = latest_attempt["elapsed_ms"]
        if elapsed_ms is not None:
            diagnostic_parts.append(f"elapsed_ms={elapsed_ms}")
        attempt_output = " ".join(diagnostic_parts) or None
        if is_structure_checkpoint:
            attempt_value = "checkpointed"
            attempt_status = "pass"
        elif attempt_value in {"failed", "cancelled"}:
            attempt_status = "warn" if truth_age_status == "pass" else "fail"
        elif attempt_value == "running":
            attempt_age_s = max(
                0.0,
                now_s - int(latest_attempt["started_at_ms"]) / 1000.0,
            )
            if attempt_age_s > attempt_timeout_s:
                attempt_status = "fail"
                attempt_output = "snapshot-subprocess-timeout-exceeded"
            else:
                attempt_status = "pass"
                attempt_output = None
        else:
            attempt_status = "pass"
    overall = _severity(overall, attempt_status)
    checks["snapshot:latest_attempt"] = [
        {
            "componentId": "snapshot-scheduler",
            "componentType": "component",
            "observedValue": attempt_value,
            "status": attempt_status,
            "output": attempt_output,
            "time": _utc_now_iso(),
        }
    ]

    scheduler_state = store.get_scheduler_state()
    failure_counter = (
        int(scheduler_state.get("failure_counter", 0)) if scheduler_state is not None else 0
    )
    from polyarb.daemon.scheduler import SnapshotScheduler

    if failure_counter == 0:
        counter_status = "pass"
    elif failure_counter < SnapshotScheduler.FAILURE_THRESHOLD:
        counter_status = "warn"
    else:
        counter_status = "fail"
    overall = _severity(overall, counter_status)
    checks["snapshot:failure_counter"] = [
        {
            "componentId": "snapshot-scheduler",
            "componentType": "component",
            "observedValue": failure_counter,
            "status": counter_status,
            "time": _utc_now_iso(),
        }
    ]

    sync_window = store.get_latest_structure_sync()
    if sync_window is None:
        sync_value = "idle"
        sync_status = "pass"
        sync_output = None
    else:
        sync_value = str(sync_window["status"])
        if sync_value == "published":
            sync_status = "pass"
        elif sync_value == "failed":
            sync_status = "fail"
        else:
            sync_status = "warn"
        stage = (
            "events"
            if sync_value == "open"
            else "markets"
            if sync_value == "events_complete"
            else "publish"
        )
        sync_output = (
            f"stage={stage} event_pages={int(sync_window['event_pages'])} "
            f"market_pages={int(sync_window['market_pages'])}"
        )
    overall = _severity(overall, sync_status)
    checks["snapshot:structure_sync"] = [
        {
            "componentId": "snapshot-scheduler",
            "componentType": "component",
            "observedValue": sync_value,
            "status": sync_status,
            **({"output": sync_output} if sync_output is not None else {}),
            "time": _utc_now_iso(),
        }
    ]

    # ── Check 3: Supabase mirror age (only if mirror enabled) ────────────
    # supabase:mirror_age_seconds — seconds since last successful Supabase push.
    # Uses supabase_mirror_at_ms column added in Plan 03.
    # warn if > 25h (cron interval 12h + 13h buffer); fail if > 48h.
    if settings.supabase_mirror_enabled:
        if last_snapshot is not None and last_snapshot.get("supabase_mirror_at_ms") is not None:
            mirror_age_s: float | None = now_s - last_snapshot["supabase_mirror_at_ms"] / 1000.0
            if mirror_age_s < _MIRROR_WARN_S:
                mirror_status = "pass"
            elif mirror_age_s < _MIRROR_FAIL_S:
                mirror_status = "warn"
            else:
                mirror_status = "fail"
        else:
            # No mirror timestamp yet (mirror disabled or first run) → warn (not fail)
            mirror_age_s = None
            mirror_status = "warn"
        overall = _severity(overall, mirror_status)
        checks["supabase:mirror_age_seconds"] = [
            {
                "componentId": "supabase-mirror",
                "componentType": "datastore",
                "observedValue": round(mirror_age_s, 1) if mirror_age_s is not None else None,
                "observedUnit": "s",
                "status": mirror_status,
                "time": _utc_now_iso(),
            }
        ]

    # ── Check 4: R2 upload recent success (only if R2 enabled) ───────────
    # r2:upload_recent_success — True if last snapshot has a parquet_r2_url.
    # Uses parquet_r2_url column added in Plan 03.
    if settings.r2_enabled:
        if last_snapshot is not None and last_snapshot.get("parquet_r2_url"):
            r2_status = "pass"
            r2_value: bool = True
        elif last_snapshot is not None:
            # Snapshot exists but no R2 URL — warn (first run or upload failed)
            r2_status = "warn"
            r2_value = False
        else:
            r2_status = "warn"
            r2_value = False
        overall = _severity(overall, r2_status)
        checks["r2:upload_recent_success"] = [
            {
                "componentId": "r2-archive",
                "componentType": "system",
                "observedValue": r2_value,
                "status": r2_status,
                "time": _utc_now_iso(),
            }
        ]

    # ── Check 5: production opportunity quote freshness ──────────────────
    if settings.neg_risk_quote_worker_enabled:
        from polyarb.routing.opportunity_scanner import (
            QUOTE_WARN_SECONDS,
        )

        runtime_snapshot = (
            quote_worker_runtime.snapshot() if quote_worker_runtime is not None else None
        )
        quote_run = (
            quote_worker_runtime.certified_projection()
            if quote_worker_runtime is not None
            else None
        )
        quote_output: str | None = None
        if quote_run is None:
            quote_age_s: float | None = None
            quote_status = "fail"
            quote_output = "certified-projection-unavailable"
        else:
            quote_age_s = max(0.0, now_s - quote_run.quoted_at_ms / 1000.0)
            universe_age_s = max(
                0.0,
                now_s - quote_run.universe_taken_at_ms / 1000.0,
            )
            availability = decide_feed_availability(
                source_snapshot_id=quote_run.universe_snapshot_id,
                latest_structure_snapshot_id=(
                    market_truth.last_complete_snapshot_id
                ),
                quote_age_seconds=quote_age_s,
                universe_age_seconds=universe_age_s,
                handoff_age_seconds=(
                    market_truth.last_complete_finished_age_seconds
                ),
            )
            if not availability.available:
                quote_status = "fail"
                quote_output = (
                    None
                    if availability.reason == "stale-quote"
                    else availability.reason
                )
            elif availability.refreshing:
                quote_status = "warn"
                quote_output = availability.reason
            elif quote_age_s < QUOTE_WARN_SECONDS:
                quote_status = "pass"
            else:
                quote_status = "warn"
        overall = _severity(overall, quote_status)
        checks["quote_feed:last_complete_age_seconds"] = [
            {
                "componentId": "neg-risk-quote-worker",
                "componentType": "component",
                "observedValue": (round(quote_age_s, 1) if quote_age_s is not None else None),
                "observedUnit": "s",
                "status": quote_status,
                "output": quote_output,
                "time": _utc_now_iso(),
            }
        ]

        if runtime_snapshot is not None:
            runtime = runtime_snapshot
            if runtime.state in {"cold-start", "error"}:
                collector_status = "warn"
            elif runtime.state == "stopped":
                collector_status = "fail"
            else:
                collector_status = "pass"
            overall = _severity(overall, collector_status)
            checks["quote_feed:collector_state"] = [
                {
                    "componentId": "neg-risk-quote-worker",
                    "componentType": "component",
                    "observedValue": runtime.state,
                    "status": collector_status,
                    "output": runtime.last_error_kind,
                    "time": _utc_now_iso(),
                }
            ]
            cleanup_failures = runtime.cleanup_consecutive_failures
            if cleanup_failures >= 3:
                retention_status = "fail"
            elif cleanup_failures:
                retention_status = "warn"
            else:
                retention_status = "pass"
            overall = _severity(overall, retention_status)
            checks["quote_feed:retention"] = [
                {
                    "componentId": "neg-risk-quote-retention",
                    "componentType": "datastore",
                    "observedValue": cleanup_failures,
                    "status": retention_status,
                    "output": runtime.last_cleanup_error_kind,
                    "time": _utc_now_iso(),
                }
            ]

    return checks, overall


def _build_health_body(
    overall: str,
    checks: dict[str, list[dict[str, Any]]],
    settings: Any,
    *,
    machine_id: str,
    boot_id: str,
) -> dict[str, Any]:
    """Shared IETF body shape — same for /health and /healthz (D-06 full mirror)."""
    return {
        "status": overall,
        "version": settings.version,
        "releaseId": settings.release_id,
        "machineId": machine_id,
        "bootId": boot_id,
        "qualificationPolicy": {
            "candidateQuoteHardStaleS": settings.candidate_quote_hard_stale_s,
            "candidateLowerLaneMaxWaitS": (
                settings.candidate_lower_lane_max_wait_s
            ),
            "discoveryCandidateMaxWaitS": (
                settings.discovery_candidate_max_wait_s
            ),
            "producerStallDetectionS": settings.producer_stall_detection_s,
        },
        "serviceId": "polyarb-l1",
        "description": (
            "Polymarket L1 observation daemon — "
            f"Structure 5m + Quote {settings.neg_risk_quote_interval_s}s"
        ),
        "checks": checks,
    }


async def health(request: Request) -> JSONResponse:
    """GET /health — IETF strict三态 health response.

    Returns 200 for pass/warn, 503 for fail. Better Stack外探针 reads this
    endpoint — 503 is the告警 signal. Fly platform probe reads /healthz
    instead (per Phase 02.1 D-05).

    Reads from app.state.sqlite_store and app.state.settings.
    All SQLite reads use mode=ro URI (P3.8: HTTP server never writes).
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store: SQLiteStore = request.app.state.sqlite_store
    settings = request.app.state.settings

    checks, overall = await asyncio.to_thread(
        _build_health_checks,
        store,
        settings,
        time.time(),
        getattr(request.app.state, "quote_worker_runtime", None),
    )
    body = _build_health_body(
        overall,
        checks,
        settings,
        machine_id=request.app.state.machine_id,
        boot_id=request.app.state.boot_id,
    )
    http_status = 503 if overall == "fail" else 200
    return JSONResponse(body, status_code=http_status, media_type=HEALTH_CONTENT_TYPE)


async def healthz(request: Request) -> JSONResponse:
    """GET /healthz — Fly-friendly probe. ALWAYS HTTP 200.

    Same JSON body schema as /health (D-06 full mirror). The underlying check
    status is exposed in body["status"], but the HTTP code is always 200 so
    Fly platform's [http_service.checks] never marks the machine unhealthy.

    Why this matters (BUG-6, Phase 02 Inj 2 + Plan 02.1-02 Inj 4):
    Fly proxy stops routing traffic to a machine whose service check fails.
    If we point Fly at /health (IETF strict) then a PAUSED daemon /
    stale Supabase mirror / failed R2 upload pulls the entire machine out of
    the routing pool — including /control/unpause, blocking recovery.
    /healthz keeps the proxy happy so external administrative endpoints
    stay reachable, while /health continues to deliver true alarm semantics
    to Better Stack.

    D-22 + D-06: public endpoint, no HMAC, same body schema as /health.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store: SQLiteStore = request.app.state.sqlite_store
    settings = request.app.state.settings

    checks, overall = await asyncio.to_thread(
        _build_health_checks,
        store,
        settings,
        time.time(),
        getattr(request.app.state, "quote_worker_runtime", None),
    )
    body = _build_health_body(
        overall,
        checks,
        settings,
        machine_id=request.app.state.machine_id,
        boot_id=request.app.state.boot_id,
    )
    # KEY: ignore overall when deciding HTTP code — always 200.
    return JSONResponse(body, status_code=200, media_type=HEALTH_CONTENT_TYPE)
