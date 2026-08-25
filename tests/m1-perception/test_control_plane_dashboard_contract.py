"""Dashboard contract tests for the self-healing control-plane decoder."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DECODER = ROOT / "dashboard/lib/control-plane.ts"


def _run_node_case(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["node", "--no-warnings", "--input-type=module", "-e", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_control_plane_decoder_exports_strict_operator_types() -> None:
    source = DECODER.read_text()

    for exported in (
        "export type ActiveTask",
        "export type RuntimeIncident",
        "export type RecoveryAction",
        "export type QualificationView",
        "export type ControlPlaneRead",
        "export function decodeControlPlaneRead",
    ):
        assert exported in source

    for literal in (
        '"healthy" | "degraded" | "recovering" | "critical"',
        '"accumulating"',
        '"invalidated"',
        '"qualified"',
        '"pending"',
        '"running"',
        '"succeeded"',
        '"failed"',
        '"stale-noop"',
        '"disabled-action"',
    ):
        assert literal in source

    assert "raw_state: RecoveryActionRawState" in source
    assert 'resultCode === "disabled-action" ? "failed" : null' in source
    assert "CONTROL_PLANE_SAMPLE_LIMIT = 20" in source


def test_control_plane_decoder_rejects_malformed_operator_facts() -> None:
    """Every malformed new operator surface must become a single unavailable result."""
    fixture = {
        "status": "available",
        "job_counts": {"leased": 1},
        "open_incidents": [],
        "runtime_watchdog": {
            "current": None,
            "recent_events": [
                {
                    "incident_key": "runtime-1",
                    "severity": "critical",
                    "summary": "Runtime stalled",
                    "kind": "detected",
                    "occurred_at": "2026-08-25T11:59:20+00:00",
                    "detail": {"failures": ["heartbeat"], "source": "runtime-watchdog"},
                }
            ],
        },
        "soak_evidence": {
            "latest_run_id": "run-1",
            "latest_observed_at": "2026-08-25T11:59:30+00:00",
        },
        "cloud_usage": {
            "budget_day": "2026-08-25",
            "used_bytes": 1024,
            "daily_budget_bytes": 4096,
            "threshold_percent": 25,
            "latest_observation": {
                "observation_id": "usage-1",
                "source": "gamma",
                "operation": "markets",
                "bytes_received": 1024,
                "item_count": 10,
                "artifact_key": "r2://usage",
                "artifact_digest": "sha256:" + "a" * 64,
                "observed_at": "2026-08-25T11:58:00+00:00",
            },
        },
        "runtime_controller": {
            "status": "healthy",
            "controller_id": "m1-runtime-reconciler",
            "owner_id": "runtime-controller",
            "epoch": 4,
            "claimed_at": "2026-08-25T11:59:00+00:00",
            "last_tick_at": "2026-08-25T11:59:30+00:00",
            "lease_expires_at": "2026-08-25T12:00:30+00:00",
            "lease_active": True,
            "lease_age_seconds": 30.0,
            "lease_overdue_seconds": 0.0,
        },
        "active_tasks": {
            "items": [
                {
                    "job_key": "job-1",
                    "attempt_id": "attempt-1",
                    "job_type": "structure-fetch",
                    "worker_id": "worker-1",
                    "lease_epoch": 7,
                    "stage": "fetch-page",
                    "recovery_state": "active",
                    "progress": {"current": 1, "total": 4},
                    "started_at": "2026-08-25T11:58:00+00:00",
                    "last_heartbeat_at": "2026-08-25T11:59:20+00:00",
                    "last_progress_at": "2026-08-25T11:59:10+00:00",
                    "lease_deadline_at": "2026-08-25T12:00:20+00:00",
                    "heartbeat_deadline_at": "2026-08-25T11:59:50+00:00",
                    "progress_deadline_at": "2026-08-25T12:00:10+00:00",
                    "attempt_deadline_at": "2026-08-25T12:02:00+00:00",
                    "heartbeat_age_seconds": 10.0,
                    "progress_age_seconds": 20.0,
                    "lease_overdue_seconds": 0.0,
                    "attempt_overdue_seconds": 0.0,
                }
            ],
            "total": 1,
        },
        "runtime_incidents": {
            "items": [
                {
                    "incident_key": "runtime-1",
                    "component": "runtime",
                    "severity": "critical",
                    "state": "open",
                    "summary": "Runtime stalled",
                    "opened_at": "2026-08-25T11:59:00+00:00",
                    "updated_at": "2026-08-25T11:59:20+00:00",
                    "age_seconds": 60.0,
                    "transition": "recovery-started",
                    "transitions": [
                        {
                            "kind": "recovery-started",
                            "occurred_at": "2026-08-25T11:59:20+00:00",
                            "age_seconds": 40.0,
                            "reason_code": "job.lease-expired",
                            "qualification_impact": "breaking",
                        }
                    ],
                }
            ],
            "total": 1,
        },
        "recovery_actions": {
            "items": [
                {
                    "action_id": "action-1",
                    "incident_key": "runtime-1",
                    "target_type": "worker-process",
                    "target_id": "worker-1",
                    "action_type": "restart-worker-process",
                    "state": "completed",
                    "result_code": "succeeded",
                    "expected_controller_epoch": 4,
                    "expected_attempt_id": "attempt-1",
                    "expected_lease_epoch": 7,
                    "requested_at": "2026-08-25T11:59:10+00:00",
                    "started_at": "2026-08-25T11:59:15+00:00",
                    "finished_at": "2026-08-25T11:59:25+00:00",
                    "next_allowed_at": "2026-08-25T12:09:10+00:00",
                    "worker_id": "worker-1",
                    "worker_epoch": 8,
                    "worker_lease_expires_at": "2026-08-25T12:00:25+00:00",
                },
                {
                    "action_id": "action-2",
                    "incident_key": "runtime-2",
                    "target_type": "job",
                    "target_id": "job-2",
                    "action_type": "retry-job",
                    "state": "completed",
                    "result_code": "disabled-action",
                    "expected_controller_epoch": 4,
                    "expected_attempt_id": "attempt-2",
                    "expected_lease_epoch": 1,
                    "requested_at": "2026-08-25T11:58:10+00:00",
                    "started_at": "2026-08-25T11:58:15+00:00",
                    "finished_at": "2026-08-25T11:58:16+00:00",
                    "next_allowed_at": "2026-08-25T12:08:10+00:00",
                    "worker_id": None,
                    "worker_epoch": 0,
                    "worker_lease_expires_at": None,
                },
            ],
            "total": 2,
        },
        "qualification": {
            "state": "accumulating",
            "epoch_id": "qualification-api",
            "started_at": "2026-08-25T00:00:00+00:00",
            "eligible_seconds": 3600,
            "required_seconds": 86400,
            "max_gap_seconds": 900,
            "last_fact_at": "2026-08-25T11:59:00+00:00",
            "last_fact_age_seconds": 60.0,
            "last_breaker": {
                "observed_at": "2026-08-25T10:00:00+00:00",
                "reason": "runtime-stalled",
                "fact_id": "fact-1",
            },
            "policy_version": "m1-rolling-qualification-v1",
            "release_id": "release-a",
            "config_id": "config-a",
            "role_identity": ["m1", "structure"],
            "certificate": None,
        },
        "pending_alert_outbox": [],
    }
    script = f"""
import assert from "node:assert/strict";
import {{ pathToFileURL }} from "node:url";
const {{ decodeControlPlaneRead }} = await import(pathToFileURL({json.dumps(str(DECODER))}).href);

const fixture = {json.dumps(fixture)};
const clone = (value) => structuredClone(value);
const unavailable = (value) => {{
  assert.deepEqual(
    decodeControlPlaneRead(value),
    {{ status: "unavailable", reason: "control-plane-read-unavailable" }},
  );
}};

const decoded = decodeControlPlaneRead(fixture);
assert.equal(decoded.status, "available");
assert.equal(decoded.active_tasks.items[0].stage, "fetch-page");
assert.equal(decoded.recovery_actions.items[0].state, "succeeded");
assert.equal(decoded.recovery_actions.items[0].raw_state, "completed");
assert.equal(decoded.recovery_actions.items[1].state, "failed");
assert.equal(decoded.recovery_actions.items[1].result_code, "disabled-action");
assert.equal(
  decoded.runtime_incidents.items[0].transitions[0].reason_code,
  "job.lease-expired",
);
assert.equal(
  decoded.runtime_incidents.items[0].transitions[0].qualification_impact,
  "breaking",
);
assert.equal(decoded.qualification.policy_version, "m1-rolling-qualification-v1");

const missingController = clone(fixture);
missingController.runtime_controller = {{
  status: "unavailable",
  reason: "missing-controller",
  controller_id: "m1-runtime-reconciler",
  owner_id: null,
  epoch: null,
  claimed_at: null,
  last_tick_at: null,
  lease_expires_at: null,
  lease_active: false,
  lease_age_seconds: null,
  lease_overdue_seconds: null,
}};
assert.equal(decodeControlPlaneRead(missingController).status, "available");

for (const mutate of [
  (body) => {{ delete body.active_tasks.items[0].stage; }},
  (body) => {{ body.active_tasks.items[0].lease_deadline_at = 123; }},
  (body) => {{
    body.runtime_incidents.items[0].transitions[0].kind = "suppressed";
  }},
  (body) => {{
    body.runtime_watchdog.recent_events[0].detail.failures = Array.from(
      {{ length: 21 }},
      (_, index) => `f-${{index}}`,
    );
  }},
  (body) => {{
    body.runtime_watchdog.recent_events = Array.from(
      {{ length: 21 }},
      () => clone(fixture.runtime_watchdog.recent_events[0]),
    );
  }},
  (body) => {{
    body.runtime_incidents.items[0].transitions = Array.from(
      {{ length: 21 }},
      () => clone(fixture.runtime_incidents.items[0].transitions[0]),
    );
  }},
  (body) => {{
    body.runtime_incidents.items = Array.from(
      {{ length: 21 }},
      () => clone(fixture.runtime_incidents.items[0]),
    );
  }},
  (body) => {{
    body.recovery_actions.items = Array.from(
      {{ length: 21 }},
      () => clone(fixture.recovery_actions.items[0]),
    );
  }},
  (body) => {{
    body.recovery_actions.items[0].state = "completed";
    body.recovery_actions.items[0].result_code = null;
  }},
  (body) => {{
    body.recovery_actions.items[0].state = "completed";
    body.recovery_actions.items[0].result_code = "ignored";
  }},
  (body) => {{
    body.recovery_actions.items[0].state = "pending";
    body.recovery_actions.items[0].result_code = "succeeded";
  }},
  (body) => {{
    body.runtime_watchdog.recent_events[0].severity = "notice";
  }},
  (body) => {{
    body.runtime_incidents.items[0].transitions[0].qualification_impact = "ignored";
  }},
  (body) => {{ body.recovery_actions.items[0].action_type = "shell"; }},
  (body) => {{ delete body.qualification.policy_version; }},
  (body) => {{ body.qualification.role_identity = []; }},
]) {{
  const body = clone(fixture);
  mutate(body);
  unavailable(body);
}}
"""
    result = _run_node_case(script)

    assert result.returncode == 0, result.stderr
