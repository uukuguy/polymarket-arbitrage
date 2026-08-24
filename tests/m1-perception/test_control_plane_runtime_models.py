from __future__ import annotations

import json
from collections import UserDict
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from polyarb.control_plane.runtime_models import (
    RuntimeDeadlineProfile,
    RuntimeEvent,
    RuntimeEventKind,
    RuntimeProgress,
)


def _valid_runtime_event_kwargs(**overrides: Any) -> dict[str, Any]:
    valid: dict[str, Any] = {
        "job_key": "job-1",
        "attempt_id": "attempt-1",
        "lease_epoch": 1,
        "worker_id": "worker-1",
        "event_sequence": 1,
        "kind": RuntimeEventKind.STARTED,
        "stage": "claim",
        "progress": None,
        "detail": {},
        "occurred_at": datetime(2026, 8, 24, 1, 2, 3, tzinfo=UTC),
        "idempotency_key": "attempt-1:1",
    }
    valid.update(overrides)
    return valid


def test_deadline_profile_separates_liveness_progress_and_attempt() -> None:
    profile = RuntimeDeadlineProfile(
        policy_version="runtime-v1",
        lease_seconds=120,
        heartbeat_seconds=30,
        progress_seconds=90,
        attempt_seconds=300,
    )

    assert profile.missed_heartbeat_incident_seconds == 90


def test_deadline_profile_rejects_invalid_policy_bounds() -> None:
    with pytest.raises(ValueError, match="positive"):
        RuntimeDeadlineProfile(
            policy_version="",
            lease_seconds=120,
            heartbeat_seconds=30,
            progress_seconds=90,
            attempt_seconds=300,
        )

    with pytest.raises(ValueError, match="three times per lease"):
        RuntimeDeadlineProfile(
            policy_version="runtime-v1",
            lease_seconds=60,
            heartbeat_seconds=30,
            progress_seconds=90,
            attempt_seconds=300,
        )

    with pytest.raises(ValueError, match="progress deadline cannot exceed attempt"):
        RuntimeDeadlineProfile(
            policy_version="runtime-v1",
            lease_seconds=120,
            heartbeat_seconds=30,
            progress_seconds=301,
            attempt_seconds=300,
        )


def test_runtime_progress_is_monotonic_and_bounded() -> None:
    assert RuntimeProgress(sequence=2, current=10, total=20, stage="upload").current == 10

    with pytest.raises(ValueError, match="current cannot exceed total"):
        RuntimeProgress(sequence=2, current=21, total=20, stage="upload")


def test_runtime_progress_rejects_negative_values_and_empty_stage() -> None:
    with pytest.raises(ValueError, match="runtime progress values are invalid"):
        RuntimeProgress(sequence=-1, current=0, total=10, stage="upload")

    with pytest.raises(ValueError, match="runtime progress values are invalid"):
        RuntimeProgress(sequence=1, current=0, total=10, stage="")

    with pytest.raises(ValueError, match="current cannot exceed total"):
        RuntimeProgress(sequence=1, current=0, total=-1, stage="upload")


def test_runtime_event_requires_bounded_timezone_aware_identity() -> None:
    progress = RuntimeProgress(sequence=1, current=1, total=3, stage="upload")
    event = RuntimeEvent(
        job_key="job-1",
        attempt_id="attempt-1",
        lease_epoch=1,
        worker_id="worker-1",
        event_sequence=1,
        kind=RuntimeEventKind.STAGE_CHANGED,
        stage="upload",
        progress=progress,
        detail={"component": "structure-fetch", "data_product": "market-snapshot"},
        occurred_at=datetime(2026, 8, 24, 1, 2, 3, tzinfo=UTC),
        idempotency_key="attempt-1:1",
    )

    assert event.kind.value == "job.stage-changed"
    assert event.progress == progress
    assert json.dumps(event.detail, sort_keys=True)


def test_runtime_event_rejects_invalid_sequences_naive_time_and_large_detail() -> None:
    with pytest.raises(ValueError, match="identities"):
        RuntimeEvent(**_valid_runtime_event_kwargs(job_key=""))

    with pytest.raises(ValueError, match="sequences"):
        RuntimeEvent(**_valid_runtime_event_kwargs(lease_epoch=0))

    with pytest.raises(ValueError, match="timezone-aware"):
        RuntimeEvent(
            **_valid_runtime_event_kwargs(occurred_at=datetime(2026, 8, 24, 1, 2, 3))
        )

    with pytest.raises(ValueError, match="detail is not bounded"):
        RuntimeEvent(
            **_valid_runtime_event_kwargs(detail={str(index): index for index in range(21)})
        )


def test_runtime_event_requires_exact_runtime_model_types() -> None:
    class ProgressSubclass(RuntimeProgress):
        pass

    with pytest.raises(TypeError, match="kind must be RuntimeEventKind"):
        RuntimeEvent(**_valid_runtime_event_kwargs(kind=RuntimeEventKind.STARTED.value))

    with pytest.raises(TypeError, match="progress must be RuntimeProgress or None"):
        RuntimeEvent(
            **_valid_runtime_event_kwargs(
                progress=ProgressSubclass(sequence=1, current=0, total=1, stage="claim")
            )
        )

    with pytest.raises(TypeError, match="detail root must be a dict"):
        RuntimeEvent(**_valid_runtime_event_kwargs(detail=UserDict({"sample": 1})))


@pytest.mark.parametrize(
    ("kind", "detail"),
    [
        (RuntimeEventKind.STARTED, {"job_type": "structure-fetch", "component": "control-plane"}),
        (
            RuntimeEventKind.STAGE_CHANGED,
            {
                "component": "structure-fetch",
                "data_product": "market-snapshot",
                "reason_code": "checkpoint.advance",
            },
        ),
        (
            RuntimeEventKind.LEASE_AT_RISK,
            {
                "component": "quote-batch",
                "deadline_kind": "lease",
                "deadline_at": "2026-08-24T01:04:03+00:00",
                "freshness_seconds": 42,
                "recovery_policy": "retry-soon",
                "qualification_impact": "none",
            },
        ),
        (
            RuntimeEventKind.PROGRESS_STALLED,
            {
                "component": "structure-materialize",
                "failure_signature": "progress.stalled",
                "data_product": "structure-sync",
                "freshness_seconds": 90,
                "deadline_kind": "progress",
                "deadline_at": "2026-08-24T01:04:03+00:00",
                "qualification_impact": "delayed",
                "recovery_policy": "retry-same-input",
            },
        ),
        (
            RuntimeEventKind.RETRYABLE_FAILED,
            {
                "component": "quote-batch",
                "failure_signature": "upstream.timeout",
                "reason_code": "timeout",
                "retry_count": 2,
                "recovery_policy": "exponential-backoff",
                "qualification_impact": "none",
            },
        ),
        (
            RuntimeEventKind.RETRY_SCHEDULED,
            {
                "reason_code": "timeout",
                "retry_count": 3,
                "backoff_seconds": 30,
                "next_decision_at": "2026-08-24T01:04:33+00:00",
                "recovery_policy": "exponential-backoff",
            },
        ),
        (
            RuntimeEventKind.RECOVERY_STARTED,
            {
                "component": "quote-batch",
                "recovery_policy": "exponential-backoff",
                "retry_count": 3,
                "reason_code": "timeout",
            },
        ),
        (
            RuntimeEventKind.RECOVERED,
            {
                "component": "quote-batch",
                "result_code": "ok",
                "retry_count": 3,
                "qualification_impact": "restored",
            },
        ),
        (
            RuntimeEventKind.TERMINAL_FAILED,
            {
                "component": "structure-certify",
                "failure_signature": "validation.failed",
                "reason_code": "invalid-input",
                "result_code": "failed",
                "qualification_impact": "blocked",
            },
        ),
        (
            RuntimeEventKind.SUCCEEDED,
            {
                "component": "structure-certify",
                "data_product": "structure-sync",
                "result_code": "ok",
                "freshness_seconds": 12,
                "qualification_impact": "qualified",
            },
        ),
    ],
)
def test_runtime_event_detail_allowlist_accepts_declared_facts_for_each_kind(
    kind: RuntimeEventKind,
    detail: dict[str, object],
) -> None:
    event = RuntimeEvent(**_valid_runtime_event_kwargs(kind=kind, detail=detail))

    assert dict(event.detail) == detail
    assert json.loads(json.dumps(event.detail, sort_keys=True)) == detail


def test_runtime_event_rejects_unknown_detail_keys_even_when_value_looks_safe() -> None:
    with pytest.raises(ValueError, match="detail keys are not allowed"):
        RuntimeEvent(**_valid_runtime_event_kwargs(detail={"safe": "token=abc"}))


@pytest.mark.parametrize(
    "detail",
    [
        {"job_type": "quote-batch", "headers": "x-safe"},
        {"job_type": "quote-batch", "response_body": "redacted"},
        {"job_type": "quote-batch", "dsn": "postgresql://example"},
    ],
)
def test_runtime_event_rejects_undeclared_transport_or_secret_detail_keys(
    detail: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="detail keys are not allowed"):
        RuntimeEvent(**_valid_runtime_event_kwargs(detail=detail))


@pytest.mark.parametrize(
    "detail",
    [
        {"job_type": "quote-batch", "component": {"name": "worker"}},
        {"job_type": "quote-batch", "component": ["worker"]},
        {"job_type": "quote-batch", "component": ("worker",)},
    ],
)
def test_runtime_event_rejects_nested_or_sequence_detail_values(
    detail: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="flat JSON scalars"):
        RuntimeEvent(**_valid_runtime_event_kwargs(detail=detail))


@pytest.mark.parametrize(
    ("kind", "detail"),
    [
        (RuntimeEventKind.STARTED, {"job_type": "quote=batch"}),
        (RuntimeEventKind.STARTED, {"job_type": "quote batch"}),
        (RuntimeEventKind.STARTED, {"job_type": "/quote-batch"}),
        (RuntimeEventKind.STARTED, {"job_type": "Bearer\tabc"}),
        (RuntimeEventKind.LEASE_AT_RISK, {"deadline_at": "2026-08-24T01:04:03"}),
        (RuntimeEventKind.RETRY_SCHEDULED, {"retry_count": -1}),
        (RuntimeEventKind.LEASE_AT_RISK, {"freshness_seconds": -0.1}),
        (RuntimeEventKind.LEASE_AT_RISK, {"freshness_seconds": float("nan")}),
    ],
)
def test_runtime_event_rejects_invalid_declared_detail_values(
    kind: RuntimeEventKind,
    detail: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="detail value is invalid"):
        RuntimeEvent(**_valid_runtime_event_kwargs(kind=kind, detail=detail))


@pytest.mark.parametrize(
    ("kind", "key", "value"),
    [
        (
            RuntimeEventKind.RETRYABLE_FAILED,
            "failure_signature",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJVaDQssw5c",
        ),
        (RuntimeEventKind.RETRYABLE_FAILED, "reason_code", "BearerABCDEF0123456789"),
        (RuntimeEventKind.RETRYABLE_FAILED, "reason_code", "BasicABCDEF0123456789"),
        (RuntimeEventKind.RETRYABLE_FAILED, "reason_code", "bearer.ABCDEF0123456789"),
        (
            RuntimeEventKind.RETRYABLE_FAILED,
            "failure_signature",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0",
        ),
        (RuntimeEventKind.STARTED, "component", "Authorization:BearerABCDEF0123456789"),
        (RuntimeEventKind.STARTED, "component", "Authorization:BasicABCDEF0123456789"),
        (RuntimeEventKind.STARTED, "job_type", "POLYMARKET_API_KEY:ABCDEF0123456789"),
        (RuntimeEventKind.STARTED, "recovery_policy", "sk-live-ABCDEF0123456789"),
        (RuntimeEventKind.STARTED, "recovery_policy", "sk-proj-ABCDEF0123456789abcdef"),
        (RuntimeEventKind.STARTED, "recovery_policy", "sk-ABCDEF0123456789abcdef"),
        (RuntimeEventKind.STAGE_CHANGED, "result_code", "xoxb-ABCDEF0123456789"),
        (RuntimeEventKind.STAGE_CHANGED, "data_product", "ghp-ABCDEF0123456789"),
        (RuntimeEventKind.STAGE_CHANGED, "data_product", "ghp_ABCDEF0123456789abcdef"),
        (
            RuntimeEventKind.STAGE_CHANGED,
            "data_product",
            "github_pat_11AA22BB33CC44DD55EE66FF77GG88HH",
        ),
        (RuntimeEventKind.STAGE_CHANGED, "result_code", "glpat-ABCDEF0123456789"),
        (RuntimeEventKind.STAGE_CHANGED, "result_code", "AKIAIOSFODNN7EXAMPLE"),
        (RuntimeEventKind.STARTED, "component", "postgres:db.example.com:5432:polyarb"),
        (RuntimeEventKind.STAGE_CHANGED, "component", "https:api.example.com:v1"),
        (RuntimeEventKind.RETRYABLE_FAILED, "reason_code", "token:ABCDEF0123456789"),
        (
            RuntimeEventKind.RETRYABLE_FAILED,
            "reason_code",
            "A1B2C3D4E5F6G7H8I9J0K1L2M3N4P5Q6",
        ),
    ],
)
def test_runtime_event_code_details_reject_credential_shaped_values(
    kind: RuntimeEventKind,
    key: str,
    value: str,
) -> None:
    with pytest.raises(ValueError, match="detail value is invalid"):
        RuntimeEvent(**_valid_runtime_event_kwargs(kind=kind, detail={key: value}))


def test_runtime_event_code_detail_rejection_does_not_echo_secret_value() -> None:
    secret_value = "github_pat_11AA22BB33CC44DD55EE66FF77GG88HH"

    with pytest.raises(ValueError) as exc_info:
        RuntimeEvent(
            **_valid_runtime_event_kwargs(
                kind=RuntimeEventKind.STAGE_CHANGED,
                detail={"data_product": secret_value},
            )
        )

    assert "data_product" in str(exc_info.value)
    assert secret_value not in str(exc_info.value)


@pytest.mark.parametrize(
    ("kind", "key", "value"),
    [
        (RuntimeEventKind.STARTED, "component", "quote-batch-worker"),
        (RuntimeEventKind.STARTED, "job_type", "future-worker"),
        (RuntimeEventKind.STAGE_CHANGED, "data_product", "l2-top-of-book"),
        (RuntimeEventKind.LEASE_AT_RISK, "deadline_kind", "deadline"),
        (RuntimeEventKind.RETRYABLE_FAILED, "failure_signature", "upstream.retryable"),
        (RuntimeEventKind.RETRYABLE_FAILED, "qualification_impact", "degraded"),
        (RuntimeEventKind.RETRYABLE_FAILED, "reason_code", "new.reason"),
        (RuntimeEventKind.RETRYABLE_FAILED, "recovery_policy", "retry-later"),
        (RuntimeEventKind.STAGE_CHANGED, "result_code", "success"),
    ],
)
def test_runtime_event_code_details_reject_unregistered_taxonomy_values(
    kind: RuntimeEventKind,
    key: str,
    value: str,
) -> None:
    with pytest.raises(ValueError, match="detail value is invalid"):
        RuntimeEvent(**_valid_runtime_event_kwargs(kind=kind, detail={key: value}))


@pytest.mark.parametrize(
    ("kind", "key", "value"),
    [
        (RuntimeEventKind.STARTED, "component", "control-plane"),
        (RuntimeEventKind.STARTED, "component", "structure-fetch"),
        (RuntimeEventKind.STARTED, "component", "quote-batch"),
        (RuntimeEventKind.STARTED, "component", "structure-materialize"),
        (RuntimeEventKind.STARTED, "component", "structure-certify"),
        (RuntimeEventKind.STARTED, "data_product", "market-snapshot"),
        (RuntimeEventKind.STARTED, "data_product", "structure-sync"),
        (RuntimeEventKind.LEASE_AT_RISK, "deadline_kind", "lease"),
        (RuntimeEventKind.LEASE_AT_RISK, "deadline_kind", "heartbeat"),
        (RuntimeEventKind.LEASE_AT_RISK, "deadline_kind", "progress"),
        (RuntimeEventKind.LEASE_AT_RISK, "deadline_kind", "attempt"),
        (RuntimeEventKind.RETRYABLE_FAILED, "failure_signature", "progress.stalled"),
        (RuntimeEventKind.RETRYABLE_FAILED, "failure_signature", "upstream.timeout"),
        (RuntimeEventKind.RETRYABLE_FAILED, "failure_signature", "validation.failed"),
        (RuntimeEventKind.STARTED, "job_type", "opportunity-certify"),
        (RuntimeEventKind.STARTED, "job_type", "quote-admit"),
        (RuntimeEventKind.STARTED, "job_type", "quote-batch"),
        (RuntimeEventKind.STARTED, "job_type", "quote-certify"),
        (RuntimeEventKind.STARTED, "job_type", "quote-scan"),
        (RuntimeEventKind.STARTED, "job_type", "structure-certify"),
        (RuntimeEventKind.STARTED, "job_type", "structure-fetch"),
        (RuntimeEventKind.STARTED, "job_type", "structure-materialize"),
        (RuntimeEventKind.STARTED, "job_type", "structure-normalize"),
        (RuntimeEventKind.RETRYABLE_FAILED, "qualification_impact", "none"),
        (RuntimeEventKind.RETRYABLE_FAILED, "qualification_impact", "delayed"),
        (RuntimeEventKind.RETRYABLE_FAILED, "qualification_impact", "restored"),
        (RuntimeEventKind.RETRYABLE_FAILED, "qualification_impact", "blocked"),
        (RuntimeEventKind.RETRYABLE_FAILED, "qualification_impact", "qualified"),
        (RuntimeEventKind.RETRYABLE_FAILED, "qualification_impact", "invalidated"),
        (RuntimeEventKind.RETRYABLE_FAILED, "reason_code", "checkpoint.advance"),
        (RuntimeEventKind.RETRYABLE_FAILED, "reason_code", "timeout"),
        (RuntimeEventKind.RETRYABLE_FAILED, "reason_code", "invalid-input"),
        (RuntimeEventKind.STARTED, "recovery_policy", "retry-job"),
        (RuntimeEventKind.STARTED, "recovery_policy", "retry-soon"),
        (RuntimeEventKind.STARTED, "recovery_policy", "retry-same-input"),
        (RuntimeEventKind.STARTED, "recovery_policy", "exponential-backoff"),
        (RuntimeEventKind.STAGE_CHANGED, "result_code", "ok"),
        (RuntimeEventKind.STAGE_CHANGED, "result_code", "failed"),
    ],
)
def test_runtime_event_code_details_accept_registered_taxonomy_values(
    kind: RuntimeEventKind,
    key: str,
    value: str,
) -> None:
    event = RuntimeEvent(**_valid_runtime_event_kwargs(kind=kind, detail={key: value}))

    assert event.detail[key] == value


@pytest.mark.parametrize(
    ("kind", "detail"),
    [
        (
            RuntimeEventKind.STARTED,
            {
                "component": "control-plane",
                "job_type": "structure-fetch",
                "recovery_policy": "retry-job",
            },
        ),
        (
            RuntimeEventKind.STAGE_CHANGED,
            {
                "component": "structure-fetch",
                "data_product": "market-snapshot",
                "reason_code": "checkpoint.advance",
                "result_code": "ok",
            },
        ),
        (
            RuntimeEventKind.RETRYABLE_FAILED,
            {
                "component": "quote-batch",
                "failure_signature": "upstream.timeout",
                "reason_code": "timeout",
                "recovery_policy": "exponential-backoff",
                "qualification_impact": "none",
            },
        ),
        (
            RuntimeEventKind.PROGRESS_STALLED,
            {
                "component": "structure-materialize",
                "failure_signature": "progress.stalled",
                "data_product": "structure-sync",
                "deadline_kind": "progress",
                "qualification_impact": "delayed",
                "recovery_policy": "retry-same-input",
            },
        ),
    ],
)
def test_runtime_event_code_details_accept_closed_taxonomy_style_codes(
    kind: RuntimeEventKind,
    detail: dict[str, object],
) -> None:
    event = RuntimeEvent(**_valid_runtime_event_kwargs(kind=kind, detail=detail))

    assert dict(event.detail) == detail


def test_runtime_event_rejects_oversize_flat_payload() -> None:
    with pytest.raises(ValueError, match="detail value is invalid"):
        RuntimeEvent(
            **_valid_runtime_event_kwargs(
                kind=RuntimeEventKind.RETRYABLE_FAILED,
                detail={"failure_signature": "x" * 1_000_000},
            )
        )


def test_runtime_event_defensively_freezes_detail_after_creation() -> None:
    source_detail: dict[str, object] = {
        "job_type": "quote-batch",
        "component": "quote-batch",
    }

    event = RuntimeEvent(**_valid_runtime_event_kwargs(detail=source_detail))
    source_detail["job_type"] = "quote-certify"

    assert event.detail["job_type"] == "quote-batch"

    with pytest.raises(TypeError):
        cast(dict[str, Any], event.detail)["job_type"] = "quote-certify"
