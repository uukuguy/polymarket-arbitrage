from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from polyarb.control_plane.qualification import (
    QualificationFact,
    QualificationState,
    RollingQualificationPolicy,
)
from polyarb.control_plane.qualification_service import (
    FactCursor,
    InMemoryQualificationStore,
    QualificationFactRecord,
    QualificationService,
    StaticQualificationFactSource,
    freshness_row_to_fact_record,
    incident_event_row_to_fact_record,
    recovery_action_row_to_fact_record,
    runtime_event_row_to_fact_record,
)
from polyarb.control_plane.qualification_store import certificate_digest

NOW = datetime(2026, 8, 25, 0, 0, tzinfo=UTC)


def _policy() -> RollingQualificationPolicy:
    return RollingQualificationPolicy(
        release_id="release-a",
        config_id="config-a",
        role_identity=("opportunity", "quote", "structure"),
        required_seconds=86_400,
        max_gap_seconds=3_600,
    )


def _record(
    source: str,
    index: int,
    at: datetime,
    *,
    reason: str = "healthy",
    **kwargs: object,
) -> QualificationFactRecord:
    return QualificationFactRecord(
        cursor=FactCursor(
            observed_at=at, source_rank=index % 10, stable_id=f"{source}:{index:03d}"
        ),
        fact=QualificationFact(
            fact_id=f"{source}:{index:03d}", observed_at=at, reason=reason, **kwargs
        ),
        source=source,
    )


def test_real_schema_rows_map_to_fail_loud_qualification_facts() -> None:
    runtime = runtime_event_row_to_fact_record(
        {
            "event_id": "runtime-1",
            "kind": "job.failed",
            "occurred_at": NOW,
            "job_key": "quote:one:admit",
            "attempt_id": "attempt-a",
            "lease_epoch": 4,
            "event_sequence": 7,
            "progress_current": 11,
            "detail": {"reason_code": "lease.expired"},
        }
    )
    assert runtime.cursor == FactCursor(NOW, 10, "runtime-1")
    assert runtime.fact.reason == "lease.expired"
    assert runtime.fact.progress_count == 11

    incident = incident_event_row_to_fact_record(
        {
            "incident_event_id": "incident-1",
            "incident_key": "incident:p1",
            "kind": "detected",
            "severity": "critical",
            "state": "open",
            "occurred_at": NOW + timedelta(seconds=1),
            "detail": {"qualification_breaking": True, "reason_code": "incident.p1"},
        }
    )
    assert incident.cursor == FactCursor(NOW + timedelta(seconds=1), 20, "incident-1")
    assert incident.fact.reason == "incident.p1-slo"

    action = recovery_action_row_to_fact_record(
        {
            "action_id": "action-1",
            "action_type": "retry-job",
            "target_id": "quote:one:admit",
            "state": "completed",
            "result_code": "succeeded",
            "requested_at": NOW + timedelta(seconds=10),
            "finished_at": NOW + timedelta(seconds=35),
            "detail": {"reason_code": "upstream.timeout", "recovery_slo_seconds": 60},
        }
    )
    assert action.cursor == FactCursor(NOW + timedelta(seconds=35), 30, "action-1")
    assert action.fact.reason == "recovery.retry"
    assert action.fact.recovery_duration_seconds == 25
    assert action.fact.recovery_slo_seconds == 60
    assert action.fact.signature == "upstream.timeout"

    freshness = freshness_row_to_fact_record(
        {
            "fact_id": "freshness:quote:1",
            "data_product": "quote",
            "observed_at": NOW + timedelta(seconds=20),
            "freshness_seconds": 119,
            "freshness_slo_seconds": 120,
            "progress_count": 20,
            "successful_count": 20,
        }
    )
    assert freshness.cursor == FactCursor(NOW + timedelta(seconds=20), 40, "freshness:quote:1")
    assert freshness.fact.reason == "healthy"
    assert freshness.fact.freshness_product == "quote"

    with pytest.raises(ValueError, match="unknown runtime event kind"):
        runtime_event_row_to_fact_record(
            {
                "event_id": "runtime-bad",
                "kind": "job.unclassified",
                "occurred_at": NOW,
                "job_key": "job-a",
                "attempt_id": "attempt-a",
                "lease_epoch": 1,
                "event_sequence": 1,
                "detail": {},
            }
        )

    with pytest.raises(ValueError, match="freshness row is malformed"):
        freshness_row_to_fact_record(
            {
                "fact_id": "freshness:bad",
                "data_product": "quote",
                "observed_at": NOW,
                "freshness_seconds": "not-an-int",
                "freshness_slo_seconds": 120,
            }
        )


def test_virtual_26h_recovery_replay_seals_one_reproducible_certificate() -> None:
    first_start = NOW
    recovered = NOW + timedelta(hours=2)
    records = [
        _record("freshness", 0, first_start, progress_count=1, successful_count=1),
        _record(
            "recovery",
            1,
            first_start + timedelta(hours=1),
            reason="recovery.retry",
            signature="upstream.timeout",
            recovery_duration_seconds=20,
            recovery_slo_seconds=60,
            progress_count=2,
            successful_count=1,
        ),
        _record(
            "runtime",
            2,
            first_start + timedelta(hours=2),
            reason="lease.expired",
            progress_count=3,
            successful_count=1,
        ),
        _record(
            "recovery",
            3,
            recovered + timedelta(seconds=1),
            reason="recovery.confirmed",
            recovery_confirmed=True,
            progress_count=4,
            successful_count=1,
        ),
    ]
    for hour in range(1, 25):
        records.append(
            _record(
                "freshness",
                10 + hour,
                recovered + timedelta(seconds=1, hours=hour),
                progress_count=10 + hour,
                successful_count=10 + hour,
            )
        )

    def run_once() -> InMemoryQualificationStore:
        store = InMemoryQualificationStore()
        service = QualificationService(
            policy=_policy(),
            fact_source=StaticQualificationFactSource(records),
            state_store=store,
            writer_id="test-service",
            batch_size=5,
        )
        for tick in range(8):
            service.tick(NOW + timedelta(hours=tick * 4))
        return store

    left = run_once()
    right = run_once()

    assert len(left.epochs) == 3
    invalidated = left.epochs[0]
    assert invalidated.state is QualificationState.INVALIDATED
    assert invalidated.invalidated_at == NOW + timedelta(hours=2)
    assert invalidated.invalidation_reason == "lease.expired"
    assert left.epochs[1].state is QualificationState.RECOVERING
    assert left.epochs[2].state is QualificationState.QUALIFIED
    assert len(left.certificates) == 1
    assert left.certificates[0]["payload"]["identity"]["epoch_id"] == left.epochs[2].epoch_id
    assert left.certificates[0]["digest"] == certificate_digest(left.certificates[0]["payload"])
    assert left.certificates[0]["digest"] == right.certificates[0]["digest"]


def test_tick_cursor_is_total_ordered_and_crash_replay_is_exact() -> None:
    at = NOW + timedelta(minutes=1)
    records = (
        _record("incident", 3, at, reason="healthy"),
        _record("runtime", 1, at, reason="healthy"),
        _record("recovery", 2, at, reason="healthy"),
        _record("freshness", 4, at + timedelta(minutes=1), reason="healthy"),
    )
    store = InMemoryQualificationStore(fail_before_commit_once=True)
    service = QualificationService(
        policy=_policy(),
        fact_source=StaticQualificationFactSource(records),
        state_store=store,
        writer_id="test-service",
        batch_size=3,
    )

    with pytest.raises(RuntimeError, match="injected before commit"):
        service.tick(at)
    assert store.cursor is None
    assert store.applied_cursors == ()

    service.tick(at)
    assert store.applied_cursors == (
        FactCursor(at, 1, "runtime:001"),
        FactCursor(at, 2, "recovery:002"),
        FactCursor(at, 3, "incident:003"),
    )
    assert store.cursor == FactCursor(at, 3, "incident:003")

    store.fail_after_commit_once = True
    with pytest.raises(RuntimeError, match="injected after commit"):
        service.tick(at + timedelta(minutes=1))
    assert store.cursor == FactCursor(at + timedelta(minutes=1), 4, "freshness:004")
    service.tick(at + timedelta(minutes=1))
    assert store.applied_cursors == (
        FactCursor(at, 1, "runtime:001"),
        FactCursor(at, 2, "recovery:002"),
        FactCursor(at, 3, "incident:003"),
        FactCursor(at + timedelta(minutes=1), 4, "freshness:004"),
    )


def test_no_events_require_explicit_freshness_observation_before_qualifying() -> None:
    store = InMemoryQualificationStore()
    service = QualificationService(
        policy=_policy(),
        fact_source=StaticQualificationFactSource(()),
        state_store=store,
        writer_id="test-service",
    )

    result = service.tick(NOW + timedelta(hours=26))

    assert result.applied == 0
    assert store.current.state is QualificationState.ACCUMULATING
    assert not store.certificates


def test_qualified_without_certificate_is_sealed_on_next_tick() -> None:
    records = tuple(
        _record(
            "freshness",
            hour,
            NOW + timedelta(hours=hour),
            progress_count=hour + 1,
            successful_count=hour + 1,
        )
        for hour in range(25)
    )
    store = InMemoryQualificationStore()
    service = QualificationService(
        policy=_policy(),
        fact_source=StaticQualificationFactSource(records),
        state_store=store,
        writer_id="test-service",
        batch_size=25,
    )
    service.tick(NOW)
    first = service.tick(NOW + timedelta(hours=24))
    assert first.state is QualificationState.QUALIFIED
    store.certificates.clear()

    sealed = service.tick(NOW + timedelta(hours=25))

    assert sealed.applied == 0
    assert sealed.certificate_digest == store.certificates[0]["digest"]
