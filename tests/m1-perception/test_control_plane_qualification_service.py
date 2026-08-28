from __future__ import annotations

import asyncio
import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from types import SimpleNamespace
from typing import Any, cast

import pytest

import polyarb.control_plane.qualification_service as qualification_service_module
from polyarb.control_plane.qualification import (
    QualificationFact,
    QualificationState,
    RollingQualificationPolicy,
)
from polyarb.control_plane.qualification_service import (
    FactCursor,
    InMemoryQualificationStore,
    PostgresQualificationFactSource,
    PostgresQualificationServiceStore,
    QualificationFactRecord,
    QualificationService,
    QualificationServiceStopRequested,
    StaticQualificationFactSource,
    freshness_row_to_fact_record,
    incident_event_row_to_fact_record,
    ledger_row_to_fact_record,
    recovery_action_row_to_fact_record,
    run_qualification_service,
    runtime_event_row_to_fact_record,
)
from polyarb.control_plane.qualification_store import certificate_digest

NOW = datetime(2026, 8, 25, 0, 0, tzinfo=UTC)


def test_qualification_service_stop_detaches_a_stalled_tick_after_database_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    release = threading.Event()
    stop = asyncio.Event()

    class Policy:
        stop_grace_seconds = 0.05

    class Service:
        def tick(self, _now: datetime):
            started.set()
            release.wait()
            raise AssertionError("detached tick result must not re-enter the stopped service")

    monkeypatch.setattr(qualification_service_module, "CONTROL_PLANE_DB_POLICY", Policy())

    async def run() -> dict[str, object]:
        task = asyncio.create_task(
            run_qualification_service(
                cast(Any, Service()),
                interval_seconds=30,
                stop_event=stop,
            )
        )
        while not started.is_set():
            await asyncio.sleep(0)
        stop.set()
        return await task

    safety_release = threading.Timer(0.4, release.set)
    safety_release.start()
    before = monotonic()
    try:
        assert asyncio.run(run()) == {"status": "stopped", "ticks": 0}
        assert monotonic() - before < 0.2
    finally:
        release.set()
        safety_release.cancel()


def test_qualification_service_requests_cooperative_stop_before_grace_detach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    stop_requested = threading.Event()
    stop = asyncio.Event()

    class Policy:
        stop_grace_seconds = 0.2

    class Service:
        def request_stop(self) -> None:
            stop_requested.set()

        def tick(self, _now: datetime):
            started.set()
            assert stop_requested.wait(timeout=1)
            raise RuntimeError("cooperative qualification stop")

    monkeypatch.setattr(qualification_service_module, "CONTROL_PLANE_DB_POLICY", Policy())

    async def run() -> dict[str, object]:
        task = asyncio.create_task(
            run_qualification_service(
                cast(Any, Service()),
                interval_seconds=30,
                stop_event=stop,
            )
        )
        while not started.is_set():
            await asyncio.sleep(0)
        stop.set()
        return await task

    assert asyncio.run(run()) == {"status": "stopped", "ticks": 0}
    assert stop_requested.is_set()


def test_qualification_store_stop_forbids_starting_certificate_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = PostgresQualificationServiceStore(lambda: cast(Any, None))
    store.request_stop()
    certificate_started = False

    def reject_certificate_io(*_args: object, **_kwargs: object) -> None:
        nonlocal certificate_started
        certificate_started = True

    monkeypatch.setattr(
        qualification_service_module,
        "insert_qualification_certificate",
        reject_certificate_io,
    )

    with pytest.raises(QualificationServiceStopRequested):
        store.ensure_certificate(cast(Any, SimpleNamespace(state=QualificationState.QUALIFIED)))

    assert certificate_started is False


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
    **kwargs: Any,
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
            "kind": "job.terminal-failed",
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


def test_legitimate_incident_kinds_map_to_closed_qualification_reasons() -> None:
    base = {
        "incident_event_id": "incident-1",
        "incident_key": "incident:retry",
        "severity": "warning",
        "state": "open",
        "occurred_at": NOW,
        "detail": {},
    }

    attempt = incident_event_row_to_fact_record({**base, "kind": "attempt-failed"})
    assert attempt.fact.reason == "recovery.started"

    recovery_started = incident_event_row_to_fact_record(
        {**base, "incident_event_id": "incident-2", "kind": "recovery-started"}
    )
    assert recovery_started.fact.reason == "recovery.started"

    for index, kind in enumerate(("circuit-opened", "circuit-probe-failed", "escalated")):
        record = incident_event_row_to_fact_record(
            {**base, "incident_event_id": f"incident-break-{index}", "kind": kind}
        )
        assert record.fact.reason == "incident.p1-slo"

    critical = incident_event_row_to_fact_record(
        {
            **base,
            "incident_event_id": "incident-critical",
            "kind": "detected",
            "severity": "critical",
            "detail": {"qualification_breaking": True, "reason_code": "incident.p1"},
        }
    )
    assert critical.fact.reason == "incident.p1-slo"

    for index, kind in enumerate(("recovered", "resolved")):
        record = incident_event_row_to_fact_record(
            {
                **base,
                "incident_event_id": f"incident-recovered-{index}",
                "kind": kind,
                "state": "resolved",
            }
        )
        assert record.fact.reason == "recovery.confirmed"
        assert record.fact.recovery_confirmed is True

    with pytest.raises(ValueError, match="unknown incident event kind"):
        incident_event_row_to_fact_record({**base, "kind": "not-a-real-transition"})


@pytest.mark.parametrize("severity", ["info", "warning", "critical"])
def test_incident_mapper_accepts_schema_severity_enum(severity: str) -> None:
    record = incident_event_row_to_fact_record(
        {
            "incident_event_id": "incident-" + severity,
            "incident_key": "incident:severity",
            "kind": "detected",
            "severity": severity,
            "state": "open",
            "occurred_at": NOW,
            "detail": {},
        }
    )

    assert record.fact.reason == ("incident.p1-slo" if severity == "critical" else "healthy")


@pytest.mark.parametrize("state", ["open", "acknowledged", "resolved"])
def test_incident_mapper_accepts_schema_state_enum(state: str) -> None:
    record = incident_event_row_to_fact_record(
        {
            "incident_event_id": "incident-" + state,
            "incident_key": "incident:state",
            "kind": "detected",
            "severity": "warning",
            "state": state,
            "occurred_at": NOW,
            "detail": {},
        }
    )

    assert record.fact.reason == ("recovery.confirmed" if state == "resolved" else "healthy")


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("severity", "urgent", "unknown incident severity"),
        ("state", "stale", "unknown incident state"),
    ],
)
def test_incident_mapper_rejects_unknown_schema_enums(
    field: str,
    value: str,
    match: str,
) -> None:
    row = {
        "incident_event_id": "incident-bad-" + field,
        "incident_key": "incident:bad",
        "kind": "detected",
        "severity": "warning",
        "state": "open",
        "occurred_at": NOW,
        "detail": {},
    }
    row[field] = value

    with pytest.raises(ValueError, match=match):
        incident_event_row_to_fact_record(row)


def test_successful_recovery_action_types_map_to_closed_qualification_reasons() -> None:
    expected_reasons = {
        "heartbeat-job": "recovery.heartbeat",
        "cancel-job": "recovery.started",
        "retry-job": "recovery.retry",
        "reclaim-job": "recovery.reclaim",
        "probe-circuit": "recovery.circuit-probe",
        "restart-worker-process": "recovery.process-replacement",
        "restart-machine": "recovery.machine-replacement",
    }

    for index, (action_type, expected_reason) in enumerate(expected_reasons.items()):
        record = recovery_action_row_to_fact_record(
            {
                "action_id": f"action-{index}",
                "action_type": action_type,
                "target_id": "job-a",
                "state": "completed",
                "result_code": "succeeded",
                "requested_at": NOW,
                "finished_at": NOW + timedelta(seconds=20),
                "detail": {"reason_code": action_type, "recovery_slo_seconds": 60},
            }
        )
        assert record.fact.reason == expected_reason
        if expected_reason == "recovery.started":
            assert record.fact.recovery_duration_seconds is None
            assert record.fact.recovery_slo_seconds is None
            assert record.fact.signature is None
        else:
            assert record.fact.recovery_duration_seconds == 20
            assert record.fact.recovery_slo_seconds == 60
            assert record.fact.signature == action_type

    with pytest.raises(ValueError, match="unknown successful recovery action type"):
        recovery_action_row_to_fact_record(
            {
                "action_id": "action-unknown",
                "action_type": "unknown-action",
                "target_id": "job-a",
                "state": "completed",
                "result_code": "succeeded",
                "requested_at": NOW,
                "finished_at": NOW + timedelta(seconds=20),
                "detail": {},
            }
        )


def test_recovery_action_ledger_versions_have_distinct_fact_ids_and_replay() -> None:
    running = ledger_row_to_fact_record(
        {
            "ingest_seq": 1,
            "source": "recovery",
            "source_id": "action-same",
            "source_version": "1",
            "qualification_observed_at": NOW + timedelta(seconds=1),
            "payload": {
                "action_id": "action-same",
                "action_type": "retry-job",
                "target_id": "job-a",
                "state": "running",
                "result_code": None,
                "requested_at": NOW,
                "started_at": NOW + timedelta(seconds=1),
                "finished_at": None,
                "detail": {},
            },
        }
    )
    completed = ledger_row_to_fact_record(
        {
            "ingest_seq": 2,
            "source": "recovery",
            "source_id": "action-same",
            "source_version": "2",
            "qualification_observed_at": NOW + timedelta(seconds=2),
            "payload": {
                "action_id": "action-same",
                "action_type": "retry-job",
                "target_id": "job-a",
                "state": "completed",
                "result_code": "succeeded",
                "requested_at": NOW,
                "started_at": NOW + timedelta(seconds=1),
                "finished_at": NOW + timedelta(seconds=2),
                "detail": {"reason_code": "retry-job", "recovery_slo_seconds": 60},
            },
        }
    )

    assert running.fact.fact_id != completed.fact.fact_id
    store = InMemoryQualificationStore()
    service = QualificationService(
        policy=_policy(),
        fact_source=StaticQualificationFactSource((running, completed)),
        state_store=store,
        writer_id="test-service",
        batch_size=2,
    )

    service.tick(NOW)
    result = service.tick(NOW + timedelta(seconds=3))

    assert result.applied == 2
    assert result.state is QualificationState.ACCUMULATING
    assert [fact.reason for fact in store.current.facts] == ["healthy", "recovery.retry"]


def test_recovery_action_exact_version_keeps_same_fact_id_and_digest() -> None:
    row = {
        "ingest_seq": 1,
        "source": "recovery",
        "source_id": "action-exact",
        "source_version": "1",
        "qualification_observed_at": NOW + timedelta(seconds=1),
        "payload": {
            "action_id": "action-exact",
            "action_type": "retry-job",
            "target_id": "job-a",
            "state": "completed",
            "result_code": "succeeded",
            "requested_at": NOW,
            "finished_at": NOW + timedelta(seconds=1),
            "detail": {"reason_code": "retry-job", "recovery_slo_seconds": 60},
        },
    }
    first = ledger_row_to_fact_record(row)
    replay = ledger_row_to_fact_record(row)
    direct = recovery_action_row_to_fact_record(row["payload"])

    assert first.fact.fact_id == replay.fact.fact_id
    assert first.fact.digest == replay.fact.digest
    assert direct.fact.fact_id == "recovery:job-a:action-exact"


def test_retryable_failure_runtime_and_incident_facts_do_not_qualify_or_crash() -> None:
    runtime = runtime_event_row_to_fact_record(
        {
            "event_id": "runtime-retryable",
            "kind": "job.retryable-failed",
            "occurred_at": NOW + timedelta(minutes=1),
            "job_key": "structure:source:retry",
            "attempt_id": "attempt-retry",
            "lease_epoch": 1,
            "event_sequence": 3,
            "progress_current": 2,
            "detail": {
                "qualification_impact": "delayed",
                "reason_code": "timeout",
                "retry_count": 1,
            },
        }
    )
    incident = incident_event_row_to_fact_record(
        {
            "incident_event_id": "incident-retryable",
            "incident_key": "job-retry:structure:source:retry",
            "kind": "attempt-failed",
            "severity": "warning",
            "state": "open",
            "occurred_at": NOW + timedelta(minutes=1, seconds=1),
            "detail": {
                "job_key": "structure:source:retry",
                "stage": "source-fetch",
                "error_class": "TimeoutError",
                "consecutive_failures": 1,
                "retry_after_seconds": 15,
            },
        }
    )
    store = InMemoryQualificationStore()
    service = QualificationService(
        policy=_policy(),
        fact_source=StaticQualificationFactSource((runtime, incident)),
        state_store=store,
        writer_id="test-service",
        batch_size=2,
    )

    initialized = service.tick(NOW)
    assert initialized.applied == 0
    result = service.tick(NOW + timedelta(minutes=2))

    assert result.state is QualificationState.ACCUMULATING
    assert result.applied == 2
    assert store.current.qualified_at is None
    assert [fact.reason for fact in store.current.facts] == [
        "recovery.started",
        "recovery.started",
    ]
    assert not store.certificates


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
    payload = cast(Mapping[str, object], left.certificates[0]["payload"])
    identity = cast(Mapping[str, object], payload["identity"])
    assert identity["epoch_id"] == left.epochs[2].epoch_id
    assert left.certificates[0]["certificate_digest"] == certificate_digest(payload)
    assert left.certificates[0]["certificate_digest"] == right.certificates[0]["certificate_digest"]


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


def test_new_release_cursor_is_seeded_from_current_ledger_high_water() -> None:
    source = Path("src/polyarb/control_plane/qualification_service.py").read_text()
    ensure = source[source.index("def _ensure_source_cursor_row") :]

    assert "FROM public.m1_qualification_ingress_ledger AS ledger" in ensure
    assert "FROM public.m1_qualification_source_cursors AS predecessor" in ensure
    assert "predecessor.identity_key <> %s" in ensure
    assert "ELSE NULL END" in ensure
    assert "ORDER BY ledger.ingest_seq DESC" in ensure
    assert "'stable_id', 'baseline:' || %s" in ensure
    assert "'ingest_seq', ledger.ingest_seq" in ensure
    assert "ON CONFLICT (identity_key) DO NOTHING" in ensure


def test_recovering_nonconfirmation_facts_are_observed_without_entering_epoch() -> None:
    breaker_at = NOW + timedelta(minutes=1)
    second_breaker_at = NOW + timedelta(minutes=2)
    confirmed_at = NOW + timedelta(minutes=3)
    records = (
        _record("runtime", 1, breaker_at, reason="lease.expired"),
        _record("runtime", 2, second_breaker_at, reason="lease.expired"),
        _record(
            "runtime",
            3,
            confirmed_at,
            reason="recovery.confirmed",
            recovery_confirmed=True,
        ),
    )
    store = InMemoryQualificationStore()
    service = QualificationService(
        policy=_policy(),
        fact_source=StaticQualificationFactSource(records),
        state_store=store,
        writer_id="test-service",
        batch_size=3,
    )

    first = service.tick(breaker_at)
    assert first.state is QualificationState.RECOVERING
    recovered = service.tick(confirmed_at)

    assert recovered.state is QualificationState.ACCUMULATING
    assert store.epochs[0].state is QualificationState.INVALIDATED
    assert store.epochs[0].facts == (records[0].fact,)
    assert store.epochs[1].state is QualificationState.RECOVERING
    assert store.epochs[1].facts == ()
    assert store.epochs[2].facts == (records[2].fact,)
    assert store.recovery_observations == (records[1],)
    assert store.cursor == records[2].cursor


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


def test_postgres_freshness_observations_use_bounded_wrapper() -> None:
    cursor = _FreshnessCursor(
        [
            {
                "fact_id": "freshness:structure:cursor:structure:current",
                "data_product": "structure",
                "observed_at": NOW,
                "freshness_seconds": 3,
                "freshness_slo_seconds": 900,
                "progress_count": 4,
                "successful_count": 4,
            },
            None,
            {
                "fact_id": "freshness:opportunity:cursor:opportunity:current",
                "data_product": "opportunity",
                "observed_at": NOW,
                "freshness_seconds": 7,
                "freshness_slo_seconds": 900,
                "progress_count": 2,
                "successful_count": 2,
            },
        ]
    )
    source = PostgresQualificationFactSource(lambda: cast(Any, None))

    source._insert_freshness_observations(cast(Any, cursor), now=NOW)

    structure_query = cursor.calls[0][0]
    assert "pointer.pointer_key = 'quote:current'" in structure_query
    assert "pointer.generation_key ~ '^quote:[0-9a-f]{64}$'" in structure_query
    assert "'structure:' || substr(pointer.generation_key, 7)" in structure_query
    assert "m1_quote_admission_inputs" not in structure_query
    assert "pointer.pointer_key = 'structure:current'" not in structure_query
    quote_query = cursor.calls[2][0]
    assert "pointer.generation_key ~ '^quote:[0-9a-f]{64}$'" in quote_query
    writes = [
        statement for statement, _params in cursor.calls if "m1_record_qualification" in statement
    ]
    assert len(writes) == 3
    assert all("public.m1_record_qualification_freshness_ingress" in call for call in writes)
    assert all("m1_record_qualification_ingress(" not in call for call in writes)
    assert [params[1] for _statement, params in cursor.calls if len(params) == 4] == [
        "structure",
        "quote",
        "opportunity",
    ]


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
    assert sealed.certificate_digest == store.certificates[0]["certificate_digest"]


class _FreshnessCursor:
    def __init__(self, rows: list[Mapping[str, object] | None]) -> None:
        self._rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, statement: object, params: tuple[object, ...]) -> None:
        self.calls.append((str(statement), params))

    def fetchone(self) -> Mapping[str, object] | None:
        if not self._rows:
            return None
        return self._rows.pop(0)


def test_qualification_status_never_transfers_unbounded_epoch_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    last_record = _record("freshness", 1, NOW, progress_count=1, successful_count=1)
    cursor = _StatusCursor(
        {
            "epoch_id": "epoch-status",
            "state": "accumulating",
            "version": 7,
            "started_at": NOW - timedelta(minutes=5),
            "last_fact_at": NOW,
            "invalidated_at": None,
            "invalidation_reason": None,
            "qualified_at": None,
            "previous_epoch_id": None,
            "max_gap_seconds": 3,
            "contained_recoveries": ["recovery.retry"],
            "contained_recovery_count": 1,
            "last_fact_record": last_record.to_json(),
        }
    )
    connection = _StatusConnection(cursor)

    def list_certificates(_factory: object, *, limit: int) -> list[object]:
        assert limit == 1
        assert connection.closed is True
        return []

    monkeypatch.setattr(
        qualification_service_module,
        "list_qualification_certificates",
        list_certificates,
    )

    connection_factory = cast(Any, lambda: connection)
    status = PostgresQualificationServiceStore(connection_factory).status(now=NOW)

    epoch_query = next(query for query in cursor.queries if "m1_qualification_epochs" in query)
    assert "SELECT *" not in epoch_query
    assert "fact_records" not in epoch_query
    assert "fact_digests" not in epoch_query
    assert "jsonb_array_elements" not in epoch_query
    assert "m1_qualification_epoch_facts" in epoch_query
    assert "runtime_contained_recovery_count" in epoch_query
    assert "LIMIT 20" in epoch_query
    assert status["last_fact"] == {
        "fact_id": "freshness:001",
        "reason": "healthy",
        "observed_at": NOW.isoformat(),
        "source": "freshness",
        "cursor": last_record.cursor.to_json(),
    }
    assert status["contained_recoveries"] == ["recovery.retry"]
    assert status["contained_recovery_count"] == 1
    assert status["contained_recoveries_truncated"] is False


class _StatusConnection:
    def __init__(self, cursor: _StatusCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def __enter__(self) -> _StatusConnection:
        self.closed = False
        return self

    def __exit__(self, *_args: object) -> None:
        self.closed = True
        return None

    def cursor(self, **_kwargs: object) -> _StatusCursor:
        return self._cursor


class _StatusCursor:
    def __init__(self, epoch_row: Mapping[str, object]) -> None:
        self._epoch_row = epoch_row
        self._next_row: Mapping[str, object] | None = None
        self.queries: list[str] = []

    def __enter__(self) -> _StatusCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: object, _params: object = None) -> None:
        query = str(statement)
        self.queries.append(query)
        if "m1_qualification_epochs" in query:
            assert "SELECT *" not in query
            self._next_row = self._epoch_row
        else:
            self._next_row = None

    def fetchone(self) -> Mapping[str, object] | None:
        row = self._next_row
        self._next_row = None
        return row
