"""Cross-job runtime coverage contracts for M1 transactional workers."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import psycopg
import pytest
from psycopg.rows import dict_row

from polyarb.control_plane.models import JobLease
from polyarb.control_plane.postgres import PostgresControlPlane
from polyarb.control_plane.runtime_contract import RUNTIME_STAGE_REGISTRY, AttemptRuntime
from polyarb.control_plane.runtime_models import (
    RuntimeDeadlineProfile,
    RuntimeEventKind,
    RuntimeProgress,
)

REQUIRED_JOB_TYPES = (
    "structure-fetch",
    "structure-materialize",
    "structure-normalize",
    "structure-certify",
    "quote-admit",
    "quote-batch",
    "quote-certify",
    "opportunity-certify",
)
SECRET_LIKE_DETAIL_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "secret",
    "token",
)
PROFILE = RuntimeDeadlineProfile(
    policy_version="runtime-v1",
    lease_seconds=120,
    heartbeat_seconds=30,
    progress_seconds=120,
    attempt_seconds=1200,
)
NOW = datetime(2030, 1, 1, tzinfo=UTC)


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(
                ["docker", "info"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            ).returncode
            == 0
        )
    except OSError:
        return False


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    if not _docker_available():
        pytest.skip("Docker daemon unavailable; runtime coverage test skipped")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = postgres.get_connection_url()
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if dsn.startswith(prefix):
                dsn = "postgresql://" + dsn[len(prefix) :]
        with psycopg.connect(dsn, autocommit=True) as connection:
            for role in ("anon", "authenticated", "service_role"):
                connection.execute(f"CREATE ROLE {role} NOLOGIN")
        result = subprocess.run(
            ["uv", "run", "alembic", "upgrade", "head"],
            env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        yield dsn


@pytest.fixture()
def control_plane(postgres_dsn: str) -> Iterator[PostgresControlPlane]:
    def connect() -> psycopg.Connection[Any]:
        return psycopg.connect(postgres_dsn)

    with connect() as connection:
        for table in (
            "m1_job_runtime_events",
            "m1_job_runtime_state",
            "m1_job_attempts",
            "m1_jobs",
        ):
            connection.execute(f"TRUNCATE {table} CASCADE")
    yield PostgresControlPlane(connect)


class _CapturingStore:
    def __init__(self) -> None:
        self.progress: list[dict[str, object]] = []

    def record_runtime_progress(
        self,
        lease: JobLease,
        *,
        progress: RuntimeProgress,
        now: datetime,
        detail: dict[str, object] | None = None,
    ) -> object:
        self.progress.append(
            {"lease": lease, "progress": progress, "now": now, "detail": detail}
        )
        return object()

    def heartbeat_runtime_attempt(
        self,
        lease: JobLease,
        *,
        now: datetime,  # noqa: ARG002
        lease_seconds: int,  # noqa: ARG002
    ) -> JobLease:
        return lease


def test_runtime_registry_has_exact_eight_job_types_with_meaningful_stage_names() -> None:
    assert tuple(RUNTIME_STAGE_REGISTRY) == REQUIRED_JOB_TYPES
    for job_type, stages in RUNTIME_STAGE_REGISTRY.items():
        assert stages
        assert len(stages) == len(set(stages))
        assert all(
            stage and stage != "started" and not stage.startswith("job.")
            for stage in stages
        )


@pytest.mark.parametrize("secret_key", SECRET_LIKE_DETAIL_KEY_PARTS)
def test_runtime_reporter_rejects_secret_like_detail_keys_before_persistence(
    secret_key: str,
) -> None:
    lease = JobLease(
        job_key=f"runtime-secret:{secret_key}",
        job_type="quote-batch",
        input_identity=f"runtime-secret:{secret_key}",
        lease_owner="runtime-worker",
        lease_epoch=1,
        lease_expires_at=NOW + timedelta(seconds=PROFILE.lease_seconds),
        checkpoint_cursor=None,
        checkpoint_digest=None,
    )
    store = _CapturingStore()
    runtime = AttemptRuntime(store=store, lease=lease, profile=PROFILE, clock=lambda: NOW)

    with pytest.raises(ValueError, match="secret-like runtime detail key"):
        runtime.progress(
            stage="read-input",
            current=1,
            total=1,
            detail={secret_key: "redacted"},
        )

    assert store.progress == []


def test_all_transactional_job_types_persist_one_start_progress_chain_and_terminal_event(
    control_plane: PostgresControlPlane,
) -> None:
    assert tuple(RUNTIME_STAGE_REGISTRY) == REQUIRED_JOB_TYPES
    for index, job_type in enumerate(REQUIRED_JOB_TYPES, start=1):
        _claim_progress_and_complete(control_plane, job_type=job_type, offset=index)

    rows = _runtime_event_rows(control_plane)
    rows_by_job_type = {job_type: [] for job_type in REQUIRED_JOB_TYPES}
    for row in rows:
        rows_by_job_type[str(row["job_type"])].append(row)

    assert set(rows_by_job_type) == set(REQUIRED_JOB_TYPES)
    for job_type, events in rows_by_job_type.items():
        stages = RUNTIME_STAGE_REGISTRY[job_type]
        kinds = [row["kind"] for row in events]
        assert kinds.count(RuntimeEventKind.STARTED.value) == 1
        assert kinds.count(RuntimeEventKind.SUCCEEDED.value) == 1
        assert kinds.count(RuntimeEventKind.STAGE_CHANGED.value) == len(stages)
        assert len(events) == len(stages) + 2
        assert [row["event_sequence"] for row in events] == list(range(1, len(events) + 1))
        assert events[0]["kind"] == RuntimeEventKind.STARTED.value
        assert events[0]["stage"] == "started"

        progress_events = [
            row for row in events if row["kind"] == RuntimeEventKind.STAGE_CHANGED.value
        ]
        assert [row["stage"] for row in progress_events] == list(stages)
        assert [row["progress_sequence"] for row in progress_events] == list(
            range(1, len(stages) + 1)
        )
        assert all(row["progress_current"] >= 1 for row in progress_events)
        assert all(row["progress_total"] >= row["progress_current"] for row in progress_events)

        terminal = events[-1]
        assert terminal["kind"] == RuntimeEventKind.SUCCEEDED.value
        assert terminal["stage"] == stages[-1]
        assert terminal["progress_sequence"] == len(stages)
        assert terminal["progress_current"] == len(stages)
        assert terminal["progress_total"] == len(stages)

        for event in events:
            detail = cast(dict[str, object], event["detail"])
            assert not _secret_like_detail_keys(detail)


def _claim_progress_and_complete(
    control_plane: PostgresControlPlane, *, job_type: str, offset: int
) -> None:
    now = NOW + timedelta(minutes=offset)
    job_key = f"runtime-coverage:{job_type}"
    control_plane.enqueue_job(
        job_key=job_key,
        job_type=job_type,
        input_identity=f"runtime-coverage:{job_type}",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id=f"{job_type}-worker",
        job_types=(job_type,),
        lease_seconds=PROFILE.lease_seconds,
        now=now,
    )
    assert lease is not None

    runtime = AttemptRuntime(
        store=control_plane,
        lease=lease,
        profile=PROFILE,
        clock=lambda: now + timedelta(seconds=1),
    )
    stages = RUNTIME_STAGE_REGISTRY[job_type]
    for current, stage in enumerate(stages, start=1):
        runtime.progress(
            stage=stage,
            current=current,
            total=len(stages),
            detail={"component": job_type},
        )

    with (
        control_plane._connection_factory() as connection,  # noqa: SLF001
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        PostgresControlPlane._append_job_succeeded_cursor(  # noqa: SLF001
            cursor,
            lease=runtime.current_lease,
            stage=stages[-1],
            component=job_type,
            data_product=(
                "structure-sync" if job_type.startswith("structure-") else "market-snapshot"
            ),
            now=now + timedelta(seconds=2),
        )
        cursor.execute(
            """
            UPDATE m1_jobs
            SET state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
                updated_at = %s
            WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
              AND state = 'leased'
            """,
            (
                now + timedelta(seconds=2),
                runtime.current_lease.job_key,
                runtime.current_lease.lease_owner,
                runtime.current_lease.lease_epoch,
            ),
        )
        assert cursor.rowcount == 1
        cursor.execute(
            """
            UPDATE m1_job_attempts
            SET state = 'succeeded', finished_at = %s
            WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
            """,
            (
                now + timedelta(seconds=2),
                runtime.current_lease.job_key,
                runtime.current_lease.lease_epoch,
            ),
        )
        assert cursor.rowcount == 1


def _runtime_event_rows(control_plane: PostgresControlPlane) -> list[dict[str, object]]:
    with control_plane._connection_factory() as connection, connection.cursor(  # noqa: SLF001
        row_factory=dict_row
    ) as cursor:
        cursor.execute(
            """
            SELECT job.job_type, event.job_key, event.event_sequence, event.kind,
                   event.stage, event.progress_sequence, event.progress_current,
                   event.progress_total, event.detail
            FROM m1_job_runtime_events AS event
            JOIN m1_jobs AS job ON job.job_key = event.job_key
            ORDER BY job.job_type, event.event_sequence
            """
        )
        return list(cursor.fetchall())


def _secret_like_detail_keys(detail: dict[str, object]) -> set[str]:
    found: set[str] = set()
    for key in detail:
        normalized = key.casefold().replace("-", "_")
        if any(part in normalized for part in SECRET_LIKE_DETAIL_KEY_PARTS):
            found.add(key)
    return found
