"""Fenced, synchronous Postgres repository for durable M1 worker effects."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .models import CheckpointReceipt, JobLease, JobState


class ControlPlaneError(RuntimeError):
    """Base class for control-plane semantic failures."""


class StaleLeaseError(ControlPlaneError):
    """A worker attempted an effect after its lease was fenced out."""


class JobIdentityConflict(ControlPlaneError):
    """A deterministic job key was reused for a different input."""


class CheckpointConflictError(ControlPlaneError):
    """An idempotency key was reused for a different checkpoint."""


ConnectionFactory = Callable[[], psycopg.Connection[Any]]


class PostgresControlPlane:
    """Own atomic job transitions; callers provide the connection factory."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def enqueue_job(
        self,
        *,
        job_key: str,
        job_type: str,
        input_identity: str,
        now: datetime,
    ) -> None:
        self._validate_nonempty(job_key=job_key, job_type=job_type, input_identity=input_identity)
        self._validate_aware(now, "now")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                INSERT INTO m1_jobs (
                    job_key, job_type, input_identity, state, next_attempt_at,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, 'runnable', %s, %s, %s)
                ON CONFLICT (job_key) DO NOTHING
                """,
                (job_key, job_type, input_identity, now, now, now),
            )
            cursor.execute(
                "SELECT job_type, input_identity FROM m1_jobs WHERE job_key = %s",
                (job_key,),
            )
            existing = cursor.fetchone()
            if existing is None:
                raise ControlPlaneError("job insert was not durable")
            if existing["job_type"] != job_type or existing["input_identity"] != input_identity:
                raise JobIdentityConflict(f"job key {job_key!r} names another input")

    def claim_job(
        self,
        *,
        worker_id: str,
        job_types: Sequence[str],
        lease_seconds: int,
        now: datetime,
    ) -> JobLease | None:
        self._validate_nonempty(worker_id=worker_id)
        self._validate_aware(now, "now")
        if not job_types or any(not job_type.strip() for job_type in job_types):
            raise ValueError("job_types must contain non-empty values")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        expires_at = now + timedelta(seconds=lease_seconds)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT job_key, job_type, input_identity, checkpoint_cursor,
                       checkpoint_digest, lease_epoch
                FROM m1_jobs
                WHERE job_type = ANY(%s)
                  AND (
                      (state IN ('runnable', 'retryable', 'checkpointed')
                       AND (next_attempt_at IS NULL OR next_attempt_at <= %s))
                      OR (state = 'leased' AND lease_expires_at <= %s)
                  )
                ORDER BY next_attempt_at NULLS FIRST, updated_at, job_key
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                (list(job_types), now, now),
            )
            job = cursor.fetchone()
            if job is None:
                return None
            epoch = int(job["lease_epoch"]) + 1
            cursor.execute(
                """
                UPDATE m1_jobs
                SET state = 'leased', lease_owner = %s, lease_epoch = %s,
                    lease_expires_at = %s, attempt_count = attempt_count + 1,
                    next_attempt_at = NULL, updated_at = %s
                WHERE job_key = %s
                """,
                (worker_id, epoch, expires_at, now, job["job_key"]),
            )
            cursor.execute(
                """
                INSERT INTO m1_job_attempts (
                    attempt_id, job_key, lease_epoch, worker_id, state, started_at
                ) VALUES (%s, %s, %s, %s, 'running', %s)
                """,
                (str(uuid4()), job["job_key"], epoch, worker_id, now),
            )
            return JobLease(
                job_key=job["job_key"],
                job_type=job["job_type"],
                input_identity=job["input_identity"],
                lease_owner=worker_id,
                lease_epoch=epoch,
                lease_expires_at=expires_at,
                checkpoint_cursor=job["checkpoint_cursor"],
                checkpoint_digest=job["checkpoint_digest"],
            )

    def heartbeat(self, lease: JobLease, *, now: datetime, lease_seconds: int = 30) -> JobLease:
        self._validate_aware(now, "now")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        expires_at = now + timedelta(seconds=lease_seconds)
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE m1_jobs SET lease_expires_at = %s, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (expires_at, now, lease.job_key, lease.lease_owner, lease.lease_epoch),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
        return JobLease(
            job_key=lease.job_key,
            job_type=lease.job_type,
            input_identity=lease.input_identity,
            lease_owner=lease.lease_owner,
            lease_epoch=lease.lease_epoch,
            lease_expires_at=expires_at,
            checkpoint_cursor=lease.checkpoint_cursor,
            checkpoint_digest=lease.checkpoint_digest,
        )

    def checkpoint(
        self,
        lease: JobLease,
        *,
        checkpoint_cursor: str,
        checkpoint_digest: str,
        idempotency_key: str,
        now: datetime,
        artifact_key: str | None = None,
    ) -> CheckpointReceipt:
        self._validate_nonempty(
            checkpoint_cursor=checkpoint_cursor,
            checkpoint_digest=checkpoint_digest,
            idempotency_key=idempotency_key,
        )
        self._validate_aware(now, "now")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT receipt_id, job_key, lease_epoch, idempotency_key, checkpoint_cursor,
                       checkpoint_digest, committed_at
                FROM m1_checkpoint_receipts WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                receipt = self._receipt(existing)
                if (
                    receipt.job_key != lease.job_key
                    or receipt.lease_epoch != lease.lease_epoch
                    or receipt.checkpoint_cursor != checkpoint_cursor
                    or receipt.checkpoint_digest != checkpoint_digest
                ):
                    raise CheckpointConflictError(f"idempotency conflict for {idempotency_key!r}")
                return receipt
            cursor.execute(
                """
                UPDATE m1_jobs
                SET checkpoint_cursor = %s, checkpoint_digest = %s, state = 'checkpointed',
                    updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (
                    checkpoint_cursor,
                    checkpoint_digest,
                    now,
                    lease.job_key,
                    lease.lease_owner,
                    lease.lease_epoch,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            receipt = CheckpointReceipt(
                receipt_id=str(uuid4()),
                job_key=lease.job_key,
                lease_epoch=lease.lease_epoch,
                idempotency_key=idempotency_key,
                checkpoint_cursor=checkpoint_cursor,
                checkpoint_digest=checkpoint_digest,
                committed_at=now,
            )
            cursor.execute(
                """
                INSERT INTO m1_checkpoint_receipts (
                    receipt_id, job_key, lease_epoch, idempotency_key, checkpoint_cursor,
                    checkpoint_digest, artifact_key, committed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    receipt.receipt_id,
                    receipt.job_key,
                    receipt.lease_epoch,
                    receipt.idempotency_key,
                    receipt.checkpoint_cursor,
                    receipt.checkpoint_digest,
                    artifact_key,
                    receipt.committed_at,
                ),
            )
            cursor.execute(
                """
                UPDATE m1_job_attempts SET state = 'checkpointed', finished_at = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (now, lease.job_key, lease.lease_epoch),
            )
            return receipt

    def finish(
        self,
        lease: JobLease,
        *,
        state: JobState,
        now: datetime,
        next_attempt_at: datetime | None = None,
        error_class: str | None = None,
    ) -> None:
        if state not in {JobState.RETRYABLE, JobState.SUCCEEDED, JobState.QUARANTINED}:
            raise ValueError("finish only accepts retryable, succeeded, or quarantined")
        self._validate_aware(now, "now")
        if next_attempt_at is not None:
            self._validate_aware(next_attempt_at, "next_attempt_at")
        if state is JobState.RETRYABLE and next_attempt_at is None:
            raise ValueError("retryable finish requires next_attempt_at")
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE m1_jobs
                SET state = %s, next_attempt_at = %s, last_error_class = %s,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (
                    state.value,
                    next_attempt_at,
                    error_class,
                    now,
                    lease.job_key,
                    lease.lease_owner,
                    lease.lease_epoch,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            cursor.execute(
                """
                UPDATE m1_job_attempts
                SET state = %s, finished_at = %s, error_class = %s
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (state.value, now, error_class, lease.job_key, lease.lease_epoch),
            )

    def record_incident_event(
        self,
        *,
        incident_key: str,
        dedupe_key: str,
        component: str,
        severity: str,
        summary: str,
        kind: str,
        detail: dict[str, object],
        idempotency_key: str,
        channels: Sequence[str],
        now: datetime,
    ) -> str:
        """Persist an incident event and every alert intent in one transaction."""
        self._validate_nonempty(
            incident_key=incident_key,
            dedupe_key=dedupe_key,
            component=component,
            severity=severity,
            summary=summary,
            kind=kind,
            idempotency_key=idempotency_key,
        )
        self._validate_aware(now, "now")
        if not channels or any(not channel.strip() for channel in channels):
            raise ValueError("channels must contain non-empty values")
        if len(set(channels)) != len(channels):
            raise ValueError("channels must be unique")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                INSERT INTO m1_incidents (
                    incident_key, dedupe_key, component, severity, state, summary,
                    opened_at, updated_at
                ) VALUES (%s, %s, %s, %s, 'open', %s, %s, %s)
                ON CONFLICT (dedupe_key) DO NOTHING
                """,
                (incident_key, dedupe_key, component, severity, summary, now, now),
            )
            cursor.execute(
                "SELECT incident_key FROM m1_incidents WHERE dedupe_key = %s",
                (dedupe_key,),
            )
            incident = cursor.fetchone()
            if incident is None or incident["incident_key"] != incident_key:
                raise JobIdentityConflict(f"dedupe key {dedupe_key!r} names another incident")
            cursor.execute(
                "SELECT incident_event_id FROM m1_incident_events WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                return str(existing["incident_event_id"])
            event_id = str(uuid4())
            cursor.execute(
                """
                INSERT INTO m1_incident_events (
                    incident_event_id, incident_key, kind, detail, idempotency_key, occurred_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (event_id, incident_key, kind, Jsonb(detail), idempotency_key, now),
            )
            for channel in channels:
                cursor.execute(
                    """
                    INSERT INTO m1_alert_outbox (
                        outbox_id, incident_event_id, channel, payload, state,
                        next_attempt_at, created_at
                    ) VALUES (%s, %s, %s, %s, 'pending', %s, %s)
                    ON CONFLICT (incident_event_id, channel) DO NOTHING
                    """,
                    (
                        str(uuid4()),
                        event_id,
                        channel,
                        Jsonb({"incident_key": incident_key, "kind": kind}),
                        now,
                        now,
                    ),
                )
            return event_id

    def operational_snapshot(
        self,
        *,
        now: datetime,
        sample_limit: int = 20,
    ) -> dict[str, object]:
        """Read the bounded, durable operator view without touching SQLite.

        This projection deliberately uses only control-plane facts.  A stalled
        data worker or unavailable Fly volume must therefore not turn an
        operator incident into an empty/healthy response.
        """
        self._validate_aware(now, "now")
        if not 1 <= sample_limit <= 100:
            raise ValueError("sample_limit must be in 1..100")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5000ms'")
            cursor.execute(
                "SELECT state, count(*) AS count FROM m1_jobs GROUP BY state"
            )
            job_counts = {
                str(row["state"]): int(row["count"])
                for row in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT extract(epoch FROM (%s - min(created_at))) AS age_seconds
                FROM m1_jobs WHERE state IN ('runnable', 'retryable', 'checkpointed')
                """,
                (now,),
            )
            oldest = cursor.fetchone()
            cursor.execute(
                """
                SELECT count(*) AS count FROM m1_jobs
                WHERE state = 'leased' AND lease_expires_at <= %s
                """,
                (now,),
            )
            expired = cursor.fetchone()
            cursor.execute(
                """
                SELECT job_key, lease_epoch, worker_id, state
                FROM m1_job_attempts ORDER BY started_at DESC, attempt_id DESC LIMIT %s
                """,
                (sample_limit,),
            )
            attempts = [
                {
                    "job_key": str(row["job_key"]),
                    "lease_epoch": int(row["lease_epoch"]),
                    "worker_id": str(row["worker_id"]),
                    "state": str(row["state"]),
                }
                for row in cursor.fetchall()
            ]
            cursor.execute(
                """
                SELECT incident_key, component, severity, summary
                FROM m1_incidents WHERE state = 'open'
                ORDER BY opened_at DESC, incident_key DESC LIMIT %s
                """,
                (sample_limit,),
            )
            incidents = [
                {
                    "incident_key": str(row["incident_key"]),
                    "component": str(row["component"]),
                    "severity": str(row["severity"]),
                    "summary": str(row["summary"]),
                }
                for row in cursor.fetchall()
            ]
            cursor.execute(
                """
                SELECT i.incident_key, o.channel, o.state
                FROM m1_alert_outbox o
                JOIN m1_incident_events e ON e.incident_event_id = o.incident_event_id
                JOIN m1_incidents i ON i.incident_key = e.incident_key
                WHERE o.state = 'pending'
                ORDER BY o.created_at DESC, o.outbox_id DESC LIMIT %s
                """,
                (sample_limit,),
            )
            outbox = [
                {
                    "incident_key": str(row["incident_key"]),
                    "channel": str(row["channel"]),
                    "state": str(row["state"]),
                }
                for row in cursor.fetchall()
            ]
        age = None if oldest is None else oldest["age_seconds"]
        return {
            "job_counts": job_counts,
            "oldest_runnable_age_seconds": None if age is None else float(age),
            "expired_leases": 0 if expired is None else int(expired["count"]),
            "recent_attempts": attempts,
            "open_incidents": incidents,
            "pending_alert_outbox": outbox,
        }

    @staticmethod
    def _receipt(row: dict[str, Any]) -> CheckpointReceipt:
        return CheckpointReceipt(
            receipt_id=row["receipt_id"],
            job_key=row["job_key"],
            lease_epoch=int(row["lease_epoch"]),
            idempotency_key=row["idempotency_key"],
            checkpoint_cursor=row["checkpoint_cursor"],
            checkpoint_digest=row["checkpoint_digest"],
            committed_at=row["committed_at"],
        )

    @staticmethod
    def _validate_nonempty(**values: str) -> None:
        for field, value in values.items():
            if not value.strip():
                raise ValueError(f"{field} must be non-empty")

    @staticmethod
    def _validate_aware(value: datetime, field: str) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")
