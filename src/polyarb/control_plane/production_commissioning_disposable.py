"""Real transactional normal-turn fixtures for disposable commissioning databases."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO
from typing import Any

from botocore.exceptions import ReadTimeoutError
from psycopg.types.json import Jsonb

from .models import JobLease, JobState, QuoteBatchLeg, QuoteBatchSpec
from .postgres import (
    IncompleteStructureGenerationError,
    PostgresControlPlane,
    PublicationPointerConflictError,
    StaleLeaseError,
)
from .production_commissioning_runner import AttackIdentity, AttackStageReceipt
from .quote_admission import (
    QuoteAdmissionShardUnavailable,
    TransactionalQuoteAdmitter,
)
from .reconciler import RuntimeReconciler
from .recovery_executor import RecoveryExecutor
from .recovery_models import RecoveryActionType
from .recovery_records import RecoveryActionRecord, RuntimeControllerLease
from .recovery_store import claim_controller, read_runtime_reconcile_states, schedule_action
from .runtime_contract import RUNTIME_STAGE_REGISTRY, AttemptRuntime
from .runtime_deadlines import runtime_deadline_profile, runtime_retry_policy
from .runtime_models import RuntimeDeadlineProfile, RuntimeEventKind
from .structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    StructureRangeArtifact,
    StructureShardArtifact,
    StructureShardReceipt,
    canonical_structure_bundle_bytes,
    canonical_structure_manifest_bytes,
    canonical_structure_range_bytes,
    canonical_structure_shard_bytes,
    canonical_structure_shard_manifest_bytes,
)
from .structure_worker import (
    StructureNormalizationInputInvalid,
    TransactionalStructureCertifier,
    TransactionalStructureWorker,
)


class DisposableCommissioningError(RuntimeError):
    """A disposable database did not produce the required real durable fact."""


class _DisposableObjectStore:
    """Small exact-byte object boundary for provider-independent attacks."""

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self._read_timeout_keys: set[str] = set()
        self._read_counts: dict[str, int] = {}

    def arm_read_timeout(self, key: str) -> None:
        if key not in self._objects:
            raise DisposableCommissioningError("read-timeout-artifact-missing")
        self._read_timeout_keys.add(key)

    def read_count(self, key: str) -> int:
        return self._read_counts.get(key, 0)

    def restore(self, *, key: str, payload: bytes, digest: str) -> None:
        if sha256(payload).hexdigest() != digest:
            raise DisposableCommissioningError("object-restore-digest-mismatch")
        self._objects[key] = (payload, {"sha256": digest})

    def contains(self, key: str) -> bool:
        return key in self._objects

    def get_object(self, **kwargs: Any) -> dict[str, object]:
        key = str(kwargs["Key"])
        self._read_counts[key] = self.read_count(key) + 1
        if key in self._read_timeout_keys:
            self._read_timeout_keys.remove(key)
            raise ReadTimeoutError(endpoint_url=f"https://r2.invalid/{key}")
        try:
            payload, _metadata = self._objects[key]
        except KeyError as error:
            raise FileNotFoundError("commissioning-object-unavailable") from error
        return {"Body": BytesIO(payload)}

    def put_object(self, **kwargs: Any) -> None:
        key = str(kwargs["Key"])
        body = kwargs["Body"]
        metadata = kwargs.get("Metadata", {})
        if not isinstance(body, bytes) or not isinstance(metadata, dict):
            raise DisposableCommissioningError("invalid-disposable-object-write")
        self._objects[key] = (
            body,
            {str(name): str(value) for name, value in metadata.items()},
        )

    def head_object(self, **kwargs: Any) -> dict[str, object]:
        key = str(kwargs["Key"])
        try:
            payload, metadata = self._objects[key]
        except KeyError as error:
            raise FileNotFoundError("commissioning-object-unavailable") from error
        return {"ContentLength": len(payload), "Metadata": dict(metadata)}


@dataclass(frozen=True)
class PreparedNormalTurn:
    """A real domain transaction held immediately before its terminal boundary."""

    control_plane: PostgresControlPlane
    lease: JobLease
    _commit: Callable[[JobLease, datetime], None]

    def complete(self, *, now: datetime, lease: JobLease | None = None) -> dict[str, str]:
        """Commit with the supplied owner and return only database-backed proof IDs."""

        active_lease = lease or self.lease
        if now.tzinfo is None or now.utcoffset() is None:
            raise DisposableCommissioningError("invalid-now")
        if (
            active_lease.job_key != self.lease.job_key
            or active_lease.job_type != self.lease.job_type
            or active_lease.input_identity != self.lease.input_identity
        ):
            raise DisposableCommissioningError("replacement-identity-mismatch")
        now = now.astimezone(UTC)
        if active_lease.lease_epoch != self.lease.lease_epoch:
            _record_progress(self.control_plane, active_lease, now - timedelta(seconds=1))
        self._commit(active_lease, now)
        return _normal_turn_proof(self.control_plane, active_lease)


class StaleOwnerCommissioningAdapter:
    """Run the shared stale-terminal-write attack through real node transactions."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._prepared: PreparedNormalTurn | None = None
        self._replacement: JobLease | None = None
        self._old_attempt_id: str | None = None
        self._replacement_attempt_id: str | None = None
        self._recovered_proof: dict[str, str] | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if identity.attack_id != "stale-owner-terminal-write":
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _attempt_id(self, lease: JobLease) -> str:
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT attempt_id FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (lease.job_key, lease.lease_epoch),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("attempt-fact-missing")
        return str(row[0])

    def _assert_stale_absent(self) -> None:
        prepared = self._need(self._prepared, "preflight-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            attempt = connection.execute(
                """
                SELECT count(*) FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s AND state = 'succeeded'
                """,
                (prepared.lease.job_key, prepared.lease.lease_epoch),
            ).fetchone()
            event = connection.execute(
                """
                SELECT count(*) FROM m1_job_runtime_events
                WHERE job_key = %s AND lease_epoch = %s AND kind = %s
                """,
                (
                    prepared.lease.job_key,
                    prepared.lease.lease_epoch,
                    RuntimeEventKind.SUCCEEDED.value,
                ),
            ).fetchone()
        if attempt != (0,) or event != (0,):
            raise DisposableCommissioningError("stale-terminal-effect")

    def _transition_at(self) -> datetime:
        prepared = self._need(self._prepared, "preflight-missing")
        return prepared.lease.lease_expires_at + timedelta(microseconds=1)

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        self._prepared = prepare_normal_turn(
            self._control_plane,
            node_id=identity.node_id,
            experiment_id=identity.experiment_id,
            now=self._started_at,
        )
        self._old_attempt_id = self._attempt_id(self._prepared.lease)
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"attempt:{self._old_attempt_id}",
            occurred_at=self._prepared.lease.lease_expires_at - timedelta(seconds=119),
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        transition_at = self._transition_at()
        self._replacement = self._control_plane.claim_job(
            worker_id=f"commissioning:replacement:{identity.node_id}",
            job_types=(identity.node_id,),
            lease_seconds=120,
            now=transition_at,
        )
        replacement = self._need(self._replacement, "replacement-claim-missing")
        if (
            replacement.job_key != prepared.lease.job_key
            or replacement.lease_epoch != prepared.lease.lease_epoch + 1
        ):
            raise DisposableCommissioningError("replacement-lease-mismatch")
        self._replacement_attempt_id = self._attempt_id(replacement)
        try:
            prepared.complete(
                lease=prepared.lease,
                now=transition_at + timedelta(seconds=1),
            )
        except StaleLeaseError:
            pass
        else:
            raise DisposableCommissioningError("stale-owner-not-fenced")
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"attempt:{self._old_attempt_id}",
            occurred_at=transition_at + timedelta(seconds=1),
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        self._assert_stale_absent()
        return AttackStageReceipt(
            stage="detected",
            receipt_id=f"attempt:{self._replacement_attempt_id}",
            occurred_at=self._transition_at() + timedelta(seconds=2),
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"attempt:{self._replacement_attempt_id}",
            occurred_at=self._transition_at() + timedelta(seconds=3),
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        self._assert_stale_absent()
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"attempt:{self._old_attempt_id}",
            occurred_at=self._transition_at() + timedelta(seconds=4),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        replacement = self._need(self._replacement, "replacement-claim-missing")
        self._recovered_proof = prepared.complete(
            lease=replacement,
            now=self._transition_at() + timedelta(seconds=5),
        )
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{self._recovered_proof['success_fact_id']}",
            occurred_at=self._transition_at() + timedelta(seconds=5),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        proof = self._need(self._recovered_proof, "recovery-proof-missing")
        replacement = self._need(self._replacement, "replacement-claim-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            attempt = connection.execute(
                "SELECT lease_epoch FROM m1_job_attempts WHERE attempt_id = %s",
                (proof["attempt_id"],),
            ).fetchone()
        if attempt != (replacement.lease_epoch,):
            raise DisposableCommissioningError("replacement-proof-mismatch")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=proof["postcondition_fact_id"],
            occurred_at=self._transition_at() + timedelta(seconds=6),
        )


class HeartbeatOutageCommissioningAdapter:
    """Prove controller renewal of one live attempt after heartbeat outage."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._prepared: PreparedNormalTurn | None = None
        self._attempt_id_value: str | None = None
        self._profile: RuntimeDeadlineProfile | None = None
        self._channels: tuple[str, ...] | None = None
        self._last_heartbeat_at: datetime | None = None
        self._prior_lease_expires_at: datetime | None = None
        self._detected_at: datetime | None = None
        self._renewed_at: datetime | None = None
        self._renewed_lease: JobLease | None = None
        self._controller: RuntimeControllerLease | None = None
        self._action: RecoveryActionRecord | None = None
        self._recovered_proof: dict[str, str] | None = None
        self._recovery_event_id: str | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if identity.attack_id != "heartbeat-outage":
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _attempt_id(self, lease: JobLease) -> str:
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT attempt_id FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (lease.job_key, lease.lease_epoch),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("attempt-fact-missing")
        return str(row[0])

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = prepare_normal_turn(
            self._control_plane,
            node_id=identity.node_id,
            experiment_id=identity.experiment_id,
            now=self._started_at,
        )
        self._prepared = prepared
        self._attempt_id_value = self._attempt_id(prepared.lease)
        candidates = read_runtime_reconcile_states(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=f"commissioning:heartbeat-outage:{identity.node_id}",
            now=self._started_at,
            target_id=prepared.lease.job_key,
            sample_limit=1,
        )
        if len(candidates) != 1:
            raise DisposableCommissioningError("persisted-runtime-profile-missing")
        runtime = candidates[0].runtime_state
        self._profile = runtime.profile
        self._channels = candidates[0].channels
        self._last_heartbeat_at = runtime.last_heartbeat_at
        self._prior_lease_expires_at = runtime.lease_expires_at
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"attempt:{self._attempt_id_value}",
            occurred_at=runtime.last_heartbeat_at,
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        last_heartbeat_at = self._need(
            self._last_heartbeat_at, "last-heartbeat-missing"
        )
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"attempt:{self._attempt_id_value}",
            occurred_at=last_heartbeat_at + timedelta(microseconds=1),
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        last_heartbeat_at = self._need(
            self._last_heartbeat_at, "last-heartbeat-missing"
        )
        detected_at = last_heartbeat_at + timedelta(seconds=profile.heartbeat_seconds)
        if detected_at >= prepared.lease.lease_expires_at:
            raise DisposableCommissioningError("heartbeat-due-outside-live-lease")
        controller = claim_controller(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=f"commissioning:heartbeat-outage:{identity.node_id}",
            owner_id=f"commissioning:heartbeat-controller:{identity.node_id}",
            lease_seconds=profile.lease_seconds,
            now=detected_at,
        )
        candidates = read_runtime_reconcile_states(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=controller.controller_id,
            now=detected_at,
            target_id=prepared.lease.job_key,
            sample_limit=1,
        )
        if len(candidates) != 1:
            raise DisposableCommissioningError("heartbeat-candidate-missing")
        candidate = candidates[0]
        decision = RuntimeReconciler().evaluate(candidate.runtime_state, now=detected_at)
        if (
            decision.action is not RecoveryActionType.HEARTBEAT_JOB
            or decision.reason_code != "job.lease-at-risk"
            or decision.incident_severity != "warning"
            or decision.qualification_breaking
        ):
            raise DisposableCommissioningError(
                f"heartbeat-outage-misclassified:{decision.reason_code}"
            )
        action = schedule_action(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller=controller,
            decision=decision,
            incident_key=candidate.incident_key,
            component=candidate.component,
            target_type=candidate.target_type,
            target_id=candidate.target_id,
            recovery_episode_key=candidate.runtime_state.recovery_episode_key,
            expected_attempt_id=candidate.runtime_state.attempt_id,
            expected_lease_epoch=candidate.runtime_state.lease_epoch,
            recovery_budget_remaining=candidate.runtime_state.recovery_budget.remaining_actions,
            cooldown_seconds=candidate.cooldown_seconds,
            channels=candidate.channels,
            now=detected_at,
        )
        if action.state != "pending" or action.incident_key != candidate.incident_key:
            raise DisposableCommissioningError("heartbeat-recovery-not-scheduled")
        self._detected_at = detected_at
        self._controller = controller
        self._action = action
        return AttackStageReceipt(
            stage="detected",
            receipt_id=f"incident:{candidate.incident_key}",
            occurred_at=detected_at,
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        detected_at = self._need(self._detected_at, "detection-time-missing")
        controller = self._need(self._controller, "controller-missing")
        action = self._need(self._action, "recovery-action-missing")
        renewed_at = detected_at + timedelta(microseconds=1)
        result = RecoveryExecutor(
            connection_factory=self._control_plane._connection_factory,  # noqa: SLF001
            control_plane=self._control_plane,
            controller=controller,
            worker_id=f"commissioning:heartbeat-recovery:{identity.node_id}",
            action_lease_seconds=profile.heartbeat_seconds,
            heartbeat_lease_seconds=profile.lease_seconds,
        ).run_once(
            now=renewed_at,
            expected_action_id=action.action_id,
        )
        if result is None or result.action_id != action.action_id or result.outcome != "succeeded":
            raise DisposableCommissioningError("heartbeat-recovery-execution-failed")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            renewed = connection.execute(
                """
                SELECT j.lease_expires_at, r.last_heartbeat_at, r.lease_deadline_at,
                       r.attempt_id, r.lease_epoch
                FROM m1_jobs AS j
                JOIN m1_job_runtime_state AS r ON r.job_key = j.job_key
                WHERE j.job_key = %s
                """,
                (prepared.lease.job_key,),
            ).fetchone()
        expected_deadline = renewed_at + timedelta(seconds=profile.lease_seconds)
        if renewed != (
            expected_deadline,
            renewed_at,
            expected_deadline,
            self._attempt_id_value,
            prepared.lease.lease_epoch,
        ):
            raise DisposableCommissioningError("heartbeat-renewal-not-exact")
        if expected_deadline <= self._need(
            self._prior_lease_expires_at, "prior-lease-deadline-missing"
        ):
            raise DisposableCommissioningError("heartbeat-lease-not-extended")
        self._renewed_at = renewed_at
        self._renewed_lease = JobLease(
            job_key=prepared.lease.job_key,
            job_type=prepared.lease.job_type,
            input_identity=prepared.lease.input_identity,
            lease_owner=prepared.lease.lease_owner,
            lease_epoch=prepared.lease.lease_epoch,
            lease_expires_at=expected_deadline,
            checkpoint_cursor=prepared.lease.checkpoint_cursor,
            checkpoint_digest=prepared.lease.checkpoint_digest,
        )
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"action:{action.action_id}",
            occurred_at=renewed_at,
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        detected_at = self._need(self._detected_at, "detection-time-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            state = connection.execute(
                """
                SELECT j.state, j.lease_owner, j.lease_epoch,
                       count(a.attempt_id), min(a.state)
                FROM m1_jobs AS j
                JOIN m1_job_attempts AS a ON a.job_key = j.job_key
                WHERE j.job_key = %s
                GROUP BY j.state, j.lease_owner, j.lease_epoch
                """,
                (prepared.lease.job_key,),
            ).fetchone()
        if state != (
            "leased",
            prepared.lease.lease_owner,
            prepared.lease.lease_epoch,
            1,
            "running",
        ):
            raise DisposableCommissioningError("heartbeat-cleanup-changed-attempt")
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"attempt:{self._attempt_id_value}",
            occurred_at=detected_at + timedelta(microseconds=2),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        detected_at = self._need(self._detected_at, "detection-time-missing")
        renewed_lease = self._need(self._renewed_lease, "renewed-lease-missing")
        completed_at = detected_at + timedelta(seconds=5)
        self._recovered_proof = prepared.complete(
            lease=renewed_lease,
            now=completed_at,
        )
        recovered = self._control_plane.record_job_recovery(
            renewed_lease,
            component=identity.node_id,
            channels=self._need(self._channels, "recovery-channels-missing"),
            now=completed_at + timedelta(seconds=1),
        )
        if not recovered:
            raise DisposableCommissioningError("heartbeat-incident-not-resolved")
        action = self._need(self._action, "recovery-action-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            event = connection.execute(
                """
                SELECT incident_event_id FROM m1_incident_events
                WHERE incident_key = %s AND kind = 'recovered'
                ORDER BY occurred_at DESC LIMIT 1
                """,
                (action.incident_key,),
            ).fetchone()
        if event is None:
            raise DisposableCommissioningError("heartbeat-recovery-event-missing")
        self._recovery_event_id = str(event[0])
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{self._recovery_event_id}",
            occurred_at=completed_at + timedelta(seconds=1),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        proof = self._need(self._recovered_proof, "recovery-proof-missing")
        action = self._need(self._action, "recovery-action-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            state = connection.execute(
                """
                SELECT i.state, i.resolved_at IS NOT NULL, a.state, a.result_code,
                       r.last_heartbeat_at, count(ja.attempt_id), min(ja.state)
                FROM m1_incidents AS i
                JOIN m1_recovery_actions AS a ON a.incident_key = i.incident_key
                JOIN m1_job_runtime_state AS r ON r.job_key = a.target_id
                JOIN m1_job_attempts AS ja ON ja.job_key = a.target_id
                WHERE i.incident_key = %s AND a.action_id = %s
                GROUP BY i.state, i.resolved_at, a.state, a.result_code,
                         r.last_heartbeat_at
                """,
                (action.incident_key, action.action_id),
            ).fetchone()
            attempt = connection.execute(
                """
                SELECT attempt_id, lease_epoch FROM m1_job_attempts
                WHERE attempt_id = %s
                """,
                (proof["attempt_id"],),
            ).fetchone()
        if state != (
            "resolved",
            True,
            "completed",
            "succeeded",
            self._renewed_at,
            1,
            "succeeded",
        ):
            raise DisposableCommissioningError("heartbeat-recovery-not-closed")
        if attempt != (self._attempt_id_value, prepared.lease.lease_epoch):
            raise DisposableCommissioningError("heartbeat-attempt-identity-changed")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=proof["postcondition_fact_id"],
            occurred_at=datetime.fromisoformat(proof["succeeded_at"]) + timedelta(seconds=2),
        )


class WorkerExitCommissioningAdapter:
    """Prove lease-bound worker loss reclaim and successor business recovery."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._prepared: PreparedNormalTurn | None = None
        self._attempt_id_value: str | None = None
        self._profile: RuntimeDeadlineProfile | None = None
        self._channels: tuple[str, ...] | None = None
        self._last_heartbeat_at: datetime | None = None
        self._lease_expires_at: datetime | None = None
        self._detected_at: datetime | None = None
        self._controller: RuntimeControllerLease | None = None
        self._action: RecoveryActionRecord | None = None
        self._replacement: JobLease | None = None
        self._recovered_proof: dict[str, str] | None = None
        self._recovery_event_id: str | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if identity.attack_id != "worker-exit":
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _attempt_id(self, lease: JobLease) -> str:
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT attempt_id FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (lease.job_key, lease.lease_epoch),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("attempt-fact-missing")
        return str(row[0])

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = prepare_normal_turn(
            self._control_plane,
            node_id=identity.node_id,
            experiment_id=identity.experiment_id,
            now=self._started_at,
        )
        self._prepared = prepared
        self._attempt_id_value = self._attempt_id(prepared.lease)
        candidates = read_runtime_reconcile_states(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=f"commissioning:worker-exit:{identity.node_id}",
            now=self._started_at,
            target_id=prepared.lease.job_key,
            sample_limit=1,
        )
        if len(candidates) != 1:
            raise DisposableCommissioningError("persisted-runtime-profile-missing")
        runtime = candidates[0].runtime_state
        self._profile = runtime.profile
        self._channels = candidates[0].channels
        self._last_heartbeat_at = runtime.last_heartbeat_at
        self._lease_expires_at = runtime.lease_expires_at
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"attempt:{self._attempt_id_value}",
            occurred_at=runtime.last_heartbeat_at,
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        last_heartbeat_at = self._need(
            self._last_heartbeat_at, "last-heartbeat-missing"
        )
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"attempt:{self._attempt_id_value}",
            occurred_at=last_heartbeat_at + timedelta(microseconds=1),
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        lease_expires_at = self._need(
            self._lease_expires_at, "lease-deadline-missing"
        )
        before_expiry = lease_expires_at - timedelta(microseconds=1)
        before_candidates = read_runtime_reconcile_states(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=f"commissioning:worker-exit:{identity.node_id}",
            now=before_expiry,
            target_id=prepared.lease.job_key,
            sample_limit=1,
        )
        if len(before_candidates) != 1:
            raise DisposableCommissioningError("pre-expiry-candidate-missing")
        before = RuntimeReconciler().evaluate(
            before_candidates[0].runtime_state,
            now=before_expiry,
        )
        if before.action is not None or before.reason_code != "job.heartbeat-missing-fence":
            raise DisposableCommissioningError(
                f"worker-exit-reclaimed-before-expiry:{before.reason_code}"
            )

        detected_at = lease_expires_at + timedelta(microseconds=1)
        controller = claim_controller(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=f"commissioning:worker-exit:{identity.node_id}",
            owner_id=f"commissioning:reclaim-controller:{identity.node_id}",
            lease_seconds=profile.lease_seconds,
            now=detected_at,
        )
        candidates = read_runtime_reconcile_states(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=controller.controller_id,
            now=detected_at,
            target_id=prepared.lease.job_key,
            sample_limit=1,
        )
        if len(candidates) != 1:
            raise DisposableCommissioningError("expired-worker-candidate-missing")
        candidate = candidates[0]
        decision = RuntimeReconciler().evaluate(candidate.runtime_state, now=detected_at)
        if (
            decision.action is not RecoveryActionType.RECLAIM_JOB
            or decision.reason_code != "job.heartbeat-missing"
            or decision.incident_severity != "critical"
            or not decision.qualification_breaking
        ):
            raise DisposableCommissioningError(
                f"worker-exit-misclassified:{decision.reason_code}"
            )
        action = schedule_action(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller=controller,
            decision=decision,
            incident_key=candidate.incident_key,
            component=candidate.component,
            target_type=candidate.target_type,
            target_id=candidate.target_id,
            recovery_episode_key=candidate.runtime_state.recovery_episode_key,
            expected_attempt_id=candidate.runtime_state.attempt_id,
            expected_lease_epoch=candidate.runtime_state.lease_epoch,
            recovery_budget_remaining=candidate.runtime_state.recovery_budget.remaining_actions,
            cooldown_seconds=candidate.cooldown_seconds,
            channels=candidate.channels,
            now=detected_at,
        )
        if action.state != "pending" or action.incident_key != candidate.incident_key:
            raise DisposableCommissioningError("worker-exit-recovery-not-scheduled")
        self._detected_at = detected_at
        self._controller = controller
        self._action = action
        return AttackStageReceipt(
            stage="detected",
            receipt_id=f"incident:{candidate.incident_key}",
            occurred_at=detected_at,
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        profile = self._need(self._profile, "runtime-profile-missing")
        detected_at = self._need(self._detected_at, "detection-time-missing")
        controller = self._need(self._controller, "controller-missing")
        action = self._need(self._action, "recovery-action-missing")
        started_at = detected_at + timedelta(microseconds=1)
        result = RecoveryExecutor(
            connection_factory=self._control_plane._connection_factory,  # noqa: SLF001
            control_plane=self._control_plane,
            controller=controller,
            worker_id=f"commissioning:reclaim-worker:{identity.node_id}",
            action_lease_seconds=profile.heartbeat_seconds,
            heartbeat_lease_seconds=profile.heartbeat_seconds,
        ).run_once(
            now=started_at,
            expected_action_id=action.action_id,
        )
        if result is None or result.action_id != action.action_id or result.outcome != "succeeded":
            raise DisposableCommissioningError("worker-exit-reclaim-failed")
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"action:{action.action_id}",
            occurred_at=started_at,
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        detected_at = self._need(self._detected_at, "detection-time-missing")
        cleanup_at = detected_at + timedelta(microseconds=2)
        try:
            prepared.complete(lease=prepared.lease, now=cleanup_at)
        except StaleLeaseError:
            pass
        else:
            raise DisposableCommissioningError("exited-worker-not-fenced")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            state = connection.execute(
                """
                SELECT j.state, j.lease_owner, a.state, a.error_class,
                       count(e.event_id)
                FROM m1_jobs AS j
                JOIN m1_job_attempts AS a
                  ON a.job_key = j.job_key AND a.lease_epoch = j.lease_epoch
                LEFT JOIN m1_job_runtime_events AS e
                  ON e.attempt_id = a.attempt_id AND e.kind = 'job.succeeded'
                WHERE j.job_key = %s
                GROUP BY j.state, j.lease_owner, a.state, a.error_class
                """,
                (prepared.lease.job_key,),
            ).fetchone()
        if state != (
            "retryable",
            None,
            "retryable",
            "RecoveryLeaseExpired",
            0,
        ):
            raise DisposableCommissioningError("worker-exit-cleanup-not-fenced")
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"attempt:{self._attempt_id_value}",
            occurred_at=cleanup_at,
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        detected_at = self._need(self._detected_at, "detection-time-missing")
        replacement = self._control_plane.claim_job(
            worker_id=f"commissioning:replacement-worker:{identity.node_id}",
            job_types=(identity.node_id,),
            lease_seconds=profile.lease_seconds,
            now=detected_at + timedelta(microseconds=3),
        )
        if (
            replacement is None
            or replacement.job_key != prepared.lease.job_key
            or replacement.lease_epoch != prepared.lease.lease_epoch + 1
        ):
            raise DisposableCommissioningError("replacement-claim-mismatch")
        self._replacement = replacement
        completed_at = detected_at + timedelta(seconds=5)
        self._recovered_proof = prepared.complete(lease=replacement, now=completed_at)
        recovered = self._control_plane.record_job_recovery(
            replacement,
            component=identity.node_id,
            channels=self._need(self._channels, "recovery-channels-missing"),
            now=completed_at + timedelta(seconds=1),
        )
        if not recovered:
            raise DisposableCommissioningError("worker-exit-incident-not-resolved")
        action = self._need(self._action, "recovery-action-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            event = connection.execute(
                """
                SELECT incident_event_id FROM m1_incident_events
                WHERE incident_key = %s AND kind = 'recovered'
                ORDER BY occurred_at DESC LIMIT 1
                """,
                (action.incident_key,),
            ).fetchone()
        if event is None:
            raise DisposableCommissioningError("worker-exit-recovery-event-missing")
        self._recovery_event_id = str(event[0])
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{self._recovery_event_id}",
            occurred_at=completed_at + timedelta(seconds=1),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        proof = self._need(self._recovered_proof, "recovery-proof-missing")
        replacement = self._need(self._replacement, "replacement-missing")
        action = self._need(self._action, "recovery-action-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            state = connection.execute(
                """
                SELECT i.state, i.resolved_at IS NOT NULL, a.state, a.result_code
                FROM m1_incidents AS i
                JOIN m1_recovery_actions AS a ON a.incident_key = i.incident_key
                WHERE i.incident_key = %s AND a.action_id = %s
                """,
                (action.incident_key, action.action_id),
            ).fetchone()
            attempt = connection.execute(
                "SELECT lease_epoch FROM m1_job_attempts WHERE attempt_id = %s",
                (proof["attempt_id"],),
            ).fetchone()
        if state != ("resolved", True, "completed", "succeeded"):
            raise DisposableCommissioningError("worker-exit-recovery-not-closed")
        if attempt != (replacement.lease_epoch,):
            raise DisposableCommissioningError("worker-exit-successor-proof-mismatch")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=proof["postcondition_fact_id"],
            occurred_at=datetime.fromisoformat(proof["succeeded_at"]) + timedelta(seconds=2),
        )


class ProgressStallCommissioningAdapter:
    """Prove live-lease progress-stall cancellation and successor recovery."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._prepared: PreparedNormalTurn | None = None
        self._live_lease: JobLease | None = None
        self._controller: RuntimeControllerLease | None = None
        self._action: RecoveryActionRecord | None = None
        self._old_attempt_id: str | None = None
        self._checkpoint_event_id: str | None = None
        self._checkpoint_at: datetime | None = None
        self._profile: RuntimeDeadlineProfile | None = None
        self._channels: tuple[str, ...] | None = None
        self._detected_at: datetime | None = None
        self._replacement: JobLease | None = None
        self._recovered_proof: dict[str, str] | None = None
        self._recovery_event_id: str | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if identity.attack_id != "progress-stall":
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _attempt_id(self, lease: JobLease) -> str:
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT attempt_id FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (lease.job_key, lease.lease_epoch),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("attempt-fact-missing")
        return str(row[0])

    def _assert_old_owner_cancelled(self) -> None:
        prepared = self._need(self._prepared, "preflight-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            job = connection.execute(
                """
                SELECT state, lease_owner FROM m1_jobs WHERE job_key = %s
                """,
                (prepared.lease.job_key,),
            ).fetchone()
            attempt = connection.execute(
                """
                SELECT state, error_class FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (prepared.lease.job_key, prepared.lease.lease_epoch),
            ).fetchone()
            stale_success = connection.execute(
                """
                SELECT count(*) FROM m1_job_runtime_events
                WHERE job_key = %s AND lease_epoch = %s AND kind = %s
                """,
                (
                    prepared.lease.job_key,
                    prepared.lease.lease_epoch,
                    RuntimeEventKind.SUCCEEDED.value,
                ),
            ).fetchone()
        if job != ("retryable", None):
            raise DisposableCommissioningError("stalled-job-not-retryable")
        if attempt != ("retryable", "RecoveryProgressStalled"):
            raise DisposableCommissioningError("stalled-attempt-not-cancelled")
        if stale_success != (0,):
            raise DisposableCommissioningError("stalled-owner-terminal-effect")

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = prepare_normal_turn(
            self._control_plane,
            node_id=identity.node_id,
            experiment_id=identity.experiment_id,
            now=self._started_at,
        )
        self._prepared = prepared
        self._old_attempt_id = self._attempt_id(prepared.lease)
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            checkpoint = connection.execute(
                """
                SELECT event_id, occurred_at
                FROM m1_job_runtime_events
                WHERE job_key = %s AND lease_epoch = %s
                  AND progress_sequence IS NOT NULL
                ORDER BY event_sequence DESC
                LIMIT 1
                """,
                (prepared.lease.job_key, prepared.lease.lease_epoch),
            ).fetchone()
        if checkpoint is None:
            raise DisposableCommissioningError("progress-checkpoint-missing")
        self._checkpoint_event_id = str(checkpoint[0])
        checkpoint_at = checkpoint[1].astimezone(UTC)
        self._checkpoint_at = checkpoint_at
        candidates = read_runtime_reconcile_states(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=f"commissioning:progress-stall:{identity.node_id}",
            now=checkpoint_at,
            target_id=prepared.lease.job_key,
            sample_limit=1,
        )
        if len(candidates) != 1:
            raise DisposableCommissioningError("persisted-runtime-profile-missing")
        self._profile = candidates[0].runtime_state.profile
        self._channels = candidates[0].channels
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"event:{self._checkpoint_event_id}",
            occurred_at=checkpoint_at,
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        checkpoint_at = self._need(self._checkpoint_at, "checkpoint-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        heartbeat_at = prepared.lease.lease_expires_at - timedelta(
            seconds=profile.heartbeat_seconds
        )
        self._live_lease = self._control_plane.heartbeat_runtime_attempt(
            prepared.lease,
            now=heartbeat_at,
            lease_seconds=profile.lease_seconds,
        )
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"event:{self._checkpoint_event_id}",
            occurred_at=max(checkpoint_at + timedelta(microseconds=1), heartbeat_at),
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        live_lease = self._need(self._live_lease, "live-lease-missing")
        checkpoint_at = self._need(self._checkpoint_at, "checkpoint-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        detected_at = checkpoint_at + timedelta(
            seconds=profile.progress_seconds,
            microseconds=1,
        )
        if detected_at >= live_lease.lease_expires_at:
            raise DisposableCommissioningError("progress-deadline-not-inside-live-lease")
        self._detected_at = detected_at
        controller = claim_controller(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=f"commissioning:progress-stall:{identity.node_id}",
            owner_id=f"commissioning:controller:{identity.node_id}",
            lease_seconds=profile.lease_seconds,
            now=detected_at,
        )
        candidates = read_runtime_reconcile_states(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=controller.controller_id,
            now=detected_at,
            target_id=live_lease.job_key,
            sample_limit=1,
        )
        if len(candidates) != 1:
            raise DisposableCommissioningError("progress-candidate-missing")
        candidate = candidates[0]
        decision = RuntimeReconciler().evaluate(candidate.runtime_state, now=detected_at)
        if (
            decision.action is not RecoveryActionType.CANCEL_JOB
            or decision.reason_code != "job.progress-stalled"
            or decision.qualification_breaking
        ):
            raise DisposableCommissioningError(
                f"progress-stall-misclassified:{decision.reason_code}"
            )
        action = schedule_action(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller=controller,
            decision=decision,
            incident_key=candidate.incident_key,
            component=candidate.component,
            target_type=candidate.target_type,
            target_id=candidate.target_id,
            recovery_episode_key=candidate.runtime_state.recovery_episode_key,
            expected_attempt_id=candidate.runtime_state.attempt_id,
            expected_lease_epoch=candidate.runtime_state.lease_epoch,
            recovery_budget_remaining=(
                candidate.runtime_state.recovery_budget.remaining_actions
            ),
            cooldown_seconds=candidate.cooldown_seconds,
            channels=candidate.channels,
            now=detected_at,
        )
        if action.state != "pending" or action.incident_key != candidate.incident_key:
            raise DisposableCommissioningError("progress-recovery-not-scheduled")
        self._controller = controller
        self._action = action
        return AttackStageReceipt(
            stage="detected",
            receipt_id=f"incident:{candidate.incident_key}",
            occurred_at=detected_at,
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        controller = self._need(self._controller, "controller-missing")
        action = self._need(self._action, "recovery-action-missing")
        detected_at = self._need(self._detected_at, "detection-time-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        result = RecoveryExecutor(
            connection_factory=self._control_plane._connection_factory,  # noqa: SLF001
            control_plane=self._control_plane,
            controller=controller,
            worker_id=f"commissioning:recovery:{identity.node_id}",
            action_lease_seconds=profile.heartbeat_seconds,
            heartbeat_lease_seconds=profile.heartbeat_seconds,
        ).run_once(
            now=detected_at + timedelta(seconds=1),
            expected_action_id=action.action_id,
        )
        if result is None or result.action_id != action.action_id or result.outcome != "succeeded":
            raise DisposableCommissioningError("progress-recovery-execution-failed")
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"action:{action.action_id}",
            occurred_at=detected_at + timedelta(seconds=1),
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        detected_at = self._need(self._detected_at, "detection-time-missing")
        try:
            prepared.complete(
                lease=prepared.lease,
                now=detected_at + timedelta(seconds=2),
            )
        except StaleLeaseError:
            pass
        else:
            raise DisposableCommissioningError("cancelled-owner-not-fenced")
        self._assert_old_owner_cancelled()
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"attempt:{self._old_attempt_id}",
            occurred_at=detected_at + timedelta(seconds=2),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        detected_at = self._need(self._detected_at, "detection-time-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            retry = connection.execute(
                "SELECT next_attempt_at FROM m1_jobs WHERE job_key = %s",
                (prepared.lease.job_key,),
            ).fetchone()
        if retry is None or retry[0] is None:
            raise DisposableCommissioningError("retry-deadline-missing")
        claim_at = max(retry[0].astimezone(UTC), detected_at + timedelta(seconds=3))
        replacement = self._control_plane.claim_job(
            worker_id=f"commissioning:successor:{identity.node_id}",
            job_types=(identity.node_id,),
            lease_seconds=self._need(self._profile, "runtime-profile-missing").lease_seconds,
            now=claim_at,
        )
        if (
            replacement is None
            or replacement.job_key != prepared.lease.job_key
            or replacement.lease_epoch != prepared.lease.lease_epoch + 1
        ):
            raise DisposableCommissioningError("successor-claim-mismatch")
        self._replacement = replacement
        completed_at = claim_at + timedelta(seconds=5)
        self._recovered_proof = prepared.complete(lease=replacement, now=completed_at)
        recovered = self._control_plane.record_job_recovery(
            replacement,
            component=identity.node_id,
            channels=self._need(self._channels, "recovery-channels-missing"),
            now=completed_at + timedelta(seconds=1),
        )
        if not recovered:
            raise DisposableCommissioningError("recovery-incident-not-resolved")
        action = self._need(self._action, "recovery-action-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            event = connection.execute(
                """
                SELECT incident_event_id FROM m1_incident_events
                WHERE incident_key = %s AND kind = 'recovered'
                ORDER BY occurred_at DESC LIMIT 1
                """,
                (action.incident_key,),
            ).fetchone()
        if event is None:
            raise DisposableCommissioningError("recovery-event-missing")
        self._recovery_event_id = str(event[0])
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{self._recovery_event_id}",
            occurred_at=completed_at + timedelta(seconds=1),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        proof = self._need(self._recovered_proof, "recovery-proof-missing")
        replacement = self._need(self._replacement, "successor-missing")
        action = self._need(self._action, "recovery-action-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            state = connection.execute(
                """
                SELECT i.state, i.resolved_at IS NOT NULL, a.state, a.result_code
                FROM m1_incidents AS i
                JOIN m1_recovery_actions AS a ON a.incident_key = i.incident_key
                WHERE i.incident_key = %s AND a.action_id = %s
                """,
                (action.incident_key, action.action_id),
            ).fetchone()
            attempt = connection.execute(
                "SELECT lease_epoch FROM m1_job_attempts WHERE attempt_id = %s",
                (proof["attempt_id"],),
            ).fetchone()
        if state != ("resolved", True, "completed", "succeeded"):
            raise DisposableCommissioningError("progress-recovery-not-closed")
        if attempt != (replacement.lease_epoch,):
            raise DisposableCommissioningError("successor-proof-mismatch")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=proof["postcondition_fact_id"],
            occurred_at=datetime.fromisoformat(proof["succeeded_at"]) + timedelta(seconds=2),
        )


class RetryBudgetCommissioningAdapter:
    """Prove exact retry exhaustion, fenced probe release, and business recovery."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._prepared: PreparedNormalTurn | None = None
        self._profile: RuntimeDeadlineProfile | None = None
        self._channels: tuple[str, ...] | None = None
        self._attempt_ids: list[str] = []
        self._retry_incident_key: str | None = None
        self._probe_due_at: datetime | None = None
        self._injected_at: datetime | None = None
        self._controller: RuntimeControllerLease | None = None
        self._action: RecoveryActionRecord | None = None
        self._replacement: JobLease | None = None
        self._recovered_proof: dict[str, str] | None = None
        self._recovery_event_id: str | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if identity.attack_id != "retry-budget-exhaustion":
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _attempt_id(self, lease: JobLease) -> str:
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT attempt_id FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (lease.job_key, lease.lease_epoch),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("attempt-fact-missing")
        return str(row[0])

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = prepare_normal_turn(
            self._control_plane,
            node_id=identity.node_id,
            experiment_id=identity.experiment_id,
            now=self._started_at,
        )
        self._prepared = prepared
        first_attempt_id = self._attempt_id(prepared.lease)
        self._attempt_ids.append(first_attempt_id)
        candidates = read_runtime_reconcile_states(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=f"commissioning:retry-budget:{identity.node_id}",
            now=self._started_at,
            target_id=prepared.lease.job_key,
            sample_limit=1,
        )
        if len(candidates) != 1:
            raise DisposableCommissioningError("persisted-runtime-profile-missing")
        self._profile = candidates[0].runtime_state.profile
        self._channels = candidates[0].channels
        self._retry_incident_key = f"commissioning:retry-budget:{prepared.lease.job_key}"
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"attempt:{first_attempt_id}",
            occurred_at=candidates[0].runtime_state.last_progress_at,
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        channels = self._need(self._channels, "recovery-channels-missing")
        incident_key = self._need(self._retry_incident_key, "retry-incident-missing")
        retry_budget = runtime_retry_policy(identity.node_id).retry_budget
        current = prepared.lease
        last_failure_at: datetime | None = None
        for failure_number in range(1, retry_budget + 1):
            failure_at = current.lease_expires_at - timedelta(
                seconds=profile.heartbeat_seconds
            )
            next_attempt_at = self._control_plane.finish_retryable_with_incident(
                current,
                error_class="CommissioningValidationFault",
                incident_key=incident_key,
                dedupe_key=f"job-retry:{current.job_key}",
                component=identity.node_id,
                summary=f"{identity.node_id} commissioning retry budget fault",
                detail={"job_key": current.job_key, "stage": identity.node_id},
                channels=channels,
                now=failure_at,
            )
            last_failure_at = failure_at
            if failure_number == retry_budget:
                self._probe_due_at = next_attempt_at
                break
            replacement = self._control_plane.claim_job(
                worker_id=f"commissioning:retry:{identity.node_id}:{failure_number + 1}",
                job_types=(identity.node_id,),
                lease_seconds=profile.lease_seconds,
                now=next_attempt_at,
            )
            if (
                replacement is None
                or replacement.job_key != prepared.lease.job_key
                or replacement.lease_epoch != current.lease_epoch + 1
            ):
                raise DisposableCommissioningError("retry-attempt-claim-mismatch")
            current = replacement
            self._attempt_ids.append(self._attempt_id(current))
            _record_progress(self._control_plane, current, next_attempt_at)
        self._injected_at = self._need(last_failure_at, "retry-failure-missing")
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"attempt:{self._attempt_ids[-1]}",
            occurred_at=self._injected_at,
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        incident_key = self._need(self._retry_incident_key, "retry-incident-missing")
        injected_at = self._need(self._injected_at, "injection-time-missing")
        retry_budget = runtime_retry_policy(identity.node_id).retry_budget
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            circuit = connection.execute(
                """
                SELECT consecutive_failures, state, next_probe_at
                FROM m1_job_circuits WHERE job_key = %s
                """,
                (prepared.lease.job_key,),
            ).fetchone()
            incident = connection.execute(
                """
                SELECT incident_key, state, count(*) OVER ()
                FROM m1_incidents WHERE dedupe_key = %s
                """,
                (f"job-retry:{prepared.lease.job_key}",),
            ).fetchone()
        if circuit != (retry_budget, "open", self._probe_due_at):
            raise DisposableCommissioningError("retry-circuit-not-open")
        if incident != (incident_key, "open", 1):
            raise DisposableCommissioningError("retry-incident-not-deduplicated")
        return AttackStageReceipt(
            stage="detected",
            receipt_id=f"incident:{incident_key}",
            occurred_at=injected_at + timedelta(microseconds=1),
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        probe_due_at = self._need(self._probe_due_at, "probe-deadline-missing")
        blocked = self._control_plane.claim_job(
            worker_id=f"commissioning:blocked-worker:{identity.node_id}",
            job_types=(identity.node_id,),
            lease_seconds=profile.lease_seconds,
            now=probe_due_at,
        )
        if blocked is not None:
            raise DisposableCommissioningError("open-circuit-worker-not-blocked")
        controller = claim_controller(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=f"commissioning:retry-budget:{identity.node_id}",
            owner_id=f"commissioning:probe-controller:{identity.node_id}",
            lease_seconds=profile.lease_seconds,
            now=probe_due_at,
        )
        candidates = read_runtime_reconcile_states(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller_id=controller.controller_id,
            now=probe_due_at,
            target_id=prepared.lease.job_key,
            sample_limit=1,
        )
        if len(candidates) != 1:
            raise DisposableCommissioningError("probe-candidate-missing")
        candidate = candidates[0]
        decision = RuntimeReconciler().evaluate(candidate.runtime_state, now=probe_due_at)
        if (
            decision.action is not RecoveryActionType.PROBE_CIRCUIT
            or decision.reason_code != "circuit.probe-due"
            or decision.qualification_breaking
        ):
            raise DisposableCommissioningError(f"probe-misclassified:{decision.reason_code}")
        action = schedule_action(
            self._control_plane._connection_factory,  # noqa: SLF001
            controller=controller,
            decision=decision,
            incident_key=candidate.incident_key,
            component=candidate.component,
            target_type=candidate.target_type,
            target_id=candidate.target_id,
            recovery_episode_key=candidate.runtime_state.recovery_episode_key,
            expected_attempt_id=candidate.runtime_state.attempt_id,
            expected_lease_epoch=candidate.runtime_state.lease_epoch,
            recovery_budget_remaining=candidate.runtime_state.recovery_budget.remaining_actions,
            cooldown_seconds=candidate.cooldown_seconds,
            channels=candidate.channels,
            now=probe_due_at,
        )
        result = RecoveryExecutor(
            connection_factory=self._control_plane._connection_factory,  # noqa: SLF001
            control_plane=self._control_plane,
            controller=controller,
            worker_id=f"commissioning:probe-executor:{identity.node_id}",
            action_lease_seconds=profile.heartbeat_seconds,
            heartbeat_lease_seconds=profile.heartbeat_seconds,
        ).run_once(
            now=probe_due_at + timedelta(microseconds=1),
            expected_action_id=action.action_id,
        )
        if result is None or result.action_id != action.action_id or result.outcome != "succeeded":
            raise DisposableCommissioningError("probe-action-execution-failed")
        self._controller = controller
        self._action = action
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"action:{action.action_id}",
            occurred_at=probe_due_at + timedelta(microseconds=1),
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        probe_due_at = self._need(self._probe_due_at, "probe-deadline-missing")
        try:
            prepared.complete(
                lease=prepared.lease,
                now=probe_due_at + timedelta(microseconds=2),
            )
        except StaleLeaseError:
            pass
        else:
            raise DisposableCommissioningError("failed-retry-owner-not-fenced")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            stale_successes = connection.execute(
                """
                SELECT count(*) FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch <= %s AND state = 'succeeded'
                """,
                (prepared.lease.job_key, len(self._attempt_ids)),
            ).fetchone()
            job = connection.execute(
                "SELECT state, lease_owner FROM m1_jobs WHERE job_key = %s",
                (prepared.lease.job_key,),
            ).fetchone()
        if stale_successes != (0,) or job != ("retryable", None):
            raise DisposableCommissioningError("probe-cleanup-failed")
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"attempt:{self._attempt_ids[-1]}",
            occurred_at=probe_due_at + timedelta(microseconds=2),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        profile = self._need(self._profile, "runtime-profile-missing")
        probe_due_at = self._need(self._probe_due_at, "probe-deadline-missing")
        replacement = self._control_plane.claim_job(
            worker_id=f"commissioning:successful-probe:{identity.node_id}",
            job_types=(identity.node_id,),
            lease_seconds=profile.lease_seconds,
            now=probe_due_at + timedelta(microseconds=3),
        )
        if (
            replacement is None
            or replacement.job_key != prepared.lease.job_key
            or replacement.lease_epoch != len(self._attempt_ids) + 1
        ):
            raise DisposableCommissioningError("successful-probe-claim-mismatch")
        self._replacement = replacement
        completed_at = probe_due_at + timedelta(seconds=5)
        self._recovered_proof = prepared.complete(lease=replacement, now=completed_at)
        recovered = self._control_plane.record_job_recovery(
            replacement,
            component=identity.node_id,
            channels=self._need(self._channels, "recovery-channels-missing"),
            now=completed_at + timedelta(seconds=1),
        )
        if not recovered:
            raise DisposableCommissioningError("probe-incidents-not-resolved")
        action = self._need(self._action, "probe-action-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            event = connection.execute(
                """
                SELECT incident_event_id FROM m1_incident_events
                WHERE incident_key = %s AND kind = 'recovered'
                ORDER BY occurred_at DESC LIMIT 1
                """,
                (action.incident_key,),
            ).fetchone()
        if event is None:
            raise DisposableCommissioningError("probe-recovery-event-missing")
        self._recovery_event_id = str(event[0])
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{self._recovery_event_id}",
            occurred_at=completed_at + timedelta(seconds=1),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "preflight-missing")
        replacement = self._need(self._replacement, "successful-probe-missing")
        proof = self._need(self._recovered_proof, "recovery-proof-missing")
        action = self._need(self._action, "probe-action-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            state = connection.execute(
                """
                SELECT c.consecutive_failures, c.state, c.next_probe_at,
                       c.failure_fingerprint, i.state, i.resolved_at IS NOT NULL,
                       a.state, a.result_code
                FROM m1_job_circuits AS c
                JOIN m1_incidents AS i ON i.dedupe_key = %s
                JOIN m1_recovery_actions AS a ON a.action_id = %s
                WHERE c.job_key = %s
                """,
                (f"job-retry:{prepared.lease.job_key}", action.action_id, prepared.lease.job_key),
            ).fetchone()
            attempt = connection.execute(
                "SELECT lease_epoch FROM m1_job_attempts WHERE attempt_id = %s",
                (proof["attempt_id"],),
            ).fetchone()
        if state != (0, "closed", None, None, "resolved", True, "completed", "succeeded"):
            raise DisposableCommissioningError("probe-recovery-not-closed")
        if attempt != (replacement.lease_epoch,):
            raise DisposableCommissioningError("probe-successor-proof-mismatch")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=proof["postcondition_fact_id"],
            occurred_at=datetime.fromisoformat(proof["succeeded_at"]) + timedelta(seconds=2),
        )


class R2ReadTimeoutCommissioningAdapter:
    """Fail one exact immutable GET, then retry the same node input durably."""

    _READ_STAGES = {
        "structure-materialize": "read-page-receipts",
        "structure-normalize": "read-range",
        "structure-certify": "verify-parity",
        "quote-admit": "read-manifest",
        "quote-certify": "verify-batches",
        "opportunity-certify": "read-current-quote",
    }

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._objects = _DisposableObjectStore()
        self._prepared: PreparedNormalTurn | None = None
        self._replacement: JobLease | None = None
        self._artifact_key: str | None = None
        self._artifact_digest: str | None = None
        self._retry_due_at: datetime | None = None
        self._failure_event_id: str | None = None
        self._recovered_proof: dict[str, str] | None = None

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _require_identity(self, identity: AttackIdentity) -> None:
        if (
            identity.attack_id != "r2-read-timeout"
            or identity.node_id not in self._READ_STAGES
        ):
            raise DisposableCommissioningError("wrong-attack-adapter")

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = prepare_normal_turn(
            self._control_plane,
            node_id=identity.node_id,
            experiment_id=identity.experiment_id,
            now=self._started_at,
            progress_through=self._READ_STAGES[identity.node_id],
        )
        payload = (
            f"{identity.node_id}:{prepared.lease.input_identity}:immutable-r2-input\n"
        ).encode()
        digest = sha256(payload).hexdigest()
        key = f"commissioning/r2-read/{digest}/artifact.bin"
        self._objects.restore(key=key, payload=payload, digest=digest)
        self._prepared = prepared
        self._artifact_key = key
        self._artifact_digest = digest
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT attempt_id FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (prepared.lease.job_key, prepared.lease.lease_epoch),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("read-timeout-attempt-missing")
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"attempt:{row[0]}:artifact:{key}:{digest}",
            occurred_at=self._started_at,
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        key = self._need(self._artifact_key, "read-timeout-key-missing")
        self._objects.arm_read_timeout(key)
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"r2:get:{key}:ReadTimeoutError",
            occurred_at=self._started_at + timedelta(seconds=1),
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "read-timeout-turn-missing")
        key = self._need(self._artifact_key, "read-timeout-key-missing")
        digest = self._need(self._artifact_digest, "read-timeout-digest-missing")
        try:
            self._objects.get_object(Bucket="commissioning-artifacts", Key=key)
        except ReadTimeoutError as error:
            failure_at = self._started_at + timedelta(seconds=2)
            self._retry_due_at = self._control_plane.finish_retryable_with_incident(
                prepared.lease,
                error_class=type(error).__name__,
                incident_key=f"incident:job-retry:{prepared.lease.job_key}",
                dedupe_key=f"job-retry:{prepared.lease.job_key}",
                component=identity.node_id,
                summary=f"{identity.node_id} R2 read timeout",
                detail={
                    "job_key": prepared.lease.job_key,
                    "lease_epoch": prepared.lease.lease_epoch,
                    "stage": self._READ_STAGES[identity.node_id],
                    "object_operation": "get",
                    "artifact_key": key,
                    "artifact_digest": digest,
                    "error_class": type(error).__name__,
                },
                channels=("dashboard",),
                now=failure_at,
            )
        else:
            raise DisposableCommissioningError("r2-read-timeout-not-injected")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT event_id FROM m1_job_runtime_events
                WHERE job_key = %s AND lease_epoch = %s
                  AND kind = 'job.retryable-failed'
                """,
                (prepared.lease.job_key, prepared.lease.lease_epoch),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("r2-read-timeout-event-missing")
        self._failure_event_id = str(row[0])
        return AttackStageReceipt(
            stage="detected",
            receipt_id=f"event:{self._failure_event_id}",
            occurred_at=self._started_at + timedelta(seconds=2),
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "read-timeout-turn-missing")
        due_at = self._need(self._retry_due_at, "read-timeout-retry-due-missing")
        replacement = self._control_plane.claim_job(
            worker_id=f"commissioning:r2-read-retry:{identity.node_id}",
            job_types=(identity.node_id,),
            lease_seconds=120,
            now=due_at,
        )
        if (
            replacement is None
            or replacement.job_key != prepared.lease.job_key
            or replacement.lease_epoch != prepared.lease.lease_epoch + 1
            or replacement.input_identity != prepared.lease.input_identity
        ):
            raise DisposableCommissioningError("r2-read-replacement-mismatch")
        self._replacement = replacement
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"retry:{replacement.job_key}:{replacement.lease_epoch}",
            occurred_at=due_at,
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "read-timeout-turn-missing")
        key = self._need(self._artifact_key, "read-timeout-key-missing")
        digest = self._need(self._artifact_digest, "read-timeout-digest-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            try:
                _postcondition_fact(connection, prepared.lease)
            except DisposableCommissioningError:
                pass
            else:
                raise DisposableCommissioningError("r2-read-partial-postcondition")
            attempt = connection.execute(
                """
                SELECT state, error_class FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (prepared.lease.job_key, prepared.lease.lease_epoch),
            ).fetchone()
        if attempt != ("retryable", "ReadTimeoutError"):
            raise DisposableCommissioningError("r2-read-failure-shape")
        if not self._objects.contains(key) or self._objects.read_count(key) != 1:
            raise DisposableCommissioningError("r2-read-cleanup-shape")
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"artifact:{key}:{digest}:unchanged",
            occurred_at=self._need(self._retry_due_at, "read-timeout-retry-due-missing")
            + timedelta(microseconds=1),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "read-timeout-turn-missing")
        replacement = self._need(self._replacement, "read-timeout-replacement-missing")
        key = self._need(self._artifact_key, "read-timeout-key-missing")
        digest = self._need(self._artifact_digest, "read-timeout-digest-missing")
        response = self._objects.get_object(Bucket="commissioning-artifacts", Key=key)
        body = response.get("Body")
        if not isinstance(body, BytesIO):
            raise DisposableCommissioningError("r2-read-recovery-body-missing")
        payload = body.read()
        if sha256(payload).hexdigest() != digest:
            raise DisposableCommissioningError("r2-read-recovery-digest-mismatch")
        completed_at = self._need(
            self._retry_due_at, "read-timeout-retry-due-missing"
        ) + timedelta(seconds=2)
        self._recovered_proof = prepared.complete(lease=replacement, now=completed_at)
        if not self._control_plane.record_job_recovery(
            replacement,
            component=identity.node_id,
            channels=("dashboard",),
            now=completed_at + timedelta(seconds=1),
        ):
            raise DisposableCommissioningError("r2-read-incident-not-resolved")
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{self._recovered_proof['success_fact_id']}",
            occurred_at=completed_at + timedelta(seconds=1),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        prepared = self._need(self._prepared, "read-timeout-turn-missing")
        replacement = self._need(self._replacement, "read-timeout-replacement-missing")
        proof = self._need(self._recovered_proof, "read-timeout-proof-missing")
        key = self._need(self._artifact_key, "read-timeout-key-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            state = connection.execute(
                """
                SELECT job.input_identity, circuit.consecutive_failures, circuit.state,
                       circuit.next_probe_at, incident.state,
                       count(runtime.event_id) FILTER (
                         WHERE runtime.kind = 'job.succeeded'
                       ),
                       (SELECT failed.stage FROM m1_job_runtime_events AS failed
                        WHERE failed.job_key = job.job_key
                          AND failed.lease_epoch = %s
                          AND failed.kind = 'job.retryable-failed')
                FROM m1_jobs AS job
                JOIN m1_job_circuits AS circuit USING (job_key)
                JOIN m1_incidents AS incident
                  ON incident.dedupe_key = 'job-retry:' || job.job_key
                LEFT JOIN m1_job_runtime_events AS runtime USING (job_key)
                WHERE job.job_key = %s
                GROUP BY job.job_key, job.input_identity, circuit.consecutive_failures,
                         circuit.state, circuit.next_probe_at, incident.state
                """,
                (prepared.lease.lease_epoch, prepared.lease.job_key),
            ).fetchone()
        if state != (
            prepared.lease.input_identity,
            0,
            "closed",
            None,
            "resolved",
            1,
            self._READ_STAGES[identity.node_id],
        ):
            raise DisposableCommissioningError("r2-read-recovery-state")
        if replacement.input_identity != prepared.lease.input_identity:
            raise DisposableCommissioningError("r2-read-input-identity-changed")
        if self._objects.read_count(key) != 2:
            raise DisposableCommissioningError("r2-read-count-mismatch")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=proof["postcondition_fact_id"],
            occurred_at=datetime.fromisoformat(proof["succeeded_at"]) + timedelta(seconds=2),
        )


class SourceReceiptGapCommissioningAdapter:
    """Prove a missing source receipt gates, then releases, materialization."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._window_key: str | None = None
        self._withheld_lease: JobLease | None = None
        self._withheld_attempt_id: str | None = None
        self._materializer_lease: JobLease | None = None
        self._bundle: StructureBundleArtifact | None = None
        self._source_digest: str | None = None
        self._recovered_proof: dict[str, str] | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if (
            identity.attack_id != "source-receipt-gap"
            or identity.node_id != "structure-materialize"
        ):
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _attempt_id(self, lease: JobLease) -> str:
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT attempt_id FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (lease.job_key, lease.lease_epoch),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("withheld-attempt-missing")
        return str(row[0])

    def _assert_incomplete_barrier(self) -> str:
        window_key = self._need(self._window_key, "source-window-missing")
        withheld = self._need(self._withheld_lease, "withheld-lease-missing")
        try:
            self._control_plane.structure_source_window_digest(window_key)
        except IncompleteStructureGenerationError:
            pass
        else:
            raise DisposableCommissioningError("source-gap-not-detected")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            shape = connection.execute(
                """
                SELECT source_window.state,
                       count(DISTINCT input.job_key),
                       count(DISTINCT receipt.job_key),
                       array_agg(input.job_key ORDER BY input.job_key)
                           FILTER (WHERE receipt.job_key IS NULL),
                       count(DISTINCT materializer.job_key),
                       count(DISTINCT bundle.window_key),
                       (SELECT count(*) FROM m1_incidents),
                       (SELECT count(*) FROM m1_recovery_actions)
                FROM m1_structure_source_windows AS source_window
                JOIN m1_structure_source_page_inputs AS input
                  ON input.window_key = source_window.window_key
                LEFT JOIN m1_structure_source_page_receipts AS receipt
                  ON receipt.job_key = input.job_key
                LEFT JOIN m1_jobs AS materializer
                  ON materializer.job_key = source_window.window_key || ':materialize'
                LEFT JOIN m1_structure_source_window_bundles AS bundle
                  ON bundle.window_key = source_window.window_key
                WHERE source_window.window_key = %s
                GROUP BY source_window.state
                """,
                (window_key,),
            ).fetchone()
            attempt = connection.execute(
                """
                SELECT state FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (withheld.job_key, withheld.lease_epoch),
            ).fetchone()
        expected = (
            "events-complete",
            3,
            2,
            [withheld.job_key],
            0,
            0,
            0,
            0,
        )
        if shape != expected or attempt != ("running",):
            raise DisposableCommissioningError("source-gap-barrier-shape")
        return withheld.job_key

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        window_key = identity.experiment_id
        self._window_key = window_key
        self._control_plane.admit_structure_source_window(
            window_key=window_key,
            now=self._started_at,
        )
        event = _claim(self._control_plane, "structure-fetch", self._started_at)
        _record_progress(self._control_plane, event, self._started_at)
        self._control_plane.record_structure_source_page(
            event,
            artifact_key=f"structure-source/{identity.experiment_id}/events-0.json",
            artifact_digest=sha256(f"{identity.experiment_id}:events".encode()).hexdigest(),
            next_cursor=None,
            completed=True,
            record_count=1,
            market_batches=(("market-a",), ("market-b",)),
            now=self._started_at + timedelta(seconds=1),
        )
        first_market = _claim(
            self._control_plane,
            "structure-fetch",
            self._started_at + timedelta(seconds=2),
        )
        _record_progress(
            self._control_plane,
            first_market,
            self._started_at + timedelta(seconds=2),
        )
        self._control_plane.record_structure_source_page(
            first_market,
            artifact_key=f"structure-source/{identity.experiment_id}/markets-0.json",
            artifact_digest=sha256(f"{identity.experiment_id}:markets:0".encode()).hexdigest(),
            next_cursor=None,
            completed=True,
            record_count=1,
            now=self._started_at + timedelta(seconds=3),
        )
        withheld = _claim(
            self._control_plane,
            "structure-fetch",
            self._started_at + timedelta(seconds=4),
        )
        _record_progress(
            self._control_plane,
            withheld,
            self._started_at + timedelta(seconds=4),
        )
        self._withheld_lease = withheld
        self._withheld_attempt_id = self._attempt_id(withheld)
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"attempt:{self._withheld_attempt_id}",
            occurred_at=self._started_at + timedelta(seconds=4),
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        self._need(self._withheld_lease, "preflight-missing")
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"attempt:{self._withheld_attempt_id}",
            occurred_at=self._started_at + timedelta(seconds=5),
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        missing_job_key = self._assert_incomplete_barrier()
        return AttackStageReceipt(
            stage="detected",
            receipt_id=f"barrier:{identity.experiment_id}:missing:{missing_job_key}",
            occurred_at=self._started_at + timedelta(seconds=6),
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        self._assert_incomplete_barrier()
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"attempt:{self._withheld_attempt_id}",
            occurred_at=self._started_at + timedelta(seconds=7),
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        self._assert_incomplete_barrier()
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"barrier:{identity.experiment_id}:partial-publication-absent",
            occurred_at=self._started_at + timedelta(seconds=8),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        withheld = self._need(self._withheld_lease, "withheld-lease-missing")
        window_key = self._need(self._window_key, "source-window-missing")
        receipt_at = self._started_at + timedelta(seconds=9)
        self._control_plane.record_structure_source_page(
            withheld,
            artifact_key=f"structure-source/{identity.experiment_id}/markets-1.json",
            artifact_digest=sha256(f"{identity.experiment_id}:markets:1".encode()).hexdigest(),
            next_cursor=None,
            completed=True,
            record_count=1,
            now=receipt_at,
        )
        materializer = _claim(
            self._control_plane,
            "structure-materialize",
            receipt_at + timedelta(seconds=1),
        )
        self._materializer_lease = materializer
        _record_progress(
            self._control_plane,
            materializer,
            receipt_at + timedelta(seconds=1),
        )
        source_digest = self._control_plane.structure_source_window_digest(window_key)
        self._source_digest = source_digest
        bundle = StructureBundleArtifact.from_bytes(
            f'{{"kind":"{identity.attack_id}"}}\n'.encode()
        )
        self._bundle = bundle
        completed_at = receipt_at + timedelta(seconds=2)
        specs = self._control_plane.admit_structure_source_bundle(
            materializer,
            identity=_identity(
                identity.experiment_id,
                window_key,
                source_digest,
                "gamma-source-window-events-v3-sharded",
            ),
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=completed_at,
        )
        if len(specs) != 1:
            raise DisposableCommissioningError("source-gap-successor-shape")
        self._recovered_proof = _normal_turn_proof(self._control_plane, materializer)
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{self._recovered_proof['success_fact_id']}",
            occurred_at=completed_at,
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        window_key = self._need(self._window_key, "source-window-missing")
        materializer = self._need(self._materializer_lease, "materializer-lease-missing")
        bundle = self._need(self._bundle, "source-bundle-missing")
        source_digest = self._need(self._source_digest, "source-digest-missing")
        proof = self._need(self._recovered_proof, "recovery-proof-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            shape = connection.execute(
                """
                SELECT source_window.state,
                       count(DISTINCT input.job_key),
                       count(DISTINCT receipt.job_key),
                       count(DISTINCT bundle.window_key),
                       count(DISTINCT range_input.job_key),
                       (SELECT count(*) FROM m1_incidents),
                       (SELECT count(*) FROM m1_recovery_actions)
                FROM m1_structure_source_windows AS source_window
                JOIN m1_structure_source_page_inputs AS input
                  ON input.window_key = source_window.window_key
                LEFT JOIN m1_structure_source_page_receipts AS receipt
                  ON receipt.job_key = input.job_key
                LEFT JOIN m1_structure_source_window_bundles AS bundle
                  ON bundle.window_key = source_window.window_key
                LEFT JOIN m1_structure_range_inputs AS range_input
                  ON range_input.bundle_digest = bundle.bundle_digest
                WHERE source_window.window_key = %s
                GROUP BY source_window.state
                """,
                (window_key,),
            ).fetchone()
            attempt = connection.execute(
                """
                SELECT state FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (materializer.job_key, materializer.lease_epoch),
            ).fetchone()
        persisted_bundle = self._control_plane.structure_source_window_bundle(window_key)
        if shape != ("complete", 3, 3, 1, 1, 0, 0):
            raise DisposableCommissioningError("source-gap-recovery-shape")
        if attempt != ("succeeded",):
            raise DisposableCommissioningError("source-gap-materializer-not-succeeded")
        if persisted_bundle != {
            "source_digest": source_digest,
            "bundle_key": bundle.key,
            "bundle_digest": bundle.sha256,
        }:
            raise DisposableCommissioningError("source-gap-bundle-mismatch")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=proof["postcondition_fact_id"],
            occurred_at=datetime.fromisoformat(proof["succeeded_at"]) + timedelta(seconds=1),
        )


class QuoteBatchIncompleteCommissioningAdapter:
    """Prove one failed Quote batch blocks publication until fenced recovery."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._batches: tuple[QuoteBatchSpec, ...] = ()
        self._withheld_lease: JobLease | None = None
        self._withheld_attempt_id: str | None = None
        self._retry_due_at: datetime | None = None
        self._incident_key: str | None = None
        self._incident_event_id: str | None = None
        self._replacement: JobLease | None = None
        self._certifier: JobLease | None = None
        self._recovered_proof: dict[str, str] | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if (
            identity.attack_id != "quote-batch-incomplete"
            or identity.node_id != "quote-certify"
        ):
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _attempt_id(self, lease: JobLease) -> str:
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT attempt_id FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (lease.job_key, lease.lease_epoch),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("quote-batch-attempt-missing")
        return str(row[0])

    def _generation_key(self) -> str:
        if len(self._batches) != 2:
            raise DisposableCommissioningError("quote-batch-plan-missing")
        return self._batches[0].generation_key

    def _assert_incomplete_barrier(self) -> tuple[str, str]:
        generation_key = self._generation_key()
        withheld = self._need(self._withheld_lease, "withheld-quote-batch-missing")
        retry_due_at = self._need(self._retry_due_at, "quote-batch-retry-due-missing")
        incident_key = f"incident:job-retry:{withheld.job_key}"
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            shape = connection.execute(
                """
                SELECT
                  (SELECT count(*) FROM m1_quote_batch_inputs
                   WHERE job_key LIKE %s),
                  (SELECT count(*) FROM m1_quote_batch_receipts
                   WHERE job_key LIKE %s),
                  (SELECT state FROM m1_jobs WHERE job_key = %s),
                  (SELECT state FROM m1_jobs WHERE job_key = %s),
                  (SELECT count(*) FROM m1_generation_manifests
                   WHERE generation_key = %s),
                  (SELECT count(*) FROM m1_publication_pointers
                   WHERE pointer_key = 'quote:current'),
                  (SELECT count(*) FROM m1_jobs
                   WHERE job_key = %s)
                """,
                (
                    f"{generation_key}:batch:%",
                    f"{generation_key}:batch:%",
                    withheld.job_key,
                    f"{generation_key}:certify",
                    generation_key,
                    f"{generation_key}:opportunity-certify",
                ),
            ).fetchone()
            circuit = connection.execute(
                """
                SELECT consecutive_failures, state, next_probe_at
                FROM m1_job_circuits WHERE job_key = %s
                """,
                (withheld.job_key,),
            ).fetchone()
            incident = connection.execute(
                """
                SELECT incident.incident_key, incident.state, incident.component,
                       event.incident_event_id, event.kind,
                       outbox.channel, outbox.state
                FROM m1_incidents AS incident
                JOIN m1_incident_events AS event
                  ON event.incident_key = incident.incident_key
                JOIN m1_alert_outbox AS outbox
                  ON outbox.incident_event_id = event.incident_event_id
                WHERE incident.dedupe_key = %s
                """,
                (f"job-retry:{withheld.job_key}",),
            ).fetchone()
        if shape != (2, 1, "retryable", "waiting", 0, 0, 0):
            raise DisposableCommissioningError("quote-batch-incomplete-shape")
        if circuit != (1, "closed", retry_due_at):
            raise DisposableCommissioningError("quote-batch-retry-circuit-shape")
        if incident is None or incident[:3] != (incident_key, "open", "quote-batch"):
            raise DisposableCommissioningError("quote-batch-incident-shape")
        if incident[4:] != ("attempt-failed", "dashboard", "pending"):
            raise DisposableCommissioningError("quote-batch-alert-shape")
        return incident_key, str(incident[3])

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        structure_digest = sha256(f"{identity.experiment_id}:structure".encode()).hexdigest()
        universe_hash = sha256(f"{identity.experiment_id}:universe".encode()).hexdigest()
        self._batches = self._control_plane.enqueue_quote_generation(
            structure_receipt_digest=structure_digest,
            universe_hash=universe_hash,
            legs=(
                _leg(f"{identity.experiment_id}:token-a"),
                _leg(f"{identity.experiment_id}:token-b"),
            ),
            batch_size=1,
            now=self._started_at,
        )
        if len(self._batches) != 2:
            raise DisposableCommissioningError("quote-batch-plan-shape")
        first = _claim(self._control_plane, "quote-batch", self._started_at)
        _record_progress(self._control_plane, first, self._started_at)
        self._control_plane.record_quote_batch(
            first,
            token_range_digest=self._batches[0].token_range_digest,
            quote_digest=sha256(f"{identity.experiment_id}:quote:0".encode()).hexdigest(),
            artifact_key=f"quote-batches/{identity.experiment_id}/0.ndjson",
            artifact_digest=sha256(f"{identity.experiment_id}:artifact:0".encode()).hexdigest(),
            successful_response_count=1,
            quoted_at=self._started_at,
            now=self._started_at + timedelta(seconds=1),
            terminal=True,
        )
        withheld = _claim(
            self._control_plane,
            "quote-batch",
            self._started_at + timedelta(seconds=2),
        )
        if withheld.job_key != self._batches[1].job_key:
            raise DisposableCommissioningError("withheld-quote-batch-identity")
        _record_progress(
            self._control_plane,
            withheld,
            self._started_at + timedelta(seconds=2),
        )
        self._withheld_lease = withheld
        self._withheld_attempt_id = self._attempt_id(withheld)
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"attempt:{self._withheld_attempt_id}",
            occurred_at=self._started_at + timedelta(seconds=2),
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        withheld = self._need(self._withheld_lease, "withheld-quote-batch-missing")
        injected_at = self._started_at + timedelta(seconds=3)
        self._retry_due_at = self._control_plane.finish_retryable_with_incident(
            withheld,
            error_class="IncompleteQuoteBatchReceipt",
            incident_key=f"incident:job-retry:{withheld.job_key}",
            dedupe_key=f"job-retry:{withheld.job_key}",
            component="quote-batch",
            summary="quote batch receipt missing from certification barrier",
            detail={
                "job_key": withheld.job_key,
                "lease_epoch": withheld.lease_epoch,
                "stage": "commit-receipt",
                "failure_signature": "quote.batch-receipt-incomplete",
            },
            channels=("dashboard",),
            now=injected_at,
        )
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"attempt:{self._withheld_attempt_id}",
            occurred_at=injected_at,
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        self._incident_key, self._incident_event_id = self._assert_incomplete_barrier()
        return AttackStageReceipt(
            stage="detected",
            receipt_id=self._incident_key,
            occurred_at=self._started_at + timedelta(seconds=4),
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        self._assert_incomplete_barrier()
        event_id = self._need(self._incident_event_id, "quote-batch-incident-event-missing")
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"incident-event:{event_id}",
            occurred_at=self._started_at + timedelta(seconds=5),
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        self._assert_incomplete_barrier()
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"barrier:{self._generation_key()}:partial-pointer-absent",
            occurred_at=self._started_at + timedelta(seconds=6),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        withheld = self._need(self._withheld_lease, "withheld-quote-batch-missing")
        retry_due_at = self._need(self._retry_due_at, "quote-batch-retry-due-missing")
        replacement = _claim(self._control_plane, "quote-batch", retry_due_at)
        if (
            replacement.job_key != withheld.job_key
            or replacement.lease_epoch != withheld.lease_epoch + 1
        ):
            raise DisposableCommissioningError("quote-batch-replacement-identity")
        self._replacement = replacement
        _record_progress(self._control_plane, replacement, retry_due_at)
        self._control_plane.record_quote_batch(
            replacement,
            token_range_digest=self._batches[1].token_range_digest,
            quote_digest=sha256(f"{identity.experiment_id}:quote:1".encode()).hexdigest(),
            artifact_key=f"quote-batches/{identity.experiment_id}/1.ndjson",
            artifact_digest=sha256(f"{identity.experiment_id}:artifact:1".encode()).hexdigest(),
            successful_response_count=1,
            quoted_at=retry_due_at,
            now=retry_due_at + timedelta(seconds=1),
            terminal=True,
        )
        if not self._control_plane.record_job_recovery(
            replacement,
            component="quote-batch",
            channels=("dashboard",),
            now=retry_due_at + timedelta(seconds=2),
        ):
            raise DisposableCommissioningError("quote-batch-incident-not-recovered")
        certifier = _claim(
            self._control_plane,
            "quote-certify",
            retry_due_at + timedelta(seconds=3),
        )
        if certifier.job_key != f"{self._generation_key()}:certify":
            raise DisposableCommissioningError("quote-certifier-identity")
        self._certifier = certifier
        _record_progress(
            self._control_plane,
            certifier,
            retry_due_at + timedelta(seconds=3),
        )
        self._control_plane.certify_quote_generation(
            certifier,
            generation_key=self._generation_key(),
            now=retry_due_at + timedelta(seconds=4),
        )
        self._recovered_proof = _normal_turn_proof(self._control_plane, certifier)
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{self._recovered_proof['success_fact_id']}",
            occurred_at=retry_due_at + timedelta(seconds=4),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        generation_key = self._generation_key()
        certifier = self._need(self._certifier, "quote-certifier-missing")
        proof = self._need(self._recovered_proof, "quote-certifier-proof-missing")
        replacement = self._need(self._replacement, "quote-batch-replacement-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            shape = connection.execute(
                """
                SELECT
                  (SELECT count(*) FROM m1_quote_batch_inputs
                   WHERE job_key LIKE %s),
                  (SELECT count(*) FROM m1_quote_batch_receipts
                   WHERE job_key LIKE %s),
                  (SELECT count(*) FROM m1_jobs
                   WHERE job_key LIKE %s AND state = 'succeeded'),
                  (SELECT state FROM m1_jobs WHERE job_key = %s),
                  (SELECT count(*) FROM m1_generation_manifests
                   WHERE generation_key = %s),
                  (SELECT count(*) FROM m1_publication_pointers
                   WHERE pointer_key = 'quote:current' AND generation_key = %s),
                  (SELECT count(*) FROM m1_jobs
                   WHERE job_key = %s AND state = 'runnable')
                """,
                (
                    f"{generation_key}:batch:%",
                    f"{generation_key}:batch:%",
                    f"{generation_key}:batch:%",
                    certifier.job_key,
                    generation_key,
                    generation_key,
                    f"{generation_key}:opportunity-certify",
                ),
            ).fetchone()
            incident = connection.execute(
                """
                SELECT incident.state, circuit.consecutive_failures, circuit.state,
                       array_agg(event.kind ORDER BY event.occurred_at, event.kind)
                FROM m1_incidents AS incident
                JOIN m1_job_circuits AS circuit
                  ON incident.dedupe_key = 'job-retry:' || circuit.job_key
                JOIN m1_incident_events AS event
                  ON event.incident_key = incident.incident_key
                WHERE circuit.job_key = %s
                GROUP BY incident.state, circuit.consecutive_failures, circuit.state
                """,
                (replacement.job_key,),
            ).fetchone()
            attempts = connection.execute(
                """
                SELECT count(*), min(state)
                FROM m1_job_attempts
                WHERE job_key = %s
                """,
                (certifier.job_key,),
            ).fetchone()
        if shape != (2, 2, 2, "succeeded", 1, 1, 1):
            raise DisposableCommissioningError("quote-batch-recovery-shape")
        if incident != ("resolved", 0, "closed", ["attempt-failed", "recovered"]):
            raise DisposableCommissioningError("quote-batch-recovery-incident-shape")
        if attempts != (1, "succeeded"):
            raise DisposableCommissioningError("quote-certifier-attempt-shape")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=proof["postcondition_fact_id"],
            occurred_at=datetime.fromisoformat(proof["succeeded_at"]) + timedelta(seconds=1),
        )


class QuoteAdmissionMissingShardCommissioningAdapter:
    """Prove a manifest-named shard blocks, then safely resumes, admission."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._clock_at = self._started_at
        self._objects = _DisposableObjectStore()
        self._worker: TransactionalQuoteAdmitter | None = None
        self._admission_job_key: str | None = None
        self._missing_shard: StructureShardArtifact | None = None
        self._retry_due_at: datetime | None = None
        self._incident_key: str | None = None
        self._incident_event_id: str | None = None
        self._recovered_proof: dict[str, str] | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if (
            identity.attack_id != "quote-admission-missing-shard"
            or identity.node_id != "quote-admit"
        ):
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _assert_incomplete(self) -> tuple[str, str]:
        job_key = self._need(self._admission_job_key, "quote-admission-job-missing")
        missing = self._need(self._missing_shard, "missing-structure-shard-missing")
        retry_due_at = self._need(self._retry_due_at, "quote-admission-retry-due-missing")
        incident_key = f"incident:job-retry:{job_key}"
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            job = connection.execute(
                """
                SELECT state, lease_epoch, last_error_class, next_attempt_at
                FROM m1_jobs WHERE job_key = %s
                """,
                (job_key,),
            ).fetchone()
            circuit = connection.execute(
                """
                SELECT consecutive_failures, state, next_probe_at
                FROM m1_job_circuits WHERE job_key = %s
                """,
                (job_key,),
            ).fetchone()
            incident = connection.execute(
                """
                SELECT incident.incident_key, incident.state, incident.component,
                       event.incident_event_id, event.kind,
                       event.detail->>'missing_artifact_key',
                       outbox.channel, outbox.state
                FROM m1_incidents AS incident
                JOIN m1_incident_events AS event
                  ON event.incident_key = incident.incident_key
                JOIN m1_alert_outbox AS outbox
                  ON outbox.incident_event_id = event.incident_event_id
                WHERE incident.dedupe_key = %s
                """,
                (f"job-retry:{job_key}",),
            ).fetchone()
            partial = connection.execute(
                """
                SELECT
                  (SELECT count(*) FROM m1_quote_batch_inputs),
                  (SELECT count(*) FROM m1_jobs WHERE job_type = 'quote-batch'),
                  (SELECT count(*) FROM m1_checkpoint_receipts
                   WHERE job_key = %s AND checkpoint_sequence IS NOT NULL),
                  (SELECT count(*) FROM m1_publication_pointers
                   WHERE pointer_key = 'quote:current')
                """,
                (job_key,),
            ).fetchone()
        if job != (
            "retryable",
            1,
            "QuoteAdmissionShardUnavailable",
            retry_due_at,
        ):
            raise DisposableCommissioningError("quote-admission-retry-shape")
        if circuit != (1, "closed", retry_due_at):
            raise DisposableCommissioningError("quote-admission-circuit-shape")
        if incident is None or incident[:3] != (incident_key, "open", "quote-admit"):
            raise DisposableCommissioningError("quote-admission-incident-shape")
        if incident[4:] != (
            "attempt-failed",
            missing.key,
            "dashboard",
            "pending",
        ):
            raise DisposableCommissioningError("quote-admission-alert-shape")
        if partial != (0, 0, 0, 0):
            raise DisposableCommissioningError("quote-admission-partial-effect")
        if self._objects.contains(missing.key):
            raise DisposableCommissioningError("missing-shard-unexpectedly-present")
        return incident_key, str(incident[3])

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        bundle_identity = StructureBundleIdentity(
            publication_id=f"commissioning:{identity.experiment_id}",
            window_id=f"window:{identity.experiment_id}",
            snapshot_id=42,
            comparison_receipt_digest=sha256(
                f"{identity.experiment_id}:comparison".encode()
            ).hexdigest(),
            normalization_contract_version="structure-v7",
            component_counts={
                "events": 0,
                "event_tags": 0,
                "memberships": 0,
                "group_truth": 0,
                "markets": 2,
                "issues": 0,
            },
            source_kind="gamma-source-window-events-v3-sharded",
        )
        shards = tuple(
            StructureShardArtifact.from_bytes(
                canonical_structure_shard_bytes(
                    window_key=bundle_identity.window_id,
                    source_digest=sha256(
                        f"{identity.experiment_id}:source".encode()
                    ).hexdigest(),
                    component="markets",
                    ordinal=index,
                    rows=(
                        {
                            "market_id": f"market-{index}",
                            "condition_id": f"condition-{index}",
                            "slug": f"market-{index}",
                            "yes_token_id": f"yes-{index}",
                            "event_id": "event-a",
                            "active": True,
                            "closed": False,
                            "neg_risk": True,
                            "neg_risk_market_id": f"neg-risk-{index}",
                        },
                    ),
                )
            )
            for index in range(2)
        )
        manifest = StructureBundleArtifact.from_bytes(
            canonical_structure_shard_manifest_bytes(
                identity=bundle_identity,
                shards=tuple(
                    StructureShardReceipt("markets", index, shard.key, shard.sha256, 1)
                    for index, shard in enumerate(shards)
                ),
            )
        )
        self._objects.restore(key=manifest.key, payload=manifest.payload, digest=manifest.sha256)
        self._objects.restore(key=shards[0].key, payload=shards[0].payload, digest=shards[0].sha256)
        self._missing_shard = shards[1]
        specs = self._control_plane.enqueue_structure_generation(
            identity=bundle_identity,
            bundle=manifest,
            ranges=(("markets", "", ""),),
            now=self._started_at,
        )
        if len(specs) != 1:
            raise DisposableCommissioningError("quote-admission-structure-plan-shape")
        spec = specs[0]
        range_artifact_digest = sha256(
            f"{identity.experiment_id}:normalized-range".encode()
        ).hexdigest()
        normalizer = _claim(self._control_plane, "structure-normalize", self._started_at)
        _record_progress(self._control_plane, normalizer, self._started_at)
        self._control_plane.complete_structure_range(
            normalizer,
            range_digest=spec.range_digest,
            artifact_key=f"structure-ranges/{identity.experiment_id}.ndjson",
            artifact_digest=range_artifact_digest,
            record_count=2,
            now=self._started_at + timedelta(seconds=1),
        )
        certifier = _claim(
            self._control_plane,
            "structure-certify",
            self._started_at + timedelta(seconds=2),
        )
        _record_progress(
            self._control_plane,
            certifier,
            self._started_at + timedelta(seconds=2),
        )
        certification_manifest = sha256(
            canonical_structure_manifest_bytes(
                generation_key=spec.generation_key,
                bundle_digest=manifest.sha256,
                receipts=(
                    {
                        "job_key": spec.job_key,
                        "component": "markets",
                        "ordinal": 0,
                        "range_digest": spec.range_digest,
                        "artifact_key": f"structure-ranges/{identity.experiment_id}.ndjson",
                        "artifact_digest": range_artifact_digest,
                        "record_count": 2,
                    },
                ),
            )
        ).hexdigest()
        self._control_plane.certify_structure_generation(
            certifier,
            generation_key=spec.generation_key,
            artifact_key=f"structure-manifests/{certification_manifest}/manifest.ndjson",
            artifact_digest=certification_manifest,
            now=self._started_at + timedelta(seconds=3),
        )
        self._admission_job_key = f"{spec.generation_key}:quote-admit"
        self._clock_at = self._started_at + timedelta(seconds=4)
        self._worker = TransactionalQuoteAdmitter(
            control_plane=self._control_plane,
            object_client=self._objects,
            bucket="commissioning-artifacts",
            worker_id="commissioning:quote-admit",
            now=lambda: self._clock_at,
            batch_size=10,
            lease_seconds=120,
        )
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"postgres:m1_quote_admission_inputs:{self._admission_job_key}",
            occurred_at=self._clock_at,
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        worker = self._need(self._worker, "quote-admission-worker-missing")
        missing = self._need(self._missing_shard, "missing-structure-shard-missing")
        self._clock_at = self._started_at + timedelta(seconds=5)
        try:
            asyncio.run(worker.run_once())
        except QuoteAdmissionShardUnavailable as error:
            if error.artifact_key != missing.key:
                raise DisposableCommissioningError("wrong-missing-structure-shard") from error
        else:
            raise DisposableCommissioningError("missing-structure-shard-not-detected")
        job_key = self._need(self._admission_job_key, "quote-admission-job-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT attempt.attempt_id, job.next_attempt_at
                FROM m1_jobs AS job
                JOIN m1_job_attempts AS attempt
                  ON attempt.job_key = job.job_key AND attempt.lease_epoch = job.lease_epoch
                WHERE job.job_key = %s
                """,
                (job_key,),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("quote-admission-failure-fact-missing")
        self._retry_due_at = row[1]
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"attempt:{row[0]}",
            occurred_at=self._clock_at,
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        self._incident_key, self._incident_event_id = self._assert_incomplete()
        return AttackStageReceipt(
            stage="detected",
            receipt_id=self._incident_key,
            occurred_at=self._started_at + timedelta(seconds=6),
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        self._assert_incomplete()
        missing = self._need(self._missing_shard, "missing-structure-shard-missing")
        self._objects.restore(key=missing.key, payload=missing.payload, digest=missing.sha256)
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"artifact-restored:{missing.key}:{missing.sha256}",
            occurred_at=self._started_at + timedelta(seconds=7),
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        job_key = self._need(self._admission_job_key, "quote-admission-job-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            partial = connection.execute(
                """
                SELECT
                  (SELECT count(*) FROM m1_quote_batch_inputs),
                  (SELECT count(*) FROM m1_jobs WHERE job_type = 'quote-batch'),
                  (SELECT state FROM m1_jobs WHERE job_key = %s)
                """,
                (job_key,),
            ).fetchone()
        if partial != (0, 0, "retryable"):
            raise DisposableCommissioningError("quote-admission-cleanup-shape")
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"postgres:m1_quote_batch_inputs:{job_key}:absent",
            occurred_at=self._started_at + timedelta(seconds=8),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        worker = self._need(self._worker, "quote-admission-worker-missing")
        retry_due_at = self._need(self._retry_due_at, "quote-admission-retry-due-missing")
        self._clock_at = retry_due_at
        result = asyncio.run(worker.run_once())
        if result.outcome != "admitted":
            raise DisposableCommissioningError("quote-admission-recovery-outcome")
        job_key = self._need(self._admission_job_key, "quote-admission-job-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            attempt = connection.execute(
                """
                SELECT attempt.attempt_id, attempt.finished_at, event.event_id,
                       event.occurred_at, batch.job_key
                FROM m1_job_attempts AS attempt
                JOIN m1_job_runtime_events AS event
                  ON event.job_key = attempt.job_key
                 AND event.lease_epoch = attempt.lease_epoch
                 AND event.kind = %s
                JOIN m1_quote_batch_inputs AS batch
                  ON batch.structure_receipt_digest =
                     (SELECT bundle_digest FROM m1_quote_admission_inputs
                      WHERE job_key = attempt.job_key)
                WHERE attempt.job_key = %s AND attempt.lease_epoch = 2
                  AND attempt.state = 'succeeded'
                """,
                (RuntimeEventKind.SUCCEEDED.value, job_key),
            ).fetchone()
        if attempt is None or attempt[1] is None:
            raise DisposableCommissioningError("quote-admission-success-fact-missing")
        self._recovered_proof = {
            "attempt_id": str(attempt[0]),
            "terminal_fact_id": f"attempt:{attempt[0]}",
            "success_fact_id": str(attempt[2]),
            "postcondition_fact_id": f"postgres:m1_quote_batch_inputs:{attempt[4]}",
            "succeeded_at": attempt[3].astimezone(UTC).isoformat(),
        }
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{self._recovered_proof['success_fact_id']}",
            occurred_at=attempt[3],
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        job_key = self._need(self._admission_job_key, "quote-admission-job-missing")
        proof = self._need(self._recovered_proof, "quote-admission-proof-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            shape = connection.execute(
                """
                SELECT admission.state,
                       (SELECT count(*) FROM m1_job_attempts WHERE job_key = admission.job_key),
                       (SELECT array_agg(state ORDER BY lease_epoch)
                        FROM m1_job_attempts WHERE job_key = admission.job_key),
                       batch.state, jsonb_array_length(input.legs),
                       jsonb_array_length(input.token_ids),
                       input.input_artifact_key, input.input_artifact_digest,
                       input.leg_count
                FROM m1_jobs AS admission
                JOIN m1_quote_admission_inputs AS admitted
                  ON admitted.job_key = admission.job_key
                JOIN m1_quote_batch_inputs AS input
                  ON input.structure_receipt_digest = admitted.bundle_digest
                JOIN m1_jobs AS batch ON batch.job_key = input.job_key
                WHERE admission.job_key = %s
                """,
                (job_key,),
            ).fetchone()
            incident = connection.execute(
                """
                SELECT incident.state, circuit.consecutive_failures, circuit.state,
                       array_agg(event.kind ORDER BY event.occurred_at, event.kind)
                FROM m1_incidents AS incident
                JOIN m1_job_circuits AS circuit
                  ON incident.dedupe_key = 'job-retry:' || circuit.job_key
                JOIN m1_incident_events AS event
                  ON event.incident_key = incident.incident_key
                WHERE circuit.job_key = %s
                GROUP BY incident.state, circuit.consecutive_failures, circuit.state
                """,
                (job_key,),
            ).fetchone()
            pointer = connection.execute(
                "SELECT count(*) FROM m1_publication_pointers WHERE pointer_key = 'quote:current'"
            ).fetchone()
        if shape is None or shape[:6] != (
            "succeeded",
            2,
            ["retryable", "succeeded"],
            "runnable",
            None,
            None,
        ):
            raise DisposableCommissioningError("quote-admission-recovery-shape")
        if shape[8] != 2:
            raise DisposableCommissioningError("quote-admission-artifact-count")
        artifact_key, artifact_digest = str(shape[6]), str(shape[7])
        head = self._objects.head_object(Bucket="commissioning-artifacts", Key=artifact_key)
        if (
            not isinstance(head["ContentLength"], int)
            or head["ContentLength"] <= 0
            or head["Metadata"] != {"sha256": artifact_digest}
            or incident != ("resolved", 0, "closed", ["attempt-failed", "recovered"])
            or pointer != (0,)
        ):
            raise DisposableCommissioningError("quote-admission-recovery-proof")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=proof["postcondition_fact_id"],
            occurred_at=datetime.fromisoformat(proof["succeeded_at"]) + timedelta(seconds=1),
        )


class NormalizationPayloadCorruptCommissioningAdapter:
    """Quarantine one authenticated bad shard without replacing certified truth."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._clock_at = self._started_at
        self._objects = _DisposableObjectStore()
        self._worker: TransactionalStructureWorker | None = None
        self._job_key: str | None = None
        self._generation_key: str | None = None
        self._corrupt: StructureShardArtifact | None = None
        self._prior_generation_key: str | None = None
        self._terminal_event_id: str | None = None
        self._incident_key: str | None = None
        self._incident_event_id: str | None = None
        self._outbox_id: str | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if (
            identity.attack_id != "normalization-payload-corrupt"
            or identity.node_id != "structure-normalize"
        ):
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _certify_prior_generation(self, identity: AttackIdentity) -> str:
        prior_identity = StructureBundleIdentity(
            publication_id=f"commissioning:prior:{identity.experiment_id}",
            window_id=f"prior:{identity.experiment_id}",
            snapshot_id=1,
            comparison_receipt_digest=sha256(
                f"{identity.experiment_id}:prior-comparison".encode()
            ).hexdigest(),
            normalization_contract_version="structure-v7",
            component_counts={
                "events": 1,
                "event_tags": 0,
                "memberships": 0,
                "group_truth": 0,
                "markets": 0,
                "issues": 0,
            },
        )
        prior_bundle = StructureBundleArtifact.from_bytes(
            canonical_structure_bundle_bytes(
                identity=prior_identity,
                components={
                    "events": ({"id": "prior-event"},),
                    "event_tags": (),
                    "memberships": (),
                    "group_truth": (),
                    "markets": (),
                    "issues": (),
                },
            )
        )
        specs = self._control_plane.enqueue_structure_generation(
            identity=prior_identity,
            bundle=prior_bundle,
            ranges=(("events", "", ""),),
            now=self._started_at,
        )
        if len(specs) != 1:
            raise DisposableCommissioningError("prior-structure-plan-shape")
        spec = specs[0]
        range_digest = sha256(f"{identity.experiment_id}:prior-range".encode()).hexdigest()
        range_key = f"structure-ranges/{range_digest}/rows.ndjson"
        normalizer = _claim(
            self._control_plane, "structure-normalize", self._started_at
        )
        _record_progress(self._control_plane, normalizer, self._started_at)
        self._control_plane.complete_structure_range(
            normalizer,
            range_digest=spec.range_digest,
            artifact_key=range_key,
            artifact_digest=range_digest,
            record_count=1,
            now=self._started_at + timedelta(seconds=1),
        )
        certifier = _claim(
            self._control_plane,
            "structure-certify",
            self._started_at + timedelta(seconds=2),
        )
        _record_progress(
            self._control_plane,
            certifier,
            self._started_at + timedelta(seconds=2),
        )
        manifest_digest = sha256(
            canonical_structure_manifest_bytes(
                generation_key=spec.generation_key,
                bundle_digest=prior_bundle.sha256,
                receipts=(
                    {
                        "job_key": spec.job_key,
                        "component": "events",
                        "ordinal": 0,
                        "range_digest": spec.range_digest,
                        "artifact_key": range_key,
                        "artifact_digest": range_digest,
                        "record_count": 1,
                    },
                ),
            )
        ).hexdigest()
        self._control_plane.certify_structure_generation(
            certifier,
            generation_key=spec.generation_key,
            artifact_key=f"structure-manifests/{manifest_digest}/manifest.ndjson",
            artifact_digest=manifest_digest,
            now=self._started_at + timedelta(seconds=3),
        )
        self._control_plane.publish_structure_shadow(
            generation_key=spec.generation_key,
            now=self._started_at + timedelta(seconds=4),
        )
        return spec.generation_key

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        self._prior_generation_key = self._certify_prior_generation(identity)
        corrupt_identity = StructureBundleIdentity(
            publication_id=f"commissioning:corrupt:{identity.experiment_id}",
            window_id=f"corrupt:{identity.experiment_id}",
            snapshot_id=2,
            comparison_receipt_digest=sha256(
                f"{identity.experiment_id}:corrupt-comparison".encode()
            ).hexdigest(),
            normalization_contract_version="gamma-source-window-events-v3-sharded",
            component_counts={
                "events": 1,
                "event_tags": 0,
                "memberships": 0,
                "group_truth": 0,
                "markets": 0,
                "issues": 0,
            },
            source_kind="gamma-source-window-events-v3-sharded",
        )
        corrupt = StructureShardArtifact.from_bytes(
            b'{"kind":"not-a-structure-shard"}\n'
        )
        manifest = StructureBundleArtifact.from_bytes(
            canonical_structure_shard_manifest_bytes(
                identity=corrupt_identity,
                shards=(
                    StructureShardReceipt("events", 0, corrupt.key, corrupt.sha256, 1),
                ),
            )
        )
        self._objects.restore(
            key=manifest.key,
            payload=manifest.payload,
            digest=manifest.sha256,
        )
        specs = self._control_plane.enqueue_structure_generation(
            identity=corrupt_identity,
            bundle=manifest,
            ranges=(("events", "shard:00000000", "shard:00000001"),),
            now=self._started_at + timedelta(seconds=5),
        )
        if len(specs) != 1:
            raise DisposableCommissioningError("corrupt-structure-plan-shape")
        spec = specs[0]
        self._job_key = spec.job_key
        self._generation_key = spec.generation_key
        self._corrupt = corrupt
        self._clock_at = self._started_at + timedelta(seconds=6)
        self._worker = TransactionalStructureWorker(
            control_plane=self._control_plane,
            object_client=self._objects,
            bucket="commissioning-artifacts",
            worker_id="commissioning:structure-normalize",
            now=lambda: self._clock_at,
            lease_seconds=120,
        )
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"pointer:structure:current:shadow:{self._prior_generation_key}",
            occurred_at=self._started_at + timedelta(seconds=6),
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        corrupt = self._need(self._corrupt, "corrupt-structure-shard-missing")
        self._objects.restore(key=corrupt.key, payload=corrupt.payload, digest=corrupt.sha256)
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"artifact:{corrupt.key}:{corrupt.sha256}",
            occurred_at=self._started_at + timedelta(seconds=7),
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        worker = self._need(self._worker, "normalization-worker-missing")
        corrupt = self._need(self._corrupt, "corrupt-structure-shard-missing")
        self._clock_at = self._started_at + timedelta(seconds=8)
        try:
            asyncio.run(worker.run_once())
        except StructureNormalizationInputInvalid as error:
            if error.artifact_key != corrupt.key:
                raise DisposableCommissioningError("wrong-corrupt-artifact") from error
        else:
            raise DisposableCommissioningError("corrupt-artifact-not-detected")
        job_key = self._need(self._job_key, "normalization-job-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT runtime.event_id, incident.incident_key,
                       event.incident_event_id, outbox.outbox_id
                FROM m1_job_runtime_events AS runtime
                JOIN m1_incidents AS incident
                  ON incident.dedupe_key = %s
                JOIN m1_incident_events AS event
                  ON event.incident_key = incident.incident_key
                JOIN m1_alert_outbox AS outbox
                  ON outbox.incident_event_id = event.incident_event_id
                WHERE runtime.job_key = %s
                  AND runtime.kind = 'job.terminal-failed'
                """,
                (f"input-quarantine:{job_key}", job_key),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("quarantine-chain-missing")
        self._terminal_event_id = str(row[0])
        self._incident_key = str(row[1])
        self._incident_event_id = str(row[2])
        self._outbox_id = str(row[3])
        return AttackStageReceipt(
            stage="detected",
            receipt_id=f"event:{self._terminal_event_id}",
            occurred_at=self._started_at + timedelta(seconds=8),
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"operator-action:{self._incident_key}:{self._outbox_id}",
            occurred_at=self._started_at + timedelta(seconds=9),
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        job_key = self._need(self._job_key, "normalization-job-missing")
        generation_key = self._need(self._generation_key, "normalization-generation-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            partial = connection.execute(
                """
                SELECT
                  (SELECT count(*) FROM m1_structure_range_receipts WHERE job_key = %s),
                  (SELECT count(*) FROM m1_generation_manifests WHERE generation_key = %s),
                  (SELECT state FROM m1_jobs WHERE job_key = %s),
                  (SELECT state FROM m1_jobs WHERE job_key = %s || ':certify')
                """,
                (job_key, generation_key, job_key, generation_key),
            ).fetchone()
        if partial != (0, 0, "quarantined", "waiting"):
            raise DisposableCommissioningError("normalization-quarantine-partial-effect")
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"postgres:m1_structure_range_receipts:{job_key}:absent",
            occurred_at=self._started_at + timedelta(seconds=10),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"incident:{self._incident_event_id}:operator-action-required",
            occurred_at=self._started_at + timedelta(seconds=11),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        job_key = self._need(self._job_key, "normalization-job-missing")
        prior_generation = self._need(
            self._prior_generation_key, "prior-structure-generation-missing"
        )
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            shape = connection.execute(
                """
                SELECT job.state, attempt.state, runtime.kind,
                       runtime.detail->>'qualification_impact',
                       incident.state, incident.severity, event.kind,
                       outbox.channel, outbox.state,
                       pointer.generation_key,
                       (SELECT count(*) FROM m1_job_circuits WHERE job_key = job.job_key)
                FROM m1_jobs AS job
                JOIN m1_job_attempts AS attempt
                  ON attempt.job_key = job.job_key AND attempt.lease_epoch = job.lease_epoch
                JOIN m1_job_runtime_events AS runtime
                  ON runtime.job_key = job.job_key
                 AND runtime.lease_epoch = job.lease_epoch
                 AND runtime.kind = 'job.terminal-failed'
                JOIN m1_incidents AS incident
                  ON incident.dedupe_key = 'input-quarantine:' || job.job_key
                JOIN m1_incident_events AS event
                  ON event.incident_key = incident.incident_key
                JOIN m1_alert_outbox AS outbox
                  ON outbox.incident_event_id = event.incident_event_id
                JOIN m1_publication_pointers AS pointer
                  ON pointer.pointer_key = 'structure:current:shadow'
                WHERE job.job_key = %s
                """,
                (job_key,),
            ).fetchone()
        if shape != (
            "quarantined",
            "quarantined",
            "job.terminal-failed",
            "blocked",
            "open",
            "critical",
            "escalated",
            "dashboard",
            "pending",
            prior_generation,
            0,
        ):
            raise DisposableCommissioningError("normalization-quarantine-proof-shape")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=f"pointer:structure:current:shadow:{prior_generation}",
            occurred_at=self._started_at + timedelta(seconds=12),
        )


class StructureParityMismatchCommissioningAdapter:
    """Invalidate one frozen-count conflict without replacing certified truth."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._clock_at = self._started_at
        self._objects = _DisposableObjectStore()
        self._certifier: TransactionalStructureCertifier | None = None
        self._job_key: str | None = None
        self._generation_key: str | None = None
        self._prior_generation_key: str | None = None
        self._terminal_event_id: str | None = None
        self._incident_key: str | None = None
        self._incident_event_id: str | None = None
        self._outbox_id: str | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if (
            identity.attack_id != "structure-parity-mismatch"
            or identity.node_id != "structure-certify"
        ):
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        prior_builder = NormalizationPayloadCorruptCommissioningAdapter(
            control_plane=self._control_plane,
            started_at=self._started_at,
        )
        self._prior_generation_key = prior_builder._certify_prior_generation(identity)  # noqa: SLF001
        candidate_identity = StructureBundleIdentity(
            publication_id=f"commissioning:parity:{identity.experiment_id}",
            window_id=f"parity:{identity.experiment_id}",
            snapshot_id=2,
            comparison_receipt_digest=sha256(
                f"{identity.experiment_id}:parity-comparison".encode()
            ).hexdigest(),
            normalization_contract_version="structure-v7",
            component_counts={
                "events": 1,
                "event_tags": 0,
                "memberships": 0,
                "group_truth": 0,
                "markets": 0,
                "issues": 0,
            },
        )
        components = {
            "events": ({"id": "candidate-event"},),
            "event_tags": (),
            "memberships": (),
            "group_truth": (),
            "markets": (),
            "issues": (),
        }
        bundle = StructureBundleArtifact.from_bytes(
            canonical_structure_bundle_bytes(
                identity=candidate_identity,
                components=components,
            )
        )
        self._objects.restore(key=bundle.key, payload=bundle.payload, digest=bundle.sha256)
        specs = self._control_plane.enqueue_structure_generation(
            identity=candidate_identity,
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=self._started_at + timedelta(seconds=5),
        )
        if len(specs) != 1:
            raise DisposableCommissioningError("parity-structure-plan-shape")
        spec = specs[0]
        artifact = StructureRangeArtifact.from_bytes(
            canonical_structure_range_bytes(
                bundle_digest=bundle.sha256,
                component="events",
                range_digest=spec.range_digest,
                rows=components["events"],
            )
        )
        self._objects.restore(
            key=artifact.key,
            payload=artifact.payload,
            digest=artifact.sha256,
        )
        normalizer = _claim(
            self._control_plane,
            "structure-normalize",
            self._started_at + timedelta(seconds=6),
        )
        _record_progress(
            self._control_plane,
            normalizer,
            self._started_at + timedelta(seconds=6),
        )
        self._control_plane.complete_structure_range(
            normalizer,
            range_digest=spec.range_digest,
            artifact_key=artifact.key,
            artifact_digest=artifact.sha256,
            record_count=1,
            now=self._started_at + timedelta(seconds=7),
        )
        self._job_key = f"{spec.generation_key}:certify"
        self._generation_key = spec.generation_key
        self._clock_at = self._started_at + timedelta(seconds=9)
        self._certifier = TransactionalStructureCertifier(
            control_plane=self._control_plane,
            object_client=self._objects,
            bucket="commissioning-artifacts",
            worker_id="commissioning:structure-certify",
            now=lambda: self._clock_at,
            lease_seconds=120,
        )
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"pointer:structure:current:shadow:{self._prior_generation_key}",
            occurred_at=self._started_at + timedelta(seconds=8),
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        generation_key = self._need(self._generation_key, "parity-generation-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            changed = connection.execute(
                """
                UPDATE m1_structure_generation_inputs
                SET identity = jsonb_set(identity, '{component_counts,events}', '2'::jsonb)
                WHERE generation_key = %s
                """,
                (generation_key,),
            ).rowcount
        if changed != 1:
            raise DisposableCommissioningError("parity-injection-missing")
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"postgres:m1_structure_generation_inputs:{generation_key}:events=2",
            occurred_at=self._started_at + timedelta(seconds=9),
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        certifier = self._need(self._certifier, "parity-certifier-missing")
        self._clock_at = self._started_at + timedelta(seconds=10)
        result = certifier.run_once()
        if result.outcome != "quarantined":
            raise DisposableCommissioningError("parity-mismatch-not-quarantined")
        job_key = self._need(self._job_key, "parity-job-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT runtime.event_id, incident.incident_key,
                       event.incident_event_id, outbox.outbox_id
                FROM m1_job_runtime_events AS runtime
                JOIN m1_incidents AS incident ON incident.dedupe_key = %s
                JOIN m1_incident_events AS event USING (incident_key)
                JOIN m1_alert_outbox AS outbox USING (incident_event_id)
                WHERE runtime.job_key = %s AND runtime.kind = 'job.terminal-failed'
                """,
                (f"integrity-conflict:{job_key}", job_key),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("parity-quarantine-chain-missing")
        self._terminal_event_id = str(row[0])
        self._incident_key = str(row[1])
        self._incident_event_id = str(row[2])
        self._outbox_id = str(row[3])
        return AttackStageReceipt(
            stage="detected",
            receipt_id=f"event:{self._terminal_event_id}",
            occurred_at=self._started_at + timedelta(seconds=10),
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"operator-action:{self._incident_key}:{self._outbox_id}",
            occurred_at=self._started_at + timedelta(seconds=11),
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        generation_key = self._need(self._generation_key, "parity-generation-missing")
        prior_generation = self._need(
            self._prior_generation_key, "prior-structure-generation-missing"
        )
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            shape = connection.execute(
                """
                SELECT
                  (SELECT count(*) FROM m1_generation_manifests WHERE generation_key = %s),
                  (SELECT count(*) FROM m1_quote_admission_inputs WHERE generation_key = %s),
                  (SELECT generation_key FROM m1_publication_pointers
                   WHERE pointer_key = 'structure:current:shadow')
                """,
                (generation_key, generation_key),
            ).fetchone()
        if shape != (0, 0, prior_generation):
            raise DisposableCommissioningError("parity-quarantine-partial-effect")
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"postgres:m1_generation_manifests:{generation_key}:absent",
            occurred_at=self._started_at + timedelta(seconds=12),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"incident:{self._incident_event_id}:operator-action-required",
            occurred_at=self._started_at + timedelta(seconds=13),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        job_key = self._need(self._job_key, "parity-job-missing")
        prior_generation = self._need(
            self._prior_generation_key, "prior-structure-generation-missing"
        )
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            shape = connection.execute(
                """
                SELECT job.state, attempt.state, runtime.kind,
                       runtime.detail->>'reason_code',
                       runtime.detail->>'qualification_impact',
                       incident.state, incident.severity, event.kind,
                       outbox.channel, outbox.state, pointer.generation_key,
                       (SELECT count(*) FROM m1_job_circuits WHERE job_key = job.job_key)
                FROM m1_jobs AS job
                JOIN m1_job_attempts AS attempt
                  ON attempt.job_key = job.job_key AND attempt.lease_epoch = job.lease_epoch
                JOIN m1_job_runtime_events AS runtime
                  ON runtime.job_key = job.job_key
                 AND runtime.lease_epoch = job.lease_epoch
                 AND runtime.kind = 'job.terminal-failed'
                JOIN m1_incidents AS incident
                  ON incident.dedupe_key = 'integrity-conflict:' || job.job_key
                JOIN m1_incident_events AS event USING (incident_key)
                JOIN m1_alert_outbox AS outbox USING (incident_event_id)
                JOIN m1_publication_pointers AS pointer
                  ON pointer.pointer_key = 'structure:current:shadow'
                WHERE job.job_key = %s
                """,
                (job_key,),
            ).fetchone()
        if shape != (
            "quarantined",
            "quarantined",
            "job.terminal-failed",
            "integrity.conflict",
            "invalidated",
            "open",
            "critical",
            "escalated",
            "dashboard",
            "pending",
            prior_generation,
            0,
        ):
            raise DisposableCommissioningError("parity-quarantine-proof-shape")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=f"pointer:structure:current:shadow:{prior_generation}",
            occurred_at=self._started_at + timedelta(seconds=14),
        )


class PublicationPointerConflictCommissioningAdapter:
    """Race one stale publisher against a newer lineage on each pointer node."""

    def __init__(
        self,
        *,
        control_plane: PostgresControlPlane,
        started_at: datetime,
    ) -> None:
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise DisposableCommissioningError("invalid-started-at")
        self._control_plane = control_plane
        self._started_at = started_at.astimezone(UTC)
        self._stale: PreparedNormalTurn | None = None
        self._stale_active_lease: JobLease | None = None
        self._current: PreparedNormalTurn | None = None
        self._stale_generation: str | None = None
        self._current_generation: str | None = None
        self._detector_id: str | None = None
        self._incident_event_id: str | None = None

    @staticmethod
    def _require_identity(identity: AttackIdentity) -> None:
        if identity.attack_id != "publication-pointer-conflict" or identity.node_id not in {
            "structure-certify",
            "quote-certify",
            "opportunity-certify",
        }:
            raise DisposableCommissioningError("wrong-attack-adapter")

    @staticmethod
    def _need[T](value: T | None, reason: str) -> T:
        if value is None:
            raise DisposableCommissioningError(reason)
        return value

    def _generation(self, prepared: PreparedNormalTurn) -> str:
        if prepared.lease.job_type == "opportunity-certify":
            return prepared.lease.input_identity
        return prepared.lease.job_key.removesuffix(":certify")

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        stale = prepare_normal_turn(
            self._control_plane,
            node_id=identity.node_id,
            experiment_id=f"{identity.experiment_id}:stale",
            now=self._started_at,
        )
        if identity.node_id == "structure-certify":
            stale.complete(now=self._started_at + timedelta(seconds=5))
            stale_generation = self._generation(stale)
            self._control_plane.publish_structure_shadow(
                generation_key=stale_generation,
                expected_generation_key=None,
                now=self._started_at + timedelta(seconds=6),
            )
        else:
            self._control_plane.finish(
                stale.lease,
                state=JobState.RETRYABLE,
                next_attempt_at=self._started_at + timedelta(seconds=100),
                error_class="PublicationRaceStaging",
                now=self._started_at + timedelta(seconds=1),
            )
        current = prepare_normal_turn(
            self._control_plane,
            node_id=identity.node_id,
            experiment_id=f"{identity.experiment_id}:current",
            now=self._started_at + timedelta(seconds=10),
        )
        if identity.node_id == "structure-certify":
            current.complete(now=self._started_at + timedelta(seconds=15))
        else:
            with self._control_plane._connection_factory() as connection:  # noqa: SLF001
                connection.execute(
                    "UPDATE m1_jobs SET next_attempt_at = %s WHERE job_key = %s",
                    (self._started_at + timedelta(seconds=32), stale.lease.job_key),
                )
        self._stale = stale
        self._stale_active_lease = stale.lease
        self._current = current
        self._stale_generation = self._generation(stale)
        self._current_generation = self._generation(current)
        return AttackStageReceipt(
            stage="preflight",
            receipt_id=f"candidate:{self._stale_generation}:expected-predecessor",
            occurred_at=self._started_at + timedelta(seconds=30),
        )

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        self._require_identity(identity)
        stale_generation = self._need(self._stale_generation, "stale-generation-missing")
        current_generation = self._need(self._current_generation, "current-generation-missing")
        current = self._need(self._current, "current-turn-missing")
        if identity.node_id == "structure-certify":
            self._control_plane.publish_structure_shadow(
                generation_key=current_generation,
                expected_generation_key=stale_generation,
                now=self._started_at + timedelta(seconds=31),
            )
        else:
            current.complete(now=self._started_at + timedelta(seconds=31))
        return AttackStageReceipt(
            stage="injected",
            receipt_id=f"pointer:{identity.node_id}:{current_generation}",
            occurred_at=self._started_at + timedelta(seconds=31),
        )

    def _quarantine_stale(self, identity: AttackIdentity, lease: JobLease) -> None:
        self._control_plane.finish_quarantined_with_incident(
            lease,
            error_class="PublicationPointerConflictError",
            incident_key=f"incident:publication-superseded:{lease.job_key}",
            dedupe_key=f"publication-superseded:{lease.job_key}",
            component=identity.node_id,
            summary=f"{identity.node_id} stale publication superseded",
            detail={
                "job_key": lease.job_key,
                "lease_epoch": lease.lease_epoch,
                "reason_code": "publication.superseded",
            },
            channels=("dashboard",),
            qualification_impact="delayed",
            reason_code="publication.superseded",
            severity="warning",
            incident_kind="detected",
            qualification_breaking=False,
            now=self._started_at + timedelta(seconds=32),
        )

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        stale = self._need(self._stale, "stale-turn-missing")
        stale_generation = self._need(self._stale_generation, "stale-generation-missing")
        active_lease = stale.lease
        if identity.node_id != "structure-certify":
            replacement = self._control_plane.claim_job(
                worker_id=f"commissioning:stale-publisher:{identity.node_id}",
                job_types=(identity.node_id,),
                lease_seconds=120,
                now=self._started_at + timedelta(seconds=32),
            )
            if replacement is None or replacement.job_key != stale.lease.job_key:
                raise DisposableCommissioningError("stale-publisher-reclaim-missing")
            active_lease = replacement
            self._stale_active_lease = replacement
        try:
            if identity.node_id == "structure-certify":
                self._control_plane.publish_structure_shadow(
                    generation_key=stale_generation,
                    expected_generation_key=None,
                    now=self._started_at + timedelta(seconds=32),
                )
            else:
                stale.complete(
                    lease=active_lease,
                    now=self._started_at + timedelta(seconds=32),
                )
        except PublicationPointerConflictError:
            pass
        else:
            raise DisposableCommissioningError("stale-pointer-publication-not-rejected")
        if identity.node_id == "structure-certify":
            self._detector_id = f"cas:{stale_generation}:rejected"
        else:
            self._quarantine_stale(identity, active_lease)
            with self._control_plane._connection_factory() as connection:  # noqa: SLF001
                row = connection.execute(
                    """
                    SELECT runtime.event_id, event.incident_event_id
                    FROM m1_job_runtime_events AS runtime
                    JOIN m1_incidents AS incident
                      ON incident.dedupe_key = 'publication-superseded:' || runtime.job_key
                    JOIN m1_incident_events AS event USING (incident_key)
                    WHERE runtime.job_key = %s AND runtime.kind = 'job.terminal-failed'
                    """,
                    (active_lease.job_key,),
                ).fetchone()
            if row is None:
                raise DisposableCommissioningError("pointer-conflict-warning-chain-missing")
            self._detector_id = f"event:{row[0]}"
            self._incident_event_id = str(row[1])
        return AttackStageReceipt(
            stage="detected",
            receipt_id=self._need(self._detector_id, "pointer-detector-missing"),
            occurred_at=self._started_at + timedelta(seconds=32),
        )

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        current_generation = self._need(self._current_generation, "current-generation-missing")
        return AttackStageReceipt(
            stage="recovery-started",
            receipt_id=f"lineage-preserved:{current_generation}",
            occurred_at=self._started_at + timedelta(seconds=33),
        )

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        stale = self._need(self._stale, "stale-turn-missing")
        expected_state = "succeeded" if identity.node_id == "structure-certify" else "quarantined"
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            state = connection.execute(
                "SELECT state FROM m1_jobs WHERE job_key = %s",
                (stale.lease.job_key,),
            ).fetchone()
            circuits = connection.execute(
                "SELECT count(*) FROM m1_job_circuits WHERE job_key = %s",
                (stale.lease.job_key,),
            ).fetchone()
        if state != (expected_state,) or circuits != (0,):
            raise DisposableCommissioningError("pointer-conflict-cleanup-shape")
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id=f"postgres:m1_job_circuits:{stale.lease.job_key}:absent",
            occurred_at=self._started_at + timedelta(seconds=34),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        current = self._need(self._current, "current-turn-missing")
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            row = connection.execute(
                """
                SELECT event_id FROM m1_job_runtime_events
                WHERE job_key = %s AND kind = 'job.succeeded'
                """,
                (current.lease.job_key,),
            ).fetchone()
        if row is None:
            raise DisposableCommissioningError("current-pointer-success-missing")
        return AttackStageReceipt(
            stage="recovered",
            receipt_id=f"event:{row[0]}",
            occurred_at=self._started_at + timedelta(seconds=35),
        )

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        self._require_identity(identity)
        current_generation = self._need(self._current_generation, "current-generation-missing")
        if identity.node_id == "structure-certify":
            pointer_table = "m1_publication_pointers"
            pointer_key = "structure:current:shadow"
        elif identity.node_id == "quote-certify":
            pointer_table = "m1_publication_pointers"
            pointer_key = "quote:current"
        else:
            pointer_table = "m1_opportunity_publication_pointers"
            pointer_key = "opportunity:current"
        with self._control_plane._connection_factory() as connection:  # noqa: SLF001
            pointer = connection.execute(
                f"SELECT generation_key FROM {pointer_table} WHERE pointer_key = %s",  # noqa: S608
                (pointer_key,),
            ).fetchone()
            matching_success = connection.execute(
                """
                SELECT count(*) FROM m1_job_runtime_events AS event
                JOIN m1_jobs AS job USING (job_key)
                WHERE event.kind = 'job.succeeded' AND job.job_key = %s
                """,
                (self._need(self._current, "current-turn-missing").lease.job_key,),
            ).fetchone()
        if pointer != (current_generation,) or matching_success != (1,):
            raise DisposableCommissioningError("pointer-conflict-postcondition")
        return AttackStageReceipt(
            stage="verified",
            receipt_id=f"pointer:{pointer_key}:{current_generation}",
            occurred_at=self._started_at + timedelta(seconds=36),
        )


def _require(value: Any, reason: str) -> Any:
    if not value:
        raise DisposableCommissioningError(reason)
    return value


def complete_normal_turn(
    control_plane: PostgresControlPlane,
    *,
    node_id: str,
    experiment_id: str,
    now: datetime,
) -> dict[str, str]:
    """Complete one real domain transaction and return database-backed proof IDs."""

    prepared = prepare_normal_turn(
        control_plane,
        node_id=node_id,
        experiment_id=experiment_id,
        now=now,
    )
    return prepared.complete(now=now + timedelta(seconds=30))


def prepare_normal_turn(
    control_plane: PostgresControlPlane,
    *,
    node_id: str,
    experiment_id: str,
    now: datetime,
    progress_through: str | None = None,
) -> PreparedNormalTurn:
    """Prepare one real transaction and stop immediately before its fenced commit."""

    if node_id not in RUNTIME_STAGE_REGISTRY:
        raise DisposableCommissioningError("unknown-node")
    if not experiment_id or "\x00" in experiment_id or len(experiment_id) > 200:
        raise DisposableCommissioningError("invalid-experiment-id")
    if now.tzinfo is None or now.utcoffset() is None:
        raise DisposableCommissioningError("invalid-now")
    now = now.astimezone(UTC)
    preparers = {
        "structure-fetch": _prepare_structure_fetch,
        "structure-materialize": _prepare_structure_materialize,
        "structure-normalize": _prepare_structure_normalize,
        "structure-certify": _prepare_structure_certify,
        "quote-admit": _prepare_quote_admit,
        "quote-batch": _prepare_quote_batch,
        "quote-certify": _prepare_quote_certify,
        "opportunity-certify": _prepare_opportunity_certify,
    }
    return preparers[node_id](
        control_plane,
        experiment_id,
        now,
        progress_through=progress_through,
    )


def _normal_turn_proof(control_plane: PostgresControlPlane, lease: JobLease) -> dict[str, str]:
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        attempt = connection.execute(
            """
            SELECT attempt_id, finished_at FROM m1_job_attempts
            WHERE job_key = %s AND lease_epoch = %s AND state = 'succeeded'
            """,
            (lease.job_key, lease.lease_epoch),
        ).fetchone()
        success = connection.execute(
            """
            SELECT event_id, occurred_at FROM m1_job_runtime_events
            WHERE job_key = %s AND lease_epoch = %s AND kind = %s
            """,
            (lease.job_key, lease.lease_epoch, RuntimeEventKind.SUCCEEDED.value),
        ).fetchone()
        postcondition = _postcondition_fact(connection, lease)
    if attempt is None or attempt[1] is None:
        raise DisposableCommissioningError("terminal-attempt-missing")
    if success is None:
        raise DisposableCommissioningError("success-event-missing")
    return {
        "attempt_id": str(attempt[0]),
        "terminal_fact_id": f"attempt:{attempt[0]}",
        "success_fact_id": str(success[0]),
        "postcondition_fact_id": postcondition,
        "succeeded_at": success[1].astimezone(UTC).isoformat(),
    }


def _record_progress(
    control_plane: PostgresControlPlane,
    lease: JobLease,
    now: datetime,
    *,
    through: str | None = None,
) -> None:
    runtime = AttemptRuntime(
        store=control_plane,
        lease=lease,
        profile=runtime_deadline_profile(lease.job_type, 120),
        clock=lambda: now + timedelta(seconds=1),
    )
    stages = RUNTIME_STAGE_REGISTRY[lease.job_type]
    if through is not None:
        try:
            stages = stages[: stages.index(through) + 1]
        except ValueError as error:
            raise DisposableCommissioningError("unknown-runtime-progress-stage") from error
    for index, stage in enumerate(stages, start=1):
        runtime.progress(
            stage=stage, current=index, total=len(stages), detail={"component": lease.job_type}
        )


def _claim(control_plane: PostgresControlPlane, node_id: str, now: datetime) -> JobLease:
    lease = control_plane.claim_job(
        worker_id=f"commissioning:{node_id}",
        job_types=(node_id,),
        lease_seconds=120,
        now=now,
    )
    if lease is None:
        raise DisposableCommissioningError(f"claim-missing:{node_id}")
    return lease


def _identity(tag: str, window: str, comparison: str, source_kind: str) -> StructureBundleIdentity:
    return StructureBundleIdentity(
        publication_id=f"commissioning:{tag}",
        window_id=window,
        snapshot_id=42,
        comparison_receipt_digest=comparison,
        normalization_contract_version="structure-v7",
        source_kind=source_kind,
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
    )


def _leg(tag: str) -> QuoteBatchLeg:
    return QuoteBatchLeg(
        neg_risk_market_id=f"neg-risk-{tag}",
        market_id=f"market-{tag}",
        condition_id=f"condition-{tag}",
        slug=f"slug-{tag}",
        yes_token_id=tag,
        event_id=f"event-{tag}",
        membership_hash=f"membership-{tag}",
    )


def _generation(control_plane: PostgresControlPlane, tag: str, now: datetime):
    bundle = StructureBundleArtifact.from_bytes(f'{{"kind":"commissioning-{tag}"}}\n'.encode())
    specs = control_plane.enqueue_structure_generation(
        identity=_identity(
            tag,
            f"commissioning:{tag}",
            sha256(f"{tag}:comparison".encode()).hexdigest(),
            "legacy-publication-v1",
        ),
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )
    if len(specs) != 1:
        raise DisposableCommissioningError("structure-generation-shape")
    return specs[0], bundle


def _prepare_structure_fetch(
    cp: PostgresControlPlane,
    tag: str,
    now: datetime,
    *,
    progress_through: str | None = None,
) -> PreparedNormalTurn:
    window = f"commissioning:{tag}:source"
    cp.admit_structure_source_window(window_key=window, now=now)
    lease = _claim(cp, "structure-fetch", now)
    _record_progress(cp, lease, now, through=progress_through)

    def commit(active_lease: JobLease, completed_at: datetime) -> None:
        _require(
            cp.record_structure_source_page(
                active_lease,
                artifact_key=f"structure-source/{tag}.json",
                artifact_digest=sha256(tag.encode()).hexdigest(),
                next_cursor=None,
                completed=True,
                record_count=1,
                now=completed_at,
            ),
            "source-successor-missing",
        )

    return PreparedNormalTurn(cp, lease, commit)


def _prepare_structure_materialize(
    cp: PostgresControlPlane,
    tag: str,
    now: datetime,
    *,
    progress_through: str | None = None,
) -> PreparedNormalTurn:
    window = f"commissioning:{tag}:materialize"
    cp.admit_structure_source_window(window_key=window, now=now)
    source = _claim(cp, "structure-fetch", now)
    cp.record_structure_source_page(
        source,
        artifact_key=f"structure-source/{tag}.json",
        artifact_digest=sha256(tag.encode()).hexdigest(),
        next_cursor=None,
        completed=True,
        record_count=1,
        event_embedded_markets=True,
        now=now,
    )
    lease = _claim(cp, "structure-materialize", now)
    bundle = StructureBundleArtifact.from_bytes(f'{{"kind":"{tag}"}}\n'.encode())
    identity = _identity(
        tag,
        window,
        cp.structure_source_window_digest(window),
        "gamma-source-window-events-v3-sharded",
    )
    _record_progress(cp, lease, now, through=progress_through)

    def commit(active_lease: JobLease, completed_at: datetime) -> None:
        specs = cp.admit_structure_source_bundle(
            active_lease,
            identity=identity,
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=completed_at,
        )
        _require(len(specs) == 1, "materialize-successor-missing")

    return PreparedNormalTurn(cp, lease, commit)


def _prepare_structure_normalize(
    cp: PostgresControlPlane,
    tag: str,
    now: datetime,
    *,
    progress_through: str | None = None,
) -> PreparedNormalTurn:
    spec, _bundle = _generation(cp, tag, now)
    lease = _claim(cp, "structure-normalize", now)
    _record_progress(cp, lease, now, through=progress_through)

    def commit(active_lease: JobLease, completed_at: datetime) -> None:
        cp.complete_structure_range(
            active_lease,
            range_digest=spec.range_digest,
            artifact_key=f"structure-ranges/{tag}.ndjson",
            artifact_digest=sha256(f"{tag}:range-artifact".encode()).hexdigest(),
            record_count=1,
            now=completed_at,
        )

    return PreparedNormalTurn(cp, lease, commit)


def _prepare_structure_certify(
    cp: PostgresControlPlane,
    tag: str,
    now: datetime,
    *,
    progress_through: str | None = None,
) -> PreparedNormalTurn:
    spec, bundle = _generation(cp, tag, now)
    range_digest = sha256(f"{tag}:range-artifact".encode()).hexdigest()
    range_lease = _claim(cp, "structure-normalize", now)
    cp.complete_structure_range(
        range_lease,
        range_digest=spec.range_digest,
        artifact_key=f"structure-ranges/{tag}.ndjson",
        artifact_digest=range_digest,
        record_count=1,
        now=now,
    )
    lease = _claim(cp, "structure-certify", now)
    _record_progress(cp, lease, now, through=progress_through)
    manifest = sha256(
        canonical_structure_manifest_bytes(
            generation_key=spec.generation_key,
            bundle_digest=bundle.sha256,
            receipts=(
                {
                    "job_key": spec.job_key,
                    "component": "events",
                    "ordinal": 0,
                    "range_digest": spec.range_digest,
                    "artifact_key": f"structure-ranges/{tag}.ndjson",
                    "artifact_digest": range_digest,
                    "record_count": 1,
                },
            ),
        )
    ).hexdigest()

    def commit(active_lease: JobLease, completed_at: datetime) -> None:
        cp.certify_structure_generation(
            active_lease,
            generation_key=spec.generation_key,
            artifact_key=f"structure-manifests/{manifest}/manifest.ndjson",
            artifact_digest=manifest,
            now=completed_at,
        )

    return PreparedNormalTurn(cp, lease, commit)


def _prepare_quote_admit(
    cp: PostgresControlPlane,
    tag: str,
    now: datetime,
    *,
    progress_through: str | None = None,
) -> PreparedNormalTurn:
    structure = sha256(f"{tag}:structure".encode()).hexdigest()
    universe = sha256(f"{tag}:universe".encode()).hexdigest()
    generation = f"structure:{structure}"
    job_key = f"{generation}:quote-admit"
    bundle_key = f"bundles/{tag}.ndjson"
    cp.enqueue_job(
        job_key=job_key,
        job_type="quote-admit",
        input_identity=f"{generation}:{bundle_key}:{structure}",
        now=now,
    )
    with cp._connection_factory() as connection:  # noqa: SLF001
        connection.execute(
            """
            INSERT INTO m1_structure_generation_inputs
                (generation_key, bundle_key, bundle_digest, identity, admitted_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (generation, bundle_key, structure, Jsonb({}), now),
        )
        connection.execute(
            """
            INSERT INTO m1_quote_admission_inputs
                (job_key, generation_key, bundle_key, bundle_digest, admitted_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (job_key, generation, bundle_key, structure, now),
        )
    lease = _claim(cp, "quote-admit", now)
    _record_progress(cp, lease, now, through=progress_through)
    legs = (_leg(f"{tag}-token"),)
    batches = cp.quote_batches_from_legs(
        structure_receipt_digest=structure, universe_hash=universe, legs=legs, batch_size=1
    )
    artifacts = {
        batch.job_key: (
            f"quote-inputs/{sha256(batch.job_key.encode()).hexdigest()}/batch.ndjson",
            sha256(batch.job_key.encode()).hexdigest(),
            len(batch.legs),
        )
        for batch in batches
    }

    def commit(active_lease: JobLease, completed_at: datetime) -> None:
        cp.admit_quote_generation(
            active_lease,
            structure_receipt_digest=structure,
            universe_hash=universe,
            legs=legs,
            batch_size=1,
            input_artifacts=artifacts,
            now=completed_at,
        )

    return PreparedNormalTurn(cp, lease, commit)


def _prepare_quote_batch(
    cp: PostgresControlPlane,
    tag: str,
    now: datetime,
    *,
    progress_through: str | None = None,
) -> PreparedNormalTurn:
    batch = cp.enqueue_quote_generation(
        structure_receipt_digest=sha256(f"{tag}:structure".encode()).hexdigest(),
        universe_hash=sha256(f"{tag}:universe".encode()).hexdigest(),
        legs=(_leg(f"{tag}-token"),),
        batch_size=1,
        now=now,
    )[0]
    lease = _claim(cp, "quote-batch", now)
    _record_progress(cp, lease, now, through=progress_through)

    def commit(active_lease: JobLease, completed_at: datetime) -> None:
        cp.record_quote_batch(
            active_lease,
            token_range_digest=batch.token_range_digest,
            quote_digest=sha256(f"{tag}:quote".encode()).hexdigest(),
            artifact_key=f"quote-batches/{tag}.ndjson",
            artifact_digest=sha256(f"{tag}:artifact".encode()).hexdigest(),
            successful_response_count=1,
            quoted_at=completed_at,
            now=completed_at,
            terminal=True,
        )

    return PreparedNormalTurn(cp, lease, commit)


def _prepare_quote_certify(
    cp: PostgresControlPlane,
    tag: str,
    now: datetime,
    *,
    progress_through: str | None = None,
) -> PreparedNormalTurn:
    batches = cp.enqueue_quote_generation(
        structure_receipt_digest=sha256(f"{tag}:structure".encode()).hexdigest(),
        universe_hash=sha256(f"{tag}:universe".encode()).hexdigest(),
        legs=(_leg(f"{tag}-a"), _leg(f"{tag}-b")),
        batch_size=1,
        now=now,
    )
    for index, batch in enumerate(batches):
        batch_lease = _claim(cp, "quote-batch", now + timedelta(seconds=index))
        cp.record_quote_batch(
            batch_lease,
            token_range_digest=batch.token_range_digest,
            quote_digest=sha256(f"{tag}:quote:{index}".encode()).hexdigest(),
            artifact_key=f"quote-batches/{tag}-{index}.ndjson",
            artifact_digest=sha256(f"{tag}:artifact:{index}".encode()).hexdigest(),
            successful_response_count=1,
            quoted_at=now,
            now=now + timedelta(seconds=index),
            terminal=True,
        )
    at = now + timedelta(seconds=len(batches))
    lease = _claim(cp, "quote-certify", at)
    _record_progress(cp, lease, at, through=progress_through)

    def commit(active_lease: JobLease, completed_at: datetime) -> None:
        cp.certify_quote_generation(
            active_lease,
            generation_key=batches[0].generation_key,
            now=completed_at,
        )

    return PreparedNormalTurn(cp, lease, commit)


def _structure_prerequisite(cp: PostgresControlPlane, tag: str, now: datetime) -> tuple[str, str]:
    prepared = _prepare_structure_certify(cp, f"{tag}:structure", now)
    prepared.complete(now=now + timedelta(seconds=5))
    generation = prepared.lease.job_key.removesuffix(":certify")
    with cp._connection_factory() as connection:  # noqa: SLF001
        row = connection.execute(
            "SELECT bundle_digest FROM m1_structure_generation_inputs WHERE generation_key=%s",
            (generation,),
        ).fetchone()
    return generation, str(_require(row, "structure-prerequisite-missing")[0])


def _quote_prerequisite(cp: PostgresControlPlane, tag: str, structure: str, now: datetime) -> str:
    batch = cp.enqueue_quote_generation(
        structure_receipt_digest=structure,
        universe_hash=sha256(f"{tag}:universe".encode()).hexdigest(),
        legs=(_leg(f"{tag}-token"),),
        batch_size=1,
        now=now,
    )[0]
    lease = _claim(cp, "quote-batch", now)
    cp.record_quote_batch(
        lease,
        token_range_digest=batch.token_range_digest,
        quote_digest=sha256(f"{tag}:quote".encode()).hexdigest(),
        artifact_key=f"quote-batches/{tag}.ndjson",
        artifact_digest=sha256(f"{tag}:artifact".encode()).hexdigest(),
        successful_response_count=1,
        quoted_at=now,
        now=now,
        terminal=True,
    )
    certifier = _claim(cp, "quote-certify", now)
    cp.certify_quote_generation(certifier, generation_key=batch.generation_key, now=now)
    return batch.generation_key


def _prepare_opportunity_certify(
    cp: PostgresControlPlane,
    tag: str,
    now: datetime,
    *,
    progress_through: str | None = None,
) -> PreparedNormalTurn:
    structure_generation, structure_digest = _structure_prerequisite(cp, tag, now)
    quote_generation = _quote_prerequisite(cp, tag, structure_digest, now + timedelta(seconds=10))
    claimed_at = now + timedelta(seconds=20)
    lease = _claim(cp, "opportunity-certify", claimed_at)
    _record_progress(cp, lease, claimed_at, through=progress_through)
    rows = (
        {
            "group_id": f"{tag}-group",
            "event_id": f"{tag}-event",
            "membership_hash": f"{tag}-membership",
            "bundle_cost": 0.91,
            "gross_edge_bps": 900.0,
            "max_bundle_size": 4.0,
            "legs": [{"yes_token_id": f"{tag}-token", "ask_price": 0.91, "ask_size": 4.0}],
            "structure_observed_at_ms": 1,
            "quote_started_at_ms": 2,
            "quote_quoted_at_ms": 3,
        },
    )

    def commit(active_lease: JobLease, completed_at: datetime) -> None:
        cp.publish_opportunity_projection(
            quote_generation_key=quote_generation,
            structure_generation_key=structure_generation,
            rows=rows,
            now=completed_at,
            lease=active_lease,
        )

    return PreparedNormalTurn(cp, lease, commit)


def _postcondition_fact(connection: Any, lease: JobLease) -> str:
    if lease.job_type == "structure-fetch":
        row = connection.execute(
            "SELECT job_key FROM m1_structure_source_page_receipts WHERE job_key=%s",
            (lease.job_key,),
        ).fetchone()
        table = "m1_structure_source_page_receipts"
    elif lease.job_type == "structure-materialize":
        row = connection.execute(
            "SELECT window_key FROM m1_structure_source_window_bundles WHERE producer_job_key=%s",
            (lease.job_key,),
        ).fetchone()
        table = "m1_structure_source_window_bundles"
    elif lease.job_type == "structure-normalize":
        row = connection.execute(
            "SELECT job_key FROM m1_structure_range_receipts WHERE job_key=%s", (lease.job_key,)
        ).fetchone()
        table = "m1_structure_range_receipts"
    elif lease.job_type == "structure-certify":
        generation = lease.job_key.removesuffix(":certify")
        row = connection.execute(
            "SELECT job_key FROM m1_quote_admission_inputs WHERE generation_key=%s", (generation,)
        ).fetchone()
        table = "m1_quote_admission_inputs"
    elif lease.job_type == "quote-admit":
        rows = connection.execute(
            """
            SELECT batch.job_key
            FROM m1_quote_batch_inputs AS batch
            JOIN m1_quote_admission_inputs AS admission
              ON admission.job_key = %s
             AND admission.bundle_digest = batch.structure_receipt_digest
            ORDER BY batch.job_key
            """,
            (lease.job_key,),
        ).fetchall()
        if len(rows) != 1:
            raise DisposableCommissioningError("postcondition-ambiguous:quote-admit")
        row = rows[0]
        table = "m1_quote_batch_inputs"
    elif lease.job_type == "quote-batch":
        row = connection.execute(
            "SELECT job_key FROM m1_quote_batch_receipts WHERE job_key=%s", (lease.job_key,)
        ).fetchone()
        table = "m1_quote_batch_receipts"
    elif lease.job_type == "quote-certify":
        generation = lease.job_key.removesuffix(":certify")
        row = connection.execute(
            """
            SELECT generation_key FROM m1_publication_pointers
            WHERE pointer_key = 'quote:current' AND generation_key = %s
            """,
            (generation,),
        ).fetchone()
        table = "m1_publication_pointers"
    else:
        generation = lease.job_key.removesuffix(":opportunity-certify")
        row = connection.execute(
            """
            SELECT generation_key FROM m1_opportunity_publication_pointers
            WHERE pointer_key = 'opportunity:current' AND generation_key = %s
            """,
            (generation,),
        ).fetchone()
        table = "m1_opportunity_publication_pointers"
    if row is None:
        raise DisposableCommissioningError(f"postcondition-missing:{lease.job_type}")
    return f"postgres:{table}:{row[0]}"


__all__ = [
    "DisposableCommissioningError",
    "HeartbeatOutageCommissioningAdapter",
    "NormalizationPayloadCorruptCommissioningAdapter",
    "PreparedNormalTurn",
    "PublicationPointerConflictCommissioningAdapter",
    "ProgressStallCommissioningAdapter",
    "R2ReadTimeoutCommissioningAdapter",
    "QuoteAdmissionMissingShardCommissioningAdapter",
    "QuoteBatchIncompleteCommissioningAdapter",
    "RetryBudgetCommissioningAdapter",
    "SourceReceiptGapCommissioningAdapter",
    "StructureParityMismatchCommissioningAdapter",
    "StaleOwnerCommissioningAdapter",
    "WorkerExitCommissioningAdapter",
    "complete_normal_turn",
    "prepare_normal_turn",
]
