"""Real transactional normal-turn fixtures for disposable commissioning databases."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

from psycopg.types.json import Jsonb

from .models import JobLease, QuoteBatchLeg
from .postgres import PostgresControlPlane, StaleLeaseError
from .production_commissioning_runner import AttackIdentity, AttackStageReceipt
from .runtime_contract import RUNTIME_STAGE_REGISTRY, AttemptRuntime
from .runtime_deadlines import runtime_deadline_profile
from .runtime_models import RuntimeEventKind
from .structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    canonical_structure_manifest_bytes,
)


class DisposableCommissioningError(RuntimeError):
    """A disposable database did not produce the required real durable fact."""


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
    return preparers[node_id](control_plane, experiment_id, now)


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


def _record_progress(control_plane: PostgresControlPlane, lease: JobLease, now: datetime) -> None:
    runtime = AttemptRuntime(
        store=control_plane,
        lease=lease,
        profile=runtime_deadline_profile(lease.job_type, 120),
        clock=lambda: now + timedelta(seconds=1),
    )
    stages = RUNTIME_STAGE_REGISTRY[lease.job_type]
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
    cp: PostgresControlPlane, tag: str, now: datetime
) -> PreparedNormalTurn:
    window = f"commissioning:{tag}:source"
    cp.admit_structure_source_window(window_key=window, now=now)
    lease = _claim(cp, "structure-fetch", now)
    _record_progress(cp, lease, now)

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
    cp: PostgresControlPlane, tag: str, now: datetime
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
    _record_progress(cp, lease, now)

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
    cp: PostgresControlPlane, tag: str, now: datetime
) -> PreparedNormalTurn:
    spec, _bundle = _generation(cp, tag, now)
    lease = _claim(cp, "structure-normalize", now)
    _record_progress(cp, lease, now)

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
    cp: PostgresControlPlane, tag: str, now: datetime
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
    _record_progress(cp, lease, now)
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


def _prepare_quote_admit(cp: PostgresControlPlane, tag: str, now: datetime) -> PreparedNormalTurn:
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
    _record_progress(cp, lease, now)
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


def _prepare_quote_batch(cp: PostgresControlPlane, tag: str, now: datetime) -> PreparedNormalTurn:
    batch = cp.enqueue_quote_generation(
        structure_receipt_digest=sha256(f"{tag}:structure".encode()).hexdigest(),
        universe_hash=sha256(f"{tag}:universe".encode()).hexdigest(),
        legs=(_leg(f"{tag}-token"),),
        batch_size=1,
        now=now,
    )[0]
    lease = _claim(cp, "quote-batch", now)
    _record_progress(cp, lease, now)

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


def _prepare_quote_certify(cp: PostgresControlPlane, tag: str, now: datetime) -> PreparedNormalTurn:
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
    _record_progress(cp, lease, at)

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
    cp: PostgresControlPlane, tag: str, now: datetime
) -> PreparedNormalTurn:
    structure_generation, structure_digest = _structure_prerequisite(cp, tag, now)
    quote_generation = _quote_prerequisite(cp, tag, structure_digest, now + timedelta(seconds=10))
    claimed_at = now + timedelta(seconds=20)
    lease = _claim(cp, "opportunity-certify", claimed_at)
    _record_progress(cp, lease, claimed_at)
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
    "PreparedNormalTurn",
    "StaleOwnerCommissioningAdapter",
    "complete_normal_turn",
    "prepare_normal_turn",
]
