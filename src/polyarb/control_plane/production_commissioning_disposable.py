"""Real transactional normal-turn fixtures for disposable commissioning databases."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

from psycopg.types.json import Jsonb

from .models import JobLease, QuoteBatchLeg
from .postgres import PostgresControlPlane
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

    if node_id not in RUNTIME_STAGE_REGISTRY:
        raise DisposableCommissioningError("unknown-node")
    if not experiment_id or "\x00" in experiment_id or len(experiment_id) > 200:
        raise DisposableCommissioningError("invalid-experiment-id")
    if now.tzinfo is None or now.utcoffset() is None:
        raise DisposableCommissioningError("invalid-now")
    now = now.astimezone(UTC)
    completers = {
        "structure-fetch": _complete_structure_fetch,
        "structure-materialize": _complete_structure_materialize,
        "structure-normalize": _complete_structure_normalize,
        "structure-certify": _complete_structure_certify,
        "quote-admit": _complete_quote_admit,
        "quote-batch": _complete_quote_batch,
        "quote-certify": _complete_quote_certify,
        "opportunity-certify": _complete_opportunity_certify,
    }
    lease = completers[node_id](control_plane, experiment_id, now)
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


def _complete_structure_fetch(cp: PostgresControlPlane, tag: str, now: datetime) -> JobLease:
    window = f"commissioning:{tag}:source"
    cp.admit_structure_source_window(window_key=window, now=now)
    lease = _claim(cp, "structure-fetch", now)
    _record_progress(cp, lease, now)
    _require(
        cp.record_structure_source_page(
            lease,
            artifact_key=f"structure-source/{tag}.json",
            artifact_digest=sha256(tag.encode()).hexdigest(),
            next_cursor=None,
            completed=True,
            record_count=1,
            now=now + timedelta(seconds=2),
        ),
        "source-successor-missing",
    )
    return lease


def _complete_structure_materialize(cp: PostgresControlPlane, tag: str, now: datetime) -> JobLease:
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
    _record_progress(cp, lease, now)
    specs = cp.admit_structure_source_bundle(
        lease,
        identity=_identity(
            tag,
            window,
            cp.structure_source_window_digest(window),
            "gamma-source-window-events-v3-sharded",
        ),
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now + timedelta(seconds=2),
    )
    _require(len(specs) == 1, "materialize-successor-missing")
    return lease


def _complete_structure_normalize(cp: PostgresControlPlane, tag: str, now: datetime) -> JobLease:
    spec, _bundle = _generation(cp, tag, now)
    lease = _claim(cp, "structure-normalize", now)
    _record_progress(cp, lease, now)
    cp.complete_structure_range(
        lease,
        range_digest=spec.range_digest,
        artifact_key=f"structure-ranges/{tag}.ndjson",
        artifact_digest=sha256(f"{tag}:range-artifact".encode()).hexdigest(),
        record_count=1,
        now=now + timedelta(seconds=2),
    )
    return lease


def _complete_structure_certify(cp: PostgresControlPlane, tag: str, now: datetime) -> JobLease:
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
    cp.certify_structure_generation(
        lease,
        generation_key=spec.generation_key,
        artifact_key=f"structure-manifests/{manifest}/manifest.ndjson",
        artifact_digest=manifest,
        now=now + timedelta(seconds=2),
    )
    return lease


def _complete_quote_admit(cp: PostgresControlPlane, tag: str, now: datetime) -> JobLease:
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
    cp.admit_quote_generation(
        lease,
        structure_receipt_digest=structure,
        universe_hash=universe,
        legs=legs,
        batch_size=1,
        input_artifacts=artifacts,
        now=now + timedelta(seconds=2),
    )
    return lease


def _complete_quote_batch(cp: PostgresControlPlane, tag: str, now: datetime) -> JobLease:
    batch = cp.enqueue_quote_generation(
        structure_receipt_digest=sha256(f"{tag}:structure".encode()).hexdigest(),
        universe_hash=sha256(f"{tag}:universe".encode()).hexdigest(),
        legs=(_leg(f"{tag}-token"),),
        batch_size=1,
        now=now,
    )[0]
    lease = _claim(cp, "quote-batch", now)
    _record_progress(cp, lease, now)
    cp.record_quote_batch(
        lease,
        token_range_digest=batch.token_range_digest,
        quote_digest=sha256(f"{tag}:quote".encode()).hexdigest(),
        artifact_key=f"quote-batches/{tag}.ndjson",
        artifact_digest=sha256(f"{tag}:artifact".encode()).hexdigest(),
        successful_response_count=1,
        quoted_at=now,
        now=now + timedelta(seconds=2),
        terminal=True,
    )
    return lease


def _complete_quote_certify(cp: PostgresControlPlane, tag: str, now: datetime) -> JobLease:
    batches = cp.enqueue_quote_generation(
        structure_receipt_digest=sha256(f"{tag}:structure".encode()).hexdigest(),
        universe_hash=sha256(f"{tag}:universe".encode()).hexdigest(),
        legs=(_leg(f"{tag}-a"), _leg(f"{tag}-b")),
        batch_size=1,
        now=now,
    )
    for index, batch in enumerate(batches):
        lease = _claim(cp, "quote-batch", now + timedelta(seconds=index))
        cp.record_quote_batch(
            lease,
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
    cp.certify_quote_generation(
        lease, generation_key=batches[0].generation_key, now=at + timedelta(seconds=1)
    )
    return lease


def _structure_prerequisite(cp: PostgresControlPlane, tag: str, now: datetime) -> tuple[str, str]:
    lease = _complete_structure_certify(cp, f"{tag}:structure", now)
    generation = lease.job_key.removesuffix(":certify")
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


def _complete_opportunity_certify(cp: PostgresControlPlane, tag: str, now: datetime) -> JobLease:
    structure_generation, structure_digest = _structure_prerequisite(cp, tag, now)
    quote_generation = _quote_prerequisite(cp, tag, structure_digest, now + timedelta(seconds=10))
    lease = _claim(cp, "opportunity-certify", now + timedelta(seconds=20))
    _record_progress(cp, lease, now + timedelta(seconds=20))
    cp.publish_opportunity_projection(
        quote_generation_key=quote_generation,
        structure_generation_key=structure_generation,
        rows=(
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
        ),
        now=now + timedelta(seconds=22),
        lease=lease,
    )
    return lease


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


__all__ = ["DisposableCommissioningError", "complete_normal_turn"]
