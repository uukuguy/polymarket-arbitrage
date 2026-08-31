"""Fenced, synchronous Postgres repository for durable M1 worker effects."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .models import (
    AlertDeliveryLease,
    CloudUsageDecision,
    CheckpointReceipt,
    JobLease,
    JobState,
    QuoteBatchLeg,
    QuoteBatchReceipt,
    QuoteBatchSpec,
    SourceAdmissionDecision,
    StructureRangeReceipt,
    StructureRangeSpec,
    StructureSourcePageSpec,
)
from .soak_evidence import SoakEvidenceError, _observed_at, _validated
from .structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    canonical_structure_manifest_bytes,
)


class ControlPlaneError(RuntimeError):
    """Base class for control-plane semantic failures."""


class StaleLeaseError(ControlPlaneError):
    """A worker attempted an effect after its lease was fenced out."""


class JobIdentityConflict(ControlPlaneError):
    """A deterministic job key was reused for a different input."""


class CheckpointConflictError(ControlPlaneError):
    """An idempotency key was reused for a different checkpoint."""


class IncompleteQuoteGenerationError(ControlPlaneError):
    """A Quote certifier cannot publish until every batch has a receipt."""


class OpportunityProjectionCurrentError(ControlPlaneError):
    """The current Quote already has its complete opportunity projection."""


class IncompleteStructureGenerationError(ControlPlaneError):
    """A Structure certifier cannot prove every admitted range is present."""


class SoakEvidenceConflictError(ControlPlaneError):
    """A cloud soak run or observation conflicts with immutable evidence."""

def _quote_batch_leg_payload(leg: QuoteBatchLeg) -> dict[str, str | None]:
    """Keep batch input JSON explicit and independent of routing internals."""
    return {
        "neg_risk_market_id": leg.neg_risk_market_id,
        "market_id": leg.market_id,
        "condition_id": leg.condition_id,
        "slug": leg.slug,
        "yes_token_id": leg.yes_token_id,
        "event_id": leg.event_id,
        "membership_hash": leg.membership_hash,
    }


def _persisted_legs(value: object) -> tuple[QuoteBatchLeg, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise JobIdentityConflict("quote batch legs must be a JSON array")
    legs: list[QuoteBatchLeg] = []
    for payload in value:
        if not isinstance(payload, Mapping):
            raise JobIdentityConflict("quote batch leg must be a JSON object")
        try:
            slug = payload.get("slug")
            if slug is not None and not isinstance(slug, str):
                raise TypeError("slug")
            legs.append(
                QuoteBatchLeg(
                    neg_risk_market_id=str(payload["neg_risk_market_id"]),
                    market_id=str(payload["market_id"]),
                    condition_id=str(payload["condition_id"]),
                    slug=slug,
                    yes_token_id=str(payload["yes_token_id"]),
                    event_id=str(payload.get("event_id", "")),
                    membership_hash=str(payload.get("membership_hash", "")),
                )
            )
        except (KeyError, TypeError, ValueError) as error:
            raise JobIdentityConflict("quote batch leg is malformed") from error
    return tuple(legs)


ConnectionFactory = Callable[[], psycopg.Connection[Any]]


class PostgresControlPlane:
    """Own atomic job transitions; callers provide the connection factory."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connection_factory = connection_factory

    def start_soak_run(
        self, *, run_id: str, baseline_record: Mapping[str, object]
    ) -> None:
        """Create one immutable cloud soak run, or prove its exact replay."""
        if not run_id:
            raise ValueError("run_id must be non-empty")
        baseline = _validated(baseline_record)
        if baseline["kind"] != "m1-transactional-soak-v2":
            raise SoakEvidenceError("cloud soak runs require V2 evidence")
        machine_ids = sorted(str(machine_id) for machine_id in baseline["machine_states"])
        digest = str(baseline_record["snapshot_sha256"])
        started_at = _observed_at(str(baseline["observed_at"]))
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """
                INSERT INTO m1_soak_runs (
                    run_id, control_api_url, machine_ids, baseline_record,
                    baseline_snapshot_sha256, started_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO NOTHING
                """,
                (
                    run_id,
                    baseline["control_api_url"],
                    Jsonb(machine_ids),
                    Jsonb(dict(baseline_record)),
                    digest,
                    started_at,
                ),
            )
            cursor.execute(
                """
                SELECT control_api_url, machine_ids, baseline_snapshot_sha256
                FROM m1_soak_runs WHERE run_id = %s
                """,
                (run_id,),
            )
            persisted = cursor.fetchone()
            if persisted is None or (
                persisted["control_api_url"] != baseline["control_api_url"]
                or persisted["machine_ids"] != machine_ids
                or persisted["baseline_snapshot_sha256"] != digest
            ):
                raise SoakEvidenceConflictError("cloud soak run identity conflicts")
            cursor.execute(
                """
                INSERT INTO m1_soak_observations (run_id, observed_at, record, snapshot_sha256)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, observed_at) DO NOTHING
                """,
                (run_id, started_at, Jsonb(dict(baseline_record)), digest),
            )
            cursor.execute(
                """
                SELECT snapshot_sha256 FROM m1_soak_observations
                WHERE run_id = %s AND observed_at = %s
                """,
                (run_id, started_at),
            )
            baseline_observation = cursor.fetchone()
            if baseline_observation is None or baseline_observation["snapshot_sha256"] != digest:
                raise SoakEvidenceConflictError("cloud soak baseline conflicts")

    def append_soak_observation(
        self, *, run_id: str, record: Mapping[str, object]
    ) -> None:
        """Append one canonical observation; exact retransmission is harmless."""
        observation = _validated(record)
        if observation["kind"] != "m1-transactional-soak-v2":
            raise SoakEvidenceError("cloud soak observations require V2 evidence")
        observed_at = _observed_at(str(observation["observed_at"]))
        digest = str(record["snapshot_sha256"])
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """
                SELECT control_api_url, machine_ids FROM m1_soak_runs WHERE run_id = %s
                """,
                (run_id,),
            )
            run = cursor.fetchone()
            machine_ids = sorted(str(machine_id) for machine_id in observation["machine_states"])
            if run is None or run["control_api_url"] != observation["control_api_url"] or run[
                "machine_ids"
            ] != machine_ids:
                raise SoakEvidenceConflictError("cloud soak observation identity conflicts")
            cursor.execute(
                """
                INSERT INTO m1_soak_observations (run_id, observed_at, record, snapshot_sha256)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, observed_at) DO NOTHING
                """,
                (run_id, observed_at, Jsonb(dict(record)), digest),
            )
            cursor.execute(
                """
                SELECT snapshot_sha256 FROM m1_soak_observations
                WHERE run_id = %s AND observed_at = %s
                """,
                (run_id, observed_at),
            )
            persisted = cursor.fetchone()
            if persisted is None or persisted["snapshot_sha256"] != digest:
                raise SoakEvidenceConflictError("cloud soak observation conflicts")

    def read_soak_observations(self, run_id: str) -> tuple[dict[str, object], ...]:
        """Return immutable cloud observations in verifier order."""
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT record FROM m1_soak_observations
                WHERE run_id = %s ORDER BY observed_at ASC
                """,
                (run_id,),
            )
            return tuple(dict(row["record"]) for row in cursor.fetchall())

    def deployment_preflight(self, *, expected_database: str) -> dict[str, object]:
        """Prove the named authority has the complete additive 021 schema.

        This is intentionally read-only: passing it authorizes shadow-only
        operator steps, never a migration, scheduler loop, or pointer change.
        """
        if not expected_database:
            raise ValueError("expected_database must be non-empty")
        required_tables = (
            "m1_jobs",
            "m1_job_circuits",
            "m1_job_attempts",
            "m1_checkpoint_receipts",
            "m1_quote_batch_inputs",
            "m1_quote_batch_receipts",
            "m1_quote_admission_inputs",
            "m1_structure_generation_inputs",
            "m1_structure_range_inputs",
            "m1_structure_range_receipts",
            "m1_generation_manifests",
            "m1_publication_pointers",
            "m1_incidents",
            "m1_incident_events",
            "m1_alert_outbox",
            "m1_alert_deliveries",
            "m1_structure_source_windows",
            "m1_structure_source_page_inputs",
            "m1_structure_source_page_receipts",
            "m1_structure_source_window_bundles",
            "m1_cloud_usage_observations",
        )
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5000ms'")
            cursor.execute(
                "SELECT current_database() AS database_name, version() AS postgres_version"
            )
            identity = cursor.fetchone()
            if identity is None or str(identity["database_name"]) != expected_database:
                raise ControlPlaneError("control-plane database identity mismatch")
            cursor.execute(
                """
                SELECT relname
                FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                WHERE pg_namespace.nspname = 'public' AND relkind = 'r'
                  AND relname = ANY(%s)
                """,
                (list(required_tables),),
            )
            found = {str(row["relname"]) for row in cursor.fetchall()}
            if found != set(required_tables):
                raise ControlPlaneError("control-plane revision 021 schema is incomplete")
            cursor.execute(
                """
                SELECT attname FROM pg_catalog.pg_attribute
                WHERE attrelid = 'public.m1_alert_outbox'::regclass
                  AND attnum > 0 AND NOT attisdropped
                  AND attname = ANY(%s)
                """,
                (["lease_owner", "lease_epoch", "lease_expires_at"],),
            )
            delivery_lease_columns = {str(row["attname"]) for row in cursor.fetchall()}
            if delivery_lease_columns != {"lease_owner", "lease_epoch", "lease_expires_at"}:
                raise ControlPlaneError("control-plane alert delivery lease schema is incomplete")
            return {
                "database_name": str(identity["database_name"]),
                "postgres_version": str(identity["postgres_version"]),
                "revision_021_tables": len(found),
            }

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
            self._enqueue_job_cursor(
                cursor,
                job_key=job_key,
                job_type=job_type,
                input_identity=input_identity,
                now=now,
            )

    def admit_structure_source_window(
        self,
        *,
        window_key: str,
        now: datetime,
    ) -> tuple[StructureSourcePageSpec, ...]:
        """Create one source window and its first, restart-safe event page.

        The input cursor is deliberately absent for ordinal zero.  Markets are
        not admitted here: an event terminal receipt is the only authority
        allowed to release the market traversal for the same source window.
        """
        self._validate_nonempty(window_key=window_key)
        self._validate_aware(now, "now")
        first = StructureSourcePageSpec(
            window_key=window_key,
            stream="events",
            ordinal=0,
            requested_cursor=None,
        )
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                INSERT INTO m1_structure_source_windows (
                    window_key, state, admitted_at, updated_at
                ) VALUES (%s, 'running', %s, %s)
                ON CONFLICT (window_key) DO NOTHING
                """,
                (window_key, now, now),
            )
            self._enqueue_structure_source_page_cursor(cursor, spec=first, now=now)
        return (first,)

    def admit_due_structure_source_window(
        self,
        *,
        cadence_seconds: int,
        now: datetime,
        structure_high_water: int = 2_000,
        quote_high_water: int = 512,
    ) -> SourceAdmissionDecision:
        """Admit one deterministic current window unless a source traversal is active.

        The advisory transaction lock gives every replaceable scheduler process
        the same admission boundary. A time bucket is the durable idempotency
        key: restarts cannot create another collection in the same cadence.
        """
        if (
            isinstance(cadence_seconds, bool)
            or cadence_seconds <= 0
            or isinstance(structure_high_water, bool)
            or structure_high_water <= 0
            or isinstance(quote_high_water, bool)
            or quote_high_water <= 0
        ):
            raise ValueError("source admission bounds must be positive")
        self._validate_aware(now, "now")
        bucket = int(now.timestamp()) // cadence_seconds
        window_key = f"structure-source:{cadence_seconds}:{bucket}"
        first = StructureSourcePageSpec(
            window_key=window_key,
            stream="events",
            ordinal=0,
            requested_cursor=None,
        )
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("m1:structure-source-window-admission",),
            )
            cursor.execute(
                """
                SELECT count(*) AS count FROM m1_jobs
                WHERE job_type = 'structure-normalize'
                  AND state IN ('runnable', 'retryable', 'leased', 'checkpointed')
                """
            )
            structure_unfinished = cursor.fetchone()
            if int(structure_unfinished["count"]) >= structure_high_water:
                return SourceAdmissionDecision(state="backpressured:structure", job_key=None)
            cursor.execute(
                """
                SELECT count(*) AS count FROM m1_jobs
                WHERE job_type = 'quote-batch'
                  AND state IN ('runnable', 'retryable', 'leased', 'checkpointed')
                """
            )
            quote_unfinished = cursor.fetchone()
            if int(quote_unfinished["count"]) >= quote_high_water:
                return SourceAdmissionDecision(state="backpressured:quote", job_key=None)
            cursor.execute(
                """
                SELECT window_key FROM m1_structure_source_windows
                WHERE state IN ('running', 'events-complete')
                ORDER BY admitted_at
                LIMIT 1
                FOR UPDATE
                """
            )
            if cursor.fetchone() is not None:
                return SourceAdmissionDecision(state="busy", job_key=None)
            cursor.execute(
                "SELECT window_key FROM m1_structure_source_windows WHERE window_key = %s",
                (window_key,),
            )
            if cursor.fetchone() is not None:
                return SourceAdmissionDecision(state="busy", job_key=None)
            cursor.execute(
                """
                INSERT INTO m1_structure_source_windows (
                    window_key, state, admitted_at, updated_at
                ) VALUES (%s, 'running', %s, %s)
                """,
                (window_key, now, now),
            )
            self._enqueue_structure_source_page_cursor(cursor, spec=first, now=now)
        return SourceAdmissionDecision(state="admitted", job_key=first.job_key)

    def structure_source_page_spec(self, job_key: str) -> StructureSourcePageSpec:
        """Load an admitted source page exactly as a replacement worker sees it."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """
                SELECT window_key, stream, ordinal, requested_cursor,
                       market_ids_json, market_ids_digest
                FROM m1_structure_source_page_inputs
                WHERE job_key = %s
                """,
                (job_key,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"Structure source page is unavailable for {job_key!r}")
        return self._structure_source_page_spec_from_row(row)

    def structure_source_page_receipt(self, job_key: str) -> dict[str, object] | None:
        """Return one authenticated source-page effect, never a mutable cursor view."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """
                SELECT artifact_key, artifact_digest, next_cursor, completed, record_count
                FROM m1_structure_source_page_receipts
                WHERE job_key = %s
                """,
                (job_key,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {
            "artifact_key": str(row["artifact_key"]),
            "artifact_digest": str(row["artifact_digest"]),
            "next_cursor": (None if row["next_cursor"] is None else str(row["next_cursor"])),
            "completed": bool(row["completed"]),
            "record_count": int(row["record_count"]),
        }

    def quarantine_structure_source_page(
        self,
        lease: JobLease,
        *,
        error_class: str,
        now: datetime,
    ) -> None:
        """Fail-close one leased source page and release its window for a later bucket."""
        self._validate_aware(now, "now")
        self._validate_nonempty(error_class=error_class)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT window_key FROM m1_structure_source_page_inputs
                WHERE job_key = %s
                """,
                (lease.job_key,),
            )
            page = cursor.fetchone()
            if page is None:
                raise ControlPlaneError(
                    f"Structure source page is unavailable for {lease.job_key!r}"
                )
            cursor.execute(
                """
                UPDATE m1_jobs
                SET state = 'quarantined', next_attempt_at = NULL, last_error_class = %s,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                  AND state IN ('leased', 'checkpointed')
                """,
                (error_class, now, lease.job_key, lease.lease_owner, lease.lease_epoch),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            cursor.execute(
                """
                UPDATE m1_jobs AS sibling
                SET state = 'quarantined', next_attempt_at = NULL, last_error_class = %s,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
                FROM m1_structure_source_page_inputs AS sibling_input
                WHERE sibling_input.window_key = %s
                  AND sibling_input.job_key = sibling.job_key
                  AND sibling.job_key <> %s
                  AND sibling.state IN ('runnable', 'retryable', 'checkpointed')
                """,
                (error_class, now, page["window_key"], lease.job_key),
            )
            cursor.execute(
                """
                UPDATE m1_job_attempts
                SET state = 'quarantined', finished_at = %s, error_class = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (now, error_class, lease.job_key, lease.lease_epoch),
            )
            cursor.execute(
                """
                UPDATE m1_structure_source_windows
                SET state = 'quarantined', updated_at = %s
                WHERE window_key = %s AND state IN ('running', 'events-complete')
                """,
                (now, page["window_key"]),
            )
            if cursor.rowcount != 1:
                raise CheckpointConflictError("source window is no longer active")

    def structure_source_window_digest(self, window_key: str) -> str:
        """Return the exact ordered page-receipt digest a materializer must bind."""
        self._validate_nonempty(window_key=window_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
            return self._structure_source_window_digest_cursor(
                cursor, window_key, lock_window=False
            )

    def structure_source_window_bundle(self, window_key: str) -> dict[str, str] | None:
        """Read the one immutable bundle receipt bound to a source window."""
        self._validate_nonempty(window_key=window_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """
                SELECT source_digest, bundle_key, bundle_digest
                FROM m1_structure_source_window_bundles WHERE window_key = %s
                """,
                (window_key,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {
            "source_digest": str(row["source_digest"]),
            "bundle_key": str(row["bundle_key"]),
            "bundle_digest": str(row["bundle_digest"]),
        }

    def structure_source_window_pages(
        self, window_key: str
    ) -> tuple[tuple[StructureSourcePageSpec, str, str], ...]:
        """Return only receipt-authenticated R2 page references for one terminal window."""
        self._validate_nonempty(window_key=window_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
            self._structure_source_window_digest_cursor(cursor, window_key, lock_window=False)
            cursor.execute(
                """
                SELECT input.window_key, input.stream, input.ordinal, input.requested_cursor,
                       input.market_ids_json, input.market_ids_digest,
                       receipt.artifact_key, receipt.artifact_digest
                FROM m1_structure_source_page_inputs AS input
                JOIN m1_structure_source_page_receipts AS receipt
                  ON receipt.job_key = input.job_key
                WHERE input.window_key = %s
                ORDER BY CASE input.stream WHEN 'events' THEN 0 WHEN 'markets' THEN 1 END,
                         input.ordinal
                """,
                (window_key,),
            )
            rows = cursor.fetchall()
        return tuple(
            (
                self._structure_source_page_spec_from_row(row),
                str(row["artifact_key"]),
                str(row["artifact_digest"]),
            )
            for row in rows
        )

    def structure_materializer_shards(self, window_key: str) -> tuple[tuple[str, str, str], ...]:
        """Return only fenced shard receipts for one source materializer job."""
        self._validate_nonempty(window_key=window_key)
        job_key = f"{window_key}:materialize"
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """
                SELECT checkpoint_cursor, checkpoint_digest, artifact_key
                FROM m1_checkpoint_receipts
                WHERE job_key = %s
                  AND checkpoint_cursor LIKE 'shard:%%'
                  AND artifact_key IS NOT NULL
                ORDER BY checkpoint_cursor
                """,
                (job_key,),
            )
            rows = cursor.fetchall()
        return tuple(
            (str(row["checkpoint_cursor"]), str(row["checkpoint_digest"]), str(row["artifact_key"]))
            for row in rows
        )

    def structure_materializer_batches(self, window_key: str) -> tuple[tuple[str, str, str], ...]:
        """Return the ordered immutable batch receipts for v3 finalization."""
        self._validate_nonempty(window_key=window_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """
                SELECT checkpoint_cursor, checkpoint_digest, artifact_key
                FROM m1_checkpoint_receipts
                WHERE job_key = %s
                  AND checkpoint_cursor LIKE 'shard-batch:%%'
                  AND artifact_key IS NOT NULL
                ORDER BY checkpoint_cursor
                """,
                (f"{window_key}:materialize",),
            )
            rows = cursor.fetchall()
        return tuple(
            (str(row["checkpoint_cursor"]), str(row["checkpoint_digest"]), str(row["artifact_key"]))
            for row in rows
        )

    def structure_source_event_pages(
        self, window_key: str
    ) -> tuple[tuple[StructureSourcePageSpec, str, str], ...]:
        """Return prior receipt-authenticated event evidence during terminal sealing.

        The terminal event worker calls this before it atomically changes the
        window to ``events-complete``.  Unlike the materializer-facing full
        page list, it therefore must not require a terminal window state.
        """
        self._validate_nonempty(window_key=window_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT input.window_key, input.stream, input.ordinal, input.requested_cursor,
                       input.market_ids_json, input.market_ids_digest,
                       receipt.artifact_key, receipt.artifact_digest
                FROM m1_structure_source_page_inputs AS input
                JOIN m1_structure_source_page_receipts AS receipt
                  ON receipt.job_key = input.job_key
                WHERE input.window_key = %s AND input.stream = 'events'
                ORDER BY input.ordinal
                """,
                (window_key,),
            )
            rows = cursor.fetchall()
        return tuple(
            (
                self._structure_source_page_spec_from_row(row),
                str(row["artifact_key"]),
                str(row["artifact_digest"]),
            )
            for row in rows
        )

    def admit_structure_source_bundle(
        self,
        lease: JobLease,
        *,
        identity: StructureBundleIdentity,
        bundle: StructureBundleArtifact,
        ranges: Sequence[tuple[str, str, str]],
        now: datetime,
    ) -> tuple[StructureRangeSpec, ...]:
        """Fence one source-window bundle receipt and all downstream range jobs together."""
        self._validate_aware(now, "now")
        if lease.job_type != "structure-materialize":
            raise ValueError("source bundle admission requires a structure-materialize lease")
        if identity.source_kind not in {
            "gamma-source-window-v1",
            "gamma-source-window-events-v2",
            "gamma-source-window-events-v3-sharded",
        }:
            raise ValueError("source bundle identity must name a Gamma source window")
        if identity.window_id != lease.input_identity:
            raise JobIdentityConflict("source bundle lease names another window")
        if not ranges:
            raise ValueError("Structure generation requires at least one range")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            source_digest = self._structure_source_window_digest_cursor(cursor, identity.window_id)
            if identity.comparison_receipt_digest != source_digest:
                raise CheckpointConflictError("source bundle does not bind current page receipts")
            cursor.execute(
                """
                SELECT source_digest, bundle_key, bundle_digest
                FROM m1_structure_source_window_bundles
                WHERE window_key = %s
                """,
                (identity.window_id,),
            )
            existing = cursor.fetchone()
            expected = {
                "source_digest": source_digest,
                "bundle_key": bundle.key,
                "bundle_digest": bundle.sha256,
            }
            if existing is not None:
                persisted = {
                    "source_digest": str(existing["source_digest"]),
                    "bundle_key": str(existing["bundle_key"]),
                    "bundle_digest": str(existing["bundle_digest"]),
                }
                if persisted != expected:
                    raise CheckpointConflictError("source window names another bundle")
                return self._structure_generation_specs_cursor(cursor, bundle.sha256)
            cursor.execute(
                """
                UPDATE m1_jobs
                SET state = 'succeeded', checkpoint_cursor = 'bundle', checkpoint_digest = %s,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (bundle.sha256, now, lease.job_key, lease.lease_owner, lease.lease_epoch),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            cursor.execute(
                """
                INSERT INTO m1_structure_source_window_bundles (
                    window_key, producer_job_key, source_digest, bundle_key, bundle_digest,
                    committed_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    identity.window_id,
                    lease.job_key,
                    source_digest,
                    bundle.key,
                    bundle.sha256,
                    now,
                ),
            )
            specs = self._enqueue_structure_generation_cursor(
                cursor, identity=identity, bundle=bundle, ranges=ranges, now=now
            )
            cursor.execute(
                """
                UPDATE m1_job_attempts
                SET state = 'succeeded', finished_at = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (now, lease.job_key, lease.lease_epoch),
            )
            return specs

    @staticmethod
    def _structure_generation_specs_cursor(
        cursor: psycopg.Cursor[dict[str, Any]], bundle_digest: str
    ) -> tuple[StructureRangeSpec, ...]:
        cursor.execute(
            """
            SELECT job_key, bundle_key, bundle_digest, component, ordinal, range_start, range_end
            FROM m1_structure_range_inputs WHERE bundle_digest = %s
            ORDER BY component, ordinal
            """,
            (bundle_digest,),
        )
        rows = cursor.fetchall()
        if not rows:
            raise ControlPlaneError("source bundle has no Structure range inputs")
        return tuple(
            StructureRangeSpec.create(
                bundle_key=str(row["bundle_key"]),
                bundle_digest=str(row["bundle_digest"]),
                component=str(row["component"]),
                ordinal=int(row["ordinal"]),
                range_start=str(row["range_start"]),
                range_end=str(row["range_end"]),
            )
            for row in rows
        )

    @staticmethod
    def _structure_source_window_digest_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        window_key: str,
        *,
        lock_window: bool = True,
    ) -> str:
        cursor.execute(
            "SELECT state FROM m1_structure_source_windows WHERE window_key = %s"
            + (" FOR SHARE" if lock_window else ""),
            (window_key,),
        )
        window = cursor.fetchone()
        if window is None or str(window["state"]) != "complete":
            raise IncompleteStructureGenerationError("source window is not terminal")
        cursor.execute(
            """
            SELECT input.stream, input.ordinal, receipt.artifact_digest
            FROM m1_structure_source_page_inputs AS input
            LEFT JOIN m1_structure_source_page_receipts AS receipt
              ON receipt.job_key = input.job_key
            WHERE input.window_key = %s
            ORDER BY CASE input.stream WHEN 'events' THEN 0 WHEN 'markets' THEN 1 END, input.ordinal
            """,
            (window_key,),
        )
        rows = cursor.fetchall()
        if not rows or any(row["artifact_digest"] is None for row in rows):
            raise IncompleteStructureGenerationError("source window is missing page receipts")
        receipts = [
            {
                "stream": str(row["stream"]),
                "ordinal": int(row["ordinal"]),
                "artifact_digest": str(row["artifact_digest"]),
            }
            for row in rows
        ]
        return sha256(
            json.dumps({"pages": receipts}, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def record_structure_source_page(
        self,
        lease: JobLease,
        *,
        artifact_key: str,
        artifact_digest: str,
        next_cursor: str | None,
        completed: bool,
        record_count: int,
        market_batches: tuple[tuple[str, ...], ...] | None = None,
        event_embedded_markets: bool = False,
        now: datetime,
    ) -> StructureSourcePageSpec | None:
        """Atomically record one source page and release only its legal successor.

        A process crash before this transaction leaves the original page leased
        and reclaimable.  A crash after it leaves both receipt and successor,
        so replay cannot skip the opaque continuation.
        """
        self._validate_aware(now, "now")
        if lease.job_type != "structure-fetch":
            raise ValueError("source page receipt requires a structure-fetch lease")
        self._validate_nonempty(artifact_key=artifact_key)
        if len(artifact_digest) != 64:
            raise ValueError("artifact_digest must be a sha256 digest")
        if isinstance(record_count, bool) or record_count < 0:
            raise ValueError("record_count must be non-negative")
        if completed and next_cursor is not None:
            raise ValueError("completed source page cannot name a successor cursor")
        if not completed and (next_cursor is None or not next_cursor):
            raise ValueError("incomplete source page requires a successor cursor")
        if type(event_embedded_markets) is not bool:
            raise TypeError("event_embedded_markets must be a bool")
        if event_embedded_markets and market_batches is not None:
            raise ValueError("event embedded markets cannot also name market batches")
        normalized_market_batches: tuple[tuple[str, ...], ...] | None = None
        if market_batches is not None:
            if not completed:
                raise ValueError("market batches require a terminal event page")
            normalized_market_batches = tuple(tuple(market_ids) for market_ids in market_batches)
            for ordinal, market_ids in enumerate(normalized_market_batches):
                StructureSourcePageSpec(
                    window_key="validated-window",
                    stream="markets",
                    ordinal=ordinal,
                    requested_cursor=None,
                    market_ids=tuple(market_ids),
                )
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            spec = self._structure_source_page_spec_cursor(cursor, lease.job_key)
            if lease.input_identity != spec.input_identity:
                raise JobIdentityConflict("source page lease identity does not match input")
            if normalized_market_batches is not None and spec.stream != "events":
                raise ValueError("market batches require an event source page")
            if event_embedded_markets and (
                spec.stream != "events" or not completed or next_cursor is not None
            ):
                raise ValueError("event embedded markets require a terminal event source page")
            if spec.market_ids and (not completed or next_cursor is not None):
                raise ValueError("scoped market batch must be terminal without a cursor")
            cursor.execute(
                """
                SELECT artifact_key, artifact_digest, next_cursor, completed, record_count
                FROM m1_structure_source_page_receipts WHERE job_key = %s
                """,
                (lease.job_key,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                persisted = {
                    "artifact_key": str(existing["artifact_key"]),
                    "artifact_digest": str(existing["artifact_digest"]),
                    "next_cursor": (
                        None if existing["next_cursor"] is None else str(existing["next_cursor"])
                    ),
                    "completed": bool(existing["completed"]),
                    "record_count": int(existing["record_count"]),
                }
                expected = {
                    "artifact_key": artifact_key,
                    "artifact_digest": artifact_digest,
                    "next_cursor": next_cursor,
                    "completed": completed,
                    "record_count": record_count,
                }
                if persisted != expected:
                    raise CheckpointConflictError(
                        f"source page receipt conflicts for {lease.job_key!r}"
                    )
                if spec.stream == "events" and completed and normalized_market_batches is not None:
                    return self._admit_scoped_market_batches_cursor(
                        cursor, event_spec=spec, market_batches=normalized_market_batches, now=now
                    )
                if event_embedded_markets:
                    self._complete_event_embedded_source_window_cursor(
                        cursor, event_spec=spec, now=now
                    )
                    return None
                return self._source_successor_spec_cursor(
                    cursor, spec=spec, next_cursor=next_cursor, completed=completed
                )
            cursor.execute(
                """
                UPDATE m1_jobs
                SET checkpoint_cursor = %s, checkpoint_digest = %s, state = 'succeeded',
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (
                    f"{spec.stream}:{spec.ordinal}",
                    artifact_digest,
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
                idempotency_key=f"structure-source-page:{lease.job_key}:{artifact_digest}",
                checkpoint_cursor=f"{spec.stream}:{spec.ordinal}",
                checkpoint_digest=artifact_digest,
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
                INSERT INTO m1_structure_source_page_receipts (
                    job_key, artifact_key, artifact_digest, next_cursor, completed,
                    record_count, committed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    lease.job_key,
                    artifact_key,
                    artifact_digest,
                    next_cursor,
                    completed,
                    record_count,
                    now,
                ),
            )
            cursor.execute(
                """
                UPDATE m1_job_attempts
                SET state = 'succeeded', finished_at = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (now, lease.job_key, lease.lease_epoch),
            )
            scoped_market_admission = (
                spec.stream == "events" and completed and normalized_market_batches is not None
            )
            embedded_event_completion = spec.stream == "events" and event_embedded_markets
            if scoped_market_admission:
                successor = self._admit_scoped_market_batches_cursor(
                    cursor, event_spec=spec, market_batches=normalized_market_batches, now=now
                )
            elif embedded_event_completion:
                self._complete_event_embedded_source_window_cursor(cursor, event_spec=spec, now=now)
                successor = None
            else:
                successor = self._source_successor_spec_cursor(
                    cursor, spec=spec, next_cursor=next_cursor, completed=completed
                )
            if successor is not None and not scoped_market_admission:
                self._enqueue_structure_source_page_cursor(cursor, spec=successor, now=now)
            elif completed and spec.stream == "markets":
                if spec.market_ids:
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM m1_structure_source_page_inputs AS input
                            LEFT JOIN m1_structure_source_page_receipts AS receipt
                              ON receipt.job_key = input.job_key
                            WHERE input.window_key = %s
                              AND input.stream = 'markets'
                              AND receipt.job_key IS NULL
                        ) AS has_unfinished_batches
                        """,
                        (spec.window_key,),
                    )
                    row = cursor.fetchone()
                    if row is None:
                        raise CheckpointConflictError(
                            "scoped market batch completion is unavailable"
                        )
                    if bool(row["has_unfinished_batches"]):
                        return None
                cursor.execute(
                    """
                    UPDATE m1_structure_source_windows
                    SET state = 'complete', updated_at = %s
                    WHERE window_key = %s AND state = 'events-complete'
                    """,
                    (now, spec.window_key),
                )
                if cursor.rowcount != 1:
                    raise CheckpointConflictError("market stream completed before events stream")
                materializer_job_key = f"{spec.window_key}:materialize"
                self._enqueue_job_cursor(
                    cursor,
                    job_key=materializer_job_key,
                    job_type="structure-materialize",
                    input_identity=spec.window_key,
                    now=now,
                )
            return successor

    def _complete_event_embedded_source_window_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        event_spec: StructureSourcePageSpec,
        now: datetime,
    ) -> None:
        """Release materialization from a terminal event-only source chain."""
        cursor.execute(
            """
            UPDATE m1_structure_source_windows
            SET state = 'complete', updated_at = %s
            WHERE window_key = %s AND state = 'running'
            """,
            (now, event_spec.window_key),
        )
        if cursor.rowcount != 1:
            cursor.execute(
                "SELECT state FROM m1_structure_source_windows WHERE window_key = %s",
                (event_spec.window_key,),
            )
            row = cursor.fetchone()
            if row is None or row["state"] != "complete":
                raise CheckpointConflictError("event source terminal transition is invalid")
        self._enqueue_job_cursor(
            cursor,
            job_key=f"{event_spec.window_key}:materialize",
            job_type="structure-materialize",
            input_identity=event_spec.window_key,
            now=now,
        )

    @staticmethod
    def _source_successor_spec_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        spec: StructureSourcePageSpec,
        next_cursor: str | None,
        completed: bool,
    ) -> StructureSourcePageSpec | None:
        if not completed:
            assert next_cursor is not None
            return StructureSourcePageSpec(
                window_key=spec.window_key,
                stream=spec.stream,
                ordinal=spec.ordinal + 1,
                requested_cursor=next_cursor,
            )
        if spec.stream == "events":
            cursor.execute(
                """
                UPDATE m1_structure_source_windows
                SET state = 'events-complete', updated_at = clock_timestamp()
                WHERE window_key = %s AND state = 'running'
                """,
                (spec.window_key,),
            )
            if cursor.rowcount != 1:
                raise CheckpointConflictError("event stream terminal transition is invalid")
            return StructureSourcePageSpec(
                window_key=spec.window_key,
                stream="markets",
                ordinal=0,
                requested_cursor=None,
            )
        return None

    def _admit_scoped_market_batches_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        event_spec: StructureSourcePageSpec,
        market_batches: tuple[tuple[str, ...], ...],
        now: datetime,
    ) -> StructureSourcePageSpec:
        if not market_batches:
            raise CheckpointConflictError("terminal events produced no scoped market batches")
        cursor.execute(
            """
            UPDATE m1_structure_source_windows
            SET state = 'events-complete', updated_at = %s
            WHERE window_key = %s AND state = 'running'
            """,
            (now, event_spec.window_key),
        )
        if cursor.rowcount != 1:
            cursor.execute(
                "SELECT state FROM m1_structure_source_windows WHERE window_key = %s",
                (event_spec.window_key,),
            )
            row = cursor.fetchone()
            if row is None or row["state"] != "events-complete":
                raise CheckpointConflictError("event stream terminal transition is invalid")
        specs = tuple(
            StructureSourcePageSpec(
                window_key=event_spec.window_key,
                stream="markets",
                ordinal=ordinal,
                requested_cursor=None,
                market_ids=market_ids,
            )
            for ordinal, market_ids in enumerate(market_batches)
        )
        for spec in specs:
            self._enqueue_structure_source_page_cursor(cursor, spec=spec, now=now)
        return specs[0]

    def _enqueue_structure_source_page_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        spec: StructureSourcePageSpec,
        now: datetime,
    ) -> None:
        self._enqueue_job_cursor(
            cursor,
            job_key=spec.job_key,
            job_type="structure-fetch",
            input_identity=spec.input_identity,
            now=now,
        )
        cursor.execute(
            """
            INSERT INTO m1_structure_source_page_inputs (
                job_key, window_key, stream, ordinal, requested_cursor,
                market_ids_json, market_ids_digest, admitted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_key) DO NOTHING
            """,
            (
                spec.job_key,
                spec.window_key,
                spec.stream,
                spec.ordinal,
                spec.requested_cursor,
                (
                    None
                    if not spec.market_ids
                    else json.dumps(spec.market_ids, separators=(",", ":"))
                ),
                spec.market_ids_digest,
                now,
            ),
        )
        persisted = self._structure_source_page_spec_cursor(cursor, spec.job_key)
        if persisted != spec:
            raise JobIdentityConflict(f"source page {spec.job_key!r} names another input")

    @staticmethod
    def _structure_source_page_spec_from_row(row: Mapping[str, Any]) -> StructureSourcePageSpec:
        raw_market_ids = row.get("market_ids_json")
        market_ids: tuple[str, ...] = ()
        if raw_market_ids is not None:
            try:
                decoded = json.loads(str(raw_market_ids))
            except json.JSONDecodeError as error:
                raise ControlPlaneError("source market batch is malformed") from error
            if not isinstance(decoded, list) or not all(
                isinstance(value, str) for value in decoded
            ):
                raise ControlPlaneError("source market batch is malformed")
            market_ids = tuple(decoded)
        spec = StructureSourcePageSpec(
            window_key=str(row["window_key"]),
            stream=str(row["stream"]),
            ordinal=int(row["ordinal"]),
            requested_cursor=(
                None if row["requested_cursor"] is None else str(row["requested_cursor"])
            ),
            market_ids=market_ids,
        )
        persisted_digest = row.get("market_ids_digest")
        if (None if persisted_digest is None else str(persisted_digest)) != spec.market_ids_digest:
            raise JobIdentityConflict("source market batch digest does not match input")
        return spec

    @classmethod
    def _structure_source_page_spec_cursor(
        cls, cursor: psycopg.Cursor[dict[str, Any]], job_key: str
    ) -> StructureSourcePageSpec:
        cursor.execute(
            """
            SELECT window_key, stream, ordinal, requested_cursor,
                   market_ids_json, market_ids_digest
            FROM m1_structure_source_page_inputs
            WHERE job_key = %s
            """,
            (job_key,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"Structure source page is unavailable for {job_key!r}")
        return cls._structure_source_page_spec_from_row(row)

    def enqueue_quote_generation(
        self,
        *,
        structure_receipt_digest: str,
        universe_hash: str,
        token_ids: Sequence[str] | None = None,
        legs: Sequence[QuoteBatchLeg] | None = None,
        batch_size: int,
        now: datetime,
    ) -> tuple[QuoteBatchSpec, ...]:
        """Admit deterministic Quote ranges for one immutable Structure truth."""
        self._validate_aware(now, "now")
        if isinstance(batch_size, bool) or batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if (token_ids is None) == (legs is None):
            raise ValueError("provide exactly one of token_ids or legs")
        if legs is not None:
            normalized_legs = tuple(sorted(legs, key=lambda leg: leg.yes_token_id))
            if not normalized_legs:
                raise ValueError("legs must contain at least one entry")
            if len({leg.yes_token_id for leg in normalized_legs}) != len(normalized_legs):
                raise ValueError("legs must have one unambiguous entry per yes_token_id")
            batches = tuple(
                QuoteBatchSpec.from_legs(
                    structure_receipt_digest=structure_receipt_digest,
                    universe_hash=universe_hash,
                    ordinal=ordinal,
                    legs=normalized_legs[start : start + batch_size],
                )
                for ordinal, start in enumerate(range(0, len(normalized_legs), batch_size))
            )
        else:
            normalized = tuple(sorted(set(token_ids or ())))
            if not normalized or any(not token_id for token_id in normalized):
                raise ValueError("token_ids must contain non-empty values")
            batches = tuple(
                QuoteBatchSpec.from_tokens(
                    structure_receipt_digest=structure_receipt_digest,
                    universe_hash=universe_hash,
                    ordinal=ordinal,
                    token_ids=normalized[start : start + batch_size],
                )
                for ordinal, start in enumerate(range(0, len(normalized), batch_size))
            )
        generation_key = batches[0].generation_key
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            for batch in batches:
                self._enqueue_job_cursor(
                    cursor,
                    job_key=batch.job_key,
                    job_type="quote-batch",
                    input_identity=batch.input_identity,
                    now=now,
                )
                cursor.execute(
                    """
                    INSERT INTO m1_quote_batch_inputs (
                        job_key, structure_receipt_digest, universe_hash,
                        token_range_digest, token_ids, legs, admitted_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_key) DO NOTHING
                    """,
                    (
                        batch.job_key,
                        batch.structure_receipt_digest,
                        batch.universe_hash,
                        batch.token_range_digest,
                        Jsonb(batch.token_ids),
                        Jsonb([_quote_batch_leg_payload(leg) for leg in batch.legs])
                        if batch.legs
                        else None,
                        now,
                    ),
                )
                cursor.execute(
                    """
                    SELECT structure_receipt_digest, universe_hash, token_range_digest,
                           token_ids, legs
                    FROM m1_quote_batch_inputs WHERE job_key = %s
                    """,
                    (batch.job_key,),
                )
                persisted = cursor.fetchone()
                if persisted is None or (
                    persisted["structure_receipt_digest"] != batch.structure_receipt_digest
                    or persisted["universe_hash"] != batch.universe_hash
                    or persisted["token_range_digest"] != batch.token_range_digest
                    or tuple(persisted["token_ids"]) != batch.token_ids
                    or _persisted_legs(persisted["legs"]) != batch.legs
                ):
                    raise JobIdentityConflict(
                        f"quote batch {batch.job_key!r} names another immutable input"
                    )
            self._enqueue_job_cursor(
                cursor,
                job_key=f"{generation_key}:certify",
                job_type="quote-certify",
                input_identity=f"{generation_key}:{universe_hash}",
                now=now,
            )
        return batches

    def quote_admission_input(self, job_key: str) -> tuple[str, str, str]:
        """Load the immutable Structure bundle identity for one Quote-admit job."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT generation_key, bundle_key, bundle_digest
                FROM m1_quote_admission_inputs WHERE job_key = %s
                """,
                (job_key,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"Quote admission input is unavailable for {job_key!r}")
        return (str(row["generation_key"]), str(row["bundle_key"]), str(row["bundle_digest"]))

    def admit_quote_generation(
        self,
        lease: JobLease,
        *,
        structure_receipt_digest: str,
        universe_hash: str,
        legs: Sequence[QuoteBatchLeg],
        batch_size: int,
        input_artifacts: Mapping[str, tuple[str, str, int]],
        now: datetime,
    ) -> tuple[QuoteBatchSpec, ...]:
        """Fence one Structure-derived Quote universe and all its batch work together."""
        self._validate_aware(now, "now")
        if lease.job_type != "quote-admit":
            raise ValueError("Quote generation admission requires a quote-admit lease")
        if len(structure_receipt_digest) != 64 or len(universe_hash) != 64:
            raise ValueError("Quote admission digests must be sha256")
        if not legs or batch_size <= 0:
            raise ValueError("Quote admission requires legs and positive batch_size")
        batches = self.quote_batches_from_legs(
            structure_receipt_digest=structure_receipt_digest,
            universe_hash=universe_hash,
            legs=legs,
            batch_size=batch_size,
        )
        if set(input_artifacts) != {batch.job_key for batch in batches}:
            raise JobIdentityConflict("Quote admission requires one R2 input reference per batch")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            generation_key, bundle_key, bundle_digest = self._quote_admission_input_cursor(
                cursor, lease.job_key
            )
            if lease.input_identity != f"{generation_key}:{bundle_key}:{bundle_digest}":
                raise JobIdentityConflict("Quote admission lease names another Structure bundle")
            if structure_receipt_digest != bundle_digest:
                raise CheckpointConflictError("Quote admission names another Structure bundle")
            cursor.execute(
                """
                UPDATE m1_jobs
                SET state = 'succeeded', checkpoint_cursor = 'quote-batches',
                    checkpoint_digest = %s, lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (universe_hash, now, lease.job_key, lease.lease_owner, lease.lease_epoch),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            self._enqueue_quote_generation_cursor(
                cursor, batches=batches, input_artifacts=input_artifacts, now=now
            )
            cursor.execute(
                """
                UPDATE m1_job_attempts SET state = 'succeeded', finished_at = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (now, lease.job_key, lease.lease_epoch),
            )
        return batches

    @staticmethod
    def quote_batches_from_legs(
        *,
        structure_receipt_digest: str,
        universe_hash: str,
        legs: Sequence[QuoteBatchLeg],
        batch_size: int,
    ) -> tuple[QuoteBatchSpec, ...]:
        normalized_legs = tuple(sorted(legs, key=lambda leg: leg.yes_token_id))
        if not normalized_legs:
            raise ValueError("legs must contain at least one entry")
        if len({leg.yes_token_id for leg in normalized_legs}) != len(normalized_legs):
            raise ValueError("legs must have one unambiguous entry per yes_token_id")
        return tuple(
            QuoteBatchSpec.from_legs(
                structure_receipt_digest=structure_receipt_digest,
                universe_hash=universe_hash,
                ordinal=ordinal,
                legs=normalized_legs[start : start + batch_size],
            )
            for ordinal, start in enumerate(range(0, len(normalized_legs), batch_size))
        )

    @staticmethod
    def _quote_admission_input_cursor(
        cursor: psycopg.Cursor[dict[str, Any]], job_key: str
    ) -> tuple[str, str, str]:
        cursor.execute(
            """
            SELECT generation_key, bundle_key, bundle_digest
            FROM m1_quote_admission_inputs WHERE job_key = %s
            """,
            (job_key,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"Quote admission input is unavailable for {job_key!r}")
        return (str(row["generation_key"]), str(row["bundle_key"]), str(row["bundle_digest"]))

    def _enqueue_quote_generation_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        batches: Sequence[QuoteBatchSpec],
        input_artifacts: Mapping[str, tuple[str, str, int]] | None = None,
        now: datetime,
    ) -> None:
        for batch in batches:
            artifact = (input_artifacts or {}).get(batch.job_key)
            if artifact is None:
                raise JobIdentityConflict(
                    f"quote batch {batch.job_key!r} requires an R2 input artifact reference"
                )
            if (
                artifact[0] != f"quote-inputs/{artifact[1]}/batch.ndjson"
                or len(artifact[1]) != 64
                or artifact[2] != len(batch.legs)
            ):
                raise JobIdentityConflict(
                    f"quote batch {batch.job_key!r} has an invalid input artifact reference"
                )
            self._enqueue_job_cursor(
                cursor,
                job_key=batch.job_key,
                job_type="quote-batch",
                input_identity=batch.input_identity,
                now=now,
            )
            cursor.execute(
                """
                INSERT INTO m1_quote_batch_inputs (
                    job_key, structure_receipt_digest, universe_hash,
                    token_range_digest, token_ids, legs, input_artifact_key,
                    input_artifact_digest, leg_count, admitted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_key) DO NOTHING
                """,
                (
                    batch.job_key,
                    batch.structure_receipt_digest,
                    batch.universe_hash,
                    batch.token_range_digest,
                    None if artifact else Jsonb(batch.token_ids),
                    None
                    if artifact
                    else Jsonb([_quote_batch_leg_payload(leg) for leg in batch.legs]),
                    artifact[0],
                    artifact[1],
                    artifact[2],
                    now,
                ),
            )
            cursor.execute(
                """
                SELECT structure_receipt_digest, universe_hash, token_range_digest,
                       token_ids, legs, input_artifact_key, input_artifact_digest, leg_count
                FROM m1_quote_batch_inputs WHERE job_key = %s
                """,
                (batch.job_key,),
            )
            persisted = cursor.fetchone()
            if persisted is None or (
                persisted["structure_receipt_digest"] != batch.structure_receipt_digest
                or persisted["universe_hash"] != batch.universe_hash
                or persisted["token_range_digest"] != batch.token_range_digest
                or persisted["token_ids"] is not None
                or persisted["legs"] is not None
                or (
                    persisted["input_artifact_key"] != artifact[0]
                    or persisted["input_artifact_digest"] != artifact[1]
                    or persisted["leg_count"] != artifact[2]
                )
            ):
                raise JobIdentityConflict(
                    f"quote batch {batch.job_key!r} names another immutable input"
                )
        generation_key = batches[0].generation_key
        self._enqueue_job_cursor(
            cursor,
            job_key=f"{generation_key}:certify",
            job_type="quote-certify",
            input_identity=f"{generation_key}:{batches[0].universe_hash}",
            now=now,
        )

    def enqueue_structure_generation(
        self,
        *,
        identity: StructureBundleIdentity,
        bundle: StructureBundleArtifact,
        ranges: Sequence[tuple[str, str, str]],
        now: datetime,
    ) -> tuple[StructureRangeSpec, ...]:
        """Admit immutable Structure ranges without reading SQLite on takeover."""
        self._validate_aware(now, "now")
        if not ranges:
            raise ValueError("Structure generation requires at least one range")
        specs = tuple(
            StructureRangeSpec.create(
                bundle_key=bundle.key,
                bundle_digest=bundle.sha256,
                component=component,
                ordinal=ordinal,
                range_start=range_start,
                range_end=range_end,
            )
            for ordinal, (component, range_start, range_end) in enumerate(ranges)
        )
        if len({spec.job_key for spec in specs}) != len(specs):
            raise ValueError("Structure ranges must have unique component/ordinal identities")
        generation_key = specs[0].generation_key
        identity_payload = identity.header()
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                INSERT INTO m1_structure_generation_inputs (
                    generation_key, bundle_key, bundle_digest, identity, admitted_at
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (generation_key) DO NOTHING
                """,
                (generation_key, bundle.key, bundle.sha256, Jsonb(identity_payload), now),
            )
            cursor.execute(
                """
                SELECT bundle_key, bundle_digest, identity
                FROM m1_structure_generation_inputs WHERE generation_key = %s
                """,
                (generation_key,),
            )
            persisted_generation = cursor.fetchone()
            if persisted_generation is None or (
                persisted_generation["bundle_key"] != bundle.key
                or persisted_generation["bundle_digest"] != bundle.sha256
                or persisted_generation["identity"] != identity_payload
            ):
                raise JobIdentityConflict(
                    f"Structure generation {generation_key!r} names another immutable input"
                )
            for spec in specs:
                self._enqueue_job_cursor(
                    cursor,
                    job_key=spec.job_key,
                    job_type="structure-normalize",
                    input_identity=spec.input_identity,
                    now=now,
                )
                cursor.execute(
                    """
                    INSERT INTO m1_structure_range_inputs (
                        job_key, generation_key, bundle_key, bundle_digest, component, ordinal,
                        range_start, range_end, range_digest, admitted_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_key) DO NOTHING
                    """,
                    (
                        spec.job_key,
                        generation_key,
                        spec.bundle_key,
                        spec.bundle_digest,
                        spec.component,
                        spec.ordinal,
                        spec.range_start,
                        spec.range_end,
                        spec.range_digest,
                        now,
                    ),
                )
            self._enqueue_job_cursor(
                cursor,
                job_key=f"{generation_key}:certify",
                job_type="structure-certify",
                input_identity=generation_key,
                now=now,
            )
        return specs

    def _enqueue_structure_generation_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        identity: StructureBundleIdentity,
        bundle: StructureBundleArtifact,
        ranges: Sequence[tuple[str, str, str]],
        now: datetime,
    ) -> tuple[StructureRangeSpec, ...]:
        """Cursor-scoped form keeps source bundle receipt and child jobs atomic."""
        specs = tuple(
            StructureRangeSpec.create(
                bundle_key=bundle.key,
                bundle_digest=bundle.sha256,
                component=component,
                ordinal=ordinal,
                range_start=range_start,
                range_end=range_end,
            )
            for ordinal, (component, range_start, range_end) in enumerate(ranges)
        )
        if len({spec.job_key for spec in specs}) != len(specs):
            raise ValueError("Structure ranges must have unique component/ordinal identities")
        generation_key = specs[0].generation_key
        identity_payload = identity.header()
        cursor.execute(
            """
            INSERT INTO m1_structure_generation_inputs (
                generation_key, bundle_key, bundle_digest, identity, admitted_at
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (generation_key) DO NOTHING
            """,
            (generation_key, bundle.key, bundle.sha256, Jsonb(identity_payload), now),
        )
        cursor.execute(
            """
            SELECT bundle_key, bundle_digest, identity
            FROM m1_structure_generation_inputs WHERE generation_key = %s
            """,
            (generation_key,),
        )
        persisted_generation = cursor.fetchone()
        if persisted_generation is None or (
            persisted_generation["bundle_key"] != bundle.key
            or persisted_generation["bundle_digest"] != bundle.sha256
            or persisted_generation["identity"] != identity_payload
        ):
            raise JobIdentityConflict(
                f"Structure generation {generation_key!r} names another immutable input"
            )
        for spec in specs:
            self._enqueue_job_cursor(
                cursor,
                job_key=spec.job_key,
                job_type="structure-normalize",
                input_identity=spec.input_identity,
                now=now,
            )
            cursor.execute(
                """
                INSERT INTO m1_structure_range_inputs (
                    job_key, generation_key, bundle_key, bundle_digest, component, ordinal,
                    range_start, range_end, range_digest, admitted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_key) DO NOTHING
                """,
                (
                    spec.job_key,
                    generation_key,
                    spec.bundle_key,
                    spec.bundle_digest,
                    spec.component,
                    spec.ordinal,
                    spec.range_start,
                    spec.range_end,
                    spec.range_digest,
                    now,
                ),
            )
        self._enqueue_job_cursor(
            cursor,
            job_key=f"{generation_key}:certify",
            job_type="structure-certify",
            input_identity=generation_key,
            now=now,
        )
        return specs

    def structure_range_spec(self, job_key: str) -> StructureRangeSpec:
        """Load a frozen Structure range for a replacement worker."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT bundle_key, bundle_digest, component, ordinal, range_start, range_end
                FROM m1_structure_range_inputs WHERE job_key = %s
                """,
                (job_key,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"Structure range input is unavailable for {job_key!r}")
        return StructureRangeSpec.create(
            bundle_key=str(row["bundle_key"]),
            bundle_digest=str(row["bundle_digest"]),
            component=str(row["component"]),
            ordinal=int(row["ordinal"]),
            range_start=str(row["range_start"]),
            range_end=str(row["range_end"]),
        )

    def quote_batch_spec(self, job_key: str) -> QuoteBatchSpec:
        """Load the admitted immutable token range for a replacement worker."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT structure_receipt_digest, universe_hash, token_ids, legs
                FROM m1_quote_batch_inputs WHERE job_key = %s
                """,
                (job_key,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"quote batch input is unavailable for {job_key!r}")
        try:
            ordinal = int(job_key.rsplit(":", maxsplit=1)[1])
        except (IndexError, ValueError) as error:
            raise JobIdentityConflict(f"quote batch has malformed job key {job_key!r}") from error
        kwargs: dict[str, Any] = dict(
            structure_receipt_digest=str(row["structure_receipt_digest"]),
            universe_hash=str(row["universe_hash"]),
            ordinal=ordinal,
        )
        persisted_legs = _persisted_legs(row["legs"])
        if persisted_legs:
            return QuoteBatchSpec.from_legs(legs=persisted_legs, **kwargs)
        return QuoteBatchSpec.from_tokens(
            token_ids=tuple(str(token_id) for token_id in row["token_ids"]), **kwargs
        )

    def quote_batch_input_reference(self, job_key: str) -> tuple[str, str, int] | None:
        """Return the authenticated R2 input reference, when the batch has been migrated."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT input_artifact_key, input_artifact_digest, leg_count
                FROM m1_quote_batch_inputs WHERE job_key = %s
                """,
                (job_key,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"quote batch input is unavailable for {job_key!r}")
        values = (
            row["input_artifact_key"],
            row["input_artifact_digest"],
            row["leg_count"],
        )
        if values == (None, None, None):
            return None
        key, digest, leg_count = values
        if (
            not isinstance(key, str)
            or not isinstance(digest, str)
            or len(digest) != 64
            or not isinstance(leg_count, int)
            or leg_count <= 0
        ):
            raise JobIdentityConflict(f"quote batch {job_key!r} has a malformed R2 input reference")
        if key != f"quote-inputs/{digest}/batch.ndjson":
            raise JobIdentityConflict(f"quote batch {job_key!r} has a mismatched R2 input key")
        return key, digest, leg_count

    def quote_batch_receipt(self, job_key: str) -> QuoteBatchReceipt | None:
        """Read a prior immutable receipt so a replacement avoids refetching it."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT job_key, quote_digest, artifact_key, artifact_digest,
                       successful_response_count
                FROM m1_quote_batch_receipts WHERE job_key = %s
                """,
                (job_key,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return QuoteBatchReceipt(
            job_key=str(row["job_key"]),
            quote_digest=str(row["quote_digest"]),
            artifact_key=str(row["artifact_key"]),
            artifact_digest=str(row["artifact_digest"]),
            successful_response_count=int(row["successful_response_count"]),
        )

    def structure_range_receipt(self, job_key: str) -> StructureRangeReceipt | None:
        """Read a prior immutable range result so takeover never reprocesses it."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT job_key, bundle_digest, component, range_digest, artifact_key,
                       artifact_digest, record_count
                FROM m1_structure_range_receipts WHERE job_key = %s
                """,
                (job_key,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return StructureRangeReceipt(
            job_key=str(row["job_key"]),
            bundle_digest=str(row["bundle_digest"]),
            component=str(row["component"]),
            range_digest=str(row["range_digest"]),
            artifact_key=str(row["artifact_key"]),
            artifact_digest=str(row["artifact_digest"]),
            record_count=int(row["record_count"]),
        )

    def structure_manifest_payload(self, generation_key: str) -> bytes:
        """Build the only manifest payload valid for a complete frozen generation."""
        self._validate_nonempty(generation_key=generation_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT input.job_key, input.bundle_digest, input.component, input.ordinal,
                       input.range_digest, receipt.artifact_key, receipt.artifact_digest,
                       receipt.record_count, input.admitted_at, receipt.committed_at
                FROM m1_structure_range_inputs AS input
                LEFT JOIN m1_structure_range_receipts AS receipt ON receipt.job_key = input.job_key
                WHERE input.generation_key = %s
                ORDER BY input.component, input.ordinal
                """,
                (generation_key,),
            )
            rows = cursor.fetchall()
        if not rows or any(row["artifact_digest"] is None for row in rows):
            raise IncompleteStructureGenerationError(
                "Structure generation is missing range receipts"
            )
        bundle_digest = str(rows[0]["bundle_digest"])
        receipts: list[dict[str, object]] = []
        for row in rows:
            if (
                str(row["bundle_digest"]) != bundle_digest
                or str(row["component"]) != str(row["component"])
                or row["committed_at"] < row["admitted_at"]
            ):
                raise IncompleteStructureGenerationError("Structure receipt identity is invalid")
            receipts.append(
                {
                    "job_key": str(row["job_key"]),
                    "component": str(row["component"]),
                    "ordinal": int(row["ordinal"]),
                    "range_digest": str(row["range_digest"]),
                    "artifact_key": str(row["artifact_key"]),
                    "artifact_digest": str(row["artifact_digest"]),
                    "record_count": int(row["record_count"]),
                }
            )
        return canonical_structure_manifest_bytes(
            generation_key=generation_key,
            bundle_digest=bundle_digest,
            receipts=receipts,
        )

    def structure_generation_receipts(
        self, generation_key: str
    ) -> tuple[tuple[StructureRangeSpec, StructureRangeReceipt], ...]:
        """Return every admitted range and its durable artifact receipt for re-verification."""
        self._validate_nonempty(generation_key=generation_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT input.job_key, input.bundle_key, input.bundle_digest, input.component,
                       input.ordinal, input.range_start, input.range_end,
                       receipt.range_digest, receipt.artifact_key, receipt.artifact_digest,
                       receipt.record_count
                FROM m1_structure_range_inputs AS input
                LEFT JOIN m1_structure_range_receipts AS receipt ON receipt.job_key = input.job_key
                WHERE input.generation_key = %s
                ORDER BY input.component, input.ordinal
                """,
                (generation_key,),
            )
            rows = cursor.fetchall()
        if not rows or any(row["artifact_digest"] is None for row in rows):
            raise IncompleteStructureGenerationError(
                "Structure generation is missing range receipts"
            )
        result: list[tuple[StructureRangeSpec, StructureRangeReceipt]] = []
        for row in rows:
            spec = StructureRangeSpec.create(
                bundle_key=str(row["bundle_key"]),
                bundle_digest=str(row["bundle_digest"]),
                component=str(row["component"]),
                ordinal=int(row["ordinal"]),
                range_start=str(row["range_start"]),
                range_end=str(row["range_end"]),
            )
            receipt = StructureRangeReceipt(
                job_key=str(row["job_key"]),
                bundle_digest=spec.bundle_digest,
                component=spec.component,
                range_digest=str(row["range_digest"]),
                artifact_key=str(row["artifact_key"]),
                artifact_digest=str(row["artifact_digest"]),
                record_count=int(row["record_count"]),
            )
            if receipt.range_digest != spec.range_digest:
                raise IncompleteStructureGenerationError(
                    "Structure receipt range identity is invalid"
                )
            result.append((spec, receipt))
        return tuple(result)

    @staticmethod
    def _enqueue_job_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        job_key: str,
        job_type: str,
        input_identity: str,
        now: datetime,
    ) -> None:
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

    def record_quote_batch(
        self,
        lease: JobLease,
        *,
        token_range_digest: str,
        quote_digest: str,
        artifact_key: str,
        artifact_digest: str,
        successful_response_count: int,
        quoted_at: datetime,
        now: datetime,
    ) -> CheckpointReceipt:
        """Commit one bounded Quote range under its current worker fence."""
        self._validate_aware(quoted_at, "quoted_at")
        self._validate_aware(now, "now")
        if lease.job_type != "quote-batch":
            raise ValueError("quote batch receipt requires a quote-batch lease")
        for field, value in (
            ("token_range_digest", token_range_digest),
            ("quote_digest", quote_digest),
            ("artifact_digest", artifact_digest),
        ):
            if len(value) != 64:
                raise ValueError(f"{field} must be a sha256 digest")
        if isinstance(successful_response_count, bool) or successful_response_count < 0:
            raise ValueError("successful_response_count must be non-negative")
        structure_digest, universe_hash, ordinal, expected_range_digest = (
            self._quote_batch_identity(lease.input_identity)
        )
        if token_range_digest != expected_range_digest:
            raise CheckpointConflictError("quote batch range does not match its job identity")
        idempotency_key = f"quote-batch:{lease.job_key}:{quote_digest}"
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT checkpoint.receipt_id, checkpoint.checkpoint_cursor,
                       checkpoint.checkpoint_digest, checkpoint.lease_epoch,
                       checkpoint.committed_at, batch.artifact_key,
                       batch.artifact_digest, batch.successful_response_count
                FROM m1_checkpoint_receipts AS checkpoint
                LEFT JOIN m1_quote_batch_receipts AS batch
                    ON batch.job_key = checkpoint.job_key
                WHERE checkpoint.idempotency_key = %s
                """,
                (idempotency_key,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if (
                    str(existing["checkpoint_cursor"]) != ordinal
                    or str(existing["checkpoint_digest"]) != quote_digest
                    or str(existing["artifact_key"]) != artifact_key
                    or str(existing["artifact_digest"]) != artifact_digest
                    or int(existing["successful_response_count"]) != successful_response_count
                ):
                    raise CheckpointConflictError(f"idempotency conflict for {idempotency_key!r}")
                return CheckpointReceipt(
                    receipt_id=str(existing["receipt_id"]),
                    job_key=lease.job_key,
                    lease_epoch=int(existing["lease_epoch"]),
                    idempotency_key=idempotency_key,
                    checkpoint_cursor=ordinal,
                    checkpoint_digest=quote_digest,
                    committed_at=existing["committed_at"],
                )
            cursor.execute(
                """
                UPDATE m1_jobs
                SET checkpoint_cursor = %s, checkpoint_digest = %s, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (
                    ordinal,
                    quote_digest,
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
                checkpoint_cursor=ordinal,
                checkpoint_digest=quote_digest,
                committed_at=now,
            )
            cursor.execute(
                """
                INSERT INTO m1_checkpoint_receipts (
                    receipt_id, job_key, lease_epoch, idempotency_key, checkpoint_cursor,
                    checkpoint_digest, committed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    receipt.receipt_id,
                    receipt.job_key,
                    receipt.lease_epoch,
                    receipt.idempotency_key,
                    receipt.checkpoint_cursor,
                    receipt.checkpoint_digest,
                    receipt.committed_at,
                ),
            )
            cursor.execute(
                """
                INSERT INTO m1_quote_batch_receipts (
                    job_key, structure_receipt_digest, universe_hash, token_range_digest,
                    quote_digest, artifact_key, artifact_digest,
                    successful_response_count, quoted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    lease.job_key,
                    structure_digest,
                    universe_hash,
                    token_range_digest,
                    quote_digest,
                    artifact_key,
                    artifact_digest,
                    successful_response_count,
                    quoted_at,
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

    def record_structure_range(
        self,
        lease: JobLease,
        *,
        range_digest: str,
        artifact_key: str,
        artifact_digest: str,
        record_count: int,
        now: datetime,
    ) -> CheckpointReceipt:
        """Atomically checkpoint one normalized Structure range under its lease fence."""
        self._validate_aware(now, "now")
        if lease.job_type != "structure-normalize":
            raise ValueError("structure range receipt requires a structure-normalize lease")
        for field, value in (("range_digest", range_digest), ("artifact_digest", artifact_digest)):
            if len(value) != 64:
                raise ValueError(f"{field} must be a sha256 digest")
        if not artifact_key:
            raise ValueError("artifact_key must not be empty")
        if isinstance(record_count, bool) or record_count < 0:
            raise ValueError("record_count must be non-negative")

        spec = self.structure_range_spec(lease.job_key)
        if range_digest != spec.range_digest:
            raise CheckpointConflictError("Structure range does not match its job identity")
        checkpoint_cursor = f"{spec.component}:{spec.ordinal}"
        idempotency_key = f"structure-range:{lease.job_key}:{artifact_digest}"
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT checkpoint.receipt_id, checkpoint.checkpoint_cursor,
                       checkpoint.checkpoint_digest, checkpoint.lease_epoch,
                       checkpoint.committed_at, structure.bundle_digest,
                       structure.component, structure.range_digest, structure.artifact_key,
                       structure.artifact_digest, structure.record_count
                FROM m1_checkpoint_receipts AS checkpoint
                LEFT JOIN m1_structure_range_receipts AS structure
                    ON structure.job_key = checkpoint.job_key
                WHERE checkpoint.idempotency_key = %s
                """,
                (idempotency_key,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if (
                    str(existing["checkpoint_cursor"]) != checkpoint_cursor
                    or str(existing["checkpoint_digest"]) != artifact_digest
                    or str(existing["bundle_digest"]) != spec.bundle_digest
                    or str(existing["component"]) != spec.component
                    or str(existing["range_digest"]) != range_digest
                    or str(existing["artifact_key"]) != artifact_key
                    or str(existing["artifact_digest"]) != artifact_digest
                    or int(existing["record_count"]) != record_count
                ):
                    raise CheckpointConflictError(f"idempotency conflict for {idempotency_key!r}")
                return CheckpointReceipt(
                    receipt_id=str(existing["receipt_id"]),
                    job_key=lease.job_key,
                    lease_epoch=int(existing["lease_epoch"]),
                    idempotency_key=idempotency_key,
                    checkpoint_cursor=checkpoint_cursor,
                    checkpoint_digest=artifact_digest,
                    committed_at=existing["committed_at"],
                )
            cursor.execute(
                """
                UPDATE m1_jobs
                SET checkpoint_cursor = %s, checkpoint_digest = %s, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (
                    checkpoint_cursor,
                    artifact_digest,
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
                checkpoint_digest=artifact_digest,
                committed_at=now,
            )
            cursor.execute(
                """
                INSERT INTO m1_checkpoint_receipts (
                    receipt_id, job_key, lease_epoch, idempotency_key, checkpoint_cursor,
                    checkpoint_digest, committed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    receipt.receipt_id,
                    receipt.job_key,
                    receipt.lease_epoch,
                    receipt.idempotency_key,
                    receipt.checkpoint_cursor,
                    receipt.checkpoint_digest,
                    receipt.committed_at,
                ),
            )
            cursor.execute(
                """
                INSERT INTO m1_structure_range_receipts (
                    job_key, bundle_digest, component, range_digest, artifact_key,
                    artifact_digest, record_count, committed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    lease.job_key,
                    spec.bundle_digest,
                    spec.component,
                    range_digest,
                    artifact_key,
                    artifact_digest,
                    record_count,
                    now,
                ),
            )
            cursor.execute(
                """
                UPDATE m1_job_attempts SET state = 'checkpointed', finished_at = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (now, lease.job_key, lease.lease_epoch),
            )
            cursor.execute(
                """
                UPDATE m1_jobs SET state = 'runnable', next_attempt_at = %s,
                    last_error_class = NULL, updated_at = %s
                WHERE job_key = %s AND state = 'waiting'
                  AND (SELECT count(*) FROM m1_structure_range_receipts AS receipt
                       JOIN m1_structure_range_inputs AS input ON input.job_key = receipt.job_key
                       WHERE input.generation_key = %s)
                    = (SELECT count(*) FROM m1_structure_range_inputs WHERE generation_key = %s)
                """,
                (now, now, f"{spec.generation_key}:certify", spec.generation_key, spec.generation_key),
            )
            return receipt

    def certify_structure_generation(
        self,
        lease: JobLease,
        *,
        generation_key: str,
        artifact_key: str,
        artifact_digest: str,
        now: datetime,
    ) -> str:
        """Certify only a complete, identity-matching Structure generation."""
        self._validate_aware(now, "now")
        if (
            lease.job_type != "structure-certify"
            or lease.job_key != f"{generation_key}:certify"
            or lease.input_identity != generation_key
        ):
            raise ValueError("Structure certification requires its matching certifier lease")
        if not artifact_key or len(artifact_digest) != 64:
            raise ValueError("Structure manifest artifact identity is invalid")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT job_key, bundle_digest, component, ordinal, range_digest, admitted_at
                FROM m1_structure_range_inputs
                WHERE generation_key = %s
                ORDER BY component, ordinal
                """,
                (generation_key,),
            )
            expected = cursor.fetchall()
            if not expected:
                raise IncompleteStructureGenerationError(
                    "Structure generation has no admitted ranges"
                )
            cursor.execute(
                """
                SELECT receipt.job_key, receipt.bundle_digest, receipt.component,
                       receipt.range_digest, receipt.artifact_key, receipt.artifact_digest,
                       receipt.record_count, receipt.committed_at
                FROM m1_structure_range_receipts AS receipt
                JOIN m1_structure_range_inputs AS input ON input.job_key = receipt.job_key
                WHERE input.generation_key = %s
                ORDER BY receipt.component, input.ordinal
                """,
                (generation_key,),
            )
            receipts = {str(row["job_key"]): row for row in cursor.fetchall()}
            if set(receipts) != {str(row["job_key"]) for row in expected}:
                raise IncompleteStructureGenerationError(
                    "Structure generation is missing range receipts"
                )
            ordered: list[dict[str, Any]] = []
            for input_row in expected:
                job_key = str(input_row["job_key"])
                receipt = receipts[job_key]
                if (
                    receipt["bundle_digest"] != input_row["bundle_digest"]
                    or receipt["component"] != input_row["component"]
                    or receipt["range_digest"] != input_row["range_digest"]
                    or receipt["committed_at"] < input_row["admitted_at"]
                ):
                    raise IncompleteStructureGenerationError(
                        "Structure generation contains an invalid or stale range receipt"
                    )
                ordered.append(receipt)
            bundle_digest = str(expected[0]["bundle_digest"])
            if any(str(row["bundle_digest"]) != bundle_digest for row in expected):
                raise IncompleteStructureGenerationError(
                    "Structure generation mixes source bundles"
                )
            cursor.execute(
                """
                SELECT bundle_key, identity FROM m1_structure_generation_inputs
                WHERE generation_key = %s
                """,
                (generation_key,),
            )
            generation = cursor.fetchone()
            try:
                component_counts = generation["identity"]["component_counts"]
                expected_counts = {
                    str(component): int(count) for component, count in component_counts.items()
                }
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                raise IncompleteStructureGenerationError(
                    "Structure generation has malformed frozen component counts"
                ) from error
            bundle_key = str(generation["bundle_key"])
            actual_counts: dict[str, int] = {}
            for receipt in ordered:
                component = str(receipt["component"])
                actual_counts[component] = actual_counts.get(component, 0) + int(
                    receipt["record_count"]
                )
            admitted_components = {str(row["component"]) for row in expected}
            parity_counts = {
                component: count
                for component, count in expected_counts.items()
                if component in admitted_components
            }
            if actual_counts != parity_counts:
                raise IncompleteStructureGenerationError(
                    "Structure generation component-count parity failed"
                )
            manifest_digest = sha256(
                canonical_structure_manifest_bytes(
                    generation_key=generation_key,
                    bundle_digest=bundle_digest,
                    receipts=[
                        {
                            "job_key": str(receipt["job_key"]),
                            "component": str(receipt["component"]),
                            "ordinal": int(input_row["ordinal"]),
                            "range_digest": str(receipt["range_digest"]),
                            "artifact_key": str(receipt["artifact_key"]),
                            "artifact_digest": str(receipt["artifact_digest"]),
                            "record_count": int(receipt["record_count"]),
                        }
                        for input_row, receipt in zip(expected, ordered, strict=True)
                    ],
                )
            ).hexdigest()
            if artifact_digest != manifest_digest:
                raise CheckpointConflictError(
                    "Structure manifest digest does not match range receipts"
                )
            record_count = sum(int(row["record_count"]) for row in ordered)
            cursor.execute(
                """
                UPDATE m1_jobs
                SET state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (now, lease.job_key, lease.lease_owner, lease.lease_epoch),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            cursor.execute(
                """
                INSERT INTO m1_generation_manifests (
                    generation_key, producer_job_key, input_digest, artifact_key,
                    artifact_digest, record_count, published_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (generation_key) DO NOTHING
                """,
                (
                    generation_key,
                    lease.job_key,
                    bundle_digest,
                    artifact_key,
                    artifact_digest,
                    record_count,
                    now,
                ),
            )
            cursor.execute(
                """
                SELECT input_digest, artifact_key, artifact_digest, record_count
                FROM m1_generation_manifests WHERE generation_key = %s
                """,
                (generation_key,),
            )
            persisted = cursor.fetchone()
            if persisted is None or (
                persisted["input_digest"] != bundle_digest
                or persisted["artifact_key"] != artifact_key
                or persisted["artifact_digest"] != artifact_digest
                or int(persisted["record_count"]) != record_count
            ):
                raise CheckpointConflictError("Structure generation manifest conflicts")
            quote_admit_job_key = f"{generation_key}:quote-admit"
            quote_admit_identity = f"{generation_key}:{bundle_key}:{bundle_digest}"
            self._enqueue_job_cursor(
                cursor,
                job_key=quote_admit_job_key,
                job_type="quote-admit",
                input_identity=quote_admit_identity,
                now=now,
            )
            cursor.execute(
                """
                INSERT INTO m1_quote_admission_inputs (
                    job_key, generation_key, bundle_key, bundle_digest, admitted_at
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (job_key) DO NOTHING
                """,
                (quote_admit_job_key, generation_key, bundle_key, bundle_digest, now),
            )
            cursor.execute(
                """
                SELECT generation_key, bundle_key, bundle_digest
                FROM m1_quote_admission_inputs WHERE job_key = %s
                """,
                (quote_admit_job_key,),
            )
            quote_admission = cursor.fetchone()
            if quote_admission is None or (
                str(quote_admission["generation_key"]) != generation_key
                or str(quote_admission["bundle_key"]) != bundle_key
                or str(quote_admission["bundle_digest"]) != bundle_digest
            ):
                raise CheckpointConflictError("Structure generation names conflicting Quote input")
            cursor.execute(
                """
                UPDATE m1_job_attempts SET state = 'succeeded', finished_at = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (now, lease.job_key, lease.lease_epoch),
            )
            return manifest_digest

    def certify_quote_generation(
        self,
        lease: JobLease,
        *,
        generation_key: str,
        now: datetime,
    ) -> str:
        """Publish one complete Quote generation, never a partial batch set."""
        self._validate_aware(now, "now")
        if lease.job_type != "quote-certify" or lease.job_key != f"{generation_key}:certify":
            raise ValueError("Quote certification requires its matching quote-certify lease")
        try:
            lease_generation_key, universe_hash = lease.input_identity.rsplit(":", maxsplit=1)
        except ValueError as error:
            raise JobIdentityConflict("quote certifier has malformed input identity") from error
        if lease_generation_key != generation_key or not universe_hash:
            raise JobIdentityConflict("quote certifier identity does not match its generation")
        structure_digest = generation_key.removeprefix("quote:")
        if len(structure_digest) != 64:
            raise JobIdentityConflict("quote generation has malformed Structure receipt digest")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT job_key, input_identity, created_at
                FROM m1_jobs
                WHERE job_type = 'quote-batch' AND job_key LIKE %s
                ORDER BY job_key
                """,
                (f"{generation_key}:batch:%",),
            )
            expected = cursor.fetchall()
            if not expected:
                raise IncompleteQuoteGenerationError("Quote generation has no admitted batch jobs")
            cursor.execute(
                """
                SELECT job_key, structure_receipt_digest, universe_hash, token_range_digest,
                       quote_digest, artifact_key, artifact_digest,
                       successful_response_count, quoted_at
                FROM m1_quote_batch_receipts
                WHERE job_key LIKE %s
                ORDER BY job_key
                """,
                (f"{generation_key}:batch:%",),
            )
            receipts = {str(row["job_key"]): row for row in cursor.fetchall()}
            if set(receipts) != {str(row["job_key"]) for row in expected}:
                raise IncompleteQuoteGenerationError("Quote generation is missing batch receipts")
            ordered_receipts: list[dict[str, object]] = []
            for job in expected:
                job_key = str(job["job_key"])
                receipt = receipts[job_key]
                _structure, _universe, _ordinal, range_digest = self._quote_batch_identity(
                    str(job["input_identity"])
                )
                if (
                    receipt["structure_receipt_digest"] != structure_digest
                    or receipt["universe_hash"] != universe_hash
                    or receipt["token_range_digest"] != range_digest
                    or receipt["quoted_at"] < job["created_at"]
                ):
                    raise IncompleteQuoteGenerationError(
                        "Quote generation contains an invalid or stale batch receipt"
                    )
                ordered_receipts.append(receipt)
            artifact_digest = sha256(
                "\n".join(
                    f"{row['job_key']}:{row['token_range_digest']}:"
                    f"{row['quote_digest']}:{row['artifact_key']}:{row['artifact_digest']}"
                    for row in ordered_receipts
                ).encode()
            ).hexdigest()
            record_count = sum(int(row["successful_response_count"]) for row in ordered_receipts)
            cursor.execute(
                """
                UPDATE m1_jobs
                SET state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s AND state = 'leased'
                """,
                (now, lease.job_key, lease.lease_owner, lease.lease_epoch),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            cursor.execute(
                """
                INSERT INTO m1_generation_manifests (
                    generation_key, producer_job_key, input_digest, artifact_key,
                    artifact_digest, record_count, published_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (generation_key) DO NOTHING
                """,
                (
                    generation_key,
                    lease.job_key,
                    universe_hash,
                    f"quote-receipts:{generation_key}",
                    artifact_digest,
                    record_count,
                    now,
                ),
            )
            cursor.execute(
                "SELECT generation_key FROM m1_publication_pointers "
                "WHERE pointer_key = 'quote:current' FOR UPDATE"
            )
            current = cursor.fetchone()
            if current is None:
                cursor.execute(
                    """
                    INSERT INTO m1_publication_pointers (
                        pointer_key, generation_key, expected_generation_key,
                        lease_epoch, published_at
                    ) VALUES ('quote:current', %s, NULL, %s, %s)
                    """,
                    (generation_key, lease.lease_epoch, now),
                )
            elif str(current["generation_key"]) != generation_key:
                cursor.execute(
                    """
                    UPDATE m1_publication_pointers
                    SET generation_key = %s, expected_generation_key = %s,
                        lease_epoch = %s, published_at = %s
                    WHERE pointer_key = 'quote:current' AND generation_key = %s
                    """,
                    (
                        generation_key,
                        current["generation_key"],
                        lease.lease_epoch,
                        now,
                        current["generation_key"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise StaleLeaseError("Quote pointer changed during certification")
            self._enqueue_job_cursor(
                cursor,
                job_key=f"{generation_key}:opportunity-certify",
                job_type="opportunity-certify",
                input_identity=generation_key,
                now=now,
            )
            cursor.execute(
                """
                UPDATE m1_job_attempts SET state = 'succeeded', finished_at = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (now, lease.job_key, lease.lease_epoch),
            )
            return artifact_digest

    def publish_structure_shadow(self, *, generation_key: str, now: datetime) -> str:
        """Move only the transactional shadow pointer to a certified generation."""
        self._validate_nonempty(generation_key=generation_key)
        self._validate_aware(now, "now")
        if not generation_key.startswith("structure:"):
            raise ValueError("Structure shadow pointer requires a Structure generation")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT producer_job_key FROM m1_generation_manifests
                WHERE generation_key = %s
                """,
                (generation_key,),
            )
            manifest = cursor.fetchone()
            if manifest is None or str(manifest["producer_job_key"]) != f"{generation_key}:certify":
                raise IncompleteStructureGenerationError(
                    "Structure shadow pointer requires a certified manifest"
                )
            cursor.execute(
                """
                SELECT generation_key FROM m1_publication_pointers
                WHERE pointer_key = 'structure:current:shadow' FOR UPDATE
                """
            )
            current = cursor.fetchone()
            if current is None:
                cursor.execute(
                    """
                    INSERT INTO m1_publication_pointers (
                        pointer_key, generation_key, expected_generation_key,
                        lease_epoch, published_at
                    ) VALUES ('structure:current:shadow', %s, NULL, 0, %s)
                    """,
                    (generation_key, now),
                )
            elif str(current["generation_key"]) != generation_key:
                cursor.execute(
                    """
                    UPDATE m1_publication_pointers
                    SET generation_key = %s, expected_generation_key = %s,
                        lease_epoch = lease_epoch + 1, published_at = %s
                    WHERE pointer_key = 'structure:current:shadow' AND generation_key = %s
                    """,
                    (generation_key, current["generation_key"], now, current["generation_key"]),
                )
                if cursor.rowcount != 1:
                    raise StaleLeaseError("Structure shadow pointer changed during publication")
            return generation_key

    def structure_shadow_pointer(self) -> dict[str, object] | None:
        """Read the isolated Structure shadow pointer, never legacy current truth."""
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT generation_key, expected_generation_key, lease_epoch, published_at
                FROM m1_publication_pointers
                WHERE pointer_key = 'structure:current:shadow'
                """
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {
            "generation_key": str(row["generation_key"]),
            "expected_generation_key": row["expected_generation_key"],
            "lease_epoch": int(row["lease_epoch"]),
            "published_at": row["published_at"],
        }

    @staticmethod
    def _quote_batch_identity(input_identity: str) -> tuple[str, str, str, str]:
        parts = input_identity.split(":")
        if len(parts) != 5 or parts[0] != "quote" or not all(parts[1:]):
            raise JobIdentityConflict("quote batch job has malformed input identity")
        return parts[1], parts[2], parts[3], parts[4]

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
                      job_type <> 'structure-fetch'
                      OR NOT EXISTS (
                          SELECT 1
                          FROM m1_structure_source_page_inputs AS source_input
                          WHERE source_input.job_key = m1_jobs.job_key
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM m1_structure_source_page_inputs AS source_input
                          JOIN m1_structure_source_windows AS source_window
                            ON source_window.window_key = source_input.window_key
                          WHERE source_input.job_key = m1_jobs.job_key
                            AND source_window.state IN ('running', 'events-complete')
                      )
                  )
                  AND (
                      (state IN ('runnable', 'retryable', 'checkpointed')
                       AND (next_attempt_at IS NULL OR next_attempt_at <= %s))
                      OR (state = 'leased' AND lease_expires_at <= %s)
                  )
                ORDER BY
                    CASE WHEN state = 'retryable' AND next_attempt_at <= %s THEN 0 ELSE 1 END,
                    next_attempt_at NULLS FIRST,
                    updated_at,
                    job_key
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                (list(job_types), now, now, now),
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
        if state not in {JobState.RETRYABLE, JobState.WAITING, JobState.SUCCEEDED, JobState.QUARANTINED}:
            raise ValueError("finish only accepts retryable, waiting, succeeded, or quarantined")
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
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                  AND state IN ('leased', 'checkpointed')
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
            return self._record_incident_event(
                cursor,
                incident_key=incident_key,
                dedupe_key=dedupe_key,
                component=component,
                severity=severity,
                summary=summary,
                kind=kind,
                detail=detail,
                idempotency_key=idempotency_key,
                channels=channels,
                now=now,
            )

    def finish_retryable_with_incident(
        self,
        lease: JobLease,
        *,
        error_class: str,
        incident_key: str,
        dedupe_key: str,
        component: str,
        summary: str,
        detail: dict[str, object],
        channels: Sequence[str],
        now: datetime,
    ) -> datetime:
        """Fence retry, circuit state, and durable alert intent in one transaction."""
        self._validate_aware(now, "now")
        self._validate_nonempty(
            error_class=error_class,
            incident_key=incident_key,
            dedupe_key=dedupe_key,
            component=component,
            summary=summary,
        )
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
                SELECT consecutive_failures, state, opened_at
                FROM m1_job_circuits WHERE job_key = %s FOR UPDATE
                """,
                (lease.job_key,),
            )
            circuit = cursor.fetchone()
            failures = (0 if circuit is None else int(circuit["consecutive_failures"])) + 1
            delay_seconds = min(15 * (2 ** (failures - 1)), 300)
            next_attempt_at = now + timedelta(seconds=delay_seconds)
            circuit_state = "open" if failures >= 3 else "closed"
            opened_at = (
                now if failures == 3 else (None if circuit is None else circuit["opened_at"])
            )
            cursor.execute(
                """
                UPDATE m1_jobs
                SET state = 'retryable', next_attempt_at = %s, last_error_class = %s,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                  AND state IN ('leased', 'checkpointed')
                """,
                (
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
                SET state = 'retryable', finished_at = %s, error_class = %s
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (now, error_class, lease.job_key, lease.lease_epoch),
            )
            cursor.execute(
                """
                INSERT INTO m1_job_circuits (
                    job_key, consecutive_failures, state, opened_at, next_probe_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_key) DO UPDATE
                SET consecutive_failures = EXCLUDED.consecutive_failures,
                    state = EXCLUDED.state, opened_at = EXCLUDED.opened_at,
                    next_probe_at = EXCLUDED.next_probe_at, updated_at = EXCLUDED.updated_at
                """,
                (lease.job_key, failures, circuit_state, opened_at, next_attempt_at, now),
            )
            kind = (
                "circuit-opened"
                if failures == 3
                else ("circuit-probe-failed" if failures > 3 else "attempt-failed")
            )
            self._record_incident_event(
                cursor,
                incident_key=incident_key,
                dedupe_key=dedupe_key,
                component=component,
                severity="warning",
                summary=summary,
                kind=kind,
                detail={
                    **detail,
                    "consecutive_failures": failures,
                    "circuit_state": circuit_state,
                    "next_probe_at": next_attempt_at.isoformat(),
                    "retry_after_seconds": delay_seconds,
                },
                idempotency_key=f"job-retry:{lease.job_key}:{lease.lease_epoch}",
                channels=channels,
                now=now,
            )
            return next_attempt_at

    def claim_alert_delivery(
        self,
        *,
        worker_id: str,
        lease_seconds: int,
        now: datetime,
        acceptance_run_id: str | None = None,
    ) -> AlertDeliveryLease | None:
        """Claim one due outbox intent; an expired alert lease is safely taken over."""
        self._validate_nonempty(worker_id=worker_id)
        self._validate_aware(now, "now")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if acceptance_run_id is not None and not acceptance_run_id:
            raise ValueError("acceptance_run_id must be non-empty when provided")
        expires_at = now + timedelta(seconds=lease_seconds)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            if acceptance_run_id is None:
                cursor.execute(
                    """
                SELECT outbox_id, incident_event_id, channel, payload, lease_epoch, attempt_count
                FROM m1_alert_outbox
                WHERE (state IN ('pending', 'retryable') AND next_attempt_at <= %s)
                   OR (state = 'retryable' AND lease_expires_at <= %s)
                ORDER BY next_attempt_at, created_at, outbox_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                    (now, now),
                )
            else:
                cursor.execute(
                    """
                SELECT outbox_id, incident_event_id, channel, payload, lease_epoch, attempt_count
                FROM m1_alert_outbox
                WHERE ((state IN ('pending', 'retryable') AND next_attempt_at <= %s)
                   OR (state = 'retryable' AND lease_expires_at <= %s))
                  AND payload->>'acceptance_run_id' = %s
                ORDER BY next_attempt_at, created_at, outbox_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """,
                    (now, now, acceptance_run_id),
                )
            row = cursor.fetchone()
            if row is None:
                return None
            lease_epoch = int(row["lease_epoch"]) + 1
            attempt_number = int(row["attempt_count"]) + 1
            cursor.execute(
                """
                UPDATE m1_alert_outbox
                SET state = 'retryable', attempt_count = %s, lease_owner = %s,
                    lease_epoch = %s, lease_expires_at = %s, next_attempt_at = %s
                WHERE outbox_id = %s
                """,
                (attempt_number, worker_id, lease_epoch, expires_at, expires_at, row["outbox_id"]),
            )
            return AlertDeliveryLease(
                outbox_id=str(row["outbox_id"]),
                incident_event_id=str(row["incident_event_id"]),
                channel=str(row["channel"]),
                payload=dict(row["payload"]),
                lease_owner=worker_id,
                lease_epoch=lease_epoch,
                lease_expires_at=expires_at,
                attempt_number=attempt_number,
            )

    def record_job_recovery(
        self,
        lease: JobLease,
        *,
        component: str,
        channels: Sequence[str],
        now: datetime,
        acceptance_run_id: str | None = None,
    ) -> bool:
        """Close a failed job's circuit after durable forward progress.

        The prior retry and this recovery are independently committed because a
        worker can crash between them.  A terminal success or a durable
        checkpoint under the same lease epoch fences that second transition;
        checkpoint recovery matters for bounded workers whose full job spans
        many independently durable turns.  The recovery event has an immutable
        epoch key so replay stays harmless.
        """
        self._validate_aware(now, "now")
        self._validate_nonempty(component=component)
        if not channels or any(not channel.strip() for channel in channels):
            raise ValueError("channels must contain non-empty values")
        if len(set(channels)) != len(channels):
            raise ValueError("channels must be unique")
        if acceptance_run_id is not None and not acceptance_run_id:
            raise ValueError("acceptance_run_id must be non-empty when provided")
        dedupe_key = f"job-retry:{lease.job_key}"
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT state, lease_epoch FROM m1_jobs
                WHERE job_key = %s FOR UPDATE
                """,
                (lease.job_key,),
            )
            job = cursor.fetchone()
            if (
                job is None
                or str(job["state"]) not in {JobState.SUCCEEDED.value, "checkpointed"}
                or int(job["lease_epoch"]) != lease.lease_epoch
            ):
                raise StaleLeaseError(
                    f"durable recovery state is no longer current for {lease.job_key}"
                )
            cursor.execute(
                """
                SELECT consecutive_failures FROM m1_job_circuits
                WHERE job_key = %s FOR UPDATE
                """,
                (lease.job_key,),
            )
            circuit = cursor.fetchone()
            if circuit is None or int(circuit["consecutive_failures"]) == 0:
                return False
            cursor.execute(
                """
                UPDATE m1_job_circuits
                SET consecutive_failures = 0, state = 'closed', opened_at = NULL,
                    next_probe_at = NULL, updated_at = %s
                WHERE job_key = %s
                """,
                (now, lease.job_key),
            )
            cursor.execute(
                """
                UPDATE m1_incidents
                SET state = 'resolved', resolved_at = %s, updated_at = %s
                WHERE dedupe_key = %s AND state <> 'resolved'
                RETURNING incident_key
                """,
                (now, now, dedupe_key),
            )
            incident = cursor.fetchone()
            if incident is None:
                return False
            incident_key = str(incident["incident_key"])
            idempotency_key = f"job-recovery:{lease.job_key}:{lease.lease_epoch}"
            cursor.execute(
                "SELECT incident_event_id FROM m1_incident_events WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            if cursor.fetchone() is not None:
                return True
            event_id = str(uuid4())
            cursor.execute(
                """
                INSERT INTO m1_incident_events (
                    incident_event_id, incident_key, kind, detail, idempotency_key, occurred_at
                ) VALUES (%s, %s, 'recovered', %s, %s, %s)
                """,
                (
                    event_id,
                    incident_key,
                    Jsonb(
                        {
                            "job_key": lease.job_key,
                            "lease_epoch": lease.lease_epoch,
                            "component": component,
                        }
                    ),
                    idempotency_key,
                    now,
                ),
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
                        Jsonb(
                            {
                                "incident_key": incident_key,
                                "kind": "recovered",
                                **(
                                    {}
                                    if acceptance_run_id is None
                                    else {"acceptance_run_id": acceptance_run_id}
                                ),
                            }
                        ),
                        now,
                        now,
                    ),
                )
            return True

    def finish_alert_delivery(
        self,
        lease: AlertDeliveryLease,
        *,
        state: str,
        now: datetime,
        provider_receipt: str | None = None,
        error_class: str | None = None,
        error_detail: dict[str, object] | None = None,
        next_attempt_at: datetime | None = None,
    ) -> None:
        """Store one immutable channel receipt and release its fenced outbox lease."""
        if state not in {"delivered", "retryable", "failed"}:
            raise ValueError("invalid alert delivery state")
        self._validate_aware(now, "now")
        if next_attempt_at is not None:
            self._validate_aware(next_attempt_at, "next_attempt_at")
        if state == "retryable" and next_attempt_at is None:
            raise ValueError("retryable alert delivery requires next_attempt_at")
        if state == "delivered" and not provider_receipt:
            raise ValueError("delivered alert requires provider_receipt")
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE m1_alert_outbox
                SET state = %s, next_attempt_at = %s, lease_owner = NULL, lease_expires_at = NULL
                WHERE outbox_id = %s AND lease_owner = %s AND lease_epoch = %s
                  AND state = 'retryable'
                """,
                (state, next_attempt_at, lease.outbox_id, lease.lease_owner, lease.lease_epoch),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"alert lease is no longer current for {lease.outbox_id}")
            cursor.execute(
                """
                INSERT INTO m1_alert_deliveries (
                    delivery_id, outbox_id, attempt_number, state, provider_receipt,
                    error_class, error_detail, attempted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(uuid4()),
                    lease.outbox_id,
                    lease.attempt_number,
                    state,
                    provider_receipt,
                    error_class,
                    None if error_detail is None else Jsonb(error_detail),
                    now,
                ),
            )

    @staticmethod
    def _record_incident_event(
        cursor: object,
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
        cursor.execute("SELECT incident_key FROM m1_incidents WHERE dedupe_key = %s", (dedupe_key,))
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

    def record_cloud_usage(
        self, *, source: str, operation: str, bytes_received: int, item_count: int,
        artifact_key: str, artifact_digest: str, daily_budget_bytes: int, now: datetime,
    ) -> CloudUsageDecision:
        self._validate_nonempty(source=source, operation=operation, artifact_key=artifact_key)
        self._validate_aware(now, "now")
        if bytes_received < 0 or item_count < 0 or daily_budget_bytes <= 0 or len(artifact_digest) != 64:
            raise ValueError("invalid cloud usage observation")
        observation_id = str(uuid4())
        budget_day = now.astimezone(UTC).date()
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"m1-cloud-egress:{budget_day}",))
            cursor.execute(
                """INSERT INTO m1_cloud_usage_observations
                   (observation_id,observed_at,budget_day,source,operation,bytes_received,daily_budget_bytes,item_count,artifact_key,artifact_digest)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (observation_id, now, budget_day, source, operation, bytes_received, daily_budget_bytes, item_count, artifact_key, artifact_digest),
            )
            cursor.execute("SELECT COALESCE(sum(bytes_received),0) AS used FROM m1_cloud_usage_observations WHERE budget_day=%s", (budget_day,))
            used = int(cursor.fetchone()["used"])
            ratio = used * 100 // daily_budget_bytes
            threshold = 90 if ratio >= 90 else 75 if ratio >= 75 else 50 if ratio >= 50 else 0
            if threshold:
                dedupe_key = f"cloud-egress:{threshold}:{budget_day.isoformat()}"
                self._record_incident_event(
                    cursor,
                    incident_key=f"{dedupe_key}:{source}",
                    dedupe_key=dedupe_key,
                    component="cloud-egress",
                    severity="critical" if threshold == 90 else "warning",
                    summary=f"M1 cloud egress reached {threshold}% of its daily budget",
                    kind="detected",
                    detail={"used_bytes": used, "daily_budget_bytes": daily_budget_bytes, "threshold_percent": threshold, "observation_id": observation_id},
                    idempotency_key=f"{dedupe_key}:{observation_id}",
                    channels=("dashboard", "telegram"),
                    now=now,
                )
        return CloudUsageDecision(threshold < 90, used, threshold, observation_id)

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
            cursor.execute("SELECT state, count(*) AS count FROM m1_jobs GROUP BY state")
            job_counts = {str(row["state"]): int(row["count"]) for row in cursor.fetchall()}
            cursor.execute(
                """
                SELECT state, count(*) AS count
                FROM m1_jobs WHERE job_type = 'quote-batch' GROUP BY state
                """
            )
            quote_batch_states = {str(row["state"]): int(row["count"]) for row in cursor.fetchall()}
            cursor.execute(
                """
                SELECT state, count(*) AS count
                FROM m1_jobs WHERE job_type = 'quote-admit' GROUP BY state
                """
            )
            quote_admission_states = {
                str(row["state"]): int(row["count"]) for row in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT extract(epoch FROM (%s - min(created_at))) AS age_seconds
                FROM m1_jobs WHERE job_type = 'quote-admit' AND state = 'retryable'
                """,
                (now,),
            )
            retryable_quote_admission_age = cursor.fetchone()
            cursor.execute(
                """
                SELECT state, count(*) AS count
                FROM m1_jobs WHERE job_type = 'quote-certify' GROUP BY state
                """
            )
            quote_certifier_states = {
                str(row["state"]): int(row["count"]) for row in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT extract(epoch FROM (%s - min(created_at))) AS age_seconds
                FROM m1_jobs WHERE job_type = 'quote-batch' AND state = 'retryable'
                """,
                (now,),
            )
            retryable_quote_age = cursor.fetchone()
            cursor.execute(
                """
                SELECT pointer.generation_key, pointer.published_at,
                       manifest.artifact_key, manifest.artifact_digest,
                       manifest.record_count
                FROM m1_publication_pointers AS pointer
                JOIN m1_generation_manifests AS manifest
                    ON manifest.generation_key = pointer.generation_key
                WHERE pointer.pointer_key = 'quote:current'
                """
            )
            quote_pointer = cursor.fetchone()
            cursor.execute(
                """
                SELECT state, count(*) AS count
                FROM m1_jobs WHERE job_type = 'structure-normalize' GROUP BY state
                """
            )
            structure_range_states = {
                str(row["state"]): int(row["count"]) for row in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT state, count(*) AS count
                FROM m1_jobs WHERE job_type = 'structure-fetch' GROUP BY state
                """
            )
            source_fetch_states = {
                str(row["state"]): int(row["count"]) for row in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT state, count(*) AS count
                FROM m1_jobs WHERE job_type = 'structure-materialize' GROUP BY state
                """
            )
            source_materializer_states = {
                str(row["state"]): int(row["count"]) for row in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT extract(epoch FROM (%s - min(created_at))) AS age_seconds
                FROM m1_jobs
                WHERE job_type IN ('structure-fetch', 'structure-materialize')
                  AND state = 'retryable'
                """,
                (now,),
            )
            retryable_source_age = cursor.fetchone()
            cursor.execute(
                """
                SELECT state, count(*) AS count
                FROM m1_jobs WHERE job_type = 'structure-certify' GROUP BY state
                """
            )
            structure_certifier_states = {
                str(row["state"]): int(row["count"]) for row in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT extract(epoch FROM (%s - min(created_at))) AS age_seconds
                FROM m1_jobs
                WHERE job_type = 'structure-normalize' AND state = 'retryable'
                """,
                (now,),
            )
            retryable_structure_age = cursor.fetchone()
            cursor.execute(
                """
                SELECT generation_key, artifact_key, artifact_digest, record_count, published_at
                FROM m1_generation_manifests
                WHERE generation_key LIKE 'structure:%'
                ORDER BY published_at DESC, generation_key DESC LIMIT 1
                """
            )
            structure_manifest = cursor.fetchone()
            cursor.execute(
                """
                SELECT pointer.generation_key, pointer.expected_generation_key,
                       pointer.published_at, manifest.artifact_key, manifest.artifact_digest,
                       manifest.record_count
                FROM m1_publication_pointers AS pointer
                JOIN m1_generation_manifests AS manifest
                    ON manifest.generation_key = pointer.generation_key
                WHERE pointer.pointer_key = 'structure:current:shadow'
                """
            )
            structure_pointer = cursor.fetchone()
            cursor.execute(
                """
                SELECT extract(epoch FROM (%s - min(created_at))) AS age_seconds
                FROM m1_jobs WHERE state IN ('runnable', 'retryable', 'checkpointed')
                """,
                (now,),
            )
            oldest = cursor.fetchone()
            queue_health = {
                "structure-range": self._queue_health_snapshot_cursor(
                    cursor,
                    job_type="structure-normalize",
                    now=now,
                ),
                "quote-batch": self._queue_health_snapshot_cursor(
                    cursor,
                    job_type="quote-batch",
                    now=now,
                ),
            }
            cursor.execute(
                """
                SELECT count(*) AS count FROM m1_jobs
                WHERE state = 'leased' AND lease_expires_at <= %s
                """,
                (now,),
            )
            expired = cursor.fetchone()
            cursor.execute("SELECT count(*) AS count FROM m1_job_circuits WHERE state = 'open'")
            open_circuit_count = cursor.fetchone()
            cursor.execute(
                """
                SELECT job_key, consecutive_failures, next_probe_at
                FROM m1_job_circuits WHERE state = 'open'
                ORDER BY updated_at DESC, job_key DESC LIMIT %s
                """,
                (sample_limit,),
            )
            open_circuits = [
                {
                    "job_key": str(row["job_key"]),
                    "consecutive_failures": int(row["consecutive_failures"]),
                    "next_probe_at": row["next_probe_at"].isoformat(),
                }
                for row in cursor.fetchall()
            ]
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
                SELECT i.incident_key, i.severity, i.summary, i.opened_at, e.detail
                FROM m1_incidents i
                JOIN LATERAL (
                    SELECT detail FROM m1_incident_events
                    WHERE incident_key = i.incident_key
                    ORDER BY occurred_at DESC, incident_event_id DESC LIMIT 1
                ) e ON TRUE
                WHERE (i.dedupe_key = 'runtime-watchdog'
                       OR i.dedupe_key LIKE 'runtime-watchdog:' || chr(37))
                  AND i.state <> 'resolved'
                ORDER BY i.updated_at DESC, i.incident_key DESC LIMIT 1
                """
            )
            runtime_current = cursor.fetchone()
            cursor.execute(
                """
                SELECT i.incident_key, i.severity, i.summary, e.kind, e.occurred_at, e.detail
                FROM m1_incident_events e
                JOIN m1_incidents i ON i.incident_key = e.incident_key
                WHERE i.dedupe_key = 'runtime-watchdog'
                   OR i.dedupe_key LIKE 'runtime-watchdog:' || chr(37)
                ORDER BY e.occurred_at DESC, e.incident_event_id DESC LIMIT %s
                """,
                (sample_limit,),
            )
            runtime_events = [
                {
                    "kind": str(row["kind"]),
                    "occurred_at": row["occurred_at"].isoformat(),
                    "incident_key": str(row["incident_key"]),
                    "severity": str(row["severity"]),
                    "summary": str(row["summary"]),
                    "detail": dict(row["detail"]),
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
            cursor.execute(
                """
                SELECT run_id, observed_at FROM m1_soak_observations
                ORDER BY observed_at DESC, run_id DESC LIMIT 1
                """
            )
            latest_soak_observation = cursor.fetchone()
            budget_day = now.astimezone(UTC).date()
            cursor.execute(
                """SELECT COALESCE(sum(bytes_received),0) AS used_bytes,
                          max(daily_budget_bytes) AS daily_budget_bytes
                   FROM m1_cloud_usage_observations WHERE budget_day = %s""",
                (budget_day,),
            )
            cloud_usage = cursor.fetchone()
            cursor.execute(
                """SELECT observation_id, source, operation, bytes_received, item_count,
                          artifact_key, artifact_digest, observed_at
                   FROM m1_cloud_usage_observations WHERE budget_day = %s
                   ORDER BY observed_at DESC, observation_id DESC LIMIT 1""",
                (budget_day,),
            )
            latest_cloud_usage = cursor.fetchone()
        age = None if oldest is None else oldest["age_seconds"]
        quote_retry_age = (
            None if retryable_quote_age is None else retryable_quote_age["age_seconds"]
        )
        quote_admission_retry_age = (
            None
            if retryable_quote_admission_age is None
            else retryable_quote_admission_age["age_seconds"]
        )
        source_retry_age = (
            None if retryable_source_age is None else retryable_source_age["age_seconds"]
        )
        structure_retry_age = (
            None if retryable_structure_age is None else retryable_structure_age["age_seconds"]
        )
        return {
            "job_counts": job_counts,
            "oldest_runnable_age_seconds": None if age is None else float(age),
            "expired_leases": 0 if expired is None else int(expired["count"]),
            "open_circuit_count": (
                0 if open_circuit_count is None else int(open_circuit_count["count"])
            ),
            "open_circuits": open_circuits,
            "recent_attempts": attempts,
            "open_incidents": incidents,
            "runtime_watchdog": {
                "current": (
                    None
                    if runtime_current is None
                    else {
                        "incident_key": str(runtime_current["incident_key"]),
                        "severity": str(runtime_current["severity"]),
                        "summary": str(runtime_current["summary"]),
                        "opened_at": runtime_current["opened_at"].isoformat(),
                        "source": str(dict(runtime_current["detail"]).get("source", "unknown")),
                        "failures": list(dict(runtime_current["detail"]).get("failures", [])),
                    }
                ),
                "recent_events": runtime_events,
            },
            "soak_evidence": (
                None
                if latest_soak_observation is None
                else {
                    "latest_run_id": str(latest_soak_observation["run_id"]),
                    "latest_observed_at": latest_soak_observation["observed_at"].isoformat(),
                }
            ),
            "pending_alert_outbox": outbox,
            "cloud_usage": {
                "budget_day": budget_day.isoformat(),
                "used_bytes": int(cloud_usage["used_bytes"]),
                "daily_budget_bytes": (
                    None if cloud_usage["daily_budget_bytes"] is None else int(cloud_usage["daily_budget_bytes"])
                ),
                "threshold_percent": (
                    0 if cloud_usage["daily_budget_bytes"] is None else min(100, int(cloud_usage["used_bytes"]) * 100 // int(cloud_usage["daily_budget_bytes"]))
                ),
                "latest_observation": (
                    None if latest_cloud_usage is None else {
                        "observation_id": str(latest_cloud_usage["observation_id"]),
                        "source": str(latest_cloud_usage["source"]),
                        "operation": str(latest_cloud_usage["operation"]),
                        "bytes_received": int(latest_cloud_usage["bytes_received"]),
                        "item_count": int(latest_cloud_usage["item_count"]),
                        "artifact_key": str(latest_cloud_usage["artifact_key"]),
                        "artifact_digest": str(latest_cloud_usage["artifact_digest"]),
                        "observed_at": latest_cloud_usage["observed_at"].isoformat(),
                    }
                ),
            },
            "queue_health": queue_health,
            "quote": {
                "admission_job_states": quote_admission_states,
                "oldest_retryable_admission_age_seconds": (
                    None if quote_admission_retry_age is None else float(quote_admission_retry_age)
                ),
                "batch_job_states": quote_batch_states,
                "certifier_job_states": quote_certifier_states,
                "oldest_retryable_batch_age_seconds": (
                    None if quote_retry_age is None else float(quote_retry_age)
                ),
                "current_pointer": (
                    None
                    if quote_pointer is None
                    else {
                        "generation_key": str(quote_pointer["generation_key"]),
                        "published_at": quote_pointer["published_at"].isoformat(),
                        "artifact_key": str(quote_pointer["artifact_key"]),
                        "artifact_digest": str(quote_pointer["artifact_digest"]),
                        "record_count": int(quote_pointer["record_count"]),
                    }
                ),
            },
            "structure": {
                "source_fetch_job_states": source_fetch_states,
                "oldest_retryable_source_age_seconds": (
                    None if source_retry_age is None else float(source_retry_age)
                ),
                "source_materializer_job_states": source_materializer_states,
                "range_job_states": structure_range_states,
                "certifier_job_states": structure_certifier_states,
                "oldest_retryable_range_age_seconds": (
                    None if structure_retry_age is None else float(structure_retry_age)
                ),
                "latest_manifest": self._manifest_snapshot(structure_manifest),
                "shadow_pointer": (
                    None
                    if structure_pointer is None
                    else {
                        "generation_key": str(structure_pointer["generation_key"]),
                        "expected_generation_key": structure_pointer["expected_generation_key"],
                        "published_at": structure_pointer["published_at"].isoformat(),
                        "artifact_key": str(structure_pointer["artifact_key"]),
                        "artifact_digest": str(structure_pointer["artifact_digest"]),
                        "record_count": int(structure_pointer["record_count"]),
                    }
                ),
            },
        }

    def current_opportunities(self, *, limit: int, after_group_id: str) -> dict[str, object]:
        """Read one complete, atomically published opportunity projection."""
        if not 1 <= limit <= 500 or len(after_group_id) > 256 or "\x00" in after_group_id:
            raise ValueError("invalid-opportunity-page")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5000ms'")
            cursor.execute(
                """
                SELECT projection.generation_key, projection.record_count
                FROM m1_opportunity_publication_pointers AS pointer
                JOIN m1_opportunity_projections AS projection
                  ON projection.generation_key = pointer.generation_key
                WHERE pointer.pointer_key = 'opportunity:current'
                """
            )
            projection = cursor.fetchone()
            if projection is None:
                raise ControlPlaneError("opportunity-projection-unavailable")
            generation_key = str(projection["generation_key"])
            cursor.execute(
                """
                SELECT group_id, event_id, membership_hash, bundle_cost,
                       gross_edge_bps, max_bundle_size, legs,
                       structure_observed_at_ms, quote_started_at_ms, quote_quoted_at_ms
                FROM m1_opportunity_projection_rows
                WHERE generation_key = %s AND group_id > %s
                ORDER BY group_id LIMIT %s
                """,
                (generation_key, after_group_id, limit + 1),
            )
            rows = cursor.fetchall()
        has_more = len(rows) > limit
        page = rows[:limit]
        return {
            "status": "available",
            "current_opportunity_count": int(projection["record_count"]),
            "items": [
                {
                    "group_id": str(row["group_id"]),
                    "event_id": str(row["event_id"]),
                    "membership_hash": str(row["membership_hash"]),
                    "bundle_cost": float(row["bundle_cost"]),
                    "gross_edge_bps": float(row["gross_edge_bps"]),
                    "max_bundle_size": float(row["max_bundle_size"]),
                    "legs": row["legs"],
                    "structure_observed_at_ms": int(row["structure_observed_at_ms"]),
                    "quote_started_at_ms": int(row["quote_started_at_ms"]),
                    "quote_quoted_at_ms": int(row["quote_quoted_at_ms"]),
                }
                for row in page
            ],
            "limit": limit,
            "next_after_group_id": str(page[-1]["group_id"]) if has_more else None,
        }

    def publish_opportunity_projection(
        self,
        *,
        quote_generation_key: str,
        structure_generation_key: str,
        rows: Sequence[Mapping[str, object]],
        now: datetime,
    ) -> str:
        """Atomically publish one complete, already-authenticated projection."""
        self._validate_nonempty(
            quote_generation_key=quote_generation_key,
            structure_generation_key=structure_generation_key,
        )
        self._validate_aware(now, "now")
        normalized = tuple(
            sorted((dict(row) for row in rows), key=lambda row: str(row.get("group_id")))
        )
        required = {
            "group_id",
            "event_id",
            "membership_hash",
            "bundle_cost",
            "gross_edge_bps",
            "max_bundle_size",
            "legs",
            "structure_observed_at_ms",
            "quote_started_at_ms",
            "quote_quoted_at_ms",
        }
        if any(set(row) != required or not isinstance(row["group_id"], str) for row in normalized):
            raise ValueError("invalid-opportunity-projection-row")
        if len({str(row["group_id"]) for row in normalized}) != len(normalized):
            raise ValueError("opportunity-projection-group-duplicate")
        digest = sha256(
            "\n".join(
                json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False)
                for row in normalized
            ).encode()
        ).hexdigest()
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                "SELECT generation_key FROM m1_publication_pointers "
                "WHERE pointer_key='quote:current' FOR UPDATE"
            )
            pointer = cursor.fetchone()
            if pointer is None or str(pointer["generation_key"]) != quote_generation_key:
                raise IncompleteQuoteGenerationError(
                    "opportunity projection requires current certified Quote"
                )
            cursor.execute(
                """SELECT generation_key FROM m1_generation_manifests
                   WHERE generation_key = ANY(%s)
                     AND producer_job_key = generation_key || ':certify'""",
                ([quote_generation_key, structure_generation_key],),
            )
            if {str(row["generation_key"]) for row in cursor.fetchall()} != {
                quote_generation_key,
                structure_generation_key,
            }:
                raise IncompleteStructureGenerationError(
                    "opportunity projection requires certified generations"
                )
            cursor.execute(
                """INSERT INTO m1_opportunity_projections
                   (generation_key, structure_generation_key, projection_digest,
                    record_count, certified_at)
                   VALUES (%s,%s,%s,%s,%s) ON CONFLICT (generation_key) DO NOTHING""",
                (
                    quote_generation_key,
                    structure_generation_key,
                    digest,
                    len(normalized),
                    now,
                ),
            )
            cursor.execute(
                "SELECT projection_digest, record_count FROM m1_opportunity_projections "
                "WHERE generation_key=%s",
                (quote_generation_key,),
            )
            persisted = cursor.fetchone()
            if (
                persisted is None
                or str(persisted["projection_digest"]) != digest
                or int(persisted["record_count"]) != len(normalized)
            ):
                raise CheckpointConflictError("opportunity projection conflicts")
            for row in normalized:
                cursor.execute(
                    """INSERT INTO m1_opportunity_projection_rows
                       (generation_key, group_id, event_id, membership_hash, bundle_cost,
                        gross_edge_bps, max_bundle_size, legs, structure_observed_at_ms,
                        quote_started_at_ms, quote_quoted_at_ms)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (
                        quote_generation_key,
                        str(row["group_id"]),
                        str(row["event_id"]),
                        str(row["membership_hash"]),
                        row["bundle_cost"],
                        row["gross_edge_bps"],
                        row["max_bundle_size"],
                        Jsonb(row["legs"]),
                        row["structure_observed_at_ms"],
                        row["quote_started_at_ms"],
                        row["quote_quoted_at_ms"],
                    ),
                )
            cursor.execute(
                "SELECT count(*) AS count FROM m1_opportunity_projection_rows "
                "WHERE generation_key=%s",
                (quote_generation_key,),
            )
            if int(cursor.fetchone()["count"]) != len(normalized):
                raise CheckpointConflictError("opportunity projection rows conflict")
            cursor.execute(
                """INSERT INTO m1_opportunity_publication_pointers
                   (pointer_key, generation_key, published_at)
                   VALUES ('opportunity:current',%s,%s)
                   ON CONFLICT (pointer_key) DO UPDATE
                   SET generation_key=excluded.generation_key,
                       published_at=excluded.published_at""",
                (quote_generation_key, now),
            )
        return digest

    def current_quote_projection_inputs(
        self,
    ) -> tuple[str, str, tuple[tuple[tuple[QuoteBatchLeg, ...], QuoteBatchReceipt, datetime], ...]]:
        """Load one current certified Quote generation and its immutable batch inputs."""
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5000ms'")
            cursor.execute(
                """SELECT pointer.generation_key,
                          admission.generation_key AS structure_generation_key,
                          opportunity.generation_key AS opportunity_generation_key
                   FROM m1_publication_pointers AS pointer
                   JOIN m1_quote_admission_inputs AS admission
                     ON admission.generation_key =
                        'structure:' || substr(pointer.generation_key, 7)
                    AND admission.job_key = admission.generation_key || ':quote-admit'
                   LEFT JOIN m1_opportunity_publication_pointers AS opportunity
                     ON opportunity.pointer_key = 'opportunity:current'
                   WHERE pointer.pointer_key = 'quote:current'"""
            )
            current = cursor.fetchone()
            if current is None:
                raise IncompleteQuoteGenerationError("current Quote generation is unavailable")
            quote_generation_key = str(current["generation_key"])
            if str(current["opportunity_generation_key"] or "") == quote_generation_key:
                raise OpportunityProjectionCurrentError("current Quote is already projected")
            structure_generation_key = str(current["structure_generation_key"])
            cursor.execute(
                """SELECT input.legs, input.input_artifact_key, input.input_artifact_digest,
                          input.leg_count, receipt.job_key, receipt.quote_digest,
                          receipt.artifact_key, receipt.artifact_digest,
                          receipt.successful_response_count, receipt.quoted_at
                   FROM m1_quote_batch_inputs AS input
                   JOIN m1_quote_batch_receipts AS receipt ON receipt.job_key = input.job_key
                   WHERE input.job_key LIKE %s ORDER BY input.job_key""",
                (f"{quote_generation_key}:batch:%",),
            )
            rows = cursor.fetchall()
        if not rows:
            raise IncompleteQuoteGenerationError("current Quote generation has no batch receipts")
        batches = tuple(
            (
                _persisted_legs(row["legs"]),
                QuoteBatchReceipt(
                    job_key=str(row["job_key"]),
                    quote_digest=str(row["quote_digest"]),
                    artifact_key=str(row["artifact_key"]),
                    artifact_digest=str(row["artifact_digest"]),
                    successful_response_count=int(row["successful_response_count"]),
                ),
                row["quoted_at"],
            )
            for row in rows
        )
        if any(
            not legs and not row["input_artifact_digest"]
            for row, (legs, _receipt, _quoted_at) in zip(rows, batches, strict=True)
        ):
            raise IncompleteQuoteGenerationError("current Quote generation lacks frozen input")
        return quote_generation_key, structure_generation_key, batches

    @staticmethod
    def _queue_health_snapshot_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        job_type: str,
        now: datetime,
    ) -> dict[str, object]:
        """Read a compact hint; workers must still acquire the fenced lease."""
        cursor.execute(
            """
            SELECT count(*) AS unfinished_count,
                   extract(epoch FROM (%s - min(created_at))) AS oldest_age_seconds
            FROM m1_jobs
            WHERE job_type = %s
              AND state IN ('runnable', 'retryable', 'leased', 'checkpointed')
            """,
            (now, job_type),
        )
        aggregate = cursor.fetchone()
        cursor.execute(
            """
            SELECT job_key
            FROM m1_jobs
            WHERE job_type = %s
              AND (
                  (state IN ('runnable', 'retryable', 'checkpointed')
                   AND (next_attempt_at IS NULL OR next_attempt_at <= %s))
                  OR (state = 'leased' AND lease_expires_at <= %s)
              )
            ORDER BY
                CASE WHEN state = 'retryable' AND next_attempt_at <= %s THEN 0 ELSE 1 END,
                next_attempt_at NULLS FIRST,
                updated_at,
                job_key
            LIMIT 1
            """,
            (job_type, now, now, now),
        )
        next_job = cursor.fetchone()
        age = None if aggregate is None else aggregate["oldest_age_seconds"]
        return {
            "unfinished_count": 0 if aggregate is None else int(aggregate["unfinished_count"]),
            "oldest_age_seconds": None if age is None else float(age),
            "next_job_key": None if next_job is None else str(next_job["job_key"]),
        }

    @staticmethod
    def _manifest_snapshot(row: Mapping[str, Any] | None) -> dict[str, object] | None:
        if row is None:
            return None
        return {
            "generation_key": str(row["generation_key"]),
            "published_at": row["published_at"].isoformat(),
            "artifact_key": str(row["artifact_key"]),
            "artifact_digest": str(row["artifact_digest"]),
            "record_count": int(row["record_count"]),
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
