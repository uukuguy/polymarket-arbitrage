"""Fenced, synchronous Postgres repository for durable M1 worker effects."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from math import isfinite
from typing import Any, cast
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .alert_delivery import DEFAULT_RUNTIME_DASHBOARD_URL, runtime_incident_transition_payload
from .capacity import classify_database_capacity
from .db_deadlines import CONTROL_PLANE_DB_POLICY
from .models import (
    AlertDeliveryLease,
    CheckpointReceipt,
    CloudUsageDecision,
    JobLease,
    JobState,
    QuoteBatchLeg,
    QuoteBatchReceipt,
    QuoteBatchSpec,
    QuoteRunIdentity,
    SourceAdmissionDecision,
    StructureRangeReceipt,
    StructureRangeSpec,
    StructureSourcePageSpec,
)
from .quote_discovery import (
    decode_discovery_cursor,
    encode_discovery_cursor,
    quote_discovery,
)
from .recovery_records import RecoveryActionRecord
from .runtime_contract import RUNTIME_STAGE_REGISTRY, RetryableHeartbeatError
from .runtime_deadlines import runtime_retry_policy
from .runtime_models import RuntimeEvent, RuntimeEventKind, RuntimeProgress
from .runtime_store import (
    RuntimeEventConflict,
    RuntimeFenceError,
    RuntimeProgressConflict,
    append_runtime_event_cursor,
    start_runtime_attempt_cursor,
    update_runtime_heartbeat_cursor,
    update_runtime_progress_cursor,
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


class StructureParityMismatchError(IncompleteStructureGenerationError):
    """A complete Structure generation conflicts with its frozen source counts."""


class StructureSuccessorBusyError(IncompleteStructureGenerationError):
    """Structure certification must wait for an older executable successor."""


class PublicationPointerConflictError(ControlPlaneError):
    """A stale publication candidate no longer names the current lineage."""


class SoakEvidenceConflictError(ControlPlaneError):
    """A cloud soak run or observation conflicts with immutable evidence."""


class RuntimeEventConflictError(ControlPlaneError):
    """Runtime event idempotency or sequence was reused for different data."""


_FAILURE_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_POINTER_LINEAGE_MARKER = ":pointer="
_POINTER_LINEAGE_NONE = "none"
_POINTER_LINEAGE_UNSET = object()


def _bounded_json_octets(payload: Mapping[str, object], *, maximum: int) -> int:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    octets = len(encoded)
    if octets < 2 or octets > maximum:
        raise ValueError("structure-intelligence-payload-out-of-bounds")
    return octets


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _finite_number(value: object) -> bool:
    return (
        isinstance(value, int | float | Decimal)
        and not isinstance(value, bool)
        and isfinite(float(value))
    )


def _quote_event_context(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {"status": "not-indexed"}
    return {
        "status": "available",
        "title": _optional_text(value.get("title")),
        "is_open": _optional_bool(value.get("is_open")),
        "end_time_ms": _optional_int(value.get("end_time_ms")),
    }


def _quote_neg_risk_context(*, group_id: object, value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {"status": "not-indexed"}
    return {
        "status": "available",
        "group_id": _optional_text(group_id),
        "quality": _optional_text(value.get("quality")),
        "expected_member_count": _optional_int(value.get("expected_member_count")),
    }


def _structure_intelligence_unavailable(
    generation_key: str | None, reason_code: str
) -> dict[str, object]:
    response: dict[str, object] = {
        "schema_version": "m1.structure-intelligence.v1",
        "status": "unavailable",
        "reason_code": reason_code,
    }
    if generation_key is not None:
        response["generation_key"] = generation_key
    return response


def _structure_intelligence_page(
    generation_key: str,
    product: str,
    rows: Sequence[Mapping[str, Any]],
    identifier: str,
    limit: int,
) -> dict[str, object]:
    has_more = len(rows) > limit
    page = rows[:limit]
    return {
        "schema_version": "m1.structure-intelligence.v1",
        "status": "available",
        "generation_key": generation_key,
        "items": [{identifier: str(row[identifier]), **dict(row["payload"])} for row in page],
        "limit": limit,
        "next_after": str(page[-1][identifier]) if has_more else None,
        "product": product,
    }


def _quote_coverage_unavailable(
    status: str, reason_code: str, limit: int, generation_key: str | None = None
) -> dict[str, object]:
    page: dict[str, object] = {
        "schema_version": "m1.quote-coverage-page.v1",
        "status": status,
        "reason_code": reason_code,
        "items": [],
        "limit": limit,
        "next_after": None,
    }
    if generation_key is not None:
        page["generation_key"] = generation_key
    return page


def _event_research_unavailable(status: str, reason_code: str, event_id: str) -> dict[str, object]:
    return {
        "schema_version": "m1.event-research-detail.v1",
        "status": status,
        "reason_code": reason_code,
        "event_id": event_id,
        "groups": [],
    }


def _event_research_legs_unavailable(
    status: str, reason_code: str, event_id: str, group_id: str, limit: int
) -> dict[str, object]:
    return {
        "schema_version": "m1.event-research-group-legs.v1",
        "status": status,
        "reason_code": reason_code,
        "event_id": event_id,
        "group_id": group_id,
        "legs": [],
        "limit": limit,
        "next_after": None,
    }


def _quote_coverage_item(payload: Mapping[str, object], candidate_state: str) -> dict[str, object]:
    """Expose only group coverage health and the next operational action."""
    expected = _optional_int(payload.get("expected_member_count")) or 0
    quoted = _optional_int(payload.get("quoted_member_count")) or 0
    missing = max(expected - quoted, 0)
    if candidate_state == "incomplete-coverage":
        action = (
            "replace invalid or non-executable quote legs"
            if missing == 0
            else "complete required quote legs"
        )
        coverage_state = "coverage-gap"
    elif candidate_state == "positive-edge":
        coverage_state, action = "analysis-ready", "review the group in Analysis funnel"
    elif candidate_state == "no-edge":
        coverage_state, action = "healthy", "coverage complete; no positive group edge"
    else:
        coverage_state, action = "needs-context", "restore current structure context"
    return {
        "group_id": payload.get("group_id"),
        "event_id": payload.get("event_id"),
        "coverage_state": coverage_state,
        "candidate_state": candidate_state,
        "expected_member_count": expected,
        "quoted_member_count": quoted,
        "missing_member_count": missing,
        "quality": payload.get("quality"),
        "event": payload.get("event", {}),
        "action": action,
    }


def _candidate_display_economics(payload: dict[str, object]) -> None:
    """Attach truthful research-only bundle facts without rewriting stored rows."""
    if payload.get("candidate_state") != "positive-edge":
        return
    from .analysis_candidates import candidate_economics

    economics = candidate_economics(
        bundle_cost=payload.get("bundle_cost"), max_bundle_size=payload.get("max_bundle_size")
    )
    if economics is not None:
        payload.update(economics)


def _quote_certification_identity(
    generation_key: str,
    universe_hash: str,
    expected_generation_key: str | None,
) -> str:
    expected = _POINTER_LINEAGE_NONE if expected_generation_key is None else expected_generation_key
    return f"{generation_key}:{universe_hash}{_POINTER_LINEAGE_MARKER}{expected}"


def _parse_quote_certification_identity(
    input_identity: str,
) -> tuple[str, str, str | None, bool]:
    base, marker, expected = input_identity.partition(_POINTER_LINEAGE_MARKER)
    try:
        generation_key, universe_hash = base.rsplit(":", maxsplit=1)
    except ValueError as error:
        raise JobIdentityConflict("quote certifier has malformed input identity") from error
    if not generation_key or not universe_hash:
        raise JobIdentityConflict("quote certifier has malformed input identity")
    if not marker:
        return generation_key, universe_hash, None, False
    if not expected:
        raise JobIdentityConflict("quote certifier has empty pointer lineage")
    expected_generation = None if expected == _POINTER_LINEAGE_NONE else expected
    if expected_generation is not None and not expected_generation.startswith("quote:"):
        raise JobIdentityConflict("quote certifier has malformed pointer lineage")
    return generation_key, universe_hash, expected_generation, True


def _frozen_quote_certification_identity(
    cursor: psycopg.Cursor[dict[str, Any]],
    *,
    generation_key: str,
    universe_hash: str,
    expected_generation_key: str | None,
) -> str:
    job_key = f"{generation_key}:certify"
    cursor.execute("SELECT input_identity FROM m1_jobs WHERE job_key = %s", (job_key,))
    existing = cursor.fetchone()
    if existing is None:
        return _quote_certification_identity(generation_key, universe_hash, expected_generation_key)
    input_identity = str(existing["input_identity"])
    frozen_generation, frozen_universe, _expected, _fenced = _parse_quote_certification_identity(
        input_identity
    )
    if frozen_generation != generation_key or frozen_universe != universe_hash:
        raise JobIdentityConflict(f"job key {job_key!r} names another input")
    return input_identity


def _retry_failure_signature(error_class: str) -> str:
    """Map a bounded exception identity to the durable failure taxonomy."""
    normalized = error_class.casefold()
    if normalized in {
        "operationalerror",
        "poolclosed",
        "pooltimeout",
        "toomanyrequests",
    }:
        return "database.unavailable"
    if "timeout" in normalized or "deadline" in normalized:
        return "upstream.timeout"
    if "progress" in normalized or "stalled" in normalized:
        return "progress.stalled"
    if "service" in normalized or "cancel" in normalized:
        return "service.interrupted"
    if "malformedresponse" in normalized or "jsondecode" in normalized:
        return "upstream.malformed"
    if any(part in normalized for part in ("transport", "network", "connect", "http")):
        return "upstream.transport"
    return "validation.failed"


def _retry_failure_identity(
    *, component: str, error_class: str, detail: Mapping[str, object]
) -> tuple[str, str]:
    """Return a secret-free stable identity and its coarse failure signature."""
    supplied = detail.get("failure_fingerprint")
    if supplied is not None:
        if type(supplied) is not str or _FAILURE_FINGERPRINT_RE.fullmatch(supplied) is None:
            raise ValueError("failure_fingerprint must be a sha256 identity")
        fingerprint = supplied
    else:
        digest = sha256(f"{component}\0{error_class}".encode()).hexdigest()
        fingerprint = f"sha256:{digest}"
    return fingerprint, _retry_failure_signature(error_class)


class RuntimeProgressConflictError(ControlPlaneError):
    """Runtime progress did not strictly advance the current attempt."""


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


# Both direct psycopg connections and the bounded scoped pool return a connection
# context manager.  Keeping that contract here prevents callers from needing an
# unsafe cast when production uses the shared pool.
ConnectionFactory = Callable[[], AbstractContextManager[psycopg.Connection[Any]]]

_SNAPSHOT_RUNTIME_CONTROLLER_ID = "m1-runtime-reconciler"
_SNAPSHOT_RUNTIME_INITIAL_STAGE = "started"
_SNAPSHOT_ACTIVE_TASK_STATES = frozenset({"active", "suspect", "recovering"})
_SNAPSHOT_INCIDENT_STATES = frozenset({"open", "acknowledged", "resolved"})
_SNAPSHOT_INCIDENT_SEVERITIES = frozenset({"info", "warning", "critical"})
_SNAPSHOT_INCIDENT_TRANSITIONS = frozenset(
    {"detected", "recovery-started", "recovery-attempted", "recovered", "resolved", "escalated"}
)
_SNAPSHOT_QUALIFICATION_IMPACTS = frozenset({"breaking", "contained", "qualified", "delayed"})
_SNAPSHOT_ACTION_TARGET_TYPES = frozenset({"job", "circuit", "worker-process", "machine"})
_SNAPSHOT_ACTION_TYPES = frozenset(
    {
        "heartbeat-job",
        "cancel-job",
        "retry-job",
        "reclaim-job",
        "probe-circuit",
        "restart-worker-process",
        "restart-machine",
    }
)
_SNAPSHOT_ACTION_STATES = frozenset({"pending", "running", "completed"})
_SNAPSHOT_ACTION_RESULTS = frozenset({"succeeded", "failed", "stale-noop", "disabled-action"})
_SNAPSHOT_QUALIFICATION_STATES = frozenset(
    {"accumulating", "invalidated", "recovering", "qualified"}
)
_SNAPSHOT_QUALIFICATION_ELIGIBILITY_STATES = frozenset(
    {"eligible", "paused", "blocked", "invalidated", "qualified"}
)
_QUALIFICATION_SNAPSHOT_SQL = """
    SELECT epoch_id, state, role_identity, started_at, last_fact_at,
           required_seconds, coverage_seconds, max_gap_seconds,
           policy_version, release_id, config_id, previous_epoch_id, slo
    FROM m1_qualification_epochs
    ORDER BY CASE WHEN state IN ('accumulating', 'recovering') THEN 0 ELSE 1 END,
             updated_at DESC, epoch_id DESC
    LIMIT 1
"""
_OPERATIONAL_SNAPSHOT_SQL = """
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = {statement_timeout};
SET LOCAL lock_timeout = {lock_timeout};
WITH
snapshot AS MATERIALIZED (
    SELECT COALESCE({observed_at}::timestamptz, clock_timestamp()) AS observed_at
),
runtime_incident_sample AS MATERIALIZED (
    SELECT incident_key, component, severity, state, summary, opened_at, updated_at
    FROM m1_incidents
    WHERE state IN ('open', 'acknowledged')
      AND (component IN ('runtime', 'recovery')
           OR incident_key LIKE 'recovery:' || chr(37))
    ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
             updated_at DESC, incident_key DESC
    LIMIT {sample_limit}
),
qualification_epoch AS MATERIALIZED (
    SELECT epoch_id, state, role_identity, started_at, last_fact_at,
           required_seconds, coverage_seconds, max_gap_seconds,
           policy_version, release_id, config_id, previous_epoch_id, slo
    FROM m1_qualification_epochs
    ORDER BY CASE WHEN state IN ('accumulating', 'recovering') THEN 0 ELSE 1 END,
             updated_at DESC, epoch_id DESC
    LIMIT 1
)
SELECT
    s.observed_at AS snapshot_now,
    COALESCE((
        SELECT jsonb_object_agg(state, count)
        FROM (SELECT state, count(*) AS count FROM m1_jobs GROUP BY state) counts
    ), '{{}}'::jsonb) AS job_counts,
    COALESCE((
        SELECT jsonb_object_agg(state, count)
        FROM (SELECT state, count(*) AS count FROM m1_jobs
              WHERE job_type = 'quote-batch' GROUP BY state) counts
    ), '{{}}'::jsonb) AS quote_batch_states,
    COALESCE((
        SELECT jsonb_object_agg(state, count)
        FROM (SELECT state, count(*) AS count FROM m1_jobs
              WHERE job_type = 'quote-admit' GROUP BY state) counts
    ), '{{}}'::jsonb) AS quote_admission_states,
    (SELECT extract(epoch FROM (s.observed_at - min(created_at)))
     FROM m1_jobs WHERE job_type = 'quote-admit' AND state = 'retryable')
        AS retryable_quote_admission_age,
    COALESCE((
        SELECT jsonb_object_agg(state, count)
        FROM (SELECT state, count(*) AS count FROM m1_jobs
              WHERE job_type = 'quote-certify' GROUP BY state) counts
    ), '{{}}'::jsonb) AS quote_certifier_states,
    (SELECT extract(epoch FROM (s.observed_at - min(created_at)))
     FROM m1_jobs WHERE job_type = 'quote-batch' AND state = 'retryable')
        AS retryable_quote_age,
    (SELECT to_jsonb(pointer_row) FROM (
        SELECT pointer.generation_key, pointer.published_at,
               manifest.artifact_key, manifest.artifact_digest, manifest.record_count,
               lineage.structure_generation_key, lineage.cadence_seconds,
               lineage.cadence_bucket,
               CASE
                   WHEN lineage.cadence_seconds IS NULL THEN NULL
                   ELSE to_timestamp(
                       (lineage.cadence_bucket + 1) * lineage.cadence_seconds
                   )
               END AS next_eligible_at
        FROM m1_publication_pointers AS pointer
        JOIN m1_quote_generation_inputs AS lineage
          ON lineage.generation_key = pointer.generation_key
        JOIN m1_generation_manifests AS manifest
          ON manifest.generation_key = pointer.generation_key
        WHERE pointer.pointer_key = 'quote:current'
    ) pointer_row) AS quote_pointer,
    COALESCE((
        SELECT jsonb_object_agg(state, count)
        FROM (SELECT state, count(*) AS count FROM m1_jobs
              WHERE job_type = 'structure-normalize' GROUP BY state) counts
    ), '{{}}'::jsonb) AS structure_range_states,
    COALESCE((
        SELECT jsonb_object_agg(state, count)
        FROM (SELECT state, count(*) AS count FROM m1_jobs
              WHERE job_type = 'structure-fetch' GROUP BY state) counts
    ), '{{}}'::jsonb) AS source_fetch_states,
    COALESCE((
        SELECT jsonb_object_agg(state, count)
        FROM (SELECT state, count(*) AS count FROM m1_jobs
              WHERE job_type = 'structure-materialize' GROUP BY state) counts
    ), '{{}}'::jsonb) AS source_materializer_states,
    (SELECT extract(epoch FROM (s.observed_at - min(created_at)))
     FROM m1_jobs
     WHERE job_type IN ('structure-fetch', 'structure-materialize') AND state = 'retryable')
        AS retryable_source_age,
    COALESCE((
        SELECT jsonb_object_agg(state, count)
        FROM (SELECT state, count(*) AS count FROM m1_jobs
              WHERE job_type = 'structure-certify' GROUP BY state) counts
    ), '{{}}'::jsonb) AS structure_certifier_states,
    (SELECT extract(epoch FROM (s.observed_at - min(created_at)))
     FROM m1_jobs WHERE job_type = 'structure-normalize' AND state = 'retryable')
        AS retryable_structure_age,
    (SELECT to_jsonb(manifest_row) FROM (
        SELECT generation_key, artifact_key, artifact_digest, record_count, published_at
        FROM m1_generation_manifests
        WHERE generation_key LIKE 'structure:%'
        ORDER BY published_at DESC, generation_key DESC LIMIT 1
    ) manifest_row) AS structure_manifest,
    (SELECT to_jsonb(pointer_row) FROM (
        SELECT pointer.generation_key, pointer.expected_generation_key,
               pointer.published_at, manifest.artifact_key, manifest.artifact_digest,
               manifest.record_count
        FROM m1_publication_pointers AS pointer
        JOIN m1_generation_manifests AS manifest
          ON manifest.generation_key = pointer.generation_key
        WHERE pointer.pointer_key = 'structure:current:shadow'
    ) pointer_row) AS structure_pointer,
    (SELECT extract(epoch FROM (s.observed_at - min(created_at)))
     FROM m1_jobs WHERE state IN ('runnable', 'retryable', 'checkpointed'))
        AS oldest_runnable_age,
    jsonb_build_object(
        'unfinished_count', (SELECT count(*) FROM m1_jobs
            WHERE job_type = 'structure-normalize'
              AND state IN ('runnable', 'retryable', 'leased', 'checkpointed')),
        'oldest_age_seconds', (SELECT extract(epoch FROM (s.observed_at - min(created_at)))
            FROM m1_jobs WHERE job_type = 'structure-normalize'
              AND state IN ('runnable', 'retryable', 'leased', 'checkpointed')),
        'next_job_key', (SELECT job_key FROM m1_jobs
            WHERE job_type = 'structure-normalize'
              AND (((state IN ('runnable', 'retryable', 'checkpointed'))
                    AND (next_attempt_at IS NULL OR next_attempt_at <= s.observed_at))
                   OR (state = 'leased' AND lease_expires_at <= s.observed_at))
            ORDER BY CASE WHEN state = 'retryable' AND next_attempt_at <= s.observed_at
                          THEN 0 ELSE 1 END,
                     next_attempt_at NULLS FIRST, updated_at, job_key LIMIT 1)
    ) AS structure_queue_health,
    jsonb_build_object(
        'unfinished_count', (SELECT count(*) FROM m1_jobs
            WHERE job_type = 'quote-batch'
              AND state IN ('runnable', 'retryable', 'leased', 'checkpointed')),
        'oldest_age_seconds', (SELECT extract(epoch FROM (s.observed_at - min(created_at)))
            FROM m1_jobs WHERE job_type = 'quote-batch'
              AND state IN ('runnable', 'retryable', 'leased', 'checkpointed')),
        'next_job_key', (SELECT job_key FROM m1_jobs
            WHERE job_type = 'quote-batch'
              AND (((state IN ('runnable', 'retryable', 'checkpointed'))
                    AND (next_attempt_at IS NULL OR next_attempt_at <= s.observed_at))
                   OR (state = 'leased' AND lease_expires_at <= s.observed_at))
            ORDER BY CASE WHEN state = 'retryable' AND next_attempt_at <= s.observed_at
                          THEN 0 ELSE 1 END,
                     next_attempt_at NULLS FIRST, updated_at, job_key LIMIT 1)
    ) AS quote_queue_health,
    (SELECT count(*) FROM m1_jobs
     WHERE state = 'leased' AND lease_expires_at <= s.observed_at) AS expired_leases,
    (SELECT count(*) FROM m1_job_circuits WHERE state = 'open') AS open_circuit_count,
    COALESCE((SELECT jsonb_agg(to_jsonb(circuit_row)) FROM (
        SELECT job_key, consecutive_failures, next_probe_at, failure_fingerprint
        FROM m1_job_circuits WHERE state = 'open'
        ORDER BY updated_at DESC, job_key DESC LIMIT {sample_limit}
    ) circuit_row), '[]'::jsonb) AS open_circuits,
    COALESCE((SELECT jsonb_agg(to_jsonb(attempt_row)) FROM (
        SELECT job_key, lease_epoch, worker_id, state, error_class, error_detail
        FROM m1_job_attempts ORDER BY started_at DESC, attempt_id DESC
        LIMIT {sample_limit}
    ) attempt_row), '[]'::jsonb) AS attempts,
    COALESCE((SELECT jsonb_agg(to_jsonb(incident_row)) FROM (
        SELECT incident_key, component, severity, summary
        FROM m1_incidents WHERE state = 'open'
        ORDER BY opened_at DESC, incident_key DESC LIMIT {sample_limit}
    ) incident_row), '[]'::jsonb) AS incidents,
    (SELECT to_jsonb(runtime_current_row) FROM (
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
    ) runtime_current_row) AS runtime_current,
    COALESCE((SELECT jsonb_agg(to_jsonb(runtime_event_row)) FROM (
        SELECT i.incident_key, i.severity, i.summary, e.kind, e.occurred_at, e.detail
        FROM m1_incident_events e
        JOIN m1_incidents i ON i.incident_key = e.incident_key
        WHERE i.dedupe_key = 'runtime-watchdog'
           OR i.dedupe_key LIKE 'runtime-watchdog:' || chr(37)
        ORDER BY e.occurred_at DESC, e.incident_event_id DESC LIMIT {sample_limit}
    ) runtime_event_row), '[]'::jsonb) AS runtime_events,
    COALESCE((SELECT jsonb_agg(to_jsonb(outbox_row)) FROM (
        SELECT i.incident_key, o.channel, o.state
        FROM m1_alert_outbox o
        JOIN m1_incident_events e ON e.incident_event_id = o.incident_event_id
        JOIN m1_incidents i ON i.incident_key = e.incident_key
        WHERE o.state = 'pending'
        ORDER BY o.created_at DESC, o.outbox_id DESC LIMIT {sample_limit}
    ) outbox_row), '[]'::jsonb) AS outbox,
    (SELECT jsonb_build_object(
        'pending_count', count(*) FILTER (WHERE state IN ('pending', 'retryable')),
        'oldest_pending_age_seconds', extract(epoch FROM (
            s.observed_at - min(created_at) FILTER (WHERE state IN ('pending', 'retryable'))
        )),
        'latest_delivery_at', (SELECT max(attempted_at) FROM m1_alert_deliveries),
        'latest_delivery_state', (SELECT state FROM m1_alert_deliveries
            ORDER BY attempted_at DESC, delivery_id DESC LIMIT 1),
        'latest_delivery_channel', (SELECT o.channel FROM m1_alert_deliveries d
            JOIN m1_alert_outbox o ON o.outbox_id = d.outbox_id
            ORDER BY d.attempted_at DESC, d.delivery_id DESC LIMIT 1),
        'latest_delivery_error_class', (SELECT error_class FROM m1_alert_deliveries
            ORDER BY attempted_at DESC, delivery_id DESC LIMIT 1)
    ) FROM m1_alert_outbox) AS alert_delivery,
    (SELECT to_jsonb(soak_row) FROM (
        SELECT run_id, observed_at FROM m1_soak_observations
        ORDER BY observed_at DESC, run_id DESC LIMIT 1
    ) soak_row) AS latest_soak_observation,
    (SELECT to_jsonb(cloud_row) FROM (
        SELECT COALESCE(sum(bytes_received), 0) AS used_bytes,
               max(daily_budget_bytes) AS daily_budget_bytes
        FROM m1_cloud_usage_observations
        WHERE budget_day = (s.observed_at AT TIME ZONE 'UTC')::date
    ) cloud_row) AS cloud_usage,
    (SELECT to_jsonb(cloud_row) FROM (
        SELECT observation_id, source, operation, bytes_received, item_count,
               artifact_key, artifact_digest, observed_at
        FROM m1_cloud_usage_observations
        WHERE budget_day = (s.observed_at AT TIME ZONE 'UTC')::date
        ORDER BY observed_at DESC, observation_id DESC LIMIT 1
    ) cloud_row) AS latest_cloud_usage,
    (SELECT to_jsonb(controller_row) FROM (
        SELECT controller_id, owner_id, lease_epoch, lease_expires_at, claimed_at, updated_at,
               extract(epoch FROM (s.observed_at - updated_at)) AS lease_age_seconds,
               extract(epoch FROM greatest(s.observed_at - lease_expires_at,
                                            interval '0 seconds')) AS lease_overdue_seconds
        FROM m1_runtime_controller_leases
        WHERE controller_id = {controller_id} LIMIT 1
    ) controller_row) AS runtime_controller,
    (SELECT count(*) FROM m1_job_runtime_state runtime
     JOIN m1_jobs job ON job.job_key = runtime.job_key WHERE job.state = 'leased')
        AS active_task_total,
    COALESCE((SELECT jsonb_agg(to_jsonb(task_row)) FROM (
        SELECT job.job_key, runtime.attempt_id, job.job_type, runtime.worker_id,
               runtime.lease_epoch, runtime.stage, runtime.recovery_state,
               runtime.progress_current, runtime.progress_total,
               runtime.started_at, runtime.last_heartbeat_at, runtime.last_progress_at,
               runtime.lease_deadline_at, runtime.heartbeat_deadline_at,
               runtime.progress_deadline_at, runtime.attempt_deadline_at,
               extract(epoch FROM (s.observed_at - runtime.last_heartbeat_at))
                   AS heartbeat_age_seconds,
               extract(epoch FROM (s.observed_at - runtime.last_progress_at))
                   AS progress_age_seconds,
               extract(epoch FROM greatest(
                   s.observed_at - (
                       runtime.last_heartbeat_at
                       + 3 * (runtime.heartbeat_deadline_at - runtime.last_heartbeat_at)
                   ),
                   interval '0 seconds'
               )) AS heartbeat_missing_overdue_seconds,
               extract(epoch FROM greatest(s.observed_at - runtime.progress_deadline_at,
                                            interval '0 seconds'))
                   AS progress_overdue_seconds,
               extract(epoch FROM greatest(s.observed_at - runtime.lease_deadline_at,
                                            interval '0 seconds')) AS lease_overdue_seconds,
               extract(epoch FROM greatest(s.observed_at - runtime.attempt_deadline_at,
                                            interval '0 seconds')) AS attempt_overdue_seconds
        FROM m1_job_runtime_state runtime
        JOIN m1_jobs job ON job.job_key = runtime.job_key
        WHERE job.state = 'leased'
        ORDER BY CASE runtime.recovery_state WHEN 'recovering' THEN 0
                                             WHEN 'suspect' THEN 1 ELSE 2 END,
                 least(runtime.lease_deadline_at, runtime.heartbeat_deadline_at,
                       runtime.progress_deadline_at, runtime.attempt_deadline_at),
                 runtime.started_at, job.job_key
        LIMIT {sample_limit}
    ) task_row), '[]'::jsonb) AS active_tasks,
    (SELECT count(*) FROM m1_incidents
     WHERE state IN ('open', 'acknowledged')
       AND (component IN ('runtime', 'recovery')
            OR incident_key LIKE 'recovery:' || chr(37))) AS runtime_incident_total,
    COALESCE((SELECT jsonb_agg(to_jsonb(incident_row))
              FROM runtime_incident_sample incident_row), '[]'::jsonb)
        AS runtime_incidents,
    COALESCE((SELECT jsonb_agg(to_jsonb(event_row)) FROM (
        SELECT incident_key, kind, detail, occurred_at,
               extract(epoch FROM (s.observed_at - occurred_at)) AS age_seconds
        FROM (
            SELECT event.*, row_number() OVER (
                PARTITION BY incident_key
                ORDER BY occurred_at DESC, incident_event_id DESC
            ) AS event_rank
            FROM m1_incident_events event
            WHERE incident_key IN (SELECT incident_key FROM runtime_incident_sample)
        ) ranked
        WHERE event_rank <= {sample_limit}
        ORDER BY incident_key, occurred_at DESC
    ) event_row), '[]'::jsonb) AS runtime_incident_events,
    (SELECT count(*) FROM m1_recovery_actions) AS recovery_action_total,
    COALESCE((SELECT jsonb_agg(to_jsonb(action_row)) FROM (
        SELECT action_id, incident_key, target_type, target_id, action_type,
               state, result_code, expected_controller_epoch,
               expected_attempt_id, expected_lease_epoch, requested_at,
               started_at, finished_at, next_allowed_at, worker_id,
               worker_epoch, worker_lease_expires_at
        FROM m1_recovery_actions
        ORDER BY CASE state WHEN 'pending' THEN 0 WHEN 'running' THEN 1 ELSE 2 END,
                 COALESCE(finished_at, started_at, requested_at) DESC, action_id DESC
        LIMIT {sample_limit}
    ) action_row), '[]'::jsonb) AS recovery_actions,
    (SELECT to_jsonb(q) FROM qualification_epoch q) AS qualification_epoch,
    (SELECT to_jsonb(certificate_row) FROM (
        SELECT certificate_id, certificate_digest, evidence_digest, qualified_at, created_at
        FROM m1_qualification_certificates
        WHERE epoch_id = (SELECT epoch_id FROM qualification_epoch)
        ORDER BY created_at DESC, certificate_id DESC LIMIT 1
    ) certificate_row) AS qualification_certificate,
    COALESCE(
        (SELECT to_jsonb(breaker_row) FROM (
            SELECT fact_id, reason, observed_at
            FROM m1_qualification_recovery_observations
            WHERE recovering_epoch_id = (SELECT epoch_id FROM qualification_epoch)
              AND reason NOT IN ('healthy', 'recovery.confirmed')
              AND (SELECT state FROM qualification_epoch) = 'recovering'
            ORDER BY observed_at DESC, ingest_seq DESC LIMIT 1
        ) breaker_row),
        (SELECT to_jsonb(breaker_row) FROM (
            SELECT epoch_id AS fact_id, invalidation_reason AS reason,
                   invalidated_at AS observed_at
            FROM m1_qualification_epochs
            WHERE epoch_id = (SELECT previous_epoch_id FROM qualification_epoch)
              AND state = 'invalidated'
              AND (SELECT state FROM qualification_epoch) = 'recovering'
        ) breaker_row)
    ) AS qualification_breaker
FROM snapshot s
"""
_CURRENT_OPPORTUNITIES_SQL = """
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = {statement_timeout};
SET LOCAL lock_timeout = {lock_timeout};
WITH current_projection AS MATERIALIZED (
    SELECT projection.generation_key, projection.record_count
    FROM m1_opportunity_publication_pointers AS pointer
    JOIN m1_opportunity_projections AS projection
      ON projection.generation_key = pointer.generation_key
    WHERE pointer.pointer_key = 'opportunity:current'
)
SELECT current_projection.generation_key, current_projection.record_count,
       COALESCE((SELECT jsonb_agg(to_jsonb(page_row)) FROM (
           SELECT rows.group_id, rows.event_id, rows.membership_hash, rows.bundle_cost,
                  rows.gross_edge_bps, rows.max_bundle_size, rows.legs,
                  rows.structure_observed_at_ms, rows.quote_started_at_ms,
                  rows.quote_quoted_at_ms
           FROM m1_opportunity_projection_rows AS rows
           WHERE rows.generation_key = current_projection.generation_key
             AND rows.group_id > {after_group_id}
           ORDER BY rows.group_id
           LIMIT {page_size}
       ) page_row), '[]'::jsonb) AS rows
FROM current_projection
"""
_ALERT_BOUNDED_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:/._ @#=+-]{0,255}$")
_ALERT_SECRET_WORDS = ("secret", "token", "password", "api_key", "apikey", "authorization")
_ALERT_ACTIONS = frozenset(
    {
        "none",
        "heartbeat-job",
        "cancel-job",
        "retry-job",
        "reclaim-job",
        "probe-circuit",
        "restart-worker-process",
        "restart-machine",
    }
)
_ALERT_QUALIFICATION_IMPACTS = frozenset(
    {"none", "unknown", "delayed", "invalidated", "recovering", "qualified", "breaking"}
)


def _safe_alert_text(value: object, *, max_len: int = 256) -> str | None:
    if not isinstance(value, str) or not value or len(value) > max_len:
        return None
    if not _ALERT_BOUNDED_TEXT.fullmatch(value):
        return None
    lower = value.lower()
    if any(word in lower for word in _ALERT_SECRET_WORDS):
        return None
    return value


def _alert_transition(kind: str) -> str:
    if kind == "recovered":
        return "recovered"
    if kind == "recovery-started":
        return "recovery-started"
    if kind == "escalated":
        return "escalated"
    return "detected"


def _alert_action(kind: str, detail: Mapping[str, object]) -> str:
    action = _safe_alert_text(detail.get("action_type")) or _safe_alert_text(detail.get("action"))
    if action in _ALERT_ACTIONS:
        return action
    if kind == "recovered":
        return "none"
    if kind == "recovery-started":
        return "reclaim-job"
    if kind in {"circuit-opened", "circuit-probe-failed"}:
        return "probe-circuit"
    if kind == "attempt-failed":
        return "retry-job"
    return "none"


def _alert_qualification_impact(kind: str, detail: Mapping[str, object]) -> str:
    impact = _safe_alert_text(detail.get("qualification_impact"))
    if impact in _ALERT_QUALIFICATION_IMPACTS:
        return impact
    if detail.get("qualification_breaking") is True:
        return "breaking"
    if kind == "recovered":
        return "none"
    if kind == "recovery-started":
        return "breaking"
    return "unknown"


def _alert_reason(kind: str, detail: Mapping[str, object]) -> str:
    for field in ("reason_code", "reason", "error_class", "failure_class"):
        reason = _safe_alert_text(detail.get(field))
        if reason is not None:
            return reason
    return kind


def _incident_alert_payload(
    *,
    incident_key: str,
    component: str,
    kind: str,
    detail: Mapping[str, object],
    now: datetime,
    acceptance_run_id: str | None = None,
) -> dict[str, object]:
    return runtime_incident_transition_payload(
        transition=_alert_transition(kind),
        incident_id=incident_key,
        incident_key=incident_key,
        component=component,
        source="transactional-control-plane",
        job_key=_safe_alert_text(detail.get("job_key")),
        stage=(
            _safe_alert_text(detail.get("stage"), max_len=128)
            or _safe_alert_text(detail.get("job_type"), max_len=128)
            or component
        ),
        reason=_alert_reason(kind, detail),
        action=_alert_action(kind, detail),
        qualification_impact=_alert_qualification_impact(kind, detail),
        dashboard_url=DEFAULT_RUNTIME_DASHBOARD_URL,
        occurred_at=now,
        acceptance_run_id=acceptance_run_id,
    )


def _set_structure_read_timeouts(cursor: psycopg.Cursor[Any], *, read_only: bool) -> None:
    """Bound Structure reads and mixed receipt-recovery transactions."""
    if read_only:
        cursor.execute("SET TRANSACTION READ ONLY")
    cursor.execute(
        sql.SQL("SET LOCAL statement_timeout = {}").format(
            sql.Literal(CONTROL_PLANE_DB_POLICY.statement_setting)
        )
    )
    cursor.execute(
        sql.SQL("SET LOCAL lock_timeout = {}").format(
            sql.Literal(CONTROL_PLANE_DB_POLICY.lock_setting)
        )
    )


def _set_fenced_transaction_timeouts(
    cursor: psycopg.Cursor[Any],
    *,
    lease: JobLease,
    now: datetime,
    action_deadline: datetime | None = None,
) -> tuple[int, int]:
    """Bound one fenced transaction below the lease's remaining lifetime."""
    remaining_ms = int((lease.lease_expires_at - now).total_seconds() * 1000)
    if action_deadline is not None:
        remaining_ms = min(
            remaining_ms,
            int((action_deadline - now).total_seconds() * 1000),
        )
    if remaining_ms <= 1:
        raise StaleLeaseError(
            f"lease is no longer current or has no safe terminal budget for {lease.job_key}"
        )
    statement_timeout_ms = min(CONTROL_PLANE_DB_POLICY.statement_timeout_ms, remaining_ms - 1)
    lock_timeout_ms = min(CONTROL_PLANE_DB_POLICY.lock_timeout_ms, statement_timeout_ms)
    cursor.execute(
        sql.SQL("SET LOCAL statement_timeout = {}").format(sql.Literal(f"{statement_timeout_ms}ms"))
    )
    cursor.execute(
        sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(f"{lock_timeout_ms}ms"))
    )
    return statement_timeout_ms, lock_timeout_ms


def _set_snapshot_read_timeouts(cursor: psycopg.Cursor[Any]) -> None:
    cursor.execute("SET TRANSACTION READ ONLY")
    cursor.execute(
        sql.SQL("SET LOCAL statement_timeout = {}").format(
            sql.Literal(CONTROL_PLANE_DB_POLICY.statement_setting)
        )
    )
    cursor.execute(
        sql.SQL("SET LOCAL lock_timeout = {}").format(
            sql.Literal(CONTROL_PLANE_DB_POLICY.lock_setting)
        )
    )


def _snapshot_aware(value: object, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as error:
            raise ControlPlaneError(f"{field} is not timezone-aware") from error
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ControlPlaneError(f"{field} is not timezone-aware")
    return value.astimezone(UTC)


def _snapshot_mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(type(key) is not str for key in value):
        raise ControlPlaneError(f"{field} is malformed")
    return value


def _snapshot_optional_mapping(value: object, field: str) -> Mapping[str, object] | None:
    if value is None:
        return None
    return _snapshot_mapping(value, field)


def _snapshot_rows(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise ControlPlaneError(f"{field} is malformed")
    return tuple(_snapshot_mapping(row, field) for row in value)


def _snapshot_count_map(value: object, field: str) -> dict[str, int]:
    mapping = _snapshot_mapping(value, field)
    return {
        _snapshot_text(key, f"{field}.state"): _snapshot_int(count, f"{field}.count")
        for key, count in mapping.items()
    }


def _snapshot_queue_health(value: object, field: str) -> dict[str, object]:
    mapping = _snapshot_mapping(value, field)
    age = mapping.get("oldest_age_seconds")
    next_job_key = mapping.get("next_job_key")
    return {
        "unfinished_count": _snapshot_int(mapping.get("unfinished_count"), f"{field}.count"),
        "oldest_age_seconds": (
            None if age is None else _snapshot_seconds(age, f"{field}.oldest_age_seconds")
        ),
        "next_job_key": (
            None if next_job_key is None else _snapshot_text(next_job_key, f"{field}.next_job_key")
        ),
    }


def _snapshot_seconds(value: object, field: str) -> float:
    if value is None:
        seconds = 0.0
    elif isinstance(value, int | float | Decimal) and not isinstance(value, bool):
        seconds = float(value)
    else:
        raise ControlPlaneError(f"{field} is malformed")
    if seconds < 0:
        raise ControlPlaneError(f"{field} is negative")
    return seconds


def _snapshot_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ControlPlaneError(f"{field} is malformed")
    result = value
    if result < 0:
        raise ControlPlaneError(f"{field} is negative")
    return result


def _snapshot_text(value: object, field: str, *, limit: int = 256) -> str:
    if type(value) is not str or not value:
        raise ControlPlaneError(f"{field} is malformed")
    text = value.replace("\x00", "")
    text = "".join(character if character.isprintable() else " " for character in text)
    if any(
        marker in text.casefold()
        for marker in ("authorization", "api_key", "apikey", "password", "secret", "token", "dsn")
    ):
        return "<redacted>"
    return text[:limit]


def _snapshot_transition_detail(detail: object) -> dict[str, object]:
    if not isinstance(detail, Mapping):
        raise ControlPlaneError("incident source is malformed")
    result: dict[str, object] = {}
    reason_code = detail.get("reason_code")
    if isinstance(reason_code, str) and reason_code:
        result["reason_code"] = _snapshot_text(reason_code, "reason_code")
    impact = detail.get("qualification_impact")
    if impact is not None:
        if type(impact) is not str or impact not in _SNAPSHOT_QUALIFICATION_IMPACTS:
            raise ControlPlaneError("qualification impact is malformed")
        result["qualification_impact"] = impact
    elif bool(detail.get("qualification_breaking")):
        result["qualification_impact"] = "breaking"
    return result


def _snapshot_role_identity(value: object) -> list[str]:
    if not isinstance(value, list) or not value or any(type(item) is not str for item in value):
        raise ControlPlaneError("qualification source is malformed")
    return [_snapshot_text(item, "role_identity") for item in value]


def _snapshot_json_array(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise ControlPlaneError(f"{field} is malformed")
    return list(value)


def _snapshot_text_array(value: object, field: str) -> list[str]:
    if not isinstance(value, list):
        raise ControlPlaneError(f"{field} is malformed")
    return [_snapshot_text(item, field) for item in value]


_RUNTIME_COLUMNS: dict[str, dict[str, tuple[str, bool, str | None]]] = {
    "m1_job_runtime_state": {
        "job_key": ("text", True, None),
        "attempt_id": ("text", True, None),
        "lease_epoch": ("bigint", True, None),
        "worker_id": ("text", True, None),
        "stage": ("text", True, None),
        "started_at": ("timestamp with time zone", True, None),
        "last_heartbeat_at": ("timestamp with time zone", True, None),
        "last_progress_at": ("timestamp with time zone", True, None),
        "progress_sequence": ("bigint", True, "'0'::bigint"),
        "progress_current": ("bigint", True, "'0'::bigint"),
        "progress_total": ("bigint", False, None),
        "lease_deadline_at": ("timestamp with time zone", True, None),
        "heartbeat_deadline_at": ("timestamp with time zone", True, None),
        "progress_deadline_at": ("timestamp with time zone", True, None),
        "attempt_deadline_at": ("timestamp with time zone", True, None),
        "recovery_state": ("text", True, "'active'::text"),
        "updated_at": ("timestamp with time zone", True, "clock_timestamp()"),
        "policy_version": ("text", True, None),
        "profile_lease_seconds": ("integer", True, None),
        "profile_heartbeat_seconds": ("integer", True, None),
        "profile_progress_seconds": ("integer", True, None),
        "profile_attempt_seconds": ("integer", True, None),
    },
    "m1_job_runtime_events": {
        "event_id": ("text", True, None),
        "job_key": ("text", True, None),
        "attempt_id": ("text", True, None),
        "lease_epoch": ("bigint", True, None),
        "worker_id": ("text", True, None),
        "event_sequence": ("bigint", True, None),
        "kind": ("text", True, None),
        "stage": ("text", True, None),
        "progress_sequence": ("bigint", False, None),
        "progress_current": ("bigint", False, None),
        "progress_total": ("bigint", False, None),
        "detail": ("jsonb", True, "'{}'::jsonb"),
        "occurred_at": ("timestamp with time zone", True, None),
        "idempotency_key": ("text", True, None),
    },
}
_RUNTIME_CONSTRAINTS = {
    ("m1_job_runtime_state", "pk_m1_job_runtime_state"): (
        "p",
        ("job_key",),
        None,
        (),
    ),
    ("m1_job_runtime_state", "uq_m1_job_runtime_state_attempt"): (
        "u",
        ("attempt_id",),
        None,
        (),
    ),
    ("m1_job_runtime_state", "fk_m1_runtime_state_job"): (
        "f",
        ("job_key",),
        "public.m1_jobs",
        ("job_key",),
    ),
    ("m1_job_runtime_state", "fk_m1_runtime_state_attempt"): (
        "f",
        ("attempt_id",),
        "public.m1_job_attempts",
        ("attempt_id",),
    ),
    ("m1_job_runtime_events", "pk_m1_job_runtime_events"): (
        "p",
        ("event_id",),
        None,
        (),
    ),
    ("m1_job_runtime_events", "fk_m1_runtime_events_job"): (
        "f",
        ("job_key",),
        "public.m1_jobs",
        ("job_key",),
    ),
    ("m1_job_runtime_events", "fk_m1_runtime_events_attempt"): (
        "f",
        ("attempt_id",),
        "public.m1_job_attempts",
        ("attempt_id",),
    ),
    ("m1_job_runtime_events", "uq_m1_runtime_events_attempt_sequence"): (
        "u",
        ("attempt_id", "event_sequence"),
        None,
        (),
    ),
    ("m1_job_runtime_events", "uq_m1_runtime_events_idempotency"): (
        "u",
        ("idempotency_key",),
        None,
        (),
    ),
}
_RUNTIME_CHECK_CONSTRAINTS = {
    ("m1_job_runtime_state", "ck_m1_runtime_state_epoch"): ("CHECK (lease_epoch > 0)"),
    ("m1_job_runtime_state", "ck_m1_runtime_state_progress"): (
        "CHECK (progress_sequence >= 0 AND progress_current >= 0 AND "
        "(progress_total IS NULL OR progress_total >= 0 AND "
        "progress_current <= progress_total))"
    ),
    ("m1_job_runtime_state", "ck_m1_runtime_state_recovery"): (
        "CHECK (recovery_state = ANY (ARRAY['active'::text, 'suspect'::text, "
        "'recovering'::text, 'recovered'::text, 'terminal'::text]))"
    ),
    ("m1_job_runtime_state", "ck_m1_runtime_state_policy_profile"): (
        "CHECK (policy_version <> ''::text AND profile_lease_seconds > 0 AND "
        "profile_heartbeat_seconds > 0 AND profile_progress_seconds > 0 AND "
        "profile_attempt_seconds >= profile_progress_seconds)"
    ),
    ("m1_job_runtime_events", "ck_m1_runtime_events_detail_size"): (
        "CHECK (jsonb_typeof(detail) = 'object'::text AND "
        "octet_length(detail::text) <= 4096 AND pg_column_size(detail) <= 4096)"
    ),
    ("m1_job_runtime_events", "ck_m1_runtime_events_epoch"): ("CHECK (lease_epoch > 0)"),
    ("m1_job_runtime_events", "ck_m1_runtime_events_kind"): (
        "CHECK (kind = ANY (ARRAY['job.started'::text, "
        "'job.stage-changed'::text, 'job.lease-at-risk'::text, "
        "'job.progress-stalled'::text, 'job.retryable-failed'::text, "
        "'job.retry-scheduled'::text, 'job.recovery-started'::text, "
        "'job.recovered'::text, 'job.terminal-failed'::text, "
        "'job.succeeded'::text]))"
    ),
    ("m1_job_runtime_events", "ck_m1_runtime_events_progress_current"): (
        "CHECK (progress_current IS NULL OR progress_current >= 0)"
    ),
    ("m1_job_runtime_events", "ck_m1_runtime_events_progress_pair"): (
        "CHECK ((progress_sequence IS NULL) = (progress_current IS NULL))"
    ),
    ("m1_job_runtime_events", "ck_m1_runtime_events_progress_sequence"): (
        "CHECK (progress_sequence IS NULL OR progress_sequence >= 0)"
    ),
    ("m1_job_runtime_events", "ck_m1_runtime_events_progress_total"): (
        "CHECK (progress_total IS NULL OR progress_total >= 0 AND "
        "progress_current IS NOT NULL AND progress_current <= progress_total)"
    ),
    ("m1_job_runtime_events", "ck_m1_runtime_events_sequence"): ("CHECK (event_sequence > 0)"),
}
_RUNTIME_INDEXES = {
    ("m1_job_runtime_state", "m1_job_runtime_state_deadlines"): (
        False,
        ("lease_deadline_at", "heartbeat_deadline_at", "progress_deadline_at"),
    ),
    ("m1_job_runtime_events", "m1_job_runtime_events_job_occurred"): (
        False,
        ("job_key", "occurred_at", "event_sequence"),
    ),
    ("m1_job_runtime_events", "m1_job_runtime_events_attempt_sequence"): (
        False,
        ("attempt_id", "event_sequence"),
    ),
}
_RUNTIME_APPEND_ONLY_FUNCTION_SOURCE = (
    "\n        BEGIN\n"
    "            RAISE EXCEPTION 'runtime events are append-only';\n"
    "        END;\n"
    "        "
)
_RUNTIME_APPEND_ONLY_FUNCTION_SOURCE_SHA256 = sha256(
    _RUNTIME_APPEND_ONLY_FUNCTION_SOURCE.encode()
).hexdigest()
_RUNTIME_APPEND_ONLY_TRIGGER_TGTYPE = 27


def _runtime_column_fingerprint(
    cursor: psycopg.Cursor[Any], tables: Sequence[str]
) -> dict[str, dict[str, tuple[str, bool, str | None]]]:
    cursor.execute(
        """
        SELECT pg_class.relname AS table_name,
               pg_attribute.attname AS column_name,
               pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
                 AS data_type,
               pg_attribute.attnotnull AS not_null,
               pg_get_expr(pg_attrdef.adbin, pg_attrdef.adrelid) AS default_expr
        FROM pg_catalog.pg_attribute
        JOIN pg_catalog.pg_class
          ON pg_class.oid = pg_attribute.attrelid
        JOIN pg_catalog.pg_namespace
          ON pg_namespace.oid = pg_class.relnamespace
        LEFT JOIN pg_catalog.pg_attrdef
          ON pg_attrdef.adrelid = pg_attribute.attrelid
         AND pg_attrdef.adnum = pg_attribute.attnum
        WHERE pg_namespace.nspname = 'public'
          AND pg_class.relname = ANY(%s)
          AND pg_attribute.attnum > 0
          AND pg_attribute.attisdropped IS FALSE
        ORDER BY pg_class.relname, pg_attribute.attnum
        """,
        (list(tables),),
    )
    fingerprint: dict[str, dict[str, tuple[str, bool, str | None]]] = {
        table: {} for table in tables
    }
    for row in cursor.fetchall():
        fingerprint[str(row["table_name"])][str(row["column_name"])] = (
            str(row["data_type"]),
            bool(row["not_null"]),
            None if row["default_expr"] is None else str(row["default_expr"]),
        )
    return fingerprint


def _runtime_constraint_fingerprint(
    cursor: psycopg.Cursor[Any],
) -> dict[tuple[str, str], tuple[str, tuple[str, ...], str | None, tuple[str, ...]]]:
    cursor.execute(
        """
        SELECT pg_class.relname AS table_name,
               pg_constraint.conname AS constraint_name,
               pg_constraint.contype AS constraint_type,
               array_agg(pg_attribute.attname ORDER BY key.ordinality)
                 AS local_columns,
               CASE
                   WHEN pg_constraint.contype = 'f'
                   THEN foreign_namespace.nspname || '.' || foreign_class.relname
                   ELSE NULL
               END AS foreign_table,
               coalesce(
                   array_agg(foreign_attribute.attname ORDER BY foreign_key.ordinality)
                     FILTER (WHERE foreign_attribute.attname IS NOT NULL),
                   ARRAY[]::text[]
               ) AS foreign_columns
        FROM pg_catalog.pg_constraint
        JOIN pg_catalog.pg_class
          ON pg_class.oid = pg_constraint.conrelid
        JOIN pg_catalog.pg_namespace
          ON pg_namespace.oid = pg_class.relnamespace
        JOIN unnest(pg_constraint.conkey)
          WITH ORDINALITY AS key(attnum, ordinality)
          ON true
        JOIN pg_catalog.pg_attribute
          ON pg_attribute.attrelid = pg_class.oid
         AND pg_attribute.attnum = key.attnum
        LEFT JOIN unnest(pg_constraint.confkey)
          WITH ORDINALITY AS foreign_key(attnum, ordinality)
          ON foreign_key.ordinality = key.ordinality
        LEFT JOIN pg_catalog.pg_attribute AS foreign_attribute
          ON foreign_attribute.attrelid = pg_constraint.confrelid
         AND foreign_attribute.attnum = foreign_key.attnum
        LEFT JOIN pg_catalog.pg_class AS foreign_class
          ON foreign_class.oid = pg_constraint.confrelid
        LEFT JOIN pg_catalog.pg_namespace AS foreign_namespace
          ON foreign_namespace.oid = foreign_class.relnamespace
        WHERE pg_namespace.nspname = 'public'
          AND pg_class.relname = ANY(%s)
          AND pg_constraint.conname = ANY(%s)
        GROUP BY pg_class.relname, pg_constraint.conname,
                 pg_constraint.contype, foreign_namespace.nspname, foreign_class.relname
        """,
        (
            sorted({table for table, _name in _RUNTIME_CONSTRAINTS}),
            sorted({name for _table, name in _RUNTIME_CONSTRAINTS}),
        ),
    )
    return {
        (str(row["table_name"]), str(row["constraint_name"])): (
            str(row["constraint_type"]),
            tuple(str(column) for column in row["local_columns"]),
            None if row["foreign_table"] is None else str(row["foreign_table"]),
            tuple(str(column) for column in row["foreign_columns"]),
        )
        for row in cursor.fetchall()
    }


def _runtime_check_constraint_fingerprint(
    cursor: psycopg.Cursor[Any],
) -> dict[tuple[str, str], str]:
    cursor.execute(
        """
        SELECT pg_class.relname AS table_name,
               pg_constraint.conname AS constraint_name,
               pg_get_constraintdef(pg_constraint.oid, true) AS constraint_definition
        FROM pg_catalog.pg_constraint
        JOIN pg_catalog.pg_class
          ON pg_class.oid = pg_constraint.conrelid
        JOIN pg_catalog.pg_namespace
          ON pg_namespace.oid = pg_class.relnamespace
        WHERE pg_namespace.nspname = 'public'
          AND pg_constraint.contype = 'c'
          AND pg_class.relname = ANY(%s)
          AND pg_constraint.conname = ANY(%s)
        """,
        (
            sorted({table for table, _name in _RUNTIME_CHECK_CONSTRAINTS}),
            sorted({name for _table, name in _RUNTIME_CHECK_CONSTRAINTS}),
        ),
    )
    return {
        (str(row["table_name"]), str(row["constraint_name"])): str(row["constraint_definition"])
        for row in cursor.fetchall()
    }


def _runtime_index_fingerprint(
    cursor: psycopg.Cursor[Any],
) -> dict[tuple[str, str], tuple[bool, tuple[str, ...]]]:
    cursor.execute(
        """
        SELECT table_class.relname AS table_name,
               index_class.relname AS index_name,
               pg_index.indisunique AS is_unique,
               array_agg(pg_attribute.attname ORDER BY key.ordinality) AS columns
        FROM pg_catalog.pg_index
        JOIN pg_catalog.pg_class AS table_class
          ON table_class.oid = pg_index.indrelid
        JOIN pg_catalog.pg_namespace
          ON pg_namespace.oid = table_class.relnamespace
        JOIN pg_catalog.pg_class AS index_class
          ON index_class.oid = pg_index.indexrelid
        JOIN unnest(pg_index.indkey)
          WITH ORDINALITY AS key(attnum, ordinality)
          ON true
        JOIN pg_catalog.pg_attribute
          ON pg_attribute.attrelid = table_class.oid
         AND pg_attribute.attnum = key.attnum
        WHERE pg_namespace.nspname = 'public'
          AND table_class.relname = ANY(%s)
          AND index_class.relname = ANY(%s)
        GROUP BY table_class.relname, index_class.relname, pg_index.indisunique
        """,
        (
            sorted({table for table, _name in _RUNTIME_INDEXES}),
            sorted({name for _table, name in _RUNTIME_INDEXES}),
        ),
    )
    return {
        (str(row["table_name"]), str(row["index_name"])): (
            bool(row["is_unique"]),
            tuple(str(column) for column in row["columns"]),
        )
        for row in cursor.fetchall()
    }


class PostgresControlPlane:
    """Own atomic job transitions; callers provide the connection factory."""

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        *,
        readiness_connection_factory: ConnectionFactory | None = None,
        database_capacity_budget_bytes: int = 450_000_000,
    ) -> None:
        if database_capacity_budget_bytes <= 0:
            raise ValueError("database capacity budget must be positive")
        self._connection_factory = connection_factory
        self._readiness_connection_factory = readiness_connection_factory or connection_factory
        self._database_capacity_budget_bytes = database_capacity_budget_bytes

    def database_capacity(self) -> dict[str, object]:
        """Read a bounded database-size diagnostic on an independent connection.

        This intentionally sits outside ``operational_snapshot``: provider pressure
        must not cancel the primary operator read just because a size function or
        relation-size scan is slow or unavailable.
        """
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute("SELECT pg_database_size(current_database()) AS used_bytes")
            size_row = cursor.fetchone()
            if size_row is None:
                raise ControlPlaneError("database capacity probe returned no size")
            used_bytes = int(size_row["used_bytes"])
            cursor.execute(
                """
                SELECT relation, used_bytes
                FROM (
                    SELECT c.relname AS relation, pg_total_relation_size(c.oid) AS used_bytes
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'm', 'p')
                ) relation_sizes
                ORDER BY used_bytes DESC, relation ASC
                LIMIT 10
                """
            )
            largest_relations = [
                {"relation": str(row["relation"]), "used_bytes": int(row["used_bytes"])}
                for row in cursor.fetchall()
            ]
        verdict = classify_database_capacity(
            used_bytes=used_bytes,
            budget_bytes=self._database_capacity_budget_bytes,
            provider_read_only=False,
        )
        return {
            "state": verdict.state,
            "used_bytes": verdict.used_bytes,
            "budget_bytes": verdict.budget_bytes,
            "used_percent": verdict.used_percent,
            "reason_code": verdict.reason_code,
            "largest_relations": largest_relations,
        }

    @staticmethod
    def _pool_snapshot(factory: ConnectionFactory) -> dict[str, int] | None:
        stats_reader = getattr(factory, "pool_stats", None)
        if not callable(stats_reader):
            return None
        stats = stats_reader()
        if not isinstance(stats, Mapping):
            return None
        keys = (
            "pool_size",
            "pool_available",
            "requests_waiting",
            "requests_errors",
            "connections_errors",
        )
        return {key: int(stats.get(key, 0)) for key in keys}

    def database_pool_snapshot(self) -> dict[str, object]:
        """Expose only bounded counters; never DSNs or connection parameters."""
        snapshot: dict[str, object] = {}
        operational = self._pool_snapshot(self._connection_factory)
        if operational is not None:
            snapshot["operational"] = operational
        if self._readiness_connection_factory is not self._connection_factory:
            readiness = self._pool_snapshot(self._readiness_connection_factory)
            if readiness is not None:
                snapshot["readiness"] = readiness
        return snapshot

    def close(self) -> None:
        """Close each owned connection pool exactly once."""
        factories = (self._connection_factory, self._readiness_connection_factory)
        closed: set[int] = set()
        for factory in factories:
            if id(factory) in closed:
                continue
            closed.add(id(factory))
            closer = getattr(factory, "close", None)
            if callable(closer):
                closer()

    def readiness(self) -> bool:
        """Prove the durable authority is readable without building a dashboard snapshot."""
        with (
            self._readiness_connection_factory() as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SELECT 1")
            return cursor.fetchone() == (1,)

    def start_soak_run(self, *, run_id: str, baseline_record: Mapping[str, object]) -> None:
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

    def append_soak_observation(self, *, run_id: str, record: Mapping[str, object]) -> None:
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
            cursor.execute(
                """
                SELECT control_api_url, machine_ids FROM m1_soak_runs WHERE run_id = %s
                """,
                (run_id,),
            )
            run = cursor.fetchone()
            machine_ids = sorted(str(machine_id) for machine_id in observation["machine_states"])
            if (
                run is None
                or run["control_api_url"] != observation["control_api_url"]
                or run["machine_ids"] != machine_ids
            ):
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
            _set_snapshot_read_timeouts(cursor)
            cursor.execute(
                """
                SELECT record FROM m1_soak_observations
                WHERE run_id = %s ORDER BY observed_at ASC
                """,
                (run_id,),
            )
            return tuple(dict(row["record"]) for row in cursor.fetchall())

    def deployment_preflight(self, *, expected_database: str) -> dict[str, object]:
        """Prove the named authority has the complete additive 022 schema.

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
            "m1_job_runtime_state",
            "m1_job_runtime_events",
        )
        expected_runtime_invariants = (
            "append_only_function",
            "append_only_trigger",
            "unique_attempt_event_sequence",
            "unique_idempotency_key",
        )
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_snapshot_read_timeouts(cursor)
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
                raise ControlPlaneError("control-plane revision 022 runtime schema is incomplete")
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
            if _runtime_column_fingerprint(cursor, tuple(_RUNTIME_COLUMNS)) != _RUNTIME_COLUMNS:
                raise ControlPlaneError(
                    "control-plane revision 022 runtime schema fingerprint is incomplete"
                )
            runtime_constraints = _runtime_constraint_fingerprint(cursor)
            if runtime_constraints != _RUNTIME_CONSTRAINTS:
                event_unique_constraints = (
                    ("m1_job_runtime_events", "uq_m1_runtime_events_attempt_sequence"),
                    ("m1_job_runtime_events", "uq_m1_runtime_events_idempotency"),
                )
                if any(
                    runtime_constraints.get(constraint) != _RUNTIME_CONSTRAINTS[constraint]
                    for constraint in event_unique_constraints
                ):
                    raise ControlPlaneError(
                        "control-plane revision 022 runtime event invariants are incomplete"
                    )
                raise ControlPlaneError(
                    "control-plane revision 022 runtime schema fingerprint is incomplete"
                )
            if _runtime_check_constraint_fingerprint(cursor) != _RUNTIME_CHECK_CONSTRAINTS:
                raise ControlPlaneError(
                    "control-plane revision 022 runtime schema fingerprint is incomplete"
                )
            if _runtime_index_fingerprint(cursor) != _RUNTIME_INDEXES:
                raise ControlPlaneError(
                    "control-plane revision 022 runtime schema fingerprint is incomplete"
                )
            cursor.execute(
                """
                SELECT pg_proc.prosrc AS source
                FROM pg_catalog.pg_proc
                JOIN pg_catalog.pg_namespace
                  ON pg_namespace.oid = pg_proc.pronamespace
                WHERE pg_namespace.nspname = 'public'
                  AND pg_proc.proname = 'm1_reject_runtime_event_mutation'
                  AND pg_proc.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype
                  AND pg_proc.pronargs = 0
                """
            )
            function_row = cursor.fetchone()
            found_runtime_invariants: list[str] = [
                "unique_attempt_event_sequence",
                "unique_idempotency_key",
            ]
            if (
                function_row is not None
                and sha256(str(function_row["source"]).encode()).hexdigest()
                == _RUNTIME_APPEND_ONLY_FUNCTION_SOURCE_SHA256
            ):
                found_runtime_invariants.append("append_only_function")
            cursor.execute(
                """
                SELECT pg_trigger.tgtype AS trigger_type,
                       pg_trigger.tgattr::text AS trigger_columns,
                       pg_trigger.tgenabled AS enabled
                FROM pg_catalog.pg_trigger
                JOIN pg_catalog.pg_class
                  ON pg_class.oid = pg_trigger.tgrelid
                JOIN pg_catalog.pg_namespace
                  ON pg_namespace.oid = pg_class.relnamespace
                JOIN pg_catalog.pg_proc
                  ON pg_proc.oid = pg_trigger.tgfoid
                WHERE pg_namespace.nspname = 'public'
                  AND pg_class.relname = 'm1_job_runtime_events'
                  AND pg_trigger.tgname = 'm1_runtime_events_immutable'
                  AND pg_trigger.tgisinternal IS FALSE
                  AND pg_proc.proname = 'm1_reject_runtime_event_mutation'
                """
            )
            trigger = cursor.fetchone()
            if trigger is not None:
                if (
                    str(trigger["enabled"]) in {"O", "A"}
                    and int(trigger["trigger_type"]) == _RUNTIME_APPEND_ONLY_TRIGGER_TGTYPE
                    and str(trigger["trigger_columns"]) == ""
                ):
                    found_runtime_invariants.append("append_only_trigger")
            if tuple(sorted(found_runtime_invariants)) != tuple(
                sorted(expected_runtime_invariants)
            ):
                raise ControlPlaneError(
                    "control-plane revision 022 runtime event invariants are incomplete"
                )
            return {
                "database_name": str(identity["database_name"]),
                "postgres_version": str(identity["postgres_version"]),
                "revision_022_tables": len(found),
                "runtime_event_invariants": list(expected_runtime_invariants),
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
        structure_high_water: int = 1,
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
                SELECT count(*) AS count FROM (
                    SELECT 1 FROM m1_jobs
                    WHERE job_type IN (
                        'structure-materialize', 'structure-normalize', 'structure-certify'
                    )
                      AND state IN (
                        'waiting', 'runnable', 'retryable', 'leased', 'checkpointed'
                      )
                    LIMIT %s
                ) AS unfinished_structure_pipeline
                """,
                (structure_high_water,),
            )
            structure_unfinished = cursor.fetchone()
            if structure_unfinished is None:
                raise RuntimeError("structure backlog count was not returned")
            if int(str(structure_unfinished["count"])) >= structure_high_water:
                return SourceAdmissionDecision(state="backpressured:structure", job_key=None)
            cursor.execute(
                """
                SELECT count(*) AS count FROM (
                    SELECT 1 FROM m1_jobs
                    WHERE job_type = 'quote-batch'
                      AND state IN ('runnable', 'retryable', 'leased', 'checkpointed')
                    LIMIT %s
                ) AS unfinished_quote_pipeline
                """,
                (quote_high_water,),
            )
            quote_unfinished = cursor.fetchone()
            if quote_unfinished is None:
                raise RuntimeError("quote backlog count was not returned")
            if int(str(quote_unfinished["count"])) >= quote_high_water:
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

    def admit_due_quote_refresh(
        self,
        *,
        cadence_seconds: int,
        now: datetime,
    ) -> SourceAdmissionDecision:
        """Admit one run-scoped Quote refresh over current certified Structure truth."""
        if isinstance(cadence_seconds, bool) or cadence_seconds <= 0:
            raise ValueError("quote refresh cadence_seconds must be positive")
        self._validate_aware(now, "now")
        cadence_bucket = int(now.timestamp()) // cadence_seconds
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("m1:quote-generation-admission",),
            )
            cursor.execute(
                """
                SELECT 1 FROM m1_jobs
                WHERE (
                    job_type IN (
                        'quote-admit', 'quote-batch', 'quote-certify', 'opportunity-certify'
                    )
                    AND state IN (
                        'waiting', 'runnable', 'retryable', 'leased', 'checkpointed'
                    )
                ) OR (
                    job_type = 'structure-certify'
                    AND state IN ('runnable', 'retryable', 'leased', 'checkpointed')
                )
                LIMIT 1
                """
            )
            if cursor.fetchone() is not None:
                return SourceAdmissionDecision(state="busy", job_key=None)
            cursor.execute(
                """
                SELECT lineage.structure_generation_key, lineage.universe_hash,
                       admission.bundle_key, admission.bundle_digest
                FROM m1_publication_pointers AS pointer
                JOIN m1_quote_generation_inputs AS lineage
                  ON lineage.generation_key = pointer.generation_key
                JOIN m1_quote_admission_inputs AS admission
                  ON admission.generation_key = lineage.structure_generation_key
                WHERE pointer.pointer_key = 'quote:current'
                ORDER BY admission.admitted_at, admission.job_key
                LIMIT 1
                """
            )
            current = cursor.fetchone()
            if current is None:
                return SourceAdmissionDecision(state="busy", job_key=None)
            identity = QuoteRunIdentity.create(
                structure_generation_key=str(current["structure_generation_key"]),
                universe_hash=str(current["universe_hash"]),
                cadence_seconds=cadence_seconds,
                cadence_bucket=cadence_bucket,
            )
            job_key = f"{identity.generation_key}:admit"
            cursor.execute(
                "SELECT generation_key FROM m1_quote_generation_inputs WHERE generation_key = %s",
                (identity.generation_key,),
            )
            if cursor.fetchone() is not None:
                return SourceAdmissionDecision(state="busy", job_key=None)
            cursor.execute(
                """
                INSERT INTO m1_quote_generation_inputs (
                    generation_key, structure_generation_key, universe_hash,
                    cadence_seconds, cadence_bucket, admitted_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    identity.generation_key,
                    identity.structure_generation_key,
                    identity.universe_hash,
                    identity.cadence_seconds,
                    identity.cadence_bucket,
                    now,
                ),
            )
            bundle_key = str(current["bundle_key"])
            bundle_digest = str(current["bundle_digest"])
            input_identity = (
                f"{identity.structure_generation_key}:{bundle_key}:{bundle_digest}:"
                f"{identity.generation_key}"
            )
            self._enqueue_job_cursor(
                cursor,
                job_key=job_key,
                job_type="quote-admit",
                input_identity=input_identity,
                now=now,
            )
            cursor.execute(
                """
                INSERT INTO m1_quote_admission_inputs (
                    job_key, generation_key, bundle_key, bundle_digest,
                    quote_generation_key, admitted_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    job_key,
                    identity.structure_generation_key,
                    bundle_key,
                    bundle_digest,
                    identity.generation_key,
                    now,
                ),
            )
        return SourceAdmissionDecision(state="admitted", job_key=job_key)

    def structure_source_page_spec(self, job_key: str) -> StructureSourcePageSpec:
        """Load an admitted source page exactly as a replacement worker sees it."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
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
            _set_structure_read_timeouts(cursor, read_only=True)
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
            _set_structure_read_timeouts(cursor, read_only=False)
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
                self._recover_structure_terminal_success_cursor(
                    cursor,
                    lease=lease,
                    stage="commit-bundle",
                    component="structure-materialize",
                    data_product="structure-sync",
                    checkpoint_cursor="bundle",
                    checkpoint_digest=bundle.sha256,
                    now=now,
                )
                return self._structure_generation_specs_cursor(cursor, bundle.sha256)
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            self._append_job_succeeded_cursor(
                cursor,
                lease=lease,
                stage="commit-bundle",
                component="structure-materialize",
                data_product="structure-sync",
                now=now,
            )
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
            _set_structure_read_timeouts(cursor, read_only=False)
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
                if self._recover_structure_terminal_success_cursor(
                    cursor,
                    lease=lease,
                    stage="commit-page",
                    component="structure-fetch",
                    data_product="structure-sync",
                    checkpoint_cursor=f"{spec.stream}:{spec.ordinal}",
                    checkpoint_digest=artifact_digest,
                    now=now,
                ):
                    return None
                if spec.stream == "events" and completed and normalized_market_batches is not None:
                    return self._admit_scoped_market_batches_cursor(
                        cursor, event_spec=spec, market_batches=normalized_market_batches, now=now
                    )
                if event_embedded_markets:
                    self._complete_event_embedded_source_window_cursor(
                        cursor, event_spec=spec, now=now
                    )
                    return None
                successor = self._source_successor_spec_cursor(
                    cursor, spec=spec, next_cursor=next_cursor, completed=completed
                )
                if successor is not None:
                    self._enqueue_structure_source_page_cursor(cursor, spec=successor, now=now)
                return successor
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            self._append_job_succeeded_cursor(
                cursor,
                lease=lease,
                stage="commit-page",
                component="structure-fetch",
                data_product="structure-sync",
                now=now,
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
                if normalized_market_batches is None:
                    raise RuntimeError("scoped market admission is missing normalized batches")
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
            structure_generation_key = f"structure:{structure_receipt_digest}"
            cursor.execute(
                """
                INSERT INTO m1_quote_generation_inputs (
                    generation_key, structure_generation_key, universe_hash,
                    cadence_seconds, cadence_bucket, admitted_at
                ) VALUES (%s, %s, %s, NULL, NULL, %s)
                ON CONFLICT (generation_key) DO NOTHING
                """,
                (generation_key, structure_generation_key, universe_hash, now),
            )
            cursor.execute(
                """
                SELECT structure_generation_key, universe_hash
                FROM m1_quote_generation_inputs WHERE generation_key = %s
                """,
                (generation_key,),
            )
            lineage = cursor.fetchone()
            if lineage is None or (
                str(lineage["structure_generation_key"]) != structure_generation_key
                or str(lineage["universe_hash"]) != universe_hash
            ):
                raise JobIdentityConflict("Quote generation lineage conflicts")
            cursor.execute(
                "SELECT generation_key FROM m1_publication_pointers "
                "WHERE pointer_key = 'quote:current' FOR UPDATE"
            )
            pointer = cursor.fetchone()
            expected_generation_key = None if pointer is None else str(pointer["generation_key"])
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
                input_identity=_frozen_quote_certification_identity(
                    cursor,
                    generation_key=generation_key,
                    universe_hash=universe_hash,
                    expected_generation_key=expected_generation_key,
                ),
                now=now,
                initial_state=JobState.WAITING,
            )
        return batches

    def quote_admission_input(self, job_key: str) -> tuple[str, str, str, str]:
        """Load the immutable Structure bundle identity for one Quote-admit job."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_snapshot_read_timeouts(cursor)
            cursor.execute(
                """
                SELECT generation_key, bundle_key, bundle_digest, quote_generation_key
                FROM m1_quote_admission_inputs WHERE job_key = %s
                """,
                (job_key,),
            )
            row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"Quote admission input is unavailable for {job_key!r}")
        return (
            str(row["generation_key"]),
            str(row["bundle_key"]),
            str(row["bundle_digest"]),
            str(row["quote_generation_key"]),
        )

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
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            # Keep every terminal lock and statement bounded below the live
            # lease.  SET LOCAL makes both limits rollback-scoped.
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            generation_key, bundle_key, bundle_digest, quote_generation_key = (
                self._quote_admission_input_cursor(cursor, lease.job_key)
            )
            legacy_job_key = f"{generation_key}:quote-admit"
            expected_input_identity = f"{generation_key}:{bundle_key}:{bundle_digest}"
            if lease.job_key != legacy_job_key:
                expected_input_identity = f"{expected_input_identity}:{quote_generation_key}"
            if lease.input_identity != expected_input_identity:
                raise JobIdentityConflict("Quote admission lease names another Structure bundle")
            if structure_receipt_digest != bundle_digest:
                raise CheckpointConflictError("Quote admission names another Structure bundle")
            batches = self.quote_batches_from_legs(
                structure_receipt_digest=structure_receipt_digest,
                quote_generation_digest=quote_generation_key.removeprefix("quote:"),
                universe_hash=universe_hash,
                legs=legs,
                batch_size=batch_size,
            )
            if set(input_artifacts) != {batch.job_key for batch in batches}:
                raise JobIdentityConflict(
                    "Quote admission requires one R2 input reference per batch"
                )
            self._append_quote_admission_success_cursor(cursor, lease=lease, now=now)
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
    def _append_job_succeeded_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        lease: JobLease,
        stage: str,
        component: str,
        data_product: str,
        now: datetime,
    ) -> RuntimeEvent:
        """Append one terminal success while the supplied job fence is leased.

        The runtime event is deliberately appended before the specialized
        receipt/pointer method releases ``m1_jobs``.  All callers share this
        cursor-level implementation so an injected event failure rolls back
        the receipt, pointer, job, and attempt rows in the surrounding
        transaction.
        """
        cursor.execute(
            """
            SELECT attempt_id, lease_epoch, worker_id, progress_sequence,
                   progress_current, progress_total
            FROM public.m1_job_runtime_state
            WHERE job_key = %s
            FOR UPDATE
            """,
            (lease.job_key,),
        )
        state = cursor.fetchone()
        if state is None:
            raise StaleLeaseError(f"runtime attempt is no longer current for {lease.job_key}")
        if (
            int(state["lease_epoch"]) != lease.lease_epoch
            or str(state["worker_id"]) != lease.lease_owner
        ):
            raise StaleLeaseError(f"runtime attempt is no longer current for {lease.job_key}")
        cursor.execute(
            """
            SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
            FROM m1_job_runtime_events
            WHERE attempt_id = %s
            """,
            (state["attempt_id"],),
        )
        sequence_row = cursor.fetchone()
        if sequence_row is None:
            raise ControlPlaneError("runtime event sequence query returned no row")
        progress_sequence = int(state["progress_sequence"])
        progress = (
            None
            if progress_sequence == 0
            else RuntimeProgress(
                sequence=progress_sequence,
                current=int(state["progress_current"]),
                total=(None if state["progress_total"] is None else int(state["progress_total"])),
                stage=stage,
            )
        )
        try:
            return append_runtime_event_cursor(
                cursor,
                RuntimeEvent(
                    job_key=lease.job_key,
                    attempt_id=str(state["attempt_id"]),
                    lease_epoch=lease.lease_epoch,
                    worker_id=lease.lease_owner,
                    event_sequence=int(sequence_row["next_sequence"]),
                    kind=RuntimeEventKind.SUCCEEDED,
                    stage=stage,
                    progress=progress,
                    detail={
                        "component": component,
                        "data_product": data_product,
                        "qualification_impact": "qualified",
                        "result_code": "ok",
                    },
                    occurred_at=now,
                    idempotency_key=f"runtime:{state['attempt_id']}:succeeded",
                ),
            )
        except RuntimeFenceError as error:
            # Keep the public control-plane contract stable: callers should
            # treat a fenced terminal operation as stale, never as a normal
            # Quote admission failure eligible for retry under the old lease.
            raise StaleLeaseError(str(error)) from error

    @staticmethod
    def _append_historical_job_succeeded_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        lease: JobLease,
        stage: str,
        component: str,
        data_product: str,
        now: datetime,
    ) -> RuntimeEvent:
        """Append one success fact for a proven terminal attempt.

        The normal append helper intentionally requires a live leased job.
        Recovery instead locks the durable succeeded job, attempt, and runtime
        projection and only repairs the missing immutable event.
        """
        cursor.execute(
            """
            SELECT attempt_id, lease_epoch, worker_id, progress_sequence,
                   progress_current, progress_total
            FROM m1_job_runtime_state WHERE job_key = %s FOR UPDATE
            """,
            (lease.job_key,),
        )
        state = cursor.fetchone()
        if state is None:
            raise StaleLeaseError(f"runtime attempt is no longer current for {lease.job_key}")
        if (
            int(state["lease_epoch"]) != lease.lease_epoch
            or str(state["worker_id"]) != lease.lease_owner
        ):
            raise StaleLeaseError(f"runtime attempt is no longer current for {lease.job_key}")
        cursor.execute(
            """
            SELECT state, worker_id FROM m1_job_attempts
            WHERE job_key = %s AND lease_epoch = %s FOR UPDATE
            """,
            (lease.job_key, lease.lease_epoch),
        )
        attempt = cursor.fetchone()
        if (
            attempt is None
            or str(attempt["state"]) != JobState.SUCCEEDED.value
            or str(attempt["worker_id"]) != lease.lease_owner
        ):
            raise StaleLeaseError(f"durable attempt is no longer current for {lease.job_key}")
        cursor.execute(
            """
            SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
            FROM m1_job_runtime_events WHERE attempt_id = %s
            """,
            (state["attempt_id"],),
        )
        sequence_row = cursor.fetchone()
        if sequence_row is None:
            raise ControlPlaneError("runtime event sequence query returned no row")
        progress_sequence = int(state["progress_sequence"])
        progress = (
            None
            if progress_sequence == 0
            else RuntimeProgress(
                sequence=progress_sequence,
                current=int(state["progress_current"]),
                total=(None if state["progress_total"] is None else int(state["progress_total"])),
                stage=stage,
            )
        )
        return PostgresControlPlane._append_structure_recovery_event_cursor(
            cursor,
            event=RuntimeEvent(
                job_key=lease.job_key,
                attempt_id=str(state["attempt_id"]),
                lease_epoch=lease.lease_epoch,
                worker_id=lease.lease_owner,
                event_sequence=int(sequence_row["next_sequence"]),
                kind=RuntimeEventKind.SUCCEEDED,
                stage=stage,
                progress=progress,
                detail={
                    "component": component,
                    "data_product": data_product,
                    "qualification_impact": "qualified",
                    "result_code": "ok",
                },
                occurred_at=now,
                idempotency_key=f"runtime:{state['attempt_id']}:succeeded",
            ),
        )

    @staticmethod
    def _recover_structure_terminal_success_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        lease: JobLease,
        stage: str,
        component: str,
        data_product: str,
        checkpoint_cursor: str,
        checkpoint_digest: str,
        now: datetime,
    ) -> bool:
        """Complete a receipt-backed Structure terminal effect exactly once."""
        cursor.execute(
            """
            SELECT state, lease_owner, lease_epoch, checkpoint_cursor, checkpoint_digest
            FROM m1_jobs
            WHERE job_key = %s
            FOR UPDATE
            """,
            (lease.job_key,),
        )
        job = cursor.fetchone()
        if job is None:
            raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
        if str(job["state"]) == "succeeded":
            if (
                int(job["lease_epoch"]) != lease.lease_epoch
                or str(job["checkpoint_cursor"]) != checkpoint_cursor
                or str(job["checkpoint_digest"]) != checkpoint_digest
            ):
                raise CheckpointConflictError(
                    f"succeeded Structure job has conflicting durable checkpoint: {lease.job_key}"
                )
            cursor.execute(
                """
                SELECT attempt_id, lease_epoch, worker_id, state
                FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s
                FOR UPDATE
                """,
                (lease.job_key, lease.lease_epoch),
            )
            attempt = cursor.fetchone()
            if (
                attempt is None
                or str(attempt["state"]) != "succeeded"
                or int(attempt["lease_epoch"]) != lease.lease_epoch
                or str(attempt["worker_id"]) != lease.lease_owner
            ):
                raise StaleLeaseError(f"durable attempt is no longer current for {lease.job_key}")
            cursor.execute(
                """
                SELECT attempt_id, lease_epoch, worker_id, progress_sequence,
                       progress_current, progress_total
                FROM m1_job_runtime_state
                WHERE job_key = %s
                FOR UPDATE
                """,
                (lease.job_key,),
            )
            runtime_state = cursor.fetchone()
            if (
                runtime_state is None
                or str(runtime_state["attempt_id"]) != str(attempt["attempt_id"])
                or int(runtime_state["lease_epoch"]) != lease.lease_epoch
                or str(runtime_state["worker_id"]) != lease.lease_owner
            ):
                raise StaleLeaseError(f"runtime attempt is no longer current for {lease.job_key}")
            cursor.execute(
                """
                SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
                FROM m1_job_runtime_events
                WHERE attempt_id = %s
                """,
                (attempt["attempt_id"],),
            )
            sequence_row = cursor.fetchone()
            if sequence_row is None:
                raise ControlPlaneError("runtime event sequence query returned no row")
            progress_sequence = int(runtime_state["progress_sequence"])
            progress = (
                None
                if progress_sequence == 0
                else RuntimeProgress(
                    sequence=progress_sequence,
                    current=int(runtime_state["progress_current"]),
                    total=(
                        None
                        if runtime_state["progress_total"] is None
                        else int(runtime_state["progress_total"])
                    ),
                    stage=stage,
                )
            )
            PostgresControlPlane._append_structure_recovery_event_cursor(
                cursor,
                event=RuntimeEvent(
                    job_key=lease.job_key,
                    attempt_id=str(attempt["attempt_id"]),
                    lease_epoch=lease.lease_epoch,
                    worker_id=lease.lease_owner,
                    event_sequence=int(sequence_row["next_sequence"]),
                    kind=RuntimeEventKind.SUCCEEDED,
                    stage=stage,
                    progress=progress,
                    detail={
                        "component": component,
                        "data_product": data_product,
                        "qualification_impact": "qualified",
                        "result_code": "ok",
                    },
                    occurred_at=now,
                    idempotency_key=f"runtime:{attempt['attempt_id']}:succeeded",
                ),
            )
            return True
        if (
            str(job["state"]) != "leased"
            or str(job["lease_owner"]) != lease.lease_owner
            or int(job["lease_epoch"]) != lease.lease_epoch
        ):
            raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
        _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
        PostgresControlPlane._append_job_succeeded_cursor(
            cursor,
            lease=lease,
            stage=stage,
            component=component,
            data_product=data_product,
            now=now,
        )
        cursor.execute(
            """
            UPDATE m1_jobs
            SET checkpoint_cursor = %s, checkpoint_digest = %s,
                state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
                updated_at = %s
            WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
              AND state = 'leased'
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
        cursor.execute(
            """
            UPDATE m1_job_attempts
            SET state = 'succeeded', finished_at = %s
            WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
            """,
            (now, lease.job_key, lease.lease_epoch),
        )
        return False

    @staticmethod
    def _append_structure_recovery_event_cursor(
        cursor: psycopg.Cursor[dict[str, Any]], *, event: RuntimeEvent
    ) -> RuntimeEvent:
        """Repair one proven terminal event without manufacturing a live lease.

        The caller has already locked and matched the succeeded job, durable
        attempt, and runtime projection.  This path only appends to the
        immutable event table; it never changes ``m1_jobs`` to make the normal
        live-fence append helper accept a historical terminal row.
        """
        cursor.execute(
            """
            SELECT job_key, attempt_id, lease_epoch, worker_id, event_sequence,
                   kind, stage, progress_sequence, progress_current, progress_total,
                   detail, idempotency_key
            FROM m1_job_runtime_events
            WHERE idempotency_key = %s
            FOR UPDATE
            """,
            (event.idempotency_key,),
        )
        existing = cursor.fetchone()
        if existing is not None:
            expected_progress_sequence = None if event.progress is None else event.progress.sequence
            expected_progress_current = None if event.progress is None else event.progress.current
            expected_progress_total = None if event.progress is None else event.progress.total
            if (
                str(existing["job_key"]) != event.job_key
                or str(existing["attempt_id"]) != event.attempt_id
                or int(existing["lease_epoch"]) != event.lease_epoch
                or str(existing["worker_id"]) != event.worker_id
                or int(existing["event_sequence"]) < 1
                or str(existing["kind"]) != event.kind.value
                or str(existing["stage"]) != event.stage
                or (
                    None
                    if existing["progress_sequence"] is None
                    else int(existing["progress_sequence"])
                )
                != expected_progress_sequence
                or (
                    None
                    if existing["progress_current"] is None
                    else int(existing["progress_current"])
                )
                != expected_progress_current
                or (None if existing["progress_total"] is None else int(existing["progress_total"]))
                != expected_progress_total
                or dict(existing["detail"]) != dict(event.detail)
                or str(existing["idempotency_key"]) != event.idempotency_key
            ):
                raise RuntimeEventConflictError(
                    f"runtime success event conflicts: {event.idempotency_key!r}"
                )
            return event

        cursor.execute(
            """
            SELECT event_id
            FROM m1_job_runtime_events
            WHERE job_key = %s AND attempt_id = %s AND lease_epoch = %s
              AND worker_id = %s AND kind = %s
            FOR UPDATE
            """,
            (
                event.job_key,
                event.attempt_id,
                event.lease_epoch,
                event.worker_id,
                event.kind.value,
            ),
        )
        if cursor.fetchone() is not None:
            raise RuntimeEventConflictError(
                f"runtime success event already exists for {event.attempt_id!r}"
            )
        progress_sequence = None if event.progress is None else event.progress.sequence
        progress_current = None if event.progress is None else event.progress.current
        progress_total = None if event.progress is None else event.progress.total
        cursor.execute(
            """
            INSERT INTO m1_job_runtime_events (
                event_id, job_key, attempt_id, lease_epoch, worker_id,
                event_sequence, kind, stage, progress_sequence, progress_current,
                progress_total, detail, occurred_at, idempotency_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(uuid4()),
                event.job_key,
                event.attempt_id,
                event.lease_epoch,
                event.worker_id,
                event.event_sequence,
                event.kind.value,
                event.stage,
                progress_sequence,
                progress_current,
                progress_total,
                Jsonb(dict(event.detail)),
                event.occurred_at,
                event.idempotency_key,
            ),
        )
        # The normal helper is now an idempotency-only validator because the
        # row exists.  Keeping this call gives injected append failures the
        # same all-or-nothing rollback boundary as live terminal commits.
        return append_runtime_event_cursor(cursor, event)

    @staticmethod
    def _append_quote_admission_success_cursor(
        cursor: psycopg.Cursor[dict[str, Any]], *, lease: JobLease, now: datetime
    ) -> RuntimeEvent:
        """Keep Quote admission's established control-plane event payload."""
        return PostgresControlPlane._append_job_succeeded_cursor(
            cursor,
            lease=lease,
            stage="commit-admission",
            component="control-plane",
            data_product="market-snapshot",
            now=now,
        )

    @staticmethod
    def _append_retry_runtime_events_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        lease: JobLease,
        component: str,
        error_class: str,
        retry_count: int,
        backoff_seconds: int,
        next_attempt_at: datetime,
        now: datetime,
        detail: Mapping[str, object] | None = None,
    ) -> tuple[RuntimeEvent, RuntimeEvent]:
        """Append bounded failure and retry facts before releasing a job lease."""
        cursor.execute(
            """
            SELECT attempt_id, lease_epoch, worker_id, stage, progress_sequence,
                   progress_current, progress_total
            FROM public.m1_job_runtime_state
            WHERE job_key = %s
            FOR UPDATE
            """,
            (lease.job_key,),
        )
        state = cursor.fetchone()
        if state is None:
            raise StaleLeaseError(f"runtime attempt is no longer current for {lease.job_key}")
        if (
            int(state["lease_epoch"]) != lease.lease_epoch
            or str(state["worker_id"]) != lease.lease_owner
        ):
            raise StaleLeaseError(f"runtime attempt is no longer current for {lease.job_key}")

        # ``finish_retryable_with_incident`` intentionally preserves the
        # checkpointed state as a legal retry source.  The cursor-level runtime
        # fence is defined in terms of a leased job, so move that state into its
        # leased representation inside this transaction; a later failure rolls
        # the temporary transition back with every other effect.
        cursor.execute(
            """
            UPDATE public.m1_jobs SET state = 'leased'
            WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
              AND state = 'checkpointed'
            """,
            (lease.job_key, lease.lease_owner, lease.lease_epoch),
        )
        stage = str(state["stage"])
        progress_sequence = int(state["progress_sequence"])
        progress = (
            None
            if progress_sequence == 0
            else RuntimeProgress(
                sequence=progress_sequence,
                current=int(state["progress_current"]),
                total=(None if state["progress_total"] is None else int(state["progress_total"])),
                stage=stage,
            )
        )
        failure_signature = _retry_failure_signature(error_class)
        interrupted = failure_signature == "service.interrupted"
        detail = {} if detail is None else detail
        explicit_reason = detail.get("reason_code")
        explicit_impact = detail.get("qualification_impact")
        if explicit_reason == "freshness.quote":
            reason_code = "freshness.quote"
        elif failure_signature == "upstream.timeout":
            reason_code = "timeout"
        elif failure_signature == "progress.stalled":
            reason_code = "invalid-input"
        elif failure_signature == "service.interrupted":
            reason_code = "service-stop"
        else:
            reason_code = "invalid-input"
        qualification_impact = "blocked" if explicit_impact == "breaking" else "delayed"
        recovery_policy = "retry-same-input" if interrupted else "exponential-backoff"
        attempt_id = str(state["attempt_id"])
        cursor.execute(
            """
            SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
            FROM public.m1_job_runtime_events
            WHERE attempt_id = %s
            """,
            (attempt_id,),
        )
        sequence_row = cursor.fetchone()
        if sequence_row is None:
            raise ControlPlaneError("runtime event sequence query returned no row")
        first_sequence = int(sequence_row["next_sequence"])
        try:
            failed = append_runtime_event_cursor(
                cursor,
                RuntimeEvent(
                    job_key=lease.job_key,
                    attempt_id=attempt_id,
                    lease_epoch=lease.lease_epoch,
                    worker_id=lease.lease_owner,
                    event_sequence=first_sequence,
                    kind=RuntimeEventKind.RETRYABLE_FAILED,
                    stage=stage,
                    progress=progress,
                    detail={
                        "component": component,
                        "failure_signature": failure_signature,
                        "qualification_impact": qualification_impact,
                        "reason_code": reason_code,
                        "recovery_policy": recovery_policy,
                        "retry_count": retry_count,
                    },
                    occurred_at=now,
                    idempotency_key=f"runtime:{attempt_id}:retryable-failed",
                ),
            )
            scheduled = append_runtime_event_cursor(
                cursor,
                RuntimeEvent(
                    job_key=lease.job_key,
                    attempt_id=attempt_id,
                    lease_epoch=lease.lease_epoch,
                    worker_id=lease.lease_owner,
                    event_sequence=first_sequence + 1,
                    kind=RuntimeEventKind.RETRY_SCHEDULED,
                    stage=stage,
                    progress=progress,
                    detail={
                        "backoff_seconds": backoff_seconds,
                        "next_decision_at": next_attempt_at.isoformat(),
                        "reason_code": reason_code,
                        "recovery_policy": recovery_policy,
                        "retry_count": retry_count,
                    },
                    occurred_at=now,
                    idempotency_key=f"runtime:{attempt_id}:retry-scheduled",
                ),
            )
        except RuntimeFenceError as error:
            raise StaleLeaseError(str(error)) from error
        except RuntimeEventConflict as error:
            raise RuntimeEventConflictError(str(error)) from error
        return failed, scheduled

    @staticmethod
    def quote_batches_from_legs(
        *,
        structure_receipt_digest: str,
        quote_generation_digest: str | None = None,
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
                quote_generation_digest=quote_generation_digest,
                universe_hash=universe_hash,
                ordinal=ordinal,
                legs=normalized_legs[start : start + batch_size],
            )
            for ordinal, start in enumerate(range(0, len(normalized_legs), batch_size))
        )

    @staticmethod
    def _quote_admission_input_cursor(
        cursor: psycopg.Cursor[dict[str, Any]], job_key: str
    ) -> tuple[str, str, str, str]:
        cursor.execute(
            """
            SELECT generation_key, bundle_key, bundle_digest, quote_generation_key
            FROM m1_quote_admission_inputs WHERE job_key = %s
            """,
            (job_key,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"Quote admission input is unavailable for {job_key!r}")
        return (
            str(row["generation_key"]),
            str(row["bundle_key"]),
            str(row["bundle_digest"]),
            str(row["quote_generation_key"]),
        )

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
        structure_generation_key = f"structure:{batches[0].structure_receipt_digest}"
        cursor.execute(
            """
            INSERT INTO m1_quote_generation_inputs (
                generation_key, structure_generation_key, universe_hash,
                cadence_seconds, cadence_bucket, admitted_at
            ) VALUES (%s, %s, %s, NULL, NULL, %s)
            ON CONFLICT (generation_key) DO NOTHING
            """,
            (generation_key, structure_generation_key, batches[0].universe_hash, now),
        )
        cursor.execute(
            """
            SELECT structure_generation_key, universe_hash
            FROM m1_quote_generation_inputs WHERE generation_key = %s
            """,
            (generation_key,),
        )
        lineage = cursor.fetchone()
        if lineage is None or (
            str(lineage["structure_generation_key"]) != structure_generation_key
            or str(lineage["universe_hash"]) != batches[0].universe_hash
        ):
            raise JobIdentityConflict("Quote generation lineage conflicts")
        cursor.execute(
            "SELECT generation_key FROM m1_publication_pointers "
            "WHERE pointer_key = 'quote:current' FOR UPDATE"
        )
        pointer = cursor.fetchone()
        expected_generation_key = None if pointer is None else str(pointer["generation_key"])
        self._enqueue_job_cursor(
            cursor,
            job_key=f"{generation_key}:certify",
            job_type="quote-certify",
            input_identity=_frozen_quote_certification_identity(
                cursor,
                generation_key=generation_key,
                universe_hash=batches[0].universe_hash,
                expected_generation_key=expected_generation_key,
            ),
            now=now,
            initial_state=JobState.WAITING,
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
                initial_state=JobState.WAITING,
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
            initial_state=JobState.WAITING,
        )
        return specs

    def structure_range_spec(self, job_key: str) -> StructureRangeSpec:
        """Load a frozen Structure range for a replacement worker."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=True)
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
            quote_generation_digest=job_key.split(":", maxsplit=2)[1],
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
            _set_structure_read_timeouts(cursor, read_only=True)
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
            _set_structure_read_timeouts(cursor, read_only=True)
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
            _set_structure_read_timeouts(cursor, read_only=True)
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
        initial_state: JobState = JobState.RUNNABLE,
    ) -> None:
        if initial_state not in {JobState.RUNNABLE, JobState.WAITING}:
            raise ValueError("new jobs must start runnable or waiting")
        cursor.execute(
            """
            INSERT INTO m1_jobs (
                job_key, job_type, input_identity, state, next_attempt_at,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_key) DO NOTHING
            """,
            (
                job_key,
                job_type,
                input_identity,
                initial_state.value,
                now if initial_state is JobState.RUNNABLE else None,
                now,
                now,
            ),
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

    @staticmethod
    def _wake_structure_certifier_cursor(
        cursor: psycopg.Cursor[dict[str, Any]], *, generation_key: str, now: datetime
    ) -> None:
        certifier_job_key = f"{generation_key}:certify"
        if not PostgresControlPlane._try_lock_certifier_job_cursor(
            cursor,
            certifier_job_key=certifier_job_key,
            unavailable_message="Structure certifier barrier job is unavailable",
        ):
            return
        cursor.execute(
            """
            UPDATE m1_jobs SET state = 'runnable', next_attempt_at = %s,
                last_error_class = NULL, updated_at = %s
            WHERE job_key = %s AND state = 'waiting'
              AND (SELECT count(*) FROM m1_structure_range_receipts AS receipt
                   JOIN m1_structure_range_inputs AS input
                     ON input.job_key = receipt.job_key
                   JOIN m1_jobs AS sibling ON sibling.job_key = input.job_key
                   WHERE input.generation_key = %s
                     AND sibling.state = 'succeeded')
                = (SELECT count(*) FROM m1_structure_range_inputs
                   WHERE generation_key = %s)
            """,
            (now, now, certifier_job_key, generation_key, generation_key),
        )

    @staticmethod
    def _wake_quote_certifier_cursor(
        cursor: psycopg.Cursor[dict[str, Any]], *, generation_key: str, now: datetime
    ) -> None:
        certifier_job_key = f"{generation_key}:certify"
        if not PostgresControlPlane._try_lock_certifier_job_cursor(
            cursor,
            certifier_job_key=certifier_job_key,
            unavailable_message="Quote certifier barrier job is unavailable",
        ):
            return

        cursor.execute(
            """
            UPDATE m1_jobs SET state = 'runnable', next_attempt_at = %s,
                last_error_class = NULL, updated_at = %s
            WHERE job_key = %s AND state = 'waiting'
              AND (SELECT count(*) FROM m1_quote_batch_receipts AS receipt
                   JOIN m1_quote_batch_inputs AS input
                     ON input.job_key = receipt.job_key
                   JOIN m1_jobs AS sibling ON sibling.job_key = input.job_key
                   WHERE input.job_key LIKE %s
                     AND sibling.state = 'succeeded')
                = (SELECT count(*) FROM m1_quote_batch_inputs
                   WHERE job_key LIKE %s)
            """,
            (
                now,
                now,
                certifier_job_key,
                f"{generation_key}:batch:%",
                f"{generation_key}:batch:%",
            ),
        )

    @staticmethod
    def _try_lock_certifier_job_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        certifier_job_key: str,
        unavailable_message: str,
    ) -> bool:
        """Acquire a successor row only when doing so cannot block a producer.

        Terminal producers must commit their own durable success independently
        of sibling fan-in.  A busy successor therefore means "skip direct wake",
        not a producer failure.  Each certifier turn runs
        ``repair_ready_certifiers`` before claiming work, so committed receipts
        remain the authoritative lost-wakeup recovery path.
        """
        cursor.execute(
            "SELECT job_key FROM m1_jobs WHERE job_key = %s",
            (certifier_job_key,),
        )
        if cursor.fetchone() is None:
            raise ControlPlaneError(unavailable_message)
        cursor.execute(
            "SELECT job_key FROM m1_jobs WHERE job_key = %s FOR UPDATE SKIP LOCKED",
            (certifier_job_key,),
        )
        return cursor.fetchone() is not None

    @staticmethod
    def _wake_terminal_successor_cursor(
        cursor: psycopg.Cursor[dict[str, Any]], *, lease: JobLease, now: datetime
    ) -> None:
        """Wake a receipt-gated successor only after its producer is terminal."""
        if lease.job_type == "quote-batch":
            quote_generation_digest, _structure, _universe, _ordinal, _range_digest = (
                PostgresControlPlane._quote_batch_identity(lease.input_identity)
            )
            PostgresControlPlane._wake_quote_certifier_cursor(
                cursor,
                generation_key=f"quote:{quote_generation_digest}",
                now=now,
            )
            return
        if lease.job_type != "structure-normalize":
            return
        cursor.execute(
            "SELECT generation_key FROM m1_structure_range_inputs WHERE job_key = %s",
            (lease.job_key,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError("Structure range input is unavailable at terminal wakeup")
        PostgresControlPlane._wake_structure_certifier_cursor(
            cursor,
            generation_key=str(row["generation_key"]),
            now=now,
        )

    def repair_ready_certifiers(self, *, job_type: str, now: datetime) -> int:
        """Repair waiting certifiers whose durable producer barrier is complete.

        Normal producer completion performs the same transition.  This bounded
        sweep is a crash/lost-wakeup recovery path, so a historical waiting row
        cannot require an operator SQL mutation to resume.
        """
        if job_type not in {"structure-certify", "quote-certify"}:
            raise ValueError("repair supports Structure or Quote certifiers only")
        self._validate_aware(now, "now")
        if job_type == "structure-certify":
            ready_predicate = """
                EXISTS (
                    SELECT 1 FROM m1_structure_range_inputs AS input
                    WHERE input.generation_key = certifier.input_identity
                )
                AND NOT EXISTS (
                    SELECT 1 FROM m1_structure_range_inputs AS input
                    LEFT JOIN m1_structure_range_receipts AS receipt
                      ON receipt.job_key = input.job_key
                    LEFT JOIN m1_jobs AS sibling ON sibling.job_key = input.job_key
                    WHERE input.generation_key = certifier.input_identity
                      AND (receipt.job_key IS NULL
                           OR sibling.state IS DISTINCT FROM 'succeeded')
                )
            """
        else:
            ready_predicate = """
                EXISTS (
                    SELECT 1 FROM m1_quote_batch_inputs AS input
                    WHERE input.job_key LIKE
                          regexp_replace(certifier.job_key, ':certify$', ':batch:%%')
                )
                AND NOT EXISTS (
                    SELECT 1 FROM m1_quote_batch_inputs AS input
                    LEFT JOIN m1_quote_batch_receipts AS receipt
                      ON receipt.job_key = input.job_key
                    LEFT JOIN m1_jobs AS sibling ON sibling.job_key = input.job_key
                    WHERE input.job_key LIKE
                          regexp_replace(certifier.job_key, ':certify$', ':batch:%%')
                      AND (receipt.job_key IS NULL
                           OR sibling.state IS DISTINCT FROM 'succeeded')
                )
            """
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=False)
            cursor.execute(
                f"""
                WITH ready_certifier AS (
                    SELECT certifier.job_key
                    FROM m1_jobs AS certifier
                    WHERE certifier.job_type = %s AND certifier.state = 'waiting'
                      AND {ready_predicate}
                    ORDER BY certifier.created_at, certifier.job_key
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE m1_jobs AS certifier
                SET state = 'runnable', next_attempt_at = %s,
                    last_error_class = NULL, updated_at = %s
                FROM ready_certifier
                WHERE certifier.job_key = ready_certifier.job_key
                """,
                (job_type, now, now),
            )
            return cursor.rowcount

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
        terminal: bool = False,
        research_rows: Sequence[tuple[str, Mapping[str, object]]] | None = None,
    ) -> CheckpointReceipt:
        """Commit one bounded Quote range under its current worker fence.

        The historical API keeps the receipt checkpointed until the caller
        explicitly finishes it.  Transactional workers pass ``terminal=True``
        so the receipt, runtime success fact, job transition, and attempt
        transition share one database transaction.
        """
        self._validate_aware(quoted_at, "quoted_at")
        self._validate_aware(now, "now")
        if type(terminal) is not bool:
            raise TypeError("terminal must be a bool")
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
        _quote_generation, structure_digest, universe_hash, ordinal, expected_range_digest = (
            self._quote_batch_identity(lease.input_identity)
        )
        if token_range_digest != expected_range_digest:
            raise CheckpointConflictError("quote batch range does not match its job identity")
        idempotency_key = f"quote-batch:{lease.job_key}:{quote_digest}"
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=False)
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
                receipt = CheckpointReceipt(
                    receipt_id=str(existing["receipt_id"]),
                    job_key=lease.job_key,
                    lease_epoch=int(existing["lease_epoch"]),
                    idempotency_key=idempotency_key,
                    checkpoint_cursor=ordinal,
                    checkpoint_digest=quote_digest,
                    committed_at=existing["committed_at"],
                )
                if terminal:
                    self._finish_quote_batch_terminal_cursor(
                        cursor,
                        lease=lease,
                        checkpoint_cursor=ordinal,
                        checkpoint_digest=quote_digest,
                        now=now,
                        allow_historical=True,
                    )
                return receipt
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
            for token_id, payload in research_rows or ():
                self._validate_nonempty(token_id=token_id)
                cursor.execute(
                    """INSERT INTO m1_business_quote_staging_rows(generation_key, token_id, payload)
                       VALUES (%s, %s, %s) ON CONFLICT (generation_key, token_id) DO NOTHING""",
                    (f"quote:{_quote_generation}", token_id, Jsonb(dict(payload))),
                )
            cursor.execute(
                """
                UPDATE m1_job_attempts SET state = 'checkpointed', finished_at = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (now, lease.job_key, lease.lease_epoch),
            )
            if terminal:
                self._finish_quote_batch_terminal_cursor(
                    cursor,
                    lease=lease,
                    checkpoint_cursor=ordinal,
                    checkpoint_digest=quote_digest,
                    now=now,
                )
            return receipt

    def recover_quote_batch_success(self, lease: JobLease, *, now: datetime) -> QuoteBatchReceipt:
        """Repair a receipt-backed Quote success fact exactly once.

        A prior worker may have committed the immutable receipt and then died
        before the runtime success event was introduced.  This method accepts
        only that narrow durable proof and never refetches CLOB/R2 input.
        """
        self._validate_aware(now, "now")
        if lease.job_type != "quote-batch":
            raise ValueError("Quote batch recovery requires a quote-batch lease")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=False)
            cursor.execute(
                """
                SELECT job_key, structure_receipt_digest, universe_hash,
                       token_range_digest, quote_digest, artifact_key, artifact_digest,
                       successful_response_count, quoted_at
                FROM m1_quote_batch_receipts WHERE job_key = %s
                """,
                (lease.job_key,),
            )
            row = cursor.fetchone()
            if row is None:
                raise ControlPlaneError(f"quote batch receipt is unavailable for {lease.job_key!r}")
            receipt = QuoteBatchReceipt(
                job_key=str(row["job_key"]),
                quote_digest=str(row["quote_digest"]),
                artifact_key=str(row["artifact_key"]),
                artifact_digest=str(row["artifact_digest"]),
                successful_response_count=int(row["successful_response_count"]),
            )
            self._finish_quote_batch_terminal_cursor(
                cursor,
                lease=lease,
                checkpoint_cursor=self._quote_batch_identity(lease.input_identity)[3],
                checkpoint_digest=receipt.quote_digest,
                now=now,
                allow_historical=True,
            )
            return receipt

    @staticmethod
    def _finish_quote_batch_terminal_cursor(
        cursor: psycopg.Cursor[dict[str, Any]],
        *,
        lease: JobLease,
        checkpoint_cursor: str,
        checkpoint_digest: str,
        now: datetime,
        allow_historical: bool = False,
    ) -> None:
        """Seal a Quote batch and its runtime success under one cursor."""
        quote_generation_digest, _structure, _universe_hash, _ordinal, _range_digest = (
            PostgresControlPlane._quote_batch_identity(lease.input_identity)
        )
        generation_key = f"quote:{quote_generation_digest}"
        cursor.execute(
            """
            SELECT state, lease_owner, lease_epoch, checkpoint_cursor, checkpoint_digest
            FROM m1_jobs WHERE job_key = %s FOR UPDATE
            """,
            (lease.job_key,),
        )
        job = cursor.fetchone()
        if job is None:
            raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
        state = str(job["state"])
        if state == JobState.SUCCEEDED.value:
            if (
                int(job["lease_epoch"]) != lease.lease_epoch
                or str(job["checkpoint_cursor"]) != checkpoint_cursor
                or str(job["checkpoint_digest"]) != checkpoint_digest
            ):
                raise CheckpointConflictError(
                    f"succeeded Quote batch has conflicting durable checkpoint: {lease.job_key}"
                )
            if not allow_historical and str(job["lease_owner"] or "") not in {
                "",
                lease.lease_owner,
            }:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            PostgresControlPlane._append_historical_job_succeeded_cursor(
                cursor,
                lease=lease,
                stage="commit-receipt",
                component="quote-batch",
                data_product="market-snapshot",
                now=now,
            )
            PostgresControlPlane._wake_quote_certifier_cursor(
                cursor,
                generation_key=generation_key,
                now=now,
            )
            return
        if (
            state not in {JobState.LEASED.value, JobState.CHECKPOINTED.value}
            or str(job["lease_owner"]) != lease.lease_owner
            or int(job["lease_epoch"]) != lease.lease_epoch
        ):
            raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
        _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
        PostgresControlPlane._append_job_succeeded_cursor(
            cursor,
            lease=lease,
            stage="commit-receipt",
            component="quote-batch",
            data_product="market-snapshot",
            now=now,
        )
        cursor.execute(
            """
            UPDATE m1_jobs
            SET state = 'succeeded', checkpoint_cursor = %s, checkpoint_digest = %s,
                lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
            WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
              AND state IN ('leased', 'checkpointed')
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
        cursor.execute(
            """
            UPDATE m1_job_attempts SET state = 'succeeded', finished_at = %s
            WHERE job_key = %s AND lease_epoch = %s AND state IN ('running', 'checkpointed')
            """,
            (now, lease.job_key, lease.lease_epoch),
        )
        PostgresControlPlane._wake_quote_certifier_cursor(
            cursor,
            generation_key=generation_key,
            now=now,
        )

    def record_structure_range(
        self,
        lease: JobLease,
        *,
        range_digest: str,
        artifact_key: str,
        artifact_digest: str,
        record_count: int,
        now: datetime,
        research_rows: Sequence[tuple[str, Mapping[str, object]]] | None = None,
    ) -> CheckpointReceipt:
        """Atomically checkpoint one normalized Structure range under its lease fence."""
        return self._record_structure_range(
            lease,
            range_digest=range_digest,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            record_count=record_count,
            research_rows=research_rows,
            now=now,
            terminal=False,
        )

    def complete_structure_range(
        self,
        lease: JobLease,
        *,
        range_digest: str,
        artifact_key: str,
        artifact_digest: str,
        record_count: int,
        now: datetime,
        research_rows: Sequence[tuple[str, Mapping[str, object]]] | None = None,
    ) -> CheckpointReceipt:
        """Commit one normalized range and its terminal runtime success atomically."""
        return self._record_structure_range(
            lease,
            range_digest=range_digest,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            record_count=record_count,
            research_rows=research_rows,
            now=now,
            terminal=True,
        )

    def _record_structure_range(
        self,
        lease: JobLease,
        *,
        range_digest: str,
        artifact_key: str,
        artifact_digest: str,
        record_count: int,
        research_rows: Sequence[tuple[str, Mapping[str, object]]] | None,
        now: datetime,
        terminal: bool,
    ) -> CheckpointReceipt:
        """Persist a range receipt, optionally sealing the leased attempt."""
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
        if research_rows is not None and len(research_rows) > record_count:
            raise ValueError("structure research rows cannot exceed the range record count")

        spec = self.structure_range_spec(lease.job_key)
        if range_digest != spec.range_digest:
            raise CheckpointConflictError("Structure range does not match its job identity")
        checkpoint_cursor = f"{spec.component}:{spec.ordinal}"
        idempotency_key = f"structure-range:{lease.job_key}:{artifact_digest}"
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=False)
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
                if terminal:
                    self._recover_structure_terminal_success_cursor(
                        cursor,
                        lease=lease,
                        stage="commit-range",
                        component="structure-normalize",
                        data_product="structure-sync",
                        checkpoint_cursor=checkpoint_cursor,
                        checkpoint_digest=artifact_digest,
                        now=now,
                    )
                    self._wake_structure_certifier_cursor(
                        cursor,
                        generation_key=spec.generation_key,
                        now=now,
                    )
                return CheckpointReceipt(
                    receipt_id=str(existing["receipt_id"]),
                    job_key=lease.job_key,
                    lease_epoch=int(existing["lease_epoch"]),
                    idempotency_key=idempotency_key,
                    checkpoint_cursor=checkpoint_cursor,
                    checkpoint_digest=artifact_digest,
                    committed_at=existing["committed_at"],
                )
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            if terminal:
                self._append_job_succeeded_cursor(
                    cursor,
                    lease=lease,
                    stage="commit-range",
                    component="structure-normalize",
                    data_product="structure-sync",
                    now=now,
                )
                cursor.execute(
                    """
                    UPDATE m1_jobs
                    SET checkpoint_cursor = %s, checkpoint_digest = %s,
                        state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
                        updated_at = %s
                    WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                      AND state = 'leased'
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
            else:
                cursor.execute(
                    """
                    UPDATE m1_jobs
                    SET checkpoint_cursor = %s, checkpoint_digest = %s, updated_at = %s
                    WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                      AND state = 'leased'
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
            for entity_id, payload in research_rows or ():
                self._validate_nonempty(entity_id=entity_id)
                cursor.execute(
                    """INSERT INTO m1_business_structure_rows(generation_key, entity_id, payload)
                       VALUES (%s, %s, %s) ON CONFLICT (generation_key, entity_id) DO NOTHING""",
                    (spec.generation_key, entity_id, Jsonb(dict(payload))),
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
                UPDATE m1_job_attempts SET state = %s, finished_at = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (
                    "succeeded" if terminal else "checkpointed",
                    now,
                    lease.job_key,
                    lease.lease_epoch,
                ),
            )
            if terminal:
                self._wake_structure_certifier_cursor(
                    cursor,
                    generation_key=spec.generation_key,
                    now=now,
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
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("m1:quote-generation-admission",),
            )
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
            if generation is None:
                raise IncompleteStructureGenerationError(
                    "Structure generation is missing its frozen input"
                )
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
                raise StructureParityMismatchError(
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
            # Validate the entire candidate before applying the serialization
            # gate.  A corrupt candidate must be quarantined immediately even
            # while the prior Structure's executable successor is still live;
            # only a valid publication is allowed to wait behind that chain.
            cursor.execute(
                """
                SELECT 1 FROM m1_jobs
                WHERE job_type IN (
                    'quote-admit', 'quote-batch', 'quote-certify', 'opportunity-certify'
                )
                  AND state IN (
                    'waiting', 'runnable', 'retryable', 'leased', 'checkpointed'
                  )
                LIMIT 1
                """
            )
            if cursor.fetchone() is not None:
                raise StructureSuccessorBusyError(
                    "Structure certification is waiting for the current executable successor"
                )
            record_count = sum(int(row["record_count"]) for row in ordered)
            self._append_job_succeeded_cursor(
                cursor,
                lease=lease,
                stage="commit-certification",
                component="structure-certify",
                data_product="structure-sync",
                now=now,
            )
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
            self._prune_superseded_business_research_rows_cursor(
                cursor,
                product="structure",
                current_generation_key=generation_key,
            )
            quote_generation_key = f"quote:{bundle_digest}"
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
                    job_key, generation_key, bundle_key, bundle_digest,
                    quote_generation_key, admitted_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_key) DO NOTHING
                """,
                (
                    quote_admit_job_key,
                    generation_key,
                    bundle_key,
                    bundle_digest,
                    quote_generation_key,
                    now,
                ),
            )
            cursor.execute(
                """
                SELECT generation_key, bundle_key, bundle_digest, quote_generation_key
                FROM m1_quote_admission_inputs WHERE job_key = %s
                """,
                (quote_admit_job_key,),
            )
            quote_admission = cursor.fetchone()
            if quote_admission is None or (
                str(quote_admission["generation_key"]) != generation_key
                or str(quote_admission["bundle_key"]) != bundle_key
                or str(quote_admission["bundle_digest"]) != bundle_digest
                or str(quote_admission["quote_generation_key"]) != quote_generation_key
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
        (
            lease_generation_key,
            universe_hash,
            expected_generation_key,
            has_lineage_fence,
        ) = _parse_quote_certification_identity(lease.input_identity)
        if lease_generation_key != generation_key or not universe_hash:
            raise JobIdentityConflict("quote certifier identity does not match its generation")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            cursor.execute(
                """
                SELECT structure_generation_key, universe_hash
                FROM m1_quote_generation_inputs
                WHERE generation_key = %s
                """,
                (generation_key,),
            )
            lineage = cursor.fetchone()
            if lineage is None:
                raise JobIdentityConflict("quote generation has no authoritative Structure lineage")
            structure_generation_key = str(lineage["structure_generation_key"])
            if not structure_generation_key.startswith("structure:"):
                raise JobIdentityConflict("quote generation has malformed Structure lineage")
            structure_digest = structure_generation_key.removeprefix("structure:")
            if len(structure_digest) != 64 or str(lineage["universe_hash"]) != universe_hash:
                raise JobIdentityConflict("quote generation lineage does not match its certifier")
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
                (
                    batch_generation,
                    batch_structure,
                    batch_universe,
                    _ordinal,
                    range_digest,
                ) = self._quote_batch_identity(
                    str(job["input_identity"])
                )
                if (
                    f"quote:{batch_generation}" != generation_key
                    or batch_structure != structure_digest
                    or batch_universe != universe_hash
                    or receipt["structure_receipt_digest"] != structure_digest
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
            record_count = sum(
                int(str(row["successful_response_count"])) for row in ordered_receipts
            )
            self._append_job_succeeded_cursor(
                cursor,
                lease=lease,
                stage="publish-pointer",
                component="quote-certify",
                data_product="market-snapshot",
                now=now,
            )
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
                if has_lineage_fence and expected_generation_key is not None:
                    raise PublicationPointerConflictError(
                        "Quote publication predecessor is no longer current"
                    )
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
                current_generation_key = str(current["generation_key"])
                if not has_lineage_fence or current_generation_key != expected_generation_key:
                    raise PublicationPointerConflictError(
                        "Quote publication predecessor is no longer current"
                    )
                cursor.execute(
                    """
                    UPDATE m1_publication_pointers
                    SET generation_key = %s, expected_generation_key = %s,
                        lease_epoch = %s, published_at = %s
                    WHERE pointer_key = 'quote:current' AND generation_key = %s
                    """,
                    (
                        generation_key,
                        current_generation_key,
                        lease.lease_epoch,
                        now,
                        current_generation_key,
                    ),
                )
                if cursor.rowcount != 1:
                    raise StaleLeaseError("Quote pointer changed during certification")
            # A complete quote candidate may contain tens of thousands of
            # dashboard rows.  Its one safe retirement point is after the
            # pointer fence is held, but the ordinary request timeout is too
            # short for that bounded delete.  Never extend beyond the lease.
            remaining_prune_ms = int(
                (lease.lease_expires_at - datetime.now(UTC)).total_seconds() * 1000
            ) - 1
            if remaining_prune_ms <= 0:
                raise StaleLeaseError("Quote publication lease expired before index retirement")
            cursor.execute(
                sql.SQL("SET LOCAL statement_timeout = {}").format(
                    sql.Literal(f"{min(110_000, remaining_prune_ms)}ms")
                )
            )
            self._prune_superseded_business_research_rows_cursor(
                cursor,
                product="quote",
                current_generation_key=generation_key,
            )
            self._promote_staged_business_quote_rows_cursor(
                cursor, generation_key=generation_key
            )
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

    def recover_quote_certification_success(
        self, lease: JobLease, *, generation_key: str, now: datetime
    ) -> str:
        """Repair a proven Quote pointer publication missing its success fact."""
        self._validate_aware(now, "now")
        if lease.job_type != "quote-certify" or lease.job_key != f"{generation_key}:certify":
            raise ValueError("Quote certification recovery requires its matching lease")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=False)
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
                or str(job["state"]) != JobState.SUCCEEDED.value
                or int(job["lease_epoch"]) != lease.lease_epoch
            ):
                raise StaleLeaseError(
                    f"durable Quote certification is not proven for {lease.job_key}"
                )
            cursor.execute(
                """
                SELECT artifact_digest FROM m1_generation_manifests
                WHERE generation_key = %s AND producer_job_key = %s
                """,
                (generation_key, lease.job_key),
            )
            manifest = cursor.fetchone()
            cursor.execute(
                """
                SELECT generation_key FROM m1_publication_pointers
                WHERE pointer_key = 'quote:current'
                """,
            )
            pointer = cursor.fetchone()
            if (
                manifest is None
                or pointer is None
                or str(pointer["generation_key"]) != generation_key
            ):
                raise IncompleteQuoteGenerationError(
                    "Quote certification recovery lacks durable pointer proof"
                )
            self._append_historical_job_succeeded_cursor(
                cursor,
                lease=lease,
                stage="publish-pointer",
                component="quote-certify",
                data_product="market-snapshot",
                now=now,
            )
            return str(manifest["artifact_digest"])

    def publish_structure_shadow(
        self,
        *,
        generation_key: str,
        now: datetime,
        expected_generation_key: str | None | object = _POINTER_LINEAGE_UNSET,
    ) -> str:
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
                if expected_generation_key not in {_POINTER_LINEAGE_UNSET, None}:
                    raise PublicationPointerConflictError(
                        "Structure shadow predecessor is no longer current"
                    )
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
                current_generation_key = str(current["generation_key"])
                if (
                    expected_generation_key is _POINTER_LINEAGE_UNSET
                    or expected_generation_key != current_generation_key
                ):
                    raise PublicationPointerConflictError(
                        "Structure shadow predecessor is no longer current"
                    )
                cursor.execute(
                    """
                    UPDATE m1_publication_pointers
                    SET generation_key = %s, expected_generation_key = %s,
                        lease_epoch = lease_epoch + 1, published_at = %s
                    WHERE pointer_key = 'structure:current:shadow' AND generation_key = %s
                    """,
                    (generation_key, current_generation_key, now, current_generation_key),
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
    def _quote_batch_identity(input_identity: str) -> tuple[str, str, str, str, str]:
        parts = input_identity.split(":")
        if parts[0] != "quote" or not all(parts[1:]):
            raise JobIdentityConflict("quote batch job has malformed input identity")
        if len(parts) == 5:
            return parts[1], parts[1], parts[2], parts[3], parts[4]
        if len(parts) == 6:
            return parts[1], parts[2], parts[3], parts[4], parts[5]
        raise JobIdentityConflict("quote batch job has malformed input identity")

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
            # A worker identity is a capacity lane, not merely an audit label.
            # Serialize claims for that identity and refuse a second live lease
            # when a prior terminal write could not close its first one.  Lease
            # expiry remains the durable recovery authority; opening more work
            # here would turn a database outage into an unbounded retry storm.
            cursor.execute(
                """
                WITH worker_lock AS (
                    SELECT pg_try_advisory_xact_lock(
                        hashtextextended(%s, 0)
                    ) AS acquired
                )
                SELECT worker_lock.acquired, active.job_key
                FROM worker_lock
                LEFT JOIN LATERAL (
                    SELECT job_key
                    FROM m1_jobs
                    WHERE lease_owner = %s
                      AND state = 'leased'
                      AND (lease_expires_at IS NULL OR lease_expires_at > %s)
                    FOR UPDATE
                    LIMIT 1
                ) AS active ON worker_lock.acquired
                """,
                (worker_id, worker_id, now),
            )
            worker_guard = cursor.fetchone()
            if (
                worker_guard is None
                or not bool(worker_guard["acquired"])
                or worker_guard["job_key"] is not None
            ):
                return None
            materializer_job_key: str | None = None
            if "structure-materialize" in job_types:
                # A source window can remain durably queued for materialization
                # long after its admission turn.  Select only the oldest such
                # shape, and do not let it overtake any prior range/certifier
                # generation.  The later FOR UPDATE SKIP LOCKED makes this
                # fixed observation safe across replacement coordinators.
                cursor.execute(
                    """
                    SELECT (
                        SELECT materializer.job_key
                        FROM m1_jobs AS materializer
                        WHERE materializer.job_type = 'structure-materialize'
                          AND materializer.state IN (
                            'waiting', 'runnable', 'retryable', 'leased', 'checkpointed'
                          )
                        ORDER BY materializer.created_at, materializer.job_key
                        LIMIT 1
                    ) AS oldest_job_key,
                    NOT EXISTS (
                        SELECT 1 FROM m1_jobs AS predecessor
                        WHERE predecessor.job_type IN (
                            'structure-normalize', 'structure-certify'
                        )
                          AND predecessor.state IN (
                            'waiting', 'runnable', 'retryable', 'leased', 'checkpointed'
                          )
                    ) AS predecessors_complete
                    """
                )
                materializer_guard = cursor.fetchone()
                if materializer_guard is None:
                    raise ControlPlaneError("Structure materializer guard was not returned")
                if bool(materializer_guard["predecessors_complete"]):
                    oldest_job_key = materializer_guard["oldest_job_key"]
                    if oldest_job_key is not None:
                        materializer_job_key = str(oldest_job_key)
            cursor.execute(
                """
                SELECT job_key, job_type, input_identity, checkpoint_cursor,
                       checkpoint_digest, lease_epoch, state, attempt_count
                FROM m1_jobs
                WHERE job_type = ANY(%s)
                  AND (job_type <> 'structure-materialize' OR job_key = %s)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM m1_job_circuits AS circuit
                      WHERE circuit.job_key = m1_jobs.job_key
                        AND circuit.state = 'open'
                        AND circuit.next_probe_at <= %s
                  )
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
                (list(job_types), materializer_job_key, now, now, now, now),
            )
            job = cursor.fetchone()
            if job is None:
                return None
            if str(job["state"]) == "leased":
                cursor.execute(
                    """
                    SELECT runtime.attempt_id, runtime.worker_id, runtime.stage,
                           runtime.progress_sequence, runtime.progress_current,
                           runtime.progress_total, attempt.state AS attempt_state
                    FROM m1_job_runtime_state AS runtime
                    JOIN m1_job_attempts AS attempt
                      ON attempt.attempt_id = runtime.attempt_id
                    WHERE runtime.job_key = %s AND runtime.lease_epoch = %s
                    FOR UPDATE
                    """,
                    (job["job_key"], job["lease_epoch"]),
                )
                expired_runtime = cursor.fetchone()
                if expired_runtime is None:
                    raise ControlPlaneError(
                        f"expired lease has no runtime attempt: {job['job_key']}"
                    )
                attempt_state = str(expired_runtime["attempt_state"])
                if attempt_state not in {"running", "checkpointed"}:
                    raise ControlPlaneError(
                        f"expired lease attempt cannot be reclaimed: {job['job_key']}"
                    )
                if attempt_state == "running":
                    cursor.execute(
                        """
                        UPDATE m1_job_attempts
                        SET state = 'retryable', finished_at = %s,
                            error_class = 'LeaseExpired',
                            error_detail = %s
                        WHERE attempt_id = %s AND job_key = %s
                          AND lease_epoch = %s AND state = 'running'
                        """,
                        (
                            now,
                            Jsonb({"reason_code": "job.lease-expired"}),
                            expired_runtime["attempt_id"],
                            job["job_key"],
                            job["lease_epoch"],
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise ControlPlaneError(
                            f"expired running attempt changed during reclaim: {job['job_key']}"
                        )
                cursor.execute(
                    """
                    SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
                    FROM m1_job_runtime_events
                    WHERE attempt_id = %s
                    """,
                    (expired_runtime["attempt_id"],),
                )
                sequence_row = cursor.fetchone()
                if sequence_row is None:
                    raise ControlPlaneError("runtime event sequence query returned no row")
                progress_sequence = int(expired_runtime["progress_sequence"])
                progress = (
                    None
                    if progress_sequence == 0
                    else RuntimeProgress(
                        sequence=progress_sequence,
                        current=int(expired_runtime["progress_current"]),
                        total=(
                            None
                            if expired_runtime["progress_total"] is None
                            else int(expired_runtime["progress_total"])
                        ),
                        stage=str(expired_runtime["stage"]),
                    )
                )
                expired_event = RuntimeEvent(
                    job_key=str(job["job_key"]),
                    attempt_id=str(expired_runtime["attempt_id"]),
                    lease_epoch=int(job["lease_epoch"]),
                    worker_id=str(expired_runtime["worker_id"]),
                    event_sequence=int(sequence_row["next_sequence"]),
                    kind=RuntimeEventKind.RETRYABLE_FAILED,
                    stage=str(expired_runtime["stage"]),
                    progress=progress,
                    detail={
                        "component": str(job["job_type"]),
                        "failure_signature": "upstream.timeout",
                        "qualification_impact": "delayed",
                        "reason_code": "job.lease-expired",
                        "recovery_policy": "reclaim-job",
                        "retry_count": int(job["attempt_count"]),
                    },
                    occurred_at=now,
                    idempotency_key=(f"runtime:{expired_runtime['attempt_id']}:lease-expired"),
                )
                cursor.execute(
                    """
                    INSERT INTO m1_job_runtime_events (
                        event_id, job_key, attempt_id, lease_epoch, worker_id,
                        event_sequence, kind, stage, progress_sequence,
                        progress_current, progress_total, detail, occurred_at,
                        idempotency_key
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(uuid4()),
                        expired_event.job_key,
                        expired_event.attempt_id,
                        expired_event.lease_epoch,
                        expired_event.worker_id,
                        expired_event.event_sequence,
                        expired_event.kind.value,
                        expired_event.stage,
                        None if progress is None else progress.sequence,
                        None if progress is None else progress.current,
                        None if progress is None else progress.total,
                        Jsonb(dict(expired_event.detail)),
                        expired_event.occurred_at,
                        expired_event.idempotency_key,
                    ),
                )
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
            attempt_id = str(uuid4())
            cursor.execute(
                """
                INSERT INTO m1_job_attempts (
                    attempt_id, job_key, lease_epoch, worker_id, state, started_at
                ) VALUES (%s, %s, %s, %s, 'running', %s)
                """,
                (attempt_id, job["job_key"], epoch, worker_id, now),
            )
            start_runtime_attempt_cursor(
                cursor,
                job_key=str(job["job_key"]),
                job_type=str(job["job_type"]),
                attempt_id=attempt_id,
                lease_epoch=epoch,
                worker_id=worker_id,
                started_at=now,
                lease_deadline_at=expires_at,
                lease_seconds=lease_seconds,
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
        """Renew a lease and its runtime liveness projection atomically."""
        return self.heartbeat_runtime_attempt(lease, now=now, lease_seconds=lease_seconds)

    def heartbeat_runtime_attempt(
        self, lease: JobLease, *, now: datetime, lease_seconds: int = 30
    ) -> JobLease:
        """Renew job/runtime state under the current attempt fence."""
        self._validate_aware(now, "now")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        try:
            with (
                self._connection_factory() as connection,
                connection.cursor(row_factory=dict_row) as cursor,
            ):
                _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
                cursor.execute(
                    """
                    SELECT attempt_id FROM m1_job_runtime_state
                    WHERE job_key = %s AND lease_epoch = %s AND worker_id = %s
                    """,
                    (lease.job_key, lease.lease_epoch, lease.lease_owner),
                )
                runtime_state = cursor.fetchone()
                if runtime_state is None:
                    raise StaleLeaseError(
                        f"runtime attempt is no longer current for {lease.job_key}"
                    )
                try:
                    heartbeat = update_runtime_heartbeat_cursor(
                        cursor,
                        job_key=lease.job_key,
                        attempt_id=str(runtime_state["attempt_id"]),
                        lease_epoch=lease.lease_epoch,
                        worker_id=lease.lease_owner,
                        now=now,
                        lease_seconds=lease_seconds,
                    )
                except RuntimeFenceError as error:
                    raise StaleLeaseError(str(error)) from error
        except psycopg.OperationalError as error:
            raise RetryableHeartbeatError("heartbeat database unavailable") from error
        expires_at = heartbeat["lease_deadline_at"]
        return JobLease(
            job_key=lease.job_key,
            job_type=lease.job_type,
            input_identity=lease.input_identity,
            lease_owner=lease.lease_owner,
            lease_epoch=lease.lease_epoch,
            lease_expires_at=expires_at,  # type: ignore[arg-type]
            checkpoint_cursor=lease.checkpoint_cursor,
            checkpoint_digest=lease.checkpoint_digest,
        )

    # ------------------------------------------------------------------
    # Plan 03 job-level recovery actions

    @staticmethod
    def _recovery_action(action: RecoveryActionRecord, expected: str) -> None:
        if type(action) is not RecoveryActionRecord:
            raise TypeError("recovery action must be RecoveryActionRecord")
        if action.action_type != expected:
            raise ValueError(f"recovery action is not {expected}")

    @staticmethod
    def _recovery_component(action: RecoveryActionRecord) -> str:
        component = action.detail.get("component")
        return component if isinstance(component, str) and component.strip() else "control-plane"

    @staticmethod
    def _recovery_channels(action: RecoveryActionRecord) -> tuple[str, ...]:
        """Decode the canonical channel list persisted by recovery_store."""
        encoded = action.detail.get("channels")
        if not isinstance(encoded, str):
            return ("dashboard",)
        try:
            parsed = json.loads(encoded)
        except (TypeError, ValueError):
            return ("dashboard",)
        if not isinstance(parsed, list) or not parsed:
            return ("dashboard",)
        channels = tuple(value for value in parsed if isinstance(value, str) and value.strip())
        if not channels or len(set(channels)) != len(channels):
            return ("dashboard",)
        return channels

    def _recovery_job_lease_cursor(
        self,
        cursor: psycopg.Cursor[Any],
        action: RecoveryActionRecord,
        *,
        now: datetime,
    ) -> JobLease:
        """Materialize a JobLease under the caller's existing transaction."""
        self._validate_aware(now, "now")
        if action.target_type != "job":
            raise ValueError("job recovery action must target a job")
        cursor.execute(
            """
            SELECT j.job_key, j.job_type, j.input_identity, j.lease_owner,
                   j.lease_epoch, j.lease_expires_at, j.checkpoint_cursor,
                   j.checkpoint_digest, j.state
            FROM public.m1_jobs AS j
            JOIN public.m1_job_runtime_state AS r ON r.job_key = j.job_key
            WHERE j.job_key = %s
              AND r.attempt_id = %s
              AND r.lease_epoch = %s
              AND r.worker_id = j.lease_owner
              AND j.lease_epoch = %s
            FOR UPDATE
            """,
            (
                action.target_id,
                action.expected_attempt_id,
                action.expected_lease_epoch,
                action.expected_lease_epoch,
            ),
        )
        row = cursor.fetchone()
        if row is None or row["state"] != JobState.LEASED.value or row["lease_owner"] is None:
            raise StaleLeaseError(f"recovery action fence is stale for {action.target_id}")
        if row["lease_expires_at"] is None:
            raise StaleLeaseError(f"job lease is missing for {action.target_id}")
        expires_at = row["lease_expires_at"]
        if expires_at <= now:
            raise StaleLeaseError(f"job lease has expired for {action.target_id}")
        return JobLease(
            job_key=str(row["job_key"]),
            job_type=str(row["job_type"]),
            input_identity=str(row["input_identity"]),
            lease_owner=str(row["lease_owner"]),
            lease_epoch=int(row["lease_epoch"]),
            lease_expires_at=expires_at,
            checkpoint_cursor=row["checkpoint_cursor"],
            checkpoint_digest=row["checkpoint_digest"],
        )

    def _finish_retryable_with_incident_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
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
        action_deadline: datetime | None = None,
    ) -> datetime:
        """Retry transition composed into an outer recovery transaction."""
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
        failure_fingerprint, failure_signature = _retry_failure_identity(
            component=component,
            error_class=error_class,
            detail=detail,
        )
        if failure_signature == "service.interrupted":
            raise ValueError("service interruption must use finish_interrupted")
        _set_fenced_transaction_timeouts(
            cursor,
            lease=lease,
            now=now,
            action_deadline=action_deadline,
        )
        cursor.execute(
            """
            SELECT consecutive_failures, state, opened_at, failure_fingerprint
            FROM public.m1_job_circuits WHERE job_key = %s FOR UPDATE
            """,
            (lease.job_key,),
        )
        circuit = cursor.fetchone()
        previous_opened_at = None
        if circuit is not None and str(circuit["failure_fingerprint"]) == failure_fingerprint:
            failures = int(circuit["consecutive_failures"]) + 1
            previous_opened_at = circuit["opened_at"]
        else:
            failures = 1
        retry_policy = runtime_retry_policy(component)
        retry_budget = retry_policy.retry_budget
        delay_seconds = retry_policy.retry_backoff_seconds(failures)
        next_attempt_at = now + timedelta(seconds=delay_seconds)
        circuit_state = "open" if failures >= retry_budget else "closed"
        opened_at = now if failures == retry_budget else previous_opened_at
        cursor.execute(
            """
            INSERT INTO public.m1_job_circuits (
                job_key, consecutive_failures, state, opened_at, next_probe_at,
                updated_at, failure_fingerprint
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_key) DO UPDATE
            SET consecutive_failures = EXCLUDED.consecutive_failures,
                state = EXCLUDED.state, opened_at = EXCLUDED.opened_at,
                next_probe_at = EXCLUDED.next_probe_at, updated_at = EXCLUDED.updated_at,
                failure_fingerprint = EXCLUDED.failure_fingerprint
            """,
            (
                lease.job_key,
                failures,
                circuit_state,
                opened_at,
                next_attempt_at,
                now,
                failure_fingerprint,
            ),
        )
        self._append_retry_runtime_events_cursor(
            cursor,
            lease=lease,
            component=component,
            error_class=error_class,
            retry_count=failures,
            backoff_seconds=delay_seconds,
            next_attempt_at=next_attempt_at,
            now=now,
            detail=detail,
        )
        cursor.execute(
            """
            UPDATE public.m1_jobs
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
            UPDATE public.m1_job_attempts
            SET state = 'retryable', finished_at = %s, error_class = %s,
                error_detail = %s
            WHERE job_key = %s AND lease_epoch = %s
            """,
            (
                now,
                error_class,
                Jsonb(
                    {
                        "failure_fingerprint": failure_fingerprint,
                        "failure_signature": failure_signature,
                    }
                ),
                lease.job_key,
                lease.lease_epoch,
            ),
        )
        kind = (
            "circuit-opened"
            if failures == retry_budget
            else ("circuit-probe-failed" if failures > retry_budget else "attempt-failed")
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
                "failure_fingerprint": failure_fingerprint,
                "failure_signature": failure_signature,
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

    def _heartbeat_recovering_job_cursor(
        self,
        action: RecoveryActionRecord,
        *,
        now: datetime,
        lease_seconds: int = 30,
        cursor: psycopg.Cursor[Any],
    ) -> str:
        """Renew only the exact job attempt named by a heartbeat action."""
        self._recovery_action(action, "heartbeat-job")
        lease = self._recovery_job_lease_cursor(cursor, action, now=now)
        try:
            update_runtime_heartbeat_cursor(
                cursor,
                job_key=lease.job_key,
                attempt_id=action.expected_attempt_id,
                lease_epoch=lease.lease_epoch,
                worker_id=lease.lease_owner,
                now=now,
                lease_seconds=lease_seconds,
            )
        except RuntimeFenceError as error:
            raise StaleLeaseError(str(error)) from error
        return "succeeded"

    def _cancel_stalled_job_cursor(
        self,
        action: RecoveryActionRecord,
        *,
        now: datetime,
        cursor: psycopg.Cursor[dict[str, Any]],
        action_deadline: datetime | None = None,
    ) -> str:
        """Cooperatively end a current stalled attempt and put it on retry backoff."""
        self._recovery_action(action, "cancel-job")
        lease = self._recovery_job_lease_cursor(cursor, action, now=now)
        self._finish_retryable_with_incident_cursor(
            cursor,
            lease,
            error_class="RecoveryProgressStalled",
            incident_key=action.incident_key or f"recovery:job:{action.target_id}",
            dedupe_key=f"recovery:job:{action.target_id}",
            component=self._recovery_component(action),
            summary=f"{self._recovery_component(action)} recovery cancelled stalled job",
            detail={
                "action_id": action.action_id,
                "reason_code": action.detail.get("reason_code", "job.progress-stalled"),
                "recovery_action": action.action_type,
            },
            channels=self._recovery_channels(action),
            now=now,
            action_deadline=action_deadline,
        )
        return "succeeded"

    def _release_retryable_job_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        action: RecoveryActionRecord,
        *,
        now: datetime,
        action_deadline: datetime | None = None,
    ) -> str:
        self._recovery_action(action, "retry-job")
        self._validate_aware(now, "now")
        if action.target_type != "job":
            raise ValueError("retry recovery action must target a job")
        cursor.execute(
            """
            SELECT j.state, j.lease_owner, j.lease_epoch, j.lease_expires_at,
                   r.attempt_id, r.lease_epoch AS runtime_epoch
            FROM public.m1_jobs AS j
            JOIN public.m1_job_runtime_state AS r ON r.job_key = j.job_key
            WHERE j.job_key = %s
              AND r.attempt_id = %s
              AND r.lease_epoch = %s
              AND j.lease_epoch = %s
            FOR UPDATE
            """,
            (
                action.target_id,
                action.expected_attempt_id,
                action.expected_lease_epoch,
                action.expected_lease_epoch,
            ),
        )
        row = cursor.fetchone()
        if row is None:
            raise StaleLeaseError(f"retry action fence is stale for {action.target_id}")
        if row["state"] == JobState.RETRYABLE.value:
            cursor.execute(
                """
                UPDATE public.m1_jobs
                SET next_attempt_at = %s, updated_at = %s
                WHERE job_key = %s AND state = 'retryable'
                """,
                (now, now, action.target_id),
            )
            return "succeeded"
        if (
            row["state"] != JobState.LEASED.value
            or row["lease_owner"] is None
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= now
        ):
            raise StaleLeaseError(f"retry action job is not current for {action.target_id}")
        lease = self._recovery_job_lease_cursor(cursor, action, now=now)
        self._finish_retryable_with_incident_cursor(
            cursor,
            lease,
            error_class="RecoveryRetryRequested",
            incident_key=action.incident_key or f"recovery:job:{action.target_id}",
            dedupe_key=f"recovery:job:{action.target_id}",
            component=self._recovery_component(action),
            summary=f"{self._recovery_component(action)} recovery retry requested",
            detail={
                "action_id": action.action_id,
                "reason_code": action.detail.get("reason_code", "job.retry-requested"),
                "recovery_action": action.action_type,
            },
            channels=self._recovery_channels(action),
            now=now,
            action_deadline=action_deadline,
        )
        return "succeeded"

    def _reclaim_expired_job_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        action: RecoveryActionRecord,
        *,
        now: datetime,
    ) -> str:
        self._recovery_action(action, "reclaim-job")
        self._validate_aware(now, "now")
        if action.target_type != "job":
            raise ValueError("reclaim recovery action must target a job")
        event_idempotency = f"{action.idempotency_key}:reclaimed"
        cursor.execute(
            """
            SELECT event_id FROM public.m1_job_runtime_events
            WHERE idempotency_key = %s
            """,
            (event_idempotency,),
        )
        existing_event = cursor.fetchone()
        cursor.execute(
            """
            SELECT j.job_key, j.state, j.lease_owner, j.lease_epoch,
                   j.lease_expires_at, j.attempt_count,
                   r.attempt_id, r.worker_id, r.lease_epoch AS runtime_epoch,
                   r.stage
            FROM public.m1_jobs AS j
            JOIN public.m1_job_runtime_state AS r ON r.job_key = j.job_key
            WHERE j.job_key = %s
            FOR UPDATE
            """,
            (action.target_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise StaleLeaseError(f"reclaim job is missing for {action.target_id}")
        exact_runtime = (
            str(row["attempt_id"]) == action.expected_attempt_id
            and int(row["runtime_epoch"]) == action.expected_lease_epoch
            and int(row["lease_epoch"]) == action.expected_lease_epoch
        )
        if not exact_runtime:
            raise StaleLeaseError(f"reclaim action fence is stale for {action.target_id}")
        if existing_event is not None:
            if row["state"] == JobState.RETRYABLE.value and row["lease_owner"] is None:
                return "succeeded"
            raise StaleLeaseError(f"reclaim replay is no longer current for {action.target_id}")
        if (
            row["state"] != JobState.LEASED.value
            or row["lease_owner"] is None
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] > now
        ):
            raise StaleLeaseError(f"job lease is not expired for {action.target_id}")
        cursor.execute(
            """
            UPDATE public.m1_jobs
            SET state = 'retryable', next_attempt_at = %s,
                last_error_class = 'RecoveryLeaseExpired', lease_owner = NULL,
                lease_expires_at = NULL, updated_at = %s
            WHERE job_key = %s AND state = 'leased'
              AND lease_epoch = %s AND lease_owner = %s
            """,
            (
                now,
                now,
                action.target_id,
                action.expected_lease_epoch,
                row["lease_owner"],
            ),
        )
        if cursor.rowcount != 1:
            raise StaleLeaseError(f"job changed during reclaim for {action.target_id}")
        cursor.execute(
            """
            UPDATE public.m1_job_attempts
            SET state = 'retryable', finished_at = %s,
                error_class = 'RecoveryLeaseExpired'
            WHERE attempt_id = %s AND job_key = %s AND lease_epoch = %s
              AND state = 'running'
            """,
            (now, action.expected_attempt_id, action.target_id, action.expected_lease_epoch),
        )
        if cursor.rowcount != 1:
            raise StaleLeaseError(f"attempt changed during reclaim for {action.target_id}")
        cursor.execute(
            """
            UPDATE public.m1_job_runtime_state
            SET recovery_state = 'recovered', updated_at = %s
            WHERE job_key = %s AND attempt_id = %s AND lease_epoch = %s
            """,
            (now, action.target_id, action.expected_attempt_id, action.expected_lease_epoch),
        )
        if cursor.rowcount != 1:
            raise StaleLeaseError(f"runtime state changed during reclaim for {action.target_id}")
        cursor.execute(
            """
            SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
            FROM public.m1_job_runtime_events
            WHERE attempt_id = %s
            """,
            (action.expected_attempt_id,),
        )
        sequence_row = cursor.fetchone()
        if sequence_row is None:
            raise RuntimeError("reclaim event sequence query returned no row")
        cursor.execute(
            """
            INSERT INTO public.m1_job_runtime_events (
                event_id, job_key, attempt_id, lease_epoch, worker_id,
                event_sequence, kind, stage, progress_sequence, progress_current,
                progress_total, detail, occurred_at, idempotency_key
            ) VALUES (%s, %s, %s, %s, %s, %s, 'job.retry-scheduled', %s,
                      NULL, NULL, NULL, %s, %s, %s)
            """,
            (
                str(uuid4()),
                action.target_id,
                action.expected_attempt_id,
                action.expected_lease_epoch,
                row["worker_id"],
                int(sequence_row["next_sequence"]),
                row["stage"],
                Jsonb(
                    {
                        "backoff_seconds": 0,
                        "next_decision_at": now.isoformat(),
                        "reason_code": action.detail.get("reason_code", "job.lease-expired"),
                        "recovery_policy": "reclaim-job",
                        "retry_count": int(row["attempt_count"]),
                    }
                ),
                now,
                event_idempotency,
            ),
        )
        return "succeeded"

    def _release_one_circuit_probe_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        action: RecoveryActionRecord,
        *,
        now: datetime,
    ) -> str:
        self._recovery_action(action, "probe-circuit")
        self._validate_aware(now, "now")
        if action.target_type != "circuit":
            raise ValueError("circuit probe action must target a circuit")
        event_idempotency = f"{action.idempotency_key}:probe-released"
        cursor.execute(
            """
            SELECT c.state AS circuit_state, c.next_probe_at,
                   c.consecutive_failures, j.state AS job_state, j.job_type,
                   j.lease_epoch, j.lease_owner, j.lease_expires_at,
                   r.attempt_id, r.lease_epoch AS runtime_epoch,
                   r.worker_id, r.stage, a.state AS attempt_state
            FROM public.m1_job_circuits AS c
            JOIN public.m1_jobs AS j ON j.job_key = c.job_key
            JOIN public.m1_job_runtime_state AS r ON r.job_key = c.job_key
            JOIN public.m1_job_attempts AS a
              ON a.attempt_id = r.attempt_id
             AND a.job_key = r.job_key
             AND a.lease_epoch = r.lease_epoch
            WHERE c.job_key = %s
              AND r.attempt_id = %s
              AND r.lease_epoch = %s
              AND j.lease_epoch = %s
            FOR UPDATE
            """,
            (
                action.target_id,
                action.expected_attempt_id,
                action.expected_lease_epoch,
                action.expected_lease_epoch,
            ),
        )
        row = cursor.fetchone()
        if row is None:
            raise StaleLeaseError(f"circuit probe fence is stale for {action.target_id}")
        if row["circuit_state"] != "open" or row["next_probe_at"] is None:
            raise StaleLeaseError(f"circuit is not open for {action.target_id}")
        if row["next_probe_at"] > now:
            raise StaleLeaseError(f"circuit probe is not due for {action.target_id}")
        job_state = str(row["job_state"])
        reclaimed_expired_lease = False
        if job_state == JobState.LEASED.value:
            if (
                row["lease_owner"] is None
                or row["lease_expires_at"] is None
                or row["lease_expires_at"] > now
                or row["attempt_state"] not in {"running", "checkpointed"}
            ):
                raise StaleLeaseError(f"circuit target lease is active for {action.target_id}")
            cursor.execute(
                """
                UPDATE public.m1_jobs
                SET state = 'retryable', next_attempt_at = %s,
                    last_error_class = 'RecoveryLeaseExpired', lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = %s
                WHERE job_key = %s AND state = 'leased'
                  AND lease_epoch = %s AND lease_owner = %s
                  AND lease_expires_at <= %s
                """,
                (
                    now,
                    now,
                    action.target_id,
                    action.expected_lease_epoch,
                    row["lease_owner"],
                    now,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(
                    f"circuit target changed during expired reclaim for {action.target_id}"
                )
            if row["attempt_state"] == "running":
                cursor.execute(
                    """
                    UPDATE public.m1_job_attempts
                    SET state = 'retryable', finished_at = %s,
                        error_class = 'RecoveryLeaseExpired'
                    WHERE attempt_id = %s AND job_key = %s AND lease_epoch = %s
                      AND state = 'running'
                    """,
                    (
                        now,
                        action.expected_attempt_id,
                        action.target_id,
                        action.expected_lease_epoch,
                    ),
                )
                if cursor.rowcount != 1:
                    raise StaleLeaseError(
                        f"circuit attempt changed during expired reclaim for {action.target_id}"
                    )
            cursor.execute(
                """
                UPDATE public.m1_job_runtime_state
                SET recovery_state = 'recovered', updated_at = %s
                WHERE job_key = %s AND attempt_id = %s AND lease_epoch = %s
                """,
                (
                    now,
                    action.target_id,
                    action.expected_attempt_id,
                    action.expected_lease_epoch,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(
                    f"circuit runtime changed during expired reclaim for {action.target_id}"
                )
            job_state = JobState.RETRYABLE.value
            reclaimed_expired_lease = True
        if job_state not in {JobState.RETRYABLE.value, JobState.RUNNABLE.value}:
            raise StaleLeaseError(f"circuit target is not retryable for {action.target_id}")
        retry_policy = runtime_retry_policy(str(row["job_type"]))
        probe_delay_seconds = retry_policy.retry_backoff_seconds(int(row["consecutive_failures"]))
        cursor.execute(
            """
            SELECT event_id FROM public.m1_job_runtime_events
            WHERE idempotency_key = %s
            """,
            (event_idempotency,),
        )
        if cursor.fetchone() is not None:
            return "succeeded"
        cursor.execute(
            """
            UPDATE public.m1_jobs
            SET state = 'retryable', next_attempt_at = %s, updated_at = %s
            WHERE job_key = %s AND lease_epoch = %s
              AND state IN ('retryable', 'runnable')
            """,
            (now, now, action.target_id, action.expected_lease_epoch),
        )
        if cursor.rowcount != 1:
            raise StaleLeaseError(f"circuit target changed for {action.target_id}")
        cursor.execute(
            """
            UPDATE public.m1_job_circuits
            SET next_probe_at = %s, updated_at = %s
            WHERE job_key = %s AND state = 'open'
            """,
            (now + timedelta(seconds=probe_delay_seconds), now, action.target_id),
        )
        if cursor.rowcount != 1:
            raise StaleLeaseError(f"circuit changed during probe release for {action.target_id}")
        cursor.execute(
            """
            SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
            FROM public.m1_job_runtime_events
            WHERE attempt_id = %s
            """,
            (action.expected_attempt_id,),
        )
        sequence_row = cursor.fetchone()
        if sequence_row is None:
            raise RuntimeError("probe event sequence query returned no row")
        cursor.execute(
            """
            INSERT INTO public.m1_job_runtime_events (
                event_id, job_key, attempt_id, lease_epoch, worker_id,
                event_sequence, kind, stage, progress_sequence, progress_current,
                progress_total, detail, occurred_at, idempotency_key
            ) VALUES (%s, %s, %s, %s, %s, %s, 'job.retry-scheduled', %s,
                      NULL, NULL, NULL, %s, %s, %s)
            """,
            (
                str(uuid4()),
                action.target_id,
                action.expected_attempt_id,
                action.expected_lease_epoch,
                row["worker_id"],
                int(sequence_row["next_sequence"]),
                row["stage"],
                Jsonb(
                    {
                        "backoff_seconds": 0,
                        "next_decision_at": now.isoformat(),
                        "reason_code": action.detail.get("reason_code", "circuit.probe-due"),
                        "recovery_policy": "probe-circuit",
                        "reclaimed_expired_lease": reclaimed_expired_lease,
                        "retry_count": 0,
                    }
                ),
                now,
                event_idempotency,
            ),
        )
        return "succeeded"

    def _execute_recovery_action_cursor(
        self,
        cursor: psycopg.Cursor[dict[str, Any]],
        action: RecoveryActionRecord,
        *,
        now: datetime,
        heartbeat_lease_seconds: int = 30,
    ) -> str:
        """Dispatch a claimed action using the store-owned transaction."""
        if action.action_type == "heartbeat-job":
            return self._heartbeat_recovering_job_cursor(
                action,
                cursor=cursor,
                now=now,
                lease_seconds=heartbeat_lease_seconds,
            )
        if action.action_type == "cancel-job":
            return self._cancel_stalled_job_cursor(
                action,
                cursor=cursor,
                now=now,
                action_deadline=action.worker_lease_expires_at,
            )
        if action.action_type == "retry-job":
            return self._release_retryable_job_cursor(
                cursor,
                action,
                now=now,
                action_deadline=action.worker_lease_expires_at,
            )
        if action.action_type == "reclaim-job":
            return self._reclaim_expired_job_cursor(cursor, action, now=now)
        if action.action_type == "probe-circuit":
            return self._release_one_circuit_probe_cursor(cursor, action, now=now)
        if action.action_type in {"restart-worker-process", "restart-machine"}:
            return "disabled-action"
        return "disabled-action"

    def record_runtime_progress(
        self,
        lease: JobLease,
        *,
        progress: RuntimeProgress,
        now: datetime,
        idempotency_key: str | None = None,
        detail: dict[str, object] | None = None,
    ) -> RuntimeEvent:
        """Persist task progress and its event under one lease transaction."""
        self._validate_aware(now, "now")
        if type(progress) is not RuntimeProgress:
            raise TypeError("progress must be RuntimeProgress")
        event_detail: dict[str, object] = (
            {"component": "control-plane"} if detail is None else dict(detail)
        )
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            cursor.execute(
                """
                SELECT attempt_id FROM m1_job_attempts
                WHERE job_key = %s AND lease_epoch = %s AND worker_id = %s
                """,
                (lease.job_key, lease.lease_epoch, lease.lease_owner),
            )
            attempt = cursor.fetchone()
            if attempt is None:
                raise StaleLeaseError(f"runtime attempt is no longer known for {lease.job_key}")
            attempt_id = str(attempt["attempt_id"])
            if idempotency_key is None:
                idempotency_key = f"runtime:{attempt_id}:progress:{progress.sequence}"
            self._validate_nonempty(idempotency_key=idempotency_key)
            event = RuntimeEvent(
                job_key=lease.job_key,
                attempt_id=attempt_id,
                lease_epoch=lease.lease_epoch,
                worker_id=lease.lease_owner,
                event_sequence=1,
                kind=RuntimeEventKind.STAGE_CHANGED,
                stage=progress.stage,
                progress=progress,
                detail=event_detail,
                occurred_at=now,
                idempotency_key=idempotency_key,
            )
            try:
                return update_runtime_progress_cursor(cursor, event=event)
            except RuntimeFenceError as error:
                raise StaleLeaseError(str(error)) from error
            except RuntimeProgressConflict as error:
                raise RuntimeProgressConflictError(str(error)) from error
            except RuntimeEventConflict as error:
                raise RuntimeEventConflictError(str(error)) from error

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

    def record_running_checkpoint(
        self,
        lease: JobLease,
        *,
        checkpoint_cursor: str,
        checkpoint_digest: str,
        artifact_key: str,
        idempotency_key: str,
        now: datetime,
    ) -> CheckpointReceipt:
        """Persist resumable work without releasing the current lease."""
        self._validate_nonempty(
            checkpoint_cursor=checkpoint_cursor,
            checkpoint_digest=checkpoint_digest,
            artifact_key=artifact_key,
            idempotency_key=idempotency_key,
        )
        self._validate_aware(now, "now")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            cursor.execute(
                """
                SELECT lease_epoch
                FROM m1_jobs
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                  AND state = 'leased' AND lease_expires_at > %s
                FOR UPDATE
                """,
                (
                    lease.job_key,
                    lease.lease_owner,
                    lease.lease_epoch,
                    now,
                ),
            )
            if cursor.fetchone() is None:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            cursor.execute(
                """
                SELECT receipt_id, job_key, lease_epoch, idempotency_key,
                       checkpoint_cursor, checkpoint_digest, artifact_key, committed_at
                FROM m1_checkpoint_receipts WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                receipt = self._receipt(existing)
                if (
                    receipt.job_key != lease.job_key
                    or receipt.checkpoint_cursor != checkpoint_cursor
                    or receipt.checkpoint_digest != checkpoint_digest
                    or str(existing["artifact_key"]) != artifact_key
                ):
                    raise CheckpointConflictError(f"idempotency conflict for {idempotency_key!r}")
                return receipt
            cursor.execute(
                """
                UPDATE m1_jobs
                SET checkpoint_cursor = %s, checkpoint_digest = %s, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                  AND state = 'leased' AND lease_expires_at > %s
                """,
                (
                    checkpoint_cursor,
                    checkpoint_digest,
                    now,
                    lease.job_key,
                    lease.lease_owner,
                    lease.lease_epoch,
                    now,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            cursor.execute(
                """
                SELECT COALESCE(MAX(checkpoint_sequence), 0) + 1 AS next_sequence
                FROM m1_checkpoint_receipts
                WHERE job_key = %s AND checkpoint_sequence IS NOT NULL
                """,
                (lease.job_key,),
            )
            sequence_row = cursor.fetchone()
            if sequence_row is None:
                raise ControlPlaneError("checkpoint sequence query returned no row")
            checkpoint_sequence = int(sequence_row["next_sequence"])
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
                    receipt_id, job_key, lease_epoch, idempotency_key,
                    checkpoint_cursor, checkpoint_digest, artifact_key, committed_at,
                    checkpoint_sequence
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    checkpoint_sequence,
                ),
            )
            return receipt

    def running_checkpoints(self, job_key: str) -> tuple[tuple[str, str, str], ...]:
        """Return immutable resumable artifacts in their committed order."""
        self._validate_nonempty(job_key=job_key)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT checkpoint_cursor, checkpoint_digest, artifact_key
                FROM m1_checkpoint_receipts
                WHERE job_key = %s AND checkpoint_sequence IS NOT NULL
                ORDER BY checkpoint_sequence
                """,
                (job_key,),
            )
            rows = cursor.fetchall()
        return tuple(
            (
                str(row["checkpoint_cursor"]),
                str(row["checkpoint_digest"]),
                str(row["artifact_key"]),
            )
            for row in rows
        )

    def finish(
        self,
        lease: JobLease,
        *,
        state: JobState,
        now: datetime,
        next_attempt_at: datetime | None = None,
        error_class: str | None = None,
    ) -> None:
        if state not in {
            JobState.RETRYABLE,
            JobState.WAITING,
            JobState.SUCCEEDED,
            JobState.QUARANTINED,
        }:
            raise ValueError("finish only accepts retryable, waiting, succeeded, or quarantined")
        self._validate_aware(now, "now")
        if next_attempt_at is not None:
            self._validate_aware(next_attempt_at, "next_attempt_at")
        if state is JobState.RETRYABLE and next_attempt_at is None:
            raise ValueError("retryable finish requires next_attempt_at")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
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
            if state is JobState.SUCCEEDED:
                self._wake_terminal_successor_cursor(cursor, lease=lease, now=now)

    def finish_interrupted(
        self,
        lease: JobLease,
        *,
        component: str,
        now: datetime,
    ) -> datetime:
        """Release a stopped attempt for immediate resume without defect accounting."""
        self._validate_aware(now, "now")
        self._validate_nonempty(component=component)
        retry_policy = runtime_retry_policy(component)
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            cursor.execute(
                """
                SELECT consecutive_failures, state
                FROM m1_job_circuits
                WHERE job_key = %s
                FOR UPDATE
                """,
                (lease.job_key,),
            )
            circuit = cursor.fetchone()
            self._append_retry_runtime_events_cursor(
                cursor,
                lease=lease,
                component=component,
                error_class="ServiceStopRequested",
                retry_count=0,
                backoff_seconds=0,
                next_attempt_at=now,
                now=now,
            )
            cursor.execute(
                """
                UPDATE m1_jobs
                SET state = 'retryable', next_attempt_at = %s,
                    last_error_class = 'ServiceStopRequested',
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                  AND state IN ('leased', 'checkpointed')
                """,
                (now, now, lease.job_key, lease.lease_owner, lease.lease_epoch),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
            if circuit is not None and circuit["state"] == "open":
                probe_delay_seconds = retry_policy.retry_backoff_seconds(
                    int(circuit["consecutive_failures"])
                )
                cursor.execute(
                    """
                    UPDATE m1_job_circuits
                    SET next_probe_at = %s, updated_at = %s
                    WHERE job_key = %s AND state = 'open'
                    """,
                    (
                        now + timedelta(seconds=probe_delay_seconds),
                        now,
                        lease.job_key,
                    ),
                )
                if cursor.rowcount != 1:
                    raise StaleLeaseError(
                        f"circuit changed during interruption for {lease.job_key}"
                    )
            cursor.execute(
                """
                UPDATE m1_job_attempts
                SET state = 'retryable', finished_at = %s,
                    error_class = 'ServiceStopRequested', error_detail = %s
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (
                    now,
                    Jsonb({"failure_signature": "service.interrupted"}),
                    lease.job_key,
                    lease.lease_epoch,
                ),
            )
        return now

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
        failure_fingerprint, failure_signature = _retry_failure_identity(
            component=component,
            error_class=error_class,
            detail=detail,
        )
        if failure_signature == "service.interrupted":
            raise ValueError("service interruption must use finish_interrupted")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            cursor.execute(
                """
                SELECT consecutive_failures, state, opened_at, failure_fingerprint
                FROM m1_job_circuits WHERE job_key = %s FOR UPDATE
                """,
                (lease.job_key,),
            )
            circuit = cursor.fetchone()
            previous_opened_at = None
            if circuit is not None and str(circuit["failure_fingerprint"]) == failure_fingerprint:
                failures = int(circuit["consecutive_failures"]) + 1
                previous_opened_at = circuit["opened_at"]
            else:
                failures = 1
            retry_policy = runtime_retry_policy(component)
            retry_budget = retry_policy.retry_budget
            delay_seconds = retry_policy.retry_backoff_seconds(failures)
            next_attempt_at = now + timedelta(seconds=delay_seconds)
            circuit_state = "open" if failures >= retry_budget else "closed"
            opened_at = now if failures == retry_budget else previous_opened_at
            cursor.execute(
                """
                INSERT INTO m1_job_circuits (
                    job_key, consecutive_failures, state, opened_at, next_probe_at,
                    updated_at, failure_fingerprint
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_key) DO UPDATE
                SET consecutive_failures = EXCLUDED.consecutive_failures,
                    state = EXCLUDED.state, opened_at = EXCLUDED.opened_at,
                    next_probe_at = EXCLUDED.next_probe_at, updated_at = EXCLUDED.updated_at,
                    failure_fingerprint = EXCLUDED.failure_fingerprint
                """,
                (
                    lease.job_key,
                    failures,
                    circuit_state,
                    opened_at,
                    next_attempt_at,
                    now,
                    failure_fingerprint,
                ),
            )
            self._append_retry_runtime_events_cursor(
                cursor,
                lease=lease,
                component=component,
                error_class=error_class,
                retry_count=failures,
                backoff_seconds=delay_seconds,
                next_attempt_at=next_attempt_at,
                now=now,
                detail=detail,
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
                SET state = 'retryable', finished_at = %s, error_class = %s,
                    error_detail = %s
                WHERE job_key = %s AND lease_epoch = %s
                """,
                (
                    now,
                    error_class,
                    Jsonb(
                        {
                            "failure_fingerprint": failure_fingerprint,
                            "failure_signature": failure_signature,
                        }
                    ),
                    lease.job_key,
                    lease.lease_epoch,
                ),
            )
            kind = (
                "circuit-opened"
                if failures == retry_budget
                else ("circuit-probe-failed" if failures > retry_budget else "attempt-failed")
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
                    "failure_fingerprint": failure_fingerprint,
                    "failure_signature": failure_signature,
                    "job_key": lease.job_key,
                    "stage": detail.get("stage", component),
                    "error_class": error_class,
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

    def finish_quarantined_with_incident(
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
        qualification_impact: str = "blocked",
        reason_code: str = "failure.schema",
        severity: str = "critical",
        incident_kind: str = "escalated",
        qualification_breaking: bool = True,
        now: datetime,
    ) -> str:
        """Atomically quarantine one non-retryable defect and surface operator action."""
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
        semantic = (
            qualification_impact,
            reason_code,
            severity,
            incident_kind,
            qualification_breaking,
        )
        if semantic not in {
            ("blocked", "failure.schema", "critical", "escalated", True),
            ("blocked", "freshness.quote", "warning", "attempt-failed", True),
            ("invalidated", "integrity.conflict", "critical", "escalated", True),
            ("delayed", "publication.superseded", "warning", "detected", False),
        }:
            raise ValueError("quarantine incident semantic is not allowed")
        detail_reason = detail.get("reason_code")
        if detail_reason is not None and detail_reason != reason_code:
            raise ValueError("incident detail reason_code conflicts with terminal fact")
        failure_fingerprint, failure_signature = _retry_failure_identity(
            component=component,
            error_class=error_class,
            detail=detail,
        )
        if failure_signature == "service.interrupted":
            raise ValueError("service interruption must use finish_interrupted")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            cursor.execute(
                """
                SELECT attempt_id, lease_epoch, worker_id, stage, progress_sequence,
                       progress_current, progress_total
                FROM public.m1_job_runtime_state
                WHERE job_key = %s
                FOR UPDATE
                """,
                (lease.job_key,),
            )
            state = cursor.fetchone()
            if state is None or (
                int(state["lease_epoch"]) != lease.lease_epoch
                or str(state["worker_id"]) != lease.lease_owner
            ):
                raise StaleLeaseError(f"runtime attempt is no longer current for {lease.job_key}")
            attempt_id = str(state["attempt_id"])
            stage = str(state["stage"])
            progress_sequence = int(state["progress_sequence"])
            progress = (
                None
                if progress_sequence == 0
                else RuntimeProgress(
                    sequence=progress_sequence,
                    current=int(state["progress_current"]),
                    total=(
                        None if state["progress_total"] is None else int(state["progress_total"])
                    ),
                    stage=stage,
                )
            )
            cursor.execute(
                """
                SELECT COALESCE(MAX(event_sequence), 0) + 1 AS next_sequence
                FROM public.m1_job_runtime_events
                WHERE attempt_id = %s
                """,
                (attempt_id,),
            )
            sequence_row = cursor.fetchone()
            if sequence_row is None:
                raise ControlPlaneError("runtime event sequence query returned no row")
            append_runtime_event_cursor(
                cursor,
                RuntimeEvent(
                    job_key=lease.job_key,
                    attempt_id=attempt_id,
                    lease_epoch=lease.lease_epoch,
                    worker_id=lease.lease_owner,
                    event_sequence=int(sequence_row["next_sequence"]),
                    kind=RuntimeEventKind.TERMINAL_FAILED,
                    stage=stage,
                    progress=progress,
                    detail={
                        "component": component,
                        "failure_signature": failure_signature,
                        "qualification_impact": qualification_impact,
                        "reason_code": reason_code,
                        "result_code": "failed",
                    },
                    occurred_at=now,
                    idempotency_key=f"runtime:{attempt_id}:terminal-failed",
                ),
            )
            cursor.execute(
                """
                UPDATE public.m1_job_circuits
                SET consecutive_failures = 0, state = 'closed', opened_at = NULL,
                    next_probe_at = NULL, updated_at = %s, failure_fingerprint = NULL
                WHERE job_key = %s
                """,
                (now, lease.job_key),
            )
            cursor.execute(
                """
                UPDATE public.m1_jobs
                SET state = 'quarantined', next_attempt_at = NULL,
                    last_error_class = %s, lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = %s
                WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                  AND state IN ('leased', 'checkpointed')
                """,
                (
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
                UPDATE public.m1_job_attempts
                SET state = 'quarantined', finished_at = %s, error_class = %s,
                    error_detail = %s
                WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                """,
                (
                    now,
                    error_class,
                    Jsonb(
                        {
                            "failure_fingerprint": failure_fingerprint,
                            "failure_signature": failure_signature,
                        }
                    ),
                    lease.job_key,
                    lease.lease_epoch,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleLeaseError(f"runtime attempt is no longer current for {lease.job_key}")
            return self._record_incident_event(
                cursor,
                incident_key=incident_key,
                dedupe_key=dedupe_key,
                component=component,
                severity=severity,
                summary=summary,
                kind=incident_kind,
                detail={
                    **detail,
                    "error_class": error_class,
                    "failure_fingerprint": failure_fingerprint,
                    "failure_signature": failure_signature,
                    "qualification_breaking": qualification_breaking,
                },
                idempotency_key=f"input-quarantine:{lease.job_key}:{lease.lease_epoch}",
                channels=channels,
                now=now,
            )

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
                WHERE (
                    state IN ('pending', 'retryable')
                    AND COALESCE(next_attempt_at, created_at) <= %s
                )
                   OR (state = 'retryable' AND lease_expires_at <= %s)
                ORDER BY COALESCE(next_attempt_at, created_at), created_at, outbox_id
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
                WHERE ((state IN ('pending', 'retryable')
                    AND COALESCE(next_attempt_at, created_at) <= %s)
                   OR (state = 'retryable' AND lease_expires_at <= %s))
                  AND payload->>'acceptance_run_id' = %s
                ORDER BY COALESCE(next_attempt_at, created_at), created_at, outbox_id
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
        """Close a failed job's circuit and recovery incidents after progress.

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
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
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
            if circuit is not None and int(circuit["consecutive_failures"]) > 0:
                cursor.execute(
                    """
                    UPDATE m1_job_circuits
                    SET consecutive_failures = 0, state = 'closed', opened_at = NULL,
                        next_probe_at = NULL, updated_at = %s, failure_fingerprint = NULL
                    WHERE job_key = %s
                    """,
                    (now, lease.job_key),
                )
            incident_dedupe_keys = (
                f"job-retry:{lease.job_key}",
                f"recovery:job:{lease.job_key}",
                f"recovery:circuit:{lease.job_key}",
                *(("freshness:quote",) if component == "opportunity-certify" else ()),
            )
            cursor.execute(
                """
                SELECT incident_key, dedupe_key
                FROM m1_incidents
                WHERE dedupe_key = ANY(%s) AND state <> 'resolved'
                ORDER BY dedupe_key
                FOR UPDATE
                """,
                (list(incident_dedupe_keys),),
            )
            incidents = cursor.fetchall()
            if not incidents:
                return False
            suffix_by_dedupe = {
                incident_dedupe_keys[0]: "",
                incident_dedupe_keys[1]: ":runtime-job",
                incident_dedupe_keys[2]: ":runtime-circuit",
                **(
                    {"freshness:quote": ":freshness-quote"}
                    if component == "opportunity-certify"
                    else {}
                ),
            }
            for incident in incidents:
                incident_key = str(incident["incident_key"])
                incident_dedupe_key = str(incident["dedupe_key"])
                cursor.execute(
                    """
                    UPDATE m1_incidents
                    SET state = 'resolved', resolved_at = %s, updated_at = %s
                    WHERE incident_key = %s AND state <> 'resolved'
                    """,
                    (now, now, incident_key),
                )
                idempotency_key = (
                    f"job-recovery:{lease.job_key}:{lease.lease_epoch}"
                    f"{suffix_by_dedupe[incident_dedupe_key]}"
                )
                cursor.execute(
                    "SELECT incident_event_id FROM m1_incident_events WHERE idempotency_key = %s",
                    (idempotency_key,),
                )
                if cursor.fetchone() is not None:
                    continue
                event_id = str(uuid4())
                detail_payload = {
                    "job_key": lease.job_key,
                    "lease_epoch": lease.lease_epoch,
                    "component": component,
                    "reason": "runtime-healthy",
                    "action_type": "none",
                    "qualification_impact": "none",
                }
                cursor.execute(
                    """
                    INSERT INTO m1_incident_events (
                        incident_event_id, incident_key, kind, detail,
                        idempotency_key, occurred_at
                    ) VALUES (%s, %s, 'recovered', %s, %s, %s)
                    """,
                    (
                        event_id,
                        incident_key,
                        Jsonb(detail_payload),
                        idempotency_key,
                        now,
                    ),
                )
                alert_payload = _incident_alert_payload(
                    incident_key=incident_key,
                    component=component,
                    kind="recovered",
                    detail=detail_payload,
                    now=now,
                    acceptance_run_id=acceptance_run_id,
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
                            Jsonb(alert_payload),
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
        cursor: Any,
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
            INSERT INTO public.m1_incidents (
                incident_key, dedupe_key, component, severity, state, summary,
                opened_at, updated_at
            ) VALUES (%s, %s, %s, %s, 'open', %s, %s, %s)
            ON CONFLICT (dedupe_key) DO NOTHING
            """,
            (incident_key, dedupe_key, component, severity, summary, now, now),
        )
        cursor.execute(
            "SELECT incident_key FROM public.m1_incidents WHERE dedupe_key = %s",
            (dedupe_key,),
        )
        incident = cursor.fetchone()
        if incident is None or incident["incident_key"] != incident_key:
            raise JobIdentityConflict(f"dedupe key {dedupe_key!r} names another incident")
        cursor.execute(
            "SELECT incident_event_id FROM public.m1_incident_events WHERE idempotency_key = %s",
            (idempotency_key,),
        )
        existing = cursor.fetchone()
        if existing is not None:
            return str(existing["incident_event_id"])
        event_id = str(uuid4())
        alert_payload = _incident_alert_payload(
            incident_key=incident_key,
            component=component,
            kind=kind,
            detail=detail,
            now=now,
        )
        cursor.execute(
            """
            INSERT INTO public.m1_incident_events (
                incident_event_id, incident_key, kind, detail, idempotency_key, occurred_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (event_id, incident_key, kind, Jsonb(detail), idempotency_key, now),
        )
        for channel in channels:
            cursor.execute(
                """
                INSERT INTO public.m1_alert_outbox (
                    outbox_id, incident_event_id, channel, payload, state,
                    next_attempt_at, created_at
                ) VALUES (%s, %s, %s, %s, 'pending', %s, %s)
                ON CONFLICT (incident_event_id, channel) DO NOTHING
                """,
                (
                    str(uuid4()),
                    event_id,
                    channel,
                    Jsonb(alert_payload),
                    now,
                    now,
                ),
            )
        return event_id

    def record_cloud_usage(
        self,
        *,
        source: str,
        operation: str,
        bytes_received: int,
        item_count: int,
        artifact_key: str,
        artifact_digest: str,
        daily_budget_bytes: int,
        now: datetime,
    ) -> CloudUsageDecision:
        self._validate_nonempty(source=source, operation=operation, artifact_key=artifact_key)
        self._validate_aware(now, "now")
        if (
            bytes_received < 0
            or item_count < 0
            or daily_budget_bytes <= 0
            or len(artifact_digest) != 64
        ):
            raise ValueError("invalid cloud usage observation")
        observation_id = str(uuid4())
        budget_day = now.astimezone(UTC).date()
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"m1-cloud-egress:{budget_day}",),
            )
            cursor.execute(
                """INSERT INTO m1_cloud_usage_observations
                   (observation_id,observed_at,budget_day,source,operation,bytes_received,daily_budget_bytes,item_count,artifact_key,artifact_digest)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    observation_id,
                    now,
                    budget_day,
                    source,
                    operation,
                    bytes_received,
                    daily_budget_bytes,
                    item_count,
                    artifact_key,
                    artifact_digest,
                ),
            )
            cursor.execute(
                "SELECT COALESCE(sum(bytes_received),0) AS used "
                "FROM m1_cloud_usage_observations WHERE budget_day=%s",
                (budget_day,),
            )
            usage_total = cursor.fetchone()
            if usage_total is None:
                raise RuntimeError("cloud usage total was not returned")
            used = int(str(usage_total["used"]))
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
                    detail={
                        "used_bytes": used,
                        "daily_budget_bytes": daily_budget_bytes,
                        "threshold_percent": threshold,
                        "observation_id": observation_id,
                    },
                    idempotency_key=f"{dedupe_key}:{observation_id}",
                    channels=("dashboard", "telegram"),
                    now=now,
                )
        return CloudUsageDecision(threshold < 90, used, threshold, observation_id)

    def operational_snapshot(
        self,
        *,
        now: datetime | None = None,
        sample_limit: int = 20,
    ) -> dict[str, object]:
        """Read the bounded, durable operator view without touching SQLite.

        This projection deliberately uses only control-plane facts.  A stalled
        data worker or unavailable Fly volume must therefore not turn an
        operator incident into an empty/healthy response.
        """
        if now is not None:
            self._validate_aware(now, "now")
        if not 1 <= sample_limit <= 100:
            raise ValueError("sample_limit must be in 1..100")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            query = sql.SQL(_OPERATIONAL_SNAPSHOT_SQL).format(
                observed_at=sql.Literal(now),
                sample_limit=sql.Literal(sample_limit),
                controller_id=sql.Literal(_SNAPSHOT_RUNTIME_CONTROLLER_ID),
                statement_timeout=sql.Literal(CONTROL_PLANE_DB_POLICY.statement_setting),
                lock_timeout=sql.Literal(CONTROL_PLANE_DB_POLICY.lock_setting),
            )
            cursor.execute(query)
            for setup_command in (
                "repeatable-read transaction",
                "statement deadline",
                "lock deadline",
            ):
                if not cursor.nextset():
                    raise ControlPlaneError(f"snapshot data result missing after {setup_command}")
            snapshot_row = cursor.fetchone()
            if snapshot_row is None:
                raise ControlPlaneError("snapshot database projection is unavailable")

        now = _snapshot_aware(snapshot_row["snapshot_now"], "snapshot_now")
        job_counts = _snapshot_count_map(snapshot_row["job_counts"], "job_counts")
        quote_batch_states = _snapshot_count_map(
            snapshot_row["quote_batch_states"], "quote_batch_states"
        )
        quote_admission_states = _snapshot_count_map(
            snapshot_row["quote_admission_states"], "quote_admission_states"
        )
        retryable_quote_admission_age = snapshot_row["retryable_quote_admission_age"]
        quote_certifier_states = _snapshot_count_map(
            snapshot_row["quote_certifier_states"], "quote_certifier_states"
        )
        retryable_quote_age = snapshot_row["retryable_quote_age"]
        quote_pointer = _snapshot_optional_mapping(snapshot_row["quote_pointer"], "quote_pointer")
        structure_range_states = _snapshot_count_map(
            snapshot_row["structure_range_states"], "structure_range_states"
        )
        source_fetch_states = _snapshot_count_map(
            snapshot_row["source_fetch_states"], "source_fetch_states"
        )
        source_materializer_states = _snapshot_count_map(
            snapshot_row["source_materializer_states"], "source_materializer_states"
        )
        retryable_source_age = snapshot_row["retryable_source_age"]
        structure_certifier_states = _snapshot_count_map(
            snapshot_row["structure_certifier_states"], "structure_certifier_states"
        )
        retryable_structure_age = snapshot_row["retryable_structure_age"]
        structure_manifest = _snapshot_optional_mapping(
            snapshot_row["structure_manifest"], "structure_manifest"
        )
        structure_pointer = _snapshot_optional_mapping(
            snapshot_row["structure_pointer"], "structure_pointer"
        )
        age = snapshot_row["oldest_runnable_age"]
        queue_health = {
            "structure-range": _snapshot_queue_health(
                snapshot_row["structure_queue_health"], "structure_queue_health"
            ),
            "quote-batch": _snapshot_queue_health(
                snapshot_row["quote_queue_health"], "quote_queue_health"
            ),
        }
        expired = snapshot_row["expired_leases"]
        open_circuit_count = snapshot_row["open_circuit_count"]
        open_circuit_rows = _snapshot_rows(snapshot_row["open_circuits"], "open_circuits")
        open_circuits = [
            {
                "job_key": _snapshot_text(row["job_key"], "job_key"),
                "consecutive_failures": _snapshot_int(
                    row["consecutive_failures"], "consecutive_failures"
                ),
                "next_probe_at": _snapshot_aware(row["next_probe_at"], "next_probe_at").isoformat(),
                "failure_fingerprint": _snapshot_text(
                    row["failure_fingerprint"], "failure_fingerprint"
                ),
            }
            for row in open_circuit_rows
        ]
        attempt_rows = _snapshot_rows(snapshot_row["attempts"], "attempts")
        attempts = []
        for row in attempt_rows:
            attempt: dict[str, object] = {
                "job_key": _snapshot_text(row["job_key"], "job_key"),
                "lease_epoch": _snapshot_int(row["lease_epoch"], "lease_epoch"),
                "worker_id": _snapshot_text(row["worker_id"], "worker_id"),
                "state": _snapshot_text(row["state"], "state"),
            }
            if row["error_class"] is not None:
                attempt["error_class"] = _snapshot_text(row["error_class"], "error_class")
            error_detail = row["error_detail"]
            if error_detail is not None:
                if not isinstance(error_detail, Mapping):
                    raise ControlPlaneError("attempt error detail is malformed")
                for key in ("failure_signature", "failure_fingerprint"):
                    value = error_detail.get(key)
                    if value is not None:
                        attempt[key] = _snapshot_text(value, key)
            attempts.append(attempt)
        incident_rows = _snapshot_rows(snapshot_row["incidents"], "incidents")
        incidents = [
            {
                "incident_key": _snapshot_text(row["incident_key"], "incident_key"),
                "component": _snapshot_text(row["component"], "component"),
                "severity": _snapshot_text(row["severity"], "severity"),
                "summary": _snapshot_text(row["summary"], "summary"),
            }
            for row in incident_rows
        ]
        runtime_current = _snapshot_optional_mapping(
            snapshot_row["runtime_current"], "runtime_current"
        )
        runtime_event_rows = _snapshot_rows(snapshot_row["runtime_events"], "runtime_events")
        runtime_events = [
            {
                "kind": _snapshot_text(row["kind"], "kind"),
                "occurred_at": _snapshot_aware(row["occurred_at"], "occurred_at").isoformat(),
                "incident_key": _snapshot_text(row["incident_key"], "incident_key"),
                "severity": _snapshot_text(row["severity"], "severity"),
                "summary": _snapshot_text(row["summary"], "summary"),
                "detail": dict(_snapshot_mapping(row["detail"], "detail")),
            }
            for row in runtime_event_rows
        ]
        outbox_rows = _snapshot_rows(snapshot_row["outbox"], "outbox")
        outbox = [
            {
                "incident_key": _snapshot_text(row["incident_key"], "incident_key"),
                "channel": _snapshot_text(row["channel"], "channel"),
                "state": _snapshot_text(row["state"], "state"),
            }
            for row in outbox_rows
        ]
        alert_delivery_raw = _snapshot_mapping(snapshot_row["alert_delivery"], "alert_delivery")
        latest_delivery_at = alert_delivery_raw["latest_delivery_at"]
        latest_delivery_state = alert_delivery_raw["latest_delivery_state"]
        latest_delivery_channel = alert_delivery_raw["latest_delivery_channel"]
        latest_delivery_error_class = alert_delivery_raw["latest_delivery_error_class"]
        alert_delivery = {
            "pending_count": _snapshot_int(
                alert_delivery_raw["pending_count"], "alert_delivery.pending_count"
            ),
            "oldest_pending_age_seconds": (
                None
                if alert_delivery_raw["oldest_pending_age_seconds"] is None
                else _snapshot_seconds(
                    alert_delivery_raw["oldest_pending_age_seconds"],
                    "alert_delivery.oldest_pending_age_seconds",
                )
            ),
            "latest_delivery_at": (
                None
                if latest_delivery_at is None
                else _snapshot_aware(
                    latest_delivery_at, "alert_delivery.latest_delivery_at"
                ).isoformat()
            ),
            "latest_delivery_state": (
                None if latest_delivery_state is None else str(latest_delivery_state)
            ),
            "latest_delivery_channel": (
                None if latest_delivery_channel is None else str(latest_delivery_channel)
            ),
            "latest_delivery_error_class": (
                None
                if latest_delivery_error_class is None
                else _snapshot_text(
                    latest_delivery_error_class,
                    "alert_delivery.latest_delivery_error_class",
                )
            ),
        }
        latest_soak_observation = _snapshot_optional_mapping(
            snapshot_row["latest_soak_observation"], "latest_soak_observation"
        )
        cloud_usage = _snapshot_mapping(snapshot_row["cloud_usage"], "cloud_usage")
        latest_cloud_usage = _snapshot_optional_mapping(
            snapshot_row["latest_cloud_usage"], "latest_cloud_usage"
        )
        budget_day = now.astimezone(UTC).date()
        runtime_controller_row = _snapshot_optional_mapping(
            snapshot_row["runtime_controller"], "runtime_controller"
        )
        active_task_rows = _snapshot_rows(snapshot_row["active_tasks"], "active_tasks")
        active_task_total = snapshot_row["active_task_total"]
        runtime_incident_rows = _snapshot_rows(
            snapshot_row["runtime_incidents"], "runtime_incidents"
        )
        runtime_incident_event_rows = _snapshot_rows(
            snapshot_row["runtime_incident_events"], "runtime_incident_events"
        )
        runtime_incident_total = snapshot_row["runtime_incident_total"]
        recovery_action_rows = _snapshot_rows(snapshot_row["recovery_actions"], "recovery_actions")
        recovery_action_total = snapshot_row["recovery_action_total"]
        qualification_epoch = _snapshot_optional_mapping(
            snapshot_row["qualification_epoch"], "qualification_epoch"
        )
        qualification_certificate = _snapshot_optional_mapping(
            snapshot_row["qualification_certificate"], "qualification_certificate"
        )
        qualification_breaker = _snapshot_optional_mapping(
            snapshot_row["qualification_breaker"], "qualification_breaker"
        )
        quote_retry_age = retryable_quote_age
        quote_admission_retry_age = retryable_quote_admission_age
        source_retry_age = retryable_source_age
        structure_retry_age = retryable_structure_age
        runtime_controller = self._runtime_controller_snapshot(
            runtime_controller_row,
            now=now,
        )
        active_tasks = self._active_tasks_snapshot(
            active_task_rows,
            total=active_task_total,
        )
        runtime_incidents = self._runtime_incidents_snapshot(
            runtime_incident_rows,
            runtime_incident_event_rows,
            total=runtime_incident_total,
            now=now,
        )
        recovery_actions = self._recovery_actions_snapshot(
            recovery_action_rows,
            total=recovery_action_total,
        )
        qualification = self._qualification_snapshot(
            qualification_epoch,
            qualification_certificate,
            qualification_breaker,
            now=now,
        )
        return {
            # Database size must be collected by an independent bounded probe.
            # pg_database_size() inside this statement can cancel the entire
            # business/runtime read on a pressure-bound provider.
            "database_capacity": {
                "state": "unavailable",
                "reason_code": "database-size-observation-unavailable",
            },
            "job_counts": job_counts,
            "oldest_runnable_age_seconds": (
                None if age is None else _snapshot_seconds(age, "oldest_runnable_age_seconds")
            ),
            "expired_leases": _snapshot_int(expired, "expired_leases"),
            "open_circuit_count": _snapshot_int(open_circuit_count, "open_circuit_count"),
            "open_circuits": open_circuits,
            "recent_attempts": attempts,
            "open_incidents": incidents,
            "runtime_controller": runtime_controller,
            "active_tasks": active_tasks,
            "runtime_incidents": runtime_incidents,
            "recovery_actions": recovery_actions,
            "qualification": qualification,
            "runtime_watchdog": {
                "current": (
                    None
                    if runtime_current is None
                    else {
                        "incident_key": str(runtime_current["incident_key"]),
                        "severity": str(runtime_current["severity"]),
                        "summary": str(runtime_current["summary"]),
                        "opened_at": _snapshot_aware(
                            runtime_current["opened_at"], "runtime_watchdog.opened_at"
                        ).isoformat(),
                        "source": _snapshot_text(
                            _snapshot_mapping(runtime_current["detail"], "detail").get(
                                "source", "unknown"
                            ),
                            "runtime_watchdog.source",
                        ),
                        "failures": _snapshot_text_array(
                            _snapshot_mapping(runtime_current["detail"], "detail").get(
                                "failures", []
                            ),
                            "runtime_watchdog.failures",
                        ),
                    }
                ),
                "recent_events": runtime_events,
            },
            "soak_evidence": (
                None
                if latest_soak_observation is None
                else {
                    "latest_run_id": str(latest_soak_observation["run_id"]),
                    "latest_observed_at": _snapshot_aware(
                        latest_soak_observation["observed_at"], "soak.observed_at"
                    ).isoformat(),
                }
            ),
            "pending_alert_outbox": outbox,
            "alert_delivery": alert_delivery,
            "cloud_usage": {
                "budget_day": budget_day.isoformat(),
                "used_bytes": _snapshot_int(cloud_usage["used_bytes"], "cloud_usage.used_bytes"),
                "daily_budget_bytes": (
                    None
                    if cloud_usage["daily_budget_bytes"] is None
                    else _snapshot_int(
                        cloud_usage["daily_budget_bytes"], "cloud_usage.daily_budget_bytes"
                    )
                ),
                "threshold_percent": (
                    0
                    if cloud_usage["daily_budget_bytes"] is None
                    else min(
                        100,
                        _snapshot_int(cloud_usage["used_bytes"], "cloud_usage.used_bytes")
                        * 100
                        // _snapshot_int(
                            cloud_usage["daily_budget_bytes"],
                            "cloud_usage.daily_budget_bytes",
                        ),
                    )
                ),
                "latest_observation": (
                    None
                    if latest_cloud_usage is None
                    else {
                        "observation_id": str(latest_cloud_usage["observation_id"]),
                        "source": str(latest_cloud_usage["source"]),
                        "operation": str(latest_cloud_usage["operation"]),
                        "bytes_received": _snapshot_int(
                            latest_cloud_usage["bytes_received"], "cloud_usage.bytes_received"
                        ),
                        "item_count": _snapshot_int(
                            latest_cloud_usage["item_count"], "cloud_usage.item_count"
                        ),
                        "artifact_key": str(latest_cloud_usage["artifact_key"]),
                        "artifact_digest": str(latest_cloud_usage["artifact_digest"]),
                        "observed_at": _snapshot_aware(
                            latest_cloud_usage["observed_at"], "cloud_usage.observed_at"
                        ).isoformat(),
                    }
                ),
            },
            "queue_health": queue_health,
            "quote": {
                "admission_job_states": quote_admission_states,
                "oldest_retryable_admission_age_seconds": (
                    None
                    if quote_admission_retry_age is None
                    else _snapshot_seconds(quote_admission_retry_age, "quote.admission_retry_age")
                ),
                "batch_job_states": quote_batch_states,
                "certifier_job_states": quote_certifier_states,
                "oldest_retryable_batch_age_seconds": (
                    None
                    if quote_retry_age is None
                    else _snapshot_seconds(quote_retry_age, "quote.batch_retry_age")
                ),
                "current_pointer": (
                    None
                    if quote_pointer is None
                    else {
                        "generation_key": str(quote_pointer["generation_key"]),
                        "parent_structure_generation_key": str(
                            quote_pointer["structure_generation_key"]
                        ),
                        "cadence_seconds": (
                            None
                            if quote_pointer["cadence_seconds"] is None
                            else _snapshot_int(
                                quote_pointer["cadence_seconds"], "quote.cadence_seconds"
                            )
                        ),
                        "cadence_bucket": (
                            None
                            if quote_pointer["cadence_bucket"] is None
                            else _snapshot_int(
                                quote_pointer["cadence_bucket"], "quote.cadence_bucket"
                            )
                        ),
                        "next_eligible_at": (
                            None
                            if quote_pointer["next_eligible_at"] is None
                            else _snapshot_aware(
                                quote_pointer["next_eligible_at"], "quote.next_eligible_at"
                            ).isoformat()
                        ),
                        "published_at": _snapshot_aware(
                            quote_pointer["published_at"], "quote.published_at"
                        ).isoformat(),
                        "artifact_key": str(quote_pointer["artifact_key"]),
                        "artifact_digest": str(quote_pointer["artifact_digest"]),
                        "record_count": _snapshot_int(
                            quote_pointer["record_count"], "quote.record_count"
                        ),
                    }
                ),
            },
            "structure": {
                "source_fetch_job_states": source_fetch_states,
                "oldest_retryable_source_age_seconds": (
                    None
                    if source_retry_age is None
                    else _snapshot_seconds(source_retry_age, "structure.source_retry_age")
                ),
                "source_materializer_job_states": source_materializer_states,
                "range_job_states": structure_range_states,
                "certifier_job_states": structure_certifier_states,
                "oldest_retryable_range_age_seconds": (
                    None
                    if structure_retry_age is None
                    else _snapshot_seconds(structure_retry_age, "structure.range_retry_age")
                ),
                "latest_manifest": self._manifest_snapshot(structure_manifest),
                "shadow_pointer": (
                    None
                    if structure_pointer is None
                    else {
                        "generation_key": str(structure_pointer["generation_key"]),
                        "expected_generation_key": structure_pointer["expected_generation_key"],
                        "published_at": _snapshot_aware(
                            structure_pointer["published_at"], "structure.published_at"
                        ).isoformat(),
                        "artifact_key": str(structure_pointer["artifact_key"]),
                        "artifact_digest": str(structure_pointer["artifact_digest"]),
                        "record_count": _snapshot_int(
                            structure_pointer["record_count"], "structure.record_count"
                        ),
                    }
                ),
            },
        }

    @staticmethod
    def _runtime_controller_snapshot(
        row: Mapping[str, object] | None,
        *,
        now: datetime,
    ) -> dict[str, object]:
        _snapshot_aware(now, "now")
        if row is None:
            return {
                "status": "unavailable",
                "reason": "missing-controller",
                "controller_id": _SNAPSHOT_RUNTIME_CONTROLLER_ID,
                "owner_id": None,
                "epoch": None,
                "claimed_at": None,
                "last_tick_at": None,
                "lease_expires_at": None,
                "lease_active": False,
                "lease_age_seconds": None,
                "lease_overdue_seconds": None,
            }
        lease_epoch = _snapshot_int(row["lease_epoch"], "lease_epoch")
        if lease_epoch <= 0:
            raise ControlPlaneError("runtime controller epoch is malformed")
        lease_expires_at = _snapshot_aware(row["lease_expires_at"], "lease_expires_at")
        lease_active = lease_expires_at > now
        return {
            "status": "healthy" if lease_active else "critical",
            "controller_id": _snapshot_text(row["controller_id"], "controller_id"),
            "owner_id": _snapshot_text(row["owner_id"], "owner_id"),
            "epoch": lease_epoch,
            "claimed_at": _snapshot_aware(row["claimed_at"], "claimed_at").isoformat(),
            "last_tick_at": _snapshot_aware(row["updated_at"], "updated_at").isoformat(),
            "lease_expires_at": lease_expires_at.isoformat(),
            "lease_active": lease_active,
            "lease_age_seconds": _snapshot_seconds(row["lease_age_seconds"], "lease_age_seconds"),
            "lease_overdue_seconds": _snapshot_seconds(
                row["lease_overdue_seconds"], "lease_overdue_seconds"
            ),
        }

    @staticmethod
    def _active_tasks_snapshot(
        rows: Sequence[Mapping[str, object]],
        *,
        total: object,
    ) -> dict[str, object]:
        items = []
        for row in rows:
            recovery_state = _snapshot_text(row["recovery_state"], "recovery_state")
            if recovery_state not in _SNAPSHOT_ACTIVE_TASK_STATES:
                raise ControlPlaneError("runtime task state is malformed")
            job_type = _snapshot_text(row["job_type"], "job_type")
            if job_type not in RUNTIME_STAGE_REGISTRY:
                raise ControlPlaneError("runtime task job type is malformed")
            stage = _snapshot_text(row["stage"], "stage")
            if (
                stage != _SNAPSHOT_RUNTIME_INITIAL_STAGE
                and stage not in RUNTIME_STAGE_REGISTRY[job_type]
            ):
                raise ControlPlaneError("runtime task stage is malformed")
            progress_current = _snapshot_int(row["progress_current"], "progress_current")
            progress_total = (
                None
                if row["progress_total"] is None
                else _snapshot_int(row["progress_total"], "progress_total")
            )
            if progress_total is not None and progress_current > progress_total:
                raise ControlPlaneError("runtime task progress is malformed")
            items.append(
                {
                    "job_key": _snapshot_text(row["job_key"], "job_key"),
                    "attempt_id": _snapshot_text(row["attempt_id"], "attempt_id"),
                    "job_type": job_type,
                    "worker_id": _snapshot_text(row["worker_id"], "worker_id"),
                    "lease_epoch": _snapshot_int(row["lease_epoch"], "lease_epoch"),
                    "stage": stage,
                    "recovery_state": recovery_state,
                    "progress": {"current": progress_current, "total": progress_total},
                    "started_at": _snapshot_aware(row["started_at"], "started_at").isoformat(),
                    "last_heartbeat_at": _snapshot_aware(
                        row["last_heartbeat_at"], "last_heartbeat_at"
                    ).isoformat(),
                    "last_progress_at": _snapshot_aware(
                        row["last_progress_at"], "last_progress_at"
                    ).isoformat(),
                    "lease_deadline_at": _snapshot_aware(
                        row["lease_deadline_at"], "lease_deadline_at"
                    ).isoformat(),
                    "heartbeat_deadline_at": _snapshot_aware(
                        row["heartbeat_deadline_at"], "heartbeat_deadline_at"
                    ).isoformat(),
                    "progress_deadline_at": _snapshot_aware(
                        row["progress_deadline_at"], "progress_deadline_at"
                    ).isoformat(),
                    "attempt_deadline_at": _snapshot_aware(
                        row["attempt_deadline_at"], "attempt_deadline_at"
                    ).isoformat(),
                    "heartbeat_age_seconds": _snapshot_seconds(
                        row["heartbeat_age_seconds"], "heartbeat_age_seconds"
                    ),
                    "progress_age_seconds": _snapshot_seconds(
                        row["progress_age_seconds"], "progress_age_seconds"
                    ),
                    "heartbeat_missing_overdue_seconds": _snapshot_seconds(
                        row["heartbeat_missing_overdue_seconds"],
                        "heartbeat_missing_overdue_seconds",
                    ),
                    "progress_overdue_seconds": _snapshot_seconds(
                        row["progress_overdue_seconds"], "progress_overdue_seconds"
                    ),
                    "lease_overdue_seconds": _snapshot_seconds(
                        row["lease_overdue_seconds"], "lease_overdue_seconds"
                    ),
                    "attempt_overdue_seconds": _snapshot_seconds(
                        row["attempt_overdue_seconds"], "attempt_overdue_seconds"
                    ),
                }
            )
        return {"items": items, "total": _snapshot_int(total, "active_tasks.total")}

    @staticmethod
    def _runtime_incidents_snapshot(
        rows: Sequence[Mapping[str, object]],
        event_rows: Sequence[Mapping[str, object]],
        *,
        total: object,
        now: datetime,
    ) -> dict[str, object]:
        _snapshot_aware(now, "now")
        transitions: dict[str, list[dict[str, object]]] = {
            _snapshot_text(row["incident_key"], "incident_key"): [] for row in rows
        }
        for event in event_rows:
            incident_key = _snapshot_text(event["incident_key"], "incident_key")
            kind = _snapshot_text(event["kind"], "incident_transition")
            if kind not in _SNAPSHOT_INCIDENT_TRANSITIONS:
                raise ControlPlaneError("incident transition is malformed")
            transitions.setdefault(incident_key, []).append(
                {
                    "kind": kind,
                    "occurred_at": _snapshot_aware(event["occurred_at"], "occurred_at").isoformat(),
                    "age_seconds": _snapshot_seconds(event["age_seconds"], "age_seconds"),
                    **_snapshot_transition_detail(event["detail"]),
                }
            )
        items = []
        for row in rows:
            state = _snapshot_text(row["state"], "incident_state")
            if state not in _SNAPSHOT_INCIDENT_STATES:
                raise ControlPlaneError("incident state is malformed")
            severity = _snapshot_text(row["severity"], "incident_severity")
            if severity not in _SNAPSHOT_INCIDENT_SEVERITIES:
                raise ControlPlaneError("incident severity is malformed")
            incident_key = _snapshot_text(row["incident_key"], "incident_key")
            incident_transitions = transitions.get(incident_key, [])
            transition = incident_transitions[0]["kind"] if incident_transitions else None
            items.append(
                {
                    "incident_key": incident_key,
                    "component": _snapshot_text(row["component"], "component"),
                    "severity": severity,
                    "state": state,
                    "summary": _snapshot_text(row["summary"], "summary"),
                    "opened_at": _snapshot_aware(row["opened_at"], "opened_at").isoformat(),
                    "updated_at": _snapshot_aware(row["updated_at"], "updated_at").isoformat(),
                    "age_seconds": _snapshot_seconds(
                        (now - _snapshot_aware(row["opened_at"], "opened_at")).total_seconds(),
                        "incident_age_seconds",
                    ),
                    "transition": transition,
                    "transitions": incident_transitions,
                }
            )
        return {"items": items, "total": _snapshot_int(total, "runtime_incidents.total")}

    @staticmethod
    def _recovery_actions_snapshot(
        rows: Sequence[Mapping[str, object]],
        *,
        total: object,
    ) -> dict[str, object]:
        items = []
        for row in rows:
            state = _snapshot_text(row["state"], "action_state")
            if state not in _SNAPSHOT_ACTION_STATES:
                raise ControlPlaneError("recovery action state is malformed")
            target_type = _snapshot_text(row["target_type"], "target_type")
            if target_type not in _SNAPSHOT_ACTION_TARGET_TYPES:
                raise ControlPlaneError("recovery action target type is malformed")
            action_type = _snapshot_text(row["action_type"], "action_type")
            if action_type not in _SNAPSHOT_ACTION_TYPES:
                raise ControlPlaneError("recovery action type is malformed")
            result_code = None
            if row["result_code"] is not None:
                result_code = _snapshot_text(row["result_code"], "result_code")
                if result_code not in _SNAPSHOT_ACTION_RESULTS:
                    raise ControlPlaneError("recovery action result is malformed")
            items.append(
                {
                    "action_id": _snapshot_text(row["action_id"], "action_id"),
                    "incident_key": (
                        None
                        if row["incident_key"] is None
                        else _snapshot_text(row["incident_key"], "incident_key")
                    ),
                    "target_type": target_type,
                    "target_id": _snapshot_text(row["target_id"], "target_id"),
                    "action_type": action_type,
                    "state": state,
                    "result_code": result_code,
                    "expected_controller_epoch": _snapshot_int(
                        row["expected_controller_epoch"], "expected_controller_epoch"
                    ),
                    "expected_attempt_id": _snapshot_text(
                        row["expected_attempt_id"], "expected_attempt_id"
                    ),
                    "expected_lease_epoch": _snapshot_int(
                        row["expected_lease_epoch"], "expected_lease_epoch"
                    ),
                    "requested_at": _snapshot_aware(
                        row["requested_at"], "requested_at"
                    ).isoformat(),
                    "started_at": None
                    if row["started_at"] is None
                    else _snapshot_aware(row["started_at"], "started_at").isoformat(),
                    "finished_at": None
                    if row["finished_at"] is None
                    else _snapshot_aware(row["finished_at"], "finished_at").isoformat(),
                    "next_allowed_at": _snapshot_aware(
                        row["next_allowed_at"], "next_allowed_at"
                    ).isoformat(),
                    "worker_id": None
                    if row["worker_id"] is None
                    else _snapshot_text(row["worker_id"], "worker_id"),
                    "worker_epoch": _snapshot_int(row["worker_epoch"], "worker_epoch"),
                    "worker_lease_expires_at": None
                    if row["worker_lease_expires_at"] is None
                    else _snapshot_aware(
                        row["worker_lease_expires_at"], "worker_lease_expires_at"
                    ).isoformat(),
                }
            )
        return {"items": items, "total": _snapshot_int(total, "recovery_actions.total")}

    @staticmethod
    def _qualification_snapshot(
        epoch: Mapping[str, object] | None,
        certificate: Mapping[str, object] | None,
        breaker: Mapping[str, object] | None,
        *,
        now: datetime,
    ) -> dict[str, object]:
        observed_at = _snapshot_aware(now, "now")
        if epoch is None:
            return {
                "state": "accumulating",
                "epoch_id": None,
                "started_at": None,
                "eligible_seconds": 0,
                "required_seconds": None,
                "max_gap_seconds": None,
                "last_fact_at": None,
                "last_fact_age_seconds": None,
                "last_breaker": None,
                "policy_version": None,
                "release_id": None,
                "config_id": None,
                "role_identity": [],
                "certificate": None,
                "eligibility_state": "blocked",
                "eligibility_reason": "commissioning-or-epoch-missing",
            }
        state = _snapshot_text(epoch["state"], "qualification_state")
        if state not in _SNAPSHOT_QUALIFICATION_STATES:
            raise ControlPlaneError("qualification state is malformed")
        role_identity = _snapshot_role_identity(epoch["role_identity"])
        slo = _snapshot_mapping(epoch["slo"], "qualification_slo")
        default_eligibility_state = {
            "invalidated": "invalidated",
            "qualified": "qualified",
            "recovering": "blocked",
        }.get(state, "eligible")
        eligibility_state = _snapshot_text(
            slo.get("eligibility_state", default_eligibility_state),
            "qualification_eligibility_state",
        )
        if eligibility_state not in _SNAPSHOT_QUALIFICATION_ELIGIBILITY_STATES:
            raise ControlPlaneError("qualification eligibility state is malformed")
        eligibility_reason = (
            None
            if slo.get("eligibility_reason") is None
            else _snapshot_text(slo["eligibility_reason"], "qualification_eligibility_reason")
        )
        started_at = _snapshot_aware(epoch["started_at"], "qualification_started_at")
        if started_at > observed_at:
            raise ControlPlaneError("qualification time source is malformed")
        last_fact_at = (
            None
            if epoch["last_fact_at"] is None
            else _snapshot_aware(epoch["last_fact_at"], "last_fact_at")
        )
        if last_fact_at is not None and (last_fact_at < started_at or last_fact_at > observed_at):
            raise ControlPlaneError("qualification time source is malformed")
        required_seconds = (
            None
            if epoch["required_seconds"] is None
            else _snapshot_int(epoch["required_seconds"], "required_seconds")
        )
        last_breaker = None
        if breaker is not None and breaker["observed_at"] is not None:
            last_breaker = {
                "observed_at": _snapshot_aware(
                    breaker["observed_at"], "breaker_observed_at"
                ).isoformat(),
                "reason": _snapshot_text(breaker["reason"], "breaker_reason"),
                "fact_id": _snapshot_text(breaker["fact_id"], "breaker_fact_id"),
            }
        certificate_summary = None
        if certificate is not None:
            certificate_summary = {
                "certificate_id": _snapshot_text(certificate["certificate_id"], "certificate_id"),
                "certificate_digest": _snapshot_text(
                    certificate["certificate_digest"], "certificate_digest"
                ),
                "evidence_digest": _snapshot_text(
                    certificate["evidence_digest"], "evidence_digest"
                ),
                "qualified_at": _snapshot_aware(
                    certificate["qualified_at"], "certificate_qualified_at"
                ).isoformat(),
                "created_at": _snapshot_aware(
                    certificate["created_at"], "certificate_created_at"
                ).isoformat(),
            }
        eligible_seconds = _snapshot_int(epoch["coverage_seconds"], "coverage_seconds")
        if required_seconds is not None and eligible_seconds > required_seconds:
            raise ControlPlaneError("qualification coverage source is malformed")
        if (
            state == "qualified"
            and required_seconds is not None
            and eligible_seconds != required_seconds
        ):
            raise ControlPlaneError("qualification coverage source is malformed")
        return {
            "state": state,
            "epoch_id": _snapshot_text(epoch["epoch_id"], "epoch_id"),
            "started_at": started_at.isoformat(),
            "eligible_seconds": eligible_seconds,
            "required_seconds": required_seconds,
            "max_gap_seconds": _snapshot_int(epoch["max_gap_seconds"], "max_gap_seconds"),
            "last_fact_at": None if last_fact_at is None else last_fact_at.isoformat(),
            "last_fact_age_seconds": None
            if last_fact_at is None
            else _snapshot_seconds(
                (observed_at - last_fact_at).total_seconds(), "last_fact_age_seconds"
            ),
            "last_breaker": last_breaker,
            "policy_version": _snapshot_text(epoch["policy_version"], "policy_version"),
            "release_id": _snapshot_text(epoch["release_id"], "release_id"),
            "config_id": _snapshot_text(epoch["config_id"], "config_id"),
            "role_identity": role_identity,
            "certificate": certificate_summary,
            "eligibility_state": eligibility_state,
            "eligibility_reason": eligibility_reason,
        }

    def business_overview(self) -> dict[str, object]:
        """Read the initial business authority inside one repeatable-read transaction.

        This deliberately starts with only durable pointer facts.  Products whose
        durable projection has not yet been published are explicit, never zero.
        """
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute(
                sql.SQL("SET LOCAL statement_timeout = {}").format(
                    sql.Literal(CONTROL_PLANE_DB_POLICY.statement_setting)
                )
            )
            cursor.execute(
                sql.SQL("SET LOCAL lock_timeout = {}").format(
                    sql.Literal(CONTROL_PLANE_DB_POLICY.lock_setting)
                )
            )
            cursor.execute(
                "SELECT clock_timestamp() AS observed_at, "
                "(SELECT to_jsonb(manifest) FROM ("
                " SELECT manifest.generation_key, manifest.published_at, manifest.record_count, "
                "        inputs.identity->'component_counts' AS component_counts"
                "      ,(SELECT count(*) FROM m1_business_structure_rows AS research"
                "         WHERE research.generation_key = manifest.generation_key)"
                "         AS indexed_record_count"
                "      ,(SELECT COALESCE(sum(receipt.record_count), 0)"
                "         FROM m1_structure_range_receipts receipt"
                "         WHERE receipt.job_key LIKE manifest.generation_key || ':normalize:%%'"
                "           AND receipt.component IN ('events', 'group_truth'))"
                "         AS expected_indexed_record_count"
                " FROM m1_generation_manifests manifest"
                " LEFT JOIN m1_structure_generation_inputs inputs"
                "   ON inputs.generation_key = manifest.generation_key"
                " WHERE manifest.generation_key LIKE 'structure:' || chr(37)"
                " ORDER BY manifest.published_at DESC, manifest.generation_key DESC LIMIT 1"
                ") manifest) AS structure, "
                "(SELECT to_jsonb(quote) FROM ("
                " SELECT pointer.generation_key, pointer.published_at, manifest.record_count, "
                "        lineage.structure_generation_key"
                "      ,(SELECT count(*) FROM m1_business_quote_rows AS research"
                "         WHERE research.generation_key = pointer.generation_key)"
                "         AS indexed_record_count"
                "      ,COALESCE((SELECT sum(input.leg_count)"
                "         FROM m1_quote_batch_inputs AS input"
                "         WHERE input.job_key LIKE pointer.generation_key || ':batch:%%'),"
                "         manifest.record_count) AS expected_indexed_record_count"
                " FROM m1_publication_pointers pointer"
                " JOIN m1_generation_manifests manifest ON manifest.generation_key = pointer.generation_key"
                " JOIN m1_quote_generation_inputs lineage ON lineage.generation_key = pointer.generation_key"
                " WHERE pointer.pointer_key = 'quote:current'"
                ") quote) AS quote, "
                "(SELECT to_jsonb(opportunity) FROM ("
                " SELECT projection.generation_key, projection.structure_generation_key, projection.record_count"
                " FROM m1_opportunity_publication_pointers pointer"
                " JOIN m1_opportunity_projections projection ON projection.generation_key = pointer.generation_key"
                " WHERE pointer.pointer_key = 'opportunity:current'"
                ") opportunity) AS opportunity, "
                "(SELECT to_jsonb(candidate) FROM ("
                " SELECT projection.generation_key, projection.structure_generation_key, projection.record_count, "
                "        projection.positive_edge_count"
                " FROM m1_publication_pointers pointer"
                " JOIN m1_quote_generation_inputs lineage ON lineage.generation_key = pointer.generation_key"
                " JOIN m1_analysis_candidate_projections projection"
                "   ON projection.generation_key = pointer.generation_key"
                "  AND projection.structure_generation_key = lineage.structure_generation_key"
                " WHERE pointer.pointer_key = 'quote:current'"
                ") candidate) AS candidate",
            )
            row = cursor.fetchone()
            if row is None:
                raise ControlPlaneError("business overview database projection is unavailable")

        observed_at = _snapshot_aware(row["observed_at"], "business_overview.observed_at").isoformat()
        structure = _snapshot_optional_mapping(row["structure"], "business_overview.structure")
        quote = _snapshot_optional_mapping(row["quote"], "business_overview.quote")
        opportunity = _snapshot_optional_mapping(row["opportunity"], "business_overview.opportunity")
        candidate = _snapshot_optional_mapping(row.get("candidate"), "business_overview.candidate")
        if structure is None:
            return {
                "schema_version": "m1.business-overview.v1", "status": "available", "observed_at": observed_at,
                "eligibility": {"state": "paused", "reason_code": "structure-not-published"},
                "structure": {"status": "not-published", "reason_code": "structure-not-published"},
                "quote": {"status": "not-published", "reason_code": "structure-not-published"},
                "analysis": {"status": "not-published", "reason_code": "not-yet-projected"},
                "opportunities": {"status": "not-published", "reason_code": "structure-not-published"},
                "blockers": [{"scope": "structure", "code": "structure-not-published", "impact": "blocking"}],
            }
        structure_indexed_count = _snapshot_int(
            structure["indexed_record_count"], "business_overview.structure.indexed_record_count"
        )
        expected_structure_indexed_count = _snapshot_int(
            structure["expected_indexed_record_count"],
            "business_overview.structure.expected_indexed_record_count",
        )
        structure_index_ready = structure_indexed_count >= expected_structure_indexed_count
        quote_indexed_count = (
            None
            if quote is None
            else _snapshot_int(quote["indexed_record_count"], "business_overview.quote.indexed_record_count")
        )
        expected_quote_indexed_count = (
            None
            if quote is None
            else _snapshot_int(
                quote["expected_indexed_record_count"],
                "business_overview.quote.expected_indexed_record_count",
            )
        )
        quote_index_ready = (
            quote_indexed_count is not None
            and expected_quote_indexed_count is not None
            and quote_indexed_count == expected_quote_indexed_count
        )
        analysis: dict[str, object]
        if quote is None:
            analysis = {"status": "not-published", "reason_code": "quote-not-published"}
        else:
            quote_current = str(quote["structure_generation_key"]) == str(structure["generation_key"])
            component_counts = {
                "structure_records": _snapshot_int(
                    structure["record_count"], "business_overview.analysis.structure_records"
                ),
                "quote_records": _snapshot_int(
                    quote["record_count"], "business_overview.analysis.quote_records"
                ),
            }
            if opportunity is not None and str(opportunity["generation_key"]) == str(quote["generation_key"]):
                component_counts["certified_opportunities"] = _snapshot_int(
                    opportunity["record_count"],
                    "business_overview.analysis.certified_opportunities",
                )
            candidate_current = candidate is not None and str(candidate["generation_key"]) == str(
                quote["generation_key"]
            )
            if candidate_current:
                component_counts["analysis_groups"] = _snapshot_int(
                    candidate["record_count"], "business_overview.analysis.analysis_groups"
                )
                component_counts["positive_edge_candidates"] = _snapshot_int(
                    candidate["positive_edge_count"], "business_overview.analysis.positive_edge_candidates"
                )
            analysis = {
                "status": (
                    "available"
                    if quote_current and quote_index_ready
                    else "unavailable" if not quote_index_ready else "lagging"
                ),
                "generation_key": str(quote["generation_key"]),
                "parent_structure_generation_key": str(quote["structure_generation_key"]),
                "component_counts": component_counts,
            }
            if not candidate_current:
                analysis["reason_code"] = (
                    "candidate-reject-detail-not-published"
                    if quote_current and quote_index_ready
                    else "research-index-incomplete"
                    if not quote_index_ready
                    else "quote-lineage-lagging"
                )
        structure_product: dict[str, object] = {
            "status": "available" if structure_index_ready else "unavailable",
            "generation_key": str(structure["generation_key"]),
            "published_at": _snapshot_aware(
                structure["published_at"], "business_overview.structure.published_at"
            ).isoformat(),
            "record_count": _snapshot_int(
                structure["record_count"], "business_overview.structure.record_count"
            ),
            "indexed_record_count": structure_indexed_count,
            "component_counts": dict(structure.get("component_counts") or {}),
        }
        if not structure_index_ready:
            structure_product["reason_code"] = "research-index-incomplete"
        quote_product: dict[str, object] | None
        if quote is None:
            quote_product = None
        else:
            quote_product = {
                "status": (
                    "available"
                    if str(quote["structure_generation_key"]) == str(structure["generation_key"])
                    and quote_index_ready
                    else "unavailable"
                    if not quote_index_ready
                    else "lagging"
                ),
                "generation_key": str(quote["generation_key"]),
                "parent_structure_generation_key": str(quote["structure_generation_key"]),
                "published_at": _snapshot_aware(
                    quote["published_at"], "business_overview.quote.published_at"
                ).isoformat(),
                "record_count": _snapshot_int(
                    quote["record_count"], "business_overview.quote.record_count"
                ),
                "indexed_record_count": quote_indexed_count,
            }
            if not quote_index_ready:
                quote_product["reason_code"] = "research-index-incomplete"
        return {
            "schema_version": "m1.business-overview.v1", "status": "available", "observed_at": observed_at,
            "eligibility": {"state": "paused", "reason_code": "not-yet-qualified"},
            "structure": structure_product,
            "quote": ({"status": "not-published", "reason_code": "quote-not-published"} if quote_product is None else quote_product),
            "analysis": analysis,
            "opportunities": ({"status": "not-published", "reason_code": "opportunity-not-published"} if opportunity is None else {"status": "available" if quote is not None and str(opportunity["generation_key"]) == str(quote["generation_key"]) else "lagging", "quote_generation_key": str(opportunity["generation_key"]), "parent_structure_generation_key": str(opportunity["structure_generation_key"]), "count": _snapshot_int(opportunity["record_count"], "business_overview.opportunity.record_count")}),
            "blockers": [],
        }

    @staticmethod
    def _prune_superseded_business_research_rows_cursor(
        cursor: psycopg.Cursor[Any], *, product: str, current_generation_key: str
    ) -> None:
        """Retain the current published research index and any unpublished candidate rows.

        Immutable range/batch artifacts remain the historical source of truth in
        R2.  PostgreSQL only serves the current dashboard generation, so an
        older row becomes disposable only after its generation has a manifest.
        This deliberately preserves rows for a candidate still being staged.
        """
        table = {
            "structure": "m1_business_structure_rows",
            "quote": "m1_business_quote_rows",
        }[product]
        cursor.execute(
            sql.SQL(
                "DELETE FROM {table} AS research "
                "USING m1_generation_manifests AS manifest "
                "WHERE manifest.generation_key = research.generation_key "
                "AND manifest.generation_key LIKE {prefix} "
                "AND research.generation_key <> {current_generation_key}"
            ).format(
                table=sql.Identifier(table),
                prefix=sql.Literal(f"{product}:%"),
                current_generation_key=sql.Literal(current_generation_key),
            )
        )

    @staticmethod
    def _promote_staged_business_quote_rows_cursor(
        cursor: psycopg.Cursor[Any], *, generation_key: str
    ) -> None:
        """Atomically replace the dashboard index from one fenced candidate."""
        cursor.execute(
            "SELECT DISTINCT generation_key FROM m1_business_quote_staging_rows "
            "WHERE generation_key <> %s LIMIT 1",
            (generation_key,),
        )
        if cursor.fetchone() is not None:
            raise ControlPlaneError("quote staging contains another generation")
        # ``DELETE`` leaves the retired generation's heap pages allocated.  This
        # relation is a single-reader publication index, so certification can
        # transactionally truncate it before copying the fenced candidate.
        cursor.execute("TRUNCATE public.m1_business_quote_rows")
        cursor.execute(
            """INSERT INTO m1_business_quote_rows(generation_key, token_id, payload)
               SELECT generation_key, token_id, payload
               FROM m1_business_quote_staging_rows
               WHERE generation_key = %s ORDER BY token_id""",
            (generation_key,),
        )
        cursor.execute("TRUNCATE public.m1_business_quote_staging_rows")

    def reuse_quote_research_space(self) -> None:
        """Make pages from a retired Quote generation reusable by the next one.

        Quote index retirement is logical and pointer-fenced during certification.
        A regular ``VACUUM`` (not ``VACUUM FULL``) then marks its dead pages for
        reuse without rewriting the relation or requiring an exclusive outage.
        PostgreSQL forbids VACUUM inside a transaction, so this deliberately
        borrows one autocommit connection and restores its pool-safe setting.
        """
        with self._connection_factory() as connection:
            original_autocommit = connection.autocommit
            connection.autocommit = True
            try:
                with connection.cursor() as cursor:
                    cursor.execute("VACUUM (ANALYZE) public.m1_business_quote_rows")
            finally:
                connection.autocommit = original_autocommit

    def stage_business_structure_rows(
        self, *, generation_key: str, rows: Sequence[tuple[str, Mapping[str, object]]]
    ) -> None:
        """Stage bounded normalized rows; publication remains pointer-gated."""
        self._validate_nonempty(generation_key=generation_key)
        prepared_rows: list[tuple[str, str, Jsonb]] = []
        for entity_id, payload in rows:
            self._validate_nonempty(entity_id=entity_id)
            prepared_rows.append((generation_key, entity_id, Jsonb(dict(payload))))
        if not prepared_rows:
            return
        with self._connection_factory() as connection, connection.cursor() as cursor:
            _set_structure_read_timeouts(cursor, read_only=False)
            cursor.executemany(
                """INSERT INTO m1_business_structure_rows(generation_key, entity_id, payload)
                   VALUES (%s,%s,%s) ON CONFLICT (generation_key, entity_id) DO NOTHING""",
                prepared_rows,
            )

    def business_structure_research_entity_ids(self, *, generation_key: str) -> frozenset[str]:
        """Return current staged IDs so an interrupted index rebuild skips known rows."""
        self._validate_nonempty(generation_key=generation_key)
        with self._connection_factory() as connection, connection.cursor() as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT entity_id FROM m1_business_structure_rows
                   WHERE generation_key=%s""",
                (generation_key,),
            )
            return frozenset(str(row[0]) for row in cursor.fetchall())

    def published_structure_range_artifacts(
        self, *, generation_key: str
    ) -> tuple[tuple[str, str, str], ...]:
        """Return authenticated range artifacts for one published Structure generation."""
        self._validate_nonempty(generation_key=generation_key)
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT receipt.component, receipt.artifact_key, receipt.artifact_digest
                   FROM m1_generation_manifests AS manifest
                   JOIN m1_structure_range_receipts AS receipt
                     ON receipt.job_key LIKE manifest.generation_key || ':normalize:%%'
                   WHERE manifest.generation_key = %s
                     AND receipt.component IN ('events', 'group_truth')
                   ORDER BY receipt.component, receipt.artifact_key""",
                (generation_key,),
            )
            return tuple(
                (str(row["component"]), str(row["artifact_key"]), str(row["artifact_digest"]))
                for row in cursor.fetchall()
            )

    def published_structure_intelligence_artifacts(
        self, *, generation_key: str
    ) -> tuple[tuple[str, str, str], ...]:
        """Return just the authenticated R2 ranges needed for business research.

        Memberships and issues remain in R2: neither contributes to the current
        event overview or neg-risk queue, so fetching them would add cost and
        memory without improving the operator view.
        """
        self._validate_nonempty(generation_key=generation_key)
        # psycopg adapts a tuple as a PostgreSQL record, not a text array;
        # ``ANY`` needs a list so the database receives ``text[]``.
        components = ["events", "event_tags", "markets", "group_truth"]
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT receipt.component, receipt.artifact_key, receipt.artifact_digest
                   FROM m1_generation_manifests AS manifest
                   JOIN m1_structure_range_receipts AS receipt
                     ON receipt.job_key LIKE manifest.generation_key || ':normalize:%%'
                   WHERE manifest.generation_key = %s
                     AND receipt.component = ANY(%s)
                   ORDER BY receipt.component, receipt.artifact_key""",
                (generation_key, components),
            )
            return tuple(
                (str(row["component"]), str(row["artifact_key"]), str(row["artifact_digest"]))
                for row in cursor.fetchall()
            )

    def retire_superseded_structure_research_rows(self, *, generation_key: str) -> int:
        """Remove only unpublished Structure index rows before a safe backfill."""
        self._validate_nonempty(generation_key=generation_key)
        with self._connection_factory() as connection, connection.cursor() as cursor:
            _set_structure_read_timeouts(cursor, read_only=False)
            cursor.execute(
                """DELETE FROM m1_business_structure_rows AS research
                   WHERE research.generation_key <> %s
                     AND NOT EXISTS (
                         SELECT 1 FROM m1_generation_manifests AS manifest
                         WHERE manifest.generation_key = research.generation_key
                     )""",
                (generation_key,),
            )
            return cursor.rowcount

    def business_structure_page(
        self, *, generation_key: str | None, limit: int, after: str
    ) -> dict[str, object]:
        """Read one current, pointer-gated page of staged Structure research rows."""
        if not 1 <= limit <= 200 or len(after) > 256:
            raise ValueError("invalid-business-structure-page")
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT manifest.generation_key, manifest.record_count,
                          (SELECT count(*) FROM m1_business_structure_rows AS research
                           WHERE research.generation_key = manifest.generation_key)
                              AS indexed_record_count,
                          (SELECT COALESCE(sum(receipt.record_count), 0)
                           FROM m1_structure_range_receipts AS receipt
                           WHERE receipt.job_key LIKE manifest.generation_key || ':normalize:%%'
                             AND receipt.component IN ('events', 'group_truth'))
                              AS expected_indexed_record_count
                   FROM m1_generation_manifests AS manifest
                   WHERE manifest.generation_key LIKE 'structure:' || chr(37)
                   ORDER BY manifest.published_at DESC, manifest.generation_key DESC
                   LIMIT 1"""
            )
            pointer = cursor.fetchone()
            if pointer is None:
                return {"schema_version": "m1.business-research-page.v1", "product": "structure", "status": "not-published", "reason_code": "structure-not-published", "items": [], "limit": limit, "next_after": None}
            current = str(pointer["generation_key"])
            if generation_key is not None and generation_key != current:
                return {"schema_version": "m1.business-research-page.v1", "product": "structure", "status": "unavailable", "reason_code": "generation-not-current", "items": [], "limit": limit, "next_after": None}
            source_record_count = int(pointer["record_count"])
            indexed_record_count = int(pointer["indexed_record_count"])
            expected_indexed_record_count = int(pointer["expected_indexed_record_count"])
            if indexed_record_count < expected_indexed_record_count:
                return {
                    "schema_version": "m1.business-research-page.v1",
                    "product": "structure",
                    "status": "unavailable",
                    "reason_code": "research-index-incomplete",
                    "generation_key": current,
                    "source_record_count": source_record_count,
                    "indexed_record_count": indexed_record_count,
                    "expected_indexed_record_count": expected_indexed_record_count,
                    "items": [],
                    "limit": limit,
                    "next_after": None,
                }
            cursor.execute(
                """SELECT entity_id, payload FROM m1_business_structure_rows
                   WHERE generation_key=%s AND entity_id > %s ORDER BY entity_id LIMIT %s""",
                (current, after, limit + 1),
            )
            rows = cursor.fetchall()
            has_more = len(rows) > limit
            page = rows[:limit]
            return {
                "schema_version": "m1.business-research-page.v1",
                "product": "structure",
                "status": "available",
                "generation_key": current,
                "source_record_count": source_record_count,
                "indexed_record_count": indexed_record_count,
                "items": [
                    {"entity_id": str(row["entity_id"]), **dict(row["payload"])}
                    for row in page
                ],
                "limit": limit,
                "next_after": str(page[-1]["entity_id"]) if has_more else None,
            }

    def stage_structure_intelligence(
        self,
        *,
        generation_key: str,
        events: Sequence[tuple[str, Mapping[str, object]]],
        groups: Sequence[tuple[str, Mapping[str, object]]],
        summary: Mapping[str, object],
    ) -> None:
        """Write a bounded Structure business projection for one immutable generation.

        This is deliberately separate from ``m1_business_structure_rows``.  That
        table is a recovery-oriented source index; these relations are the
        operator-facing event and structural-risk projections.
        """
        self._validate_nonempty(generation_key=generation_key)
        prepared_events = [
            (
                generation_key,
                event_id,
                _optional_int(payload.get("end_time_ms")),
                _optional_bool(payload.get("is_open")),
                Jsonb(dict(payload)),
                _bounded_json_octets(payload, maximum=4096),
            )
            for event_id, payload in events
        ]
        prepared_groups = [
            (
                generation_key,
                group_id,
                _optional_text(payload.get("event_id")),
                _optional_text(payload.get("quality")),
                Jsonb(dict(payload)),
                _bounded_json_octets(payload, maximum=4096),
            )
            for group_id, payload in groups
        ]
        for event_id, _payload in events:
            self._validate_nonempty(event_id=event_id)
        for group_id, _payload in groups:
            self._validate_nonempty(group_id=group_id)
        summary_octets = _bounded_json_octets(summary, maximum=4096)
        with self._connection_factory() as connection, connection.cursor() as cursor:
            _set_structure_read_timeouts(cursor, read_only=False)
            # A generation is observable only when its replacement projection
            # and summary commit together.  Clearing it inside this transaction
            # also removes entities that disappeared from the next rebuild.
            cursor.execute(
                "DELETE FROM m1_structure_intelligence_events WHERE generation_key=%s",
                (generation_key,),
            )
            cursor.execute(
                "DELETE FROM m1_structure_intelligence_groups WHERE generation_key=%s",
                (generation_key,),
            )
            cursor.execute(
                "DELETE FROM m1_structure_intelligence_summaries WHERE generation_key=%s",
                (generation_key,),
            )
            if prepared_events:
                cursor.executemany(
                    """INSERT INTO m1_structure_intelligence_events(
                           generation_key,event_id,sort_end_time_ms,is_open,payload,payload_octets)
                       VALUES (%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (generation_key,event_id) DO UPDATE
                       SET sort_end_time_ms=EXCLUDED.sort_end_time_ms,
                           is_open=EXCLUDED.is_open, payload=EXCLUDED.payload,
                           payload_octets=EXCLUDED.payload_octets""",
                    prepared_events,
                )
            if prepared_groups:
                cursor.executemany(
                    """INSERT INTO m1_structure_intelligence_groups(
                           generation_key,group_id,event_id,quality,payload,payload_octets)
                       VALUES (%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (generation_key,group_id) DO UPDATE
                       SET event_id=EXCLUDED.event_id, quality=EXCLUDED.quality,
                           payload=EXCLUDED.payload, payload_octets=EXCLUDED.payload_octets""",
                    prepared_groups,
                )
            cursor.execute(
                """INSERT INTO m1_structure_intelligence_summaries(
                       generation_key,payload,payload_octets)
                   VALUES (%s,%s,%s)
                   ON CONFLICT (generation_key) DO UPDATE
                   SET payload=EXCLUDED.payload, payload_octets=EXCLUDED.payload_octets""",
                (generation_key, Jsonb(dict(summary)), summary_octets),
            )

    def structure_intelligence_summary(self, *, generation_key: str | None) -> dict[str, object]:
        """Return the current, fully materialized Structure business summary."""
        current = self._current_structure_generation(generation_key=generation_key)
        if isinstance(current, dict):
            return current
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                "SELECT payload FROM m1_structure_intelligence_summaries WHERE generation_key=%s",
                (current,),
            )
            row = cursor.fetchone()
        if row is None:
            return _structure_intelligence_unavailable(current, "structure-intelligence-incomplete")
        return {"schema_version": "m1.structure-intelligence.v1", "status": "available", "generation_key": current, **dict(row["payload"])}

    def structure_intelligence_events(
        self, *, generation_key: str | None, limit: int, after: str, open_only: bool | None
    ) -> dict[str, object]:
        if not 1 <= limit <= 200 or len(after) > 256:
            raise ValueError("invalid-structure-intelligence-page")
        current = self._current_structure_generation(generation_key=generation_key)
        if isinstance(current, dict):
            return current
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                "SELECT 1 FROM m1_structure_intelligence_summaries WHERE generation_key=%s", (current,)
            )
            if cursor.fetchone() is None:
                return _structure_intelligence_unavailable(current, "structure-intelligence-incomplete")
            now_ms = int(datetime.now(UTC).timestamp() * 1_000)
            cursor.execute(
                """SELECT event_id, payload FROM m1_structure_intelligence_events
                   WHERE generation_key=%s AND event_id > %s
                     AND (
                         %s::boolean IS NULL
                         OR (is_open IS TRUE AND sort_end_time_ms > %s)
                     )
                   ORDER BY event_id LIMIT %s""",
                (current, after, open_only, now_ms, limit + 1),
            )
            rows = cursor.fetchall()
        return _structure_intelligence_page(current, "events", rows, "event_id", limit)

    def structure_intelligence_groups(
        self, *, generation_key: str | None, limit: int, after: str, quality: str | None
    ) -> dict[str, object]:
        if not 1 <= limit <= 200 or len(after) > 256 or (quality is not None and len(quality) > 128):
            raise ValueError("invalid-structure-intelligence-page")
        current = self._current_structure_generation(generation_key=generation_key)
        if isinstance(current, dict):
            return current
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                "SELECT 1 FROM m1_structure_intelligence_summaries WHERE generation_key=%s", (current,)
            )
            if cursor.fetchone() is None:
                return _structure_intelligence_unavailable(current, "structure-intelligence-incomplete")
            cursor.execute(
                """SELECT group_id, payload FROM m1_structure_intelligence_groups
                   WHERE generation_key=%s AND group_id > %s
                     AND (%s::text IS NULL OR quality=%s)
                   ORDER BY group_id LIMIT %s""",
                (current, after, quality, quality, limit + 1),
            )
            rows = cursor.fetchall()
        return _structure_intelligence_page(current, "groups", rows, "group_id", limit)

    def _current_structure_generation(self, *, generation_key: str | None) -> str | dict[str, object]:
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT generation_key FROM m1_generation_manifests
                   WHERE generation_key LIKE 'structure:' || chr(37)
                   ORDER BY published_at DESC, generation_key DESC LIMIT 1"""
            )
            row = cursor.fetchone()
        if row is None:
            return _structure_intelligence_unavailable(None, "structure-not-published")
        current = str(row["generation_key"])
        if generation_key is not None and generation_key != current:
            return _structure_intelligence_unavailable(current, "generation-not-current")
        return current

    def stage_business_quote_rows(
        self, *, generation_key: str, rows: Sequence[tuple[str, Mapping[str, object]]]
    ) -> None:
        """Stage bounded normalized Quote rows; publication remains pointer-gated."""
        self._validate_nonempty(generation_key=generation_key)
        with self._connection_factory() as connection, connection.cursor() as cursor:
            _set_structure_read_timeouts(cursor, read_only=False)
            for token_id, payload in rows:
                self._validate_nonempty(token_id=token_id)
                cursor.execute(
                    """INSERT INTO m1_business_quote_rows(generation_key, token_id, payload)
                       VALUES (%s,%s,%s) ON CONFLICT (generation_key, token_id) DO NOTHING""",
                    (generation_key, token_id, Jsonb(dict(payload))),
                )

    def analysis_candidate_sources(
        self, *, generation_key: str | None, after_group_id: str = "", limit: int = 500
    ) -> dict[str, object]:
        """Read current, same-lineage group inputs for bounded analysis materialization."""
        if not 1 <= limit <= 500 or len(after_group_id) > 256:
            raise ValueError("invalid-analysis-candidate-source-page")
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT pointer.generation_key, input.structure_generation_key
                     FROM m1_publication_pointers AS pointer
                     JOIN m1_quote_generation_inputs AS input
                       ON input.generation_key = pointer.generation_key
                    WHERE pointer.pointer_key = 'quote:current'"""
            )
            pointer = cursor.fetchone()
            if pointer is None:
                return {"status": "not-published", "reason_code": "quote-not-published", "items": []}
            quote_generation_key = str(pointer["generation_key"])
            structure_generation_key = str(pointer["structure_generation_key"])
            if generation_key is not None and generation_key != quote_generation_key:
                return {
                    "status": "unavailable",
                    "reason_code": "generation-not-current",
                    "generation_key": quote_generation_key,
                    "items": [],
                }
            cursor.execute(
                """SELECT generation_key FROM m1_generation_manifests
                     WHERE generation_key LIKE 'structure:' || chr(37)
                     ORDER BY published_at DESC, generation_key DESC LIMIT 1"""
            )
            current_structure = cursor.fetchone()
            if current_structure is None or str(current_structure["generation_key"]) != structure_generation_key:
                return {
                    "status": "lagging",
                    "reason_code": "quote-lineage-lagging",
                    "generation_key": quote_generation_key,
                    "parent_structure_generation_key": structure_generation_key,
                    "items": [],
                }
            cursor.execute(
                """WITH selected_groups AS (
                       SELECT generation_key, group_id, event_id, payload
                         FROM m1_structure_intelligence_groups
                        WHERE generation_key = %s AND group_id > %s
                        ORDER BY group_id ASC
                        LIMIT %s
                     )
                     SELECT groups.group_id, groups.payload AS group_payload,
                          events.payload AS event_payload,
                          COALESCE(jsonb_agg(quotes.payload) FILTER (WHERE quotes.token_id IS NOT NULL),
                                   '[]'::jsonb) AS quote_payloads
                     FROM selected_groups AS groups
                     LEFT JOIN m1_structure_intelligence_events AS events
                       ON events.generation_key = groups.generation_key
                      AND events.event_id = groups.event_id
                     LEFT JOIN m1_business_quote_rows AS quotes
                       ON quotes.generation_key = %s
                      AND quotes.payload->>'neg_risk_market_id' = groups.group_id
                    GROUP BY groups.group_id, groups.payload, events.payload
                    ORDER BY groups.group_id ASC""",
                (structure_generation_key, after_group_id, limit + 1, quote_generation_key),
            )
            rows = cursor.fetchall()
        has_more = len(rows) > limit
        page = rows[:limit]
        return {
            "status": "available",
            "generation_key": quote_generation_key,
            "parent_structure_generation_key": structure_generation_key,
            "items": [
                {
                    "group_id": str(row["group_id"]),
                    "group": dict(row["group_payload"]),
                    "event": {} if row["event_payload"] is None else dict(row["event_payload"]),
                    "quotes": tuple(row["quote_payloads"]),
                }
                for row in page
            ],
            "next_after_group_id": str(page[-1]["group_id"]) if has_more else None,
        }

    def stage_analysis_candidates(
        self,
        *,
        generation_key: str,
        structure_generation_key: str,
        rows: Sequence[Mapping[str, object]],
        now: datetime,
    ) -> None:
        """Atomically replace the current compact candidate projection only."""
        if len(rows) > 20_000:
            raise ValueError("analysis-candidate-projection-over-budget")
        normalized: list[tuple[str, str, float | None, Mapping[str, object], int]] = []
        seen_group_ids: set[str] = set()
        for row in rows:
            group_id = row.get("group_id")
            candidate_state = row.get("candidate_state")
            if not isinstance(group_id, str) or not group_id or not isinstance(candidate_state, str):
                raise ValueError("invalid-analysis-candidate-row")
            if group_id in seen_group_ids:
                raise ValueError("analysis-candidate-group-duplicate")
            seen_group_ids.add(group_id)
            payload = dict(row)
            gross_edge_bps = payload.get("gross_edge_bps")
            if not _finite_number(gross_edge_bps):
                gross_edge_bps = None
            normalized.append(
                (
                    group_id,
                    candidate_state,
                    None if gross_edge_bps is None else float(gross_edge_bps),
                    payload,
                    _bounded_json_octets(payload, maximum=2_048),
                )
            )
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=False)
            cursor.execute(
                """SELECT input.structure_generation_key
                     FROM m1_publication_pointers AS pointer
                     JOIN m1_quote_generation_inputs AS input
                       ON input.generation_key = pointer.generation_key
                    WHERE pointer.pointer_key = 'quote:current'
                      AND pointer.generation_key = %s""",
                (generation_key,),
            )
            pointer = cursor.fetchone()
            if pointer is None or str(pointer["structure_generation_key"]) != structure_generation_key:
                raise PublicationPointerConflictError("analysis candidates require current quote lineage")
            cursor.execute("DELETE FROM m1_analysis_candidate_rows WHERE generation_key <> %s", (generation_key,))
            cursor.execute("DELETE FROM m1_analysis_candidate_projections WHERE generation_key <> %s", (generation_key,))
            cursor.execute("DELETE FROM m1_analysis_candidate_rows WHERE generation_key = %s", (generation_key,))
            cursor.execute("DELETE FROM m1_analysis_candidate_projections WHERE generation_key = %s", (generation_key,))
            cursor.execute(
                """INSERT INTO m1_analysis_candidate_projections
                   (generation_key, structure_generation_key, record_count, positive_edge_count, materialized_at)
                   VALUES (%s,%s,%s,%s,%s)""",
                (
                    generation_key,
                    structure_generation_key,
                    len(normalized),
                    sum(candidate_state == "positive-edge" for _group, candidate_state, _edge, _payload, _octets in normalized),
                    now,
                ),
            )
            cursor.executemany(
                """INSERT INTO m1_analysis_candidate_rows
                   (generation_key, group_id, candidate_state, gross_edge_bps, payload, payload_octets)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                [
                    (
                        generation_key,
                        group_id,
                        candidate_state,
                        gross_edge_bps,
                        Jsonb(dict(payload)),
                        payload_octets,
                    )
                    for group_id, candidate_state, gross_edge_bps, payload, payload_octets in normalized
                ],
            )

    def business_analysis_page(
        self, *, generation_key: str | None, limit: int, after: str
    ) -> dict[str, object]:
        """Read the current bounded group candidate/rejection funnel."""
        if not 1 <= limit <= 200 or after:
            raise ValueError("invalid-business-analysis-page")
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT pointer.generation_key, input.structure_generation_key,
                          projection.record_count, projection.positive_edge_count,
                          projection.materialized_at
                     FROM m1_publication_pointers AS pointer
                     JOIN m1_quote_generation_inputs AS input
                       ON input.generation_key = pointer.generation_key
                     LEFT JOIN m1_analysis_candidate_projections AS projection
                       ON projection.generation_key = pointer.generation_key
                      AND projection.structure_generation_key = input.structure_generation_key
                    WHERE pointer.pointer_key = 'quote:current'"""
            )
            projection = cursor.fetchone()
            if projection is None:
                return {
                    "schema_version": "m1.business-research-page.v1",
                    "product": "analysis",
                    "status": "not-published",
                    "reason_code": "quote-not-published",
                    "items": [],
                    "limit": limit,
                    "next_after": None,
                }
            current = str(projection["generation_key"])
            if generation_key is not None and generation_key != current:
                return {
                    "schema_version": "m1.business-research-page.v1",
                    "product": "analysis",
                    "status": "unavailable",
                    "reason_code": "generation-not-current",
                    "items": [],
                    "limit": limit,
                    "next_after": None,
                }
            if projection["record_count"] is None:
                return {
                    "schema_version": "m1.business-research-page.v1",
                    "product": "analysis",
                    "status": "not-published",
                    "reason_code": "candidate-detail-not-published",
                    "generation_key": current,
                    "parent_structure_generation_key": str(projection["structure_generation_key"]),
                    "items": [],
                    "limit": limit,
                    "next_after": None,
                }
            cursor.execute(
                """SELECT candidate_state, count(*) AS count
                     FROM m1_analysis_candidate_rows
                    WHERE generation_key = %s
                    GROUP BY candidate_state""",
                (current,),
            )
            state_counts = {str(row["candidate_state"]): int(row["count"]) for row in cursor.fetchall()}
            cursor.execute(
                """SELECT payload FROM m1_analysis_candidate_rows
                    WHERE generation_key = %s
                    ORDER BY CASE candidate_state
                               WHEN 'positive-edge' THEN 0
                               WHEN 'no-edge' THEN 1
                               WHEN 'incomplete-coverage' THEN 2
                               WHEN 'expired-or-closed' THEN 3
                               ELSE 4 END,
                             CASE WHEN candidate_state = 'positive-edge'
                                  AND gross_edge_bps > 0
                                  AND (
                                      (payload->>'bundle_cost') ~ '^[0-9]+(\\.[0-9]+)?$'
                                      AND (payload->>'max_bundle_size') ~ '^[0-9]+(\\.[0-9]+)?$'
                                  )
                                  THEN (1 - (payload->>'bundle_cost')::double precision)
                                       * (payload->>'max_bundle_size')::double precision
                                  END DESC NULLS LAST,
                             gross_edge_bps DESC NULLS LAST, group_id ASC
                    LIMIT %s""",
                (current, limit),
            )
            items = [dict(row["payload"]) for row in cursor.fetchall()]
            for item in items:
                _candidate_display_economics(item)
        return {
            "schema_version": "m1.business-research-page.v1",
            "product": "analysis",
            "status": "available",
            "generation_key": current,
            "parent_structure_generation_key": str(projection["structure_generation_key"]),
            "summary": {
                "record_count": int(projection["record_count"]),
                "positive_edge_count": int(projection["positive_edge_count"]),
                "state_counts": state_counts,
                "materialized_at": _snapshot_aware(
                    projection["materialized_at"], "business_analysis.materialized_at"
                ).isoformat(),
            },
            "items": items,
            "limit": limit,
            "next_after": None,
        }

    def business_quote_coverage_page(
        self, *, generation_key: str | None, limit: int, after: str
    ) -> dict[str, object]:
        """Read current active group quote coverage without price-based discovery ranking."""
        if not 1 <= limit <= 200 or after:
            raise ValueError("invalid-quote-coverage-page")
        now_ms = int(datetime.now(UTC).timestamp() * 1_000)
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT pointer.generation_key, input.structure_generation_key,
                          projection.record_count, projection.materialized_at
                     FROM m1_publication_pointers AS pointer
                     JOIN m1_quote_generation_inputs AS input
                       ON input.generation_key = pointer.generation_key
                     LEFT JOIN m1_analysis_candidate_projections AS projection
                       ON projection.generation_key = pointer.generation_key
                      AND projection.structure_generation_key = input.structure_generation_key
                    WHERE pointer.pointer_key = 'quote:current'"""
            )
            projection = cursor.fetchone()
            if projection is None:
                return _quote_coverage_unavailable("not-published", "quote-not-published", limit)
            current = str(projection["generation_key"])
            if generation_key is not None and generation_key != current:
                return _quote_coverage_unavailable("unavailable", "generation-not-current", limit, current)
            if projection["record_count"] is None:
                return _quote_coverage_unavailable(
                    "not-published", "candidate-detail-not-published", limit, current
                )
            cursor.execute(
                """SELECT candidate_state, count(*) AS count
                     FROM m1_analysis_candidate_rows
                    WHERE generation_key = %s
                      AND payload->'event'->>'is_open' = 'true'
                      AND (payload->'event'->>'end_time_ms') ~ '^[0-9]+$'
                      AND (payload->'event'->>'end_time_ms')::bigint > %s
                    GROUP BY candidate_state""",
                (current, now_ms),
            )
            total_state_counts = {
                str(row["candidate_state"]): int(row["count"]) for row in cursor.fetchall()
            }
            cursor.execute(
                """WITH active_groups AS (
                       SELECT group_id, candidate_state, payload,
                              CASE candidate_state
                                  WHEN 'incomplete-coverage' THEN 0
                                  WHEN 'positive-edge' THEN 1
                                  WHEN 'no-edge' THEN 2
                                  ELSE 3 END AS priority
                         FROM m1_analysis_candidate_rows
                        WHERE generation_key = %s
                          AND payload->'event'->>'is_open' = 'true'
                          AND (payload->'event'->>'end_time_ms') ~ '^[0-9]+$'
                          AND (payload->'event'->>'end_time_ms')::bigint > %s
                     )
                     SELECT group_id, candidate_state, payload, priority
                       FROM active_groups
                      ORDER BY priority ASC,
                               GREATEST(
                                 COALESCE((payload->>'expected_member_count')::integer, 0)
                                 - COALESCE((payload->>'quoted_member_count')::integer, 0), 0
                               ) DESC,
                               (payload->'event'->>'end_time_ms')::bigint ASC,
                               group_id ASC
                      LIMIT %s""",
                (current, now_ms, limit),
            )
            rows = cursor.fetchall()
        items = [_quote_coverage_item(dict(row["payload"]), str(row["candidate_state"])) for row in rows]
        state_counts = {
            "coverage-gap": total_state_counts.get("incomplete-coverage", 0),
            "analysis-ready": total_state_counts.get("positive-edge", 0),
            "healthy": total_state_counts.get("no-edge", 0),
            "needs-context": total_state_counts.get("context-unavailable", 0),
        }
        return {
            "schema_version": "m1.quote-coverage-page.v1",
            "status": "available",
            "generation_key": current,
            "parent_structure_generation_key": str(projection["structure_generation_key"]),
            "summary": {
                "active_group_count": sum(total_state_counts.values()),
                "state_counts": state_counts,
                "materialized_at": _snapshot_aware(
                    projection["materialized_at"], "quote_coverage.materialized_at"
                ).isoformat(),
            },
            "items": items,
            "limit": limit,
            "next_after": None,
        }

    def business_event_detail(
        self, *, event_id: str, focus_group_id: str | None, observed_generation: str | None
    ) -> dict[str, object]:
        """Read one operational event and its bounded same-lineage research facts."""
        now_ms = int(datetime.now(UTC).timestamp() * 1_000)
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT pointer.generation_key, input.structure_generation_key,
                          projection.materialized_at
                     FROM m1_publication_pointers AS pointer
                     JOIN m1_quote_generation_inputs AS input ON input.generation_key=pointer.generation_key
                     LEFT JOIN m1_analysis_candidate_projections AS projection
                       ON projection.generation_key=pointer.generation_key
                      AND projection.structure_generation_key=input.structure_generation_key
                    WHERE pointer.pointer_key='quote:current'"""
            )
            anchor = cursor.fetchone()
            if anchor is None:
                return _event_research_unavailable("not-published", "quote-not-published", event_id)
            quote_generation = str(anchor["generation_key"])
            structure_generation = str(anchor["structure_generation_key"])
            if anchor["materialized_at"] is None:
                return _event_research_unavailable("not-published", "candidate-detail-not-published", event_id)
            cursor.execute(
                """SELECT payload FROM m1_structure_intelligence_events
                   WHERE generation_key=%s AND event_id=%s
                     AND is_open IS TRUE AND sort_end_time_ms > %s""",
                (structure_generation, event_id, now_ms),
            )
            event_row = cursor.fetchone()
            if event_row is None:
                return _event_research_unavailable("unavailable", "event-not-operational", event_id)
            cursor.execute(
                """WITH event_groups AS (
                       SELECT group_id, event_id, payload
                         FROM m1_structure_intelligence_groups
                        WHERE generation_key=%s AND event_id=%s
                     ), quote_counts AS (
                       SELECT groups.group_id,
                              count(DISTINCT quotes.token_id) AS observed,
                              count(DISTINCT quotes.token_id) FILTER (WHERE quotes.payload->>'terminal_state'='executable') AS executable,
                              count(DISTINCT quotes.token_id) FILTER (WHERE quotes.payload->>'terminal_state'<>'executable') AS non_executable
                         FROM event_groups AS groups
                         LEFT JOIN m1_business_quote_rows AS quotes
                           ON quotes.generation_key=%s
                          AND quotes.payload->>'neg_risk_market_id'=groups.group_id
                          AND quotes.payload->>'event_id'=groups.event_id
                        GROUP BY groups.group_id
                     )
                     SELECT groups.group_id, groups.payload AS group_payload,
                            candidates.candidate_state, candidates.payload AS candidate_payload,
                            COALESCE(counts.observed, 0) AS observed,
                            COALESCE(counts.executable, 0) AS executable,
                            COALESCE(counts.non_executable, 0) AS non_executable
                       FROM event_groups AS groups
                       LEFT JOIN m1_analysis_candidate_rows AS candidates
                         ON candidates.generation_key=%s AND candidates.group_id=groups.group_id
                       LEFT JOIN quote_counts AS counts ON counts.group_id=groups.group_id
                      ORDER BY CASE candidates.candidate_state WHEN 'positive-edge' THEN 0
                                WHEN 'incomplete-coverage' THEN 1 WHEN 'context-unavailable' THEN 2
                                WHEN 'no-edge' THEN 3 ELSE 4 END,
                               CASE WHEN candidates.candidate_state='positive-edge'
                                      AND (candidates.payload->>'bundle_cost') ~ '^[0-9]+(\\.[0-9]+)?$'
                                      AND (candidates.payload->>'max_bundle_size') ~ '^[0-9]+(\\.[0-9]+)?$'
                                    THEN (1-(candidates.payload->>'bundle_cost')::double precision)
                                         *(candidates.payload->>'max_bundle_size')::double precision END DESC NULLS LAST,
                               CASE WHEN candidates.candidate_state='positive-edge'
                                      AND (candidates.payload->>'bundle_cost') ~ '^[0-9]+(\\.[0-9]+)?$'
                                      AND (candidates.payload->>'max_bundle_size') ~ '^[0-9]+(\\.[0-9]+)?$'
                                      AND (candidates.payload->>'bundle_cost')::double precision > 0
                                    THEN ((1-(candidates.payload->>'bundle_cost')::double precision)
                                          /(candidates.payload->>'bundle_cost')::double precision) END DESC NULLS LAST,
                               CASE WHEN candidates.candidate_state='incomplete-coverage' THEN
                                 GREATEST(COALESCE((groups.payload->>'expected_member_count')::integer, 0)-COALESCE(counts.observed, 0), 0) END ASC NULLS LAST,
                               groups.group_id ASC
                      LIMIT 200""",
                (structure_generation, event_id, quote_generation, quote_generation),
            )
            rows = cursor.fetchall()
        groups: list[dict[str, object]] = []
        state_counts: dict[str, int] = {}
        for row in rows:
            group = dict(row["group_payload"])
            candidate = {} if row["candidate_payload"] is None else dict(row["candidate_payload"])
            state = str(row["candidate_state"] or "context-unavailable")
            state_counts[state] = state_counts.get(state, 0) + 1
            candidate["candidate_state"] = state
            _candidate_display_economics(candidate)
            expected = _optional_int(group.get("expected_member_count"))
            observed, executable, non_executable = (
                int(row["observed"]), int(row["executable"]), int(row["non_executable"])
            )
            groups.append({
                "group_id": str(row["group_id"]), "structure": group,
                "candidate_state": state, "candidate": candidate,
                "quote_coverage": {
                    "expected": expected,
                    "observed": observed,
                    "executable": executable,
                    "non_executable": non_executable,
                    "missing": None if expected is None else max(expected - observed, 0),
                },
            })
        focus = next((group for group in groups if group["group_id"] == focus_group_id), None)
        coverages = [cast(dict[str, object], group["quote_coverage"]) for group in groups]
        coverage_totals: dict[str, object] = {}
        for key in ("observed", "executable", "non_executable", "missing"):
            coverage_totals[key] = sum(
                value
                for coverage in coverages
                if isinstance(value := coverage[key], int)
            )
        expected_totals = [coverage["expected"] for coverage in coverages]
        coverage_totals["expected"] = (
            sum(int(expected) for expected in expected_totals if isinstance(expected, int))
            if any(isinstance(expected, int) for expected in expected_totals)
            else None
        )
        if state_counts.get("positive-edge", 0):
            research_stage = "ready-for-analysis"
        elif state_counts.get("incomplete-coverage", 0):
            research_stage = "repair-coverage"
        elif state_counts.get("context-unavailable", 0):
            research_stage = "structure-context-unavailable"
        else:
            research_stage = "no-positive-group-edge"
        return {
            "schema_version": "m1.event-research-detail.v1", "status": "available", "event_id": event_id,
            "anchor": {"quote_generation_key": quote_generation, "structure_generation_key": structure_generation,
                       "changed_since_entry": observed_generation is not None and observed_generation != quote_generation,
                       "materialized_at": _snapshot_aware(anchor["materialized_at"], "event_research.materialized_at").isoformat()},
            "event": dict(event_row["payload"]), "state_counts": state_counts,
            "research_stage": research_stage,
            "blockers": [
                {"code": "coverage-gap", "count": state_counts.get("incomplete-coverage", 0)}
            ]
            if state_counts.get("incomplete-coverage", 0)
            else [],
            "structure": {"generation_key": structure_generation, "group_count": len(groups)},
            "quote_coverage": coverage_totals,
            "analysis": {"state_counts": state_counts, "research_only": True},
            "groups": groups, "focused_group": focus,
            "cautions": ["Fees, slippage, simultaneous execution, resolution, and settlement delay are not assessed."],
        }

    def business_event_group_legs(
        self, *, event_id: str, group_id: str, limit: int, after: str
    ) -> dict[str, object]:
        """Read a stable bounded page of compact evidence for an event-owned group."""
        if not 1 <= limit <= 200 or len(after) > 256:
            raise ValueError("invalid-event-research-group-legs")
        now_ms = int(datetime.now(UTC).timestamp() * 1_000)
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT pointer.generation_key, input.structure_generation_key
                     FROM m1_publication_pointers AS pointer
                     JOIN m1_quote_generation_inputs AS input ON input.generation_key=pointer.generation_key
                    WHERE pointer.pointer_key='quote:current'"""
            )
            anchor = cursor.fetchone()
            if anchor is None:
                return _event_research_legs_unavailable("not-published", "quote-not-published", event_id, group_id, limit)
            quote_generation, structure_generation = str(anchor["generation_key"]), str(anchor["structure_generation_key"])
            cursor.execute(
                """SELECT 1 FROM m1_structure_intelligence_events
                    WHERE generation_key=%s AND event_id=%s AND is_open IS TRUE AND sort_end_time_ms>%s""",
                (structure_generation, event_id, now_ms),
            )
            if cursor.fetchone() is None:
                return _event_research_legs_unavailable("unavailable", "event-not-operational", event_id, group_id, limit)
            cursor.execute(
                """SELECT 1 FROM m1_structure_intelligence_groups
                    WHERE generation_key=%s AND event_id=%s AND group_id=%s""",
                (structure_generation, event_id, group_id),
            )
            if cursor.fetchone() is None:
                return _event_research_legs_unavailable("unavailable", "group-not-event-owned", event_id, group_id, limit)
            cursor.execute(
                """SELECT token_id, payload FROM m1_business_quote_rows
                    WHERE generation_key=%s AND token_id>%s
                      AND payload->>'event_id'=%s AND payload->>'neg_risk_market_id'=%s
                    ORDER BY token_id ASC LIMIT %s""",
                (quote_generation, after, event_id, group_id, limit + 1),
            )
            rows = cursor.fetchall()
        page, has_more = rows[:limit], len(rows) > limit
        return {
            "schema_version": "m1.event-research-group-legs.v1",
            "status": "available", "event_id": event_id, "group_id": group_id,
            "legs": [
                {"token_id": str(row["token_id"]), "market_id": dict(row["payload"]).get("market_id"),
                 "best_ask_price": dict(row["payload"]).get("best_ask_price"),
                 "best_ask_size": dict(row["payload"]).get("best_ask_size"),
                 "terminal_state": dict(row["payload"]).get("terminal_state")}
                for row in page
            ],
            "limit": limit,
            "next_after": str(page[-1]["token_id"]) if has_more else None,
            "caution": "Top-of-book evidence does not prove simultaneous multi-leg execution.",
        }

    def business_quote_page(
        self, *, generation_key: str | None, limit: int, after: str
    ) -> dict[str, object]:
        """Read one current, pointer-gated page of staged Quote research rows."""
        if not 1 <= limit <= 200 or len(after) > 256:
            raise ValueError("invalid-business-quote-page")
        cursor_position = decode_discovery_cursor(after)
        if after and cursor_position is None:
            raise ValueError("invalid-business-quote-page")
        after_score, after_notional, after_token = cursor_position or (0.0, 0.0, "")
        with self._connection_factory() as connection, connection.cursor(row_factory=dict_row) as cursor:
            _set_structure_read_timeouts(cursor, read_only=True)
            cursor.execute(
                """SELECT pointer.generation_key, manifest.record_count,
                          input.structure_generation_key,
                          COALESCE((
                              SELECT sum(batch_input.leg_count)
                              FROM m1_quote_batch_inputs AS batch_input
                              WHERE batch_input.job_key LIKE pointer.generation_key || ':batch:%'
                          ), manifest.record_count) AS expected_research_record_count,
                          (SELECT count(*) FROM m1_business_quote_rows AS research
                           WHERE research.generation_key = pointer.generation_key)
                              AS materialized_record_count
                   FROM m1_publication_pointers pointer
                   JOIN m1_generation_manifests manifest ON manifest.generation_key = pointer.generation_key
                   LEFT JOIN m1_quote_generation_inputs input ON input.generation_key = pointer.generation_key
                   WHERE pointer.pointer_key='quote:current'"""
            )
            pointer = cursor.fetchone()
            if pointer is None:
                return {"schema_version": "m1.business-research-page.v1", "product": "quote", "status": "not-published", "reason_code": "quote-not-published", "items": [], "limit": limit, "next_after": None}
            current = str(pointer["generation_key"])
            if generation_key is not None and generation_key != current:
                return {"schema_version": "m1.business-research-page.v1", "product": "quote", "status": "unavailable", "reason_code": "generation-not-current", "items": [], "limit": limit, "next_after": None}
            expected_record_count = int(pointer["expected_research_record_count"])
            materialized_record_count = int(pointer["materialized_record_count"])
            if materialized_record_count != expected_record_count:
                return {
                    "schema_version": "m1.business-research-page.v1",
                    "product": "quote",
                    "status": "unavailable",
                    "reason_code": "research-index-incomplete",
                    "generation_key": current,
                    "expected_record_count": expected_record_count,
                    "materialized_record_count": materialized_record_count,
                    "items": [],
                    "limit": limit,
                    "next_after": None,
                }
            cursor.execute(
                """WITH quote_source AS (
                       SELECT research.token_id, research.payload,
                              event_projection.payload AS event_payload,
                              group_projection.payload AS group_payload,
                              CASE WHEN research.payload->>'terminal_state' = 'executable'
                                      AND research.payload->>'best_ask_price' ~ '^[0-9]+(\\.[0-9]+)?$'
                                      AND research.payload->>'best_ask_size' ~ '^[0-9]+(\\.[0-9]+)?$'
                                   THEN (research.payload->>'best_ask_price')::double precision
                                   ELSE NULL END AS ask_price,
                              CASE WHEN research.payload->>'terminal_state' = 'executable'
                                      AND research.payload->>'best_ask_price' ~ '^[0-9]+(\\.[0-9]+)?$'
                                      AND research.payload->>'best_ask_size' ~ '^[0-9]+(\\.[0-9]+)?$'
                                   THEN (research.payload->>'best_ask_size')::double precision
                                   ELSE NULL END AS ask_size
                         FROM m1_business_quote_rows AS research
                    LEFT JOIN m1_structure_intelligence_events AS event_projection
                           ON event_projection.generation_key=%s
                          AND event_projection.event_id=research.payload->>'event_id'
                    LEFT JOIN m1_structure_intelligence_groups AS group_projection
                           ON group_projection.generation_key=%s
                          AND group_projection.group_id=research.payload->>'neg_risk_market_id'
                        WHERE research.generation_key=%s
                     ), ranked AS (
                       SELECT *,
                              CASE WHEN ask_price BETWEEN 0 AND 1 AND ask_size > 0
                                   THEN ask_price * ask_size ELSE 0 END AS executable_notional_usd,
                              CASE WHEN ask_price BETWEEN 0 AND 1 AND ask_size > 0
                                   THEN abs(ask_price - 0.5) * 10000 ELSE 0 END AS price_extremity_bps
                         FROM quote_source
                     ), scored AS (
                       SELECT *, ln(1 + executable_notional_usd) * price_extremity_bps AS discovery_score
                         FROM ranked
                     )
                     SELECT token_id, payload, event_payload, group_payload,
                            executable_notional_usd, discovery_score
                       FROM scored
                      WHERE %s OR (discovery_score, executable_notional_usd, token_id)
                           < (%s, %s, %s)
                      ORDER BY discovery_score DESC, executable_notional_usd DESC, token_id ASC
                      LIMIT %s""",
                (
                    pointer["structure_generation_key"],
                    pointer["structure_generation_key"],
                    current,
                    cursor_position is None,
                    after_score,
                    after_notional,
                    after_token,
                    limit + 1,
                ),
            )
            rows = cursor.fetchall()
            has_more = len(rows) > limit
            page = rows[:limit]
            items = []
            for row in page:
                payload = dict(row["payload"])
                discovery = quote_discovery(payload)
                discovery["executable_notional_usd"] = round(
                    float(row["executable_notional_usd"]), 8
                )
                discovery["score"] = round(float(row["discovery_score"]), 8)
                items.append(
                    {
                        "token_id": str(row["token_id"]),
                        **payload,
                        "discovery": discovery,
                        "event_context": _quote_event_context(row["event_payload"]),
                        "neg_risk_context": _quote_neg_risk_context(
                            group_id=payload.get("neg_risk_market_id"),
                            value=row["group_payload"],
                        ),
                    }
                )
            next_after = None
            if has_more:
                final = page[-1]
                next_after = encode_discovery_cursor(
                    float(final["discovery_score"]),
                    float(final["executable_notional_usd"]),
                    str(final["token_id"]),
                )
            return {"schema_version": "m1.business-research-page.v1", "product": "quote", "status": "available", "generation_key": current, "items": items, "limit": limit, "next_after": next_after}

    def current_opportunities(self, *, limit: int, after_group_id: str) -> dict[str, object]:
        """Read one complete, atomically published opportunity projection."""
        if not 1 <= limit <= 500 or len(after_group_id) > 256 or "\x00" in after_group_id:
            raise ValueError("invalid-opportunity-page")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            query = sql.SQL(_CURRENT_OPPORTUNITIES_SQL).format(
                statement_timeout=sql.Literal(CONTROL_PLANE_DB_POLICY.statement_setting),
                lock_timeout=sql.Literal(CONTROL_PLANE_DB_POLICY.lock_setting),
                after_group_id=sql.Literal(after_group_id),
                page_size=sql.Literal(limit + 1),
            )
            cursor.execute(query)
            for setup_command in (
                "repeatable-read transaction",
                "statement deadline",
                "lock deadline",
            ):
                if not cursor.nextset():
                    raise ControlPlaneError(f"opportunity result missing after {setup_command}")
            projection = cursor.fetchone()
            if projection is None:
                raise ControlPlaneError("opportunity-projection-unavailable")
            rows = _snapshot_rows(projection["rows"], "opportunity.rows")
        has_more = len(rows) > limit
        page = rows[:limit]
        return {
            "status": "available",
            "current_opportunity_count": _snapshot_int(
                projection["record_count"], "opportunity.record_count"
            ),
            "items": [
                {
                    "group_id": _snapshot_text(row["group_id"], "opportunity.group_id"),
                    "event_id": _snapshot_text(row["event_id"], "opportunity.event_id"),
                    "membership_hash": _snapshot_text(
                        row["membership_hash"], "opportunity.membership_hash"
                    ),
                    "bundle_cost": _snapshot_seconds(row["bundle_cost"], "opportunity.bundle_cost"),
                    "gross_edge_bps": _snapshot_seconds(
                        row["gross_edge_bps"], "opportunity.gross_edge_bps"
                    ),
                    "max_bundle_size": _snapshot_seconds(
                        row["max_bundle_size"], "opportunity.max_bundle_size"
                    ),
                    "legs": _snapshot_json_array(row["legs"], "opportunity.legs"),
                    "structure_observed_at_ms": _snapshot_int(
                        row["structure_observed_at_ms"], "opportunity.structure_observed_at_ms"
                    ),
                    "quote_started_at_ms": _snapshot_int(
                        row["quote_started_at_ms"], "opportunity.quote_started_at_ms"
                    ),
                    "quote_quoted_at_ms": _snapshot_int(
                        row["quote_quoted_at_ms"], "opportunity.quote_quoted_at_ms"
                    ),
                }
                for row in page
            ],
            "limit": limit,
            "next_after_group_id": (
                _snapshot_text(page[-1]["group_id"], "opportunity.group_id") if has_more else None
            ),
        }

    def publish_opportunity_projection(
        self,
        *,
        quote_generation_key: str,
        structure_generation_key: str,
        rows: Sequence[Mapping[str, object]],
        now: datetime,
        lease: JobLease | None = None,
    ) -> str:
        """Atomically publish one complete, already-authenticated projection.

        Workers pass their live ``opportunity-certify`` lease so the pointer,
        projection rows, runtime success event, and terminal job transition are
        one transaction.  Read-only callers may omit it for compatibility.
        """
        self._validate_nonempty(
            quote_generation_key=quote_generation_key,
            structure_generation_key=structure_generation_key,
        )
        self._validate_aware(now, "now")
        if lease is not None:
            if type(lease) is not JobLease or lease.job_type != "opportunity-certify":
                raise ValueError("opportunity projection requires an opportunity-certify lease")
            if lease.input_identity != quote_generation_key:
                raise JobIdentityConflict("opportunity lease names another Quote generation")
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
            if lease is None:
                _set_structure_read_timeouts(cursor, read_only=False)
            else:
                _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
            cursor.execute(
                "SELECT generation_key FROM m1_publication_pointers "
                "WHERE pointer_key='quote:current' FOR UPDATE"
            )
            pointer = cursor.fetchone()
            if pointer is None:
                raise IncompleteQuoteGenerationError(
                    "opportunity projection requires current certified Quote"
                )
            if str(pointer["generation_key"]) != quote_generation_key:
                if lease is not None:
                    raise PublicationPointerConflictError(
                        "opportunity candidate no longer names current Quote"
                    )
                raise IncompleteQuoteGenerationError(
                    "opportunity projection requires current certified Quote"
                )
            cursor.execute(
                """
                SELECT lineage.structure_generation_key
                FROM m1_quote_generation_inputs AS lineage
                JOIN m1_generation_manifests AS quote_manifest
                  ON quote_manifest.generation_key = lineage.generation_key
                 AND quote_manifest.producer_job_key = quote_manifest.generation_key || ':certify'
                JOIN m1_generation_manifests AS structure_manifest
                  ON structure_manifest.generation_key = lineage.structure_generation_key
                 AND structure_manifest.producer_job_key =
                     structure_manifest.generation_key || ':certify'
                WHERE lineage.generation_key = %s
                  AND lineage.structure_generation_key = %s
                """,
                (quote_generation_key, structure_generation_key),
            )
            if cursor.fetchone() is None:
                raise IncompleteStructureGenerationError(
                    "opportunity projection requires certified authoritative lineage"
                )
            if lease is not None:
                self._append_job_succeeded_cursor(
                    cursor,
                    lease=lease,
                    stage="publish-opportunity",
                    component="opportunity-certify",
                    data_product="market-snapshot",
                    now=now,
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
            projection_count = cursor.fetchone()
            if projection_count is None or int(str(projection_count["count"])) != len(normalized):
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
            if lease is not None:
                cursor.execute(
                    """
                    UPDATE m1_jobs
                    SET state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
                        updated_at = %s
                    WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                      AND state = 'leased'
                    """,
                    (now, lease.job_key, lease.lease_owner, lease.lease_epoch),
                )
                if cursor.rowcount != 1:
                    raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
                cursor.execute(
                    """
                    UPDATE m1_job_attempts SET state = 'succeeded', finished_at = %s
                    WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                    """,
                    (now, lease.job_key, lease.lease_epoch),
                )
        return digest

    def recover_opportunity_projection_success(
        self,
        lease: JobLease,
        *,
        quote_generation_key: str,
        structure_generation_key: str | None,
        now: datetime,
    ) -> None:
        """Repair a proven current opportunity pointer success event exactly once."""
        self._validate_aware(now, "now")
        if lease.job_type != "opportunity-certify":
            raise ValueError("opportunity recovery requires an opportunity-certify lease")
        if lease.input_identity != quote_generation_key:
            raise JobIdentityConflict("opportunity recovery names another Quote generation")
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_structure_read_timeouts(cursor, read_only=False)
            cursor.execute(
                """
                SELECT state, lease_epoch FROM m1_jobs
                WHERE job_key = %s FOR UPDATE
                """,
                (lease.job_key,),
            )
            job = cursor.fetchone()
            if job is None or int(job["lease_epoch"]) != lease.lease_epoch:
                raise StaleLeaseError(
                    f"durable opportunity projection is not proven for {lease.job_key}"
                )
            cursor.execute(
                """
                SELECT generation_key, structure_generation_key
                FROM m1_opportunity_projections
                WHERE generation_key = %s
                """,
                (quote_generation_key,),
            )
            projection = cursor.fetchone()
            cursor.execute(
                """
                SELECT generation_key FROM m1_opportunity_publication_pointers
                WHERE pointer_key = 'opportunity:current'
                """,
            )
            pointer = cursor.fetchone()
            if (
                projection is None
                or (
                    structure_generation_key is not None
                    and str(projection["structure_generation_key"]) != structure_generation_key
                )
                or pointer is None
                or str(pointer["generation_key"]) != quote_generation_key
            ):
                raise IncompleteQuoteGenerationError(
                    "opportunity recovery lacks durable current-pointer proof"
                )
            if str(job["state"]) == JobState.SUCCEEDED.value:
                self._append_historical_job_succeeded_cursor(
                    cursor,
                    lease=lease,
                    stage="publish-opportunity",
                    component="opportunity-certify",
                    data_product="market-snapshot",
                    now=now,
                )
            elif (
                str(job["state"]) == JobState.LEASED.value
                and str(job["lease_owner"]) == lease.lease_owner
            ):
                _set_fenced_transaction_timeouts(cursor, lease=lease, now=now)
                self._append_job_succeeded_cursor(
                    cursor,
                    lease=lease,
                    stage="publish-opportunity",
                    component="opportunity-certify",
                    data_product="market-snapshot",
                    now=now,
                )
                cursor.execute(
                    """
                    UPDATE m1_jobs SET state = 'succeeded', lease_owner = NULL,
                        lease_expires_at = NULL, updated_at = %s
                    WHERE job_key = %s AND lease_owner = %s AND lease_epoch = %s
                      AND state = 'leased'
                    """,
                    (now, lease.job_key, lease.lease_owner, lease.lease_epoch),
                )
                if cursor.rowcount != 1:
                    raise StaleLeaseError(f"lease is no longer current for {lease.job_key}")
                cursor.execute(
                    """
                    UPDATE m1_job_attempts SET state = 'succeeded', finished_at = %s
                    WHERE job_key = %s AND lease_epoch = %s AND state = 'running'
                    """,
                    (now, lease.job_key, lease.lease_epoch),
                )
            else:
                raise StaleLeaseError(
                    f"durable opportunity projection is not proven for {lease.job_key}"
                )

    def current_quote_projection_inputs(
        self,
    ) -> tuple[str, str, tuple[tuple[tuple[QuoteBatchLeg, ...], QuoteBatchReceipt, datetime], ...]]:
        """Load one current certified Quote generation and its immutable batch inputs."""
        with (
            self._connection_factory() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            _set_snapshot_read_timeouts(cursor)
            cursor.execute(
                """SELECT pointer.generation_key,
                          lineage.structure_generation_key,
                          opportunity.generation_key AS opportunity_generation_key
                   FROM m1_publication_pointers AS pointer
                   JOIN m1_quote_generation_inputs AS lineage
                     ON lineage.generation_key = pointer.generation_key
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
            "published_at": _snapshot_aware(row["published_at"], "published_at").isoformat(),
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
