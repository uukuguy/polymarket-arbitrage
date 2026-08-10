"""SQLite authority for certified groups and atomic all-leg quote batches."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from polyarb.perception.models import (
    CandidatePriority,
    CandidateResult,
    CandidateWatchFact,
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.priority import GroupScheduleInput, priority_components
from polyarb.storage.schemas import (
    A527_OWNER_MUTATION_GUARD_DDL,
    CANDIDATE_CURRENT_AGGREGATE_DDL,
    CANDIDATE_CURRENT_AGGREGATE_TRIGGER_DDL,
    CANDIDATE_CURRENT_OPPORTUNITY_INDEX_DDL,
    DDL,
    OWNER_JOURNAL_TRIGGER_NAMES,
    OWNER_MUTATION_GUARD_DDL,
    OWNER_TRIGGER_TABLES,
    V2_CANDIDATE_CURRENT_AGGREGATE_DDL,
    V2_OWNER_JOURNAL_TRIGGER_DDL,
    V2_OWNER_MUTATION_GUARD_DDL,
    V3_OWNER_JOURNAL_TRIGGER_NAMES,
    V3_OWNER_MUTATION_GUARD_DDL,
    V4_EVIDENCE_OWNER_DDL,
    V4_EVIDENCE_OWNER_JOURNAL_TRIGGER_DDL,
    V4_LEGACY_EVIDENCE_OWNER_DDL,
    V4_LEGACY_OWNER_JOURNAL_TRIGGER_DDL,
    V4_LEGACY_OWNER_JOURNAL_TRIGGER_NAMES,
    V4_OWNER_MUTATION_GUARD_DDL,
    migrate_fault_auth_finalize,
    migrate_fault_events_cleanup_confirmation,
)

_BUSY_TIMEOUT_MS = 5_000
_OPERATOR_AUTH_HISTORY_MAX_ROWS = 10_000
_OPERATOR_QUEUE_COMPACT_HIGH_ROWS = 8_000
_OPERATOR_QUEUE_COMPACT_LOW_ROWS = 1_000
_OPERATOR_QUEUE_UNCOMPACTED_MAX_ROWS = 20_000
_CANDIDATE_AUTHORITY_DOMAIN = "polyarb-candidate-authority-checkpoint-v1"
_CANDIDATE_AUTHORITY_VERSION = 1
_CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS = 8_000
_CANDIDATE_AUTHORITY_COMPACT_HIGH_BYTES = 4_194_304
_CANDIDATE_AUTHORITY_UNCOMPACTED_MAX_ROWS = 20_000
_DISCOVERY_AUTHORITY_DOMAIN = "polyarb-discovery-authority-checkpoint-v1"
_DISCOVERY_AUTHORITY_VERSION = 1
_DISCOVERY_AUTHORITY_COMPACT_HIGH_ROWS = 8_000
_DISCOVERY_AUTHORITY_COMPACT_LOW_ROWS = 1_000
_DISCOVERY_AUTHORITY_UNCOMPACTED_MAX_ROWS = 20_000
_DISCOVERY_STATUS_PROJECTION_DOMAIN = "polyarb-discovery-status-projection-v1"
_DISCOVERY_STATUS_PROJECTION_VERSION = 1
_OWNER_MUTATION_JOURNAL_RETAIN_ROWS = 128
_OWNER_MUTATION_LEGACY_MAX_ROWS = 1_025
_OWNER_AUTHORITY_VERSION = 5
_CANONICAL_OWNER_TRIGGER_NAMES = frozenset(OWNER_JOURNAL_TRIGGER_NAMES)
_OWNER_WRITE_ACTIONS = frozenset(
    (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE)
)
_OWNER_TABLE_NAMES = (
    "neg_risk_owner_write_context",
    "neg_risk_owner_mutation_journal",
    "neg_risk_owner_mutation_guard",
    "neg_risk_candidate_current_authority",
    "neg_risk_candidate_current_aggregate",
    "neg_risk_discovery_status_projection",
    "neg_risk_discovery_group_projection",
    "neg_risk_incident_authority_checkpoint",
    "neg_risk_incident_open_authority",
    "neg_risk_incident_open_aggregate",
    "neg_risk_incident_scope_floors",
    "neg_risk_incident_suffix_authority",
    "neg_risk_incident_replay_anchors",
    "neg_risk_resource_authority_checkpoint",
    "neg_risk_evidence_failures",
)
_V3_OWNER_TABLE_NAMES = _OWNER_TABLE_NAMES[:7]
_V4_OWNER_TABLE_NAMES = tuple(
    table
    for table in _OWNER_TABLE_NAMES
    if table != "neg_risk_incident_suffix_authority"
)
_V4_EVIDENCE_OWNER_TABLE_NAMES = _OWNER_TABLE_NAMES[7:]
_RECONCILIATION_AUTHORITY_DOMAIN = "polyarb-reconciliation-authority-checkpoint-v1"
_RECONCILIATION_AUTHORITY_VERSION = 1
_HEARTBEAT_AUTH_DOMAIN = "polyarb-producer-heartbeat-v1"
_CHILD_HEARTBEAT_PREIMAGES: dict[tuple[str, str, int], str] = {}
_GROUP_STATUSES = {"discovered", "certified", "stale", "invalidated", "closed"}
_ACTUAL_CANDIDATE_AUTHORITY_SQL = (
    "(s.group_id IS NULL OR s.promoted_at_ms IS NOT NULL OR EXISTS ("
    "SELECT 1 FROM neg_risk_candidate_watch_facts f "
    "WHERE f.group_id=c.group_id))"
)
_LIVE_CANDIDATE_GROUP_IDS_SQL = (
    "WITH current AS ("
    "SELECT r.* FROM neg_risk_group_revisions r JOIN ("
    "SELECT group_id,MAX(revision) AS revision "
    "FROM neg_risk_group_revisions GROUP BY group_id"
    ") latest ON latest.group_id=r.group_id AND latest.revision=r.revision"
    ") SELECT c.group_id FROM current c "
    "LEFT JOIN neg_risk_group_schedule s ON s.group_id=c.group_id "
    "WHERE c.status='certified' AND "
    f"{_ACTUAL_CANDIDATE_AUTHORITY_SQL}"
)

DiscoveryQuality = Literal[
    "complete-supported",
    "complete-unsupported",
    "incomplete-source",
]


def candidate_success_receipt_hash(
    *,
    transaction_id: str,
    group_id: str,
    event_id: str,
    membership_hash: str,
    quote_batch_id: str,
    group_revision_row_id: int,
    quote_batch_row_id: int,
    candidate_fact_row_id: int,
    observed_at_ms: int,
) -> str:
    """Return the canonical integrity hash for an atomic candidate success."""
    payload = json.dumps(
        {
            "candidate_fact_row_id": candidate_fact_row_id,
            "event_id": event_id,
            "group_id": group_id,
            "group_revision_row_id": group_revision_row_id,
            "membership_hash": membership_hash,
            "observed_at_ms": observed_at_ms,
            "quote_batch_id": quote_batch_id,
            "quote_batch_row_id": quote_batch_row_id,
            "transaction_id": transaction_id,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"


def candidate_authority_checkpoint_hash(
    *,
    domain: str,
    version: int,
    generation: int,
    through_group_revision_id: int,
    through_quote_rowid: int,
    through_fact_id: int,
    through_receipt_id: int,
    compacted_group_rows: int,
    compacted_quote_rows: int,
    compacted_fact_rows: int,
    compacted_receipt_rows: int,
    prefix_digest: str,
    seeds_digest: str,
) -> str:
    """Bind one rolling Candidate authority prefix and its retained seeds."""
    payload = json.dumps(
        {
            "compacted_fact_rows": compacted_fact_rows,
            "compacted_group_rows": compacted_group_rows,
            "compacted_quote_rows": compacted_quote_rows,
            "compacted_receipt_rows": compacted_receipt_rows,
            "domain": domain,
            "generation": generation,
            "prefix_digest": prefix_digest,
            "seeds_digest": seeds_digest,
            "through_fact_id": through_fact_id,
            "through_group_revision_id": through_group_revision_id,
            "through_quote_rowid": through_quote_rowid,
            "through_receipt_id": through_receipt_id,
            "version": version,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"


def discovery_authority_checkpoint_hash(
    *,
    domain: str,
    version: int,
    generation: int,
    through_batch_id: int,
    through_sample_id: int,
    through_evidence_id: int,
    compacted_batch_rows: int,
    compacted_sample_rows: int,
    compacted_evidence_rows: int,
    prefix_digest: str,
    anchor_digest: str,
) -> str:
    payload = json.dumps(
        {
            "anchor_digest": anchor_digest,
            "compacted_batch_rows": compacted_batch_rows,
            "compacted_evidence_rows": compacted_evidence_rows,
            "compacted_sample_rows": compacted_sample_rows,
            "domain": domain,
            "generation": generation,
            "prefix_digest": prefix_digest,
            "through_batch_id": through_batch_id,
            "through_evidence_id": through_evidence_id,
            "through_sample_id": through_sample_id,
            "version": version,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"


def discovery_status_projection_hash(
    *,
    domain: str,
    version: int,
    generation: int,
    raw_authority_seq: int,
    candidate_attempt_start_count: int,
    candidate_start_deadline_breach_count: int,
    projection_digest: str,
) -> str:
    payload = json.dumps(
        {
            "candidate_attempt_start_count": candidate_attempt_start_count,
            "candidate_start_deadline_breach_count": (
                candidate_start_deadline_breach_count
            ),
            "domain": domain,
            "generation": generation,
            "projection_digest": projection_digest,
            "raw_authority_seq": raw_authority_seq,
            "version": version,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"


def reconciliation_authority_checkpoint_hash(
    *,
    window_id: str,
    domain: str,
    version: int,
    generation: int,
    through_batch_id: int,
    through_sequence: int,
    compacted_batch_rows: int,
    compacted_sample_rows: int,
    prefix_digest: str,
    anchor_digest: str,
) -> str:
    payload = json.dumps(
        {
            "anchor_digest": anchor_digest,
            "compacted_batch_rows": compacted_batch_rows,
            "compacted_sample_rows": compacted_sample_rows,
            "domain": domain,
            "generation": generation,
            "prefix_digest": prefix_digest,
            "through_batch_id": through_batch_id,
            "through_sequence": through_sequence,
            "version": version,
            "window_id": window_id,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"


def _reconciliation_window_seed(row: sqlite3.Row) -> dict[str, object]:
    names = (
        "id",
        "status",
        "failure_reason",
        "next_cursor",
        "started_at_ms",
        "checkpoint_at_ms",
        "finished_at_ms",
        "pages_completed",
        "events_seen",
        "groups_staged",
        "rejected_count",
        "observations_count",
        "baseline_count",
        "baseline_digest",
        "added_count",
        "changed_count",
        "closed_count",
        "unchanged_count",
        "applied_rejected_count",
    )
    return {name: row[name] for name in names}


def _reconciliation_rows_digest(rows: list[sqlite3.Row]) -> str:
    canonical = json.dumps(
        [dict(row) for row in rows],
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}"


def validate_reconciliation_authority_checkpoint(
    con: sqlite3.Connection,
    window: sqlite3.Row,
    staged: list[sqlite3.Row],
) -> tuple[sqlite3.Row, dict[str, object]] | None:
    """Validate one compacted checkpoint using the caller's open transaction."""
    row = con.execute(
        "SELECT * FROM neg_risk_reconciliation_authority_checkpoints "
        "WHERE window_id=?",
        (window["id"],),
    ).fetchone()
    if row is None:
        return None
    try:
        anchor = json.loads(str(row["anchor_json"]))
        canonical = json.dumps(
            anchor,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        anchor_digest = f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}"
        expected_hash = reconciliation_authority_checkpoint_hash(
            window_id=str(row["window_id"]),
            domain=str(row["domain"]),
            version=int(row["version"]),
            generation=int(row["generation"]),
            through_batch_id=int(row["through_batch_id"]),
            through_sequence=int(row["through_sequence"]),
            compacted_batch_rows=int(row["compacted_batch_rows"]),
            compacted_sample_rows=int(row["compacted_sample_rows"]),
            prefix_digest=str(row["prefix_digest"]),
            anchor_digest=str(row["anchor_digest"]),
        )
        receipt = anchor["receipt"]
        if (
            row["domain"] != _RECONCILIATION_AUTHORITY_DOMAIN
            or int(row["version"]) != _RECONCILIATION_AUTHORITY_VERSION
            or int(row["generation"]) <= 0
            or str(row["window_id"]) != str(window["id"])
            or int(row["through_batch_id"]) != int(receipt["id"])
            or int(row["through_sequence"]) != int(receipt["batch_sequence"])
            or any(
                int(row[name]) < 0
                for name in (
                    "through_batch_id",
                    "through_sequence",
                    "compacted_batch_rows",
                    "compacted_sample_rows",
                )
            )
            or canonical != str(row["anchor_json"])
            or not hmac.compare_digest(str(row["anchor_digest"]), anchor_digest)
            or not hmac.compare_digest(str(row["checkpoint_hash"]), expected_hash)
            or anchor["window"] != _reconciliation_window_seed(window)
            or anchor["staging_digest"] != _reconciliation_rows_digest(staged)
            or not isinstance(anchor["seen_cursors"], list)
            or not isinstance(anchor["cumulative"], dict)
        ):
            raise ValueError("invalid-reconciliation-authority-checkpoint")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("invalid-reconciliation-authority-checkpoint") from error
    retained = con.execute(
        "SELECT "
        "(SELECT COUNT(*) FROM neg_risk_reconciliation_batches "
        " WHERE window_id=? AND batch_sequence<=?),"
        "(SELECT COUNT(*) FROM neg_risk_reconciliation_batch_samples s "
        " JOIN neg_risk_reconciliation_batches b ON b.id=s.batch_id "
        " WHERE b.window_id=? AND s.batch_id<=?)",
        (
            window["id"],
            int(row["through_sequence"]),
            window["id"],
            int(row["through_batch_id"]),
        ),
    ).fetchone()
    if any(int(value) for value in retained):
        raise ValueError("invalid-reconciliation-authority-checkpoint")
    return row, anchor


def operator_auth_receipt_hash(
    *,
    nonce: str,
    request_method: str,
    request_path: str,
    request_timestamp_s: int,
    body_hash: str,
    accepted_at_ms: int,
) -> str:
    payload = json.dumps(
        {
            "accepted_at_ms": accepted_at_ms,
            "body_hash": body_hash,
            "nonce": nonce,
            "request_method": request_method,
            "request_path": request_path,
            "request_timestamp_s": request_timestamp_s,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(b"polyarb-operator-auth-v1\0" + payload.encode()).hexdigest()


def operator_queue_receipt_hash(
    *,
    component: str,
    sequence: int,
    action: str,
    occurred_at_ms: int,
    auth_nonce: str,
    previous_hash: str | None,
    auth_receipt_hash: str | None = None,
) -> str:
    payload = json.dumps(
        {
            "action": action,
            "auth_nonce": auth_nonce,
            **(
                {}
                if auth_receipt_hash is None
                else {"auth_receipt_hash": auth_receipt_hash}
            ),
            "component": component,
            "occurred_at_ms": occurred_at_ms,
            "previous_hash": previous_hash,
            "sequence": sequence,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    domain = (
        b"polyarb-operator-queue-v1\0"
        if auth_receipt_hash is None
        else b"polyarb-operator-queue-v2\0"
    )
    return hashlib.sha256(domain + payload.encode()).hexdigest()


def operator_queue_checkpoint_hash(
    *,
    component: str,
    through_sequence: int,
    through_receipt_hash: str,
    last_occurred_at_ms: int,
    queued: bool,
    queued_at_ms: int | None,
    consumed_at_ms: int | None,
    request_nonce: str | None,
    request_auth_hash: str | None,
) -> str:
    payload = json.dumps(
        {
            "component": component,
            "consumed_at_ms": consumed_at_ms,
            "domain": "polyarb-operator-queue-checkpoint",
            "last_occurred_at_ms": last_occurred_at_ms,
            "queued": queued,
            "queued_at_ms": queued_at_ms,
            "request_auth_hash": request_auth_hash,
            "request_nonce": request_nonce,
            "through_receipt_hash": through_receipt_hash,
            "through_sequence": through_sequence,
            "version": 1,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(
        b"polyarb-operator-queue-checkpoint-v1\0" + payload.encode()
    ).hexdigest()


class ReconciliationIncompleteError(RuntimeError):
    """Raised when an incomplete calibration window is asked to publish."""


class ReconciliationUnprovableError(ValueError):
    """Raised when a legacy window has no captured baseline proof."""


@dataclass(frozen=True)
class ReconciliationWindow:
    id: str
    status: Literal["open", "complete", "applied", "failed"]
    failure_reason: str | None
    next_cursor: str | None
    started_at_ms: int
    checkpoint_at_ms: int
    finished_at_ms: int | None
    pages_completed: int
    events_seen: int
    groups_staged: int
    rejected_count: int
    observations_count: int
    baseline_count: int
    baseline_digest: str | None
    added_count: int | None
    changed_count: int | None
    closed_count: int | None
    unchanged_count: int | None
    applied_rejected_count: int | None


@dataclass(frozen=True)
class ReconciliationDiff:
    window_id: str
    added: int
    changed: int
    closed: int
    unchanged: int
    rejected: int
    started_at_ms: int
    finished_at_ms: int


@dataclass(frozen=True)
class DiscoveryScheduleCandidate:
    event_id: str
    group_id: str
    membership_hash: str
    quality: DiscoveryQuality
    reason: str | None
    activity_rank: Decimal
    liquidity_rank: Decimal
    liquidity_weight: Decimal
    legs: tuple[GroupLeg, ...] | None


@dataclass(frozen=True)
class GroupSchedule:
    group_id: str
    event_id: str
    membership_hash: str
    quality: DiscoveryQuality
    reason: str | None
    gross_edge_bps: Decimal
    activity_rank: Decimal
    liquidity_rank: Decimal
    change_rank: Decimal
    age_rank: Decimal
    priority_score: Decimal
    priority_reason: str
    priority_class: CandidatePriority
    liquidity_weight: Decimal
    first_discovered_at_ms: int
    last_discovered_at_ms: int
    last_visited_at_ms: int | None
    promoted_at_ms: int | None
    promotion_eligible_at_ms: int | None
    promotion_queue_deadline_at_ms: int | None
    candidate_start_deadline_at_ms: int | None


@dataclass(frozen=True)
class CandidateAdmissionContext:
    group_id: str
    event_id: str
    membership_hash: str
    promoted_at_ms: int
    candidate_start_deadline_at_ms: int


@dataclass(frozen=True)
class DiscoveryAdmissionProof:
    effective_capacity: int
    candidate_max_wait_ms: int
    selection_budget_ms: int
    poll_interval_ms: int
    group_timeout_ms: int
    terminal_write_budget_ms: int
    high_burst_groups: int
    reserved_non_high_slots: int
    attempt_start_write_budget_ms: int = 5_000

    @property
    def effective_start_bound_ms(self) -> int | None:
        if self.effective_capacity <= 0:
            return None
        return (
            self.poll_interval_ms
            + self.selection_budget_ms
            + self.effective_capacity * self.attempt_start_write_budget_ms
            + (self.high_burst_groups + self.effective_capacity - 1)
            * (self.group_timeout_ms + self.terminal_write_budget_ms)
        )

    def validate(self) -> None:
        if (
            self.effective_capacity < 0
            or not 0 < self.candidate_max_wait_ms <= 60_000
            or self.selection_budget_ms <= 0
            or self.poll_interval_ms <= 0
            or self.group_timeout_ms <= 0
            or self.terminal_write_budget_ms < _BUSY_TIMEOUT_MS
            or self.attempt_start_write_budget_ms < _BUSY_TIMEOUT_MS
            or self.high_burst_groups <= 0
            or self.reserved_non_high_slots <= 0
            or self.effective_capacity > self.reserved_non_high_slots
            or (
                self.effective_capacity > 0
                and self.effective_start_bound_ms > self.candidate_max_wait_ms
            )
        ):
            raise ValueError("invalid-discovery-admission-proof")


@dataclass(frozen=True)
class CoverageWindow:
    minutes: int
    visited_groups: int
    raw_fraction: Decimal
    liquidity_weighted_fraction: Decimal


@dataclass(frozen=True)
class CoverageWindows:
    known_groups: int
    total_liquidity_weight: Decimal
    by_minutes: dict[int, CoverageWindow]


@dataclass(frozen=True)
class DiscoveryStatus:
    next_cursor: str | None
    completed: bool
    last_started_at_ms: int | None
    last_finished_at_ms: int | None
    page_event_count: int
    groups_seen: int
    promoted_count: int
    queue_depth_by_class: dict[str, int]
    oldest_visit_age_ms: int | None
    coverage: CoverageWindows
    load_state: DiscoveryLoadState
    admission_proof: DiscoveryAdmissionProof | None
    promotion_queue_depth: int
    outstanding_admitted_count: int
    candidate_attempt_start_count: int
    candidate_start_deadline_breach_count: int
    candidate_start_ready: bool


@dataclass(frozen=True)
class DurableCandidateFreshness:
    candidate_count: int
    quote_p95_age_ms: int | None
    missing_quote_count: int


@dataclass(frozen=True)
class CurrentOpportunity:
    group_id: str
    event_id: str
    group_revision: int
    membership_hash: str
    quote_batch_id: str
    fact_id: int
    bundle_cost: Decimal
    gross_edge_bps: Decimal
    max_bundle_size: Decimal
    structure_observed_at_ms: int
    quote_started_at_ms: int
    quote_quoted_at_ms: int


@dataclass(frozen=True)
class CandidateCurrentSummary:
    current_group_count: int
    opportunity_count: int
    state_counts: dict[str, int]
    authority_hash: str


@dataclass(frozen=True)
class CandidateSchedulingSnapshotItem:
    group_id: str
    fact: CandidateWatchFact | None
    schedule: GroupSchedule | None


@dataclass(frozen=True)
class DiscoveryLoadState:
    degraded_streak: int
    last_reason: str | None
    last_decision: Literal["fresh", "yield", "probe"]
    probe_every_cycles: int
    updated_at_ms: int


@dataclass(frozen=True)
class ProducerReceipt:
    component: str
    attempt: int
    started_at_ms: int
    finished_at_ms: int
    outcome: str
    exit_code: int | None
    stdout_tail: str
    stderr_tail: str
    supervisor_run_id: str
    child_auth_hash: str | None


@dataclass(frozen=True)
class ProducerHistoryState:
    state: str
    latest_attempt: int | None
    supervisor_run_id: str | None
    latest_started_at_ms: int | None
    heartbeat_count: int
    heartbeat_sequence: int
    last_progress_at_ms: int | None
    terminal_at_ms: int | None


def _valid_producer_receipt_outcome(outcome: object, exit_code: object) -> bool:
    if outcome == "success":
        return type(exit_code) is int and exit_code == 0
    if outcome == "nonzero":
        return type(exit_code) is int and exit_code != 0
    if outcome in {"timeout", "cancelled", "spawn-error"}:
        return exit_code is None
    return False


def _valid_producer_receipt_tail(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return len(value.encode("utf-8")) <= 16_384
    except UnicodeEncodeError:
        return False


def _producer_receipt_output_hash(stdout_tail: str, stderr_tail: str) -> str:
    material = json.dumps(
        {"stderr_tail": stderr_tail, "stdout_tail": stdout_tail},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(b"polyarb-producer-output-v1\0" + material).hexdigest()


def validate_producer_history(
    con: sqlite3.Connection,
    component: str,
    *,
    now_ms: int,
) -> ProducerHistoryState:
    if component not in {"candidate", "discovery", "reconciliation", "quote"}:
        raise ValueError("invalid-producer-component")
    con.row_factory = sqlite3.Row
    starts = con.execute(
        "SELECT * FROM neg_risk_producer_child_starts "
        "WHERE component=? ORDER BY attempt",
        (component,),
    ).fetchall()
    receipts = con.execute(
        "SELECT * FROM neg_risk_producer_receipts "
        "WHERE component=? ORDER BY attempt",
        (component,),
    ).fetchall()
    heartbeats = con.execute(
        "SELECT * FROM neg_risk_producer_heartbeats "
        "WHERE component=? ORDER BY attempt,sequence",
        (component,),
    ).fetchall()
    if not starts:
        if receipts or heartbeats:
            raise ValueError("invalid-producer-history")
        return ProducerHistoryState(
            "never-started", None, None, None, 0, 0, None, None
        )
    receipt_by_attempt: dict[int, sqlite3.Row] = {}
    for receipt in receipts:
        attempt = receipt["attempt"]
        if type(attempt) is not int or attempt in receipt_by_attempt:
            raise ValueError("invalid-producer-history")
        receipt_by_attempt[attempt] = receipt
    heartbeat_by_attempt: dict[int, list[sqlite3.Row]] = {}
    for heartbeat in heartbeats:
        attempt = heartbeat["attempt"]
        if type(attempt) is not int:
            raise ValueError("invalid-producer-history")
        heartbeat_by_attempt.setdefault(attempt, []).append(heartbeat)

    for expected_attempt, start in enumerate(starts, start=1):
        attempt = start["attempt"]
        auth_hash = start["child_auth_hash"]
        claimed_at_ms = start["claimed_at_ms"]
        if (
            attempt != expected_attempt
            or not isinstance(start["supervisor_run_id"], str)
            or not start["supervisor_run_id"]
            or start["auth_domain"] != _HEARTBEAT_AUTH_DOMAIN
            or type(start["started_at_ms"]) is not int
            or not 0 <= start["started_at_ms"] <= now_ms
            or (
                auth_hash is not None
                and (
                    not isinstance(auth_hash, str)
                    or len(auth_hash) != 64
                    or any(char not in "0123456789abcdef" for char in auth_hash)
                    or type(claimed_at_ms) is not int
                    or not start["started_at_ms"] <= claimed_at_ms <= now_ms
                )
            )
            or (auth_hash is None and claimed_at_ms is not None)
        ):
            raise ValueError("invalid-producer-history")
        receipt = receipt_by_attempt.get(attempt)
        if attempt < len(starts) and receipt is None:
            raise ValueError("invalid-producer-history")
        if receipt is not None and (
            receipt["supervisor_run_id"] != start["supervisor_run_id"]
            or receipt["auth_domain"] != _HEARTBEAT_AUTH_DOMAIN
            or receipt["child_auth_hash"] != auth_hash
            or receipt["started_at_ms"] != start["started_at_ms"]
            or type(receipt["finished_at_ms"]) is not int
            or not start["started_at_ms"] <= receipt["finished_at_ms"] <= now_ms
            or not _valid_producer_receipt_outcome(
                receipt["outcome"],
                receipt["exit_code"],
            )
            or not _valid_producer_receipt_tail(receipt["stdout_tail"])
            or not _valid_producer_receipt_tail(receipt["stderr_tail"])
            or receipt["output_hash"]
            != _producer_receipt_output_hash(
                receipt["stdout_tail"],
                receipt["stderr_tail"],
            )
        ):
            raise ValueError("invalid-producer-history")
        attempt_heartbeats = heartbeat_by_attempt.get(attempt, [])
        if attempt_heartbeats and auth_hash is None:
            raise ValueError("invalid-producer-history")
        previous_at_ms = (
            start["started_at_ms"] if claimed_at_ms is None else claimed_at_ms
        )
        terminal_at_ms = now_ms if receipt is None else receipt["finished_at_ms"]
        for expected_sequence, heartbeat in enumerate(attempt_heartbeats, start=1):
            if (
                heartbeat["supervisor_run_id"] != start["supervisor_run_id"]
                or heartbeat["auth_domain"] != _HEARTBEAT_AUTH_DOMAIN
                or heartbeat["child_auth_hash"] != auth_hash
                or heartbeat["sequence"] != expected_sequence
                or type(heartbeat["observed_at_ms"]) is not int
                or not previous_at_ms <= heartbeat["observed_at_ms"] <= terminal_at_ms
            ):
                raise ValueError("invalid-producer-history")
            previous_at_ms = heartbeat["observed_at_ms"]
    if set(receipt_by_attempt) - {row["attempt"] for row in starts}:
        raise ValueError("invalid-producer-history")
    if set(heartbeat_by_attempt) - {row["attempt"] for row in starts}:
        raise ValueError("invalid-producer-history")

    latest = starts[-1]
    latest_receipt = receipt_by_attempt.get(latest["attempt"])
    latest_heartbeats = heartbeat_by_attempt.get(latest["attempt"], [])
    last_progress_at_ms = (
        latest_heartbeats[-1]["observed_at_ms"] if latest_heartbeats else None
    )
    if latest_receipt is None:
        state = "running" if latest_heartbeats else "starting"
        terminal_at_ms = None
    else:
        outcome = str(latest_receipt["outcome"])
        state = "unexpected-exit" if outcome == "success" else outcome
        terminal_at_ms = latest_receipt["finished_at_ms"]
    return ProducerHistoryState(
        state=state,
        latest_attempt=latest["attempt"],
        supervisor_run_id=latest["supervisor_run_id"],
        latest_started_at_ms=latest["started_at_ms"],
        heartbeat_count=len(latest_heartbeats),
        heartbeat_sequence=(
            latest_heartbeats[-1]["sequence"] if latest_heartbeats else 0
        ),
        last_progress_at_ms=last_progress_at_ms,
        terminal_at_ms=terminal_at_ms,
    )


class OpportunityPerceptionStore:
    """Transactional opportunity-first perception read model."""

    def __init__(
        self,
        db_path: Path,
        *,
        read_only: bool = False,
        busy_timeout_ms: int = _BUSY_TIMEOUT_MS,
        deadline_monotonic: float | None = None,
    ) -> None:
        if not 1 <= busy_timeout_ms <= _BUSY_TIMEOUT_MS:
            raise ValueError("invalid-busy-timeout")
        self._db_path = Path(db_path)
        self._read_only = read_only
        self._busy_timeout_ms = busy_timeout_ms
        self._deadline_monotonic = deadline_monotonic
        if not read_only:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _check_deadline(self, reason: str) -> None:
        if (
            self._deadline_monotonic is not None
            and time.monotonic() >= self._deadline_monotonic
        ):
            raise TimeoutError(reason)

    @staticmethod
    def _migrate_legacy_operator_proofs(
        con: sqlite3.Connection,
        *,
        legacy: bool,
        receipt_auth_upgrade: bool,
    ) -> None:
        if not legacy and not receipt_auth_upgrade:
            return
        body_hash = hashlib.sha256(b"{}").hexdigest()
        auth_rows = con.execute(
            "SELECT * FROM neg_risk_operator_auth_nonces "
            "ORDER BY accepted_at_ms,nonce"
        ).fetchall()
        auth_by_nonce: dict[str, sqlite3.Row] = {}
        auth_hash_by_nonce: dict[str, str] = {}
        for row in auth_rows:
            nonce = str(row["nonce"])
            path = str(row["request_path"])
            request_timestamp_s = row["request_timestamp_s"]
            accepted_at_ms = row["accepted_at_ms"]
            if (
                not nonce
                or nonce in auth_by_nonce
                or path
                not in {
                    "/control/perception/discovery",
                    "/control/perception/reconciliation",
                }
                or type(request_timestamp_s) is not int
                or type(accepted_at_ms) is not int
                or abs(accepted_at_ms - request_timestamp_s * 1_000) > 300_999
            ):
                raise ValueError("invalid-legacy-operator-auth")
            request_method = "POST" if legacy else str(row["request_method"])
            stored_body_hash = body_hash if legacy else str(row["body_hash"])
            auth_hash = operator_auth_receipt_hash(
                nonce=nonce,
                request_method=request_method,
                request_path=path,
                request_timestamp_s=request_timestamp_s,
                body_hash=stored_body_hash,
                accepted_at_ms=accepted_at_ms,
            )
            if legacy:
                con.execute(
                    "UPDATE neg_risk_operator_auth_nonces SET "
                    "request_method='POST',body_hash=?,auth_hash=? WHERE nonce=?",
                    (body_hash, auth_hash, nonce),
                )
            elif not hmac.compare_digest(str(row["auth_hash"]), auth_hash):
                raise ValueError("invalid-legacy-operator-auth")
            auth_by_nonce[nonce] = row
            auth_hash_by_nonce[nonce] = auth_hash

        queue_rows = {
            str(row["component"]): row
            for row in con.execute(
                "SELECT * FROM neg_risk_operator_queue ORDER BY component"
            ).fetchall()
        }
        for component in ("discovery", "reconciliation"):
            queued = False
            queued_nonce: str | None = None
            queued_at_ms: int | None = None
            consumed_at_ms: int | None = None
            old_previous_hash: str | None = None
            new_previous_hash: str | None = None
            rows = con.execute(
                "SELECT * FROM neg_risk_operator_queue_receipts "
                "WHERE component=? ORDER BY id",
                (component,),
            ).fetchall()
            for sequence, row in enumerate(rows, 1):
                action = str(row["action"])
                nonce = str(
                    row["request_nonce"] if legacy else row["auth_nonce"]
                )
                auth = auth_by_nonce.get(nonce)
                occurred_at_ms = row["occurred_at_ms"]
                if (
                    auth is None
                    or auth["request_path"]
                    != f"/control/perception/{component}"
                    or type(occurred_at_ms) is not int
                    or occurred_at_ms < auth["accepted_at_ms"]
                    or (action == "queued" and queued)
                    or (action == "coalesced" and not queued)
                    or (
                        action in {"consumed", "cancelled"}
                        and (not queued or nonce != queued_nonce)
                    )
                    or action
                    not in {"queued", "coalesced", "consumed", "cancelled"}
                ):
                    raise ValueError("invalid-legacy-operator-queue")
                if not legacy:
                    old_hash = operator_queue_receipt_hash(
                        component=component,
                        sequence=sequence,
                        action=action,
                        occurred_at_ms=occurred_at_ms,
                        auth_nonce=nonce,
                        previous_hash=old_previous_hash,
                    )
                    if (
                        row["sequence"] != sequence
                        or row["previous_hash"] != old_previous_hash
                        or not hmac.compare_digest(
                            str(row["receipt_hash"]),
                            old_hash,
                        )
                    ):
                        raise ValueError("invalid-legacy-operator-queue")
                if action == "queued":
                    queued = True
                    queued_nonce = nonce
                    queued_at_ms = occurred_at_ms
                    consumed_at_ms = None
                elif action in {"consumed", "cancelled"}:
                    queued = False
                    consumed_at_ms = occurred_at_ms
                receipt_hash = operator_queue_receipt_hash(
                    component=component,
                    sequence=sequence,
                    action=action,
                    occurred_at_ms=occurred_at_ms,
                    auth_nonce=nonce,
                    previous_hash=new_previous_hash,
                    auth_receipt_hash=auth_hash_by_nonce[nonce],
                )
                con.execute(
                    "UPDATE neg_risk_operator_queue_receipts SET "
                    "sequence=?,auth_nonce=?,auth_receipt_hash=?,"
                    "previous_hash=?,receipt_hash=? "
                    "WHERE id=?",
                    (
                        sequence,
                        nonce,
                        auth_hash_by_nonce[nonce],
                        new_previous_hash,
                        receipt_hash,
                        row["id"],
                    ),
                )
                old_previous_hash = (
                    None if legacy else str(row["receipt_hash"])
                )
                new_previous_hash = receipt_hash
            materialized = queue_rows.get(component)
            if materialized is None:
                if rows:
                    raise ValueError("invalid-legacy-operator-queue")
                continue
            if (
                bool(materialized["queued"]) != queued
                or materialized["request_nonce"] != queued_nonce
                or materialized["queued_at_ms"] != queued_at_ms
                or materialized["consumed_at_ms"] != consumed_at_ms
            ):
                raise ValueError("invalid-legacy-operator-queue")
            con.execute(
                "UPDATE neg_risk_operator_queue SET "
                "request_auth_hash=?,last_sequence=?,last_receipt_hash=? "
                "WHERE component=?",
                (
                    (
                        None
                        if queued_nonce is None
                        else auth_hash_by_nonce[queued_nonce]
                    ),
                    len(rows),
                    new_previous_hash,
                    component,
                ),
            )

    @staticmethod
    def _owner_mutation_event_hash(
        *,
        previous_hash: str | None,
        journal_id: int,
        writer_token: str,
        table_name: str,
        operation: str,
        row_key: str,
        old_json: str | None,
        new_json: str | None,
    ) -> str:
        payload = json.dumps(
            {
                "id": journal_id,
                "new": None if new_json is None else json.loads(new_json),
                "old": None if old_json is None else json.loads(old_json),
                "operation": operation,
                "previous_hash": previous_hash,
                "row_key": row_key,
                "table_name": table_name,
                "writer_token": writer_token,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"

    @classmethod
    def _assert_owner_journal_clean(cls, con: sqlite3.Connection) -> None:
        cls._assert_owner_trigger_manifest(con)
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash,"
            "retained_base_id,retained_base_hash,"
            "candidate_aggregate_hash,discovery_aggregate_hash,"
            "authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        if (
            guard is None
            or int(guard["authority_version"]) != _OWNER_AUTHORITY_VERSION
            or guard["migration_state"] != "complete"
            or guard["candidate_aggregate_hash"] is None
            or guard["discovery_aggregate_hash"] is None
        ):
            raise ValueError("invalid-owner-guard-state")
        cls._validate_owner_journal_chain(
            con,
            guard=guard,
            max_rows=_OWNER_MUTATION_JOURNAL_RETAIN_ROWS,
        )
        candidate_hash, discovery_hash = cls._owner_aggregate_hashes(con)
        if not hmac.compare_digest(
            str(guard["candidate_aggregate_hash"]),
            candidate_hash,
        ) or not hmac.compare_digest(
            str(guard["discovery_aggregate_hash"]),
            discovery_hash,
        ):
            raise ValueError("invalid-owner-aggregate-authority")

    @classmethod
    def _validate_owner_journal_chain(
        cls,
        con: sqlite3.Connection,
        *,
        guard: sqlite3.Row,
        max_rows: int,
    ) -> None:
        context = con.execute(
            "SELECT 1 FROM neg_risk_owner_write_context WHERE id=1"
        ).fetchone()
        if context is not None:
            raise ValueError("pending-owner-mutation")
        consumed_id = int(guard["consumed_journal_id"])
        base_id = int(guard["retained_base_id"])
        if base_id > consumed_id:
            raise ValueError("invalid-owner-mutation-chain")
        pending = con.execute(
            "SELECT 1 FROM neg_risk_owner_mutation_journal "
            "WHERE id>? ORDER BY id LIMIT 1",
            (consumed_id,),
        ).fetchone()
        if pending is not None:
            raise ValueError("pending-owner-mutation")
        sequence = con.execute(
            "SELECT seq FROM sqlite_sequence "
            "WHERE name='neg_risk_owner_mutation_journal'"
        ).fetchone()
        sequence_id = 0 if sequence is None else int(sequence["seq"])
        max_row = con.execute(
            "SELECT MAX(id) AS max_id FROM neg_risk_owner_mutation_journal"
        ).fetchone()
        max_id = None if max_row["max_id"] is None else int(max_row["max_id"])
        if (
            sequence_id != consumed_id
            or (
                consumed_id > base_id
                and max_id != consumed_id
            )
            or (
                consumed_id == base_id
                and max_id is not None
            )
        ):
            raise ValueError("invalid-owner-mutation-sequence")
        rows = con.execute(
            "SELECT * FROM neg_risk_owner_mutation_journal "
            "WHERE id>? AND id<=? ORDER BY id LIMIT ?",
            (
                base_id,
                consumed_id,
                max_rows + 1,
            ),
        ).fetchall()
        if len(rows) > max_rows:
            raise ValueError("invalid-owner-mutation-chain")
        if consumed_id == base_id:
            if rows or guard["consumed_hash"] != guard["retained_base_hash"]:
                raise ValueError("invalid-owner-mutation-chain")
        else:
            if not rows:
                raise ValueError("invalid-owner-mutation-chain")
            previous_id = base_id
            previous_hash = guard["retained_base_hash"]
            for row in rows:
                row_id = int(row["id"])
                if (
                    row_id != previous_id + 1
                    or row["writer_token"] is None
                    or row["previous_hash"] != previous_hash
                ):
                    raise ValueError("invalid-owner-mutation-chain")
                expected_hash = cls._owner_mutation_event_hash(
                    previous_hash=previous_hash,
                    journal_id=row_id,
                    writer_token=str(row["writer_token"]),
                    table_name=str(row["table_name"]),
                    operation=str(row["operation"]),
                    row_key=str(row["row_key"]),
                    old_json=row["old_json"],
                    new_json=row["new_json"],
                )
                if row["event_hash"] != expected_hash:
                    raise ValueError("invalid-owner-mutation-chain")
                previous_id = row_id
                previous_hash = expected_hash
            if previous_id != consumed_id or previous_hash != guard["consumed_hash"]:
                raise ValueError("invalid-owner-mutation-chain")

    @staticmethod
    def _owner_aggregate_hashes(con: sqlite3.Connection) -> tuple[str, str]:
        candidate = con.execute(
            "SELECT current_group_count,opportunity_count,watching_count,"
            "no_edge_count,unavailable_count,aggregate_digest "
            "FROM neg_risk_candidate_current_aggregate WHERE id=1"
        ).fetchone()
        discovery = con.execute(
            "SELECT generation,raw_authority_seq,owner_journal_id,group_count,"
            "queue_high,queue_normal,queue_explore,promotion_queue_depth,"
            "outstanding_admitted_count,total_liquidity_weight,"
            "projection_digest,checkpoint_hash "
            "FROM neg_risk_discovery_status_projection WHERE id=1"
        ).fetchone()

        def digest(domain: str, row: sqlite3.Row | None) -> str:
            payload = json.dumps(
                {
                    "domain": domain,
                    "row": None if row is None else dict(row),
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"

        return (
            digest("polyarb.owner.candidate-aggregate.v2", candidate),
            digest("polyarb.owner.discovery-aggregate.v1", discovery),
        )

    @staticmethod
    def _owner_aggregate_hashes_v2(
        con: sqlite3.Connection,
    ) -> tuple[str, str]:
        candidate = con.execute(
            "SELECT current_group_count,opportunity_count,aggregate_digest "
            "FROM neg_risk_candidate_current_aggregate WHERE id=1"
        ).fetchone()
        discovery = con.execute(
            "SELECT generation,raw_authority_seq,owner_journal_id,group_count,"
            "queue_high,queue_normal,queue_explore,promotion_queue_depth,"
            "outstanding_admitted_count,total_liquidity_weight,"
            "projection_digest,checkpoint_hash "
            "FROM neg_risk_discovery_status_projection WHERE id=1"
        ).fetchone()

        def digest(domain: str, row: sqlite3.Row | None) -> str:
            payload = json.dumps(
                {
                    "domain": domain,
                    "row": None if row is None else dict(row),
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"

        return (
            digest("polyarb.owner.candidate-aggregate.v1", candidate),
            digest("polyarb.owner.discovery-aggregate.v1", discovery),
        )

    @classmethod
    def _refresh_owner_aggregate_hashes(cls, con: sqlite3.Connection) -> None:
        candidate_hash, discovery_hash = cls._owner_aggregate_hashes(con)
        con.execute(
            "UPDATE neg_risk_owner_mutation_guard SET "
            "candidate_aggregate_hash=?,discovery_aggregate_hash=? WHERE id=1",
            (candidate_hash, discovery_hash),
        )

    @staticmethod
    def _prune_owner_mutation_journal(
        con: sqlite3.Connection,
        *,
        consumed_journal_id: int,
    ) -> None:
        cutoff = max(
            0,
            consumed_journal_id - _OWNER_MUTATION_JOURNAL_RETAIN_ROWS + 1,
        )
        base = con.execute(
            "SELECT id,event_hash FROM neg_risk_owner_mutation_journal "
            "WHERE id<? ORDER BY id DESC LIMIT 1",
            (cutoff,),
        ).fetchone()
        if base is None:
            return
        con.execute(
            "UPDATE neg_risk_owner_mutation_guard SET "
            "retained_base_id=?,retained_base_hash=? WHERE id=1",
            (int(base["id"]), str(base["event_hash"])),
        )
        con.execute(
            "DELETE FROM neg_risk_owner_mutation_journal WHERE id<=?",
            (int(base["id"]),),
        )

    @staticmethod
    def _validate_owner_trigger_sql(con: sqlite3.Connection) -> None:
        OpportunityPerceptionStore._assert_owner_trigger_manifest(con)

    def _begin_expected_owner_mutation(
        self,
        con: sqlite3.Connection,
        *,
        table_name: str,
        operation: str,
        row_key: str,
    ) -> str:
        self._assert_owner_journal_clean(con)
        token = str(uuid.uuid4())
        con.execute(
            "INSERT INTO neg_risk_owner_write_context("
            "id,writer_token,table_name,operation,row_key) VALUES(1,?,?,?,?)",
            (token, table_name, operation, row_key),
        )
        return token

    def _consume_expected_owner_mutation(
        self,
        con: sqlite3.Connection,
        *,
        writer_token: str,
        table_name: str,
        operation: str,
        row_key: str,
        finalize: bool = True,
    ) -> None:
        self._consume_expected_owner_events(
            con,
            writer_token=writer_token,
            expected_events=[(table_name, operation, row_key)],
            finalize=finalize,
        )

    def _consume_expected_owner_events(
        self,
        con: sqlite3.Connection,
        *,
        writer_token: str,
        expected_events: list[tuple[str, str, str]],
        finalize: bool,
    ) -> None:
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        if guard is None:
            raise ValueError("pending-owner-mutation")
        rows = con.execute(
            "SELECT * FROM neg_risk_owner_mutation_journal "
            "WHERE id>? ORDER BY id LIMIT ?",
            (
                int(guard["consumed_journal_id"]),
                len(expected_events) + 1,
            ),
        ).fetchall()
        if len(rows) != len(expected_events):
            raise ValueError("unexpected-owner-mutation-delta")
        previous_hash = guard["consumed_hash"]
        last_id = int(guard["consumed_journal_id"])
        for row, (table_name, operation, row_key) in zip(
            rows,
            expected_events,
            strict=True,
        ):
            if (
                row["writer_token"] != writer_token
                or row["table_name"] != table_name
                or row["operation"] != operation
                or row["row_key"] != row_key
            ):
                raise ValueError("unexpected-owner-mutation-delta")
            event_hash = self._owner_mutation_event_hash(
                previous_hash=previous_hash,
                journal_id=int(row["id"]),
                writer_token=writer_token,
                table_name=table_name,
                operation=operation,
                row_key=row_key,
                old_json=row["old_json"],
                new_json=row["new_json"],
            )
            con.execute(
                "UPDATE neg_risk_owner_mutation_journal SET "
                "previous_hash=?,event_hash=? WHERE id=?",
                (previous_hash, event_hash, int(row["id"])),
            )
            previous_hash = event_hash
            last_id = int(row["id"])
        con.execute(
            "UPDATE neg_risk_owner_mutation_guard SET "
            "consumed_journal_id=?,consumed_hash=? WHERE id=1",
            (last_id, previous_hash),
        )
        if not finalize:
            return
        con.execute(
            "DELETE FROM neg_risk_owner_write_context "
            "WHERE id=1 AND writer_token=?",
            (writer_token,),
        )
        self._refresh_owner_aggregate_hashes(con)
        self._prune_owner_mutation_journal(
            con,
            consumed_journal_id=last_id,
        )
        self._assert_owner_journal_clean(con)

    def _consume_expected_owner_mutations(
        self,
        con: sqlite3.Connection,
        *,
        writer_token: str,
        table_name: str,
        operation: str,
        expected_row_keys: list[str],
        finalize: bool = True,
    ) -> None:
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        if guard is None:
            raise ValueError("pending-owner-mutation")
        rows = con.execute(
            "SELECT * FROM neg_risk_owner_mutation_journal "
            "WHERE id>? ORDER BY id",
            (int(guard["consumed_journal_id"]),),
        ).fetchall()
        if (
            len(rows) != len(expected_row_keys)
            or sorted(str(row["row_key"]) for row in rows)
            != sorted(expected_row_keys)
            or any(
                row["writer_token"] != writer_token
                or row["table_name"] != table_name
                or row["operation"] != operation
                for row in rows
            )
        ):
            raise ValueError("unexpected-owner-mutation-delta")
        previous_hash = guard["consumed_hash"]
        last_id = int(guard["consumed_journal_id"])
        for row in rows:
            event_hash = self._owner_mutation_event_hash(
                previous_hash=previous_hash,
                journal_id=int(row["id"]),
                writer_token=writer_token,
                table_name=table_name,
                operation=operation,
                row_key=str(row["row_key"]),
                old_json=row["old_json"],
                new_json=row["new_json"],
            )
            con.execute(
                "UPDATE neg_risk_owner_mutation_journal SET "
                "previous_hash=?,event_hash=? WHERE id=?",
                (previous_hash, event_hash, int(row["id"])),
            )
            previous_hash = event_hash
            last_id = int(row["id"])
        con.execute(
            "UPDATE neg_risk_owner_mutation_guard SET "
            "consumed_journal_id=?,consumed_hash=? WHERE id=1",
            (last_id, previous_hash),
        )
        if not finalize:
            return
        con.execute(
            "DELETE FROM neg_risk_owner_write_context "
            "WHERE id=1 AND writer_token=?",
            (writer_token,),
        )
        self._refresh_owner_aggregate_hashes(con)
        self._prune_owner_mutation_journal(
            con,
            consumed_journal_id=last_id,
        )
        self._assert_owner_journal_clean(con)

    def _execute_expected_owner_bulk(
        self,
        con: sqlite3.Connection,
        *,
        table_name: str,
        operation: str,
        row_keys: list[str],
        sql: str,
        parameters: tuple[object, ...],
    ) -> None:
        if not row_keys:
            return
        token = self._begin_expected_owner_mutation(
            con,
            table_name=table_name,
            operation=operation,
            row_key="*",
        )
        con.execute(sql, parameters)
        self._consume_expected_owner_mutations(
            con,
            writer_token=token,
            table_name=table_name,
            operation=operation,
            expected_row_keys=row_keys,
            finalize=False,
        )
        self._refresh_discovery_status_projection(
            con,
            writer_token=token,
        )

    @staticmethod
    def _normalized_schema_sql(sql: str) -> str:
        return " ".join(sql.split())

    @classmethod
    def _owner_trigger_fingerprints(
        cls,
        con: sqlite3.Connection,
    ) -> tuple[tuple[str, str, str, str], ...]:
        placeholders = ",".join("?" for _ in OWNER_TRIGGER_TABLES)
        return tuple(
            sorted(
                (
                    schema_name,
                    str(row["name"]),
                    str(row["tbl_name"]),
                    cls._normalized_schema_sql(str(row["sql"])),
                )
                for schema_name, catalog in (
                    ("main", "main.sqlite_master"),
                    ("temp", "sqlite_temp_master"),
                )
                for row in con.execute(
                    "SELECT name,tbl_name,sql FROM "
                    f"{catalog} WHERE type='trigger' "
                    f"AND tbl_name IN ({placeholders})",
                    OWNER_TRIGGER_TABLES,
                )
            )
        )

    @classmethod
    @lru_cache(maxsize=1)
    def _expected_owner_trigger_fingerprints(
        cls,
    ) -> tuple[tuple[str, str, str, str], ...]:
        expected_con = sqlite3.connect(":memory:")
        expected_con.row_factory = sqlite3.Row
        try:
            expected_con.executescript(DDL)
            return cls._owner_trigger_fingerprints(expected_con)
        finally:
            expected_con.close()

    @classmethod
    def _assert_owner_trigger_manifest(cls, con: sqlite3.Connection) -> None:
        expected = cls._expected_owner_trigger_fingerprints()
        actual = cls._owner_trigger_fingerprints(con)
        if actual == expected:
            return
        expected_identities = {
            (schema_name, name, table_name)
            for schema_name, name, table_name, _sql in expected
        }
        actual_identities = {
            (schema_name, name, table_name)
            for schema_name, name, table_name, _sql in actual
        }
        if actual_identities == expected_identities:
            raise ValueError("owner-trigger-sql-drift")
        raise ValueError("invalid-owner-authority-manifest")

    @staticmethod
    def _owner_write_authorizer(
        action: int,
        table_name: str | None,
        _column_name: str | None,
        _database_name: str | None,
        source: str | None,
    ) -> int:
        if (
            action == sqlite3.SQLITE_CREATE_TEMP_TRIGGER
            and table_name in _CANONICAL_OWNER_TRIGGER_NAMES
        ):
            return sqlite3.SQLITE_DENY
        if (
            action in _OWNER_WRITE_ACTIONS
            and table_name in OWNER_TRIGGER_TABLES
            and source is not None
            and source not in _CANONICAL_OWNER_TRIGGER_NAMES
        ):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    @classmethod
    def _install_owner_write_authorizer(cls, con: sqlite3.Connection) -> None:
        placeholders = ",".join("?" for _ in OWNER_JOURNAL_TRIGGER_NAMES)
        if con.execute(
            "SELECT 1 FROM sqlite_temp_master WHERE type='trigger' "
            f"AND name IN ({placeholders}) LIMIT 1",
            OWNER_JOURNAL_TRIGGER_NAMES,
        ).fetchone() is not None:
            raise ValueError("invalid-owner-authority-manifest")
        con.set_authorizer(cls._owner_write_authorizer)

    @staticmethod
    def _quoted_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    @classmethod
    def _owner_table_fingerprints(
        cls,
        con: sqlite3.Connection,
        table_names: tuple[str, ...] = _OWNER_TABLE_NAMES,
    ) -> dict[str, tuple[str, tuple[tuple[object, ...], ...]]]:
        fingerprints = {}
        for table_name in table_names:
            schema_row = con.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            if schema_row is None or schema_row["sql"] is None:
                raise ValueError("invalid-owner-authority-manifest")
            quoted_table = cls._quoted_identifier(table_name)
            columns = tuple(
                (
                    int(row["cid"]),
                    str(row["name"]),
                    str(row["type"]),
                    int(row["notnull"]),
                    row["dflt_value"],
                    int(row["pk"]),
                )
                for row in con.execute(f"PRAGMA table_xinfo({quoted_table})")
            )
            fingerprints[table_name] = (
                cls._normalized_schema_sql(str(schema_row["sql"])),
                columns,
            )
        return fingerprints

    @classmethod
    def _owner_index_fingerprints(
        cls,
        con: sqlite3.Connection,
        table_names: tuple[str, ...] = _OWNER_TABLE_NAMES,
    ) -> dict[str, tuple[object, ...]]:
        placeholders = ",".join("?" for _ in table_names)
        index_rows = con.execute(
            "SELECT name,tbl_name,sql FROM sqlite_master "
            f"WHERE type='index' AND sql IS NOT NULL AND tbl_name IN ({placeholders}) "
            "ORDER BY name",
            table_names,
        ).fetchall()
        fingerprints: dict[str, tuple[object, ...]] = {}
        for schema_row in index_rows:
            index_name = str(schema_row["name"])
            table_name = str(schema_row["tbl_name"])
            quoted_table = cls._quoted_identifier(table_name)
            list_row = next(
                (
                    row
                    for row in con.execute(f"PRAGMA index_list({quoted_table})")
                    if str(row["name"]) == index_name
                ),
                None,
            )
            if list_row is None:
                raise ValueError("invalid-owner-authority-manifest")
            quoted_index = cls._quoted_identifier(index_name)
            columns = tuple(
                (
                    int(row["seqno"]),
                    int(row["cid"]),
                    None if row["name"] is None else str(row["name"]),
                    None if row["coll"] is None else str(row["coll"]),
                    int(row["desc"]),
                    int(row["key"]),
                )
                for row in con.execute(f"PRAGMA index_xinfo({quoted_index})")
            )
            fingerprints[index_name] = (
                table_name,
                cls._normalized_schema_sql(str(schema_row["sql"])),
                int(list_row["unique"]),
                str(list_row["origin"]),
                int(list_row["partial"]),
                columns,
            )
        return fingerprints

    @classmethod
    def _expected_owner_manifests(
        cls,
    ) -> tuple[
        tuple[object, ...],
        tuple[object, ...],
        tuple[object, ...],
        tuple[object, ...],
        tuple[object, ...],
    ]:
        current_con = sqlite3.connect(":memory:")
        current_con.row_factory = sqlite3.Row
        v4_con = sqlite3.connect(":memory:")
        v4_con.row_factory = sqlite3.Row
        v3_con = sqlite3.connect(":memory:")
        v3_con.row_factory = sqlite3.Row
        v2_con = sqlite3.connect(":memory:")
        v2_con.row_factory = sqlite3.Row
        a527_con = sqlite3.connect(":memory:")
        a527_con.row_factory = sqlite3.Row
        try:
            current_con.executescript(DDL)

            def downgrade_to_v3(con: sqlite3.Connection) -> None:
                con.executescript(DDL)
                for name in (
                    set(OWNER_JOURNAL_TRIGGER_NAMES)
                    - set(V3_OWNER_JOURNAL_TRIGGER_NAMES)
                ):
                    con.execute(f'DROP TRIGGER "{name}"')
                for table_name in reversed(_V4_EVIDENCE_OWNER_TABLE_NAMES):
                    con.execute(f'DROP TABLE "{table_name}"')
                con.execute("DROP TABLE neg_risk_owner_mutation_guard")
                con.execute(V3_OWNER_MUTATION_GUARD_DDL)

            downgrade_to_v3(v3_con)
            downgrade_to_v3(v4_con)
            v4_con.executescript(V4_LEGACY_EVIDENCE_OWNER_DDL)
            v4_con.execute("DROP TABLE neg_risk_owner_mutation_guard")
            v4_con.execute(V4_OWNER_MUTATION_GUARD_DDL)
            v4_con.executescript(V4_LEGACY_OWNER_JOURNAL_TRIGGER_DDL)
            for historical_con, guard_ddl in (
                (v2_con, V2_OWNER_MUTATION_GUARD_DDL),
                (a527_con, A527_OWNER_MUTATION_GUARD_DDL),
            ):
                downgrade_to_v3(historical_con)
                for name in V3_OWNER_JOURNAL_TRIGGER_NAMES:
                    if "candidate_current_aggregate" in name:
                        historical_con.execute(
                            f'DROP TRIGGER "{name}"'
                        )
                historical_con.execute(
                    "ALTER TABLE neg_risk_candidate_current_aggregate "
                    "RENAME TO neg_risk_candidate_current_aggregate_v3"
                )
                historical_con.execute(V2_CANDIDATE_CURRENT_AGGREGATE_DDL)
                historical_con.execute(
                    "DROP TABLE neg_risk_candidate_current_aggregate_v3"
                )
                historical_con.execute(
                    "DROP INDEX "
                    "idx_neg_risk_candidate_current_opportunity_page"
                )
                historical_con.execute(
                    "DROP TABLE neg_risk_owner_mutation_guard"
                )
                historical_con.execute(guard_ddl)
                historical_con.executescript(V2_OWNER_JOURNAL_TRIGGER_DDL)

            def manifest(
                con: sqlite3.Connection,
                table_names: tuple[str, ...],
            ) -> tuple[object, ...]:
                return (
                    cls._owner_table_fingerprints(con, table_names),
                    cls._owner_index_fingerprints(con, table_names),
                    cls._owner_trigger_fingerprints(con),
                )
            manifests = (
                manifest(current_con, _OWNER_TABLE_NAMES),
                manifest(v4_con, _V4_OWNER_TABLE_NAMES),
                manifest(v3_con, _V3_OWNER_TABLE_NAMES),
                manifest(v2_con, _V3_OWNER_TABLE_NAMES),
                manifest(a527_con, _V3_OWNER_TABLE_NAMES),
            )
        finally:
            a527_con.close()
            v2_con.close()
            v3_con.close()
            v4_con.close()
            current_con.close()
        return manifests

    @classmethod
    def _owner_manifest_state(cls, con: sqlite3.Connection) -> str:
        owner_tables = set(_OWNER_TABLE_NAMES)
        v4_owner_tables = set(_V4_OWNER_TABLE_NAMES)
        v3_owner_tables = set(_V3_OWNER_TABLE_NAMES)
        placeholders = ",".join("?" for _ in owner_tables)
        present_tables = {
            str(row["name"])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                f"AND name IN ({placeholders})",
                tuple(owner_tables),
            )
        }
        present_triggers = cls._owner_trigger_fingerprints(con)
        if not present_tables and not present_triggers:
            return "fresh"
        if present_tables not in (owner_tables, v4_owner_tables, v3_owner_tables):
            raise ValueError("invalid-owner-authority-manifest")
        current_manifest, v4_manifest, v3_manifest, v2_manifest, a527_manifest = (
            cls._expected_owner_manifests()
        )
        table_names = (
            _OWNER_TABLE_NAMES
            if present_tables == owner_tables
            else (
                _V4_OWNER_TABLE_NAMES
                if present_tables == v4_owner_tables
                else _V3_OWNER_TABLE_NAMES
            )
        )
        actual_manifest = (
            cls._owner_table_fingerprints(con, table_names),
            cls._owner_index_fingerprints(con, table_names),
            cls._owner_trigger_fingerprints(con),
        )
        if actual_manifest == current_manifest:
            return "current"
        if actual_manifest == v4_manifest:
            return "v4"
        if actual_manifest == v3_manifest:
            return "v3"
        if actual_manifest == v2_manifest:
            return "v2"
        if actual_manifest == a527_manifest:
            return "a527"
        actual_triggers = actual_manifest[2]
        for expected_manifest in (
            current_manifest,
            v4_manifest,
            v3_manifest,
            v2_manifest,
            a527_manifest,
        ):
            if actual_manifest[:2] != expected_manifest[:2]:
                continue
            expected_triggers = expected_manifest[2]
            if {
                (schema_name, name, table_name)
                for schema_name, name, table_name, _sql in actual_triggers
            } == {
                (schema_name, name, table_name)
                for schema_name, name, table_name, _sql in expected_triggers
            }:
                raise ValueError("owner-trigger-sql-drift")
        raise ValueError("invalid-owner-authority-manifest")

    def _migrate_a527_owner_guard(self, con: sqlite3.Connection) -> None:
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        if (
            guard is None
            or guard["candidate_aggregate_hash"] is None
            or guard["discovery_aggregate_hash"] is None
        ):
            raise ValueError("invalid-owner-guard-state")
        self._validate_owner_journal_chain(
            con,
            guard=guard,
            max_rows=_OWNER_MUTATION_LEGACY_MAX_ROWS,
        )
        self._prune_owner_mutation_journal(
            con,
            consumed_journal_id=int(guard["consumed_journal_id"]),
        )
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        candidate_hash, discovery_hash = self._owner_aggregate_hashes_v2(con)
        if (
            guard is None
            or not hmac.compare_digest(
                str(guard["candidate_aggregate_hash"]),
                candidate_hash,
            )
            or not hmac.compare_digest(
                str(guard["discovery_aggregate_hash"]),
                discovery_hash,
            )
        ):
            raise ValueError("invalid-owner-aggregate-authority")
        con.execute(
            "ALTER TABLE neg_risk_owner_mutation_guard "
            "RENAME TO neg_risk_owner_mutation_guard_a527"
        )
        con.execute(V2_OWNER_MUTATION_GUARD_DDL)
        con.execute(
            "INSERT INTO neg_risk_owner_mutation_guard("
            "id,consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,authority_version,migration_state"
            ") SELECT id,consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,2,'complete' "
            "FROM neg_risk_owner_mutation_guard_a527",
        )
        con.execute("DROP TABLE neg_risk_owner_mutation_guard_a527")
        if self._owner_manifest_state(con) != "v2":
            raise ValueError("invalid-owner-authority-manifest")
        self._assert_v2_owner_journal_clean(con)

    @classmethod
    def _assert_v2_owner_journal_clean(
        cls,
        con: sqlite3.Connection,
    ) -> sqlite3.Row:
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        if (
            guard is None
            or int(guard["authority_version"]) != 2
            or guard["migration_state"] != "complete"
            or guard["candidate_aggregate_hash"] is None
            or guard["discovery_aggregate_hash"] is None
        ):
            raise ValueError("invalid-owner-guard-state")
        cls._validate_owner_journal_chain(
            con,
            guard=guard,
            max_rows=_OWNER_MUTATION_JOURNAL_RETAIN_ROWS,
        )
        candidate_hash, discovery_hash = cls._owner_aggregate_hashes_v2(con)
        if not hmac.compare_digest(
            str(guard["candidate_aggregate_hash"]),
            candidate_hash,
        ) or not hmac.compare_digest(
            str(guard["discovery_aggregate_hash"]),
            discovery_hash,
        ):
            raise ValueError("invalid-owner-aggregate-authority")
        return guard

    @staticmethod
    def _execute_trigger_ddl(
        con: sqlite3.Connection,
        ddl: str,
    ) -> None:
        for fragment in ddl.split("; END;"):
            statement = fragment.strip()
            if statement:
                con.execute(statement + "; END;")

    def _migrate_v2_owner_authority(self, con: sqlite3.Connection) -> None:
        if self._owner_manifest_state(con) != "v2":
            raise ValueError("invalid-owner-authority-manifest")
        self._assert_v2_owner_journal_clean(con)
        replay_opportunity_count = self.validated_candidate_opportunity_count(
            _connection=con
        )
        rows = con.execute(
            "SELECT * FROM neg_risk_candidate_current_authority "
            "ORDER BY group_id"
        ).fetchall()
        aggregate = con.execute(
            "SELECT current_group_count,opportunity_count,aggregate_digest "
            "FROM neg_risk_candidate_current_aggregate WHERE id=1"
        ).fetchone()
        if aggregate is None:
            raise ValueError("invalid-candidate-current-aggregate")

        migrated_rows: list[tuple[str, str, str]] = []
        state_counts = {
            "watching": 0,
            "no-edge": 0,
            "unavailable": 0,
        }
        opportunity_count = 0
        aggregate_digest = 0
        for row in rows:
            self._check_deadline("owner-v2-v3-migration-deadline")
            group = con.execute(
                "SELECT * FROM neg_risk_group_revisions "
                "WHERE group_id=? AND revision=?",
                (row["group_id"], row["group_revision"]),
            ).fetchone()
            fact = con.execute(
                "SELECT * FROM neg_risk_candidate_watch_facts WHERE id=?",
                (row["fact_id"],),
            ).fetchone()
            quote = (
                None
                if row["quote_batch_id"] is None
                else con.execute(
                    "SELECT * FROM neg_risk_group_quote_batches WHERE id=?",
                    (row["quote_batch_id"],),
                ).fetchone()
            )
            latest_group = self._current_group_row(
                con,
                str(row["group_id"]),
            )
            latest_fact = con.execute(
                "SELECT * FROM neg_risk_candidate_watch_facts "
                "WHERE group_id=? ORDER BY id DESC LIMIT 1",
                (row["group_id"],),
            ).fetchone()
            if (
                group is None
                or fact is None
                or latest_group is None
                or latest_fact is None
                or int(latest_group["id"]) != int(group["id"])
                or latest_group["status"] != "certified"
                or int(latest_fact["id"]) != int(fact["id"])
                or fact["group_id"] != group["group_id"]
                or row["group_id"] != group["group_id"]
            ):
                raise ValueError("invalid-candidate-current-authority")
            last_result = str(fact["last_result"])
            if last_result not in state_counts:
                raise ValueError("invalid-candidate-current-authority")
            success = last_result in {"watching", "no-edge"}
            numeric_fields = (
                "bundle_cost",
                "gross_edge_bps",
                "max_bundle_size",
            )
            if (
                success
                and (
                    quote is None
                    or quote["status"] != "complete"
                    or quote["group_id"] != group["group_id"]
                    or int(quote["group_revision"]) != int(group["revision"])
                    or quote["membership_hash"] != group["membership_hash"]
                    or fact["membership_hash"] != group["membership_hash"]
                    or fact["observed_at_ms"] != quote["quoted_at_ms"]
                    or any(
                        fact[name] is None
                        or not math.isfinite(float(fact[name]))
                        for name in numeric_fields
                    )
                )
            ) or (
                last_result == "unavailable"
                and (
                    quote is not None
                    or fact["quote_batch_id"] is not None
                    or any(fact[name] is not None for name in numeric_fields)
                )
            ):
                raise ValueError("invalid-candidate-current-authority")
            opportunity = int(
                last_result == "watching"
                and fact["gross_edge_bps"] is not None
                and float(fact["gross_edge_bps"]) > 0
                and quote is not None
                and quote["status"] == "complete"
                and quote["membership_hash"] == group["membership_hash"]
                and fact["membership_hash"] == group["membership_hash"]
            )
            v2_payload = {
                "event_id": str(group["event_id"]),
                "fact_id": int(fact["id"]),
                "group_id": str(group["group_id"]),
                "group_revision": int(group["revision"]),
                "last_result": last_result,
                "legs": (
                    None
                    if quote is None
                    else json.loads(str(quote["legs_json"]))
                ),
                "membership_hash": str(group["membership_hash"]),
                "opportunity": opportunity,
                "quote_started_at_ms": (
                    None if quote is None else int(quote["started_at_ms"])
                ),
                "quote_quoted_at_ms": (
                    None if quote is None else int(quote["quoted_at_ms"])
                ),
                "quote_batch_id": fact["quote_batch_id"],
            }
            v2_canonical = json.dumps(
                v2_payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            v2_hash = (
                "sha256:"
                + hashlib.sha256(v2_canonical.encode()).hexdigest()
            )
            if (
                row["event_id"] != group["event_id"]
                or row["membership_hash"] != group["membership_hash"]
                or row["quote_batch_id"] != fact["quote_batch_id"]
                or row["last_result"] != last_result
                or int(row["opportunity"]) != opportunity
                or row["legs_json"]
                != (None if quote is None else quote["legs_json"])
                or row["canonical_json"] != v2_canonical
                or row["row_hash"] != v2_hash
            ):
                raise ValueError("invalid-candidate-current-authority")
            v3_payload = {
                **v2_payload,
                "bundle_cost": fact["bundle_cost"],
                "fact_observed_at_ms": int(fact["observed_at_ms"]),
                "gross_edge_bps": fact["gross_edge_bps"],
                "max_bundle_size": fact["max_bundle_size"],
                "structure_observed_at_ms": int(group["observed_at_ms"]),
            }
            v3_canonical = json.dumps(
                v3_payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            v3_hash = (
                "sha256:"
                + hashlib.sha256(v3_canonical.encode()).hexdigest()
            )
            migrated_rows.append(
                (str(row["group_id"]), v3_canonical, v3_hash)
            )
            state_counts[last_result] += 1
            opportunity_count += opportunity
            aggregate_digest ^= int(v3_hash.removeprefix("sha256:"), 16)

        old_digest = 0
        for row in rows:
            old_digest ^= int(str(row["row_hash"]).removeprefix("sha256:"), 16)
        if (
            int(aggregate["current_group_count"]) != len(rows)
            or int(aggregate["opportunity_count"]) != opportunity_count
            or int(aggregate["opportunity_count"]) != replay_opportunity_count
            or str(aggregate["aggregate_digest"]) != f"{old_digest:064x}"
        ):
            raise ValueError("invalid-candidate-current-aggregate")

        writer_token = str(uuid.uuid4())
        con.execute(
            "INSERT INTO neg_risk_owner_write_context("
            "id,writer_token,table_name,operation,row_key"
            ") VALUES(1,?,'owner-v2-v3-migration','UPDATE','*')",
            (writer_token,),
        )
        expected_events: list[tuple[str, str, str]] = []
        for group_id, canonical, row_hash in migrated_rows:
            con.execute(
                "UPDATE neg_risk_candidate_current_authority "
                "SET canonical_json=?,row_hash=? WHERE group_id=?",
                (canonical, row_hash, group_id),
            )
            expected_events.append(
                (
                    "neg_risk_candidate_current_authority",
                    "UPDATE",
                    group_id,
                )
            )
        new_digest = f"{aggregate_digest:064x}"
        con.execute(
            "UPDATE neg_risk_candidate_current_aggregate SET "
            "current_group_count=?,opportunity_count=?,aggregate_digest=? "
            "WHERE id=1",
            (len(rows), opportunity_count, new_digest),
        )
        expected_events.append(
            (
                "neg_risk_candidate_current_aggregate",
                "UPDATE",
                "1",
            )
        )
        self._consume_expected_owner_events(
            con,
            writer_token=writer_token,
            expected_events=expected_events,
            finalize=False,
        )
        con.execute(
            "DELETE FROM neg_risk_owner_write_context "
            "WHERE id=1 AND writer_token=?",
            (writer_token,),
        )
        consumed = con.execute(
            "SELECT consumed_journal_id FROM "
            "neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        if consumed is None:
            raise ValueError("invalid-owner-guard-state")
        self._prune_owner_mutation_journal(
            con,
            consumed_journal_id=int(consumed["consumed_journal_id"]),
        )
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,discovery_aggregate_hash "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        if guard is None:
            raise ValueError("invalid-owner-guard-state")

        aggregate_trigger_names = (
            name
            for name in OWNER_JOURNAL_TRIGGER_NAMES
            if "candidate_current_aggregate" in name
        )
        for name in aggregate_trigger_names:
            con.execute(f'DROP TRIGGER "{name}"')
        con.execute(
            "ALTER TABLE neg_risk_candidate_current_aggregate "
            "RENAME TO neg_risk_candidate_current_aggregate_v2"
        )
        con.execute(CANDIDATE_CURRENT_AGGREGATE_DDL)
        con.execute(
            "INSERT INTO neg_risk_candidate_current_aggregate("
            "id,current_group_count,opportunity_count,watching_count,"
            "no_edge_count,unavailable_count,aggregate_digest"
            ") VALUES(1,?,?,?,?,?,?)",
            (
                len(rows),
                opportunity_count,
                state_counts["watching"],
                state_counts["no-edge"],
                state_counts["unavailable"],
                new_digest,
            ),
        )
        con.execute("DROP TABLE neg_risk_candidate_current_aggregate_v2")
        self._execute_trigger_ddl(
            con,
            CANDIDATE_CURRENT_AGGREGATE_TRIGGER_DDL,
        )
        con.execute(
            "ALTER TABLE neg_risk_owner_mutation_guard "
            "RENAME TO neg_risk_owner_mutation_guard_v2"
        )
        con.execute(V3_OWNER_MUTATION_GUARD_DDL)
        candidate_hash, discovery_hash = self._owner_aggregate_hashes(con)
        if not hmac.compare_digest(
            str(guard["discovery_aggregate_hash"]),
            discovery_hash,
        ):
            raise ValueError("invalid-owner-aggregate-authority")
        con.execute(
            "INSERT INTO neg_risk_owner_mutation_guard("
            "id,consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,authority_version,migration_state"
            ") VALUES(1,?,?,?,?,?,?,3,'complete')",
            (
                guard["consumed_journal_id"],
                guard["consumed_hash"],
                guard["retained_base_id"],
                guard["retained_base_hash"],
                candidate_hash,
                discovery_hash,
            ),
        )
        con.execute("DROP TABLE neg_risk_owner_mutation_guard_v2")
        con.execute(CANDIDATE_CURRENT_OPPORTUNITY_INDEX_DDL)
        if self._owner_manifest_state(con) != "v3":
            raise ValueError("invalid-owner-authority-manifest")
        self._assert_v3_owner_journal_clean(con)

    @classmethod
    def _assert_v3_owner_journal_clean(
        cls,
        con: sqlite3.Connection,
    ) -> sqlite3.Row:
        if cls._owner_manifest_state(con) != "v3":
            raise ValueError("invalid-owner-authority-manifest")
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        if (
            guard is None
            or int(guard["authority_version"]) != 3
            or guard["migration_state"] != "complete"
            or guard["candidate_aggregate_hash"] is None
            or guard["discovery_aggregate_hash"] is None
        ):
            raise ValueError("invalid-owner-guard-state")
        cls._validate_owner_journal_chain(
            con,
            guard=guard,
            max_rows=_OWNER_MUTATION_JOURNAL_RETAIN_ROWS,
        )
        candidate_hash, discovery_hash = cls._owner_aggregate_hashes(con)
        if not hmac.compare_digest(
            str(guard["candidate_aggregate_hash"]),
            candidate_hash,
        ) or not hmac.compare_digest(
            str(guard["discovery_aggregate_hash"]),
            discovery_hash,
        ):
            raise ValueError("invalid-owner-aggregate-authority")
        return guard

    def _migrate_v3_owner_authority(self, con: sqlite3.Connection) -> None:
        guard = self._assert_v3_owner_journal_clean(con)
        self._check_deadline("owner-v3-v4-migration-deadline")
        for statement in V4_EVIDENCE_OWNER_DDL.split(";"):
            if statement.strip():
                con.execute(statement)
        from polyarb.perception.incidents import IncidentManager

        IncidentManager(self)._bootstrap_v4_authority(con)
        self._execute_trigger_ddl(
            con,
            V4_EVIDENCE_OWNER_JOURNAL_TRIGGER_DDL,
        )
        con.execute(
            "ALTER TABLE neg_risk_owner_mutation_guard "
            "RENAME TO neg_risk_owner_mutation_guard_v3"
        )
        con.execute(OWNER_MUTATION_GUARD_DDL)
        con.execute(
            "INSERT INTO neg_risk_owner_mutation_guard("
            "id,consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,authority_version,migration_state"
            ") VALUES(1,?,?,?,?,?,?,5,'complete')",
            (
                guard["consumed_journal_id"],
                guard["consumed_hash"],
                guard["retained_base_id"],
                guard["retained_base_hash"],
                guard["candidate_aggregate_hash"],
                guard["discovery_aggregate_hash"],
            ),
        )
        con.execute("DROP TABLE neg_risk_owner_mutation_guard_v3")
        if self._owner_manifest_state(con) != "current":
            raise ValueError("invalid-owner-authority-manifest")
        self._assert_owner_journal_clean(con)

    @classmethod
    def _assert_v4_owner_journal_clean(
        cls,
        con: sqlite3.Connection,
    ) -> sqlite3.Row:
        if cls._owner_manifest_state(con) != "v4":
            raise ValueError("invalid-owner-authority-manifest")
        guard = con.execute(
            "SELECT consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,authority_version,migration_state "
            "FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        if (
            guard is None
            or int(guard["authority_version"]) != 4
            or guard["migration_state"] != "complete"
            or guard["candidate_aggregate_hash"] is None
            or guard["discovery_aggregate_hash"] is None
        ):
            raise ValueError("invalid-owner-guard-state")
        cls._validate_owner_journal_chain(
            con,
            guard=guard,
            max_rows=_OWNER_MUTATION_JOURNAL_RETAIN_ROWS,
        )
        candidate_hash, discovery_hash = cls._owner_aggregate_hashes(con)
        if not hmac.compare_digest(
            str(guard["candidate_aggregate_hash"]),
            candidate_hash,
        ) or not hmac.compare_digest(
            str(guard["discovery_aggregate_hash"]),
            discovery_hash,
        ):
            raise ValueError("invalid-owner-aggregate-authority")
        return guard

    def _migrate_v4_owner_authority(self, con: sqlite3.Connection) -> None:
        guard = self._assert_v4_owner_journal_clean(con)
        self._check_deadline("owner-v4-v5-migration-deadline")
        for name in (
            set(V4_LEGACY_OWNER_JOURNAL_TRIGGER_NAMES)
            - set(V3_OWNER_JOURNAL_TRIGGER_NAMES)
        ):
            con.execute(f'DROP TRIGGER "{name}"')
        from polyarb.perception.incidents import IncidentManager

        IncidentManager(self)._migrate_v4_to_v5_authority(con)
        self._execute_trigger_ddl(
            con,
            V4_EVIDENCE_OWNER_JOURNAL_TRIGGER_DDL,
        )
        con.execute(
            "ALTER TABLE neg_risk_owner_mutation_guard "
            "RENAME TO neg_risk_owner_mutation_guard_v4"
        )
        con.execute(OWNER_MUTATION_GUARD_DDL)
        con.execute(
            "INSERT INTO neg_risk_owner_mutation_guard("
            "id,consumed_journal_id,consumed_hash,retained_base_id,"
            "retained_base_hash,candidate_aggregate_hash,"
            "discovery_aggregate_hash,authority_version,migration_state"
            ") VALUES(1,?,?,?,?,?,?,5,'complete')",
            (
                guard["consumed_journal_id"],
                guard["consumed_hash"],
                guard["retained_base_id"],
                guard["retained_base_hash"],
                guard["candidate_aggregate_hash"],
                guard["discovery_aggregate_hash"],
            ),
        )
        con.execute("DROP TABLE neg_risk_owner_mutation_guard_v4")
        if self._owner_manifest_state(con) != "current":
            raise ValueError("invalid-owner-authority-manifest")
        self._assert_owner_journal_clean(con)

    @staticmethod
    def _migrate_quote_producer_authority(con: sqlite3.Connection) -> None:
        """Rebuild only legacy producer tables whose CHECK excludes Quote.

        SQLite cannot alter a CHECK constraint in place.  This caller already
        holds init_schema's immediate transaction, so the three tables move as
        one atomic authority migration: legacy receipts survive, Quote gains
        the same reservation/heartbeat contract, and no partial table set can
        become visible to a concurrent worker.
        """
        tables = (
            "neg_risk_producer_receipts",
            "neg_risk_producer_child_starts",
            "neg_risk_producer_heartbeats",
        )
        table_sql = {
            str(row["name"]): str(row["sql"] or "")
            for row in con.execute(
                "SELECT name,sql FROM sqlite_master "
                "WHERE type='table' AND name IN (?,?,?)",
                tables,
            )
        }
        if all("'quote'" in table_sql.get(table, "") for table in tables):
            return
        if set(table_sql) != set(tables):
            raise ValueError("invalid-producer-authority-schema")

        legacy_names = {table: f"{table}_quote_legacy" for table in tables}
        con.execute("DROP INDEX IF EXISTS idx_neg_risk_producer_heartbeat_component")
        for table in tables:
            con.execute(f"ALTER TABLE {table} RENAME TO {legacy_names[table]}")
        con.execute(
            "CREATE TABLE neg_risk_producer_receipts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "component TEXT NOT NULL CHECK(component IN "
            "('candidate','discovery','reconciliation','quote')),"
            "attempt INTEGER NOT NULL CHECK(attempt >= 1),"
            "started_at_ms INTEGER NOT NULL CHECK(started_at_ms >= 0),"
            "finished_at_ms INTEGER NOT NULL CHECK(finished_at_ms >= started_at_ms),"
            "outcome TEXT NOT NULL CHECK(outcome IN "
            "('success','nonzero','timeout','cancelled','spawn-error')),"
            "exit_code INTEGER,stdout_tail TEXT NOT NULL,stderr_tail TEXT NOT NULL,"
            "output_hash TEXT NOT NULL,supervisor_run_id TEXT NOT NULL,"
            "child_nonce TEXT NOT NULL DEFAULT '',auth_domain TEXT NOT NULL,"
            "child_auth_hash TEXT,UNIQUE(component,attempt))"
        )
        con.execute(
            "CREATE TABLE neg_risk_producer_child_starts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "component TEXT NOT NULL CHECK(component IN "
            "('candidate','discovery','reconciliation','quote')),"
            "supervisor_run_id TEXT NOT NULL,child_nonce TEXT NOT NULL DEFAULT '',"
            "attempt INTEGER NOT NULL CHECK(attempt >= 1),"
            "started_at_ms INTEGER NOT NULL CHECK(started_at_ms >= 0),"
            "auth_domain TEXT NOT NULL,child_auth_hash TEXT,claimed_at_ms INTEGER,"
            "UNIQUE(component,supervisor_run_id,attempt),UNIQUE(component,attempt))"
        )
        con.execute(
            "CREATE TABLE neg_risk_producer_heartbeats ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "component TEXT NOT NULL CHECK(component IN "
            "('candidate','discovery','reconciliation','quote')),"
            "supervisor_run_id TEXT NOT NULL,child_nonce TEXT NOT NULL DEFAULT '',"
            "attempt INTEGER NOT NULL CHECK(attempt >= 1),auth_domain TEXT NOT NULL,"
            "child_auth_hash TEXT NOT NULL,sequence INTEGER NOT NULL CHECK(sequence >= 1),"
            "observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms >= 0),"
            "state TEXT NOT NULL CHECK(state IN ('progress','yielded','paused')),"
            "UNIQUE(component,supervisor_run_id,attempt,sequence))"
        )
        con.execute(
            "INSERT INTO neg_risk_producer_receipts("
            "id,component,attempt,started_at_ms,finished_at_ms,outcome,exit_code,"
            "stdout_tail,stderr_tail,output_hash,supervisor_run_id,child_nonce,"
            "auth_domain,child_auth_hash) SELECT "
            "id,component,attempt,started_at_ms,finished_at_ms,outcome,exit_code,"
            "stdout_tail,stderr_tail,output_hash,supervisor_run_id,child_nonce,"
            "auth_domain,child_auth_hash FROM neg_risk_producer_receipts_quote_legacy"
        )
        con.execute(
            "INSERT INTO neg_risk_producer_child_starts("
            "id,component,supervisor_run_id,child_nonce,attempt,started_at_ms,"
            "auth_domain,child_auth_hash,claimed_at_ms) SELECT "
            "id,component,supervisor_run_id,child_nonce,attempt,started_at_ms,"
            "auth_domain,child_auth_hash,claimed_at_ms "
            "FROM neg_risk_producer_child_starts_quote_legacy"
        )
        con.execute(
            "INSERT INTO neg_risk_producer_heartbeats("
            "id,component,supervisor_run_id,child_nonce,attempt,auth_domain,"
            "child_auth_hash,sequence,observed_at_ms,state) SELECT "
            "id,component,supervisor_run_id,child_nonce,attempt,auth_domain,"
            "child_auth_hash,sequence,observed_at_ms,state "
            "FROM neg_risk_producer_heartbeats_quote_legacy"
        )
        con.execute(
            "CREATE INDEX idx_neg_risk_producer_heartbeat_component "
            "ON neg_risk_producer_heartbeats(component,id)"
        )
        for table in tables:
            con.execute(f"DROP TABLE {legacy_names[table]}")

    def init_schema(self) -> None:
        con = self._connect()
        try:
            owner_manifest_state = self._owner_manifest_state(con)
            if owner_manifest_state in {"a527", "v2", "v3", "v4"}:
                con.execute("BEGIN IMMEDIATE")
                locked_state = self._owner_manifest_state(con)
                if locked_state == "a527":
                    self._migrate_a527_owner_guard(con)
                    locked_state = "v2"
                if locked_state == "v2":
                    self._migrate_v2_owner_authority(con)
                    locked_state = "v3"
                if locked_state == "v3":
                    self._migrate_v3_owner_authority(con)
                    locked_state = "current"
                if locked_state == "v4":
                    self._migrate_v4_owner_authority(con)
                elif locked_state != "current":
                    raise ValueError("invalid-owner-authority-manifest")
                con.execute("COMMIT")
                owner_manifest_state = "current"
            legacy_owner_bootstrap = owner_manifest_state == "fresh"
            schedule_evidence_existed = (
                con.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' "
                    "AND name='neg_risk_discovery_schedule_evidence'"
                ).fetchone()
                is not None
            )
            con.executescript(DDL)
            if migrate_fault_events_cleanup_confirmation(con):
                con.executescript(DDL)
            if migrate_fault_auth_finalize(con):
                con.executescript(DDL)
            self._validate_owner_trigger_sql(con)
            con.execute("BEGIN IMMEDIATE")
            locked_manifest_state = self._owner_manifest_state(con)
            if (
                owner_manifest_state == "a527"
                and locked_manifest_state not in {"a527", "current"}
            ):
                raise ValueError("invalid-owner-authority-manifest")
            owner_manifest_state = locked_manifest_state
            if owner_manifest_state == "a527":
                self._migrate_a527_owner_guard(con)
            guard = con.execute(
                "SELECT consumed_journal_id FROM neg_risk_owner_mutation_guard "
                "WHERE id=1"
            ).fetchone()
            owner_guard_bootstrap = guard is None
            journal_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM neg_risk_owner_mutation_journal"
                ).fetchone()[0]
            )
            if guard is None:
                if journal_count:
                    raise ValueError("pending-owner-mutation")
                con.execute(
                    "INSERT INTO neg_risk_owner_mutation_guard("
                    "id,consumed_journal_id,consumed_hash,retained_base_id,"
                    "retained_base_hash,candidate_aggregate_hash,"
                    "discovery_aggregate_hash,authority_version,migration_state"
                    ") VALUES(1,0,NULL,0,NULL,?,?,?,'complete')",
                    (
                        *self._owner_aggregate_hashes(con),
                        _OWNER_AUTHORITY_VERSION,
                    ),
                )
            self._assert_owner_journal_clean(con)
            aggregate_exists = con.execute(
                "SELECT 1 FROM neg_risk_candidate_current_aggregate WHERE id=1"
            ).fetchone()
            if aggregate_exists is None:
                aggregate_token = self._begin_expected_owner_mutation(
                    con,
                    table_name="neg_risk_candidate_current_aggregate",
                    operation="INSERT",
                    row_key="1",
                )
                con.execute(
                    "INSERT INTO neg_risk_candidate_current_aggregate("
                    "id,current_group_count,opportunity_count,watching_count,"
                    "no_edge_count,unavailable_count,aggregate_digest"
                    ") VALUES(1,0,0,0,0,0,?)",
                    ("0" * 64,),
                )
                self._consume_expected_owner_mutation(
                    con,
                    writer_token=aggregate_token,
                    table_name="neg_risk_candidate_current_aggregate",
                    operation="INSERT",
                    row_key="1",
                )
            if owner_guard_bootstrap:
                from polyarb.perception.incidents import IncidentManager

                IncidentManager(self)._initialize_empty_v4_authority(con)
            additive_operator_columns = {
                "neg_risk_operator_auth_nonces": {
                    "request_method": "TEXT",
                    "body_hash": "TEXT",
                    "auth_hash": "TEXT",
                },
                "neg_risk_operator_queue": {
                    "request_auth_hash": "TEXT",
                    "last_sequence": "INTEGER",
                    "last_receipt_hash": "TEXT",
                },
                "neg_risk_operator_queue_receipts": {
                    "sequence": "INTEGER",
                    "auth_nonce": "TEXT",
                    "auth_receipt_hash": "TEXT",
                    "previous_hash": "TEXT",
                    "receipt_hash": "TEXT",
                },
            }
            operator_original_columns: dict[str, set[str]] = {}
            for table, additions in additive_operator_columns.items():
                columns = {
                    str(row["name"])
                    for row in con.execute(f"PRAGMA table_info({table})")
                }
                operator_original_columns[table] = columns
                for name, declaration in additions.items():
                    if name not in columns:
                        con.execute(
                            f"ALTER TABLE {table} ADD COLUMN {name} {declaration}"
                        )
            receipt_columns = {
                str(row["name"])
                for row in con.execute(
                    "PRAGMA table_info(neg_risk_producer_receipts)"
                )
            }
            output_hash_needs_backfill = "output_hash" not in receipt_columns
            self._migrate_legacy_operator_proofs(
                con,
                legacy=(
                    "request_method"
                    not in operator_original_columns[
                        "neg_risk_operator_auth_nonces"
                    ]
                ),
                receipt_auth_upgrade=(
                    "auth_receipt_hash"
                    not in operator_original_columns[
                        "neg_risk_operator_queue_receipts"
                    ]
                ),
            )
            migrations = (
                (
                    "neg_risk_group_schedule",
                    "promotion_eligible_at_ms",
                    "INTEGER",
                ),
                (
                    "neg_risk_group_schedule",
                    "promotion_queue_deadline_at_ms",
                    "INTEGER",
                ),
                (
                    "neg_risk_group_schedule",
                    "candidate_start_deadline_at_ms",
                    "INTEGER",
                ),
                ("neg_risk_discovery_batches", "sweep_id", "INTEGER"),
                ("neg_risk_discovery_batches", "batch_sequence", "INTEGER"),
                (
                    "neg_risk_discovery_load_state",
                    "probe_every_cycles",
                    "INTEGER NOT NULL DEFAULT 10",
                ),
                (
                    "neg_risk_discovery_admission_state",
                    "selection_budget_ms",
                    "INTEGER NOT NULL DEFAULT 6000",
                ),
                (
                    "neg_risk_discovery_admission_state",
                    "terminal_write_budget_ms",
                    "INTEGER NOT NULL DEFAULT 5000",
                ),
                ("neg_risk_candidate_attempt_starts", "event_id", "TEXT"),
                (
                    "neg_risk_candidate_attempt_starts",
                    "membership_hash",
                    "TEXT",
                ),
                (
                    "neg_risk_candidate_attempt_starts",
                    "promoted_at_ms",
                    "INTEGER",
                ),
                (
                    "neg_risk_candidate_attempt_starts",
                    "candidate_max_wait_ms",
                    "INTEGER",
                ),
                ("neg_risk_discovery_batch_samples", "event_id", "TEXT"),
                (
                    "neg_risk_discovery_batch_samples",
                    "membership_hash",
                    "TEXT",
                ),
                ("neg_risk_discovery_batch_samples", "quality", "TEXT"),
                ("neg_risk_discovery_batch_samples", "reason", "TEXT"),
                (
                    "neg_risk_discovery_admission_state",
                    "attempt_start_write_budget_ms",
                    "INTEGER NOT NULL DEFAULT 5000",
                ),
                (
                    "neg_risk_reconciliation_windows",
                    "baseline_count",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "neg_risk_reconciliation_windows",
                    "failure_reason",
                    "TEXT",
                ),
                (
                    "neg_risk_reconciliation_windows",
                    "observations_count",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "neg_risk_reconciliation_windows",
                    "baseline_digest",
                    "TEXT",
                ),
                (
                    "neg_risk_reconciliation_batches",
                    "observed_count",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "neg_risk_reconciliation_batches",
                    "unique_count",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "neg_risk_reconciliation_batches",
                    "update_count",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "neg_risk_reconciliation_batches",
                    "duplicate_count",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "neg_risk_candidate_current_authority",
                    "opportunity",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "neg_risk_candidate_current_authority",
                    "canonical_json",
                    "TEXT NOT NULL DEFAULT '{}'",
                ),
                (
                    "neg_risk_discovery_status_projection",
                    "owner_journal_id",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                (
                    "neg_risk_discovery_group_projection",
                    "visit_anchor_ms",
                    "INTEGER",
                ),
                *(
                    (
                        "neg_risk_discovery_status_projection",
                        name,
                        "INTEGER NOT NULL DEFAULT 0",
                    )
                    for name in (
                        "group_count",
                        "queue_high",
                        "queue_normal",
                        "queue_explore",
                        "promotion_queue_depth",
                        "outstanding_admitted_count",
                    )
                ),
                (
                    "neg_risk_discovery_status_projection",
                    "total_liquidity_weight",
                    "REAL NOT NULL DEFAULT 0",
                ),
                (
                    "neg_risk_resource_decisions",
                    "policy_version",
                    "TEXT",
                ),
                (
                    "neg_risk_resource_decisions",
                    "sequence",
                    "INTEGER",
                ),
                (
                    "neg_risk_producer_heartbeats",
                    "supervisor_run_id",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_heartbeats",
                    "child_nonce",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_heartbeats",
                    "sequence",
                    "INTEGER",
                ),
                (
                    "neg_risk_producer_receipts",
                    "supervisor_run_id",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_receipts",
                    "child_nonce",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_receipts",
                    "auth_domain",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_receipts",
                    "child_auth_hash",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_receipts",
                    "output_hash",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_child_starts",
                    "auth_domain",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_child_starts",
                    "child_auth_hash",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_child_starts",
                    "claimed_at_ms",
                    "INTEGER",
                ),
                (
                    "neg_risk_producer_heartbeats",
                    "attempt",
                    "INTEGER",
                ),
                (
                    "neg_risk_producer_heartbeats",
                    "auth_domain",
                    "TEXT",
                ),
                (
                    "neg_risk_producer_heartbeats",
                    "child_auth_hash",
                    "TEXT",
                ),
                (
                    "neg_risk_http_probe_receipts",
                    "observed_release_id",
                    "TEXT",
                ),
                (
                    "neg_risk_http_probe_receipts",
                    "probe_nonce",
                    "TEXT",
                ),
            )
            for table, column, definition in migrations:
                existing = {str(row["name"]) for row in con.execute(f"PRAGMA table_info({table})")}
                if column not in existing:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            if output_hash_needs_backfill:
                receipt_hashes: list[tuple[str, int]] = []
                for row in con.execute(
                    "SELECT id,stdout_tail,stderr_tail "
                    "FROM neg_risk_producer_receipts ORDER BY id"
                ).fetchall():
                    if not (
                        _valid_producer_receipt_tail(row["stdout_tail"])
                        and _valid_producer_receipt_tail(row["stderr_tail"])
                    ):
                        raise ValueError(
                            "invalid-producer-receipt-output-migration"
                        )
                    receipt_hashes.append(
                        (
                            _producer_receipt_output_hash(
                                row["stdout_tail"],
                                row["stderr_tail"],
                            ),
                            int(row["id"]),
                        )
                    )
                for output_hash, receipt_id in receipt_hashes:
                    con.execute(
                        "UPDATE neg_risk_producer_receipts SET output_hash=? "
                        "WHERE id=? AND output_hash IS NULL",
                        (output_hash, receipt_id),
                    )
            self._migrate_quote_producer_authority(con)
            sweep_id = 1
            batch_sequence = 0
            previous_completed = False
            for row in con.execute(
                "SELECT id,sweep_id,batch_sequence,completed "
                "FROM neg_risk_discovery_batches ORDER BY id"
            ).fetchall():
                if batch_sequence == 0 or previous_completed:
                    if previous_completed:
                        sweep_id += 1
                    batch_sequence = 1
                else:
                    batch_sequence += 1
                if row["sweep_id"] is None or row["batch_sequence"] is None:
                    con.execute(
                        "UPDATE neg_risk_discovery_batches "
                        "SET sweep_id=?,batch_sequence=? WHERE id=?",
                        (sweep_id, batch_sequence, int(row["id"])),
                    )
                previous_completed = bool(row["completed"])
            if not schedule_evidence_existed:
                con.execute(
                    "INSERT INTO neg_risk_discovery_schedule_evidence("
                    "batch_id,group_id,event_id,membership_hash,quality,reason,"
                    "promoted,effective_at_ms"
                    ") SELECT bs.batch_id,bs.group_id,bs.event_id,"
                    "bs.membership_hash,bs.quality,bs.reason,bs.promoted,"
                    "b.finished_at_ms FROM neg_risk_discovery_batch_samples bs "
                    "JOIN neg_risk_discovery_batches b ON b.id=bs.batch_id "
                    "WHERE bs.event_id IS NOT NULL "
                    "AND bs.membership_hash IS NOT NULL "
                    "AND bs.quality IS NOT NULL"
                )
            self._compact_discovery_authority(con)
            legacy_reconciliations = con.execute(
                "SELECT w.* FROM neg_risk_reconciliation_windows w "
                "WHERE w.baseline_digest IS NOT NULL "
                "AND EXISTS(SELECT 1 FROM neg_risk_reconciliation_batches b "
                "WHERE b.window_id=w.id) "
                "AND NOT EXISTS("
                "SELECT 1 FROM neg_risk_reconciliation_authority_checkpoints c "
                "WHERE c.window_id=w.id) ORDER BY w.rowid"
            ).fetchall()
            for legacy_reconciliation in legacy_reconciliations:
                window_id = str(legacy_reconciliation["id"])
                receipts = con.execute(
                    "SELECT * FROM neg_risk_reconciliation_batches "
                    "WHERE window_id=? ORDER BY batch_sequence",
                    (window_id,),
                ).fetchall()
                batch_samples = con.execute(
                    "SELECT s.* FROM neg_risk_reconciliation_batch_samples s "
                    "JOIN neg_risk_reconciliation_batches b ON b.id=s.batch_id "
                    "WHERE b.window_id=? ORDER BY s.batch_id,s.group_id",
                    (window_id,),
                ).fetchall()
                staged = con.execute(
                    "SELECT * FROM neg_risk_reconciliation_staging "
                    "WHERE window_id=? ORDER BY group_id",
                    (window_id,),
                ).fetchall()
                baseline = con.execute(
                    "SELECT * FROM neg_risk_reconciliation_baseline "
                    "WHERE window_id=? ORDER BY group_id",
                    (window_id,),
                ).fetchall()
                evidence = con.execute(
                    "SELECT * FROM neg_risk_reconciliation_diff_evidence "
                    "WHERE window_id=? ORDER BY group_id,action",
                    (window_id,),
                ).fetchall()
                result_revisions = self._reconciliation_evidence_result_revisions(
                    con,
                    window_id,
                )
                self._validate_reconciliation_snapshot(
                    legacy_reconciliation,
                    receipts,
                    batch_samples,
                    staged,
                    baseline,
                    evidence,
                    result_revisions,
                )
                self._checkpoint_reconciliation_authority(
                    con,
                    window_id,
                )
            if legacy_owner_bootstrap:
                for row in con.execute(
                    "SELECT DISTINCT group_id "
                    "FROM neg_risk_candidate_watch_facts ORDER BY group_id"
                ).fetchall():
                    self._sync_candidate_current_authority(
                        con,
                        str(row["group_id"]),
                    )
            self._refresh_discovery_status_projection(con)
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def discovery_cursor(self) -> str | None:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT next_cursor,completed FROM neg_risk_discovery_state WHERE id=1"
            ).fetchone()
        finally:
            con.close()
        if row is None or bool(row["completed"]):
            return None
        return None if row["next_cursor"] is None else str(row["next_cursor"])

    def record_discovery_load_decision(
        self,
        *,
        degraded_reason: str | None,
        probe_every_cycles: int,
        now_ms: int,
    ) -> DiscoveryLoadState:
        if probe_every_cycles < 2:
            raise ValueError("discovery-probe-period-must-be-at-least-two")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            self._assert_owner_journal_clean(con)
            row = con.execute(
                "SELECT degraded_streak FROM neg_risk_discovery_load_state WHERE id=1"
            ).fetchone()
            if degraded_reason is None:
                streak = 0
                decision = "fresh"
            else:
                streak = (0 if row is None else int(row["degraded_streak"])) + 1
                decision = "probe" if streak % probe_every_cycles == 0 else "yield"
            con.execute(
                "INSERT INTO neg_risk_discovery_load_state("
                "id,degraded_streak,last_reason,last_decision,"
                "probe_every_cycles,updated_at_ms"
                ") VALUES (1,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "degraded_streak=excluded.degraded_streak,"
                "last_reason=excluded.last_reason,"
                "last_decision=excluded.last_decision,"
                "probe_every_cycles=excluded.probe_every_cycles,"
                "updated_at_ms=excluded.updated_at_ms",
                (
                    streak,
                    degraded_reason,
                    decision,
                    probe_every_cycles,
                    now_ms,
                ),
            )
            con.execute("COMMIT")
            return DiscoveryLoadState(
                streak,
                degraded_reason,
                decision,
                probe_every_cycles,
                now_ms,
            )
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def discovery_load_state(self) -> DiscoveryLoadState:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT degraded_streak,last_reason,last_decision,"
                "probe_every_cycles,updated_at_ms "
                "FROM neg_risk_discovery_load_state WHERE id=1"
            ).fetchone()
        finally:
            con.close()
        if row is None:
            return DiscoveryLoadState(0, None, "fresh", 10, 0)
        return DiscoveryLoadState(
            int(row["degraded_streak"]),
            None if row["last_reason"] is None else str(row["last_reason"]),
            row["last_decision"],
            int(row["probe_every_cycles"]),
            int(row["updated_at_ms"]),
        )

    def configure_discovery_admission(
        self,
        proof: DiscoveryAdmissionProof,
        *,
        now_ms: int,
    ) -> None:
        """Persist active controller proof, then reconcile only factless promotions."""
        proof.validate()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute(
                "SELECT * FROM neg_risk_discovery_admission_state WHERE id=1"
            ).fetchone()
            outstanding = int(
                con.execute(
                    "SELECT COUNT(*) FROM neg_risk_group_schedule s "
                    "WHERE s.promoted_at_ms IS NOT NULL AND NOT EXISTS ("
                    "SELECT 1 FROM neg_risk_candidate_watch_facts f "
                    "WHERE f.group_id=s.group_id)"
                ).fetchone()[0]
            )
            if (
                existing is not None
                and outstanding > 0
                and self._proof_timing(existing) != self._proof_timing(proof)
            ):
                raise ValueError("discovery-admission-timing-change-with-outstanding-work")
            self._persist_admission_proof(con, proof=proof, now_ms=now_ms)
            supported_keys = [
                str(row["group_id"])
                for row in con.execute(
                    "SELECT group_id FROM neg_risk_group_schedule "
                    "WHERE quality='complete-supported' ORDER BY group_id"
                )
            ]
            self._execute_expected_owner_bulk(
                con,
                table_name="neg_risk_group_schedule",
                operation="UPDATE",
                row_keys=supported_keys,
                sql=(
                "UPDATE neg_risk_group_schedule SET "
                "promotion_eligible_at_ms=COALESCE("
                "promotion_eligible_at_ms,first_discovered_at_ms),"
                "promotion_queue_deadline_at_ms=COALESCE("
                "promotion_eligible_at_ms,first_discovered_at_ms)+? "
                "WHERE quality='complete-supported'"
                ),
                parameters=(proof.candidate_max_wait_ms,),
            )
            factless = con.execute(
                "SELECT s.group_id FROM neg_risk_group_schedule s "
                "WHERE s.promoted_at_ms IS NOT NULL AND NOT EXISTS ("
                "SELECT 1 FROM neg_risk_candidate_watch_facts f "
                "WHERE f.group_id=s.group_id) "
                "ORDER BY s.promotion_queue_deadline_at_ms,"
                "CAST(s.priority_score AS REAL) DESC,s.group_id"
            ).fetchall()
            for row in factless[proof.effective_capacity :]:
                self._execute_expected_owner_bulk(
                    con,
                    table_name="neg_risk_group_schedule",
                    operation="UPDATE",
                    row_keys=[str(row["group_id"])],
                    sql=(
                        "UPDATE neg_risk_group_schedule SET promoted_at_ms=NULL,"
                        "candidate_start_deadline_at_ms=NULL WHERE group_id=?"
                    ),
                    parameters=(str(row["group_id"]),),
                )
            missing_deadline_keys = [
                str(row["group_id"])
                for row in con.execute(
                    "SELECT group_id FROM neg_risk_group_schedule "
                    "WHERE promoted_at_ms IS NOT NULL "
                    "AND candidate_start_deadline_at_ms IS NULL ORDER BY group_id"
                )
            ]
            self._execute_expected_owner_bulk(
                con,
                table_name="neg_risk_group_schedule",
                operation="UPDATE",
                row_keys=missing_deadline_keys,
                sql=(
                "UPDATE neg_risk_group_schedule SET "
                "candidate_start_deadline_at_ms=promoted_at_ms+? "
                "WHERE promoted_at_ms IS NOT NULL "
                "AND candidate_start_deadline_at_ms IS NULL"
                ),
                parameters=(proof.candidate_max_wait_ms,),
            )
            self._record_existing_candidate_admissions(
                con,
                proof_row=con.execute(
                    "SELECT * FROM neg_risk_discovery_admission_state WHERE id=1"
                ).fetchone(),
                recorded_at_ms=now_ms,
            )
            self._admit_waiting_candidates(con, now_ms=now_ms)
            self._refresh_discovery_status_projection(con)
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def discovery_admission_proof(self) -> DiscoveryAdmissionProof | None:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT * FROM neg_risk_discovery_admission_state WHERE id=1"
            ).fetchone()
        finally:
            con.close()
        return None if row is None else self._admission_proof_from_row(row)

    def publish_discovery_batch(
        self,
        *,
        requested_cursor: str | None,
        next_cursor: str | None,
        completed: bool,
        started_at_ms: int,
        finished_at_ms: int,
        page_event_count: int,
        candidates: tuple[DiscoveryScheduleCandidate, ...],
        admission_proof: DiscoveryAdmissionProof,
    ) -> tuple[int, tuple[str, ...]]:
        """Atomically certify, schedule, sample, promote, and advance a page."""
        admission_proof.validate()
        if started_at_ms > finished_at_ms:
            raise ValueError("invalid-discovery-timestamp-order")
        if completed != (next_cursor is None):
            raise ValueError("invalid-discovery-completion-cursor")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            if con.execute(
                "SELECT EXISTS(SELECT 1 FROM neg_risk_discovery_batches) "
                "OR EXISTS(SELECT 1 FROM neg_risk_discovery_authority_checkpoints)"
            ).fetchone()[0]:
                self.discovery_status(now_ms=started_at_ms, _connection=con)
            state = con.execute(
                "SELECT next_cursor,completed FROM neg_risk_discovery_state WHERE id=1"
            ).fetchone()
            expected_cursor = (
                None if state is None or bool(state["completed"]) else state["next_cursor"]
            )
            if requested_cursor != expected_cursor:
                raise ValueError("discovery-cursor-race")
            latest_receipt = con.execute(
                "SELECT sweep_id,batch_sequence,next_cursor,completed "
                "FROM neg_risk_discovery_batches ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if latest_receipt is None:
                sweep_id, batch_sequence = 1, 1
            elif bool(latest_receipt["completed"]):
                sweep_id = int(latest_receipt["sweep_id"]) + 1
                batch_sequence = 1
            else:
                sweep_id = int(latest_receipt["sweep_id"])
                batch_sequence = int(latest_receipt["batch_sequence"]) + 1
            configured = con.execute(
                "SELECT * FROM neg_risk_discovery_admission_state WHERE id=1"
            ).fetchone()
            if configured is None or self._admission_proof_from_row(configured) != admission_proof:
                raise ValueError("discovery-admission-proof-not-configured")

            for candidate in candidates:
                self._insert_discovery_schedule(
                    con,
                    candidate=candidate,
                    source_cursor=requested_cursor,
                    started_at_ms=started_at_ms,
                    finished_at_ms=finished_at_ms,
                    candidate_max_wait_ms=(admission_proof.candidate_max_wait_ms),
                )
                con.execute(
                    "INSERT INTO neg_risk_coverage_samples("
                    "sampled_at_ms,group_id,source_cursor,liquidity_weight"
                    ") VALUES (?,?,?,?)",
                    (
                        finished_at_ms,
                        candidate.group_id,
                        requested_cursor,
                        str(candidate.liquidity_weight),
                    ),
                )
            self._admit_waiting_candidates(con, now_ms=finished_at_ms)
            promoted = (
                [
                    (
                        Decimal(str(row["priority_score"])),
                        str(row["group_id"]),
                    )
                    for row in con.execute(
                        "SELECT group_id,priority_score "
                        "FROM neg_risk_group_schedule "
                        "WHERE promoted_at_ms IS NOT NULL AND group_id IN "
                        f"({','.join('?' for _ in candidates)})",
                        tuple(candidate.group_id for candidate in candidates),
                    ).fetchall()
                ]
                if candidates
                else []
            )
            promoted.sort(key=lambda item: (-item[0], item[1]))
            promoted_ids = {group_id for _, group_id in promoted}
            receipt = con.execute(
                "INSERT INTO neg_risk_discovery_batches("
                "sweep_id,batch_sequence,requested_cursor,next_cursor,"
                "completed,started_at_ms,"
                "finished_at_ms,page_event_count,groups_seen,promoted_count"
                ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    sweep_id,
                    batch_sequence,
                    requested_cursor,
                    next_cursor,
                    int(completed),
                    started_at_ms,
                    finished_at_ms,
                    page_event_count,
                    len(candidates),
                    len(promoted),
                ),
            )
            batch_id = int(receipt.lastrowid)
            con.executemany(
                "INSERT INTO neg_risk_discovery_batch_samples("
                "batch_id,group_id,event_id,membership_hash,quality,reason,"
                "liquidity_weight,promoted) VALUES (?,?,?,?,?,?,?,?)",
                [
                    (
                        batch_id,
                        candidate.group_id,
                        candidate.event_id,
                        candidate.membership_hash,
                        candidate.quality,
                        candidate.reason,
                        str(candidate.liquidity_weight),
                        int(candidate.group_id in promoted_ids),
                    )
                    for candidate in candidates
                ],
            )
            con.executemany(
                "INSERT INTO neg_risk_discovery_schedule_evidence("
                "batch_id,group_id,event_id,membership_hash,quality,reason,"
                "promoted,effective_at_ms) VALUES (?,?,?,?,?,?,?,?)",
                [
                    (
                        batch_id,
                        candidate.group_id,
                        candidate.event_id,
                        candidate.membership_hash,
                        candidate.quality,
                        candidate.reason,
                        int(candidate.group_id in promoted_ids),
                        finished_at_ms,
                    )
                    for candidate in candidates
                ],
            )
            con.execute(
                "INSERT INTO neg_risk_discovery_state("
                "id,next_cursor,completed,last_started_at_ms,last_finished_at_ms,"
                "page_event_count,groups_seen,promoted_count"
                ") VALUES (1,?,?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "next_cursor=excluded.next_cursor,completed=excluded.completed,"
                "last_started_at_ms=excluded.last_started_at_ms,"
                "last_finished_at_ms=excluded.last_finished_at_ms,"
                "page_event_count=excluded.page_event_count,"
                "groups_seen=excluded.groups_seen,"
                "promoted_count=excluded.promoted_count",
                (
                    next_cursor,
                    int(completed),
                    started_at_ms,
                    finished_at_ms,
                    page_event_count,
                    len(candidates),
                    len(promoted),
                ),
            )
            self._refresh_discovery_status_projection(con)
            self._compact_discovery_authority(con)
            con.execute("COMMIT")
            return batch_id, tuple(group_id for _, group_id in promoted)
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    @staticmethod
    def _persist_admission_proof(
        con: sqlite3.Connection,
        *,
        proof: DiscoveryAdmissionProof,
        now_ms: int,
    ) -> None:
        con.execute(
            "INSERT INTO neg_risk_discovery_admission_state("
            "id,effective_capacity,candidate_max_wait_ms,poll_interval_ms,"
            "selection_budget_ms,group_timeout_ms,terminal_write_budget_ms,"
            "attempt_start_write_budget_ms,high_burst_groups,reserved_non_high_slots,"
            "effective_start_bound_ms,updated_at_ms"
            ") VALUES (1,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
            "effective_capacity=excluded.effective_capacity,"
            "candidate_max_wait_ms=excluded.candidate_max_wait_ms,"
            "poll_interval_ms=excluded.poll_interval_ms,"
            "selection_budget_ms=excluded.selection_budget_ms,"
            "group_timeout_ms=excluded.group_timeout_ms,"
            "terminal_write_budget_ms=excluded.terminal_write_budget_ms,"
            "attempt_start_write_budget_ms=excluded.attempt_start_write_budget_ms,"
            "high_burst_groups=excluded.high_burst_groups,"
            "reserved_non_high_slots=excluded.reserved_non_high_slots,"
            "effective_start_bound_ms=excluded.effective_start_bound_ms,"
            "updated_at_ms=excluded.updated_at_ms",
            (
                proof.effective_capacity,
                proof.candidate_max_wait_ms,
                proof.poll_interval_ms,
                proof.selection_budget_ms,
                proof.group_timeout_ms,
                proof.terminal_write_budget_ms,
                proof.attempt_start_write_budget_ms,
                proof.high_burst_groups,
                proof.reserved_non_high_slots,
                proof.effective_start_bound_ms,
                now_ms,
            ),
        )
    @staticmethod
    def _admission_proof_from_row(
        row: sqlite3.Row,
    ) -> DiscoveryAdmissionProof:
        return DiscoveryAdmissionProof(
            effective_capacity=int(row["effective_capacity"]),
            candidate_max_wait_ms=int(row["candidate_max_wait_ms"]),
            selection_budget_ms=int(row["selection_budget_ms"]),
            poll_interval_ms=int(row["poll_interval_ms"]),
            group_timeout_ms=int(row["group_timeout_ms"]),
            terminal_write_budget_ms=int(row["terminal_write_budget_ms"]),
            high_burst_groups=int(row["high_burst_groups"]),
            reserved_non_high_slots=int(row["reserved_non_high_slots"]),
            attempt_start_write_budget_ms=int(row["attempt_start_write_budget_ms"]),
        )

    @staticmethod
    def _proof_timing(
        proof: sqlite3.Row | DiscoveryAdmissionProof,
    ) -> tuple[int, ...]:
        if isinstance(proof, DiscoveryAdmissionProof):
            return (
                proof.candidate_max_wait_ms,
                proof.selection_budget_ms,
                proof.poll_interval_ms,
                proof.group_timeout_ms,
                proof.terminal_write_budget_ms,
                proof.attempt_start_write_budget_ms,
                proof.high_burst_groups,
            )
        return (
            int(proof["candidate_max_wait_ms"]),
            int(proof["selection_budget_ms"]),
            int(proof["poll_interval_ms"]),
            int(proof["group_timeout_ms"]),
            int(proof["terminal_write_budget_ms"]),
            int(proof["attempt_start_write_budget_ms"]),
            int(proof["high_burst_groups"]),
        )

    def _record_candidate_admission(
        self,
        con: sqlite3.Connection,
        *,
        schedule: sqlite3.Row,
        proof_row: sqlite3.Row,
        recorded_at_ms: int,
        writer_token: str | None = None,
    ) -> None:
        promoted_at_ms = int(schedule["promoted_at_ms"])
        deadline = int(schedule["candidate_start_deadline_at_ms"])
        candidate_max_wait_ms = int(proof_row["candidate_max_wait_ms"])
        current = con.execute(
            "SELECT r.* FROM neg_risk_group_revisions r WHERE r.group_id=? "
            "ORDER BY r.revision DESC LIMIT 1",
            (str(schedule["group_id"]),),
        ).fetchone()
        if (
            current is None
            or current["status"] != "certified"
            or current["event_id"] != schedule["event_id"]
            or current["membership_hash"] != schedule["membership_hash"]
            or int(current["observed_at_ms"]) > promoted_at_ms
            or deadline != promoted_at_ms + candidate_max_wait_ms
        ):
            raise ValueError("invalid-candidate-admission-authority")
        exists = con.execute(
            "SELECT 1 FROM neg_risk_candidate_admissions "
            "WHERE group_id=? AND promoted_at_ms=?",
            (str(schedule["group_id"]), promoted_at_ms),
        ).fetchone()
        if exists is not None:
            return
        token = writer_token
        if token is None:
            token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_candidate_admissions",
                operation="INSERT",
                row_key=str(schedule["group_id"]),
            )
        con.execute(
            "INSERT OR IGNORE INTO neg_risk_candidate_admissions("
            "group_id,event_id,membership_hash,promoted_at_ms,"
            "candidate_start_deadline_at_ms,effective_capacity,"
            "candidate_max_wait_ms,selection_budget_ms,poll_interval_ms,"
            "group_timeout_ms,terminal_write_budget_ms,"
            "attempt_start_write_budget_ms,high_burst_groups,"
            "reserved_non_high_slots,effective_start_bound_ms,recorded_at_ms"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                str(schedule["group_id"]),
                str(schedule["event_id"]),
                str(schedule["membership_hash"]),
                promoted_at_ms,
                deadline,
                int(proof_row["effective_capacity"]),
                candidate_max_wait_ms,
                int(proof_row["selection_budget_ms"]),
                int(proof_row["poll_interval_ms"]),
                int(proof_row["group_timeout_ms"]),
                int(proof_row["terminal_write_budget_ms"]),
                int(proof_row["attempt_start_write_budget_ms"]),
                int(proof_row["high_burst_groups"]),
                int(proof_row["reserved_non_high_slots"]),
                int(proof_row["effective_start_bound_ms"]),
                recorded_at_ms,
            ),
        )
        self._consume_expected_owner_mutation(
            con,
            writer_token=token,
            table_name="neg_risk_candidate_admissions",
            operation="INSERT",
            row_key=str(schedule["group_id"]),
            finalize=False,
        )
        self._refresh_discovery_status_projection(
            con,
            writer_token=token,
        )

    def _record_existing_candidate_admissions(
        self,
        con: sqlite3.Connection,
        *,
        proof_row: sqlite3.Row,
        recorded_at_ms: int,
    ) -> None:
        schedules = con.execute(
            "SELECT s.* FROM neg_risk_group_schedule s "
            "WHERE s.promoted_at_ms IS NOT NULL "
            "AND s.candidate_start_deadline_at_ms IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 "
            "FROM neg_risk_candidate_watch_facts f "
            "WHERE f.group_id=s.group_id)"
        ).fetchall()
        for schedule in schedules:
            self._record_candidate_admission(
                con,
                schedule=schedule,
                proof_row=proof_row,
                recorded_at_ms=recorded_at_ms,
            )

    def _admit_waiting_candidates(
        self,
        con: sqlite3.Connection,
        *,
        now_ms: int,
    ) -> None:
        proof_row = con.execute(
            "SELECT * FROM neg_risk_discovery_admission_state WHERE id=1"
        ).fetchone()
        if proof_row is None:
            return
        capacity = int(proof_row["effective_capacity"])
        outstanding = int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_group_schedule s "
                "WHERE s.promoted_at_ms IS NOT NULL AND NOT EXISTS ("
                "SELECT 1 FROM neg_risk_candidate_watch_facts f "
                "WHERE f.group_id=s.group_id)"
            ).fetchone()[0]
        )
        if outstanding > capacity:
            raise ValueError("discovery-admission-capacity-reduced-below-outstanding")
        available = capacity - outstanding
        if available <= 0:
            return
        queued = con.execute(
            "SELECT s.* FROM neg_risk_group_schedule s "
            "JOIN neg_risk_group_revisions r ON r.group_id=s.group_id "
            "AND r.revision=(SELECT MAX(r2.revision) "
            "FROM neg_risk_group_revisions r2 WHERE r2.group_id=s.group_id) "
            "WHERE s.quality='complete-supported' "
            "AND s.promoted_at_ms IS NULL "
            "AND NOT EXISTS (SELECT 1 "
            "FROM neg_risk_candidate_watch_facts f "
            "WHERE f.group_id=s.group_id) "
            "AND s.promotion_queue_deadline_at_ms IS NOT NULL "
            "AND r.status='certified' AND r.event_id=s.event_id "
            "AND r.membership_hash=s.membership_hash "
            "ORDER BY s.promotion_queue_deadline_at_ms,"
            "CAST(s.priority_score AS REAL) DESC,s.group_id LIMIT ?",
            (available,),
        ).fetchall()
        candidate_max_wait_ms = int(proof_row["candidate_max_wait_ms"])
        for row in queued:
            group_id = str(row["group_id"])
            token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_group_schedule",
                operation="UPDATE",
                row_key=group_id,
            )
            con.execute(
                "UPDATE neg_risk_group_schedule SET promoted_at_ms=?,"
                "candidate_start_deadline_at_ms=? WHERE group_id=?",
                (now_ms, now_ms + candidate_max_wait_ms, group_id),
            )
            self._consume_expected_owner_mutation(
                con,
                writer_token=token,
                table_name="neg_risk_group_schedule",
                operation="UPDATE",
                row_key=group_id,
                finalize=False,
            )
            schedule = con.execute(
                "SELECT * FROM neg_risk_group_schedule WHERE group_id=?",
                (group_id,),
            ).fetchone()
            self._record_candidate_admission(
                con,
                schedule=schedule,
                proof_row=proof_row,
                recorded_at_ms=now_ms,
                writer_token=token,
            )

    def _insert_discovery_schedule(
        self,
        con: sqlite3.Connection,
        *,
        candidate: DiscoveryScheduleCandidate,
        source_cursor: str | None,
        started_at_ms: int,
        finished_at_ms: int,
        candidate_max_wait_ms: int,
    ) -> GroupSchedule:
        candidate_checkpoint = self._validated_candidate_checkpoint(con)
        prior = con.execute(
            "SELECT * FROM neg_risk_group_schedule WHERE group_id=?",
            (candidate.group_id,),
        ).fetchone()
        if prior is not None and prior["event_id"] != candidate.event_id:
            raise ValueError("discovery-group-event-identity-conflict")
        current_authority = self._current_group_row(con, candidate.group_id)
        if current_authority is not None and current_authority["event_id"] != candidate.event_id:
            raise ValueError("discovery-group-event-identity-conflict")
        last_fact = con.execute(
            "SELECT observed_at_ms,gross_edge_bps "
            "FROM neg_risk_candidate_watch_facts WHERE group_id=? "
            "ORDER BY id DESC LIMIT 1",
            (candidate.group_id,),
        ).fetchone()
        first_discovered_at_ms = (
            finished_at_ms if prior is None else int(prior["first_discovered_at_ms"])
        )
        last_visited_at_ms = (
            int(last_fact["observed_at_ms"])
            if last_fact is not None
            else (
                None
                if prior is None or prior["last_visited_at_ms"] is None
                else int(prior["last_visited_at_ms"])
            )
        )
        gross_edge_bps = (
            Decimal("0")
            if last_fact is None or last_fact["gross_edge_bps"] is None
            else Decimal(str(last_fact["gross_edge_bps"]))
        )
        changed = prior is None or prior["membership_hash"] != candidate.membership_hash
        components = priority_components(
            GroupScheduleInput(
                group_id=candidate.group_id,
                gross_edge_bps=gross_edge_bps,
                activity_rank=candidate.activity_rank,
                liquidity_rank=candidate.liquidity_rank,
                change_rank=Decimal("100") if changed else Decimal("0"),
                last_visited_at_ms=last_visited_at_ms,
                first_discovered_at_ms=first_discovered_at_ms,
            ),
            now_ms=finished_at_ms,
        )
        if changed:
            priority_class: CandidatePriority = "high"
        elif components.score >= Decimal("25"):
            priority_class = "normal"
        else:
            priority_class = "explore"

        can_promote = candidate.quality == "complete-supported" and candidate.legs is not None
        promoted_at_ms = (
            int(prior["promoted_at_ms"])
            if can_promote and prior is not None and prior["promoted_at_ms"] is not None
            else None
        )
        promotion_eligible_at_ms = (
            (
                int(prior["promotion_eligible_at_ms"])
                if prior is not None and prior["promotion_eligible_at_ms"] is not None
                else finished_at_ms
            )
            if can_promote
            else None
        )
        promotion_queue_deadline_at_ms = (
            promotion_eligible_at_ms + candidate_max_wait_ms
            if promotion_eligible_at_ms is not None
            else None
        )
        candidate_start_deadline_at_ms = (
            int(prior["candidate_start_deadline_at_ms"])
            if promoted_at_ms is not None
            and prior is not None
            and prior["candidate_start_deadline_at_ms"] is not None
            else None
        )
        if can_promote:
            self._certify_discovered_group(
                con,
                candidate=candidate,
                source_cursor=source_cursor,
                started_at_ms=started_at_ms,
                finished_at_ms=finished_at_ms,
            )
        else:
            self._revoke_discovered_group(
                con,
                group_id=candidate.group_id,
                source_cursor=source_cursor,
                started_at_ms=started_at_ms,
                finished_at_ms=finished_at_ms,
            )

        schedule_operation = "INSERT" if prior is None else "UPDATE"
        token = self._begin_expected_owner_mutation(
            con,
            table_name="neg_risk_group_schedule",
            operation=schedule_operation,
            row_key=candidate.group_id,
        )
        con.execute(
            "INSERT INTO neg_risk_group_schedule("
            "group_id,event_id,membership_hash,quality,reason,gross_edge_bps,"
            "activity_rank,liquidity_rank,change_rank,age_rank,priority_score,"
            "priority_reason,priority_class,liquidity_weight,"
            "first_discovered_at_ms,last_discovered_at_ms,last_visited_at_ms,"
            "promoted_at_ms,promotion_eligible_at_ms,"
            "promotion_queue_deadline_at_ms,candidate_start_deadline_at_ms"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(group_id) DO UPDATE SET "
            "event_id=excluded.event_id,membership_hash=excluded.membership_hash,"
            "quality=excluded.quality,reason=excluded.reason,"
            "gross_edge_bps=excluded.gross_edge_bps,"
            "activity_rank=excluded.activity_rank,"
            "liquidity_rank=excluded.liquidity_rank,"
            "change_rank=excluded.change_rank,age_rank=excluded.age_rank,"
            "priority_score=excluded.priority_score,"
            "priority_reason=excluded.priority_reason,"
            "priority_class=excluded.priority_class,"
            "liquidity_weight=excluded.liquidity_weight,"
            "last_discovered_at_ms=excluded.last_discovered_at_ms,"
            "last_visited_at_ms=excluded.last_visited_at_ms,"
            "promoted_at_ms=excluded.promoted_at_ms,"
            "promotion_eligible_at_ms=excluded.promotion_eligible_at_ms,"
            "promotion_queue_deadline_at_ms="
            "excluded.promotion_queue_deadline_at_ms,"
            "candidate_start_deadline_at_ms="
            "excluded.candidate_start_deadline_at_ms",
            (
                candidate.group_id,
                candidate.event_id,
                candidate.membership_hash,
                candidate.quality,
                candidate.reason,
                str(components.gross_edge_bps),
                str(components.activity_rank),
                str(components.liquidity_rank),
                str(components.change_rank),
                str(components.age_rank),
                str(components.score),
                components.reason,
                priority_class,
                str(candidate.liquidity_weight),
                first_discovered_at_ms,
                finished_at_ms,
                last_visited_at_ms,
                promoted_at_ms,
                promotion_eligible_at_ms,
                promotion_queue_deadline_at_ms,
                candidate_start_deadline_at_ms,
            ),
        )
        self._consume_expected_owner_mutation(
            con,
            writer_token=token,
            table_name="neg_risk_group_schedule",
            operation=schedule_operation,
            row_key=candidate.group_id,
            finalize=False,
        )
        self._refresh_discovery_status_projection(
            con,
            writer_token=token,
        )
        if candidate_checkpoint is not None:
            self._refresh_candidate_checkpoint(con, candidate_checkpoint)
        self._compact_candidate_authority(con)
        return self._group_schedule_from_row(
            con.execute(
                "SELECT * FROM neg_risk_group_schedule WHERE group_id=?",
                (candidate.group_id,),
            ).fetchone()
        )

    def _certify_discovered_group(
        self,
        con: sqlite3.Connection,
        *,
        candidate: DiscoveryScheduleCandidate,
        source_cursor: str | None,
        started_at_ms: int,
        finished_at_ms: int,
    ) -> None:
        assert candidate.legs is not None
        current = self._current_group_row(con, candidate.group_id)
        if (
            current is not None
            and current["status"] == "certified"
            and current["membership_hash"] == candidate.membership_hash
        ):
            return
        revision_number = 1 if current is None else int(current["revision"]) + 1
        revision = GroupRevision.certified(
            group_id=candidate.group_id,
            event_id=candidate.event_id,
            revision=revision_number,
            started_at_ms=started_at_ms,
            observed_at_ms=finished_at_ms,
            source_cursor=source_cursor or "<start>",
            legs=candidate.legs,
        )
        if revision.membership_hash != candidate.membership_hash:
            raise ValueError("discovery-membership-hash-mismatch")
        self._insert_group_revision(con, revision, current)

    def _revoke_discovered_group(
        self,
        con: sqlite3.Connection,
        *,
        group_id: str,
        source_cursor: str | None,
        started_at_ms: int,
        finished_at_ms: int,
    ) -> None:
        """Revoke old authority without fabricating newly unknowable identity."""
        current = self._current_group_row(con, group_id)
        if current is None or current["status"] != "certified":
            return
        prior = self._validated_group_from_row(current)
        if prior is None:
            raise ValueError("certified-group-invalid")
        revision = replace(
            prior,
            revision=prior.revision + 1,
            started_at_ms=started_at_ms,
            observed_at_ms=finished_at_ms,
            source_cursor=source_cursor or "<start>",
            status="invalidated",
        )
        self._insert_group_revision(con, revision, current)

    def _sync_reconciliation_schedule(
        self,
        con: sqlite3.Connection,
        *,
        revision: GroupRevision,
        closed: bool,
    ) -> None:
        schedule = con.execute(
            "SELECT 1 FROM neg_risk_group_schedule WHERE group_id=?",
            (revision.group_id,),
        ).fetchone()
        if schedule is None:
            return
        token = self._begin_expected_owner_mutation(
            con,
            table_name="neg_risk_group_schedule",
            operation="UPDATE",
            row_key=revision.group_id,
        )
        con.execute(
            "UPDATE neg_risk_group_schedule SET event_id=?,membership_hash=?,"
            "quality=?,reason=?,"
            "promoted_at_ms=NULL,promotion_eligible_at_ms=?,"
            "promotion_queue_deadline_at_ms=NULL,"
            "candidate_start_deadline_at_ms=NULL WHERE group_id=?",
            (
                revision.event_id,
                revision.membership_hash,
                "incomplete-source" if closed else "complete-supported",
                "reconciliation-closed" if closed else None,
                None if closed else revision.observed_at_ms,
                revision.group_id,
            ),
        )
        self._consume_expected_owner_mutation(
            con,
            writer_token=token,
            table_name="neg_risk_group_schedule",
            operation="UPDATE",
            row_key=revision.group_id,
            finalize=False,
        )
        self._refresh_discovery_status_projection(
            con,
            writer_token=token,
        )

    def group_schedule(self, group_id: str) -> GroupSchedule | None:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT * FROM neg_risk_group_schedule WHERE group_id=?",
                (group_id,),
            ).fetchone()
        finally:
            con.close()
        return None if row is None else self._group_schedule_from_row(row)

    def promoted_group_ids(self) -> tuple[str, ...]:
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT group_id FROM neg_risk_group_schedule "
                "WHERE promoted_at_ms IS NOT NULL "
                "ORDER BY CAST(priority_score AS REAL) DESC,group_id"
            ).fetchall()
        finally:
            con.close()
        return tuple(str(row["group_id"]) for row in rows)

    def actual_candidate_group_ids(self) -> tuple[str, ...]:
        """Current certified groups already watched or capacity-admitted."""
        con = self._connect()
        try:
            rows = con.execute(
                "WITH current AS ("
                "SELECT r.* FROM neg_risk_group_revisions r JOIN ("
                "SELECT group_id,MAX(revision) AS revision "
                "FROM neg_risk_group_revisions GROUP BY group_id"
                ") c ON c.group_id=r.group_id AND c.revision=r.revision"
                ") SELECT c.group_id FROM current c "
                "LEFT JOIN neg_risk_group_schedule s ON s.group_id=c.group_id "
                "WHERE c.status='certified' AND "
                f"{_ACTUAL_CANDIDATE_AUTHORITY_SQL} "
                "ORDER BY (s.promoted_at_ms IS NOT NULL) DESC,"
                "CAST(COALESCE(s.priority_score,'0') AS REAL) DESC,c.group_id"
            ).fetchall()
        finally:
            con.close()
        return tuple(str(row["group_id"]) for row in rows)

    def _validated_discovery_checkpoint(
        self,
        con: sqlite3.Connection,
    ) -> tuple[sqlite3.Row, dict[str, object]] | None:
        row = con.execute(
            "SELECT * FROM neg_risk_discovery_authority_checkpoints WHERE id=1"
        ).fetchone()
        if row is None:
            return None
        self._check_deadline("discovery-authority-deadline")
        try:
            anchor = json.loads(str(row["anchor_json"]))
            canonical = json.dumps(
                anchor,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            anchor_digest = f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}"
            expected_hash = discovery_authority_checkpoint_hash(
                domain=str(row["domain"]),
                version=int(row["version"]),
                generation=int(row["generation"]),
                through_batch_id=int(row["through_batch_id"]),
                through_sample_id=int(row["through_sample_id"]),
                through_evidence_id=int(row["through_evidence_id"]),
                compacted_batch_rows=int(row["compacted_batch_rows"]),
                compacted_sample_rows=int(row["compacted_sample_rows"]),
                compacted_evidence_rows=int(row["compacted_evidence_rows"]),
                prefix_digest=str(row["prefix_digest"]),
                anchor_digest=str(row["anchor_digest"]),
            )
            batch = anchor["batch"]
            samples = anchor["samples"]
            evidence = anchor["evidence"]
            visits = anchor["coverage_visits"]
            if (
                row["domain"] != _DISCOVERY_AUTHORITY_DOMAIN
                or int(row["version"]) != _DISCOVERY_AUTHORITY_VERSION
                or int(row["generation"]) <= 0
                or any(
                    int(row[name]) < 0
                    for name in (
                        "through_batch_id",
                        "through_sample_id",
                        "through_evidence_id",
                        "compacted_batch_rows",
                        "compacted_sample_rows",
                        "compacted_evidence_rows",
                    )
                )
                or not isinstance(anchor, dict)
                or not isinstance(batch, dict)
                or int(batch["id"]) != int(row["through_batch_id"])
                or not isinstance(samples, list)
                or not isinstance(evidence, list)
                or not isinstance(visits, list)
                or canonical != str(row["anchor_json"])
                or not hmac.compare_digest(str(row["anchor_digest"]), anchor_digest)
                or not hmac.compare_digest(str(row["checkpoint_hash"]), expected_hash)
            ):
                raise ValueError("invalid-discovery-authority-checkpoint")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("invalid-discovery-authority-checkpoint") from error
        retained_prefix = con.execute(
            "SELECT "
            "(SELECT COUNT(*) FROM neg_risk_discovery_batches WHERE id<=?),"
            "(SELECT COUNT(*) FROM neg_risk_discovery_batch_samples WHERE batch_id<=?),"
            "(SELECT COUNT(*) FROM neg_risk_discovery_schedule_evidence WHERE batch_id<=?)",
            (
                int(row["through_batch_id"]),
                int(row["through_batch_id"]),
                int(row["through_batch_id"]),
            ),
        ).fetchone()
        if any(int(value) for value in retained_prefix):
            raise ValueError("invalid-discovery-authority-checkpoint")
        return row, anchor

    def _compact_discovery_authority(self, con: sqlite3.Connection) -> None:
        count = int(
            con.execute("SELECT COUNT(*) FROM neg_risk_discovery_batches").fetchone()[0]
        )
        if count <= _DISCOVERY_AUTHORITY_COMPACT_HIGH_ROWS:
            return
        self.discovery_status(
            now_ms=int(
                con.execute(
                    "SELECT COALESCE(MAX(finished_at_ms),0) "
                    "FROM neg_risk_discovery_batches"
                ).fetchone()[0]
            ),
            _connection=con,
        )
        previous = self._validated_discovery_checkpoint(con)
        compact_target = count - _DISCOVERY_AUTHORITY_COMPACT_LOW_ROWS
        through_row = con.execute(
            "SELECT * FROM neg_risk_discovery_batches "
            "ORDER BY id LIMIT 1 OFFSET ?",
            (compact_target - 1,),
        ).fetchone()
        if through_row is None:
            return
        through_batch = int(through_row["id"])
        batch_rows = con.execute(
            "SELECT * FROM neg_risk_discovery_batches WHERE id<=? ORDER BY id",
            (through_batch,),
        ).fetchall()
        sample_rows = con.execute(
            "SELECT rowid,* FROM neg_risk_discovery_batch_samples "
            "WHERE batch_id<=? ORDER BY rowid",
            (through_batch,),
        ).fetchall()
        evidence_rows = con.execute(
            "SELECT rowid,* FROM neg_risk_discovery_schedule_evidence "
            "WHERE batch_id<=? ORDER BY rowid",
            (through_batch,),
        ).fetchall()
        latest_samples = [
            dict(row) for row in sample_rows if int(row["batch_id"]) == through_batch
        ]
        latest_evidence = [
            dict(row) for row in evidence_rows if int(row["batch_id"]) == through_batch
        ]
        cutoff = int(through_row["finished_at_ms"]) - 60 * 60_000
        prior_visits = (
            []
            if previous is None
            else list(previous[1].get("coverage_visits", []))
        )
        coverage_visits = {
            (str(item[1]), int(item[0]))
            for item in prior_visits
            if isinstance(item, list) and len(item) == 2 and int(item[0]) >= cutoff
        }
        finished_by_batch = {
            int(row["id"]): int(row["finished_at_ms"]) for row in batch_rows
        }
        coverage_visits.update(
            (str(row["group_id"]), finished_by_batch[int(row["batch_id"])])
            for row in sample_rows
            if finished_by_batch[int(row["batch_id"])] >= cutoff
        )
        anchor = {
            "batch": dict(through_row),
            "coverage_visits": [
                [finished_at_ms, group_id]
                for group_id, finished_at_ms in sorted(coverage_visits)
            ],
            "evidence": latest_evidence,
            "samples": latest_samples,
        }
        anchor_json = json.dumps(
            anchor,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        anchor_digest = f"sha256:{hashlib.sha256(anchor_json.encode()).hexdigest()}"
        prefix_payload = {
            "previous_prefix_digest": (
                None if previous is None else str(previous[0]["prefix_digest"])
            ),
            "batches": [dict(row) for row in batch_rows],
            "samples": [dict(row) for row in sample_rows],
            "evidence": [dict(row) for row in evidence_rows],
        }
        prefix_json = json.dumps(
            prefix_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        prefix_digest = f"sha256:{hashlib.sha256(prefix_json.encode()).hexdigest()}"
        prior_counts = (
            (0, 0, 0)
            if previous is None
            else tuple(
                int(previous[0][name])
                for name in (
                    "compacted_batch_rows",
                    "compacted_sample_rows",
                    "compacted_evidence_rows",
                )
            )
        )
        compacted = (
            prior_counts[0] + len(batch_rows),
            prior_counts[1] + len(sample_rows),
            prior_counts[2] + len(evidence_rows),
        )
        through_sample = max((int(row["rowid"]) for row in sample_rows), default=0)
        through_evidence = max(
            (int(row["rowid"]) for row in evidence_rows),
            default=0,
        )
        generation = 1 if previous is None else int(previous[0]["generation"]) + 1
        checkpoint_hash = discovery_authority_checkpoint_hash(
            domain=_DISCOVERY_AUTHORITY_DOMAIN,
            version=_DISCOVERY_AUTHORITY_VERSION,
            generation=generation,
            through_batch_id=through_batch,
            through_sample_id=through_sample,
            through_evidence_id=through_evidence,
            compacted_batch_rows=compacted[0],
            compacted_sample_rows=compacted[1],
            compacted_evidence_rows=compacted[2],
            prefix_digest=prefix_digest,
            anchor_digest=anchor_digest,
        )
        con.execute(
            "INSERT INTO neg_risk_discovery_authority_checkpoints("
            "id,domain,version,generation,through_batch_id,through_sample_id,"
            "through_evidence_id,compacted_batch_rows,compacted_sample_rows,"
            "compacted_evidence_rows,prefix_digest,anchor_json,anchor_digest,"
            "checkpoint_hash) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET domain=excluded.domain,"
            "version=excluded.version,generation=excluded.generation,"
            "through_batch_id=excluded.through_batch_id,"
            "through_sample_id=excluded.through_sample_id,"
            "through_evidence_id=excluded.through_evidence_id,"
            "compacted_batch_rows=excluded.compacted_batch_rows,"
            "compacted_sample_rows=excluded.compacted_sample_rows,"
            "compacted_evidence_rows=excluded.compacted_evidence_rows,"
            "prefix_digest=excluded.prefix_digest,anchor_json=excluded.anchor_json,"
            "anchor_digest=excluded.anchor_digest,"
            "checkpoint_hash=excluded.checkpoint_hash",
            (
                _DISCOVERY_AUTHORITY_DOMAIN,
                _DISCOVERY_AUTHORITY_VERSION,
                generation,
                through_batch,
                through_sample,
                through_evidence,
                *compacted,
                prefix_digest,
                anchor_json,
                anchor_digest,
                checkpoint_hash,
            ),
        )
        con.execute(
            "DELETE FROM neg_risk_discovery_batch_samples WHERE batch_id<=?",
            (through_batch,),
        )
        con.execute(
            "DELETE FROM neg_risk_discovery_schedule_evidence WHERE batch_id<=?",
            (through_batch,),
        )
        con.execute(
            "DELETE FROM neg_risk_discovery_batches WHERE id<=?",
            (through_batch,),
        )
        self.discovery_status(
            now_ms=int(through_row["finished_at_ms"]),
            _connection=con,
        )

    def coverage_windows(self, now_ms: int) -> CoverageWindows:
        con = self._connect()
        try:
            con.execute("BEGIN")
            checkpoint = self._validated_discovery_checkpoint(con)
            result = self._coverage_windows_in_snapshot(
                con,
                now_ms,
                checkpoint=(None if checkpoint is None else checkpoint[1]),
                through_batch_id=(
                    0 if checkpoint is None else int(checkpoint[0]["through_batch_id"])
                ),
            )
            con.execute("COMMIT")
            return result
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _refresh_discovery_status_projection(
        self,
        con: sqlite3.Connection,
        *,
        writer_token: str | None = None,
        finalize: bool = True,
    ) -> None:
        """Atomically materialize current Discovery identity and audit counters."""
        previous = con.execute(
            "SELECT * FROM neg_risk_discovery_status_projection WHERE id=1"
        ).fetchone()
        if writer_token is None:
            writer_token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_discovery_status_projection",
                operation="INSERT" if previous is None else "UPDATE",
                row_key="1",
            )
        expected_events: list[tuple[str, str, str]] = []
        owner_guard = con.execute(
            "SELECT consumed_journal_id FROM neg_risk_owner_mutation_guard WHERE id=1"
        ).fetchone()
        owner_journal_id = int(owner_guard["consumed_journal_id"])
        if previous is None:
            changed_group_ids = {
                str(row["group_id"])
                for row in con.execute(
                    "SELECT group_id FROM neg_risk_group_schedule"
                )
            }
            digest_value = 0
            aggregates = {
                "group_count": 0,
                "queue_high": 0,
                "queue_normal": 0,
                "queue_explore": 0,
                "promotion_queue_depth": 0,
                "outstanding_admitted_count": 0,
                "total_liquidity_weight": 0.0,
            }
        else:
            changed_group_ids = {
                str(row["row_key"])
                for row in con.execute(
                    "SELECT row_key FROM neg_risk_owner_mutation_journal "
                    "WHERE id>? AND id<=? AND table_name IN ("
                    "'neg_risk_group_revisions','neg_risk_group_schedule',"
                    "'neg_risk_candidate_watch_facts',"
                    "'neg_risk_candidate_admissions',"
                    "'neg_risk_candidate_attempt_starts')",
                    (int(previous["owner_journal_id"]), owner_journal_id),
                )
            }
            digest_value = int(
                str(previous["projection_digest"]).removeprefix("sha256:"), 16
            )
            aggregates = {name: int(previous[name]) for name in (
                "group_count", "queue_high", "queue_normal", "queue_explore",
                "promotion_queue_depth", "outstanding_admitted_count",
            )}
            aggregates["total_liquidity_weight"] = float(
                previous["total_liquidity_weight"]
            )

        def apply_contribution(payload: dict[str, object], sign: int) -> None:
            schedule_payload = payload["schedule"]
            assert isinstance(schedule_payload, dict)
            aggregates["group_count"] += sign
            aggregates["total_liquidity_weight"] += sign * float(
                schedule_payload["liquidity_weight"]
            )
            promoted = schedule_payload["promoted_at_ms"] is not None
            if promoted:
                aggregates[f"queue_{schedule_payload['priority_class']}"] += sign
            if (
                schedule_payload["quality"] == "complete-supported"
                and not promoted
            ):
                aggregates["promotion_queue_depth"] += sign
            if promoted and not bool(payload["has_candidate_fact"]):
                aggregates["outstanding_admitted_count"] += sign

        for group_id in sorted(changed_group_ids):
            prior_projection = con.execute(
                "SELECT payload_json,row_hash FROM neg_risk_discovery_group_projection "
                "WHERE group_id=?",
                (group_id,),
            ).fetchone()
            if prior_projection is not None:
                apply_contribution(
                    json.loads(str(prior_projection["payload_json"])),
                    -1,
                )
                digest_value ^= int(
                    str(prior_projection["row_hash"]).removeprefix("sha256:"), 16
                )
            schedule = con.execute(
                "SELECT * FROM neg_risk_group_schedule WHERE group_id=?",
                (group_id,),
            ).fetchone()
            if schedule is None:
                con.execute(
                    "DELETE FROM neg_risk_discovery_group_projection WHERE group_id=?",
                    (group_id,),
                )
                if prior_projection is not None:
                    expected_events.append(
                        (
                            "neg_risk_discovery_group_projection",
                            "DELETE",
                            group_id,
                        )
                    )
                continue
            current = con.execute(
                "SELECT * FROM neg_risk_group_revisions WHERE group_id=? "
                "ORDER BY revision DESC LIMIT 1",
                (group_id,),
            ).fetchone()
            first_observed = (
                None
                if current is None
                else con.execute(
                    "SELECT MIN(observed_at_ms) FROM neg_risk_group_revisions "
                    "WHERE group_id=? AND event_id=? AND membership_hash=? "
                    "AND status='certified'",
                    (
                        group_id,
                        str(current["event_id"]),
                        str(current["membership_hash"]),
                    ),
                ).fetchone()[0]
            )
            admission = None
            if schedule["promoted_at_ms"] is not None:
                admission_row = con.execute(
                    "SELECT * FROM neg_risk_candidate_admissions "
                    "WHERE group_id=? AND event_id=? AND membership_hash=? "
                    "AND promoted_at_ms=? AND candidate_start_deadline_at_ms=? "
                    "ORDER BY id DESC LIMIT 1",
                    (
                        group_id,
                        str(schedule["event_id"]),
                        str(schedule["membership_hash"]),
                        int(schedule["promoted_at_ms"]),
                        int(schedule["candidate_start_deadline_at_ms"]),
                    ),
                ).fetchone()
                admission = None if admission_row is None else dict(admission_row)
            payload_json = json.dumps(
                {
                    "admission": admission,
                    "current": None if current is None else dict(current),
                    "first_certified_observed_at_ms": first_observed,
                    "group_id": group_id,
                    "has_candidate_fact": con.execute(
                        "SELECT 1 FROM neg_risk_candidate_current_authority "
                        "WHERE group_id=? LIMIT 1",
                        (group_id,),
                    ).fetchone()
                    is not None,
                    "schedule": dict(schedule),
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            row_hash = f"sha256:{hashlib.sha256(payload_json.encode()).hexdigest()}"
            apply_contribution(json.loads(payload_json), 1)
            digest_value ^= int(row_hash.removeprefix("sha256:"), 16)
            con.execute(
                "INSERT INTO neg_risk_discovery_group_projection("
                "group_id,visit_anchor_ms,payload_json,row_hash) VALUES(?,?,?,?) "
                "ON CONFLICT(group_id) DO UPDATE SET "
                "visit_anchor_ms=excluded.visit_anchor_ms,"
                "payload_json=excluded.payload_json,row_hash=excluded.row_hash",
                (
                    group_id,
                    schedule["last_visited_at_ms"]
                    if schedule["last_visited_at_ms"] is not None
                    else schedule["first_discovered_at_ms"],
                    payload_json,
                    row_hash,
                ),
            )
            expected_events.append(
                (
                    "neg_risk_discovery_group_projection",
                    "INSERT" if prior_projection is None else "UPDATE",
                    group_id,
                )
            )
        groups_json = "[]"
        projection_digest = f"sha256:{digest_value:064x}"
        guard = con.execute(
            "SELECT * FROM neg_risk_discovery_status_raw_guard WHERE id=1"
        ).fetchone()
        if guard is None:
            counters = con.execute(
                "SELECT COUNT(*),COALESCE(SUM(deadline_breached),0) "
                "FROM neg_risk_candidate_attempt_starts"
            ).fetchone()
            admission_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM neg_risk_candidate_admissions"
                ).fetchone()[0]
            )
            con.execute(
                "INSERT INTO neg_risk_discovery_status_raw_guard("
                "id,authority_seq,candidate_attempt_start_count,"
                "candidate_start_deadline_breach_count) VALUES(1,?,?,?)",
                (
                    admission_count + int(counters[0]),
                    int(counters[0]),
                    int(counters[1]),
                ),
            )
            guard = con.execute(
                "SELECT * FROM neg_risk_discovery_status_raw_guard WHERE id=1"
            ).fetchone()
        raw_authority_seq = int(guard["authority_seq"])
        attempt_count = int(guard["candidate_attempt_start_count"])
        breach_count = int(guard["candidate_start_deadline_breach_count"])
        generation = 1 if previous is None else int(previous["generation"]) + 1
        checkpoint_hash = discovery_status_projection_hash(
            domain=_DISCOVERY_STATUS_PROJECTION_DOMAIN,
            version=_DISCOVERY_STATUS_PROJECTION_VERSION,
            generation=generation,
            raw_authority_seq=raw_authority_seq,
            candidate_attempt_start_count=attempt_count,
            candidate_start_deadline_breach_count=breach_count,
            projection_digest=projection_digest,
        )
        con.execute(
            "INSERT INTO neg_risk_discovery_status_projection("
            "id,domain,version,generation,raw_authority_seq,owner_journal_id,groups_json,"
            "candidate_attempt_start_count,"
            "candidate_start_deadline_breach_count,group_count,queue_high,"
            "queue_normal,queue_explore,promotion_queue_depth,"
            "outstanding_admitted_count,total_liquidity_weight,projection_digest,"
            "checkpoint_hash) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET domain=excluded.domain,"
            "version=excluded.version,generation=excluded.generation,"
            "raw_authority_seq=excluded.raw_authority_seq,"
            "owner_journal_id=excluded.owner_journal_id,"
            "groups_json=excluded.groups_json,"
            "candidate_attempt_start_count="
            "excluded.candidate_attempt_start_count,"
            "candidate_start_deadline_breach_count="
            "excluded.candidate_start_deadline_breach_count,"
            "group_count=excluded.group_count,queue_high=excluded.queue_high,"
            "queue_normal=excluded.queue_normal,queue_explore=excluded.queue_explore,"
            "promotion_queue_depth=excluded.promotion_queue_depth,"
            "outstanding_admitted_count=excluded.outstanding_admitted_count,"
            "total_liquidity_weight=excluded.total_liquidity_weight,"
            "projection_digest=excluded.projection_digest,"
            "checkpoint_hash=excluded.checkpoint_hash",
            (
                _DISCOVERY_STATUS_PROJECTION_DOMAIN,
                _DISCOVERY_STATUS_PROJECTION_VERSION,
                generation,
                raw_authority_seq,
                owner_journal_id,
                groups_json,
                attempt_count,
                breach_count,
                aggregates["group_count"],
                aggregates["queue_high"],
                aggregates["queue_normal"],
                aggregates["queue_explore"],
                aggregates["promotion_queue_depth"],
                aggregates["outstanding_admitted_count"],
                aggregates["total_liquidity_weight"],
                projection_digest,
                checkpoint_hash,
            ),
        )
        expected_events.append(
            (
                "neg_risk_discovery_status_projection",
                "INSERT" if previous is None else "UPDATE",
                "1",
            )
        )
        self._consume_expected_owner_events(
            con,
            writer_token=writer_token,
            expected_events=expected_events,
            finalize=finalize,
        )

    @staticmethod
    def _validated_discovery_status_projection(
        con: sqlite3.Connection,
    ) -> tuple[
        dict[str, sqlite3.Row | dict[str, object]],
        dict[tuple[str, str, str], int],
        list[dict[str, object]],
        set[str],
        dict[str, int],
    ]:
        row = con.execute(
            "SELECT * FROM neg_risk_discovery_status_projection WHERE id=1"
        ).fetchone()
        if row is None:
            raise ValueError("missing-discovery-status-projection")
        try:
            projection_rows = con.execute(
                "SELECT payload_json,row_hash "
                "FROM neg_risk_discovery_group_projection"
            ).fetchall()
            groups = [json.loads(str(item["payload_json"])) for item in projection_rows]
            digest_value = 0
            for item in projection_rows:
                expected_row_hash = (
                    f"sha256:{hashlib.sha256(str(item['payload_json']).encode()).hexdigest()}"
                )
                if not hmac.compare_digest(str(item["row_hash"]), expected_row_hash):
                    raise ValueError
                digest_value ^= int(expected_row_hash.removeprefix("sha256:"), 16)
            projection_digest = f"sha256:{digest_value:064x}"
            expected_hash = discovery_status_projection_hash(
                domain=str(row["domain"]),
                version=int(row["version"]),
                generation=int(row["generation"]),
                raw_authority_seq=int(row["raw_authority_seq"]),
                candidate_attempt_start_count=int(
                    row["candidate_attempt_start_count"]
                ),
                candidate_start_deadline_breach_count=int(
                    row["candidate_start_deadline_breach_count"]
                ),
                projection_digest=str(row["projection_digest"]),
            )
            if (
                row["domain"] != _DISCOVERY_STATUS_PROJECTION_DOMAIN
                or int(row["version"]) != _DISCOVERY_STATUS_PROJECTION_VERSION
                or int(row["generation"]) <= 0
                or int(row["raw_authority_seq"]) < 0
                or not isinstance(groups, list)
                or str(row["groups_json"]) != "[]"
                or not hmac.compare_digest(
                    str(row["projection_digest"]),
                    projection_digest,
                )
                or not hmac.compare_digest(
                    str(row["checkpoint_hash"]),
                    expected_hash,
                )
            ):
                raise ValueError
            current_revisions: dict[
                str, sqlite3.Row | dict[str, object]
            ] = {}
            revision_identities: dict[tuple[str, str, str], int] = {}
            admissions: list[dict[str, object]] = []
            fact_group_ids: set[str] = set()
            for item in groups:
                if not isinstance(item, dict):
                    raise ValueError
                current = item["current"]
                group_id = str(item["group_id"])
                if current is not None and not isinstance(current, dict):
                    raise ValueError
                if current is not None:
                    if (
                        str(current["group_id"]) != group_id
                        or group_id in current_revisions
                    ):
                        raise ValueError
                    current_revisions[group_id] = current
                first_observed = item["first_certified_observed_at_ms"]
                if current is not None and first_observed is not None:
                    revision_identities[
                        (
                            group_id,
                            str(current["event_id"]),
                            str(current["membership_hash"]),
                        )
                    ] = int(first_observed)
                if bool(item["has_candidate_fact"]):
                    fact_group_ids.add(group_id)
                admission = item.get("admission")
                if admission is not None:
                    if not isinstance(admission, dict):
                        raise ValueError
                    admissions.append(admission)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("invalid-discovery-status-projection") from error
        counters = {
            "attempts": int(row["candidate_attempt_start_count"]),
            "breaches": int(row["candidate_start_deadline_breach_count"]),
        }
        guard = con.execute(
            "SELECT authority_seq,candidate_attempt_start_count,"
            "candidate_start_deadline_breach_count "
            "FROM neg_risk_discovery_status_raw_guard WHERE id=1"
        ).fetchone()
        if (
            guard is None
            or int(guard["authority_seq"]) != int(row["raw_authority_seq"])
            or int(guard["candidate_attempt_start_count"]) != counters["attempts"]
            or int(guard["candidate_start_deadline_breach_count"])
            != counters["breaches"]
            or counters["attempts"] < 0
            or counters["breaches"] < 0
            or counters["breaches"] > counters["attempts"]
        ):
            raise ValueError("invalid-discovery-status-projection")
        return (
            current_revisions,
            revision_identities,
            admissions,
            fact_group_ids,
            counters,
        )

    def discovery_status(
        self,
        now_ms: int,
        *,
        _connection: sqlite3.Connection | None = None,
    ) -> DiscoveryStatus:
        owns_connection = _connection is None
        con = self._connect() if _connection is None else _connection
        self._install_owner_write_authorizer(con)
        try:
            if owns_connection:
                con.execute("BEGIN")
            self._check_deadline("discovery-status-deadline")
            self._assert_owner_journal_clean(con)
            checkpoint = self._validated_discovery_checkpoint(con)
            through_batch_id = 0 if checkpoint is None else int(
                checkpoint[0]["through_batch_id"]
            )
            suffix_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM neg_risk_discovery_batches WHERE id>?",
                    (through_batch_id,),
                ).fetchone()[0]
            )
            if suffix_count > _DISCOVERY_AUTHORITY_UNCOMPACTED_MAX_ROWS:
                raise ValueError("discovery-authority-bound-exceeded")
            state = con.execute("SELECT * FROM neg_risk_discovery_state WHERE id=1").fetchone()
            projection = con.execute(
                "SELECT * FROM neg_risk_discovery_status_projection WHERE id=1"
            ).fetchone()
            raw_guard = con.execute(
                "SELECT * FROM neg_risk_discovery_status_raw_guard WHERE id=1"
            ).fetchone()
            if projection is None or raw_guard is None:
                raise ValueError("missing-discovery-status-projection")
            expected_projection_hash = discovery_status_projection_hash(
                domain=str(projection["domain"]),
                version=int(projection["version"]),
                generation=int(projection["generation"]),
                raw_authority_seq=int(projection["raw_authority_seq"]),
                candidate_attempt_start_count=int(
                    projection["candidate_attempt_start_count"]
                ),
                candidate_start_deadline_breach_count=int(
                    projection["candidate_start_deadline_breach_count"]
                ),
                projection_digest=str(projection["projection_digest"]),
            )
            if (
                projection["domain"] != _DISCOVERY_STATUS_PROJECTION_DOMAIN
                or int(projection["version"]) != _DISCOVERY_STATUS_PROJECTION_VERSION
                or not hmac.compare_digest(
                    str(projection["checkpoint_hash"]),
                    expected_projection_hash,
                )
                or int(projection["raw_authority_seq"])
                != int(raw_guard["authority_seq"])
            ):
                raise ValueError("invalid-discovery-status-projection")
            candidate_counters = {
                "attempts": int(projection["candidate_attempt_start_count"]),
                "breaches": int(
                    projection["candidate_start_deadline_breach_count"]
                ),
            }
            queue = {
                priority: int(projection[f"queue_{priority}"])
                for priority in ("high", "normal", "explore")
            }
            oldest = con.execute(
                "SELECT visit_anchor_ms AS oldest "
                "FROM neg_risk_discovery_group_projection "
                "WHERE visit_anchor_ms IS NOT NULL "
                "ORDER BY visit_anchor_ms,group_id LIMIT 1"
            ).fetchone()
            revision_identities: dict[tuple[str, str, str], int] = {}
            suffix_batches = con.execute(
                "SELECT * FROM neg_risk_discovery_batches WHERE id>? ORDER BY id",
                (through_batch_id,),
            ).fetchall()
            batches = (
                suffix_batches
                if checkpoint is None
                else [checkpoint[1]["batch"], *suffix_batches]
            )
            latest_batch = batches[-1] if batches else None
            suffix_samples = con.execute(
                "SELECT * FROM neg_risk_discovery_batch_samples "
                "WHERE batch_id>? ORDER BY batch_id,group_id",
                (through_batch_id,),
            ).fetchall()
            suffix_evidence = con.execute(
                "SELECT * FROM neg_risk_discovery_schedule_evidence "
                "WHERE batch_id>? ORDER BY batch_id,group_id",
                (through_batch_id,),
            ).fetchall()
            batch_samples = (
                suffix_samples
                if checkpoint is None
                else [*checkpoint[1]["samples"], *suffix_samples]
            )
            schedule_evidence = (
                suffix_evidence
                if checkpoint is None
                else [*checkpoint[1]["evidence"], *suffix_evidence]
            )
            for sample in batch_samples:
                if sample["quality"] != "complete-supported":
                    continue
                identity = (
                    str(sample["group_id"]),
                    str(sample["event_id"]),
                    str(sample["membership_hash"]),
                )
                if identity in revision_identities:
                    continue
                first_observed = con.execute(
                    "SELECT MIN(observed_at_ms) "
                    "FROM neg_risk_group_revisions "
                    "WHERE group_id=? AND event_id=? AND membership_hash=? "
                    "AND status='certified'",
                    identity,
                ).fetchone()[0]
                if first_observed is not None:
                    revision_identities[identity] = int(first_observed)
            latest_samples = [
                row
                for row in batch_samples
                if latest_batch is not None and int(row["batch_id"]) == int(latest_batch["id"])
            ]
            load_row = con.execute(
                "SELECT degraded_streak,last_reason,last_decision,"
                "probe_every_cycles,updated_at_ms "
                "FROM neg_risk_discovery_load_state WHERE id=1"
            ).fetchone()
            admission_row = con.execute(
                "SELECT * FROM neg_risk_discovery_admission_state WHERE id=1"
            ).fetchone()
            coverage = self._coverage_windows_in_snapshot(
                con,
                now_ms,
                checkpoint=(None if checkpoint is None else checkpoint[1]),
                through_batch_id=through_batch_id,
                known_groups_override=int(projection["group_count"]),
                total_weight_override=Decimal(
                    str(projection["total_liquidity_weight"])
                ),
            )
            self._validate_discovery_snapshot(
                state=state,
                schedules=[],
                current_revisions={},
                revision_identities=revision_identities,
                batches=batches,
                batch_samples=batch_samples,
                schedule_evidence=schedule_evidence,
                latest_batch=latest_batch,
                latest_samples=latest_samples,
                load_row=load_row,
                admission_row=admission_row,
                fact_group_ids=set(),
                breach_fact_evidence=set(),
                attempt_starts=[],
                admissions=[],
                coverage=coverage,
                checkpointed_prefix=checkpoint is not None,
                deadline_check=lambda: self._check_deadline(
                    "discovery-status-deadline"
                ),
            )
            if owns_connection:
                con.execute("COMMIT")
        except BaseException:
            if owns_connection and con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            if owns_connection:
                con.close()
        oldest_age = (
            None
            if oldest is None or oldest["oldest"] is None
            else max(0, now_ms - int(oldest["oldest"]))
        )
        return DiscoveryStatus(
            next_cursor=(
                None if state is None or state["next_cursor"] is None else str(state["next_cursor"])
            ),
            completed=False if state is None else bool(state["completed"]),
            last_started_at_ms=(None if state is None else int(state["last_started_at_ms"])),
            last_finished_at_ms=(None if state is None else int(state["last_finished_at_ms"])),
            page_event_count=0 if state is None else int(state["page_event_count"]),
            groups_seen=0 if state is None else int(state["groups_seen"]),
            promoted_count=0 if state is None else int(state["promoted_count"]),
            queue_depth_by_class=queue,
            oldest_visit_age_ms=oldest_age,
            coverage=coverage,
            load_state=(
                DiscoveryLoadState(0, None, "fresh", 10, 0)
                if load_row is None
                else DiscoveryLoadState(
                    int(load_row["degraded_streak"]),
                    (None if load_row["last_reason"] is None else str(load_row["last_reason"])),
                    load_row["last_decision"],
                    int(load_row["probe_every_cycles"]),
                    int(load_row["updated_at_ms"]),
                )
            ),
            admission_proof=(
                None if admission_row is None else self._admission_proof_from_row(admission_row)
            ),
            promotion_queue_depth=int(projection["promotion_queue_depth"]),
            outstanding_admitted_count=int(
                projection["outstanding_admitted_count"]
            ),
            candidate_attempt_start_count=candidate_counters["attempts"],
            candidate_start_deadline_breach_count=candidate_counters["breaches"],
            candidate_start_ready=candidate_counters["breaches"] == 0,
        )

    @staticmethod
    def _coverage_windows_in_snapshot(
        con: sqlite3.Connection,
        now_ms: int,
        *,
        checkpoint: dict[str, object] | None = None,
        through_batch_id: int = 0,
        known_groups_override: int | None = None,
        total_weight_override: Decimal | None = None,
    ) -> CoverageWindows:
        if known_groups_override is None or total_weight_override is None:
            totals = con.execute(
                "SELECT COUNT(*) AS groups_count,"
                "COALESCE(SUM(CAST(liquidity_weight AS REAL)),0) AS total_weight "
                "FROM neg_risk_group_schedule"
            ).fetchone()
            known_groups = int(totals["groups_count"])
            total_weight = Decimal(str(totals["total_weight"]))
        else:
            known_groups = known_groups_override
            total_weight = total_weight_override
        windows: dict[int, CoverageWindow] = {}
        for minutes in (15, 30, 60):
            lower = now_ms - minutes * 60_000
            visited_ids = {
                str(row["group_id"])
                for row in con.execute(
                    "SELECT DISTINCT bs.group_id "
                    "FROM neg_risk_discovery_batch_samples bs "
                    "JOIN neg_risk_discovery_batches b ON b.id=bs.batch_id "
                    "WHERE b.id>? AND b.finished_at_ms>=? AND b.finished_at_ms<=?",
                    (through_batch_id, lower, now_ms),
                ).fetchall()
            }
            if checkpoint is not None:
                visited_ids.update(
                    str(item[1])
                    for item in checkpoint["coverage_visits"]
                    if lower <= int(item[0]) <= now_ms
                )
            if visited_ids:
                placeholders = ",".join("?" for _ in visited_ids)
                row = con.execute(
                    "SELECT COUNT(*) AS visited_groups,"
                    "COALESCE(SUM(CAST(liquidity_weight AS REAL)),0) "
                    "AS visited_weight FROM neg_risk_group_schedule "
                    f"WHERE group_id IN ({placeholders})",
                    tuple(sorted(visited_ids)),
                ).fetchone()
                visited_groups = int(row["visited_groups"])
                visited_weight = Decimal(str(row["visited_weight"]))
            else:
                visited_groups = 0
                visited_weight = Decimal("0")
            windows[minutes] = CoverageWindow(
                minutes=minutes,
                visited_groups=visited_groups,
                raw_fraction=(
                    Decimal(visited_groups) / Decimal(known_groups)
                    if known_groups
                    else Decimal("0")
                ),
                liquidity_weighted_fraction=(
                    visited_weight / total_weight if total_weight > 0 else Decimal("0")
                ),
            )
        return CoverageWindows(
            known_groups=known_groups,
            total_liquidity_weight=total_weight,
            by_minutes=windows,
        )

    @staticmethod
    def _validate_discovery_snapshot(
        *,
        state: sqlite3.Row | None,
        schedules: list[sqlite3.Row],
        current_revisions: dict[str, sqlite3.Row],
        revision_identities: dict[tuple[str, str, str], int],
        batches: list[sqlite3.Row],
        batch_samples: list[sqlite3.Row],
        schedule_evidence: list[sqlite3.Row],
        latest_batch: sqlite3.Row | None,
        latest_samples: list[sqlite3.Row],
        load_row: sqlite3.Row | None,
        admission_row: sqlite3.Row | None,
        fact_group_ids: set[str],
        breach_fact_evidence: set[tuple[str, int]],
        attempt_starts: list[sqlite3.Row],
        admissions: list[sqlite3.Row],
        coverage: CoverageWindows,
        checkpointed_prefix: bool = False,
        deadline_check: Callable[[], None] = lambda: None,
    ) -> None:
        if load_row is not None:
            streak = int(load_row["degraded_streak"])
            reason = load_row["last_reason"]
            decision = load_row["last_decision"]
            modulus = int(load_row["probe_every_cycles"])
            expected_decision = (
                "fresh" if reason is None else ("probe" if streak % modulus == 0 else "yield")
            )
            if (
                int(load_row["updated_at_ms"]) < 0
                or modulus < 2
                or decision != expected_decision
                or (decision == "fresh" and (streak != 0 or reason is not None))
                or (
                    decision in {"yield", "probe"}
                    and (
                        streak <= 0
                        or reason not in {"candidate-quote-missing", "candidate-quote-stale"}
                    )
                )
            ):
                raise ValueError("invalid-discovery-load-state")
        previous: sqlite3.Row | None = None
        samples_by_batch: dict[int, list[sqlite3.Row]] = {}
        for sample in batch_samples:
            deadline_check()
            samples_by_batch.setdefault(int(sample["batch_id"]), []).append(sample)
        evidence_by_key = {
            (int(row["batch_id"]), str(row["group_id"])): row for row in schedule_evidence
        }
        for batch in batches:
            deadline_check()
            counts = (
                int(batch["promoted_count"]),
                int(batch["groups_seen"]),
                int(batch["page_event_count"]),
            )
            if (
                int(batch["sweep_id"]) < 1
                or int(batch["batch_sequence"]) < 1
                or bool(batch["completed"]) != (batch["next_cursor"] is None)
                or int(batch["started_at_ms"]) < 0
                or int(batch["finished_at_ms"]) < 0
                or int(batch["started_at_ms"]) > int(batch["finished_at_ms"])
                or not 0 <= counts[0] <= counts[1] <= counts[2]
            ):
                raise ValueError("invalid-discovery-batch-receipt")
            if previous is None:
                if (
                    not checkpointed_prefix
                    and (
                        int(batch["sweep_id"]) != 1
                        or int(batch["batch_sequence"]) != 1
                    )
                ):
                    raise ValueError("invalid-discovery-batch-sequence")
            elif bool(previous["completed"]):
                if (
                    batch["requested_cursor"] is not None
                    or int(batch["sweep_id"]) != int(previous["sweep_id"]) + 1
                    or int(batch["batch_sequence"]) != 1
                ):
                    raise ValueError("invalid-discovery-sweep-transition")
            elif (
                batch["requested_cursor"] != previous["next_cursor"]
                or int(batch["sweep_id"]) != int(previous["sweep_id"])
                or int(batch["batch_sequence"]) != int(previous["batch_sequence"]) + 1
            ):
                raise ValueError("invalid-discovery-cursor-receipt-chain")
            samples = samples_by_batch.pop(int(batch["id"]), [])
            if len(samples) != int(batch["groups_seen"]) or sum(
                int(row["promoted"]) for row in samples
            ) != int(batch["promoted_count"]):
                raise ValueError("invalid-discovery-historical-sample-count")
            for sample in samples:
                deadline_check()
                try:
                    weight = Decimal(str(sample["liquidity_weight"]))
                except Exception as error:
                    raise ValueError("invalid-discovery-historical-sample") from error
                quality = sample["quality"]
                reason = sample["reason"]
                identity = (
                    str(sample["group_id"]),
                    str(sample["event_id"]),
                    str(sample["membership_hash"]),
                )
                evidence = evidence_by_key.pop(
                    (int(sample["batch_id"]), str(sample["group_id"])),
                    None,
                )
                if (
                    not weight.is_finite()
                    or weight < 0
                    or any(
                        sample[name] is None or not str(sample[name])
                        for name in ("group_id", "event_id", "membership_hash")
                    )
                    or quality
                    not in {
                        "complete-supported",
                        "complete-unsupported",
                        "incomplete-source",
                    }
                    or (
                        quality == "complete-supported"
                        and (
                            identity not in revision_identities
                            or revision_identities[identity] > int(batch["finished_at_ms"])
                        )
                    )
                    or (bool(sample["promoted"]) and quality != "complete-supported")
                    or (quality == "complete-supported" and reason is not None)
                    or (quality != "complete-supported" and (reason is None or not str(reason)))
                    or evidence is None
                    or any(
                        evidence[name] != sample[name]
                        for name in (
                            "event_id",
                            "membership_hash",
                            "quality",
                            "reason",
                            "promoted",
                        )
                    )
                    or int(evidence["effective_at_ms"]) != int(batch["finished_at_ms"])
                ):
                    raise ValueError("invalid-discovery-historical-sample")
            previous = batch
        if samples_by_batch:
            raise ValueError("orphan-discovery-batch-sample")
        if evidence_by_key:
            raise ValueError("orphan-discovery-schedule-evidence")
        admission_keys: set[tuple[str, str, str, int, int]] = set()
        for admission in admissions:
            deadline_check()
            identity = (
                str(admission["group_id"]),
                str(admission["event_id"]),
                str(admission["membership_hash"]),
            )
            proof = OpportunityPerceptionStore._admission_proof_from_row(admission)
            proof.validate()
            promoted_at_ms = int(admission["promoted_at_ms"])
            deadline = int(admission["candidate_start_deadline_at_ms"])
            if (
                identity not in revision_identities
                or revision_identities[identity] > promoted_at_ms
                or deadline != promoted_at_ms + proof.candidate_max_wait_ms
                or admission["effective_start_bound_ms"] != proof.effective_start_bound_ms
                or int(admission["recorded_at_ms"]) < promoted_at_ms
            ):
                raise ValueError("invalid-candidate-admission-receipt")
            admission_keys.add((*identity, promoted_at_ms, deadline))
        for attempt in attempt_starts:
            deadline_check()
            identity = (
                str(attempt["group_id"]),
                str(attempt["event_id"]),
                str(attempt["membership_hash"]),
            )
            if (
                identity not in revision_identities
                or attempt["promoted_at_ms"] is None
                or attempt["candidate_max_wait_ms"] is None
                or not 0 < int(attempt["candidate_max_wait_ms"]) <= 60_000
                or (
                    *identity,
                    int(attempt["promoted_at_ms"]),
                    int(attempt["candidate_start_deadline_at_ms"]),
                )
                not in admission_keys
                or int(attempt["started_at_ms"]) < int(attempt["promoted_at_ms"])
                or int(attempt["candidate_start_deadline_at_ms"])
                != int(attempt["promoted_at_ms"]) + int(attempt["candidate_max_wait_ms"])
                or bool(attempt["deadline_breached"])
                != (int(attempt["started_at_ms"]) > int(attempt["candidate_start_deadline_at_ms"]))
                or (
                    bool(attempt["deadline_breached"])
                    and (
                        str(attempt["group_id"]),
                        int(attempt["started_at_ms"]),
                    )
                    not in breach_fact_evidence
                )
            ):
                raise ValueError("invalid-candidate-attempt-start-receipt")
        admission_proof: DiscoveryAdmissionProof | None = None
        if admission_row is not None:
            admission_proof = OpportunityPerceptionStore._admission_proof_from_row(admission_row)
            admission_proof.validate()
            if (
                admission_row["effective_start_bound_ms"]
                != admission_proof.effective_start_bound_ms
            ):
                raise ValueError("invalid-discovery-admission-bound")
        if state is not None:
            completed = bool(state["completed"])
            if completed != (state["next_cursor"] is None):
                raise ValueError("invalid-discovery-state-cursor")
            if int(state["last_started_at_ms"]) > int(state["last_finished_at_ms"]):
                raise ValueError("invalid-discovery-state-time")
            counts = (
                int(state["page_event_count"]),
                int(state["groups_seen"]),
                int(state["promoted_count"]),
            )
            if any(value < 0 for value in counts) or counts[2] > counts[1]:
                raise ValueError("invalid-discovery-state-counts")
            if latest_batch is None:
                raise ValueError("missing-discovery-batch-receipt")
            state_fields = (
                "next_cursor",
                "completed",
                "last_started_at_ms",
                "last_finished_at_ms",
                "page_event_count",
                "groups_seen",
                "promoted_count",
            )
            receipt_fields = (
                "next_cursor",
                "completed",
                "started_at_ms",
                "finished_at_ms",
                "page_event_count",
                "groups_seen",
                "promoted_count",
            )
            if any(
                state[state_name] != latest_batch[receipt_name]
                for state_name, receipt_name in zip(
                    state_fields,
                    receipt_fields,
                    strict=True,
                )
            ):
                raise ValueError("discovery-state-receipt-mismatch")
            if len(latest_samples) != int(latest_batch["groups_seen"]):
                raise ValueError("discovery-receipt-sample-count-mismatch")
            if sum(int(row["promoted"]) for row in latest_samples) != int(
                latest_batch["promoted_count"]
            ):
                raise ValueError("discovery-receipt-promotion-count-mismatch")
        elif latest_batch is not None or latest_samples:
            raise ValueError("orphan-discovery-batch-receipt")
        for row in schedules:
            decimals = {
                name: Decimal(str(row[name]))
                for name in (
                    "gross_edge_bps",
                    "activity_rank",
                    "liquidity_rank",
                    "change_rank",
                    "age_rank",
                    "priority_score",
                    "liquidity_weight",
                )
            }
            if any(not value.is_finite() for value in decimals.values()):
                raise ValueError("invalid-discovery-schedule-decimal")
            if any(
                not Decimal("0") <= decimals[name] <= Decimal("100")
                for name in ("activity_rank", "liquidity_rank", "change_rank")
            ):
                raise ValueError("invalid-discovery-schedule-rank")
            if not Decimal("0") <= decimals["age_rank"] <= Decimal("200"):
                raise ValueError("invalid-discovery-schedule-age")
            if decimals["liquidity_weight"] < 0:
                raise ValueError("invalid-discovery-schedule-weight")
            if row["quality"] not in {
                "complete-supported",
                "complete-unsupported",
                "incomplete-source",
            } or row["priority_class"] not in {"high", "normal", "explore"}:
                raise ValueError("invalid-discovery-schedule-enum")
            if (
                row["quality"] == "complete-supported"
                and admission_proof is not None
                and (
                    row["promotion_eligible_at_ms"] is None
                    or row["promotion_queue_deadline_at_ms"] is None
                    or int(row["promotion_queue_deadline_at_ms"])
                    != int(row["promotion_eligible_at_ms"]) + admission_proof.candidate_max_wait_ms
                )
            ):
                raise ValueError("invalid-discovery-promotion-queue-deadline")
            if row["promoted_at_ms"] is not None:
                revision = current_revisions.get(str(row["group_id"]))
                if (
                    row["quality"] != "complete-supported"
                    or revision is None
                    or revision["status"] != "certified"
                    or revision["event_id"] != row["event_id"]
                    or revision["membership_hash"] != row["membership_hash"]
                    or (
                        admission_proof is not None
                        and (
                            row["promotion_eligible_at_ms"] is None
                            or row["promotion_queue_deadline_at_ms"] is None
                            or row["candidate_start_deadline_at_ms"] is None
                            or (
                                str(row["group_id"]) not in fact_group_ids
                                and int(row["candidate_start_deadline_at_ms"])
                                != int(row["promoted_at_ms"])
                                + admission_proof.candidate_max_wait_ms
                            )
                        )
                    )
                ):
                    raise ValueError("invalid-discovery-promotion-authority")
            else:
                revision = current_revisions.get(str(row["group_id"]))
                if row["quality"] == "complete-supported":
                    if (
                        revision is None
                        or revision["status"] != "certified"
                        or revision["event_id"] != row["event_id"]
                        or revision["membership_hash"] != row["membership_hash"]
                        or (
                            admission_proof is not None
                            and (
                                row["promotion_eligible_at_ms"] is None
                                or row["promotion_queue_deadline_at_ms"] is None
                                or row["candidate_start_deadline_at_ms"] is not None
                            )
                        )
                    ):
                        raise ValueError("invalid-discovery-queued-promotion-authority")
                elif revision is not None and (
                    revision["event_id"] != row["event_id"]
                    or revision["status"] not in {"invalidated", "closed"}
                ):
                    raise ValueError("invalid-discovery-unpromoted-authority")
            if (
                int(row["first_discovered_at_ms"]) > int(row["last_discovered_at_ms"])
                or (
                    row["last_visited_at_ms"] is not None
                    and int(row["last_visited_at_ms"]) > int(row["last_discovered_at_ms"])
                )
                or (
                    row["promoted_at_ms"] is not None
                    and (
                        int(row["promoted_at_ms"]) < int(row["first_discovered_at_ms"])
                        or (
                            row["promotion_eligible_at_ms"] is not None
                            and int(row["promotion_eligible_at_ms"]) > int(row["promoted_at_ms"])
                        )
                    )
                )
            ):
                raise ValueError("invalid-discovery-schedule-time")
            expected = priority_components(
                GroupScheduleInput(
                    group_id=str(row["group_id"]),
                    gross_edge_bps=decimals["gross_edge_bps"],
                    activity_rank=decimals["activity_rank"],
                    liquidity_rank=decimals["liquidity_rank"],
                    change_rank=decimals["change_rank"],
                    last_visited_at_ms=(
                        None
                        if row["last_visited_at_ms"] is None
                        else int(row["last_visited_at_ms"])
                    ),
                    first_discovered_at_ms=int(row["first_discovered_at_ms"]),
                ),
                now_ms=int(row["last_discovered_at_ms"]),
            )
            if (
                decimals["age_rank"] != expected.age_rank
                or decimals["priority_score"] != expected.score
                or row["priority_reason"] != expected.reason
            ):
                raise ValueError("invalid-discovery-schedule-score")
        if admission_proof is not None:
            outstanding = sum(
                1
                for row in schedules
                if row["promoted_at_ms"] is not None and str(row["group_id"]) not in fact_group_ids
            )
            if outstanding > admission_proof.effective_capacity:
                raise ValueError("discovery-admission-capacity-exceeded")
        for window in coverage.by_minutes.values():
            if (
                window.visited_groups > coverage.known_groups
                or not Decimal("0") <= window.raw_fraction <= Decimal("1")
                or not Decimal("0") <= window.liquidity_weighted_fraction <= Decimal("1")
            ):
                raise ValueError("invalid-discovery-coverage")

    def candidate_freshness_snapshot(
        self,
        *,
        now_ms: int,
    ) -> DurableCandidateFreshness:
        """Read the full current certified set and matching Quote authority once."""
        con = self._connect()
        try:
            con.execute("BEGIN")
            rows = con.execute(
                "WITH current AS ("
                "SELECT r.* FROM neg_risk_group_revisions r JOIN ("
                "SELECT group_id,MAX(revision) AS revision "
                "FROM neg_risk_group_revisions GROUP BY group_id"
                ") c ON c.group_id=r.group_id AND c.revision=r.revision"
                ") SELECT c.group_id,c.membership_hash,"
                "(SELECT MAX(q.quoted_at_ms) FROM neg_risk_group_quote_batches q "
                " WHERE q.group_id=c.group_id "
                " AND q.membership_hash=c.membership_hash "
                " AND q.status='complete' AND q.quoted_at_ms<=?) AS quoted_at_ms "
                "FROM current c LEFT JOIN neg_risk_group_schedule s "
                "ON s.group_id=c.group_id WHERE c.status='certified' AND "
                f"{_ACTUAL_CANDIDATE_AUTHORITY_SQL} "
                "ORDER BY c.group_id",
                (now_ms,),
            ).fetchall()
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        ages = [
            now_ms - int(row["quoted_at_ms"]) for row in rows if row["quoted_at_ms"] is not None
        ]
        missing = len(rows) - len(ages)
        ages.sort()
        p95 = None if not ages else ages[max(0, math.ceil(len(ages) * 0.95) - 1)]
        return DurableCandidateFreshness(
            candidate_count=len(rows),
            quote_p95_age_ms=p95,
            missing_quote_count=missing,
        )

    @staticmethod
    def _candidate_seed_payload(
        con: sqlite3.Connection,
        *,
        through_group_revision_id: int,
        through_quote_rowid: int,
        through_fact_id: int,
        through_receipt_id: int,
    ) -> dict[str, list[dict[str, object]]]:
        def records(query: str, parameter: int) -> list[dict[str, object]]:
            return [dict(row) for row in con.execute(query, (parameter,)).fetchall()]

        return {
            "facts": records(
                "SELECT * FROM neg_risk_candidate_watch_facts "
                "WHERE id<=? ORDER BY id",
                through_fact_id,
            ),
            "groups": records(
                "SELECT * FROM neg_risk_group_revisions WHERE id<=? ORDER BY id",
                through_group_revision_id,
            ),
            "quotes": records(
                "SELECT rowid,* FROM neg_risk_group_quote_batches "
                "WHERE rowid<=? ORDER BY rowid",
                through_quote_rowid,
            ),
            "receipts": records(
                "SELECT * FROM neg_risk_candidate_success_receipts "
                "WHERE id<=? ORDER BY id",
                through_receipt_id,
            ),
        }

    def _validated_candidate_checkpoint(
        self,
        con: sqlite3.Connection,
    ) -> sqlite3.Row | None:
        row = con.execute(
            "SELECT * FROM neg_risk_candidate_authority_checkpoints WHERE id=1"
        ).fetchone()
        if row is None:
            return None
        self._check_deadline("candidate-authority-deadline")
        if (
            row["domain"] != _CANDIDATE_AUTHORITY_DOMAIN
            or int(row["version"]) != _CANDIDATE_AUTHORITY_VERSION
            or int(row["generation"]) <= 0
            or any(
                int(row[name]) < 0
                for name in (
                    "through_group_revision_id",
                    "through_quote_rowid",
                    "through_fact_id",
                    "through_receipt_id",
                    "compacted_group_rows",
                    "compacted_quote_rows",
                    "compacted_fact_rows",
                    "compacted_receipt_rows",
                )
            )
            or len(str(row["seeds_json"]).encode("utf-8")) > 33_554_432
        ):
            raise ValueError("invalid-candidate-authority-checkpoint")
        try:
            parsed_seeds = json.loads(str(row["seeds_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            raise ValueError("invalid-candidate-authority-checkpoint")
        raw_seed_names = {"facts", "groups", "quotes", "receipts"}
        if (
            not isinstance(parsed_seeds, dict)
            or not raw_seed_names.issubset(parsed_seeds)
            or set(parsed_seeds) - raw_seed_names - {"timeline_states"}
        ):
            raise ValueError("invalid-candidate-authority-checkpoint")
        timeline_states = parsed_seeds.get("timeline_states", {})
        if not isinstance(timeline_states, dict) or any(
            not isinstance(group_id, str)
            or not group_id
            or not isinstance(state, dict)
            or set(state) != {"last_result", "opportunity"}
            or state["last_result"] not in {"watching", "no-edge", "unavailable"}
            or type(state["opportunity"]) is not bool
            or (state["opportunity"] and state["last_result"] != "watching")
            for group_id, state in timeline_states.items()
        ):
            raise ValueError("invalid-candidate-authority-checkpoint")
        canonical_seeds = json.dumps(
            parsed_seeds,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        seeds_digest = f"sha256:{hashlib.sha256(canonical_seeds.encode()).hexdigest()}"
        expected_hash = candidate_authority_checkpoint_hash(
            domain=str(row["domain"]),
            version=int(row["version"]),
            generation=int(row["generation"]),
            through_group_revision_id=int(row["through_group_revision_id"]),
            through_quote_rowid=int(row["through_quote_rowid"]),
            through_fact_id=int(row["through_fact_id"]),
            through_receipt_id=int(row["through_receipt_id"]),
            compacted_group_rows=int(row["compacted_group_rows"]),
            compacted_quote_rows=int(row["compacted_quote_rows"]),
            compacted_fact_rows=int(row["compacted_fact_rows"]),
            compacted_receipt_rows=int(row["compacted_receipt_rows"]),
            prefix_digest=str(row["prefix_digest"]),
            seeds_digest=str(row["seeds_digest"]),
        )
        actual_seeds = self._candidate_seed_payload(
            con,
            through_group_revision_id=int(row["through_group_revision_id"]),
            through_quote_rowid=int(row["through_quote_rowid"]),
            through_fact_id=int(row["through_fact_id"]),
            through_receipt_id=int(row["through_receipt_id"]),
        )
        parsed_raw_seeds = {
            name: parsed_seeds[name] for name in raw_seed_names
        }
        if (
            canonical_seeds != str(row["seeds_json"])
            or not hmac.compare_digest(str(row["seeds_digest"]), seeds_digest)
            or not hmac.compare_digest(str(row["checkpoint_hash"]), expected_hash)
            or actual_seeds != parsed_raw_seeds
        ):
            raise ValueError("invalid-candidate-authority-checkpoint")
        return row

    def _refresh_candidate_checkpoint(
        self,
        con: sqlite3.Connection,
        validated_checkpoint: sqlite3.Row,
    ) -> None:
        """Rebind retained seeds after an authorized in-transaction mutation."""
        through_group = int(validated_checkpoint["through_group_revision_id"])
        through_quote = int(validated_checkpoint["through_quote_rowid"])
        through_fact = int(validated_checkpoint["through_fact_id"])
        through_receipt = int(validated_checkpoint["through_receipt_id"])
        seeds = self._candidate_seed_payload(
            con,
            through_group_revision_id=through_group,
            through_quote_rowid=through_quote,
            through_fact_id=through_fact,
            through_receipt_id=through_receipt,
        )
        previous_seeds = json.loads(str(validated_checkpoint["seeds_json"]))
        if "timeline_states" in previous_seeds:
            seeds["timeline_states"] = previous_seeds["timeline_states"]
        seeds_json = json.dumps(
            seeds,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        seeds_digest = f"sha256:{hashlib.sha256(seeds_json.encode()).hexdigest()}"
        generation = int(validated_checkpoint["generation"]) + 1
        checkpoint_hash = candidate_authority_checkpoint_hash(
            domain=str(validated_checkpoint["domain"]),
            version=int(validated_checkpoint["version"]),
            generation=generation,
            through_group_revision_id=through_group,
            through_quote_rowid=through_quote,
            through_fact_id=through_fact,
            through_receipt_id=through_receipt,
            compacted_group_rows=int(validated_checkpoint["compacted_group_rows"]),
            compacted_quote_rows=int(validated_checkpoint["compacted_quote_rows"]),
            compacted_fact_rows=int(validated_checkpoint["compacted_fact_rows"]),
            compacted_receipt_rows=int(validated_checkpoint["compacted_receipt_rows"]),
            prefix_digest=str(validated_checkpoint["prefix_digest"]),
            seeds_digest=seeds_digest,
        )
        con.execute(
            "UPDATE neg_risk_candidate_authority_checkpoints SET "
            "generation=?,seeds_json=?,seeds_digest=?,checkpoint_hash=? WHERE id=1",
            (generation, seeds_json, seeds_digest, checkpoint_hash),
        )

    def _compact_candidate_authority(self, con: sqlite3.Connection) -> None:
        counts = con.execute(
            "SELECT "
            "(SELECT COUNT(*) FROM neg_risk_group_quote_batches),"
            "(SELECT COUNT(*) FROM neg_risk_candidate_watch_facts),"
            "(SELECT COUNT(*) FROM neg_risk_candidate_success_receipts),"
            "(SELECT COALESCE(SUM(length(legs_json)),0) "
            " FROM neg_risk_group_quote_batches)"
        ).fetchone()
        if (
            max(int(value) for value in counts[:3])
            <= _CANDIDATE_AUTHORITY_COMPACT_HIGH_ROWS
            and int(counts[3]) <= _CANDIDATE_AUTHORITY_COMPACT_HIGH_BYTES
        ):
            return

        # No prefix is discarded until the complete pre-compaction authority
        # chain has passed the same fail-closed validator used by /status.
        self.validated_candidate_opportunity_count(_connection=con)
        previous = self._validated_candidate_checkpoint(con)
        through = con.execute(
            "SELECT "
            "(SELECT COALESCE(MAX(rowid),0) FROM neg_risk_group_quote_batches),"
            "(SELECT COALESCE(MAX(id),0) FROM neg_risk_candidate_watch_facts),"
            "(SELECT COALESCE(MAX(id),0) "
            " FROM neg_risk_candidate_success_receipts)"
        ).fetchone()
        through_group = 0
        through_quote, through_fact, through_receipt = (int(value) for value in through)

        # Retain the physical rows needed by ordinary readers and by the next
        # suffix replay only for current Candidate authority. Historical groups
        # whose latest revision revoked certification are already committed by
        # prefix_digest and must not make the live seed grow without bound.
        before_counts = {
            "groups": 0,
            "quotes": int(
                con.execute(
                    "SELECT COUNT(*) FROM neg_risk_group_quote_batches WHERE rowid<=?",
                    (through_quote,),
                ).fetchone()[0]
            ),
            "facts": int(
                con.execute(
                    "SELECT COUNT(*) FROM neg_risk_candidate_watch_facts WHERE id<=?",
                    (through_fact,),
                ).fetchone()[0]
            ),
            "receipts": int(
                con.execute(
                    "SELECT COUNT(*) FROM neg_risk_candidate_success_receipts WHERE id<=?",
                    (through_receipt,),
                ).fetchone()[0]
            ),
        }
        prefix_payload = {
            "previous_prefix_digest": (
                None if previous is None else str(previous["prefix_digest"])
            ),
            "rows": self._candidate_seed_payload(
                con,
                through_group_revision_id=through_group,
                through_quote_rowid=through_quote,
                through_fact_id=through_fact,
                through_receipt_id=through_receipt,
            ),
        }
        prefix_json = json.dumps(
            prefix_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        prefix_digest = f"sha256:{hashlib.sha256(prefix_json.encode()).hexdigest()}"

        receipt_delete_predicate = "WHERE id<?"
        deleted_receipt_groups = [
            str(row["group_id"])
            for row in con.execute(
                "SELECT group_id FROM neg_risk_candidate_success_receipts "
                + receipt_delete_predicate
                + " ORDER BY id",
                (through_receipt,),
            )
        ]
        if deleted_receipt_groups:
            token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_candidate_success_receipts",
                operation="DELETE",
                row_key="*",
            )
            con.execute(
                "DELETE FROM neg_risk_candidate_success_receipts "
                + receipt_delete_predicate,
                (through_receipt,),
            )
            self._consume_expected_owner_mutations(
                con,
                writer_token=token,
                table_name="neg_risk_candidate_success_receipts",
                operation="DELETE",
                expected_row_keys=deleted_receipt_groups,
            )
        quote_delete_predicate = "WHERE rowid<?"
        deleted_quote_groups = [
            str(row["group_id"])
            for row in con.execute(
                "SELECT group_id FROM neg_risk_group_quote_batches "
                + quote_delete_predicate
                + " ORDER BY rowid",
                (through_quote,),
            )
        ]
        if deleted_quote_groups:
            token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_group_quote_batches",
                operation="DELETE",
                row_key="*",
            )
            con.execute(
                "DELETE FROM neg_risk_group_quote_batches "
                + quote_delete_predicate,
                (through_quote,),
            )
            self._consume_expected_owner_mutations(
                con,
                writer_token=token,
                table_name="neg_risk_group_quote_batches",
                operation="DELETE",
                expected_row_keys=deleted_quote_groups,
            )
        fact_delete_predicate = "WHERE id<?"
        deleted_fact_group_ids = [
            str(row["group_id"])
            for row in con.execute(
                "SELECT group_id FROM neg_risk_candidate_watch_facts "
                + fact_delete_predicate
                + " ORDER BY id",
                (through_fact,),
            ).fetchall()
        ]
        if deleted_fact_group_ids:
            writer_token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_candidate_watch_facts",
                operation="DELETE",
                row_key="*",
            )
            con.execute(
                "DELETE FROM neg_risk_candidate_watch_facts "
                + fact_delete_predicate,
                (through_fact,),
            )
            self._consume_expected_owner_mutations(
                con,
                writer_token=writer_token,
                table_name="neg_risk_candidate_watch_facts",
                operation="DELETE",
                expected_row_keys=deleted_fact_group_ids,
            )
        seeds = self._candidate_seed_payload(
            con,
            through_group_revision_id=through_group,
            through_quote_rowid=through_quote,
            through_fact_id=through_fact,
            through_receipt_id=through_receipt,
        )
        self.candidate_current_summary(_connection=con)
        timeline_states = {
            str(row["group_id"]): {
                "last_result": str(row["last_result"]),
                "opportunity": bool(row["opportunity"]),
            }
            for row in con.execute(
                "SELECT group_id,last_result,opportunity "
                "FROM neg_risk_candidate_current_authority ORDER BY group_id"
            ).fetchall()
        }
        seeds["timeline_states"] = timeline_states
        seeds_json = json.dumps(
            seeds,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        seeds_digest = f"sha256:{hashlib.sha256(seeds_json.encode()).hexdigest()}"
        after_counts = {
            name: len(seeds[name])
            for name in ("groups", "quotes", "facts", "receipts")
        }
        compacted_columns = {
            "groups": "compacted_group_rows",
            "quotes": "compacted_quote_rows",
            "facts": "compacted_fact_rows",
            "receipts": "compacted_receipt_rows",
        }
        prior_compacted = {
            name: 0 if previous is None else int(previous[column])
            for name, column in compacted_columns.items()
        }
        compacted = {
            name: prior_compacted[name] + before_counts[name] - after_counts[name]
            for name in before_counts
        }
        generation = 1 if previous is None else int(previous["generation"]) + 1
        checkpoint_hash = candidate_authority_checkpoint_hash(
            domain=_CANDIDATE_AUTHORITY_DOMAIN,
            version=_CANDIDATE_AUTHORITY_VERSION,
            generation=generation,
            through_group_revision_id=through_group,
            through_quote_rowid=through_quote,
            through_fact_id=through_fact,
            through_receipt_id=through_receipt,
            compacted_group_rows=compacted["groups"],
            compacted_quote_rows=compacted["quotes"],
            compacted_fact_rows=compacted["facts"],
            compacted_receipt_rows=compacted["receipts"],
            prefix_digest=prefix_digest,
            seeds_digest=seeds_digest,
        )
        con.execute(
            "INSERT INTO neg_risk_candidate_authority_checkpoints("
            "id,domain,version,generation,through_group_revision_id,"
            "through_quote_rowid,through_fact_id,through_receipt_id,"
            "compacted_group_rows,compacted_quote_rows,compacted_fact_rows,"
            "compacted_receipt_rows,prefix_digest,seeds_json,seeds_digest,"
            "checkpoint_hash) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "domain=excluded.domain,version=excluded.version,"
            "generation=excluded.generation,"
            "through_group_revision_id=excluded.through_group_revision_id,"
            "through_quote_rowid=excluded.through_quote_rowid,"
            "through_fact_id=excluded.through_fact_id,"
            "through_receipt_id=excluded.through_receipt_id,"
            "compacted_group_rows=excluded.compacted_group_rows,"
            "compacted_quote_rows=excluded.compacted_quote_rows,"
            "compacted_fact_rows=excluded.compacted_fact_rows,"
            "compacted_receipt_rows=excluded.compacted_receipt_rows,"
            "prefix_digest=excluded.prefix_digest,seeds_json=excluded.seeds_json,"
            "seeds_digest=excluded.seeds_digest,"
            "checkpoint_hash=excluded.checkpoint_hash",
            (
                _CANDIDATE_AUTHORITY_DOMAIN,
                _CANDIDATE_AUTHORITY_VERSION,
                generation,
                through_group,
                through_quote,
                through_fact,
                through_receipt,
                compacted["groups"],
                compacted["quotes"],
                compacted["facts"],
                compacted["receipts"],
                prefix_digest,
                seeds_json,
                seeds_digest,
                checkpoint_hash,
            ),
        )
        # The checkpoint and prefix deletes share the caller's transaction.
        # A final replay catches implementation drift before commit.
        self.validated_candidate_opportunity_count(_connection=con)

    def validated_candidate_opportunity_count(
        self,
        *,
        _connection: sqlite3.Connection | None = None,
    ) -> int:
        """Replay the complete Candidate authority chain in one read snapshot."""
        owns_connection = _connection is None
        con = self._connect() if _connection is None else _connection
        self._install_owner_write_authorizer(con)
        try:
            if owns_connection:
                con.execute("BEGIN")
                self._assert_owner_journal_clean(con)
                self._validated_candidate_checkpoint(con)
                aggregate = con.execute(
                    "SELECT current_group_count,opportunity_count,watching_count,"
                    "no_edge_count,unavailable_count,aggregate_digest "
                    "FROM neg_risk_candidate_current_aggregate WHERE id=1"
                ).fetchone()
                if (
                    aggregate is None
                    or int(aggregate["current_group_count"]) < 0
                    or int(aggregate["opportunity_count"]) < 0
                    or int(aggregate["opportunity_count"])
                    > int(aggregate["current_group_count"])
                    or any(
                        int(aggregate[column]) < 0
                        for column in (
                            "watching_count",
                            "no_edge_count",
                            "unavailable_count",
                        )
                    )
                    or sum(
                        int(aggregate[column])
                        for column in (
                            "watching_count",
                            "no_edge_count",
                            "unavailable_count",
                        )
                    )
                    != int(aggregate["current_group_count"])
                    or len(str(aggregate["aggregate_digest"])) != 64
                ):
                    raise ValueError("invalid-candidate-current-aggregate")
                con.execute("COMMIT")
                return int(aggregate["opportunity_count"])
            checkpoint = self._validated_candidate_checkpoint(con)
            sizes = con.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM neg_risk_group_quote_batches),"
                "(SELECT COUNT(*) FROM neg_risk_candidate_watch_facts),"
                "(SELECT COUNT(*) FROM neg_risk_candidate_success_receipts),"
                "(SELECT COUNT(DISTINCT group_id) "
                " FROM neg_risk_group_quote_batches),"
                "(SELECT COUNT(DISTINCT group_id) "
                " FROM neg_risk_candidate_watch_facts),"
                "(SELECT COUNT(DISTINCT group_id) "
                " FROM neg_risk_candidate_success_receipts),"
                "(SELECT COALESCE(MAX(length(legs_json)),0) "
                " FROM neg_risk_group_quote_batches),"
                "(SELECT COALESCE(SUM(length(legs_json)),0) "
                " FROM neg_risk_group_quote_batches)"
            ).fetchone()
            if (
                any(
                    int(total) - int(current)
                    > _CANDIDATE_AUTHORITY_UNCOMPACTED_MAX_ROWS
                    for total, current in zip(sizes[:3], sizes[3:6], strict=True)
                )
                or int(sizes[6]) > 65_536
                or int(sizes[7]) > 8_388_608
            ):
                raise ValueError("candidate-authority-bound-exceeded")
            quote_rows = con.execute(
                "SELECT rowid,* FROM neg_risk_group_quote_batches ORDER BY rowid"
            ).fetchall()
            fact_rows = con.execute(
                "SELECT * FROM neg_risk_candidate_watch_facts ORDER BY id"
            ).fetchall()
            receipt_rows = con.execute(
                "SELECT * FROM neg_risk_candidate_success_receipts ORDER BY id"
            ).fetchall()
            candidate_group_ids = sorted(
                {
                    str(row["group_id"])
                    for row in (*quote_rows, *fact_rows, *receipt_rows)
                }
            )
            candidate_group_anchor_ids: set[int] = set()
            if candidate_group_ids:
                minimum_times: dict[str, int] = {}
                for row in (*quote_rows, *fact_rows):
                    group_id = str(row["group_id"])
                    observed_at_ms = int(
                        row[
                            "quoted_at_ms"
                            if "quoted_at_ms" in row.keys()
                            else "observed_at_ms"
                        ]
                    )
                    minimum_times[group_id] = min(
                        observed_at_ms,
                        minimum_times.get(group_id, observed_at_ms),
                    )
                group_rows = []
                for group_id in candidate_group_ids:
                    anchor = con.execute(
                        "SELECT * FROM neg_risk_group_revisions "
                        "WHERE group_id=? AND observed_at_ms<=? "
                        "ORDER BY observed_at_ms DESC,revision DESC LIMIT 1",
                        (group_id, minimum_times.get(group_id, 0)),
                    ).fetchone()
                    if anchor is None:
                        raise ValueError("invalid-candidate-group-history")
                    candidate_group_anchor_ids.add(int(anchor["id"]))
                    group_rows.extend(
                        con.execute(
                            "SELECT * FROM neg_risk_group_revisions "
                            "WHERE group_id=? AND revision>=? ORDER BY revision",
                            (group_id, int(anchor["revision"])),
                        ).fetchall()
                    )
            else:
                group_rows = []
            groups_by_id: dict[int, tuple[sqlite3.Row, GroupRevision]] = {}
            groups_by_revision: dict[tuple[str, int], GroupRevision] = {}
            group_history: dict[str, list[GroupRevision]] = {}
            current_groups: dict[str, GroupRevision] = {}
            for row in group_rows:
                self._check_deadline("candidate-authority-deadline")
                group = self._validated_group_from_row(row)
                if group is None:
                    raise ValueError("invalid-candidate-group-history")
                key = (group.group_id, group.revision)
                if key in groups_by_revision:
                    raise ValueError("invalid-candidate-group-history")
                history = group_history.setdefault(group.group_id, [])
                previous = history[-1] if history else None
                checkpoint_seed = (
                    checkpoint is not None
                    and int(row["id"]) <= int(checkpoint["through_group_revision_id"])
                )
                bounded_anchor = int(row["id"]) in candidate_group_anchor_ids
                allowed_transitions = {
                    "discovered": {"certified", "invalidated", "closed"},
                    "certified": {"certified", "stale", "invalidated", "closed"},
                    "stale": {"certified", "stale", "invalidated", "closed"},
                    "invalidated": {"certified", "invalidated", "closed"},
                    "closed": {"closed"},
                }
                if (
                    (
                        previous is None
                        and group.revision != 1
                        and not checkpoint_seed
                        and not bounded_anchor
                    )
                    or (
                        previous is not None
                        and (
                            group.event_id != previous.event_id
                            or group.revision != previous.revision + 1
                            or group.started_at_ms < previous.started_at_ms
                            or group.observed_at_ms < previous.observed_at_ms
                            or group.status not in allowed_transitions[previous.status]
                        )
                    )
                ):
                    raise ValueError("invalid-candidate-group-history")
                history.append(group)
                groups_by_id[int(row["id"])] = (row, group)
                groups_by_revision[key] = group
                current_groups[group.group_id] = group
            quotes_by_id: dict[str, tuple[sqlite3.Row, GroupQuoteBatch]] = {}
            quotes_by_rowid: dict[int, tuple[sqlite3.Row, GroupQuoteBatch]] = {}
            complete_quote_ids: set[str] = set()
            for row in quote_rows:
                self._check_deadline("candidate-authority-deadline")
                group = groups_by_revision.get(
                    (str(row["group_id"]), int(row["group_revision"]))
                )
                if group is None:
                    raise ValueError("invalid-candidate-quote-history")
                try:
                    quote = self._quote_batch_from_row(row)
                except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                    raise ValueError("invalid-candidate-quote-history")
                as_of = [
                    revision
                    for revision in group_history[group.group_id]
                    if revision.observed_at_ms <= quote.quoted_at_ms
                ]
                as_of_group = None if not as_of else as_of[-1]
                if (
                    quote.started_at_ms > quote.quoted_at_ms
                    or as_of_group is None
                    or as_of_group.revision != group.revision
                    or quote.membership_hash != group.membership_hash
                ):
                    raise ValueError("invalid-candidate-quote-history")
                if quote.status == "failed":
                    reason = quote.failure_reason
                    if (
                        not isinstance(reason, str)
                        or not reason.strip()
                        or len(reason.encode("utf-8")) > 1_024
                        or quote.legs
                    ):
                        raise ValueError("invalid-candidate-quote-history")
                    continue
                if quote.status not in {"complete", "superseded"}:
                    raise ValueError("invalid-candidate-quote-history")
                expected = GroupQuoteBatch.complete(
                    group_id=quote.group_id,
                    membership_hash=quote.membership_hash,
                    quote_batch_id=quote.quote_batch_id,
                    started_at_ms=quote.started_at_ms,
                    quoted_at_ms=quote.quoted_at_ms,
                    legs=quote.legs,
                )
                if replace(quote, status="complete") != expected:
                    raise ValueError("invalid-candidate-quote-history")
                if tuple(leg.yes_token_id for leg in quote.legs) != tuple(
                    leg.yes_token_id for leg in group.legs
                ):
                    raise ValueError("invalid-candidate-quote-history")
                current = current_groups[group.group_id]
                if (
                    quote.status == "complete"
                    and (
                        group.revision != current.revision
                        or current.status != "certified"
                    )
                ) or (
                    quote.status == "superseded"
                    and group.revision >= current.revision
                ):
                    raise ValueError("invalid-candidate-quote-history")
                quotes_by_id[quote.quote_batch_id] = (row, quote)
                quotes_by_rowid[int(row["rowid"])] = (row, quote)
                if quote.status == "complete":
                    complete_quote_ids.add(quote.quote_batch_id)
            facts_by_id: dict[int, sqlite3.Row] = {}
            latest_fact: dict[str, sqlite3.Row] = {}
            previous_fact_at: dict[str, int] = {}
            for row in fact_rows:
                self._check_deadline("candidate-authority-deadline")
                result = str(row["last_result"])
                success = result in {"watching", "no-edge"}
                numeric = ("bundle_cost", "gross_edge_bps", "max_bundle_size")
                group_id = str(row["group_id"])
                observed_at_ms = int(row["observed_at_ms"])
                as_of = [
                    revision
                    for revision in group_history.get(group_id, [])
                    if revision.observed_at_ms <= observed_at_ms
                ]
                as_of_group = None if not as_of else as_of[-1]
                unavailable = result == "unavailable"
                if (
                    result not in {"watching", "no-edge", "unavailable"}
                    or row["priority_class"] not in {"high", "normal", "explore"}
                    or observed_at_ms < 0
                    or observed_at_ms < previous_fact_at.get(group_id, 0)
                    or int(row["next_due_at_ms"]) < observed_at_ms
                    or not math.isfinite(float(row["effective_interval_s"]))
                    or float(row["effective_interval_s"]) <= 0
                    or as_of_group is None
                    or (
                        success
                        and (
                            not all(
                                row[name] is not None
                                and math.isfinite(float(row[name]))
                                for name in numeric
                            )
                            or row["quote_batch_id"] not in quotes_by_id
                        )
                    )
                    or (
                        unavailable
                        and (
                            not isinstance(row["reason"], str)
                            or not str(row["reason"]).strip()
                            or len(str(row["reason"]).encode("utf-8")) > 1_024
                            or row["quote_batch_id"] is not None
                            or any(row[name] is not None for name in numeric)
                            or (
                                row["membership_hash"] is not None
                                and row["membership_hash"]
                                != as_of_group.membership_hash
                            )
                        )
                    )
                ):
                    raise ValueError("invalid-candidate-fact-history")
                previous_fact_at[group_id] = observed_at_ms
                facts_by_id[int(row["id"])] = row
                latest_fact[group_id] = row
            receipt_fact_ids: set[int] = set()
            for receipt in receipt_rows:
                self._check_deadline("candidate-authority-deadline")
                group_pair = groups_by_id.get(int(receipt["group_revision_row_id"]))
                quote_pair = quotes_by_rowid.get(int(receipt["quote_batch_row_id"]))
                fact = facts_by_id.get(int(receipt["candidate_fact_row_id"]))
                expected_hash = candidate_success_receipt_hash(
                    transaction_id=receipt["transaction_id"],
                    group_id=receipt["group_id"],
                    event_id=receipt["event_id"],
                    membership_hash=receipt["membership_hash"],
                    quote_batch_id=receipt["quote_batch_id"],
                    group_revision_row_id=receipt["group_revision_row_id"],
                    quote_batch_row_id=receipt["quote_batch_row_id"],
                    candidate_fact_row_id=receipt["candidate_fact_row_id"],
                    observed_at_ms=receipt["observed_at_ms"],
                )
                if (
                    group_pair is None
                    or quote_pair is None
                    or fact is None
                    or not hmac.compare_digest(str(receipt["receipt_hash"]), expected_hash)
                ):
                    raise ValueError("invalid-candidate-success-receipt")
                group_row, group = group_pair
                quote_row, quote = quote_pair
                if (
                    receipt["group_id"] != group.group_id
                    or receipt["event_id"] != group.event_id
                    or receipt["membership_hash"] != group.membership_hash
                    or receipt["quote_batch_id"] != quote.quote_batch_id
                    or quote_row["group_revision"] != group_row["revision"]
                    or fact["group_id"] != group.group_id
                    or fact["membership_hash"] != group.membership_hash
                    or fact["quote_batch_id"] != quote.quote_batch_id
                    or fact["observed_at_ms"] != quote.quoted_at_ms
                    or receipt["observed_at_ms"] != quote.quoted_at_ms
                ):
                    raise ValueError("invalid-candidate-success-receipt")
                receipt_fact_ids.add(int(fact["id"]))
            if any(
                row["last_result"] in {"watching", "no-edge"}
                and int(row["id"]) not in receipt_fact_ids
                for row in fact_rows
            ):
                raise ValueError("missing-candidate-success-receipt")
            count = sum(
                row["last_result"] == "watching"
                and float(row["gross_edge_bps"]) > 0
                and row["quote_batch_id"] in complete_quote_ids
                and current_groups.get(group_id) is not None
                and current_groups[group_id].status == "certified"
                and row["membership_hash"] == current_groups[group_id].membership_hash
                for group_id, row in latest_fact.items()
            )
            if owns_connection:
                con.execute("COMMIT")
            return count
        except BaseException:
            if owns_connection and con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            if owns_connection:
                con.close()

    def candidate_current_summary(
        self,
        *,
        _connection: sqlite3.Connection | None = None,
    ) -> CandidateCurrentSummary:
        owns_connection = _connection is None
        con = self._connect() if _connection is None else _connection
        self._install_owner_write_authorizer(con)
        try:
            if owns_connection:
                con.execute("BEGIN")
            self._assert_owner_journal_clean(con)
            guard = con.execute(
                "SELECT candidate_aggregate_hash FROM "
                "neg_risk_owner_mutation_guard WHERE id=1"
            ).fetchone()
            row = con.execute(
                "SELECT current_group_count,opportunity_count,watching_count,"
                "no_edge_count,unavailable_count,aggregate_digest "
                "FROM neg_risk_candidate_current_aggregate WHERE id=1"
            ).fetchone()
            if (
                row is None
                or guard is None
                or not isinstance(guard["candidate_aggregate_hash"], str)
                or not str(guard["candidate_aggregate_hash"]).startswith(
                    "sha256:"
                )
                or len(str(guard["candidate_aggregate_hash"])) != 71
            ):
                raise ValueError("invalid-candidate-current-aggregate")
            current_group_count = int(row["current_group_count"])
            opportunity_count = int(row["opportunity_count"])
            state_counts = {
                "watching": int(row["watching_count"]),
                "no-edge": int(row["no_edge_count"]),
                "unavailable": int(row["unavailable_count"]),
            }
            if (
                current_group_count < 0
                or not 0 <= opportunity_count <= state_counts["watching"]
                or any(count < 0 for count in state_counts.values())
                or sum(state_counts.values()) != current_group_count
                or len(str(row["aggregate_digest"])) != 64
            ):
                raise ValueError("invalid-candidate-current-aggregate")
            if owns_connection:
                con.execute("COMMIT")
            return CandidateCurrentSummary(
                current_group_count=current_group_count,
                opportunity_count=opportunity_count,
                state_counts=state_counts,
                authority_hash=str(guard["candidate_aggregate_hash"]),
            )
        except BaseException:
            if owns_connection and con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            if owns_connection:
                con.close()

    def current_opportunities(
        self,
        *,
        after_group_id: str,
        limit: int,
        _connection: sqlite3.Connection | None = None,
    ) -> tuple[tuple[CurrentOpportunity, ...], str | None]:
        if (
            not 1 <= limit <= 500
            or len(after_group_id) > 256
            or "\x00" in after_group_id
        ):
            raise ValueError("invalid-current-opportunity-page")
        owns_connection = _connection is None
        con = self._connect() if _connection is None else _connection
        self._install_owner_write_authorizer(con)
        try:
            if owns_connection:
                con.execute("BEGIN")
            summary = self.candidate_current_summary(_connection=con)
            rows = con.execute(
                "SELECT * FROM neg_risk_candidate_current_authority "
                "WHERE opportunity=1 AND group_id>? ORDER BY group_id LIMIT ?",
                (after_group_id, limit + 1),
            ).fetchall()
            has_more = len(rows) > limit
            page_rows = rows[:limit]
            items: list[CurrentOpportunity] = []
            for row in page_rows:
                self._check_deadline("candidate-current-read-deadline")
                try:
                    canonical_text = str(row["canonical_json"])
                    canonical = json.loads(canonical_text)
                    expected_hash = (
                        "sha256:"
                        + hashlib.sha256(canonical_text.encode()).hexdigest()
                    )
                    bundle_cost = Decimal(str(canonical["bundle_cost"]))
                    gross_edge_bps = Decimal(str(canonical["gross_edge_bps"]))
                    max_bundle_size = Decimal(str(canonical["max_bundle_size"]))
                    item = CurrentOpportunity(
                        group_id=str(row["group_id"]),
                        event_id=str(row["event_id"]),
                        group_revision=int(row["group_revision"]),
                        membership_hash=str(row["membership_hash"]),
                        quote_batch_id=str(row["quote_batch_id"]),
                        fact_id=int(row["fact_id"]),
                        bundle_cost=bundle_cost,
                        gross_edge_bps=gross_edge_bps,
                        max_bundle_size=max_bundle_size,
                        structure_observed_at_ms=int(
                            canonical["structure_observed_at_ms"]
                        ),
                        quote_started_at_ms=int(
                            canonical["quote_started_at_ms"]
                        ),
                        quote_quoted_at_ms=int(canonical["quote_quoted_at_ms"]),
                    )
                except (
                    KeyError,
                    TypeError,
                    ValueError,
                    json.JSONDecodeError,
                ) as error:
                    raise ValueError("invalid-candidate-current-authority") from error
                if (
                    not isinstance(canonical, dict)
                    or row["row_hash"] != expected_hash
                    or canonical.get("group_id") != item.group_id
                    or canonical.get("event_id") != item.event_id
                    or canonical.get("group_revision") != item.group_revision
                    or canonical.get("membership_hash") != item.membership_hash
                    or canonical.get("quote_batch_id") != item.quote_batch_id
                    or canonical.get("fact_id") != item.fact_id
                    or canonical.get("last_result") != "watching"
                    or canonical.get("opportunity") != 1
                    or not all(
                        value.is_finite()
                        for value in (
                            item.bundle_cost,
                            item.gross_edge_bps,
                            item.max_bundle_size,
                        )
                    )
                    or item.bundle_cost <= 0
                    or item.gross_edge_bps <= 0
                    or item.max_bundle_size <= 0
                    or item.group_revision <= 0
                    or item.structure_observed_at_ms < 0
                    or not (
                        0
                        <= item.quote_started_at_ms
                        <= item.quote_quoted_at_ms
                    )
                ):
                    raise ValueError("invalid-candidate-current-authority")
                items.append(item)
            if len(items) > summary.opportunity_count:
                raise ValueError("invalid-candidate-current-aggregate")
            next_after = items[-1].group_id if has_more else None
            if owns_connection:
                con.execute("COMMIT")
            return tuple(items), next_after
        except BaseException:
            if owns_connection and con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            if owns_connection:
                con.close()

    @staticmethod
    def _group_schedule_from_row(row: sqlite3.Row) -> GroupSchedule:
        return GroupSchedule(
            group_id=str(row["group_id"]),
            event_id=str(row["event_id"]),
            membership_hash=str(row["membership_hash"]),
            quality=row["quality"],
            reason=None if row["reason"] is None else str(row["reason"]),
            gross_edge_bps=Decimal(str(row["gross_edge_bps"])),
            activity_rank=Decimal(str(row["activity_rank"])),
            liquidity_rank=Decimal(str(row["liquidity_rank"])),
            change_rank=Decimal(str(row["change_rank"])),
            age_rank=Decimal(str(row["age_rank"])),
            priority_score=Decimal(str(row["priority_score"])),
            priority_reason=str(row["priority_reason"]),
            priority_class=row["priority_class"],
            liquidity_weight=Decimal(str(row["liquidity_weight"])),
            first_discovered_at_ms=int(row["first_discovered_at_ms"]),
            last_discovered_at_ms=int(row["last_discovered_at_ms"]),
            last_visited_at_ms=(
                None if row["last_visited_at_ms"] is None else int(row["last_visited_at_ms"])
            ),
            promoted_at_ms=(None if row["promoted_at_ms"] is None else int(row["promoted_at_ms"])),
            promotion_eligible_at_ms=(
                None
                if row["promotion_eligible_at_ms"] is None
                else int(row["promotion_eligible_at_ms"])
            ),
            promotion_queue_deadline_at_ms=(
                None
                if row["promotion_queue_deadline_at_ms"] is None
                else int(row["promotion_queue_deadline_at_ms"])
            ),
            candidate_start_deadline_at_ms=(
                None
                if row["candidate_start_deadline_at_ms"] is None
                else int(row["candidate_start_deadline_at_ms"])
            ),
        )

    def begin_reconciliation(self, *, started_at_ms: int) -> ReconciliationWindow:
        if started_at_ms < 0:
            raise ValueError("invalid-reconciliation-start")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows "
                "WHERE status IN ('open','complete') "
                "AND failure_reason IS NULL ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            if existing is not None:
                if existing["baseline_digest"] is not None:
                    con.execute("COMMIT")
                    return self._reconciliation_window_from_row(existing)
                con.execute(
                    "UPDATE neg_risk_reconciliation_windows SET "
                    "failure_reason='baseline-proof-unavailable',"
                    "finished_at_ms=MAX(checkpoint_at_ms,?) WHERE id=?",
                    (started_at_ms, existing["id"]),
                )
            window_id = uuid.uuid4().hex
            baseline = con.execute(
                "SELECT r.group_id,r.event_id,r.revision,r.membership_hash,"
                "r.status FROM neg_risk_group_revisions r "
                "JOIN (SELECT group_id,MAX(revision) revision "
                "FROM neg_risk_group_revisions GROUP BY group_id) latest "
                "ON latest.group_id=r.group_id AND latest.revision=r.revision "
                "WHERE r.status='certified' ORDER BY r.group_id"
            ).fetchall()
            baseline_digest = self._reconciliation_baseline_digest(baseline)
            con.execute(
                "INSERT INTO neg_risk_reconciliation_windows("
                "id,status,next_cursor,started_at_ms,checkpoint_at_ms,"
                "pages_completed,events_seen,groups_staged,rejected_count,"
                "observations_count,baseline_count,baseline_digest"
                ") VALUES (?,'open',NULL,?,?,0,0,0,0,0,?,?)",
                (
                    window_id,
                    started_at_ms,
                    started_at_ms,
                    len(baseline),
                    baseline_digest,
                ),
            )
            con.executemany(
                "INSERT INTO neg_risk_reconciliation_baseline("
                "window_id,group_id,event_id,revision,membership_hash,status"
                ") VALUES (?,?,?,?,?,?)",
                [
                    (
                        window_id,
                        row["group_id"],
                        row["event_id"],
                        row["revision"],
                        row["membership_hash"],
                        row["status"],
                    )
                    for row in baseline
                ],
            )
            row = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            con.execute("COMMIT")
            return self._reconciliation_window_from_row(row)
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    @staticmethod
    def _reconciliation_window_seed(row: sqlite3.Row) -> dict[str, object]:
        return _reconciliation_window_seed(row)

    @staticmethod
    def _reconciliation_rows_digest(rows: list[sqlite3.Row]) -> str:
        return _reconciliation_rows_digest(rows)

    def _validated_reconciliation_checkpoint(
        self,
        con: sqlite3.Connection,
        window: sqlite3.Row,
        staged: list[sqlite3.Row],
    ) -> tuple[sqlite3.Row, dict[str, object]] | None:
        return validate_reconciliation_authority_checkpoint(con, window, staged)

    def _validate_reconciliation_checkpoint_snapshot(
        self,
        con: sqlite3.Connection,
        window: sqlite3.Row,
        staged: list[sqlite3.Row],
        baseline: list[sqlite3.Row],
        evidence: list[sqlite3.Row],
        result_revisions: list[sqlite3.Row],
    ) -> tuple[sqlite3.Row, dict[str, object]]:
        checkpoint = self._validated_reconciliation_checkpoint(con, window, staged)
        if checkpoint is None:
            raise ValueError("reconciliation-authority-checkpoint-required")
        suffix = con.execute(
            "SELECT * FROM neg_risk_reconciliation_batches "
            "WHERE window_id=? AND batch_sequence>? ORDER BY batch_sequence LIMIT 2",
            (window["id"], int(checkpoint[0]["through_sequence"])),
        ).fetchall()
        if suffix:
            raise ValueError("invalid-reconciliation-authority-suffix")
        if (
            len(staged) != int(window["groups_staged"])
            or len(baseline) != int(window["baseline_count"])
            or str(window["baseline_digest"])
            != self._reconciliation_baseline_digest(baseline)
        ):
            raise ValueError("invalid-reconciliation-window-or-baseline-digest")
        receipt = checkpoint[1]["receipt"]
        cumulative = checkpoint[1]["cumulative"]
        status = (
            "failed"
            if window["failure_reason"] is not None
            else str(window["status"])
        )
        if (
            int(window["pages_completed"]) != int(receipt["batch_sequence"])
            or int(cumulative["observed_count"]) != int(window["observations_count"])
            or int(cumulative["unique_count"]) != int(window["groups_staged"])
            or int(cumulative["rejected_count"]) != int(window["rejected_count"])
            or int(cumulative["page_event_count"]) != int(window["events_seen"])
            or int(cumulative["observed_count"])
            != int(cumulative["unique_count"])
            + int(cumulative["update_count"])
            + int(cumulative["duplicate_count"])
            or window["next_cursor"] != receipt["next_cursor"]
            or int(window["checkpoint_at_ms"]) != int(receipt["finished_at_ms"])
            or bool(receipt["completed"]) != (receipt["next_cursor"] is None)
            or (status in {"complete", "applied"}) != bool(receipt["completed"])
            or (
                status in {"complete", "applied"}
                and (
                    window["finished_at_ms"] is None
                    or int(window["finished_at_ms"]) != int(receipt["finished_at_ms"])
                    or any(
                        int(receipt[name]) != 0
                        for name in (
                            "page_event_count",
                            "groups_staged",
                            "observed_count",
                            "unique_count",
                            "update_count",
                            "duplicate_count",
                            "rejected_count",
                        )
                    )
                )
            )
            or (status == "open" and window["finished_at_ms"] is not None)
            or (
                status == "failed"
                and (
                    window["failure_reason"] != "cursor-loop"
                    or window["finished_at_ms"] is None
                    or int(window["finished_at_ms"])
                    < int(receipt["finished_at_ms"])
                    or bool(receipt["completed"])
                )
            )
        ):
            raise ValueError("invalid-reconciliation-checkpoint")
        if status == "applied":
            self._validate_reconciliation_diff_evidence(
                window,
                staged,
                baseline,
                evidence,
                result_revisions,
            )
        elif evidence or result_revisions:
            raise ValueError("invalid-reconciliation-premature-diff-evidence")
        return checkpoint

    def _checkpoint_reconciliation_authority(
        self,
        con: sqlite3.Connection,
        window_id: str,
        *,
        previous: tuple[sqlite3.Row, dict[str, object]] | None = None,
    ) -> None:
        window = con.execute(
            "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
            (window_id,),
        ).fetchone()
        staged = con.execute(
            "SELECT * FROM neg_risk_reconciliation_staging "
            "WHERE window_id=? ORDER BY group_id",
            (window_id,),
        ).fetchall()
        if previous is None:
            previous = self._validated_reconciliation_checkpoint(con, window, staged)
        receipts = con.execute(
            "SELECT * FROM neg_risk_reconciliation_batches "
            "WHERE window_id=? ORDER BY batch_sequence",
            (window_id,),
        ).fetchall()
        if not receipts:
            return
        samples = con.execute(
            "SELECT * FROM neg_risk_reconciliation_batch_samples "
            "WHERE batch_id IN (SELECT id FROM neg_risk_reconciliation_batches "
            "WHERE window_id=?) ORDER BY batch_id,group_id",
            (window_id,),
        ).fetchall()
        latest = receipts[-1]
        prior_seen = set() if previous is None else set(previous[1]["seen_cursors"])
        seen = prior_seen | {
            str(value)
            for receipt in receipts
            for value in (receipt["requested_cursor"], receipt["next_cursor"])
            if value is not None
        }
        prior_cumulative = (
            {
                "duplicate_count": 0,
                "observed_count": 0,
                "page_event_count": 0,
                "rejected_count": 0,
                "unique_count": 0,
                "update_count": 0,
            }
            if previous is None
            else dict(previous[1]["cumulative"])
        )
        cumulative = {
            name: int(prior_cumulative[name])
            + sum(int(receipt[name]) for receipt in receipts)
            for name in prior_cumulative
        }
        anchor = {
            "cumulative": cumulative,
            "receipt": dict(latest),
            "seen_cursors": sorted(seen),
            "staging_digest": self._reconciliation_rows_digest(staged),
            "window": self._reconciliation_window_seed(window),
        }
        anchor_json = json.dumps(
            anchor,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        anchor_digest = f"sha256:{hashlib.sha256(anchor_json.encode()).hexdigest()}"
        prefix_payload = {
            "previous_prefix_digest": (
                None if previous is None else str(previous[0]["prefix_digest"])
            ),
            "receipts": [dict(row) for row in receipts],
            "samples": [dict(row) for row in samples],
        }
        prefix_json = json.dumps(
            prefix_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        prefix_digest = f"sha256:{hashlib.sha256(prefix_json.encode()).hexdigest()}"
        generation = 1 if previous is None else int(previous[0]["generation"]) + 1
        compacted_batches = len(receipts) + (
            0 if previous is None else int(previous[0]["compacted_batch_rows"])
        )
        compacted_samples = len(samples) + (
            0 if previous is None else int(previous[0]["compacted_sample_rows"])
        )
        checkpoint_hash = reconciliation_authority_checkpoint_hash(
            window_id=window_id,
            domain=_RECONCILIATION_AUTHORITY_DOMAIN,
            version=_RECONCILIATION_AUTHORITY_VERSION,
            generation=generation,
            through_batch_id=int(latest["id"]),
            through_sequence=int(latest["batch_sequence"]),
            compacted_batch_rows=compacted_batches,
            compacted_sample_rows=compacted_samples,
            prefix_digest=prefix_digest,
            anchor_digest=anchor_digest,
        )
        con.execute(
            "INSERT INTO neg_risk_reconciliation_authority_checkpoints("
            "window_id,domain,version,generation,through_batch_id,"
            "through_sequence,compacted_batch_rows,compacted_sample_rows,"
            "prefix_digest,anchor_json,anchor_digest,checkpoint_hash"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(window_id) DO UPDATE SET domain=excluded.domain,"
            "version=excluded.version,generation=excluded.generation,"
            "through_batch_id=excluded.through_batch_id,"
            "through_sequence=excluded.through_sequence,"
            "compacted_batch_rows=excluded.compacted_batch_rows,"
            "compacted_sample_rows=excluded.compacted_sample_rows,"
            "prefix_digest=excluded.prefix_digest,anchor_json=excluded.anchor_json,"
            "anchor_digest=excluded.anchor_digest,"
            "checkpoint_hash=excluded.checkpoint_hash",
            (
                window_id,
                _RECONCILIATION_AUTHORITY_DOMAIN,
                _RECONCILIATION_AUTHORITY_VERSION,
                generation,
                int(latest["id"]),
                int(latest["batch_sequence"]),
                compacted_batches,
                compacted_samples,
                prefix_digest,
                anchor_json,
                anchor_digest,
                checkpoint_hash,
            ),
        )
        con.execute(
            "DELETE FROM neg_risk_reconciliation_batch_samples "
            "WHERE batch_id IN (SELECT id FROM neg_risk_reconciliation_batches "
            "WHERE window_id=?)",
            (window_id,),
        )
        con.execute(
            "DELETE FROM neg_risk_reconciliation_batches WHERE window_id=?",
            (window_id,),
        )
        refreshed = con.execute(
            "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
            (window_id,),
        ).fetchone()
        refreshed_staged = con.execute(
            "SELECT * FROM neg_risk_reconciliation_staging "
            "WHERE window_id=? ORDER BY group_id",
            (window_id,),
        ).fetchall()
        self._validated_reconciliation_checkpoint(con, refreshed, refreshed_staged)

    def _refresh_reconciliation_checkpoint(
        self,
        con: sqlite3.Connection,
        window: sqlite3.Row,
        staged: list[sqlite3.Row],
        *,
        checkpoint: tuple[sqlite3.Row, dict[str, object]] | None = None,
    ) -> None:
        if checkpoint is None:
            checkpoint = self._validated_reconciliation_checkpoint(con, window, staged)
        if checkpoint is None:
            return
        anchor = dict(checkpoint[1])
        anchor["window"] = self._reconciliation_window_seed(window)
        anchor["staging_digest"] = self._reconciliation_rows_digest(staged)
        anchor_json = json.dumps(
            anchor,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        anchor_digest = f"sha256:{hashlib.sha256(anchor_json.encode()).hexdigest()}"
        generation = int(checkpoint[0]["generation"]) + 1
        checkpoint_hash = reconciliation_authority_checkpoint_hash(
            window_id=str(window["id"]),
            domain=str(checkpoint[0]["domain"]),
            version=int(checkpoint[0]["version"]),
            generation=generation,
            through_batch_id=int(checkpoint[0]["through_batch_id"]),
            through_sequence=int(checkpoint[0]["through_sequence"]),
            compacted_batch_rows=int(checkpoint[0]["compacted_batch_rows"]),
            compacted_sample_rows=int(checkpoint[0]["compacted_sample_rows"]),
            prefix_digest=str(checkpoint[0]["prefix_digest"]),
            anchor_digest=anchor_digest,
        )
        con.execute(
            "UPDATE neg_risk_reconciliation_authority_checkpoints SET "
            "generation=?,anchor_json=?,anchor_digest=?,checkpoint_hash=? "
            "WHERE window_id=?",
            (
                generation,
                anchor_json,
                anchor_digest,
                checkpoint_hash,
                window["id"],
            ),
        )

    def current_reconciliation(
        self,
        *,
        _connection: sqlite3.Connection | None = None,
    ) -> ReconciliationWindow | None:
        owns_connection = _connection is None
        con = self._connect() if _connection is None else _connection
        self._install_owner_write_authorizer(con)
        try:
            if owns_connection:
                con.execute("BEGIN")
            row = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            if row is None:
                if owns_connection:
                    con.execute("COMMIT")
                return None
            staged = con.execute(
                "SELECT * FROM neg_risk_reconciliation_staging WHERE window_id=? ORDER BY group_id",
                (row["id"],),
            ).fetchall()
            baseline = con.execute(
                "SELECT * FROM neg_risk_reconciliation_baseline "
                "WHERE window_id=? ORDER BY group_id",
                (row["id"],),
            ).fetchall()
            evidence = con.execute(
                "SELECT * FROM neg_risk_reconciliation_diff_evidence "
                "WHERE window_id=? ORDER BY group_id,action",
                (row["id"],),
            ).fetchall()
            result_revisions = self._reconciliation_evidence_result_revisions(con, row["id"])
            checkpoint = con.execute(
                "SELECT 1 FROM neg_risk_reconciliation_authority_checkpoints "
                "WHERE window_id=?",
                (row["id"],),
            ).fetchone()
            if checkpoint is None:
                receipts = con.execute(
                    "SELECT * FROM neg_risk_reconciliation_batches "
                    "WHERE window_id=? ORDER BY batch_sequence",
                    (row["id"],),
                ).fetchall()
                batch_samples = con.execute(
                    "SELECT * FROM neg_risk_reconciliation_batch_samples "
                    "WHERE batch_id IN (SELECT id "
                    "FROM neg_risk_reconciliation_batches WHERE window_id=?) "
                    "ORDER BY batch_id,group_id",
                    (row["id"],),
                ).fetchall()
                self._validate_reconciliation_snapshot(
                    row,
                    receipts,
                    batch_samples,
                    staged,
                    baseline,
                    evidence,
                    result_revisions,
                    deadline_check=lambda: self._check_deadline(
                        "reconciliation-status-deadline"
                    ),
                )
            else:
                self._validate_reconciliation_checkpoint_snapshot(
                    con,
                    row,
                    staged,
                    baseline,
                    evidence,
                    result_revisions,
                )
            result = self._reconciliation_window_from_row(row)
            if owns_connection:
                con.execute("COMMIT")
            return result
        except BaseException:
            if owns_connection and con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            if owns_connection:
                con.close()

    def stage_reconciliation_group(
        self,
        window_id: str,
        revision: GroupRevision,
        *,
        quality: DiscoveryQuality,
        reason: str | None,
    ) -> None:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if row is None or row["status"] != "open":
                raise ValueError("reconciliation-window-not-open")
            staged_before = con.execute(
                "SELECT * FROM neg_risk_reconciliation_staging "
                "WHERE window_id=? ORDER BY group_id",
                (window_id,),
            ).fetchall()
            checkpoint = self._validated_reconciliation_checkpoint(
                con,
                row,
                staged_before,
            )
            if checkpoint is not None:
                raise ValueError("reconciliation-checkpointed-staging-requires-page")
            self._stage_reconciliation_sample(
                con,
                window_id=window_id,
                group_id=revision.group_id,
                event_id=revision.event_id,
                membership_hash=revision.membership_hash,
                quality=quality,
                reason=reason,
                legs=revision.legs,
                observed_at_ms=revision.observed_at_ms,
                source_cursor=revision.source_cursor,
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def publish_reconciliation_batch(
        self,
        *,
        window_id: str,
        requested_cursor: str | None,
        next_cursor: str | None,
        completed: bool,
        started_at_ms: int,
        finished_at_ms: int,
        page_event_count: int,
        candidates: tuple[DiscoveryScheduleCandidate, ...],
    ) -> ReconciliationWindow:
        """Atomically stage one page, receipt it, and advance its checkpoint."""
        if started_at_ms > finished_at_ms:
            raise ValueError("invalid-reconciliation-timestamp-order")
        if completed != (next_cursor is None):
            raise ValueError("invalid-reconciliation-completion-cursor")
        if completed and (page_event_count != 0 or candidates):
            raise ValueError("reconciliation-terminal-page-must-be-empty")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            window = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if window is None or window["status"] != "open" or window["failure_reason"] is not None:
                raise ValueError("reconciliation-window-not-open")
            self.current_reconciliation(_connection=con)
            staged_before = con.execute(
                "SELECT * FROM neg_risk_reconciliation_staging "
                "WHERE window_id=? ORDER BY group_id",
                (window_id,),
            ).fetchall()
            checkpoint_before = self._validated_reconciliation_checkpoint(
                con,
                window,
                staged_before,
            )
            if window["next_cursor"] != requested_cursor:
                raise ValueError("reconciliation-cursor-race")
            if started_at_ms < int(window["checkpoint_at_ms"]):
                raise ValueError("reconciliation-clock-regression")
            prior_receipts = con.execute(
                "SELECT requested_cursor,next_cursor "
                "FROM neg_risk_reconciliation_batches WHERE window_id=?",
                (window_id,),
            ).fetchall()
            seen_cursors = (
                set()
                if checkpoint_before is None
                else set(checkpoint_before[1]["seen_cursors"])
            ) | {
                str(value)
                for receipt in prior_receipts
                for value in (receipt["requested_cursor"], receipt["next_cursor"])
                if value is not None
            }
            if next_cursor is not None and (
                next_cursor == requested_cursor or next_cursor in seen_cursors
            ):
                con.execute(
                    "UPDATE neg_risk_reconciliation_windows SET "
                    "failure_reason='cursor-loop',"
                    "finished_at_ms=? WHERE id=?",
                    (finished_at_ms, window_id),
                )
                failed = con.execute(
                    "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
                    (window_id,),
                ).fetchone()
                self._refresh_reconciliation_checkpoint(
                    con,
                    failed,
                    staged_before,
                    checkpoint=checkpoint_before,
                )
                con.execute("COMMIT")
                return self._reconciliation_window_from_row(failed)

            existing = {
                str(row["group_id"]): row
                for row in con.execute(
                    "SELECT * FROM neg_risk_reconciliation_staging WHERE window_id=?",
                    (window_id,),
                ).fetchall()
            }
            materializations: list[str] = []
            for candidate in candidates:
                prior = existing.get(candidate.group_id)
                if prior is None:
                    materialization = "unique"
                elif self._reconciliation_candidate_matches_staging(candidate, prior):
                    materialization = "duplicate"
                else:
                    materialization = "updated"
                materializations.append(materialization)
            unique_count = materializations.count("unique")
            update_count = materializations.count("updated")
            duplicate_count = materializations.count("duplicate")
            rejected = sum(candidate.quality != "complete-supported" for candidate in candidates)
            sequence = int(window["pages_completed"]) + 1
            receipt = con.execute(
                "INSERT INTO neg_risk_reconciliation_batches("
                "window_id,batch_sequence,requested_cursor,next_cursor,completed,"
                "started_at_ms,finished_at_ms,page_event_count,groups_staged,"
                "observed_count,unique_count,update_count,duplicate_count,"
                "rejected_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    window_id,
                    sequence,
                    requested_cursor,
                    next_cursor,
                    int(completed),
                    started_at_ms,
                    finished_at_ms,
                    page_event_count,
                    len(candidates),
                    len(candidates),
                    unique_count,
                    update_count,
                    duplicate_count,
                    rejected,
                ),
            )
            batch_id = int(receipt.lastrowid)
            for candidate, materialization in zip(candidates, materializations, strict=True):
                if materialization == "unique":
                    self._stage_reconciliation_sample(
                        con,
                        window_id=window_id,
                        group_id=candidate.group_id,
                        event_id=candidate.event_id,
                        membership_hash=candidate.membership_hash,
                        quality=candidate.quality,
                        reason=candidate.reason,
                        legs=candidate.legs,
                        observed_at_ms=finished_at_ms,
                        source_cursor=requested_cursor,
                    )
                elif materialization == "updated":
                    self._update_reconciliation_staging(
                        con,
                        window_id=window_id,
                        candidate=candidate,
                        observed_at_ms=finished_at_ms,
                        source_cursor=requested_cursor,
                    )
                con.execute(
                    "INSERT INTO neg_risk_reconciliation_batch_samples("
                    "batch_id,group_id,event_id,membership_hash,quality,reason,"
                    "legs_json,observed_at_ms,source_cursor,materialization"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        batch_id,
                        candidate.group_id,
                        candidate.event_id,
                        candidate.membership_hash,
                        candidate.quality,
                        candidate.reason,
                        (None if candidate.legs is None else self._group_legs_json(candidate.legs)),
                        finished_at_ms,
                        requested_cursor,
                        materialization,
                    ),
                )
            con.execute(
                "UPDATE neg_risk_reconciliation_windows SET "
                "status=?,failure_reason=NULL,next_cursor=?,"
                "checkpoint_at_ms=?,finished_at_ms=?,"
                "pages_completed=pages_completed+1,events_seen=events_seen+?,"
                "groups_staged=groups_staged+?,"
                "observations_count=observations_count+?,"
                "rejected_count=rejected_count+? "
                "WHERE id=?",
                (
                    "complete" if completed else "open",
                    next_cursor,
                    finished_at_ms,
                    finished_at_ms if completed else None,
                    page_event_count,
                    unique_count,
                    len(candidates),
                    rejected,
                    window_id,
                ),
            )
            updated = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            self._checkpoint_reconciliation_authority(
                con,
                window_id,
                previous=checkpoint_before,
            )
            con.execute("COMMIT")
            return self._reconciliation_window_from_row(updated)
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def apply_reconciliation_diff(self, window_id: str) -> ReconciliationDiff:
        """Atomically publish one completed calibration without clobbering hot state."""
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            candidate_checkpoint = self._validated_candidate_checkpoint(con)
            window = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if window is None:
                raise ValueError("reconciliation-window-not-found")
            if window["status"] == "open" and window["failure_reason"] is None:
                raise ReconciliationIncompleteError("reconciliation-window-incomplete")
            receipts = con.execute(
                "SELECT * FROM neg_risk_reconciliation_batches "
                "WHERE window_id=? ORDER BY batch_sequence",
                (window_id,),
            ).fetchall()
            batch_samples = con.execute(
                "SELECT * FROM neg_risk_reconciliation_batch_samples "
                "WHERE batch_id IN (SELECT id "
                "FROM neg_risk_reconciliation_batches WHERE window_id=?) "
                "ORDER BY batch_id,group_id",
                (window_id,),
            ).fetchall()
            staged = con.execute(
                "SELECT * FROM neg_risk_reconciliation_staging WHERE window_id=? ORDER BY group_id",
                (window_id,),
            ).fetchall()
            baseline = con.execute(
                "SELECT * FROM neg_risk_reconciliation_baseline "
                "WHERE window_id=? ORDER BY group_id",
                (window_id,),
            ).fetchall()
            evidence = con.execute(
                "SELECT * FROM neg_risk_reconciliation_diff_evidence "
                "WHERE window_id=? ORDER BY group_id,action",
                (window_id,),
            ).fetchall()
            result_revisions = self._reconciliation_evidence_result_revisions(con, window_id)
            checkpoint_before = self._validated_reconciliation_checkpoint(
                con,
                window,
                staged,
            )
            if checkpoint_before is None:
                self._validate_reconciliation_snapshot(
                    window,
                    receipts,
                    batch_samples,
                    staged,
                    baseline,
                    evidence,
                    result_revisions,
                )
            else:
                self._validate_reconciliation_checkpoint_snapshot(
                    con,
                    window,
                    staged,
                    baseline,
                    evidence,
                    result_revisions,
                )
            if window["failure_reason"] is not None:
                raise ReconciliationIncompleteError("reconciliation-window-failed")
            finished_at_ms = int(window["finished_at_ms"])
            if window["status"] == "applied":
                result = self._reconciliation_diff_from_row(window)
                con.execute("COMMIT")
                return result

            staged_by_group = {str(row["group_id"]): row for row in staged}
            baseline_by_group = {str(row["group_id"]): row for row in baseline}
            added = changed = closed = unchanged = 0
            rejected = 0
            for row in staged:
                group_id = str(row["group_id"])
                baseline_row = baseline_by_group.get(group_id)
                if row["quality"] != "complete-supported":
                    self._insert_reconciliation_diff_evidence(
                        con,
                        window_id=window_id,
                        action="rejected",
                        group_id=group_id,
                        baseline=baseline_row,
                        staged=row,
                        result=None,
                    )
                    rejected += 1
                    continue
                legs = self._group_legs_from_json(str(row["legs_json"]))
                if GroupRevision.membership_digest(legs) != row["membership_hash"]:
                    raise ValueError("reconciliation-staging-identity-mismatch")
                current = self._current_group_row(con, group_id)
                if baseline_row is not None and row["event_id"] != baseline_row["event_id"]:
                    self._insert_reconciliation_diff_evidence(
                        con,
                        window_id=window_id,
                        action="rejected",
                        group_id=group_id,
                        baseline=baseline_row,
                        staged=row,
                        result=None,
                    )
                    rejected += 1
                    continue
                if current is not None and (
                    baseline_row is None
                    or not self._group_row_matches_reconciliation_baseline(current, baseline_row)
                ):
                    self._insert_reconciliation_diff_evidence(
                        con,
                        window_id=window_id,
                        action="unchanged",
                        group_id=group_id,
                        baseline=baseline_row,
                        staged=row,
                        result=current,
                    )
                    unchanged += 1
                    continue
                if (
                    current is not None
                    and current["status"] == "certified"
                    and current["event_id"] == row["event_id"]
                    and current["membership_hash"] == row["membership_hash"]
                ):
                    self._insert_reconciliation_diff_evidence(
                        con,
                        window_id=window_id,
                        action="unchanged",
                        group_id=group_id,
                        baseline=baseline_row,
                        staged=row,
                        result=current,
                    )
                    unchanged += 1
                    continue
                revision = GroupRevision.certified(
                    group_id=group_id,
                    event_id=str(row["event_id"]),
                    revision=1 if current is None else int(current["revision"]) + 1,
                    started_at_ms=int(row["observed_at_ms"]),
                    observed_at_ms=int(row["observed_at_ms"]),
                    source_cursor=(
                        "" if row["source_cursor"] is None else str(row["source_cursor"])
                    ),
                    legs=legs,
                )
                self._insert_group_revision(con, revision, current)
                if current is None:
                    action = "added"
                    added += 1
                else:
                    action = "changed"
                    changed += 1
                    self._sync_reconciliation_schedule(
                        con,
                        revision=revision,
                        closed=False,
                    )
                result = self._current_group_row(con, group_id)
                self._insert_reconciliation_diff_evidence(
                    con,
                    window_id=window_id,
                    action=action,
                    group_id=group_id,
                    baseline=baseline_row,
                    staged=row,
                    result=result,
                )

            for baseline_row in baseline:
                group_id = str(baseline_row["group_id"])
                observation = staged_by_group.get(group_id)
                if observation is not None and observation["event_id"] == baseline_row["event_id"]:
                    continue
                current = self._current_group_row(con, group_id)
                if current is None or not (
                    self._group_row_matches_reconciliation_baseline(current, baseline_row)
                ):
                    continue
                revision = GroupRevision(
                    group_id=group_id,
                    event_id=str(current["event_id"]),
                    revision=int(current["revision"]) + 1,
                    membership_hash=str(current["membership_hash"]),
                    started_at_ms=int(window["started_at_ms"]),
                    observed_at_ms=finished_at_ms,
                    source_cursor="reconciliation-closure",
                    status="closed",
                    legs=self._group_legs_from_json(str(current["legs_json"])),
                )
                self._insert_group_revision(con, revision, current)
                self._sync_reconciliation_schedule(
                    con,
                    revision=revision,
                    closed=True,
                )
                result = self._current_group_row(con, group_id)
                self._insert_reconciliation_diff_evidence(
                    con,
                    window_id=window_id,
                    action="closed",
                    group_id=group_id,
                    baseline=baseline_row,
                    staged=None,
                    result=result,
                )
                closed += 1
            con.execute(
                "UPDATE neg_risk_reconciliation_windows SET status='applied',"
                "added_count=?,changed_count=?,closed_count=?,unchanged_count=?,"
                "applied_rejected_count=? WHERE id=?",
                (added, changed, closed, unchanged, rejected, window_id),
            )
            applied = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            evidence = con.execute(
                "SELECT * FROM neg_risk_reconciliation_diff_evidence "
                "WHERE window_id=? ORDER BY group_id,action",
                (window_id,),
            ).fetchall()
            result_revisions = self._reconciliation_evidence_result_revisions(con, window_id)
            if candidate_checkpoint is not None:
                self._refresh_candidate_checkpoint(con, candidate_checkpoint)
                self._validated_candidate_checkpoint(con)
            if checkpoint_before is None:
                self._validate_reconciliation_snapshot(
                    applied,
                    receipts,
                    batch_samples,
                    staged,
                    baseline,
                    evidence,
                    result_revisions,
                )
            else:
                self._refresh_reconciliation_checkpoint(
                    con,
                    applied,
                    staged,
                    checkpoint=checkpoint_before,
                )
                self._validate_reconciliation_checkpoint_snapshot(
                    con,
                    applied,
                    staged,
                    baseline,
                    evidence,
                    result_revisions,
                )
            self._refresh_discovery_status_projection(con)
            result = self._reconciliation_diff_from_row(applied)
            con.execute("COMMIT")
            return result
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def publish_group_revision(self, revision: GroupRevision) -> GroupRevision:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            candidate_checkpoint = self._validated_candidate_checkpoint(con)
            if GroupRevision.membership_digest(revision.legs) != revision.membership_hash:
                raise ValueError("membership-hash-mismatch")
            if revision.status == "certified":
                validated = GroupRevision.certified(
                    group_id=revision.group_id,
                    event_id=revision.event_id,
                    revision=revision.revision,
                    started_at_ms=revision.started_at_ms,
                    observed_at_ms=revision.observed_at_ms,
                    source_cursor=revision.source_cursor,
                    legs=revision.legs,
                )
                if revision != validated:
                    raise ValueError("certified-group-invalid")
            current_row = self._current_group_row(con, revision.group_id)
            if current_row is not None and revision.revision <= current_row["revision"]:
                raise ValueError("group-revision-not-monotonic")

            self._insert_group_revision(con, revision, current_row)
            if candidate_checkpoint is not None:
                self._refresh_candidate_checkpoint(con, candidate_checkpoint)
            self._compact_candidate_authority(con)
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return revision

    def publish_quote_batch(self, batch: GroupQuoteBatch) -> GroupQuoteBatch:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            self._validated_candidate_checkpoint(con)
            self._insert_validated_quote_batch(con, batch)
            self._compact_candidate_authority(con)
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return batch

    def publish_candidate_success(
        self,
        batch: GroupQuoteBatch,
        *,
        observed_at_ms: int,
        last_result: CandidateResult,
        reason: str | None,
        bundle_cost: float,
        gross_edge_bps: float,
        max_bundle_size: float,
        priority_class: CandidatePriority,
        consecutive_failures: int,
        effective_interval_s: float,
        schedule_reason: str,
        next_due_at_ms: int,
    ) -> CandidateWatchFact:
        """Atomically publish a complete batch and its positive terminal fact."""
        if last_result not in {"watching", "no-edge"}:
            raise ValueError("candidate-success-result-required")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            self._validated_candidate_checkpoint(con)
            writer_token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_group_quote_batches",
                operation="INSERT",
                row_key=batch.group_id,
            )
            self._insert_validated_quote_batch(
                con,
                batch,
                writer_token=writer_token,
                finalize=False,
            )
            fact = self._insert_candidate_watch_fact(
                con,
                group_id=batch.group_id,
                membership_hash=batch.membership_hash,
                quote_batch_id=batch.quote_batch_id,
                observed_at_ms=observed_at_ms,
                last_result=last_result,
                reason=reason,
                bundle_cost=bundle_cost,
                gross_edge_bps=gross_edge_bps,
                max_bundle_size=max_bundle_size,
                priority_class=priority_class,
                consecutive_failures=consecutive_failures,
                effective_interval_s=effective_interval_s,
                schedule_reason=schedule_reason,
                next_due_at_ms=next_due_at_ms,
                writer_token=writer_token,
                finalize=False,
            )
            group_row = self._current_group_row(con, batch.group_id)
            quote_row = con.execute(
                "SELECT rowid FROM neg_risk_group_quote_batches WHERE id=?",
                (batch.quote_batch_id,),
            ).fetchone()
            if group_row is None or quote_row is None:
                raise ValueError("candidate-success-authority-unavailable")
            transaction_id = str(uuid.uuid4())
            receipt_hash = candidate_success_receipt_hash(
                transaction_id=transaction_id,
                group_id=batch.group_id,
                event_id=str(group_row["event_id"]),
                membership_hash=batch.membership_hash,
                quote_batch_id=batch.quote_batch_id,
                group_revision_row_id=int(group_row["id"]),
                quote_batch_row_id=int(quote_row["rowid"]),
                candidate_fact_row_id=fact.id,
                observed_at_ms=observed_at_ms,
            )
            con.execute(
                "INSERT INTO neg_risk_candidate_success_receipts("
                "transaction_id,group_id,event_id,membership_hash,quote_batch_id,"
                "group_revision_row_id,quote_batch_row_id,candidate_fact_row_id,"
                "observed_at_ms,receipt_hash"
                ") VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    transaction_id,
                    batch.group_id,
                    group_row["event_id"],
                    batch.membership_hash,
                    batch.quote_batch_id,
                    group_row["id"],
                    quote_row["rowid"],
                    fact.id,
                    observed_at_ms,
                    receipt_hash,
                ),
            )
            self._consume_expected_owner_mutation(
                con,
                writer_token=writer_token,
                table_name="neg_risk_candidate_success_receipts",
                operation="INSERT",
                row_key=batch.group_id,
            )
            self._admit_waiting_candidates(con, now_ms=observed_at_ms)
            self._compact_candidate_authority(con)
            con.execute("COMMIT")
            return fact
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def current_group(self, group_id: str) -> GroupRevision | None:
        con = self._connect()
        try:
            self._assert_owner_journal_clean(con)
            row = self._current_group_row(con, group_id)
            return None if row is None else self._validated_group_from_row(row)
        finally:
            con.close()

    def current_quote_batch(
        self,
        group_id: str,
        now_ms: int,
        max_age_ms: int,
    ) -> GroupQuoteBatch | None:
        con = self._connect()
        try:
            self._assert_owner_journal_clean(con)
            row = self._current_quote_row(con, group_id, now_ms, max_age_ms)
            if row is None:
                authority = con.execute(
                    "SELECT * FROM neg_risk_candidate_current_authority "
                    "WHERE group_id=?",
                    (group_id,),
                ).fetchone()
                if authority is None or authority["quote_batch_id"] is None:
                    return None
                canonical = json.loads(str(authority["canonical_json"]))
                quoted_at_ms = canonical["quote_quoted_at_ms"]
                started_at_ms = canonical["quote_started_at_ms"]
                if (
                    quoted_at_ms is None
                    or started_at_ms is None
                    or int(quoted_at_ms) > now_ms
                    or int(quoted_at_ms) < now_ms - max_age_ms
                ):
                    return None
                return GroupQuoteBatch.complete(
                    group_id=group_id,
                    membership_hash=str(authority["membership_hash"]),
                    quote_batch_id=str(authority["quote_batch_id"]),
                    started_at_ms=int(started_at_ms),
                    quoted_at_ms=int(quoted_at_ms),
                    legs=tuple(
                        GroupQuoteLeg(*leg)
                        for leg in json.loads(str(authority["legs_json"]))
                    ),
                )
            group = self._validated_group_from_row(row, prefix="group_")
            if group is None or group.status != "certified":
                return None
            return self._validated_quote_from_row(row, group, prefix="quote_")
        finally:
            con.close()

    def record_candidate_watch_fact(
        self,
        *,
        group_id: str,
        membership_hash: str | None,
        quote_batch_id: str | None,
        observed_at_ms: int,
        last_result: CandidateResult,
        reason: str | None,
        bundle_cost: float | None,
        gross_edge_bps: float | None,
        max_bundle_size: float | None,
        priority_class: CandidatePriority,
        consecutive_failures: int,
        effective_interval_s: float,
        schedule_reason: str,
        next_due_at_ms: int,
    ) -> CandidateWatchFact:
        """Append one and only one terminal scheduling fact for a completed run."""
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            self._validated_candidate_checkpoint(con)
            fact = self._insert_candidate_watch_fact(
                con,
                group_id=group_id,
                membership_hash=membership_hash,
                quote_batch_id=quote_batch_id,
                observed_at_ms=observed_at_ms,
                last_result=last_result,
                reason=reason,
                bundle_cost=bundle_cost,
                gross_edge_bps=gross_edge_bps,
                max_bundle_size=max_bundle_size,
                priority_class=priority_class,
                consecutive_failures=consecutive_failures,
                effective_interval_s=effective_interval_s,
                schedule_reason=schedule_reason,
                next_due_at_ms=next_due_at_ms,
            )
            self._admit_waiting_candidates(con, now_ms=observed_at_ms)
            self._compact_candidate_authority(con)
            con.execute("COMMIT")
            return fact
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def candidate_watch_facts(self, group_id: str) -> tuple[CandidateWatchFact, ...]:
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT id,group_id,membership_hash,quote_batch_id,observed_at_ms,"
                "last_result,reason,bundle_cost,gross_edge_bps,max_bundle_size,"
                "priority_class,consecutive_failures,effective_interval_s,"
                "schedule_reason,next_due_at_ms "
                "FROM neg_risk_candidate_watch_facts WHERE group_id=? ORDER BY id",
                (group_id,),
            ).fetchall()
        finally:
            con.close()
        return tuple(self._candidate_watch_fact_from_row(row) for row in rows)

    def validated_group_timeline_sources(
        self,
        group_id: str,
        *,
        limit: int,
        before: tuple[int, int, int] | None = None,
        _connection: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        """Read three authenticated Candidate timeline sources with fixed caps.

        Incident history is owned by IncidentManager and is deliberately read
        separately in the caller's same transaction.
        """
        if not group_id or not 1 <= limit <= 500:
            raise ValueError("invalid-group-timeline-request")
        if before is not None and (
            len(before) != 3
            or before[0] < 0
            or before[1] not in range(4)
            or before[2] <= 0
        ):
            raise ValueError("invalid-group-timeline-request")
        owns_connection = _connection is None
        con = self._connect() if _connection is None else _connection
        try:
            if owns_connection:
                con.execute("BEGIN")
            self._assert_owner_journal_clean(con)
            # This authenticates the rolling checkpoint, its retained seeds,
            # the complete bounded suffix, receipts, and current projection.
            self.validated_candidate_opportunity_count(_connection=con)
            checkpoint = self._validated_candidate_checkpoint(con)

            def cursor_sql(
                occurred_column: str,
                id_column: str,
                class_order: int,
            ) -> tuple[str, tuple[int, ...]]:
                if before is None:
                    return "", ()
                before_ms, before_class, before_id = before
                if class_order < before_class:
                    return f"AND {occurred_column}<? ", (before_ms,)
                if class_order > before_class:
                    return f"AND {occurred_column}<=? ", (before_ms,)
                return (
                    f"AND ({occurred_column}<? OR "
                    f"({occurred_column}=? AND {id_column}<?)) ",
                    (before_ms, before_ms, before_id),
                )

            membership_cursor, membership_parameters = cursor_sql(
                "observed_at_ms", "id", 0
            )
            membership_rows = con.execute(
                "SELECT * FROM neg_risk_group_revisions WHERE group_id=? "
                f"{membership_cursor}"
                "ORDER BY observed_at_ms DESC,id DESC LIMIT ?",
                (group_id, *membership_parameters, limit + 1),
            ).fetchall()
            memberships: list[dict[str, Any]] = []
            for row in membership_rows:
                revision = self._validated_group_from_row(row)
                if revision is None or revision.group_id != group_id:
                    raise ValueError("invalid-candidate-group-history")
                memberships.append(
                    {
                        "stable_id": int(row["id"]),
                        "occurred_at_ms": revision.observed_at_ms,
                        "group_id": revision.group_id,
                        "event_id": revision.event_id,
                        "revision": revision.revision,
                        "membership_hash": revision.membership_hash,
                        "status": revision.status,
                        "leg_count": len(revision.legs),
                        "source_cursor": revision.source_cursor,
                    }
                )

            quote_cursor, quote_parameters = cursor_sql(
                "quoted_at_ms", "rowid", 1
            )
            quote_rows = con.execute(
                "SELECT rowid,* FROM neg_risk_group_quote_batches "
                "WHERE group_id=? "
                f"{quote_cursor}"
                "ORDER BY quoted_at_ms DESC,rowid DESC LIMIT ?",
                (group_id, *quote_parameters, limit + 1),
            ).fetchall()
            quotes: list[dict[str, Any]] = []
            for row in quote_rows:
                try:
                    quote = self._quote_batch_from_row(row)
                except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                    raise ValueError("invalid-candidate-quote-history")
                if quote.group_id != group_id:
                    raise ValueError("invalid-candidate-quote-history")
                quotes.append(
                    {
                        "stable_id": int(row["rowid"]),
                        "occurred_at_ms": quote.quoted_at_ms,
                        "quote_batch_id": quote.quote_batch_id,
                        "group_revision": int(row["group_revision"]),
                        "membership_hash": quote.membership_hash,
                        "status": quote.status,
                        "failure_reason": quote.failure_reason,
                        "leg_count": len(quote.legs),
                        "duration_ms": quote.quoted_at_ms - quote.started_at_ms,
                    }
                )

            fact_floor = (
                0
                if checkpoint is None
                or int(checkpoint["compacted_fact_rows"]) == 0
                else int(checkpoint["through_fact_id"])
            )
            checkpoint_timeline_states = (
                {}
                if checkpoint is None
                else json.loads(str(checkpoint["seeds_json"])).get(
                    "timeline_states",
                    {},
                )
            )
            checkpoint_state = checkpoint_timeline_states.get(group_id)
            seed_result = (
                None
                if checkpoint_state is None
                else str(checkpoint_state["last_result"])
            )
            seed_opportunity = (
                None
                if checkpoint_state is None
                else int(bool(checkpoint_state["opportunity"]))
            )
            allow_initial = int(
                checkpoint is None
                or int(checkpoint["compacted_fact_rows"]) == 0
            )
            fact_cursor, fact_parameters = cursor_sql(
                "observed_at_ms", "id", 2
            )
            fact_rows = con.execute(
                "WITH fact_states AS ("
                "SELECT *,"
                "CASE WHEN last_result='watching' AND gross_edge_bps>0 "
                "THEN 1 ELSE 0 END AS opportunity,"
                "LAG(last_result) OVER (PARTITION BY group_id ORDER BY id) "
                "AS previous_result,"
                "LAG(CASE WHEN last_result='watching' AND gross_edge_bps>0 "
                "THEN 1 ELSE 0 END) "
                "OVER (PARTITION BY group_id ORDER BY id) AS previous_opportunity "
                "FROM neg_risk_candidate_watch_facts"
                "), resolved AS ("
                "SELECT *,"
                "CASE WHEN previous_result IS NULL THEN ? "
                "ELSE previous_result END AS effective_previous_result,"
                "CASE WHEN previous_result IS NULL THEN ? "
                "ELSE previous_opportunity END AS effective_previous_opportunity "
                "FROM fact_states WHERE group_id=?"
                ") SELECT * FROM resolved WHERE id>? "
                "AND ((effective_previous_result IS NULL AND ?=1) "
                "OR effective_previous_result!=last_result "
                "OR effective_previous_opportunity!=opportunity) "
                f"{fact_cursor}"
                "ORDER BY observed_at_ms DESC,id DESC LIMIT ?",
                (
                    seed_result,
                    seed_opportunity,
                    group_id,
                    fact_floor,
                    allow_initial,
                    *fact_parameters,
                    limit + 1,
                ),
            ).fetchall()
            opportunities = [
                {
                    "stable_id": int(row["id"]),
                    "occurred_at_ms": int(row["observed_at_ms"]),
                    "from": (
                        None
                        if row["effective_previous_result"] is None
                        else {
                            "last_result": str(row["effective_previous_result"]),
                            "opportunity": bool(
                                row["effective_previous_opportunity"]
                            ),
                        }
                    ),
                    "to": {
                        "last_result": str(row["last_result"]),
                        "opportunity": bool(row["opportunity"]),
                    },
                    "reason": row["reason"],
                    "quote_batch_id": row["quote_batch_id"],
                    "gross_edge_bps": row["gross_edge_bps"],
                }
                for row in fact_rows
            ]

            floor = {
                "membership": {
                    "scope": "global",
                    "through_id": (
                        0
                        if checkpoint is None
                        else int(checkpoint["through_group_revision_id"])
                    ),
                    "compacted_count": (
                        0
                        if checkpoint is None
                        else int(checkpoint["compacted_group_rows"])
                    ),
                },
                "quote": {
                    "scope": "global",
                    "through_id": (
                        0
                        if checkpoint is None
                        else int(checkpoint["through_quote_rowid"])
                    ),
                    "compacted_count": (
                        0
                        if checkpoint is None
                        else int(checkpoint["compacted_quote_rows"])
                    ),
                },
                "opportunity": {
                    "scope": "global",
                    "through_id": (
                        0
                        if checkpoint is None
                        else int(checkpoint["through_fact_id"])
                    ),
                    "source_rows_compacted": (
                        0
                        if checkpoint is None
                        else int(checkpoint["compacted_fact_rows"])
                    ),
                },
            }
            if owns_connection:
                con.execute("COMMIT")
            return {
                "membership": memberships,
                "quote": quotes,
                "opportunity": opportunities,
                "history_floor": floor,
            }
        except BaseException:
            if owns_connection and con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            if owns_connection:
                con.close()

    def candidate_scheduling_snapshot(
        self,
        group_ids: tuple[str, ...],
    ) -> tuple[CandidateSchedulingSnapshotItem, ...]:
        """Read all due-decision facts in one bounded SQLite snapshot."""
        if not group_ids:
            return ()
        placeholders = ",".join("?" for _ in group_ids)
        con = self._connect()
        try:
            con.execute("BEGIN")
            fact_rows = con.execute(
                "SELECT f.* FROM neg_risk_candidate_watch_facts f JOIN ("
                "SELECT group_id,MAX(id) AS id "
                "FROM neg_risk_candidate_watch_facts "
                f"WHERE group_id IN ({placeholders}) GROUP BY group_id"
                ") latest ON latest.id=f.id",
                group_ids,
            ).fetchall()
            schedule_rows = con.execute(
                f"SELECT * FROM neg_risk_group_schedule WHERE group_id IN ({placeholders})",
                group_ids,
            ).fetchall()
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        facts = {
            str(row["group_id"]): self._candidate_watch_fact_from_row(row) for row in fact_rows
        }
        schedules = {
            str(row["group_id"]): self._group_schedule_from_row(row) for row in schedule_rows
        }
        return tuple(
            CandidateSchedulingSnapshotItem(
                group_id=group_id,
                fact=facts.get(group_id),
                schedule=schedules.get(group_id),
            )
            for group_id in group_ids
        )

    def record_candidate_attempt_start(
        self,
        *,
        admission: CandidateAdmissionContext,
        clock_ms: Callable[[], int],
    ) -> CandidateWatchFact | None:
        """Atomically prove an admitted first start or persist its breach fact."""
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            started_at_ms = clock_ms()
            schedule = con.execute(
                "SELECT * FROM neg_risk_group_schedule WHERE group_id=?",
                (admission.group_id,),
            ).fetchone()
            if (
                schedule is None
                or str(schedule["event_id"]) != admission.event_id
                or str(schedule["membership_hash"]) != admission.membership_hash
                or schedule["promoted_at_ms"] != admission.promoted_at_ms
                or schedule["candidate_start_deadline_at_ms"]
                != admission.candidate_start_deadline_at_ms
            ):
                raise ValueError("candidate-attempt-start-admission-mismatch")
            current = self._current_group_row(con, admission.group_id)
            if (
                current is None
                or str(current["status"]) != "certified"
                or str(current["event_id"]) != admission.event_id
                or str(current["membership_hash"]) != admission.membership_hash
            ):
                raise ValueError("candidate-attempt-start-authority-mismatch")
            deadline = admission.candidate_start_deadline_at_ms
            proof = con.execute(
                "SELECT candidate_max_wait_ms FROM neg_risk_discovery_admission_state WHERE id=1"
            ).fetchone()
            if proof is None or deadline != admission.promoted_at_ms + int(
                proof["candidate_max_wait_ms"]
            ):
                raise ValueError("candidate-attempt-start-deadline-mismatch")
            candidate_max_wait_ms = int(proof["candidate_max_wait_ms"])
            admission_receipt = con.execute(
                "SELECT 1 FROM neg_risk_candidate_admissions "
                "WHERE group_id=? AND event_id=? AND membership_hash=? "
                "AND promoted_at_ms=? AND candidate_start_deadline_at_ms=? "
                "AND candidate_max_wait_ms=? LIMIT 1",
                (
                    admission.group_id,
                    admission.event_id,
                    admission.membership_hash,
                    admission.promoted_at_ms,
                    deadline,
                    candidate_max_wait_ms,
                ),
            ).fetchone()
            if admission_receipt is None:
                raise ValueError("candidate-attempt-start-without-admission-audit")
            breached = started_at_ms > deadline
            token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_candidate_attempt_starts",
                operation="INSERT",
                row_key=admission.group_id,
            )
            con.execute(
                "INSERT INTO neg_risk_candidate_attempt_starts("
                "group_id,event_id,membership_hash,promoted_at_ms,"
                "candidate_max_wait_ms,started_at_ms,"
                "candidate_start_deadline_at_ms,deadline_breached"
                ") VALUES (?,?,?,?,?,?,?,?)",
                (
                    admission.group_id,
                    admission.event_id,
                    admission.membership_hash,
                    admission.promoted_at_ms,
                    candidate_max_wait_ms,
                    started_at_ms,
                    deadline,
                    int(breached),
                ),
            )
            self._consume_expected_owner_mutation(
                con,
                writer_token=token,
                table_name="neg_risk_candidate_attempt_starts",
                operation="INSERT",
                row_key=admission.group_id,
                finalize=False,
            )
            fact: CandidateWatchFact | None = None
            if breached:
                fact = self._insert_candidate_watch_fact(
                    con,
                    group_id=admission.group_id,
                    membership_hash=admission.membership_hash,
                    quote_batch_id=None,
                    observed_at_ms=started_at_ms,
                    last_result="unavailable",
                    reason="candidate-start-deadline-breached",
                    bundle_cost=None,
                    gross_edge_bps=None,
                    max_bundle_size=None,
                    priority_class="normal",
                    consecutive_failures=1,
                    effective_interval_s=60.0,
                    schedule_reason="candidate-start-deadline-breached",
                    next_due_at_ms=started_at_ms + 60_000,
                    writer_token=token,
                )
                self._admit_waiting_candidates(con, now_ms=started_at_ms)
            else:
                self._refresh_discovery_status_projection(
                    con,
                    writer_token=token,
                )
            con.execute("COMMIT")
            return fact
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def latest_candidate_watch_fact(
        self,
        group_id: str,
    ) -> CandidateWatchFact | None:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT id,group_id,membership_hash,quote_batch_id,observed_at_ms,"
                "last_result,reason,bundle_cost,gross_edge_bps,max_bundle_size,"
                "priority_class,consecutive_failures,effective_interval_s,"
                "schedule_reason,next_due_at_ms "
                "FROM neg_risk_candidate_watch_facts WHERE group_id=? "
                "ORDER BY id DESC LIMIT 1",
                (group_id,),
            ).fetchone()
        finally:
            con.close()
        return None if row is None else self._candidate_watch_fact_from_row(row)

    def validate_candidate_terminal_fact(
        self,
        fact: CandidateWatchFact,
    ) -> CandidateWatchFact:
        """Re-read one exact writer result; another group attempt cannot satisfy it."""
        if not isinstance(fact, CandidateWatchFact):
            raise TypeError("candidate-terminal-fact-required")
        con = self._connect()
        try:
            self._assert_owner_journal_clean(con)
            row = con.execute(
                "SELECT id,group_id,membership_hash,quote_batch_id,observed_at_ms,"
                "last_result,reason,bundle_cost,gross_edge_bps,max_bundle_size,"
                "priority_class,consecutive_failures,effective_interval_s,"
                "schedule_reason,next_due_at_ms "
                "FROM neg_risk_candidate_watch_facts WHERE id=?",
                (fact.id,),
            ).fetchone()
        finally:
            con.close()
        if row is None:
            raise ValueError("candidate-terminal-fact-missing")
        validated = self._candidate_watch_fact_from_row(row)
        if validated != fact:
            raise ValueError("candidate-terminal-fact-mismatch")
        return validated

    @staticmethod
    def _candidate_watch_fact_from_row(
        row: sqlite3.Row,
    ) -> CandidateWatchFact:
        return CandidateWatchFact(
            id=int(row[0]),
            group_id=str(row[1]),
            membership_hash=None if row[2] is None else str(row[2]),
            quote_batch_id=None if row[3] is None else str(row[3]),
            observed_at_ms=int(row[4]),
            last_result=row[5],
            reason=None if row[6] is None else str(row[6]),
            bundle_cost=None if row[7] is None else float(row[7]),
            gross_edge_bps=None if row[8] is None else float(row[8]),
            max_bundle_size=None if row[9] is None else float(row[9]),
            priority_class=row[10],
            consecutive_failures=int(row[11]),
            effective_interval_s=float(row[12]),
            schedule_reason=str(row[13]),
            next_due_at_ms=int(row[14]),
        )

    def record_http_probe(
        self,
        *,
        release_id: str,
        started_at_ms: int,
        finished_at_ms: int,
        responsive: bool,
    ) -> None:
        raise PermissionError("authenticated-http-probe-writer-required")

    def _record_http_probe_result(self, result, *, _authority: object) -> None:
        from polyarb.perception.http_probe import _HTTP_PROBE_WRITE_AUTHORITY

        if _authority is not _HTTP_PROBE_WRITE_AUTHORITY:
            raise PermissionError("authenticated-http-probe-writer-required")
        if (
            not result.expected_release_id
            or not result.probe_nonce
            or result.started_at_ms < 0
            or result.finished_at_ms < result.started_at_ms
            or result.finished_at_ms - result.started_at_ms > 2_000
            or type(result.responsive) is not bool
            or (
                result.responsive
                and result.observed_release_id != result.expected_release_id
            )
        ):
            raise ValueError("invalid-http-probe")
        con = self._connect()
        try:
            con.execute(
                "INSERT INTO neg_risk_http_probe_receipts("
                "release_id,started_at_ms,finished_at_ms,responsive,"
                "observed_release_id,probe_nonce) VALUES(?,?,?,?,?,?)",
                (
                    result.expected_release_id,
                    result.started_at_ms,
                    result.finished_at_ms,
                    int(result.responsive),
                    result.observed_release_id,
                    result.probe_nonce,
                ),
            )
            con.commit()
        finally:
            con.close()

    def record_producer_receipt(self, receipt: ProducerReceipt) -> None:
        if (
            receipt.component not in {"candidate", "discovery", "reconciliation", "quote"}
            or receipt.attempt < 1
            or receipt.started_at_ms < 0
            or receipt.finished_at_ms < receipt.started_at_ms
            or receipt.outcome not in {"success", "nonzero", "timeout", "cancelled", "spawn-error"}
            or not _valid_producer_receipt_tail(receipt.stdout_tail)
            or not _valid_producer_receipt_tail(receipt.stderr_tail)
            or not receipt.supervisor_run_id
            or (
                receipt.child_auth_hash is not None
                and (
                    len(receipt.child_auth_hash) != 64
                    or any(char not in "0123456789abcdef" for char in receipt.child_auth_hash)
                )
            )
        ):
            raise ValueError("invalid-producer-receipt")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            start = con.execute(
                "SELECT * FROM neg_risk_producer_child_starts "
                "WHERE component=? AND attempt=?",
                (receipt.component, receipt.attempt),
            ).fetchone()
            if (
                start is None
                or start["supervisor_run_id"] != receipt.supervisor_run_id
                or start["started_at_ms"] != receipt.started_at_ms
                or start["auth_domain"] != _HEARTBEAT_AUTH_DOMAIN
                or start["child_auth_hash"] != receipt.child_auth_hash
            ):
                raise ValueError("producer-receipt-reservation-mismatch")
            con.execute(
                "INSERT INTO neg_risk_producer_receipts("
                "component,attempt,started_at_ms,finished_at_ms,outcome,"
                "exit_code,stdout_tail,stderr_tail,output_hash,supervisor_run_id,"
                "child_nonce,auth_domain,child_auth_hash"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    receipt.component,
                    receipt.attempt,
                    receipt.started_at_ms,
                    receipt.finished_at_ms,
                    receipt.outcome,
                    receipt.exit_code,
                    receipt.stdout_tail,
                    receipt.stderr_tail,
                    _producer_receipt_output_hash(
                        receipt.stdout_tail,
                        receipt.stderr_tail,
                    ),
                    receipt.supervisor_run_id,
                    "",
                    _HEARTBEAT_AUTH_DOMAIN,
                    receipt.child_auth_hash,
                ),
            )
            con.commit()
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

    def producer_receipts(self, component: str) -> tuple[ProducerReceipt, ...]:
        if component not in {"candidate", "discovery", "reconciliation", "quote"}:
            raise ValueError("invalid-producer-component")
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT * FROM neg_risk_producer_receipts WHERE component=? ORDER BY attempt",
                (component,),
            ).fetchall()
            return tuple(
                ProducerReceipt(
                    component=row["component"],
                    attempt=row["attempt"],
                    started_at_ms=row["started_at_ms"],
                    finished_at_ms=row["finished_at_ms"],
                    outcome=row["outcome"],
                    exit_code=row["exit_code"],
                    stdout_tail=row["stdout_tail"],
                    stderr_tail=row["stderr_tail"],
                    supervisor_run_id=row["supervisor_run_id"],
                    child_auth_hash=row["child_auth_hash"],
                )
                for row in rows
            )
        finally:
            con.close()

    def latest_producer_receipt(self, component: str) -> ProducerReceipt | None:
        """Read one bounded supervisor receipt for an operator projection."""
        if component not in {"candidate", "discovery", "reconciliation", "quote"}:
            raise ValueError("invalid-producer-component")
        con = self._connect()
        try:
            row = con.execute(
                "SELECT * FROM neg_risk_producer_receipts WHERE component=? "
                "ORDER BY attempt DESC LIMIT 1",
                (component,),
            ).fetchone()
            if row is None:
                return None
            return ProducerReceipt(
                component=row["component"],
                attempt=row["attempt"],
                started_at_ms=row["started_at_ms"],
                finished_at_ms=row["finished_at_ms"],
                outcome=row["outcome"],
                exit_code=row["exit_code"],
                stdout_tail=row["stdout_tail"],
                stderr_tail=row["stderr_tail"],
                supervisor_run_id=row["supervisor_run_id"],
                child_auth_hash=row["child_auth_hash"],
            )
        finally:
            con.close()

    def producer_state(self, component: str) -> str:
        receipts = self.producer_receipts(component)
        return receipts[-1].outcome if receipts else "never-started"

    def reserve_producer_attempt(
        self,
        component: str,
        *,
        supervisor_run_id: str,
        started_at_ms: int,
    ) -> int:
        if (
            component not in {"candidate", "discovery", "reconciliation", "quote"}
            or not supervisor_run_id
            or started_at_ms < 0
        ):
            raise ValueError("invalid-producer-attempt-reservation")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT MAX(attempt) FROM ("
                "SELECT attempt FROM neg_risk_producer_child_starts WHERE component=? "
                "UNION ALL SELECT attempt FROM neg_risk_producer_receipts WHERE component=?"
                ")",
                (component, component),
            ).fetchone()
            attempt = 1 if row[0] is None else int(row[0]) + 1
            con.execute(
                "INSERT INTO neg_risk_producer_child_starts("
                "component,supervisor_run_id,child_nonce,attempt,started_at_ms,"
                "auth_domain,child_auth_hash,claimed_at_ms"
                ") VALUES(?,?,?,?,?,?,NULL,NULL)",
                (
                    component,
                    supervisor_run_id,
                    "",
                    attempt,
                    started_at_ms,
                    _HEARTBEAT_AUTH_DOMAIN,
                ),
            )
            con.commit()
            return attempt
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

    @staticmethod
    def _producer_auth_hash(
        component: str,
        supervisor_run_id: str,
        attempt: int,
        preimage: str,
    ) -> str:
        material = "\x00".join(
            (
                _HEARTBEAT_AUTH_DOMAIN,
                component,
                supervisor_run_id,
                str(attempt),
                preimage,
            )
        )
        return hashlib.sha256(material.encode()).hexdigest()

    def claim_producer_heartbeat_authority(self, component: str) -> str:
        run_id = os.environ.get("POLYARB_PRODUCER_SUPERVISOR_RUN_ID", "")
        attempt_text = os.environ.get("POLYARB_PRODUCER_ATTEMPT", "")
        if (
            component not in {"candidate", "discovery", "reconciliation", "quote"}
            or not run_id
            or not attempt_text.isdigit()
        ):
            raise PermissionError("producer-heartbeat-authority-required")
        attempt = int(attempt_text)
        preimage = secrets.token_urlsafe(32)
        auth_hash = self._producer_auth_hash(component, run_id, attempt, preimage)
        claimed_at_ms = int(__import__("time").time() * 1_000)
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            changed = con.execute(
                "UPDATE neg_risk_producer_child_starts "
                "SET child_auth_hash=?,claimed_at_ms=? "
                "WHERE component=? AND supervisor_run_id=? AND attempt=? "
                "AND auth_domain=? AND child_auth_hash IS NULL",
                (
                    auth_hash,
                    claimed_at_ms,
                    component,
                    run_id,
                    attempt,
                    _HEARTBEAT_AUTH_DOMAIN,
                ),
            ).rowcount
            if changed != 1:
                raise PermissionError("producer-heartbeat-authority-claim-rejected")
            con.commit()
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()
        _CHILD_HEARTBEAT_PREIMAGES[(component, run_id, attempt)] = preimage
        return auth_hash

    def reconcile_abandoned_producer_attempts(
        self,
        component: str,
        *,
        finished_at_ms: int,
    ) -> tuple[int, ...]:
        if component not in {"candidate", "discovery", "reconciliation", "quote"}:
            raise ValueError("invalid-producer-component")
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT s.* FROM neg_risk_producer_child_starts s "
                "LEFT JOIN neg_risk_producer_receipts r "
                "ON r.component=s.component AND r.attempt=s.attempt "
                "WHERE s.component=? AND r.id IS NULL ORDER BY s.attempt",
                (component,),
            ).fetchall()
        finally:
            con.close()
        for row in rows:
            self.record_producer_receipt(
                ProducerReceipt(
                    component=component,
                    attempt=row["attempt"],
                    started_at_ms=row["started_at_ms"],
                    finished_at_ms=max(finished_at_ms, row["started_at_ms"]),
                    outcome="spawn-error",
                    exit_code=None,
                    stdout_tail="",
                    stderr_tail="abandoned-reservation:supervisor-crash",
                    supervisor_run_id=row["supervisor_run_id"],
                    child_auth_hash=row["child_auth_hash"],
                )
            )
        return tuple(int(row["attempt"]) for row in rows)

    def producer_attempt_auth_hash(
        self,
        component: str,
        supervisor_run_id: str,
        attempt: int,
    ) -> str | None:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT child_auth_hash FROM neg_risk_producer_child_starts "
                "WHERE component=? AND supervisor_run_id=? AND attempt=? "
                "AND auth_domain=?",
                (
                    component,
                    supervisor_run_id,
                    attempt,
                    _HEARTBEAT_AUTH_DOMAIN,
                ),
            ).fetchone()
            if row is None:
                raise ValueError("producer-attempt-reservation-missing")
            return row["child_auth_hash"]
        finally:
            con.close()

    def record_producer_heartbeat(
        self,
        component: str,
        *,
        observed_at_ms: int,
        state: str = "progress",
        supervisor_run_id: str | None = None,
        attempt: int | None = None,
        _preimage: str | None = None,
    ) -> int:
        run_id = supervisor_run_id or os.environ.get("POLYARB_PRODUCER_SUPERVISOR_RUN_ID", "")
        attempt_value = attempt
        if attempt_value is None:
            raw_attempt = os.environ.get("POLYARB_PRODUCER_ATTEMPT", "")
            attempt_value = int(raw_attempt) if raw_attempt.isdigit() else None
        if not run_id or attempt_value is None:
            # Unsupervised mode does not publish liveness evidence.
            return 0
        preimage = _preimage or _CHILD_HEARTBEAT_PREIMAGES.get(
            (component, run_id, attempt_value)
        )
        if (
            component not in {"candidate", "discovery", "reconciliation", "quote"}
            or observed_at_ms < 0
            or state not in {"progress", "yielded", "paused"}
            or not preimage
        ):
            raise PermissionError("producer-heartbeat-authority-required")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            start = con.execute(
                "SELECT * FROM neg_risk_producer_child_starts "
                "WHERE component=? AND supervisor_run_id=? AND attempt=?",
                (component, run_id, attempt_value),
            ).fetchone()
            computed_hash = self._producer_auth_hash(
                component, run_id, attempt_value, preimage
            )
            if (
                start is None
                or start["auth_domain"] != _HEARTBEAT_AUTH_DOMAIN
                or not isinstance(start["child_auth_hash"], str)
                or not hmac.compare_digest(start["child_auth_hash"], computed_hash)
            ):
                raise PermissionError("producer-heartbeat-authority-rejected")
            row = con.execute(
                "SELECT MAX(sequence) FROM neg_risk_producer_heartbeats "
                "WHERE component=? AND supervisor_run_id=? AND attempt=?",
                (component, run_id, attempt_value),
            ).fetchone()
            sequence = 1 if row[0] is None else int(row[0]) + 1
            con.execute(
                "INSERT INTO neg_risk_producer_heartbeats("
                "component,supervisor_run_id,child_nonce,attempt,auth_domain,"
                "child_auth_hash,sequence,observed_at_ms,state"
                ") VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    component,
                    run_id,
                    "",
                    attempt_value,
                    _HEARTBEAT_AUTH_DOMAIN,
                    computed_hash,
                    sequence,
                    observed_at_ms,
                    state,
                ),
            )
            con.commit()
            return sequence
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

    def latest_producer_heartbeat_ms(self, component: str) -> int | None:
        if component not in {"candidate", "discovery", "reconciliation", "quote"}:
            raise ValueError("invalid-producer-component")
        con = self._connect()
        try:
            row = con.execute(
                "SELECT observed_at_ms FROM neg_risk_producer_heartbeats "
                "WHERE component=? ORDER BY id DESC LIMIT 1",
                (component,),
            ).fetchone()
            return None if row is None else int(row["observed_at_ms"])
        finally:
            con.close()

    def latest_resource_decision(
        self,
        *,
        now_ms: int | None = None,
        required: bool = False,
    ) -> dict | None:
        # `required=False` is the disabled capability boundary: callers do not
        # parse, validate, or consume any resource-controller history.
        if not required:
            return None
        con = self._connect()
        try:
            from polyarb.perception.resource_controller import (
                validate_resource_evidence_failure,
                validate_resource_history,
            )

            self._assert_owner_journal_clean(con)
            validate_resource_evidence_failure(con, require_resolved=True)
            decision = validate_resource_history(con)
            if decision is None:
                if required:
                    raise ValueError("resource-decision-required")
                return None
            if now_ms is not None and (
                type(now_ms) is not int
                or now_ms < decision.decided_at_ms
                or now_ms > decision.valid_until_ms
            ):
                raise ValueError("stale-resource-decision")
            return asdict(decision)
        finally:
            con.close()

    def latest_resource_decision_id(self) -> int | None:
        con = self._connect()
        try:
            from polyarb.perception.resource_controller import (
                validate_resource_evidence_failure,
                validate_resource_history,
            )

            self._assert_owner_journal_clean(con)
            validate_resource_evidence_failure(con, require_resolved=True)
            decision = validate_resource_history(con)
            if decision is None:
                return None
            row = con.execute(
                "SELECT id FROM neg_risk_resource_decisions WHERE sequence=?",
                (decision.sequence,),
            ).fetchone()
            return None if row is None else int(row["id"])
        finally:
            con.close()

    def open_incidents(self):
        from polyarb.perception.incidents import IncidentManager

        return IncidentManager(self).open_incidents()

    def group_incident_history(
        self,
        group_id: str,
        *,
        limit: int,
        before_event_id: int | None = None,
    ):
        from polyarb.perception.incidents import IncidentManager

        return IncidentManager(self).group_incident_history(
            group_id,
            limit=limit,
            before_event_id=before_event_id,
        )

    @staticmethod
    def _validated_operator_auth(
        con: sqlite3.Connection,
        *,
        deadline_monotonic: float | None = None,
    ) -> dict[str, sqlite3.Row]:
        rows = con.execute(
            "SELECT * FROM neg_risk_operator_auth_nonces "
            "ORDER BY accepted_at_ms,nonce LIMIT ?",
            (_OPERATOR_AUTH_HISTORY_MAX_ROWS + 1,),
        ).fetchall()
        if len(rows) > _OPERATOR_AUTH_HISTORY_MAX_ROWS:
            raise ValueError("operator-auth-history-capacity-exceeded")
        result: dict[str, sqlite3.Row] = {}
        for row in rows:
            if (
                deadline_monotonic is not None
                and time.monotonic() >= deadline_monotonic
            ):
                raise TimeoutError("operator-auth-deadline")
            nonce = row["nonce"]
            if (
                not isinstance(nonce, str)
                or not nonce
                or nonce in result
                or row["request_method"] != "POST"
                or not isinstance(row["request_path"], str)
                or not str(row["request_path"]).startswith("/control/perception/")
                or type(row["request_timestamp_s"]) is not int
                or type(row["accepted_at_ms"]) is not int
                or abs(
                    int(row["accepted_at_ms"])
                    - int(row["request_timestamp_s"]) * 1_000
                )
                > 300_999
                or not isinstance(row["body_hash"], str)
                or len(row["body_hash"]) != 64
            ):
                raise ValueError("invalid-operator-auth-history")
            expected = operator_auth_receipt_hash(
                nonce=nonce,
                request_method=row["request_method"],
                request_path=row["request_path"],
                request_timestamp_s=row["request_timestamp_s"],
                body_hash=row["body_hash"],
                accepted_at_ms=row["accepted_at_ms"],
            )
            if not hmac.compare_digest(str(row["auth_hash"]), expected):
                raise ValueError("invalid-operator-auth-history")
            result[nonce] = row
        return result

    @classmethod
    def _validated_operator_checkpoint(
        cls,
        con: sqlite3.Connection,
        component: Literal["discovery", "reconciliation"],
    ) -> dict[str, object]:
        row = con.execute(
            "SELECT * FROM neg_risk_operator_queue_checkpoints WHERE component=?",
            (component,),
        ).fetchone()
        if row is None:
            return {
                "through_sequence": 0,
                "through_receipt_hash": None,
                "last_occurred_at_ms": None,
                "queued": False,
                "queued_at_ms": None,
                "consumed_at_ms": None,
                "request_nonce": None,
                "request_auth_hash": None,
            }
        expected_hash = operator_queue_checkpoint_hash(
            component=component,
            through_sequence=int(row["through_sequence"]),
            through_receipt_hash=str(row["through_receipt_hash"]),
            last_occurred_at_ms=int(row["last_occurred_at_ms"]),
            queued=bool(row["queued"]),
            queued_at_ms=row["queued_at_ms"],
            consumed_at_ms=row["consumed_at_ms"],
            request_nonce=row["request_nonce"],
            request_auth_hash=row["request_auth_hash"],
        )
        if (
            row["domain"] != "polyarb-operator-queue-checkpoint"
            or row["version"] != 1
            or int(row["through_sequence"]) < 1
            or len(str(row["through_receipt_hash"])) != 64
            or (
                row["request_nonce"] is None
                or row["request_auth_hash"] is None
            )
            or len(str(row["request_auth_hash"])) != 64
            or not hmac.compare_digest(str(row["checkpoint_hash"]), expected_hash)
        ):
            raise ValueError("invalid-operator-queue-checkpoint")
        return {
            "through_sequence": int(row["through_sequence"]),
            "through_receipt_hash": str(row["through_receipt_hash"]),
            "last_occurred_at_ms": int(row["last_occurred_at_ms"]),
            "queued": bool(row["queued"]),
            "queued_at_ms": row["queued_at_ms"],
            "consumed_at_ms": row["consumed_at_ms"],
            "request_nonce": str(row["request_nonce"]),
            "request_auth_hash": str(row["request_auth_hash"]),
        }

    @classmethod
    def _validated_operator_queue(
        cls,
        con: sqlite3.Connection,
        component: Literal["discovery", "reconciliation"],
        *,
        deadline_monotonic: float | None = None,
    ) -> tuple[bool, str | None, str | None, int, str | None]:
        checkpoint = cls._validated_operator_checkpoint(con, component)
        rows = con.execute(
            "SELECT * FROM neg_risk_operator_queue_receipts "
            "WHERE component=? ORDER BY sequence LIMIT ?",
            (component, _OPERATOR_QUEUE_UNCOMPACTED_MAX_ROWS + 1),
        ).fetchall()
        if len(rows) > _OPERATOR_QUEUE_UNCOMPACTED_MAX_ROWS:
            raise ValueError("operator-queue-history-capacity-exceeded")
        queued = bool(checkpoint["queued"])
        queued_nonce = checkpoint["request_nonce"]
        queued_auth_hash = checkpoint["request_auth_hash"]
        queued_at_ms = checkpoint["queued_at_ms"]
        consumed_at_ms = checkpoint["consumed_at_ms"]
        previous_hash = checkpoint["through_receipt_hash"]
        previous_occurred_at_ms = checkpoint["last_occurred_at_ms"]
        sequence_base = int(checkpoint["through_sequence"])
        for expected_sequence, row in enumerate(rows, sequence_base + 1):
            if (
                deadline_monotonic is not None
                and time.monotonic() >= deadline_monotonic
            ):
                raise TimeoutError("operator-queue-deadline")
            action = str(row["action"])
            nonce = str(row["auth_nonce"])
            auth_receipt_hash = str(row["auth_receipt_hash"])
            expected_hash = operator_queue_receipt_hash(
                component=component,
                sequence=expected_sequence,
                action=action,
                occurred_at_ms=row["occurred_at_ms"],
                auth_nonce=nonce,
                previous_hash=previous_hash,
                auth_receipt_hash=auth_receipt_hash,
            )
            if (
                row["sequence"] != expected_sequence
                or len(auth_receipt_hash) != 64
                or row["previous_hash"] != previous_hash
                or not hmac.compare_digest(str(row["receipt_hash"]), expected_hash)
                or type(row["occurred_at_ms"]) is not int
                or (
                    previous_occurred_at_ms is not None
                    and row["occurred_at_ms"] < previous_occurred_at_ms
                )
                or (action == "queued" and queued)
                or (action == "coalesced" and not queued)
                or (action in {"consumed", "cancelled"} and (not queued or nonce != queued_nonce))
                or action not in {"queued", "coalesced", "consumed", "cancelled"}
            ):
                raise ValueError("invalid-operator-queue-history")
            if action == "queued":
                queued = True
                queued_nonce = nonce
                queued_auth_hash = auth_receipt_hash
                queued_at_ms = row["occurred_at_ms"]
                consumed_at_ms = None
            elif action in {"consumed", "cancelled"}:
                queued = False
                consumed_at_ms = row["occurred_at_ms"]
            previous_hash = str(row["receipt_hash"])
            previous_occurred_at_ms = int(row["occurred_at_ms"])
        materialized = con.execute(
            "SELECT * FROM neg_risk_operator_queue WHERE component=?",
            (component,),
        ).fetchone()
        if materialized is None:
            if rows:
                raise ValueError("invalid-operator-queue-materialization")
        elif (
            bool(materialized["queued"]) != queued
            or materialized["request_nonce"] != queued_nonce
            or materialized["request_auth_hash"] != queued_auth_hash
            or materialized["queued_at_ms"] != queued_at_ms
            or materialized["consumed_at_ms"] != consumed_at_ms
            or materialized["last_sequence"] != sequence_base + len(rows)
            or materialized["last_receipt_hash"] != previous_hash
        ):
            raise ValueError("invalid-operator-queue-materialization")
        return (
            queued,
            queued_nonce,
            queued_auth_hash,
            sequence_base + len(rows),
            previous_hash,
        )

    @classmethod
    def _compact_operator_queue(
        cls,
        con: sqlite3.Connection,
        component: Literal["discovery", "reconciliation"],
        *,
        deadline_monotonic: float,
    ) -> tuple[bool, str | None, str | None, int, str | None]:
        validated = cls._validated_operator_queue(
            con,
            component,
            deadline_monotonic=deadline_monotonic,
        )
        suffix_count = int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_operator_queue_receipts "
                "WHERE component=?",
                (component,),
            ).fetchone()[0]
        )
        if suffix_count <= _OPERATOR_QUEUE_COMPACT_HIGH_ROWS:
            return validated
        checkpoint = cls._validated_operator_checkpoint(con, component)
        compact_count = suffix_count - _OPERATOR_QUEUE_COMPACT_LOW_ROWS
        prefix = con.execute(
            "SELECT * FROM neg_risk_operator_queue_receipts "
            "WHERE component=? ORDER BY sequence LIMIT ?",
            (component, compact_count),
        ).fetchall()
        queued = bool(checkpoint["queued"])
        queued_nonce = checkpoint["request_nonce"]
        queued_auth_hash = checkpoint["request_auth_hash"]
        queued_at_ms = checkpoint["queued_at_ms"]
        consumed_at_ms = checkpoint["consumed_at_ms"]
        through_sequence = int(checkpoint["through_sequence"])
        through_hash = checkpoint["through_receipt_hash"]
        last_occurred_at_ms = checkpoint["last_occurred_at_ms"]
        for row in prefix:
            if time.monotonic() >= deadline_monotonic:
                raise TimeoutError("operator-queue-deadline")
            action = str(row["action"])
            if action == "queued":
                queued = True
                queued_nonce = str(row["auth_nonce"])
                queued_auth_hash = str(row["auth_receipt_hash"])
                queued_at_ms = int(row["occurred_at_ms"])
                consumed_at_ms = None
            elif action in {"consumed", "cancelled"}:
                queued = False
                consumed_at_ms = int(row["occurred_at_ms"])
            through_sequence = int(row["sequence"])
            through_hash = str(row["receipt_hash"])
            last_occurred_at_ms = int(row["occurred_at_ms"])
        if (
            through_sequence < 1
            or through_hash is None
            or last_occurred_at_ms is None
            or queued_nonce is None
            or queued_auth_hash is None
        ):
            raise ValueError("invalid-operator-queue-checkpoint")
        checkpoint_hash = operator_queue_checkpoint_hash(
            component=component,
            through_sequence=through_sequence,
            through_receipt_hash=through_hash,
            last_occurred_at_ms=last_occurred_at_ms,
            queued=queued,
            queued_at_ms=queued_at_ms,
            consumed_at_ms=consumed_at_ms,
            request_nonce=queued_nonce,
            request_auth_hash=queued_auth_hash,
        )
        con.execute(
            "INSERT INTO neg_risk_operator_queue_checkpoints("
            "component,domain,version,through_sequence,through_receipt_hash,"
            "last_occurred_at_ms,queued,queued_at_ms,consumed_at_ms,"
            "request_nonce,request_auth_hash,checkpoint_hash"
            ") VALUES(?, 'polyarb-operator-queue-checkpoint',1,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(component) DO UPDATE SET "
            "domain=excluded.domain,version=excluded.version,"
            "through_sequence=excluded.through_sequence,"
            "through_receipt_hash=excluded.through_receipt_hash,"
            "last_occurred_at_ms=excluded.last_occurred_at_ms,"
            "queued=excluded.queued,queued_at_ms=excluded.queued_at_ms,"
            "consumed_at_ms=excluded.consumed_at_ms,"
            "request_nonce=excluded.request_nonce,"
            "request_auth_hash=excluded.request_auth_hash,"
            "checkpoint_hash=excluded.checkpoint_hash",
            (
                component,
                through_sequence,
                through_hash,
                last_occurred_at_ms,
                int(queued),
                queued_at_ms,
                consumed_at_ms,
                queued_nonce,
                queued_auth_hash,
                checkpoint_hash,
            ),
        )
        con.execute(
            "DELETE FROM neg_risk_operator_queue_receipts "
            "WHERE component=? AND sequence<=?",
            (component, through_sequence),
        )
        return cls._validated_operator_queue(
            con,
            component,
            deadline_monotonic=deadline_monotonic,
        )

    def accept_operator_auth(
        self,
        *,
        nonce: str,
        request_method: str,
        request_path: str,
        request_timestamp_s: int,
        body_hash: str,
        accepted_at_ms: int,
        deadline_monotonic: float,
    ) -> None:
        con = self._connect(deadline_monotonic=deadline_monotonic)
        try:
            if time.monotonic() >= deadline_monotonic:
                raise TimeoutError("operator-auth-deadline")
            con.execute("BEGIN IMMEDIATE")
            # Request timestamps older than this boundary are rejected by the
            # HMAC middleware before SQLite. Queue receipts retain the accepted
            # auth proof, so expired active nonce rows can be pruned before the
            # replay-window capacity check without weakening queue history.
            con.execute(
                "DELETE FROM neg_risk_operator_auth_nonces "
                "WHERE accepted_at_ms<?",
                (accepted_at_ms - 300_999,),
            )
            self._validated_operator_auth(
                con,
                deadline_monotonic=deadline_monotonic,
            )
            for component in ("discovery", "reconciliation"):
                self._compact_operator_queue(
                    con,
                    component,
                    deadline_monotonic=deadline_monotonic,
                )
            auth_hash = operator_auth_receipt_hash(
                nonce=nonce,
                request_method=request_method,
                request_path=request_path,
                request_timestamp_s=request_timestamp_s,
                body_hash=body_hash,
                accepted_at_ms=accepted_at_ms,
            )
            con.execute(
                "INSERT INTO neg_risk_operator_auth_nonces("
                "nonce,request_method,request_path,request_timestamp_s,"
                "body_hash,accepted_at_ms,auth_hash) VALUES(?,?,?,?,?,?,?)",
                (
                    nonce,
                    request_method,
                    request_path,
                    request_timestamp_s,
                    body_hash,
                    accepted_at_ms,
                    auth_hash,
                ),
            )
            if time.monotonic() >= deadline_monotonic:
                raise TimeoutError("operator-auth-deadline")
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _validate_component_control_permission(
        self,
        con: sqlite3.Connection,
        component: Literal["discovery", "reconciliation"],
        *,
        now_ms: int,
        require_resource_decision: bool = False,
    ) -> None:
        from polyarb.perception.incidents import IncidentManager
        from polyarb.perception.resource_controller import validate_resource_history

        manager = IncidentManager(self)
        if any(
            incident.scope == component
            for incident in manager.open_incidents(_connection=con)
        ):
            raise RuntimeError("component-incident-active")
        decision = validate_resource_history(con)
        producer = validate_producer_history(con, component, now_ms=now_ms)
        heartbeat = con.execute(
            "SELECT state FROM neg_risk_producer_heartbeats "
            "WHERE component=? ORDER BY attempt DESC,sequence DESC LIMIT 1",
            (component,),
        ).fetchone()
        if (
            (
                component == "reconciliation"
                and decision is not None
                and not decision.reconciliation_enabled
            )
            or (
                require_resource_decision
                and (
                    decision is None
                    or decision.valid_until_ms < now_ms
                )
            )
            or producer.state
            not in {"never-started", "starting", "running"}
            or (heartbeat is not None and heartbeat["state"] == "paused")
        ):
            raise RuntimeError("component-paused")

    def queue_operator_wakeup(
        self,
        component: Literal["discovery", "reconciliation"],
        *,
        request_nonce: str,
        occurred_at_ms: int,
        deadline_monotonic: float | None = None,
        _before_commit: Callable[[], None] | None = None,
        require_resource_decision: bool = False,
    ) -> bool:
        """Coalesce one authenticated wake-up without invoking a producer."""
        if (
            component not in {"discovery", "reconciliation"}
            or not request_nonce
            or occurred_at_ms < 0
        ):
            raise ValueError("invalid-operator-wakeup")
        deadline = (
            time.monotonic() + 0.8
            if deadline_monotonic is None
            else deadline_monotonic
        )

        def check_deadline() -> None:
            if time.monotonic() >= deadline:
                raise TimeoutError("operator-wakeup-deadline")

        check_deadline()
        con = self._connect(deadline_monotonic=deadline)
        try:
            check_deadline()
            con.execute("BEGIN IMMEDIATE")
            check_deadline()
            (
                queued_before,
                queued_nonce,
                queued_auth_hash,
                sequence_before,
                previous_hash,
            ) = (
                self._compact_operator_queue(
                    con,
                    component,
                    deadline_monotonic=deadline,
                )
            )
            auth = self._validated_operator_auth(
                con,
                deadline_monotonic=deadline,
            )
            auth_row = auth.get(request_nonce)
            if (
                auth_row is None
                or auth_row["request_path"] != f"/control/perception/{component}"
            ):
                raise ValueError("invalid-operator-request-authority")
            prior_receipt = con.execute(
                "SELECT occurred_at_ms FROM neg_risk_operator_queue_receipts "
                "WHERE component=? ORDER BY sequence DESC LIMIT 1",
                (component,),
            ).fetchone()
            write_at_ms = max(
                occurred_at_ms,
                int(time.time() * 1_000),
                int(auth_row["accepted_at_ms"]),
                (
                    0
                    if prior_receipt is None
                    else int(prior_receipt["occurred_at_ms"])
                ),
            )
            self._validate_component_control_permission(
                con,
                component,
                now_ms=write_at_ms,
                require_resource_decision=require_resource_decision,
            )
            check_deadline()
            queued = not queued_before
            action = "queued" if queued else "coalesced"
            auth_receipt_hash = str(auth_row["auth_hash"])
            sequence = sequence_before + 1
            receipt_hash = operator_queue_receipt_hash(
                component=component,
                sequence=sequence,
                action=action,
                occurred_at_ms=write_at_ms,
                auth_nonce=request_nonce,
                previous_hash=previous_hash,
                auth_receipt_hash=auth_receipt_hash,
            )
            if queued:
                con.execute(
                    "INSERT INTO neg_risk_operator_queue("
                    "component,queued,queued_at_ms,consumed_at_ms,request_nonce,"
                    "request_auth_hash,last_sequence,last_receipt_hash"
                    ") VALUES(?,1,?,NULL,?,?,?,?) "
                    "ON CONFLICT(component) DO UPDATE SET "
                    "queued=1,queued_at_ms=excluded.queued_at_ms,"
                    "consumed_at_ms=NULL,request_nonce=excluded.request_nonce,"
                    "request_auth_hash=excluded.request_auth_hash,"
                    "last_sequence=excluded.last_sequence,"
                    "last_receipt_hash=excluded.last_receipt_hash",
                    (
                        component,
                        write_at_ms,
                        request_nonce,
                        auth_receipt_hash,
                        sequence,
                        receipt_hash,
                    ),
                )
            else:
                if queued_nonce is None:
                    raise ValueError("invalid-operator-queue-materialization")
                if queued_auth_hash is None:
                    raise ValueError("invalid-operator-queue-materialization")
                con.execute(
                    "UPDATE neg_risk_operator_queue SET last_sequence=?,"
                    "last_receipt_hash=? WHERE component=?",
                    (sequence, receipt_hash, component),
                )
            check_deadline()
            con.execute(
                "INSERT INTO neg_risk_operator_queue_receipts("
                "component,sequence,action,occurred_at_ms,auth_nonce,"
                "auth_receipt_hash,previous_hash,receipt_hash) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    component,
                    sequence,
                    action,
                    write_at_ms,
                    request_nonce,
                    auth_receipt_hash,
                    previous_hash,
                    receipt_hash,
                ),
            )
            if _before_commit is not None:
                _before_commit()
            check_deadline()
            con.execute("COMMIT")
            return queued
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def consume_operator_wakeup(
        self,
        component: Literal["discovery", "reconciliation"],
        *,
        occurred_at_ms: int,
        expected_nonce: str | None = None,
        require_resource_decision: bool = False,
    ) -> bool:
        """Atomically claim a queued hint; serial producer loops call this."""
        if component not in {"discovery", "reconciliation"} or occurred_at_ms < 0:
            raise ValueError("invalid-operator-wakeup")
        deadline = time.monotonic() + 0.8
        con = self._connect(deadline_monotonic=deadline)
        try:
            con.execute("BEGIN IMMEDIATE")
            (
                queued,
                queued_nonce,
                queued_auth_hash,
                sequence_before,
                previous_hash,
            ) = (
                self._compact_operator_queue(
                    con,
                    component,
                    deadline_monotonic=deadline,
                )
            )
            if not queued or queued_nonce is None or queued_auth_hash is None:
                con.execute("COMMIT")
                return False
            if expected_nonce is not None and queued_nonce != expected_nonce:
                con.execute("COMMIT")
                return False
            prior_receipt = con.execute(
                "SELECT occurred_at_ms FROM neg_risk_operator_queue_receipts "
                "WHERE component=? ORDER BY sequence DESC LIMIT 1",
                (component,),
            ).fetchone()
            write_at_ms = max(
                occurred_at_ms,
                int(time.time() * 1_000),
                (
                    0
                    if prior_receipt is None
                    else int(prior_receipt["occurred_at_ms"])
                ),
            )
            try:
                self._validate_component_control_permission(
                    con,
                    component,
                    now_ms=write_at_ms,
                    require_resource_decision=require_resource_decision,
                )
            except RuntimeError:
                con.execute("COMMIT")
                return False
            sequence = sequence_before + 1
            receipt_hash = operator_queue_receipt_hash(
                component=component,
                sequence=sequence,
                action="consumed",
                occurred_at_ms=write_at_ms,
                auth_nonce=queued_nonce,
                previous_hash=previous_hash,
                auth_receipt_hash=queued_auth_hash,
            )
            con.execute(
                "UPDATE neg_risk_operator_queue SET queued=0,consumed_at_ms=?,"
                "last_sequence=?,last_receipt_hash=? "
                "WHERE component=? AND queued=1",
                (write_at_ms, sequence, receipt_hash, component),
            )
            con.execute(
                "INSERT INTO neg_risk_operator_queue_receipts("
                "component,sequence,action,occurred_at_ms,auth_nonce,"
                "auth_receipt_hash,previous_hash,receipt_hash) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    component,
                    sequence,
                    "consumed",
                    write_at_ms,
                    queued_nonce,
                    queued_auth_hash,
                    previous_hash,
                    receipt_hash,
                ),
            )
            con.execute("COMMIT")
            return True
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def pending_operator_wakeup(
        self,
        component: Literal["discovery", "reconciliation"],
        *,
        now_ms: int,
        require_resource_decision: bool = False,
    ) -> str | None:
        """Return the exact runnable queued nonce without consuming it."""
        if component not in {"discovery", "reconciliation"} or now_ms < 0:
            raise ValueError("invalid-operator-wakeup")
        deadline = time.monotonic() + 0.8
        con = self._connect(deadline_monotonic=deadline)
        try:
            con.execute("BEGIN IMMEDIATE")
            queued, nonce, _, _, _ = self._compact_operator_queue(
                con,
                component,
                deadline_monotonic=deadline,
            )
            if not queued or nonce is None:
                con.execute("COMMIT")
                return None
            try:
                self._validate_component_control_permission(
                    con,
                    component,
                    now_ms=now_ms,
                    require_resource_decision=require_resource_decision,
                )
            except RuntimeError:
                con.execute("COMMIT")
                return None
            con.execute("COMMIT")
            return nonce
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _connect(
        self,
        *,
        deadline_monotonic: float | None = None,
    ) -> sqlite3.Connection:
        target = (
            f"file:{self._db_path.resolve()}?mode=ro" if self._read_only else str(self._db_path)
        )
        con = sqlite3.connect(
            target,
            uri=self._read_only,
            isolation_level=None,
            timeout=self._busy_timeout_ms / 1_000,
        )
        con.row_factory = sqlite3.Row
        self._install_owner_write_authorizer(con)
        con.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        deadline = (
            deadline_monotonic
            if deadline_monotonic is not None
            else self._deadline_monotonic
        )
        if deadline is None and self._busy_timeout_ms < _BUSY_TIMEOUT_MS:
            deadline = time.monotonic() + 0.8
        if deadline is not None:
            con.set_progress_handler(
                lambda: 1 if time.monotonic() >= deadline else 0,
                1_000,
            )
        return con

    def _insert_validated_quote_batch(
        self,
        con: sqlite3.Connection,
        batch: GroupQuoteBatch,
        *,
        writer_token: str | None = None,
        finalize: bool = True,
    ) -> None:
        validated = GroupQuoteBatch.complete(
            group_id=batch.group_id,
            membership_hash=batch.membership_hash,
            quote_batch_id=batch.quote_batch_id,
            started_at_ms=batch.started_at_ms,
            quoted_at_ms=batch.quoted_at_ms,
            legs=batch.legs,
        )
        if batch != validated:
            raise ValueError("quote-batch-not-complete")
        current_row = self._current_group_row(con, batch.group_id)
        if current_row is None:
            membership_owner = con.execute(
                "SELECT group_id FROM neg_risk_group_revisions "
                "WHERE membership_hash=? AND status='certified' "
                "ORDER BY revision DESC LIMIT 1",
                (batch.membership_hash,),
            ).fetchone()
            if membership_owner is not None:
                raise ValueError("group-identity-mismatch")
            raise ValueError("certified-group-not-found")
        if current_row["status"] != "certified":
            raise ValueError("certified-group-not-found")
        current = self._validated_group_from_row(current_row)
        if current is None:
            raise ValueError("certified-group-invalid")
        if batch.group_id != current.group_id:
            raise ValueError("group-identity-mismatch")
        if batch.membership_hash != current.membership_hash:
            raise ValueError("membership-hash-mismatch")
        if tuple(leg.yes_token_id for leg in batch.legs) != tuple(
            leg.yes_token_id for leg in current.legs
        ):
            raise ValueError("quote-leg-identity-mismatch")
        if any(leg.membership_hash != current.membership_hash for leg in batch.legs):
            raise ValueError("membership-hash-mismatch")
        if writer_token is None:
            writer_token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_group_quote_batches",
                operation="INSERT",
                row_key=batch.group_id,
            )
        con.execute(
            "INSERT INTO neg_risk_group_quote_batches("
            "id,group_id,group_revision,membership_hash,started_at_ms,"
            "quoted_at_ms,status,failure_reason,legs_json"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            (
                batch.quote_batch_id,
                batch.group_id,
                current.revision,
                batch.membership_hash,
                batch.started_at_ms,
                batch.quoted_at_ms,
                batch.status,
                batch.failure_reason,
                self._quote_legs_json(batch.legs),
            ),
        )
        self._consume_expected_owner_mutation(
            con,
            writer_token=writer_token,
            table_name="neg_risk_group_quote_batches",
            operation="INSERT",
            row_key=batch.group_id,
            finalize=finalize,
        )

    def _insert_candidate_watch_fact(
        self,
        con: sqlite3.Connection,
        *,
        group_id: str,
        membership_hash: str | None,
        quote_batch_id: str | None,
        observed_at_ms: int,
        last_result: CandidateResult,
        reason: str | None,
        bundle_cost: float | None,
        gross_edge_bps: float | None,
        max_bundle_size: float | None,
        priority_class: CandidatePriority,
        consecutive_failures: int,
        effective_interval_s: float,
        schedule_reason: str,
        next_due_at_ms: int,
        writer_token: str | None = None,
        finalize: bool = True,
    ) -> CandidateWatchFact:
        if writer_token is None:
            writer_token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_candidate_watch_facts",
                operation="INSERT",
                row_key=group_id,
            )
        cursor = con.execute(
            "INSERT INTO neg_risk_candidate_watch_facts("
            "group_id,membership_hash,quote_batch_id,observed_at_ms,last_result,"
            "reason,bundle_cost,gross_edge_bps,max_bundle_size,priority_class,"
            "consecutive_failures,effective_interval_s,schedule_reason,next_due_at_ms"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                group_id,
                membership_hash,
                quote_batch_id,
                observed_at_ms,
                last_result,
                reason,
                bundle_cost,
                gross_edge_bps,
                max_bundle_size,
                priority_class,
                consecutive_failures,
                effective_interval_s,
                schedule_reason,
                next_due_at_ms,
            ),
        )
        self._consume_expected_owner_mutation(
            con,
            writer_token=writer_token,
            table_name="neg_risk_candidate_watch_facts",
            operation="INSERT",
            row_key=group_id,
            finalize=False,
        )
        self._sync_candidate_current_authority(
            con,
            group_id,
            writer_token=writer_token,
            finalize=False,
        )
        self._refresh_discovery_status_projection(
            con,
            writer_token=writer_token,
            finalize=finalize,
        )
        return CandidateWatchFact(
            id=int(cursor.lastrowid),
            group_id=group_id,
            membership_hash=membership_hash,
            quote_batch_id=quote_batch_id,
            observed_at_ms=observed_at_ms,
            last_result=last_result,
            reason=reason,
            bundle_cost=bundle_cost,
            gross_edge_bps=gross_edge_bps,
            max_bundle_size=max_bundle_size,
            priority_class=priority_class,
            consecutive_failures=consecutive_failures,
            effective_interval_s=effective_interval_s,
            schedule_reason=schedule_reason,
            next_due_at_ms=next_due_at_ms,
        )

    @staticmethod
    def _xor_candidate_digest(digest: str, *row_hashes: str | None) -> str:
        value = int(digest, 16)
        for row_hash in row_hashes:
            if row_hash is not None:
                value ^= int(row_hash.removeprefix("sha256:"), 16)
        return f"{value:064x}"

    def _sync_candidate_current_authority(
        self,
        con: sqlite3.Connection,
        group_id: str,
        *,
        writer_token: str | None = None,
        finalize: bool = True,
    ) -> None:
        old = con.execute(
            "SELECT row_hash,opportunity,last_result "
            "FROM neg_risk_candidate_current_authority "
            "WHERE group_id=?",
            (group_id,),
        ).fetchone()
        group = self._current_group_row(con, group_id)
        fact = con.execute(
            "SELECT * FROM neg_risk_candidate_watch_facts WHERE group_id=? "
            "ORDER BY id DESC LIMIT 1",
            (group_id,),
        ).fetchone()
        if writer_token is None:
            writer_token = self._begin_expected_owner_mutation(
                con,
                table_name="neg_risk_candidate_current_authority",
                operation="UPDATE",
                row_key=group_id,
            )
        expected_events: list[tuple[str, str, str]] = []
        new_hash: str | None = None
        opportunity = 0
        if group is None or group["status"] != "certified" or fact is None:
            con.execute(
                "DELETE FROM neg_risk_candidate_current_authority WHERE group_id=?",
                (group_id,),
            )
            if old is not None:
                expected_events.append(
                    (
                        "neg_risk_candidate_current_authority",
                        "DELETE",
                        group_id,
                    )
                )
        else:
            quote = None
            if fact["quote_batch_id"] is not None:
                quote = con.execute(
                    "SELECT * FROM neg_risk_group_quote_batches WHERE id=?",
                    (fact["quote_batch_id"],),
                ).fetchone()
            opportunity = int(
                fact["last_result"] == "watching"
                and fact["gross_edge_bps"] is not None
                and float(fact["gross_edge_bps"]) > 0
                and quote is not None
                and quote["status"] == "complete"
                and quote["membership_hash"] == group["membership_hash"]
                and fact["membership_hash"] == group["membership_hash"]
            )
            canonical = json.dumps(
                {
                    "event_id": str(group["event_id"]),
                    "fact_id": int(fact["id"]),
                    "fact_observed_at_ms": int(fact["observed_at_ms"]),
                    "group_id": group_id,
                    "group_revision": int(group["revision"]),
                    "structure_observed_at_ms": int(group["observed_at_ms"]),
                    "last_result": str(fact["last_result"]),
                    "bundle_cost": fact["bundle_cost"],
                    "gross_edge_bps": fact["gross_edge_bps"],
                    "max_bundle_size": fact["max_bundle_size"],
                    "legs": None if quote is None else json.loads(str(quote["legs_json"])),
                    "membership_hash": str(group["membership_hash"]),
                    "opportunity": opportunity,
                    "quote_started_at_ms": (
                        None if quote is None else int(quote["started_at_ms"])
                    ),
                    "quote_quoted_at_ms": (
                        None if quote is None else int(quote["quoted_at_ms"])
                    ),
                    "quote_batch_id": fact["quote_batch_id"],
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            new_hash = f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}"
            con.execute(
                "INSERT INTO neg_risk_candidate_current_authority("
                "group_id,event_id,membership_hash,group_revision,quote_batch_id,"
                "fact_id,last_result,opportunity,legs_json,canonical_json,row_hash"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(group_id) DO UPDATE SET "
                "event_id=excluded.event_id,membership_hash=excluded.membership_hash,"
                "group_revision=excluded.group_revision,"
                "quote_batch_id=excluded.quote_batch_id,fact_id=excluded.fact_id,"
                "last_result=excluded.last_result,opportunity=excluded.opportunity,"
                "legs_json=excluded.legs_json,canonical_json=excluded.canonical_json,"
                "row_hash=excluded.row_hash",
                (
                    group_id,
                    group["event_id"],
                    group["membership_hash"],
                    group["revision"],
                    fact["quote_batch_id"],
                    fact["id"],
                    fact["last_result"],
                    opportunity,
                    None if quote is None else quote["legs_json"],
                    canonical,
                    new_hash,
                ),
            )
            expected_events.append(
                (
                    "neg_risk_candidate_current_authority",
                    "INSERT" if old is None else "UPDATE",
                    group_id,
                )
            )
        aggregate = con.execute(
            "SELECT * FROM neg_risk_candidate_current_aggregate WHERE id=1"
        ).fetchone()
        if aggregate is None:
            raise ValueError("invalid-candidate-current-aggregate")
        old_hash = None if old is None else str(old["row_hash"])
        old_result = None if old is None else str(old["last_result"])
        new_result = None if new_hash is None else str(fact["last_result"])

        def state_delta(state: str) -> int:
            return int(new_result == state) - int(old_result == state)

        con.execute(
            "UPDATE neg_risk_candidate_current_aggregate SET "
            "current_group_count=current_group_count+?,"
            "opportunity_count=opportunity_count+?,"
            "watching_count=watching_count+?,"
            "no_edge_count=no_edge_count+?,"
            "unavailable_count=unavailable_count+?,"
            "aggregate_digest=? WHERE id=1",
            (
                int(new_hash is not None) - int(old is not None),
                opportunity - (0 if old is None else int(old["opportunity"])),
                state_delta("watching"),
                state_delta("no-edge"),
                state_delta("unavailable"),
                self._xor_candidate_digest(
                    str(aggregate["aggregate_digest"]), old_hash, new_hash
                ),
            ),
        )
        expected_events.append(
            ("neg_risk_candidate_current_aggregate", "UPDATE", "1")
        )
        self._consume_expected_owner_events(
            con,
            writer_token=writer_token,
            expected_events=expected_events,
            finalize=finalize,
        )

    @staticmethod
    def _current_group_row(con: sqlite3.Connection, group_id: str) -> sqlite3.Row | None:
        return con.execute(
            "SELECT * FROM neg_risk_group_revisions "
            "WHERE group_id=? ORDER BY revision DESC LIMIT 1",
            (group_id,),
        ).fetchone()

    @staticmethod
    def _current_quote_row(
        con: sqlite3.Connection,
        group_id: str,
        now_ms: int,
        max_age_ms: int,
    ) -> sqlite3.Row | None:
        return con.execute(
            "WITH current_group AS ("
            "SELECT group_id,event_id,revision,membership_hash,started_at_ms,"
            "observed_at_ms,source_cursor,status,legs_json "
            "FROM neg_risk_group_revisions "
            "WHERE group_id=? ORDER BY revision DESC LIMIT 1"
            ") "
            "SELECT "
            "g.group_id AS group_group_id,"
            "g.event_id AS group_event_id,"
            "g.revision AS group_revision,"
            "g.membership_hash AS group_membership_hash,"
            "g.started_at_ms AS group_started_at_ms,"
            "g.observed_at_ms AS group_observed_at_ms,"
            "g.source_cursor AS group_source_cursor,"
            "g.status AS group_status,"
            "g.legs_json AS group_legs_json,"
            "q.id AS quote_id,"
            "q.group_id AS quote_group_id,"
            "q.membership_hash AS quote_membership_hash,"
            "q.started_at_ms AS quote_started_at_ms,"
            "q.quoted_at_ms AS quote_quoted_at_ms,"
            "q.status AS quote_status,"
            "q.failure_reason AS quote_failure_reason,"
            "q.legs_json AS quote_legs_json "
            "FROM current_group g "
            "JOIN neg_risk_group_quote_batches q "
            "ON q.group_id=g.group_id AND q.membership_hash=g.membership_hash "
            "WHERE g.status='certified' AND q.status='complete' "
            "AND q.quoted_at_ms<=? AND q.quoted_at_ms>=? "
            "ORDER BY q.quoted_at_ms DESC,q.id DESC LIMIT 1",
            (group_id, now_ms, now_ms - max_age_ms),
        ).fetchone()

    def _insert_group_revision(
        self,
        con: sqlite3.Connection,
        revision: GroupRevision,
        current_row: sqlite3.Row | None,
    ) -> None:
        writer_token = self._begin_expected_owner_mutation(
            con,
            table_name="neg_risk_group_revisions",
            operation="INSERT",
            row_key=revision.group_id,
        )
        con.execute(
            "INSERT INTO neg_risk_group_revisions("
            "group_id,event_id,revision,membership_hash,started_at_ms,"
            "observed_at_ms,source_cursor,status,legs_json"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            (
                revision.group_id,
                revision.event_id,
                revision.revision,
                revision.membership_hash,
                revision.started_at_ms,
                revision.observed_at_ms,
                revision.source_cursor,
                revision.status,
                OpportunityPerceptionStore._group_legs_json(revision.legs),
            ),
        )
        self._consume_expected_owner_mutation(
            con,
            writer_token=writer_token,
            table_name="neg_risk_group_revisions",
            operation="INSERT",
            row_key=revision.group_id,
            finalize=False,
        )
        if current_row is not None and (
            current_row["membership_hash"] != revision.membership_hash
            or revision.status != "certified"
        ):
            quote_ids = [
                str(row["id"])
                for row in con.execute(
                    "SELECT id FROM neg_risk_group_quote_batches "
                    "WHERE group_id=? AND status='complete' ORDER BY id",
                    (revision.group_id,),
                )
            ]
            if quote_ids:
                con.execute(
                    "UPDATE neg_risk_group_quote_batches SET status='superseded' "
                    "WHERE group_id=? AND status='complete'",
                    (revision.group_id,),
                )
                self._consume_expected_owner_mutations(
                    con,
                    writer_token=writer_token,
                    table_name="neg_risk_group_quote_batches",
                    operation="UPDATE",
                    expected_row_keys=[revision.group_id] * len(quote_ids),
                    finalize=False,
                )
        self._sync_candidate_current_authority(
            con,
            revision.group_id,
            writer_token=writer_token,
            finalize=False,
        )
        self._refresh_discovery_status_projection(
            con,
            writer_token=writer_token,
        )

    @staticmethod
    def _stage_reconciliation_sample(
        con: sqlite3.Connection,
        *,
        window_id: str,
        group_id: str,
        event_id: str,
        membership_hash: str,
        quality: DiscoveryQuality,
        reason: str | None,
        legs: tuple[GroupLeg, ...] | None,
        observed_at_ms: int,
        source_cursor: str | None,
    ) -> None:
        if quality == "complete-supported":
            if legs is None:
                raise ValueError("reconciliation-supported-group-missing-legs")
            if GroupRevision.membership_digest(legs) != membership_hash:
                raise ValueError("reconciliation-staging-identity-mismatch")
        elif legs is not None:
            raise ValueError("reconciliation-rejected-group-has-legs")
        con.execute(
            "INSERT INTO neg_risk_reconciliation_staging("
            "window_id,group_id,event_id,membership_hash,quality,reason,"
            "legs_json,observed_at_ms,source_cursor) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                window_id,
                group_id,
                event_id,
                membership_hash,
                quality,
                reason,
                (None if legs is None else OpportunityPerceptionStore._group_legs_json(legs)),
                observed_at_ms,
                source_cursor,
            ),
        )

    @staticmethod
    def _reconciliation_candidate_matches_staging(
        candidate: DiscoveryScheduleCandidate,
        staging: sqlite3.Row,
    ) -> bool:
        legs_json = (
            None
            if candidate.legs is None
            else OpportunityPerceptionStore._group_legs_json(candidate.legs)
        )
        return (
            candidate.event_id == staging["event_id"]
            and candidate.membership_hash == staging["membership_hash"]
            and candidate.quality == staging["quality"]
            and candidate.reason == staging["reason"]
            and legs_json == staging["legs_json"]
        )

    @staticmethod
    def _update_reconciliation_staging(
        con: sqlite3.Connection,
        *,
        window_id: str,
        candidate: DiscoveryScheduleCandidate,
        observed_at_ms: int,
        source_cursor: str | None,
    ) -> None:
        if candidate.quality == "complete-supported":
            if (
                candidate.legs is None
                or GroupRevision.membership_digest(candidate.legs) != candidate.membership_hash
            ):
                raise ValueError("reconciliation-staging-identity-mismatch")
        elif candidate.legs is not None:
            raise ValueError("reconciliation-rejected-group-has-legs")
        con.execute(
            "UPDATE neg_risk_reconciliation_staging SET "
            "event_id=?,membership_hash=?,quality=?,reason=?,legs_json=?,"
            "observed_at_ms=?,source_cursor=? "
            "WHERE window_id=? AND group_id=?",
            (
                candidate.event_id,
                candidate.membership_hash,
                candidate.quality,
                candidate.reason,
                (
                    None
                    if candidate.legs is None
                    else OpportunityPerceptionStore._group_legs_json(candidate.legs)
                ),
                observed_at_ms,
                source_cursor,
                window_id,
                candidate.group_id,
            ),
        )

    @staticmethod
    def _reconciliation_window_from_row(
        row: sqlite3.Row,
    ) -> ReconciliationWindow:
        failure_reason = None if row["failure_reason"] is None else str(row["failure_reason"])
        return ReconciliationWindow(
            id=str(row["id"]),
            status=("failed" if failure_reason is not None else row["status"]),
            failure_reason=failure_reason,
            next_cursor=(None if row["next_cursor"] is None else str(row["next_cursor"])),
            started_at_ms=int(row["started_at_ms"]),
            checkpoint_at_ms=int(row["checkpoint_at_ms"]),
            finished_at_ms=(None if row["finished_at_ms"] is None else int(row["finished_at_ms"])),
            pages_completed=int(row["pages_completed"]),
            events_seen=int(row["events_seen"]),
            groups_staged=int(row["groups_staged"]),
            rejected_count=int(row["rejected_count"]),
            observations_count=int(row["observations_count"]),
            baseline_count=int(row["baseline_count"]),
            baseline_digest=(
                None if row["baseline_digest"] is None else str(row["baseline_digest"])
            ),
            added_count=(None if row["added_count"] is None else int(row["added_count"])),
            changed_count=(None if row["changed_count"] is None else int(row["changed_count"])),
            closed_count=(None if row["closed_count"] is None else int(row["closed_count"])),
            unchanged_count=(
                None if row["unchanged_count"] is None else int(row["unchanged_count"])
            ),
            applied_rejected_count=(
                None
                if row["applied_rejected_count"] is None
                else int(row["applied_rejected_count"])
            ),
        )

    @staticmethod
    def _reconciliation_baseline_digest(rows: list[sqlite3.Row]) -> str:
        canonical_rows = sorted(
            (
                str(row["group_id"]),
                str(row["event_id"]),
                int(row["revision"]),
                str(row["membership_hash"]),
                str(row["status"]),
            )
            for row in rows
        )
        canonical = json.dumps(
            {
                "domain": "polyarb.reconciliation.baseline",
                "version": 1,
                "count": len(canonical_rows),
                "rows": canonical_rows,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        return f"sha256:{hashlib.sha256(canonical).hexdigest()}"

    @staticmethod
    def _reconciliation_evidence_result_revisions(
        con: sqlite3.Connection,
        window_id: str,
    ) -> list[sqlite3.Row]:
        return con.execute(
            "SELECT r.* FROM neg_risk_group_revisions r "
            "JOIN neg_risk_reconciliation_diff_evidence e "
            "ON e.group_id=r.group_id AND e.result_revision=r.revision "
            "WHERE e.window_id=? AND e.result_revision IS NOT NULL "
            "ORDER BY r.group_id,r.revision",
            (window_id,),
        ).fetchall()

    @staticmethod
    def _insert_reconciliation_diff_evidence(
        con: sqlite3.Connection,
        *,
        window_id: str,
        group_id: str,
        action: str,
        baseline: sqlite3.Row | None,
        staged: sqlite3.Row | None,
        result: sqlite3.Row | None,
    ) -> None:
        con.execute(
            "INSERT INTO neg_risk_reconciliation_diff_evidence("
            "window_id,group_id,action,"
            "baseline_event_id,baseline_revision,baseline_membership_hash,"
            "staged_event_id,staged_membership_hash,staged_quality,"
            "result_event_id,result_revision,result_membership_hash,result_status"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                window_id,
                group_id,
                action,
                None if baseline is None else baseline["event_id"],
                None if baseline is None else baseline["revision"],
                None if baseline is None else baseline["membership_hash"],
                None if staged is None else staged["event_id"],
                None if staged is None else staged["membership_hash"],
                None if staged is None else staged["quality"],
                None if result is None else result["event_id"],
                None if result is None else result["revision"],
                None if result is None else result["membership_hash"],
                None if result is None else result["status"],
            ),
        )

    @staticmethod
    def _validate_reconciliation_snapshot(
        row: sqlite3.Row,
        receipts: list[sqlite3.Row],
        batch_samples: list[sqlite3.Row],
        staged: list[sqlite3.Row],
        baseline: list[sqlite3.Row],
        evidence: list[sqlite3.Row],
        result_revisions: list[sqlite3.Row],
        *,
        deadline_check: Callable[[], None] = lambda: None,
    ) -> None:
        deadline_check()
        raw_status = str(row["status"])
        failure_reason = row["failure_reason"]
        status = "failed" if failure_reason is not None else raw_status
        pages = int(row["pages_completed"])
        started = int(row["started_at_ms"])
        checkpoint = int(row["checkpoint_at_ms"])
        finished = row["finished_at_ms"]
        if row["baseline_digest"] is None:
            raise ReconciliationUnprovableError("reconciliation-baseline-digest-unavailable")
        if (
            raw_status not in {"open", "complete", "applied"}
            or (failure_reason is not None and raw_status != "open")
            or pages != len(receipts)
            or checkpoint < started
            or len(staged) != int(row["groups_staged"])
            or len(baseline) != int(row["baseline_count"])
            or len(batch_samples) != int(row["observations_count"])
            or sum(int(item["page_event_count"]) for item in receipts) != int(row["events_seen"])
            or sum(int(item["unique_count"]) for item in receipts) != int(row["groups_staged"])
            or sum(int(item["observed_count"]) for item in receipts)
            != int(row["observations_count"])
            or sum(int(item["rejected_count"]) for item in receipts) != int(row["rejected_count"])
            or str(row["baseline_digest"])
            != OpportunityPerceptionStore._reconciliation_baseline_digest(baseline)
        ):
            raise ValueError("invalid-reconciliation-window-or-baseline-digest")
        for baseline_row in baseline:
            deadline_check()
            if (
                baseline_row["status"] != "certified"
                or int(baseline_row["revision"]) < 1
                or not all(
                    isinstance(baseline_row[name], str) and str(baseline_row[name]).strip()
                    for name in ("group_id", "event_id", "membership_hash")
                )
            ):
                raise ValueError("invalid-reconciliation-baseline")
        if pages == 0:
            if (
                status != "open"
                or row["next_cursor"] is not None
                or finished is not None
                or checkpoint != started
                or row["failure_reason"] is not None
            ):
                raise ValueError("invalid-reconciliation-empty-window")
            if staged or batch_samples:
                raise ValueError("invalid-reconciliation-empty-staging")
            return
        diff_counts = (
            row["added_count"],
            row["changed_count"],
            row["closed_count"],
            row["unchanged_count"],
            row["applied_rejected_count"],
        )
        present = [value is not None for value in diff_counts]
        if (
            any(present) != all(present)
            or (status == "applied") != all(present)
            or any(value is not None and int(value) < 0 for value in diff_counts)
        ):
            raise ValueError("invalid-reconciliation-diff-counts")
        if status == "applied":
            OpportunityPerceptionStore._validate_reconciliation_diff_evidence(
                row,
                staged,
                baseline,
                evidence,
                result_revisions,
                deadline_check=deadline_check,
            )
        elif evidence or result_revisions:
            raise ValueError("invalid-reconciliation-premature-diff-evidence")
        seen_cursors: set[str] = set()
        for sequence, receipt in enumerate(receipts, start=1):
            deadline_check()
            observed = int(receipt["observed_count"])
            unique = int(receipt["unique_count"])
            updated = int(receipt["update_count"])
            duplicate = int(receipt["duplicate_count"])
            requested = receipt["requested_cursor"]
            next_cursor = receipt["next_cursor"]
            if (
                int(receipt["batch_sequence"]) != sequence
                or int(receipt["started_at_ms"]) > int(receipt["finished_at_ms"])
                or bool(receipt["completed"]) != (next_cursor is None)
                or observed != int(receipt["groups_staged"])
                or observed != unique + updated + duplicate
                or not 0 <= int(receipt["rejected_count"]) <= observed
                or (sequence == 1 and requested is not None)
                or (sequence > 1 and requested != receipts[sequence - 2]["next_cursor"])
                or (
                    sequence > 1
                    and int(receipt["started_at_ms"])
                    < int(receipts[sequence - 2]["finished_at_ms"])
                )
                or (
                    next_cursor is not None
                    and (next_cursor == requested or next_cursor in seen_cursors)
                )
            ):
                raise ValueError("invalid-reconciliation-receipt-chain")
            for cursor in (requested, next_cursor):
                if cursor is not None:
                    seen_cursors.add(str(cursor))
        if int(receipts[0]["started_at_ms"]) < started:
            raise ValueError("invalid-reconciliation-window-start")
        receipts_by_id = {int(receipt["id"]): receipt for receipt in receipts}
        samples_by_batch: dict[int, list[sqlite3.Row]] = {
            int(receipt["id"]): [] for receipt in receipts
        }
        materialized: dict[str, sqlite3.Row] = {}
        for sample in batch_samples:
            deadline_check()
            receipt = receipts_by_id.get(int(sample["batch_id"]))
            if receipt is None:
                raise ValueError("orphan-reconciliation-batch-sample")
            samples_by_batch[int(sample["batch_id"])].append(sample)
            if not all(
                isinstance(sample[name], str) and str(sample[name]).strip()
                for name in ("group_id", "event_id", "membership_hash")
            ):
                raise ValueError("invalid-reconciliation-staging-identity")
            quality = str(sample["quality"])
            reason = sample["reason"]
            legs_json = sample["legs_json"]
            if quality == "complete-supported":
                if reason is not None or legs_json is None:
                    raise ValueError("invalid-reconciliation-supported-staging")
                legs = OpportunityPerceptionStore._group_legs_from_json(str(legs_json))
                if (
                    len(legs) < 2
                    or GroupRevision.membership_digest(legs) != sample["membership_hash"]
                ):
                    raise ValueError("invalid-reconciliation-staging-identity")
            elif quality in {"complete-unsupported", "incomplete-source"}:
                if not isinstance(reason, str) or not reason.strip() or legs_json is not None:
                    raise ValueError("invalid-reconciliation-rejected-staging")
            else:
                raise ValueError("invalid-reconciliation-staging-quality")
            if receipt["requested_cursor"] != sample["source_cursor"] or int(
                receipt["finished_at_ms"]
            ) != int(sample["observed_at_ms"]):
                raise ValueError("invalid-reconciliation-staging-receipt")
            group_id = str(sample["group_id"])
            prior = materialized.get(group_id)
            materialization = str(sample["materialization"])
            same_as_prior = (
                prior is not None
                and OpportunityPerceptionStore._reconciliation_rows_same_fact(prior, sample)
            )
            if (
                (materialization == "unique" and prior is not None)
                or (materialization == "updated" and (prior is None or same_as_prior))
                or (materialization == "duplicate" and not same_as_prior)
            ):
                raise ValueError("invalid-reconciliation-materialization")
            if materialization in {"unique", "updated"}:
                materialized[group_id] = sample
        for receipt in receipts:
            deadline_check()
            samples = samples_by_batch[int(receipt["id"])]
            if (
                len(samples) != int(receipt["observed_count"])
                or sum(sample["quality"] != "complete-supported" for sample in samples)
                != int(receipt["rejected_count"])
                or sum(sample["materialization"] == "unique" for sample in samples)
                != int(receipt["unique_count"])
                or sum(sample["materialization"] == "updated" for sample in samples)
                != int(receipt["update_count"])
                or sum(sample["materialization"] == "duplicate" for sample in samples)
                != int(receipt["duplicate_count"])
            ):
                raise ValueError("invalid-reconciliation-batch-staging-counts")
        staged_by_group = {str(sample["group_id"]): sample for sample in staged}
        if staged_by_group.keys() != materialized.keys():
            raise ValueError("invalid-reconciliation-materialized-groups")
        for group_id, latest_sample in materialized.items():
            deadline_check()
            staging = staged_by_group[group_id]
            if (
                not OpportunityPerceptionStore._reconciliation_rows_same_fact(
                    latest_sample, staging
                )
                or int(latest_sample["observed_at_ms"]) != int(staging["observed_at_ms"])
                or latest_sample["source_cursor"] != staging["source_cursor"]
            ):
                raise ValueError("invalid-reconciliation-materialized-staging")
        latest = receipts[-1]
        latest_samples = samples_by_batch[int(latest["id"])]
        terminal_counts = (
            latest["page_event_count"],
            latest["groups_staged"],
            latest["observed_count"],
            latest["unique_count"],
            latest["update_count"],
            latest["duplicate_count"],
            latest["rejected_count"],
        )
        if (
            latest["next_cursor"] != row["next_cursor"]
            or int(latest["finished_at_ms"]) != checkpoint
            or (status in {"complete", "applied"}) != bool(latest["completed"])
            or (
                status in {"complete", "applied"}
                and (
                    finished is None
                    or int(finished) != checkpoint
                    or any(int(value) != 0 for value in terminal_counts)
                    or latest_samples
                    or row["failure_reason"] is not None
                )
            )
            or (status == "open" and (finished is not None or row["failure_reason"] is not None))
            or (
                status == "failed"
                and (
                    row["failure_reason"] != "cursor-loop"
                    or finished is None
                    or int(finished) < checkpoint
                    or bool(latest["completed"])
                )
            )
        ):
            raise ValueError("invalid-reconciliation-checkpoint")

    @staticmethod
    def _validate_reconciliation_diff_evidence(
        window: sqlite3.Row,
        staged: list[sqlite3.Row],
        baseline: list[sqlite3.Row],
        evidence: list[sqlite3.Row],
        result_revisions: list[sqlite3.Row],
        *,
        deadline_check: Callable[[], None] = lambda: None,
    ) -> None:
        staged_by_group = {str(row["group_id"]): row for row in staged}
        baseline_by_group = {str(row["group_id"]): row for row in baseline}
        result_by_identity = {
            (str(row["group_id"]), int(row["revision"])): row for row in result_revisions
        }
        actions = {"added", "changed", "closed", "unchanged", "rejected"}
        action_counts = {action: 0 for action in actions}
        staged_actions: dict[str, list[sqlite3.Row]] = {}
        result_evidence_count = 0
        for item in evidence:
            deadline_check()
            action = str(item["action"])
            group_id = str(item["group_id"])
            if action not in actions:
                raise ValueError("invalid-reconciliation-diff-evidence-action")
            action_counts[action] += 1
            baseline_row = baseline_by_group.get(group_id)
            staged_row = staged_by_group.get(group_id)
            if action != "closed":
                staged_actions.setdefault(group_id, []).append(item)
            expected_baseline = (
                (None, None, None)
                if baseline_row is None
                else (
                    baseline_row["event_id"],
                    int(baseline_row["revision"]),
                    baseline_row["membership_hash"],
                )
            )
            actual_baseline = (
                item["baseline_event_id"],
                (None if item["baseline_revision"] is None else int(item["baseline_revision"])),
                item["baseline_membership_hash"],
            )
            if actual_baseline != expected_baseline:
                raise ValueError("invalid-reconciliation-diff-evidence-baseline")
            if action == "closed":
                if any(
                    item[name] is not None
                    for name in (
                        "staged_event_id",
                        "staged_membership_hash",
                        "staged_quality",
                    )
                ):
                    raise ValueError("invalid-reconciliation-closed-evidence-staging")
            elif staged_row is None or (
                item["staged_event_id"],
                item["staged_membership_hash"],
                item["staged_quality"],
            ) != (
                staged_row["event_id"],
                staged_row["membership_hash"],
                staged_row["quality"],
            ):
                raise ValueError("invalid-reconciliation-diff-evidence-staging")

            result_revision = item["result_revision"]
            result_row = (
                None
                if result_revision is None
                else result_by_identity.get((group_id, int(result_revision)))
            )
            if result_revision is not None:
                result_evidence_count += 1
            expected_result = (
                (None, None, None, None)
                if result_row is None
                else (
                    result_row["event_id"],
                    int(result_row["revision"]),
                    result_row["membership_hash"],
                    result_row["status"],
                )
            )
            actual_result = (
                item["result_event_id"],
                None if result_revision is None else int(result_revision),
                item["result_membership_hash"],
                item["result_status"],
            )
            if actual_result != expected_result:
                raise ValueError("invalid-reconciliation-diff-evidence-result")

            if action == "rejected":
                if (
                    result_row is not None
                    or staged_row is None
                    or not (
                        staged_row["quality"] != "complete-supported"
                        or (
                            baseline_row is not None
                            and staged_row["event_id"] != baseline_row["event_id"]
                        )
                    )
                ):
                    raise ValueError("invalid-reconciliation-rejected-evidence")
            elif action == "added":
                if (
                    baseline_row is not None
                    or staged_row is None
                    or staged_row["quality"] != "complete-supported"
                    or result_row is None
                    or int(result_row["revision"]) != 1
                    or result_row["event_id"] != staged_row["event_id"]
                    or result_row["membership_hash"] != staged_row["membership_hash"]
                    or result_row["status"] != "certified"
                ):
                    raise ValueError("invalid-reconciliation-added-evidence")
            elif action == "changed":
                if (
                    baseline_row is None
                    or staged_row is None
                    or staged_row["quality"] != "complete-supported"
                    or staged_row["event_id"] != baseline_row["event_id"]
                    or result_row is None
                    or int(result_row["revision"]) != int(baseline_row["revision"]) + 1
                    or result_row["event_id"] != staged_row["event_id"]
                    or result_row["membership_hash"] != staged_row["membership_hash"]
                    or result_row["status"] != "certified"
                ):
                    raise ValueError("invalid-reconciliation-changed-evidence")
            elif action == "unchanged":
                if (
                    staged_row is None
                    or staged_row["quality"] != "complete-supported"
                    or result_row is None
                    or (
                        baseline_row is not None
                        and OpportunityPerceptionStore._group_row_matches_reconciliation_baseline(
                            result_row, baseline_row
                        )
                        and (
                            result_row["event_id"] != staged_row["event_id"]
                            or result_row["membership_hash"] != staged_row["membership_hash"]
                        )
                    )
                ):
                    raise ValueError("invalid-reconciliation-unchanged-evidence")
            elif (
                baseline_row is None
                or result_row is None
                or result_row["event_id"] != baseline_row["event_id"]
                or int(result_row["revision"]) != int(baseline_row["revision"]) + 1
                or result_row["membership_hash"] != baseline_row["membership_hash"]
                or result_row["status"] != "closed"
                or (staged_row is not None and staged_row["event_id"] == baseline_row["event_id"])
            ):
                raise ValueError("invalid-reconciliation-closed-evidence")

        if result_evidence_count != len(result_revisions):
            raise ValueError("invalid-reconciliation-diff-evidence-results")
        if set(staged_actions) != set(staged_by_group) or any(
            len(items) != 1 for items in staged_actions.values()
        ):
            raise ValueError("invalid-reconciliation-staged-diff-evidence")
        expected_counts = (
            int(window["added_count"]),
            int(window["changed_count"]),
            int(window["closed_count"]),
            int(window["unchanged_count"]),
            int(window["applied_rejected_count"]),
        )
        actual_counts = tuple(
            action_counts[action]
            for action in ("added", "changed", "closed", "unchanged", "rejected")
        )
        if actual_counts != expected_counts or action_counts["closed"] > len(baseline):
            raise ValueError("invalid-reconciliation-diff-evidence-counts")

    @staticmethod
    def _reconciliation_rows_same_fact(
        left: sqlite3.Row,
        right: sqlite3.Row,
    ) -> bool:
        return all(
            left[name] == right[name]
            for name in (
                "group_id",
                "event_id",
                "membership_hash",
                "quality",
                "reason",
                "legs_json",
            )
        )

    @staticmethod
    def _group_row_matches_reconciliation_baseline(
        current: sqlite3.Row,
        baseline: sqlite3.Row,
    ) -> bool:
        return (
            current["group_id"] == baseline["group_id"]
            and current["event_id"] == baseline["event_id"]
            and int(current["revision"]) == int(baseline["revision"])
            and current["membership_hash"] == baseline["membership_hash"]
            and current["status"] == baseline["status"] == "certified"
        )

    @staticmethod
    def _reconciliation_diff_from_row(row: sqlite3.Row) -> ReconciliationDiff:
        if row["finished_at_ms"] is None:
            raise ValueError("reconciliation-missing-finish")
        values = (
            row["added_count"],
            row["changed_count"],
            row["closed_count"],
            row["unchanged_count"],
            row["applied_rejected_count"],
        )
        if any(value is None for value in values):
            raise ValueError("reconciliation-missing-diff")
        return ReconciliationDiff(
            window_id=str(row["id"]),
            added=int(values[0]),
            changed=int(values[1]),
            closed=int(values[2]),
            unchanged=int(values[3]),
            rejected=int(values[4]),
            started_at_ms=int(row["started_at_ms"]),
            finished_at_ms=int(row["finished_at_ms"]),
        )

    @staticmethod
    def _group_legs_from_json(value: str) -> tuple[GroupLeg, ...]:
        decoded = json.loads(value)
        if not isinstance(decoded, list):
            raise ValueError("invalid-group-legs-json")
        return tuple(GroupLeg(*leg) for leg in decoded)

    @staticmethod
    def _group_legs_json(legs: tuple[GroupLeg, ...]) -> str:
        return json.dumps(
            [[leg.market_id, leg.condition_id, leg.yes_token_id, leg.title] for leg in legs],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _quote_legs_json(legs: tuple[GroupQuoteLeg, ...]) -> str:
        return json.dumps(
            [
                [
                    leg.yes_token_id,
                    leg.membership_hash,
                    leg.best_ask_price,
                    leg.best_ask_size,
                    leg.terminal_state,
                ]
                for leg in legs
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _group_from_row(row: sqlite3.Row, *, prefix: str = "") -> GroupRevision:
        return GroupRevision(
            group_id=row[f"{prefix}group_id"],
            event_id=row[f"{prefix}event_id"],
            revision=row[f"{prefix}revision"],
            membership_hash=row[f"{prefix}membership_hash"],
            started_at_ms=row[f"{prefix}started_at_ms"],
            observed_at_ms=row[f"{prefix}observed_at_ms"],
            source_cursor=row[f"{prefix}source_cursor"],
            status=row[f"{prefix}status"],
            legs=tuple(GroupLeg(*leg) for leg in json.loads(row[f"{prefix}legs_json"])),
        )

    @staticmethod
    def _quote_batch_from_row(row: sqlite3.Row, *, prefix: str = "") -> GroupQuoteBatch:
        return GroupQuoteBatch(
            group_id=row[f"{prefix}group_id"],
            membership_hash=row[f"{prefix}membership_hash"],
            quote_batch_id=row[f"{prefix}id"],
            started_at_ms=row[f"{prefix}started_at_ms"],
            quoted_at_ms=row[f"{prefix}quoted_at_ms"],
            status=row[f"{prefix}status"],
            failure_reason=row[f"{prefix}failure_reason"],
            legs=tuple(GroupQuoteLeg(*leg) for leg in json.loads(row[f"{prefix}legs_json"])),
        )

    @classmethod
    def _validated_group_from_row(
        cls,
        row: sqlite3.Row,
        *,
        prefix: str = "",
    ) -> GroupRevision | None:
        try:
            group = cls._group_from_row(row, prefix=prefix)
            if group.status not in _GROUP_STATUSES:
                return None
            if GroupRevision.membership_digest(group.legs) != group.membership_hash:
                return None
            if group.started_at_ms > group.observed_at_ms:
                return None
            if group.status == "certified":
                validated = GroupRevision.certified(
                    group_id=group.group_id,
                    event_id=group.event_id,
                    revision=group.revision,
                    started_at_ms=group.started_at_ms,
                    observed_at_ms=group.observed_at_ms,
                    source_cursor=group.source_cursor,
                    legs=group.legs,
                )
                if group != validated:
                    return None
            return group
        except (IndexError, KeyError, TypeError, ValueError):
            return None

    @classmethod
    def _validated_quote_from_row(
        cls,
        row: sqlite3.Row,
        group: GroupRevision,
        *,
        prefix: str = "",
    ) -> GroupQuoteBatch | None:
        try:
            quote = cls._quote_batch_from_row(row, prefix=prefix)
            validated = GroupQuoteBatch.complete(
                group_id=quote.group_id,
                membership_hash=quote.membership_hash,
                quote_batch_id=quote.quote_batch_id,
                started_at_ms=quote.started_at_ms,
                quoted_at_ms=quote.quoted_at_ms,
                legs=quote.legs,
            )
            if quote != validated:
                return None
            if quote.group_id != group.group_id:
                return None
            if quote.membership_hash != group.membership_hash:
                return None
            if tuple(leg.yes_token_id for leg in quote.legs) != tuple(
                leg.yes_token_id for leg in group.legs
            ):
                return None
            return quote
        except (IndexError, KeyError, TypeError, ValueError):
            return None
