"""SQLite authority for certified groups and atomic all-leg quote batches."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Literal

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
from polyarb.storage.schemas import DDL

_BUSY_TIMEOUT_MS = 5_000
_HEARTBEAT_AUTH_DOMAIN = "polyarb-producer-heartbeat-v1"
_CHILD_HEARTBEAT_PREIMAGES: dict[tuple[str, str, int], str] = {}
_GROUP_STATUSES = {"discovered", "certified", "stale", "invalidated", "closed"}
_ACTUAL_CANDIDATE_AUTHORITY_SQL = (
    "(s.group_id IS NULL OR s.promoted_at_ms IS NOT NULL OR EXISTS ("
    "SELECT 1 FROM neg_risk_candidate_watch_facts f "
    "WHERE f.group_id=c.group_id))"
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


def validate_producer_history(
    con: sqlite3.Connection,
    component: str,
    *,
    now_ms: int,
) -> ProducerHistoryState:
    if component not in {"candidate", "discovery", "reconciliation"}:
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

    def __init__(self, db_path: Path, *, read_only: bool = False) -> None:
        self._db_path = Path(db_path)
        self._read_only = read_only
        if not read_only:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def db_path(self) -> Path:
        return self._db_path

    def init_schema(self) -> None:
        con = self._connect()
        try:
            schedule_evidence_existed = (
                con.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' "
                    "AND name='neg_risk_discovery_schedule_evidence'"
                ).fetchone()
                is not None
            )
            con.executescript(DDL)
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
            con.execute(
                "UPDATE neg_risk_group_schedule SET "
                "promotion_eligible_at_ms=COALESCE("
                "promotion_eligible_at_ms,first_discovered_at_ms),"
                "promotion_queue_deadline_at_ms=COALESCE("
                "promotion_eligible_at_ms,first_discovered_at_ms)+? "
                "WHERE quality='complete-supported'",
                (proof.candidate_max_wait_ms,),
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
                con.execute(
                    "UPDATE neg_risk_group_schedule SET promoted_at_ms=NULL,"
                    "candidate_start_deadline_at_ms=NULL WHERE group_id=?",
                    (str(row["group_id"]),),
                )
            con.execute(
                "UPDATE neg_risk_group_schedule SET "
                "candidate_start_deadline_at_ms=promoted_at_ms+? "
                "WHERE promoted_at_ms IS NOT NULL "
                "AND candidate_start_deadline_at_ms IS NULL",
                (proof.candidate_max_wait_ms,),
            )
            self._record_existing_candidate_admissions(
                con,
                proof_row=con.execute(
                    "SELECT * FROM neg_risk_discovery_admission_state WHERE id=1"
                ).fetchone(),
                recorded_at_ms=now_ms,
            )
            self._admit_waiting_candidates(con, now_ms=now_ms)
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
    ) -> tuple[str, ...]:
        """Atomically certify, schedule, sample, promote, and advance a page."""
        admission_proof.validate()
        if started_at_ms > finished_at_ms:
            raise ValueError("invalid-discovery-timestamp-order")
        if completed != (next_cursor is None):
            raise ValueError("invalid-discovery-completion-cursor")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
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
            con.execute("COMMIT")
            return tuple(group_id for _, group_id in promoted)
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

    @staticmethod
    def _record_candidate_admission(
        con: sqlite3.Connection,
        *,
        schedule: sqlite3.Row,
        proof_row: sqlite3.Row,
        recorded_at_ms: int,
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

    @classmethod
    def _record_existing_candidate_admissions(
        cls,
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
            cls._record_candidate_admission(
                con,
                schedule=schedule,
                proof_row=proof_row,
                recorded_at_ms=recorded_at_ms,
            )

    @staticmethod
    def _admit_waiting_candidates(
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
            con.execute(
                "UPDATE neg_risk_group_schedule SET promoted_at_ms=?,"
                "candidate_start_deadline_at_ms=? WHERE group_id=?",
                (now_ms, now_ms + candidate_max_wait_ms, group_id),
            )
            schedule = con.execute(
                "SELECT * FROM neg_risk_group_schedule WHERE group_id=?",
                (group_id,),
            ).fetchone()
            OpportunityPerceptionStore._record_candidate_admission(
                con,
                schedule=schedule,
                proof_row=proof_row,
                recorded_at_ms=now_ms,
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
                self._group_legs_json(revision.legs),
            ),
        )
        if current is not None and current["membership_hash"] != revision.membership_hash:
            con.execute(
                "UPDATE neg_risk_group_quote_batches SET status='superseded' "
                "WHERE group_id=? AND status='complete'",
                (revision.group_id,),
            )

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
        con.execute(
            "INSERT INTO neg_risk_group_revisions("
            "group_id,event_id,revision,membership_hash,started_at_ms,"
            "observed_at_ms,source_cursor,status,legs_json"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            (
                prior.group_id,
                prior.event_id,
                prior.revision + 1,
                prior.membership_hash,
                started_at_ms,
                finished_at_ms,
                source_cursor or "<start>",
                "invalidated",
                self._group_legs_json(prior.legs),
            ),
        )
        con.execute(
            "UPDATE neg_risk_group_quote_batches SET status='superseded' "
            "WHERE group_id=? AND status='complete'",
            (group_id,),
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

    def coverage_windows(self, now_ms: int) -> CoverageWindows:
        con = self._connect()
        try:
            con.execute("BEGIN")
            result = self._coverage_windows_in_snapshot(con, now_ms)
            con.execute("COMMIT")
            return result
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def discovery_status(self, now_ms: int) -> DiscoveryStatus:
        con = self._connect()
        try:
            con.execute("BEGIN")
            state = con.execute("SELECT * FROM neg_risk_discovery_state WHERE id=1").fetchone()
            schedules = con.execute(
                "SELECT * FROM neg_risk_group_schedule ORDER BY group_id"
            ).fetchall()
            queue_rows = con.execute(
                "SELECT priority_class,COUNT(*) AS depth "
                "FROM neg_risk_group_schedule WHERE promoted_at_ms IS NOT NULL "
                "GROUP BY priority_class"
            ).fetchall()
            oldest = con.execute(
                "SELECT MIN(COALESCE(last_visited_at_ms,first_discovered_at_ms)) "
                "AS oldest FROM neg_risk_group_schedule"
            ).fetchone()
            current_revisions = {
                str(row["group_id"]): row
                for row in con.execute(
                    "SELECT r.* FROM neg_risk_group_revisions r JOIN ("
                    "SELECT group_id,MAX(revision) AS revision "
                    "FROM neg_risk_group_revisions GROUP BY group_id"
                    ") c ON c.group_id=r.group_id AND c.revision=r.revision"
                ).fetchall()
            }
            revision_identities = {
                (
                    str(row["group_id"]),
                    str(row["event_id"]),
                    str(row["membership_hash"]),
                ): int(row["first_observed_at_ms"])
                for row in con.execute(
                    "SELECT group_id,event_id,membership_hash,"
                    "MIN(observed_at_ms) AS first_observed_at_ms "
                    "FROM neg_risk_group_revisions WHERE status='certified'"
                    " GROUP BY group_id,event_id,membership_hash"
                ).fetchall()
            }
            batches = con.execute("SELECT * FROM neg_risk_discovery_batches ORDER BY id").fetchall()
            latest_batch = batches[-1] if batches else None
            batch_samples = con.execute(
                "SELECT * FROM neg_risk_discovery_batch_samples ORDER BY batch_id,group_id"
            ).fetchall()
            schedule_evidence = con.execute(
                "SELECT * FROM neg_risk_discovery_schedule_evidence ORDER BY batch_id,group_id"
            ).fetchall()
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
            fact_group_ids = {
                str(row["group_id"])
                for row in con.execute(
                    "SELECT DISTINCT group_id FROM neg_risk_candidate_watch_facts"
                ).fetchall()
            }
            breach_fact_evidence = {
                (str(row["group_id"]), int(row["observed_at_ms"]))
                for row in con.execute(
                    "SELECT group_id,observed_at_ms "
                    "FROM neg_risk_candidate_watch_facts "
                    "WHERE last_result='unavailable' "
                    "AND reason='candidate-start-deadline-breached'"
                ).fetchall()
            }
            attempt_starts = con.execute(
                "SELECT * FROM neg_risk_candidate_attempt_starts ORDER BY id"
            ).fetchall()
            admissions = con.execute(
                "SELECT * FROM neg_risk_candidate_admissions ORDER BY id"
            ).fetchall()
            coverage = self._coverage_windows_in_snapshot(con, now_ms)
            self._validate_discovery_snapshot(
                state=state,
                schedules=schedules,
                current_revisions=current_revisions,
                revision_identities=revision_identities,
                batches=batches,
                batch_samples=batch_samples,
                schedule_evidence=schedule_evidence,
                latest_batch=latest_batch,
                latest_samples=latest_samples,
                load_row=load_row,
                admission_row=admission_row,
                fact_group_ids=fact_group_ids,
                breach_fact_evidence=breach_fact_evidence,
                attempt_starts=attempt_starts,
                admissions=admissions,
                coverage=coverage,
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        queue = {"high": 0, "normal": 0, "explore": 0}
        queue.update({str(row["priority_class"]): int(row["depth"]) for row in queue_rows})
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
            promotion_queue_depth=sum(
                1
                for row in schedules
                if row["quality"] == "complete-supported" and row["promoted_at_ms"] is None
            ),
            outstanding_admitted_count=sum(
                1
                for row in schedules
                if row["promoted_at_ms"] is not None and str(row["group_id"]) not in fact_group_ids
            ),
            candidate_attempt_start_count=len(attempt_starts),
            candidate_start_deadline_breach_count=sum(
                int(row["deadline_breached"]) for row in attempt_starts
            ),
            candidate_start_ready=not any(bool(row["deadline_breached"]) for row in attempt_starts),
        )

    @staticmethod
    def _coverage_windows_in_snapshot(
        con: sqlite3.Connection,
        now_ms: int,
    ) -> CoverageWindows:
        totals = con.execute(
            "SELECT COUNT(*) AS groups_count,"
            "COALESCE(SUM(CAST(liquidity_weight AS REAL)),0) AS total_weight "
            "FROM neg_risk_group_schedule"
        ).fetchone()
        known_groups = int(totals["groups_count"])
        total_weight = Decimal(str(totals["total_weight"]))
        windows: dict[int, CoverageWindow] = {}
        for minutes in (15, 30, 60):
            row = con.execute(
                "WITH visited AS ("
                "SELECT DISTINCT bs.group_id "
                "FROM neg_risk_discovery_batch_samples bs "
                "JOIN neg_risk_discovery_batches b ON b.id=bs.batch_id "
                "WHERE b.finished_at_ms>=? AND b.finished_at_ms<=?"
                ") SELECT COUNT(*) AS visited_groups,"
                "COALESCE(SUM(CAST(s.liquidity_weight AS REAL)),0) AS visited_weight "
                "FROM neg_risk_group_schedule s JOIN visited v USING(group_id)",
                (now_ms - minutes * 60_000, now_ms),
            ).fetchone()
            visited_groups = int(row["visited_groups"])
            visited_weight = Decimal(str(row["visited_weight"]))
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
            samples_by_batch.setdefault(int(sample["batch_id"]), []).append(sample)
        evidence_by_key = {
            (int(row["batch_id"]), str(row["group_id"])): row for row in schedule_evidence
        }
        for batch in batches:
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
                if int(batch["sweep_id"]) != 1 or int(batch["batch_sequence"]) != 1:
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
                    revision["event_id"] != row["event_id"] or revision["status"] != "invalidated"
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

    def current_reconciliation(self) -> ReconciliationWindow | None:
        con = self._connect()
        try:
            con.execute("BEGIN")
            row = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            if row is None:
                con.execute("COMMIT")
                return None
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
            self._validate_reconciliation_snapshot(
                row,
                receipts,
                batch_samples,
                staged,
                baseline,
                evidence,
                result_revisions,
            )
            result = self._reconciliation_window_from_row(row)
            con.execute("COMMIT")
            return result
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
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
                "SELECT status FROM neg_risk_reconciliation_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if row is None or row["status"] != "open":
                raise ValueError("reconciliation-window-not-open")
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
            if window["next_cursor"] != requested_cursor:
                raise ValueError("reconciliation-cursor-race")
            if started_at_ms < int(window["checkpoint_at_ms"]):
                raise ValueError("reconciliation-clock-regression")
            prior_receipts = con.execute(
                "SELECT requested_cursor,next_cursor "
                "FROM neg_risk_reconciliation_batches WHERE window_id=?",
                (window_id,),
            ).fetchall()
            seen_cursors = {
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
            self._validate_reconciliation_snapshot(
                window,
                receipts,
                batch_samples,
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
            self._validate_reconciliation_snapshot(
                applied,
                receipts,
                batch_samples,
                staged,
                baseline,
                evidence,
                result_revisions,
            )
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
            self._insert_validated_quote_batch(con, batch)
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
            self._insert_validated_quote_batch(con, batch)
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
            self._admit_waiting_candidates(con, now_ms=observed_at_ms)
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
            row = self._current_quote_row(con, group_id, now_ms, max_age_ms)
            if row is None:
                return None
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
                )
                self._admit_waiting_candidates(con, now_ms=started_at_ms)
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
            receipt.component not in {"candidate", "discovery", "reconciliation"}
            or receipt.attempt < 1
            or receipt.started_at_ms < 0
            or receipt.finished_at_ms < receipt.started_at_ms
            or receipt.outcome not in {"success", "nonzero", "timeout", "cancelled", "spawn-error"}
            or len(receipt.stdout_tail.encode()) > 16_384
            or len(receipt.stderr_tail.encode()) > 16_384
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
                "exit_code,stdout_tail,stderr_tail,supervisor_run_id,child_nonce,"
                "auth_domain,child_auth_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    receipt.component,
                    receipt.attempt,
                    receipt.started_at_ms,
                    receipt.finished_at_ms,
                    receipt.outcome,
                    receipt.exit_code,
                    receipt.stdout_tail,
                    receipt.stderr_tail,
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
        if component not in {"candidate", "discovery", "reconciliation"}:
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
            component not in {"candidate", "discovery", "reconciliation"}
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
            component not in {"candidate", "discovery", "reconciliation"}
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
        if component not in {"candidate", "discovery", "reconciliation"}:
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
            component not in {"candidate", "discovery", "reconciliation"}
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
        if component not in {"candidate", "discovery", "reconciliation"}:
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
                validate_resource_history,
            )

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
                validate_resource_history,
            )

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

    def _connect(self) -> sqlite3.Connection:
        target = (
            f"file:{self._db_path.resolve()}?mode=ro" if self._read_only else str(self._db_path)
        )
        con = sqlite3.connect(
            target,
            uri=self._read_only,
            isolation_level=None,
            timeout=_BUSY_TIMEOUT_MS / 1_000,
        )
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        return con

    def _insert_validated_quote_batch(
        self,
        con: sqlite3.Connection,
        batch: GroupQuoteBatch,
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

    @staticmethod
    def _insert_candidate_watch_fact(
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
    ) -> CandidateWatchFact:
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

    @staticmethod
    def _insert_group_revision(
        con: sqlite3.Connection,
        revision: GroupRevision,
        current_row: sqlite3.Row | None,
    ) -> None:
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
        if current_row is not None and (
            current_row["membership_hash"] != revision.membership_hash
            or revision.status != "certified"
        ):
            con.execute(
                "UPDATE neg_risk_group_quote_batches SET status='superseded' "
                "WHERE group_id=? AND status='complete'",
                (revision.group_id,),
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
    ) -> None:
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
                row, staged, baseline, evidence, result_revisions
            )
        elif evidence or result_revisions:
            raise ValueError("invalid-reconciliation-premature-diff-evidence")
        seen_cursors: set[str] = set()
        for sequence, receipt in enumerate(receipts, start=1):
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
