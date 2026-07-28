"""SQLite authority for certified groups and atomic all-leg quote batches."""

from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
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
            + (
                self.high_burst_groups
                + self.effective_capacity
                - 1
            )
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


class OpportunityPerceptionStore:
    """Transactional opportunity-first perception read model."""

    def __init__(self, db_path: Path, *, read_only: bool = False) -> None:
        self._db_path = Path(db_path)
        self._read_only = read_only
        if not read_only:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def init_schema(self) -> None:
        con = self._connect()
        try:
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
            )
            for table, column, definition in migrations:
                existing = {
                    str(row["name"])
                    for row in con.execute(f"PRAGMA table_info({table})")
                }
                if column not in existing:
                    con.execute(
                        f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
                    )
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
                raise ValueError(
                    "discovery-admission-timing-change-with-outstanding-work"
                )
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
                None
                if state is None or bool(state["completed"])
                else state["next_cursor"]
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
            if (
                configured is None
                or self._admission_proof_from_row(configured)
                != admission_proof
            ):
                raise ValueError("discovery-admission-proof-not-configured")

            for candidate in candidates:
                self._insert_discovery_schedule(
                    con,
                    candidate=candidate,
                    source_cursor=requested_cursor,
                    started_at_ms=started_at_ms,
                    finished_at_ms=finished_at_ms,
                    candidate_max_wait_ms=(
                        admission_proof.candidate_max_wait_ms
                    ),
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
            promoted = [
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
            ] if candidates else []
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
            attempt_start_write_budget_ms=int(
                row["attempt_start_write_budget_ms"]
            ),
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
            "SELECT s.group_id FROM neg_risk_group_schedule s "
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
        con.executemany(
            "UPDATE neg_risk_group_schedule SET promoted_at_ms=?,"
            "candidate_start_deadline_at_ms=? WHERE group_id=?",
            [
                (
                    now_ms,
                    now_ms + candidate_max_wait_ms,
                    str(row["group_id"]),
                )
                for row in queued
            ],
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
        if (
            current_authority is not None
            and current_authority["event_id"] != candidate.event_id
        ):
            raise ValueError("discovery-group-event-identity-conflict")
        last_fact = con.execute(
            "SELECT observed_at_ms,gross_edge_bps "
            "FROM neg_risk_candidate_watch_facts WHERE group_id=? "
            "ORDER BY id DESC LIMIT 1",
            (candidate.group_id,),
        ).fetchone()
        first_discovered_at_ms = (
            finished_at_ms
            if prior is None
            else int(prior["first_discovered_at_ms"])
        )
        last_visited_at_ms = (
            int(last_fact["observed_at_ms"])
            if last_fact is not None
            else (
                None if prior is None or prior["last_visited_at_ms"] is None
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

        can_promote = (
            candidate.quality == "complete-supported"
            and candidate.legs is not None
        )
        promoted_at_ms = (
            int(prior["promoted_at_ms"])
            if can_promote
            and prior is not None
            and prior["promoted_at_ms"] is not None
            else None
        )
        promotion_eligible_at_ms = (
            (
                int(prior["promotion_eligible_at_ms"])
                if prior is not None
                and prior["promotion_eligible_at_ms"] is not None
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
            state = con.execute(
                "SELECT * FROM neg_risk_discovery_state WHERE id=1"
            ).fetchone()
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
                    "FROM neg_risk_group_revisions"
                    " GROUP BY group_id,event_id,membership_hash"
                ).fetchall()
            }
            batches = con.execute(
                "SELECT * FROM neg_risk_discovery_batches ORDER BY id"
            ).fetchall()
            latest_batch = batches[-1] if batches else None
            batch_samples = con.execute(
                "SELECT * FROM neg_risk_discovery_batch_samples "
                "ORDER BY batch_id,group_id"
            ).fetchall()
            latest_samples = [
                row
                for row in batch_samples
                if latest_batch is not None
                and int(row["batch_id"]) == int(latest_batch["id"])
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
                    "SELECT DISTINCT group_id "
                    "FROM neg_risk_candidate_watch_facts"
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
            coverage = self._coverage_windows_in_snapshot(con, now_ms)
            self._validate_discovery_snapshot(
                state=state,
                schedules=schedules,
                current_revisions=current_revisions,
                revision_identities=revision_identities,
                batches=batches,
                batch_samples=batch_samples,
                latest_batch=latest_batch,
                latest_samples=latest_samples,
                load_row=load_row,
                admission_row=admission_row,
                fact_group_ids=fact_group_ids,
                breach_fact_evidence=breach_fact_evidence,
                attempt_starts=attempt_starts,
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
        queue.update(
            {str(row["priority_class"]): int(row["depth"]) for row in queue_rows}
        )
        oldest_age = (
            None
            if oldest is None or oldest["oldest"] is None
            else max(0, now_ms - int(oldest["oldest"]))
        )
        return DiscoveryStatus(
            next_cursor=(
                None if state is None or state["next_cursor"] is None
                else str(state["next_cursor"])
            ),
            completed=False if state is None else bool(state["completed"]),
            last_started_at_ms=(
                None if state is None else int(state["last_started_at_ms"])
            ),
            last_finished_at_ms=(
                None if state is None else int(state["last_finished_at_ms"])
            ),
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
                    (
                        None
                        if load_row["last_reason"] is None
                        else str(load_row["last_reason"])
                    ),
                    load_row["last_decision"],
                    int(load_row["probe_every_cycles"]),
                    int(load_row["updated_at_ms"]),
                )
            ),
            admission_proof=(
                None
                if admission_row is None
                else self._admission_proof_from_row(admission_row)
            ),
            promotion_queue_depth=sum(
                1
                for row in schedules
                if row["quality"] == "complete-supported"
                and row["promoted_at_ms"] is None
            ),
            outstanding_admitted_count=sum(
                1
                for row in schedules
                if row["promoted_at_ms"] is not None
                and str(row["group_id"]) not in fact_group_ids
            ),
            candidate_attempt_start_count=len(attempt_starts),
            candidate_start_deadline_breach_count=sum(
                int(row["deadline_breached"]) for row in attempt_starts
            ),
            candidate_start_ready=not any(
                bool(row["deadline_breached"]) for row in attempt_starts
            ),
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
                    visited_weight / total_weight
                    if total_weight > 0
                    else Decimal("0")
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
        latest_batch: sqlite3.Row | None,
        latest_samples: list[sqlite3.Row],
        load_row: sqlite3.Row | None,
        admission_row: sqlite3.Row | None,
        fact_group_ids: set[str],
        breach_fact_evidence: set[tuple[str, int]],
        attempt_starts: list[sqlite3.Row],
        coverage: CoverageWindows,
    ) -> None:
        if load_row is not None:
            streak = int(load_row["degraded_streak"])
            reason = load_row["last_reason"]
            decision = load_row["last_decision"]
            modulus = int(load_row["probe_every_cycles"])
            expected_decision = (
                "fresh"
                if reason is None
                else ("probe" if streak % modulus == 0 else "yield")
            )
            if (
                int(load_row["updated_at_ms"]) < 0
                or modulus < 2
                or decision != expected_decision
                or (
                    decision == "fresh"
                    and (streak != 0 or reason is not None)
                )
                or (
                    decision in {"yield", "probe"}
                    and (
                        streak <= 0
                        or reason
                        not in {"candidate-quote-missing", "candidate-quote-stale"}
                    )
                )
            ):
                raise ValueError("invalid-discovery-load-state")
        previous: sqlite3.Row | None = None
        samples_by_batch: dict[int, list[sqlite3.Row]] = {}
        for sample in batch_samples:
            samples_by_batch.setdefault(int(sample["batch_id"]), []).append(
                sample
            )
        for batch in batches:
            counts = (
                int(batch["promoted_count"]),
                int(batch["groups_seen"]),
                int(batch["page_event_count"]),
            )
            if (
                int(batch["sweep_id"]) < 1
                or int(batch["batch_sequence"]) < 1
                or bool(batch["completed"])
                != (batch["next_cursor"] is None)
                or int(batch["started_at_ms"]) < 0
                or int(batch["finished_at_ms"]) < 0
                or int(batch["started_at_ms"]) > int(batch["finished_at_ms"])
                or not 0 <= counts[0] <= counts[1] <= counts[2]
            ):
                raise ValueError("invalid-discovery-batch-receipt")
            if previous is None:
                if (
                    int(batch["sweep_id"]) != 1
                    or int(batch["batch_sequence"]) != 1
                ):
                    raise ValueError("invalid-discovery-batch-sequence")
            elif bool(previous["completed"]):
                if (
                    batch["requested_cursor"] is not None
                    or int(batch["sweep_id"])
                    != int(previous["sweep_id"]) + 1
                    or int(batch["batch_sequence"]) != 1
                ):
                    raise ValueError("invalid-discovery-sweep-transition")
            elif (
                batch["requested_cursor"] != previous["next_cursor"]
                or int(batch["sweep_id"]) != int(previous["sweep_id"])
                or int(batch["batch_sequence"])
                != int(previous["batch_sequence"]) + 1
            ):
                raise ValueError("invalid-discovery-cursor-receipt-chain")
            samples = samples_by_batch.pop(int(batch["id"]), [])
            if (
                len(samples) != int(batch["groups_seen"])
                or sum(int(row["promoted"]) for row in samples)
                != int(batch["promoted_count"])
            ):
                raise ValueError("invalid-discovery-historical-sample-count")
            for sample in samples:
                try:
                    weight = Decimal(str(sample["liquidity_weight"]))
                except Exception as error:
                    raise ValueError(
                        "invalid-discovery-historical-sample"
                    ) from error
                quality = sample["quality"]
                reason = sample["reason"]
                identity = (
                    str(sample["group_id"]),
                    str(sample["event_id"]),
                    str(sample["membership_hash"]),
                )
                if (
                    not weight.is_finite()
                    or weight < 0
                    or quality
                    not in {
                        "complete-supported",
                        "complete-unsupported",
                        "incomplete-source",
                    }
                    or identity not in revision_identities
                    or revision_identities[identity]
                    > int(batch["finished_at_ms"])
                    or (
                        bool(sample["promoted"])
                        and quality != "complete-supported"
                    )
                    or (
                        quality == "complete-supported"
                        and reason is not None
                    )
                    or (
                        quality != "complete-supported"
                        and (reason is None or not str(reason))
                    )
                ):
                    raise ValueError("invalid-discovery-historical-sample")
            previous = batch
        if samples_by_batch:
            raise ValueError("orphan-discovery-batch-sample")
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
                or int(attempt["candidate_start_deadline_at_ms"])
                != int(attempt["promoted_at_ms"])
                + int(attempt["candidate_max_wait_ms"])
                or bool(attempt["deadline_breached"]) != (
                int(attempt["started_at_ms"])
                > int(attempt["candidate_start_deadline_at_ms"])
                )
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
            admission_proof = (
                OpportunityPerceptionStore._admission_proof_from_row(
                    admission_row
                )
            )
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
                    != int(row["promotion_eligible_at_ms"])
                    + admission_proof.candidate_max_wait_ms
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
                                and int(
                                    row["candidate_start_deadline_at_ms"]
                                )
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
                        or revision["membership_hash"]
                        != row["membership_hash"]
                        or (
                            admission_proof is not None
                            and (
                                row["promotion_eligible_at_ms"] is None
                                or row["promotion_queue_deadline_at_ms"] is None
                                or row["candidate_start_deadline_at_ms"]
                                is not None
                            )
                        )
                    ):
                        raise ValueError(
                            "invalid-discovery-queued-promotion-authority"
                        )
                elif revision is not None and (
                    revision["event_id"] != row["event_id"]
                    or revision["status"] != "invalidated"
                ):
                    raise ValueError("invalid-discovery-unpromoted-authority")
            if (
                int(row["first_discovered_at_ms"])
                > int(row["last_discovered_at_ms"])
                or (
                    row["last_visited_at_ms"] is not None
                    and int(row["last_visited_at_ms"])
                    > int(row["last_discovered_at_ms"])
                )
                or (
                    row["promoted_at_ms"] is not None
                    and (
                        int(row["promoted_at_ms"])
                        < int(row["first_discovered_at_ms"])
                        or (
                            row["promotion_eligible_at_ms"] is not None
                            and int(row["promotion_eligible_at_ms"])
                            > int(row["promoted_at_ms"])
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
                if row["promoted_at_ms"] is not None
                and str(row["group_id"]) not in fact_group_ids
            )
            if outstanding > admission_proof.effective_capacity:
                raise ValueError("discovery-admission-capacity-exceeded")
        for window in coverage.by_minutes.values():
            if (
                window.visited_groups > coverage.known_groups
                or not Decimal("0") <= window.raw_fraction <= Decimal("1")
                or not Decimal("0")
                <= window.liquidity_weighted_fraction
                <= Decimal("1")
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
            now_ms - int(row["quoted_at_ms"])
            for row in rows
            if row["quoted_at_ms"] is not None
        ]
        missing = len(rows) - len(ages)
        ages.sort()
        p95 = (
            None
            if not ages
            else ages[max(0, math.ceil(len(ages) * 0.95) - 1)]
        )
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
                None if row["last_visited_at_ms"] is None
                else int(row["last_visited_at_ms"])
            ),
            promoted_at_ms=(
                None if row["promoted_at_ms"] is None
                else int(row["promoted_at_ms"])
            ),
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

    def publish_group_revision(self, revision: GroupRevision) -> GroupRevision:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            if (
                GroupRevision.membership_digest(revision.legs)
                != revision.membership_hash
            ):
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
            if (
                current_row is not None
                and current_row["membership_hash"] != revision.membership_hash
            ):
                con.execute(
                    "UPDATE neg_risk_group_quote_batches "
                    "SET status='superseded' "
                    "WHERE group_id=? AND status='complete'",
                    (revision.group_id,),
                )
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
                "SELECT * FROM neg_risk_group_schedule "
                f"WHERE group_id IN ({placeholders})",
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
            str(row["group_id"]): self._candidate_watch_fact_from_row(row)
            for row in fact_rows
        }
        schedules = {
            str(row["group_id"]): self._group_schedule_from_row(row)
            for row in schedule_rows
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
                "SELECT candidate_max_wait_ms "
                "FROM neg_risk_discovery_admission_state WHERE id=1"
            ).fetchone()
            if (
                proof is None
                or deadline
                != admission.promoted_at_ms + int(proof["candidate_max_wait_ms"])
            ):
                raise ValueError("candidate-attempt-start-deadline-mismatch")
            candidate_max_wait_ms = int(proof["candidate_max_wait_ms"])
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

    def _connect(self) -> sqlite3.Connection:
        target = (
            f"file:{self._db_path.resolve()}?mode=ro"
            if self._read_only
            else str(self._db_path)
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
        if any(
            leg.membership_hash != current.membership_hash for leg in batch.legs
        ):
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
    def _current_group_row(
        con: sqlite3.Connection, group_id: str
    ) -> sqlite3.Row | None:
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
    def _group_legs_json(legs: tuple[GroupLeg, ...]) -> str:
        return json.dumps(
            [
                [leg.market_id, leg.condition_id, leg.yes_token_id, leg.title]
                for leg in legs
            ],
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
    def _group_from_row(
        row: sqlite3.Row, *, prefix: str = ""
    ) -> GroupRevision:
        return GroupRevision(
            group_id=row[f"{prefix}group_id"],
            event_id=row[f"{prefix}event_id"],
            revision=row[f"{prefix}revision"],
            membership_hash=row[f"{prefix}membership_hash"],
            started_at_ms=row[f"{prefix}started_at_ms"],
            observed_at_ms=row[f"{prefix}observed_at_ms"],
            source_cursor=row[f"{prefix}source_cursor"],
            status=row[f"{prefix}status"],
            legs=tuple(
                GroupLeg(*leg) for leg in json.loads(row[f"{prefix}legs_json"])
            ),
        )

    @staticmethod
    def _quote_batch_from_row(
        row: sqlite3.Row, *, prefix: str = ""
    ) -> GroupQuoteBatch:
        return GroupQuoteBatch(
            group_id=row[f"{prefix}group_id"],
            membership_hash=row[f"{prefix}membership_hash"],
            quote_batch_id=row[f"{prefix}id"],
            started_at_ms=row[f"{prefix}started_at_ms"],
            quoted_at_ms=row[f"{prefix}quoted_at_ms"],
            status=row[f"{prefix}status"],
            failure_reason=row[f"{prefix}failure_reason"],
            legs=tuple(
                GroupQuoteLeg(*leg)
                for leg in json.loads(row[f"{prefix}legs_json"])
            ),
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
            if (
                GroupRevision.membership_digest(group.legs)
                != group.membership_hash
            ):
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
