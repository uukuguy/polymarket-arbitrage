"""SQLite authority for certified groups and atomic all-leg quote batches."""

from __future__ import annotations

import json
import math
import sqlite3
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


@dataclass(frozen=True)
class DurableCandidateFreshness:
    candidate_count: int
    quote_p95_age_ms: int | None
    missing_quote_count: int


@dataclass(frozen=True)
class DiscoveryLoadState:
    degraded_streak: int
    last_reason: str | None
    last_decision: Literal["fresh", "yield", "probe"]
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
                "id,degraded_streak,last_reason,last_decision,updated_at_ms"
                ") VALUES (1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "degraded_streak=excluded.degraded_streak,"
                "last_reason=excluded.last_reason,"
                "last_decision=excluded.last_decision,"
                "updated_at_ms=excluded.updated_at_ms",
                (streak, degraded_reason, decision, now_ms),
            )
            con.execute("COMMIT")
            return DiscoveryLoadState(streak, degraded_reason, decision, now_ms)
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
                "SELECT degraded_streak,last_reason,last_decision,updated_at_ms "
                "FROM neg_risk_discovery_load_state WHERE id=1"
            ).fetchone()
        finally:
            con.close()
        if row is None:
            return DiscoveryLoadState(0, None, "fresh", 0)
        return DiscoveryLoadState(
            int(row["degraded_streak"]),
            None if row["last_reason"] is None else str(row["last_reason"]),
            row["last_decision"],
            int(row["updated_at_ms"]),
        )

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
    ) -> tuple[str, ...]:
        """Atomically certify, schedule, sample, promote, and advance a page."""
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

            promoted: list[tuple[Decimal, str]] = []
            for candidate in candidates:
                schedule = self._insert_discovery_schedule(
                    con,
                    candidate=candidate,
                    source_cursor=requested_cursor,
                    started_at_ms=started_at_ms,
                    finished_at_ms=finished_at_ms,
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
                if schedule.promoted_at_ms is not None:
                    promoted.append((schedule.priority_score, schedule.group_id))

            promoted.sort(key=lambda item: (-item[0], item[1]))
            promoted_ids = {group_id for _, group_id in promoted}
            receipt = con.execute(
                "INSERT INTO neg_risk_discovery_batches("
                "requested_cursor,next_cursor,completed,started_at_ms,"
                "finished_at_ms,page_event_count,groups_seen,promoted_count"
                ") VALUES (?,?,?,?,?,?,?,?)",
                (
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
                "batch_id,group_id,liquidity_weight,promoted"
                ") VALUES (?,?,?,?)",
                [
                    (
                        batch_id,
                        candidate.group_id,
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

    def _insert_discovery_schedule(
        self,
        con: sqlite3.Connection,
        *,
        candidate: DiscoveryScheduleCandidate,
        source_cursor: str | None,
        started_at_ms: int,
        finished_at_ms: int,
    ) -> GroupSchedule:
        prior = con.execute(
            "SELECT * FROM neg_risk_group_schedule WHERE group_id=?",
            (candidate.group_id,),
        ).fetchone()
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
            finished_at_ms
            if can_promote and (prior is None or prior["promoted_at_ms"] is None)
            else (
                int(prior["promoted_at_ms"])
                if can_promote and prior is not None
                else None
            )
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
            "promoted_at_ms"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
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
            "promoted_at_ms=excluded.promoted_at_ms",
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
            latest_batch = con.execute(
                "SELECT * FROM neg_risk_discovery_batches ORDER BY id DESC LIMIT 1"
            ).fetchone()
            latest_samples = (
                []
                if latest_batch is None
                else con.execute(
                    "SELECT * FROM neg_risk_discovery_batch_samples "
                    "WHERE batch_id=? ORDER BY group_id",
                    (latest_batch["id"],),
                ).fetchall()
            )
            load_row = con.execute(
                "SELECT degraded_streak,last_reason,last_decision,updated_at_ms "
                "FROM neg_risk_discovery_load_state WHERE id=1"
            ).fetchone()
            coverage = self._coverage_windows_in_snapshot(con, now_ms)
            self._validate_discovery_snapshot(
                state=state,
                schedules=schedules,
                current_revisions=current_revisions,
                latest_batch=latest_batch,
                latest_samples=latest_samples,
                load_row=load_row,
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
                DiscoveryLoadState(0, None, "fresh", 0)
                if load_row is None
                else DiscoveryLoadState(
                    int(load_row["degraded_streak"]),
                    (
                        None
                        if load_row["last_reason"] is None
                        else str(load_row["last_reason"])
                    ),
                    load_row["last_decision"],
                    int(load_row["updated_at_ms"]),
                )
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
        latest_batch: sqlite3.Row | None,
        latest_samples: list[sqlite3.Row],
        load_row: sqlite3.Row | None,
        coverage: CoverageWindows,
    ) -> None:
        if load_row is not None:
            streak = int(load_row["degraded_streak"])
            reason = load_row["last_reason"]
            decision = load_row["last_decision"]
            if (
                int(load_row["updated_at_ms"]) < 0
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
        schedules_by_id = {str(row["group_id"]): row for row in schedules}
        for sample in latest_samples:
            schedule = schedules_by_id.get(str(sample["group_id"]))
            if (
                schedule is None
                or Decimal(str(sample["liquidity_weight"]))
                != Decimal(str(schedule["liquidity_weight"]))
                or bool(sample["promoted"])
                != (schedule["promoted_at_ms"] is not None)
            ):
                raise ValueError("invalid-discovery-receipt-sample")
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
            if row["promoted_at_ms"] is not None:
                revision = current_revisions.get(str(row["group_id"]))
                if (
                    row["quality"] != "complete-supported"
                    or revision is None
                    or revision["status"] != "certified"
                    or revision["event_id"] != row["event_id"]
                    or revision["membership_hash"] != row["membership_hash"]
                ):
                    raise ValueError("invalid-discovery-promotion-authority")
            else:
                revision = current_revisions.get(str(row["group_id"]))
                if row["quality"] == "complete-supported":
                    raise ValueError("supported-discovery-schedule-not-promoted")
                if revision is not None and (
                    revision["event_id"] != row["event_id"]
                    or (
                        revision["status"] != "invalidated"
                    )
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
                    and not int(row["first_discovered_at_ms"])
                    <= int(row["promoted_at_ms"])
                    <= int(row["last_discovered_at_ms"])
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
                "FROM current c WHERE c.status='certified' ORDER BY c.group_id",
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
            return self._insert_candidate_watch_fact(
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
