"""Durable deterministic resource shedding for opportunity-first producers."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
from dataclasses import asdict, dataclass, replace
from typing import Any

from polyarb.perception.store import OpportunityPerceptionStore

RESOURCE_POLICY_VERSION = "opportunity-resource-v1"
RESOURCE_SUFFIX_COMPACT_HIGH_PAIRS = 512
RESOURCE_SUFFIX_COMPACT_LOW_PAIRS = 256
RESOURCE_SUFFIX_HARD_MAX_PAIRS = 1_024
_ZERO_DIGEST = "sha256:" + ("0" * 64)


@dataclass(frozen=True)
class ResourceSample:
    candidate_count: int
    candidate_quote_p95_ms: int | float | None
    candidate_missing_quote_count: int
    candidate_worker_ok: bool
    discovery_worker_ok: bool
    reconciliation_running: bool
    previous_discovery_batch_limit: int
    observed_at_ms: int

    def validate(self) -> None:
        numbers = (
            self.candidate_count,
            self.candidate_missing_quote_count,
            self.previous_discovery_batch_limit,
            self.observed_at_ms,
        )
        if (
            any(isinstance(value, bool) or value < 0 for value in numbers)
            or not 1 <= self.previous_discovery_batch_limit <= 100
            or self.candidate_missing_quote_count > self.candidate_count
            or (
                self.candidate_quote_p95_ms is not None
                and (
                    not math.isfinite(self.candidate_quote_p95_ms)
                    or self.candidate_quote_p95_ms < 0
                )
            )
        ):
            raise ValueError("invalid-resource-sample")


@dataclass(frozen=True)
class ResourceDecision:
    mode: str
    reason: str
    reconciliation_enabled: bool
    discovery_batch_limit: int
    discovery_duty_multiplier: float
    normal_candidate_interval_multiplier: float
    high_candidate_interval_multiplier: float
    http_preserved: bool
    health_claimed: bool
    previous_discovery_batch_limit: int
    decided_at_ms: int
    policy_version: str
    sequence: int
    source_sample_id: int
    hot_quote_age_ms: int
    cooldown_ms: int
    decision_ttl_ms: int
    valid_until_ms: int
    mode_changed_at_ms: int


@dataclass(frozen=True)
class ResourceHistoryItem:
    sample: ResourceSample
    decision: ResourceDecision


@dataclass(frozen=True)
class ResourceHistoryFloor:
    through_sample_id: int
    through_decision_id: int
    through_sequence: int
    compacted_sample_count: int
    compacted_decision_count: int


@dataclass(frozen=True)
class ResourceHistoryPage:
    current: ResourceDecision | None
    items: tuple[ResourceHistoryItem, ...]
    next_before_sequence: int | None
    history_floor: ResourceHistoryFloor | None


@dataclass(frozen=True)
class _ResourceAuthorityState:
    checkpoint: sqlite3.Row | None
    floor_decision: ResourceDecision | None
    current_decision: ResourceDecision | None
    suffix_rows: tuple[sqlite3.Row, ...]
    final_digest: str


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _pair_digest(row: sqlite3.Row, previous_digest: str) -> str:
    return _digest(
        {
            "decided_at_ms": int(row["decided_at_ms"]),
            "decision_id": int(row["id"]),
            "decision_json": str(row["decision_json"]),
            "mode": str(row["mode"]),
            "observed_at_ms": int(row["observed_at_ms"]),
            "policy_version": str(row["policy_version"]),
            "previous_digest": previous_digest,
            "reason": str(row["reason"]),
            "sample_id": int(row["sample_id"]),
            "sample_json": str(row["sample_json"]),
            "sequence": int(row["sequence"]),
        }
    )


def _checkpoint_payload(
    *,
    generation: int,
    through_sample_id: int,
    through_decision_id: int,
    through_sequence: int,
    compacted_sample_count: int,
    compacted_decision_count: int,
    prefix_digest: str,
    last_decision_json: str | None,
    last_decision_digest: str,
) -> dict[str, Any]:
    return {
        "compacted_decision_count": compacted_decision_count,
        "compacted_sample_count": compacted_sample_count,
        "generation": generation,
        "last_decision_digest": last_decision_digest,
        "last_decision_json": last_decision_json,
        "prefix_digest": prefix_digest,
        "through_decision_id": through_decision_id,
        "through_sample_id": through_sample_id,
        "through_sequence": through_sequence,
    }


def _bounded_count(
    con: sqlite3.Connection,
    table_name: str,
) -> int:
    if table_name not in {
        "neg_risk_resource_samples",
        "neg_risk_resource_decisions",
    }:
        raise ValueError("invalid-resource-count-table")
    return int(
        con.execute(
            f'SELECT COUNT(*) FROM (SELECT 1 FROM "{table_name}" LIMIT ?)',
            (RESOURCE_SUFFIX_HARD_MAX_PAIRS + 1,),
        ).fetchone()[0]
    )


def _policy_decision(
    sample: ResourceSample,
    *,
    now_ms: int,
    hot_quote_age_ms: int,
    cooldown_ms: int,
    decision_ttl_ms: int,
    previous: ResourceDecision | None,
) -> ResourceDecision:
    unhealthy_hot_path = (
        not sample.candidate_worker_ok
        or sample.candidate_missing_quote_count > 0
        or (
            sample.candidate_count > 0
            and (
                sample.candidate_quote_p95_ms is None
                or sample.candidate_quote_p95_ms >= hot_quote_age_ms
            )
        )
    )
    common = {
        "policy_version": RESOURCE_POLICY_VERSION,
        "sequence": 0,
        "source_sample_id": 0,
        "hot_quote_age_ms": hot_quote_age_ms,
        "cooldown_ms": cooldown_ms,
        "decision_ttl_ms": decision_ttl_ms,
        "valid_until_ms": now_ms + decision_ttl_ms,
        "mode_changed_at_ms": (now_ms if previous is None else previous.mode_changed_at_ms),
        "previous_discovery_batch_limit": sample.previous_discovery_batch_limit,
        "decided_at_ms": now_ms,
        "high_candidate_interval_multiplier": 1.0,
        "http_preserved": True,
    }
    if unhealthy_hot_path:
        desired = ResourceDecision(
            mode="protect-hot-path",
            reason=("candidate-hot-path-pressure"),
            reconciliation_enabled=False,
            discovery_batch_limit=max(1, sample.previous_discovery_batch_limit // 2),
            discovery_duty_multiplier=0.25,
            normal_candidate_interval_multiplier=2.0,
            health_claimed=False,
            **common,
        )
    elif sample.candidate_count == 0:
        desired = ResourceDecision(
            mode="empty-candidate-exploration",
            reason="empty-candidate-exploration",
            reconciliation_enabled=False,
            discovery_batch_limit=min(
                100,
                max(
                    sample.previous_discovery_batch_limit + 1,
                    sample.previous_discovery_batch_limit * 2,
                ),
            ),
            discovery_duty_multiplier=1.5,
            normal_candidate_interval_multiplier=1.0,
            health_claimed=False,
            **common,
        )
    else:
        desired = ResourceDecision(
            mode="normal",
            reason="candidate-hot-path-fresh",
            reconciliation_enabled=sample.reconciliation_running,
            discovery_batch_limit=sample.previous_discovery_batch_limit,
            discovery_duty_multiplier=1.0,
            normal_candidate_interval_multiplier=1.0,
            health_claimed=True,
            **common,
        )
    if (
        previous is not None
        and previous.mode == "protect-hot-path"
        and desired.mode != "protect-hot-path"
        and now_ms - previous.mode_changed_at_ms < cooldown_ms
    ):
        desired = replace(
            previous,
            reason="hysteresis-cooldown",
            previous_discovery_batch_limit=sample.previous_discovery_batch_limit,
            decided_at_ms=now_ms,
            policy_version=RESOURCE_POLICY_VERSION,
            sequence=0,
            source_sample_id=0,
            hot_quote_age_ms=hot_quote_age_ms,
            cooldown_ms=cooldown_ms,
            decision_ttl_ms=decision_ttl_ms,
            valid_until_ms=now_ms + decision_ttl_ms,
            mode_changed_at_ms=previous.mode_changed_at_ms,
        )
    elif previous is not None and desired.mode != previous.mode:
        desired = replace(desired, mode_changed_at_ms=now_ms)
    return desired


def _validate_resource_authority(
    con: sqlite3.Connection,
) -> _ResourceAuthorityState:
    """Validate the authenticated floor and replay a bounded retained suffix."""
    con.row_factory = sqlite3.Row
    checkpoint = con.execute(
        "SELECT * FROM neg_risk_resource_authority_checkpoint WHERE id=1"
    ).fetchone()
    floor_decision: ResourceDecision | None = None
    through_sample_id = 0
    through_decision_id = 0
    through_sequence = 0
    prefix_digest = _ZERO_DIGEST
    if checkpoint is not None:
        try:
            through_sample_id = int(checkpoint["through_sample_id"])
            through_decision_id = int(checkpoint["through_decision_id"])
            through_sequence = int(checkpoint["through_sequence"])
            compacted_sample_count = int(checkpoint["compacted_sample_count"])
            compacted_decision_count = int(checkpoint["compacted_decision_count"])
            prefix_digest = str(checkpoint["prefix_digest"])
            last_decision_json = checkpoint["last_decision_json"]
            if through_sequence == 0:
                if (
                    through_sample_id != 0
                    or through_decision_id != 0
                    or compacted_sample_count != 0
                    or compacted_decision_count != 0
                    or last_decision_json is not None
                    or prefix_digest != _ZERO_DIGEST
                ):
                    raise ValueError
            else:
                if last_decision_json is None:
                    raise ValueError
                decision_raw = json.loads(str(last_decision_json))
                if not isinstance(decision_raw, dict):
                    raise ValueError
                floor_decision = ResourceDecision(**decision_raw)
                if (
                    floor_decision.sequence != through_sequence
                    or floor_decision.source_sample_id != through_sample_id
                    or compacted_sample_count != through_sequence
                    or compacted_decision_count != through_sequence
                ):
                    raise ValueError
            payload = _checkpoint_payload(
                generation=int(checkpoint["generation"]),
                through_sample_id=through_sample_id,
                through_decision_id=through_decision_id,
                through_sequence=through_sequence,
                compacted_sample_count=compacted_sample_count,
                compacted_decision_count=compacted_decision_count,
                prefix_digest=prefix_digest,
                last_decision_json=(
                    None if last_decision_json is None else str(last_decision_json)
                ),
                last_decision_digest=str(checkpoint["last_decision_digest"]),
            )
            if (
                int(checkpoint["generation"]) < 1
                or str(checkpoint["checkpoint_hash"]) != _digest(payload)
            ):
                raise ValueError
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as error:
            raise ValueError("invalid-resource-checkpoint") from error

    sample_count = _bounded_count(con, "neg_risk_resource_samples")
    decision_count = _bounded_count(con, "neg_risk_resource_decisions")
    if (
        sample_count > RESOURCE_SUFFIX_HARD_MAX_PAIRS
        or decision_count > RESOURCE_SUFFIX_HARD_MAX_PAIRS
    ):
        raise ValueError("resource-history-hard-limit")
    rows = con.execute(
        "SELECT d.*,s.id source_row_id,s.observed_at_ms,s.sample_json "
        "FROM neg_risk_resource_decisions d "
        "JOIN neg_risk_resource_samples s ON s.id=d.sample_id "
        "ORDER BY d.sequence LIMIT ?",
        (RESOURCE_SUFFIX_HARD_MAX_PAIRS + 1,),
    ).fetchall()
    if sample_count != decision_count or len(rows) != sample_count:
        raise ValueError("invalid-resource-history")
    previous = floor_decision
    previous_digest = prefix_digest
    for expected_sequence, row in enumerate(rows, start=through_sequence + 1):
        try:
            sample_raw = json.loads(row["sample_json"])
            decision_raw = json.loads(row["decision_json"])
            if not isinstance(sample_raw, dict) or not isinstance(decision_raw, dict):
                raise ValueError
            sample = ResourceSample(**sample_raw)
            sample.validate()
            decision = ResourceDecision(**decision_raw)
        except (TypeError, ValueError, KeyError) as error:
            raise ValueError("invalid-resource-history") from error
        if (
            row["sample_id"] != row["source_row_id"]
            or int(row["sample_id"]) <= through_sample_id
            or int(row["id"]) <= through_decision_id
            or decision.source_sample_id != row["sample_id"]
            or decision.sequence != expected_sequence
            or row["sequence"] != expected_sequence
            or row["policy_version"] != RESOURCE_POLICY_VERSION
            or decision.policy_version != RESOURCE_POLICY_VERSION
            or decision.mode != row["mode"]
            or decision.reason != row["reason"]
            or decision.decided_at_ms != row["decided_at_ms"]
            or sample.observed_at_ms != row["observed_at_ms"]
            or decision.decided_at_ms < sample.observed_at_ms
            or (previous is not None and decision.decided_at_ms < previous.decided_at_ms)
            or decision.hot_quote_age_ms <= 0
            or decision.cooldown_ms < 0
            or decision.decision_ttl_ms <= 0
            or decision.valid_until_ms != decision.decided_at_ms + decision.decision_ttl_ms
            or decision.mode_changed_at_ms > decision.decided_at_ms
            or (
                previous is not None
                and decision.mode == previous.mode
                and decision.mode_changed_at_ms != previous.mode_changed_at_ms
            )
            or (
                previous is not None
                and decision.mode != previous.mode
                and decision.mode_changed_at_ms != decision.decided_at_ms
            )
        ):
            raise ValueError("invalid-resource-history")
        expected = replace(
            _policy_decision(
                sample,
                now_ms=decision.decided_at_ms,
                hot_quote_age_ms=decision.hot_quote_age_ms,
                cooldown_ms=decision.cooldown_ms,
                decision_ttl_ms=decision.decision_ttl_ms,
                previous=previous,
            ),
            sequence=expected_sequence,
            source_sample_id=row["sample_id"],
        )
        if decision != expected:
            raise ValueError("invalid-resource-history")
        previous = decision
        previous_digest = _pair_digest(row, previous_digest)
    if checkpoint is not None and previous_digest != str(
        checkpoint["last_decision_digest"]
    ):
        raise ValueError("invalid-resource-history")
    return _ResourceAuthorityState(
        checkpoint=checkpoint,
        floor_decision=floor_decision,
        current_decision=previous,
        suffix_rows=tuple(rows),
        final_digest=previous_digest,
    )


def validate_resource_history(con: sqlite3.Connection) -> ResourceDecision | None:
    validate_resource_evidence_failure(con, require_resolved=True)
    return _validate_resource_authority(con).current_decision


def resource_history_page(
    con: sqlite3.Connection,
    *,
    limit: int,
    before_sequence: int | None = None,
) -> ResourceHistoryPage:
    if (
        not 1 <= limit <= 500
        or (
            before_sequence is not None
            and (type(before_sequence) is not int or before_sequence <= 0)
        )
    ):
        raise ValueError("invalid-resource-history-page")
    state = _validate_resource_authority(con)
    matching = tuple(
        row
        for row in reversed(state.suffix_rows)
        if before_sequence is None or int(row["sequence"]) < before_sequence
    )
    page_rows = matching[: limit + 1]
    items = []
    for row in page_rows[:limit]:
        try:
            sample_raw = json.loads(str(row["sample_json"]))
            decision_raw = json.loads(str(row["decision_json"]))
            if not isinstance(sample_raw, dict) or not isinstance(
                decision_raw,
                dict,
            ):
                raise ValueError
            items.append(
                ResourceHistoryItem(
                    sample=ResourceSample(**sample_raw),
                    decision=ResourceDecision(**decision_raw),
                )
            )
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as error:
            raise ValueError("invalid-resource-history") from error
    checkpoint = state.checkpoint
    return ResourceHistoryPage(
        current=state.current_decision,
        items=tuple(items),
        next_before_sequence=(
            None
            if len(page_rows) <= limit
            else int(page_rows[limit - 1]["sequence"])
        ),
        history_floor=(
            None
            if checkpoint is None or int(checkpoint["through_sequence"]) == 0
            else ResourceHistoryFloor(
                through_sample_id=int(checkpoint["through_sample_id"]),
                through_decision_id=int(checkpoint["through_decision_id"]),
                through_sequence=int(checkpoint["through_sequence"]),
                compacted_sample_count=int(checkpoint["compacted_sample_count"]),
                compacted_decision_count=int(
                    checkpoint["compacted_decision_count"]
                ),
            )
        ),
    )


def validate_resource_evidence_failure(
    con: sqlite3.Connection,
    *,
    require_resolved: bool,
) -> sqlite3.Row | None:
    row = con.execute(
        "SELECT * FROM neg_risk_evidence_failures WHERE component='resource'"
    ).fetchone()
    if row is None:
        return None
    payload = {
        "component": "resource",
        "failed_at_ms": int(row["failed_at_ms"]),
        "reason": str(row["reason"]),
        "recovered_at_ms": row["recovered_at_ms"],
    }
    if (
        str(row["row_hash"]) != _digest(payload)
        or row["reason"] != "authority-invalid"
        or (
            row["recovered_at_ms"] is not None
            and int(row["recovered_at_ms"]) < int(row["failed_at_ms"])
        )
    ):
        raise ValueError("invalid-resource-evidence-failure")
    if require_resolved and row["recovered_at_ms"] is None:
        raise ValueError("unresolved-resource-evidence-failure")
    return row


def _record_resource_evidence_failure(
    store: OpportunityPerceptionStore,
    failed_at_ms: int,
) -> None:
    con = store._connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        existing = con.execute(
            "SELECT 1 FROM neg_risk_evidence_failures "
            "WHERE component='resource'"
        ).fetchone()
        payload = {
            "component": "resource",
            "failed_at_ms": failed_at_ms,
            "reason": "authority-invalid",
            "recovered_at_ms": None,
        }
        operation = "INSERT" if existing is None else "UPDATE"
        writer_token = store._begin_expected_owner_mutation(
            con,
            table_name="neg_risk_evidence_failures",
            operation=operation,
            row_key="resource",
        )
        con.execute(
            "INSERT INTO neg_risk_evidence_failures("
            "component,failed_at_ms,reason,recovered_at_ms,row_hash"
            ") VALUES('resource',?,'authority-invalid',NULL,?) "
            "ON CONFLICT(component) DO UPDATE SET "
            "failed_at_ms=excluded.failed_at_ms,"
            "reason=excluded.reason,recovered_at_ms=NULL,"
            "row_hash=excluded.row_hash",
            (failed_at_ms, _digest(payload)),
        )
        store._consume_expected_owner_mutation(
            con,
            writer_token=writer_token,
            table_name="neg_risk_evidence_failures",
            operation=operation,
            row_key="resource",
        )
        con.commit()
    except (sqlite3.Error, TypeError, ValueError):
        con.rollback()
    finally:
        con.close()


def _recover_resource_evidence_failure(
    con: sqlite3.Connection,
    store: OpportunityPerceptionStore,
    recovered_at_ms: int,
) -> bool:
    row = validate_resource_evidence_failure(con, require_resolved=False)
    if row is None or row["recovered_at_ms"] is not None:
        return False
    payload = {
        "component": "resource",
        "failed_at_ms": int(row["failed_at_ms"]),
        "reason": str(row["reason"]),
        "recovered_at_ms": recovered_at_ms,
    }
    writer_token = store._begin_expected_owner_mutation(
        con,
        table_name="neg_risk_evidence_failures",
        operation="UPDATE",
        row_key="resource",
    )
    con.execute(
        "UPDATE neg_risk_evidence_failures SET recovered_at_ms=?,row_hash=? "
        "WHERE component='resource'",
        (recovered_at_ms, _digest(payload)),
    )
    store._consume_expected_owner_mutation(
        con,
        writer_token=writer_token,
        table_name="neg_risk_evidence_failures",
        operation="UPDATE",
        row_key="resource",
    )
    return True


def _prepare_resource_writer(
    con: sqlite3.Connection,
    store: OpportunityPerceptionStore,
    now_ms: int,
) -> _ResourceAuthorityState:
    try:
        store._assert_owner_journal_clean(con)
        prior = _validate_resource_authority(con)
        recovered = _recover_resource_evidence_failure(con, store, now_ms)
    except (sqlite3.Error, TypeError, ValueError):
        con.rollback()
        _record_resource_evidence_failure(store, now_ms)
        raise
    if not recovered:
        return prior
    con.commit()
    con.execute("BEGIN IMMEDIATE")
    try:
        store._assert_owner_journal_clean(con)
        prior = _validate_resource_authority(con)
        validate_resource_evidence_failure(con, require_resolved=True)
        return prior
    except (sqlite3.Error, TypeError, ValueError):
        con.rollback()
        _record_resource_evidence_failure(store, now_ms)
        raise


def _publish_resource_checkpoint(
    con: sqlite3.Connection,
    store: OpportunityPerceptionStore,
    prior: _ResourceAuthorityState,
    appended_row: sqlite3.Row,
) -> None:
    rows = (*prior.suffix_rows, appended_row)
    if len(rows) > RESOURCE_SUFFIX_HARD_MAX_PAIRS:
        raise ValueError("resource-history-hard-limit")
    final_digest = _pair_digest(appended_row, prior.final_digest)
    checkpoint = prior.checkpoint
    generation = 1 if checkpoint is None else int(checkpoint["generation"]) + 1
    through_sample_id = (
        0 if checkpoint is None else int(checkpoint["through_sample_id"])
    )
    through_decision_id = (
        0 if checkpoint is None else int(checkpoint["through_decision_id"])
    )
    through_sequence = 0 if checkpoint is None else int(checkpoint["through_sequence"])
    compacted_sample_count = (
        0 if checkpoint is None else int(checkpoint["compacted_sample_count"])
    )
    compacted_decision_count = (
        0 if checkpoint is None else int(checkpoint["compacted_decision_count"])
    )
    prefix_digest = _ZERO_DIGEST if checkpoint is None else str(
        checkpoint["prefix_digest"]
    )
    last_decision_json = (
        None if checkpoint is None else checkpoint["last_decision_json"]
    )
    deleted_rows: tuple[sqlite3.Row, ...] = ()
    if len(rows) > RESOURCE_SUFFIX_COMPACT_HIGH_PAIRS:
        deleted_rows = tuple(rows[: len(rows) - RESOURCE_SUFFIX_COMPACT_LOW_PAIRS])
        for row in deleted_rows:
            prefix_digest = _pair_digest(row, prefix_digest)
        floor = deleted_rows[-1]
        through_sample_id = int(floor["sample_id"])
        through_decision_id = int(floor["id"])
        through_sequence = int(floor["sequence"])
        compacted_sample_count += len(deleted_rows)
        compacted_decision_count += len(deleted_rows)
        last_decision_json = str(floor["decision_json"])
    payload = _checkpoint_payload(
        generation=generation,
        through_sample_id=through_sample_id,
        through_decision_id=through_decision_id,
        through_sequence=through_sequence,
        compacted_sample_count=compacted_sample_count,
        compacted_decision_count=compacted_decision_count,
        prefix_digest=prefix_digest,
        last_decision_json=(
            None if last_decision_json is None else str(last_decision_json)
        ),
        last_decision_digest=final_digest,
    )
    operation = "INSERT" if checkpoint is None else "UPDATE"
    writer_token = store._begin_expected_owner_mutation(
        con,
        table_name="neg_risk_resource_authority_checkpoint",
        operation=operation,
        row_key="1",
    )
    con.execute(
        "INSERT INTO neg_risk_resource_authority_checkpoint("
        "id,generation,through_sample_id,through_decision_id,through_sequence,"
        "compacted_sample_count,compacted_decision_count,prefix_digest,"
        "last_decision_json,last_decision_digest,checkpoint_hash"
        ") VALUES(1,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(id) DO UPDATE SET "
        "generation=excluded.generation,"
        "through_sample_id=excluded.through_sample_id,"
        "through_decision_id=excluded.through_decision_id,"
        "through_sequence=excluded.through_sequence,"
        "compacted_sample_count=excluded.compacted_sample_count,"
        "compacted_decision_count=excluded.compacted_decision_count,"
        "prefix_digest=excluded.prefix_digest,"
        "last_decision_json=excluded.last_decision_json,"
        "last_decision_digest=excluded.last_decision_digest,"
        "checkpoint_hash=excluded.checkpoint_hash",
        (
            generation,
            through_sample_id,
            through_decision_id,
            through_sequence,
            compacted_sample_count,
            compacted_decision_count,
            prefix_digest,
            last_decision_json,
            final_digest,
            _digest(payload),
        ),
    )
    store._consume_expected_owner_mutation(
        con,
        writer_token=writer_token,
        table_name="neg_risk_resource_authority_checkpoint",
        operation=operation,
        row_key="1",
    )
    if deleted_rows:
        through = int(deleted_rows[-1]["id"])
        con.execute(
            "DELETE FROM neg_risk_resource_decisions WHERE id<=?",
            (through,),
        )
        con.execute(
            "DELETE FROM neg_risk_resource_samples WHERE id<=?",
            (through_sample_id,),
        )


class ResourceController:
    def __init__(
        self,
        store: OpportunityPerceptionStore,
        *,
        hot_quote_age_ms: int = 20_000,
        cooldown_ms: int = 30_000,
        decision_ttl_ms: int = 15_000,
        clock_ms=None,
        _verify_store_authority: bool = True,
    ) -> None:
        if hot_quote_age_ms <= 0 or cooldown_ms < 0 or decision_ttl_ms <= 0:
            raise ValueError("invalid-resource-policy")
        self._store = store
        self._hot_quote_age_ms = hot_quote_age_ms
        self._cooldown_ms = cooldown_ms
        self._decision_ttl_ms = decision_ttl_ms
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1_000))
        self._verify_store_authority = _verify_store_authority

    def decide(self, sample: ResourceSample) -> ResourceDecision:
        sample.validate()
        now_ms = self._clock_ms()
        if sample.observed_at_ms > now_ms:
            raise ValueError("invalid-resource-sample")
        if self._verify_store_authority:
            actual = self._store.candidate_freshness_snapshot(now_ms=sample.observed_at_ms)
            scopes = {incident.scope for incident in self._store.open_incidents()}
            if (
                sample.candidate_count != actual.candidate_count
                or sample.candidate_quote_p95_ms != actual.quote_p95_age_ms
                or sample.candidate_missing_quote_count != actual.missing_quote_count
                or sample.candidate_worker_ok != ("candidate" not in scopes)
                or sample.discovery_worker_ok != ("discovery" not in scopes)
            ):
                raise ValueError("resource-sample-authority-mismatch")

        con = sqlite3.connect(self._store.db_path, timeout=5)
        con.row_factory = sqlite3.Row
        authority_mutation_started = False
        try:
            con.execute("BEGIN IMMEDIATE")
            prior = _prepare_resource_writer(con, self._store, now_ms)
            previous = prior.current_decision
            authority_mutation_started = True
            cursor = con.execute(
                "INSERT INTO neg_risk_resource_samples(observed_at_ms,sample_json) VALUES(?,?)",
                (
                    sample.observed_at_ms,
                    json.dumps(
                        asdict(sample),
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ),
                ),
            )
            sample_id = cursor.lastrowid
            sequence = 1 if previous is None else previous.sequence + 1
            desired = replace(
                _policy_decision(
                    sample,
                    now_ms=now_ms,
                    hot_quote_age_ms=self._hot_quote_age_ms,
                    cooldown_ms=self._cooldown_ms,
                    decision_ttl_ms=self._decision_ttl_ms,
                    previous=previous,
                ),
                sequence=sequence,
                source_sample_id=sample_id,
            )
            decision_cursor = con.execute(
                "INSERT INTO neg_risk_resource_decisions("
                "sample_id,decided_at_ms,mode,reason,policy_version,sequence,"
                "decision_json) VALUES(?,?,?,?,?,?,?)",
                (
                    sample_id,
                    now_ms,
                    desired.mode,
                    desired.reason,
                    desired.policy_version,
                    desired.sequence,
                    json.dumps(
                        asdict(desired),
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ),
                ),
            )
            appended = con.execute(
                "SELECT d.*,s.id source_row_id,s.observed_at_ms,s.sample_json "
                "FROM neg_risk_resource_decisions d "
                "JOIN neg_risk_resource_samples s ON s.id=d.sample_id "
                "WHERE d.id=?",
                (decision_cursor.lastrowid,),
            ).fetchone()
            _publish_resource_checkpoint(
                con,
                self._store,
                prior,
                appended,
            )
            con.commit()
            return desired
        except BaseException as error:
            con.rollback()
            if authority_mutation_started and isinstance(
                error,
                (sqlite3.Error, TypeError, ValueError),
            ):
                _record_resource_evidence_failure(self._store, now_ms)
            raise
        finally:
            con.close()

    def capture_sample(
        self,
        *,
        reconciliation_running: bool,
        previous_discovery_batch_limit: int,
    ) -> ResourceSample:
        now_ms = self._clock_ms()
        freshness = self._store.candidate_freshness_snapshot(now_ms=now_ms)
        scopes = {incident.scope for incident in self._store.open_incidents()}
        return ResourceSample(
            candidate_count=freshness.candidate_count,
            candidate_quote_p95_ms=freshness.quote_p95_age_ms,
            candidate_missing_quote_count=freshness.missing_quote_count,
            candidate_worker_ok="candidate" not in scopes,
            discovery_worker_ok="discovery" not in scopes,
            reconciliation_running=(reconciliation_running and "reconciliation" not in scopes),
            previous_discovery_batch_limit=previous_discovery_batch_limit,
            observed_at_ms=now_ms,
        )


__all__ = [
    "RESOURCE_POLICY_VERSION",
    "ResourceController",
    "ResourceDecision",
    "ResourceHistoryFloor",
    "ResourceHistoryItem",
    "ResourceHistoryPage",
    "ResourceSample",
    "resource_history_page",
    "validate_resource_evidence_failure",
    "validate_resource_history",
]
