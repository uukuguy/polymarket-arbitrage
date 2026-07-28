"""Durable deterministic resource shedding for opportunity-first producers."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from dataclasses import asdict, dataclass

from polyarb.perception.store import OpportunityPerceptionStore


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


class ResourceController:
    def __init__(
        self,
        store: OpportunityPerceptionStore,
        *,
        hot_quote_age_ms: int = 20_000,
        cooldown_ms: int = 30_000,
        clock_ms=None,
        _verify_store_authority: bool = True,
    ) -> None:
        if hot_quote_age_ms <= 0 or cooldown_ms < 0:
            raise ValueError("invalid-resource-policy")
        self._store = store
        self._hot_quote_age_ms = hot_quote_age_ms
        self._cooldown_ms = cooldown_ms
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

        desired = self._desired(sample, now_ms)
        con = sqlite3.connect(self._store.db_path, timeout=5)
        con.row_factory = sqlite3.Row
        try:
            con.execute("BEGIN IMMEDIATE")
            previous_row = con.execute(
                "SELECT * FROM neg_risk_resource_decisions ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if previous_row is not None:
                previous = ResourceDecision(**json.loads(previous_row["decision_json"]))
                if (
                    previous.mode == "protect-hot-path"
                    and desired.mode != "protect-hot-path"
                    and now_ms - previous.decided_at_ms < self._cooldown_ms
                ):
                    desired = ResourceDecision(
                        **{
                            **asdict(previous),
                            "decided_at_ms": now_ms,
                            "reason": "hysteresis-cooldown",
                        }
                    )
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
            con.execute(
                "INSERT INTO neg_risk_resource_decisions("
                "sample_id,decided_at_ms,mode,reason,decision_json"
                ") VALUES(?,?,?,?,?)",
                (
                    sample_id,
                    now_ms,
                    desired.mode,
                    desired.reason,
                    json.dumps(
                        asdict(desired),
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ),
                ),
            )
            con.commit()
            return desired
        except BaseException:
            con.rollback()
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

    def _desired(self, sample: ResourceSample, now_ms: int) -> ResourceDecision:
        unhealthy_hot_path = (
            not sample.candidate_worker_ok
            or not sample.discovery_worker_ok
            or sample.candidate_missing_quote_count > 0
            or (
                sample.candidate_count > 0
                and (
                    sample.candidate_quote_p95_ms is None
                    or sample.candidate_quote_p95_ms >= self._hot_quote_age_ms
                )
            )
        )
        if unhealthy_hot_path:
            return ResourceDecision(
                mode="protect-hot-path",
                reason=(
                    "discovery-worker-degraded"
                    if not sample.discovery_worker_ok and sample.candidate_worker_ok
                    else "candidate-hot-path-pressure"
                ),
                reconciliation_enabled=False,
                discovery_batch_limit=max(1, sample.previous_discovery_batch_limit // 2),
                discovery_duty_multiplier=0.25,
                normal_candidate_interval_multiplier=2.0,
                high_candidate_interval_multiplier=1.0,
                http_preserved=True,
                health_claimed=False,
                previous_discovery_batch_limit=sample.previous_discovery_batch_limit,
                decided_at_ms=now_ms,
            )
        if sample.candidate_count == 0:
            return ResourceDecision(
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
                high_candidate_interval_multiplier=1.0,
                http_preserved=True,
                health_claimed=False,
                previous_discovery_batch_limit=sample.previous_discovery_batch_limit,
                decided_at_ms=now_ms,
            )
        return ResourceDecision(
            mode="normal",
            reason="candidate-hot-path-fresh",
            reconciliation_enabled=sample.reconciliation_running,
            discovery_batch_limit=sample.previous_discovery_batch_limit,
            discovery_duty_multiplier=1.0,
            normal_candidate_interval_multiplier=1.0,
            high_candidate_interval_multiplier=1.0,
            http_preserved=True,
            health_claimed=True,
            previous_discovery_batch_limit=sample.previous_discovery_batch_limit,
            decided_at_ms=now_ms,
        )


__all__ = ["ResourceController", "ResourceDecision", "ResourceSample"]
