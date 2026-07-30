"""Collect bounded read-only M1 perception qualification evidence."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import canonical_digest

_READ_PATHS = (
    ("/healthz", "health"),
    ("/perception/discovery", "discovery"),
    ("/perception/reconciliation", "reconciliation"),
    ("/perception/resources?limit=1", "resources"),
    ("/perception/incidents?limit=100", "incidents"),
    ("/perception/qualification", "qualification"),
)
_MAX_RESPONSE_BYTES = 1_048_576
def export_fault_envelope(
    db_path: Path,
    fault_id: str,
    *,
    now_ms: int,
    freshness_limit_ms: int = 90_000,
) -> dict[str, object]:
    """Derive one bounded envelope from one read-only SQLite transaction."""
    authority = FaultAuthorityStore(db_path, read_only=True)
    if type(freshness_limit_ms) is not int or freshness_limit_ms <= 0:
        raise ValueError("invalid-freshness-limit")
    deadline = time.monotonic() + 0.75
    con = authority._connect(deadline)
    try:
        con.execute("BEGIN")
        history = authority._validate_history_in_connection(con, fault_id)
        projection = authority._project_fault_in_connection(
            con,
            fault_id,
            now_ms=now_ms,
            history=history,
            deadline_monotonic=deadline,
        )
        if (
            not history.valid
            or history.intent is None
            or not projection.available
        ):
            raise ValueError("fault-authority-unavailable")
        intent = history.intent
        source_facts = _fault_source_facts(
            con,
            authority=authority,
            history=history,
            projection=projection,
            now_ms=now_ms,
            freshness_limit_ms=freshness_limit_ms,
        )
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    runtime = {
        "component": intent.runtime.component,
        "release_id": intent.runtime.release_id,
        "machine_id": intent.runtime.machine_id,
        "boot_id": str(intent.runtime.boot_id),
    }
    intent_json = {
        "fault_id": intent.fault_id,
        "kind": intent.kind.value,
        "call_class": intent.call_class.value,
        "target_key": intent.target_key,
        "parameters": dict(intent.parameters),
        "nonce_digest": intent.nonce_digest,
        "runtime": runtime,
    }
    events = [
        {
            "fault_id": event.fault_id,
            "sequence": event.sequence,
            "state": event.state.value if event.state is not None else None,
            "action": event.action.value if event.action is not None else None,
            "occurred_at_ms": event.occurred_at_ms,
            "evidence": dict(event.evidence),
            "previous_hash": event.previous_hash,
            "event_hash": event.event_hash,
        }
        for event in history.events
    ]
    envelope: dict[str, object] = {
        "evidence_schema_version": 2,
        "scope": "production-fault",
        "mode": (
            "final"
            if projection.state is not None and projection.state.value == "verified"
            else "candidate"
        ),
        "app_id": "polyarb-l1",
        "release_id": runtime["release_id"],
        "machine_id": runtime["machine_id"],
        "boot_id": runtime["boot_id"],
        "fault_intent": intent_json,
        "fault_intent_digest": canonical_digest(intent_json),
        "target_digest": canonical_digest(intent.target_key),
        "parameter_digest": canonical_digest(dict(intent.parameters)),
        "nonce_digest": intent.nonce_digest,
        "fault_history": events,
        "fault_history_tail_hash": events[-1]["event_hash"],
        "open_injection_fault_count": int(
            projection.state is not None
            and projection.state.value
            in {"authorized", "armed", "injected", "detected", "contained", "cleaned"}
        ),
        "pending_verification_fault_count": int(
            projection.state is not None and projection.state.value == "recovered"
        ),
        "source_projection_active": projection.active,
    }
    envelope.update(source_facts)
    return envelope


def _fault_source_facts(
    con: sqlite3.Connection,
    *,
    authority: FaultAuthorityStore,
    history: Any,
    projection: Any,
    now_ms: int,
    freshness_limit_ms: int,
) -> dict[str, object]:
    """Validate and derive every qualification fact inside the caller's snapshot."""
    detected = next(
        (
            event
            for event in history.events
            if event.state is not None and event.state.value == "detected"
        ),
        None,
    )
    if detected is None:
        raise ValueError("fault-detection-missing")
    incident_id = detected.evidence.get("incident_id")
    detection_id = incident_id or detected.evidence.get("coverage_id")
    source_kind = "coverage:partial-or-rejected-page"
    if incident_id is not None:
        incident = con.execute(
            "SELECT incident_id,kind FROM neg_risk_incident_events "
            "WHERE incident_id=? ORDER BY sequence LIMIT 1",
            (incident_id,),
        ).fetchone()
        if incident is None:
            raise ValueError("fault-incident-source-missing")
        source_kind = str(incident["kind"])

    recovered = next(
        (
            event
            for event in reversed(history.events)
            if event.state is not None and event.state.value == "recovered"
        ),
        None,
    )
    if recovered is None:
        raise ValueError("fault-not-recovered")
    recovery_id = str(recovered.evidence.get("recovery_id", ""))
    recovery_queries = {
        "candidate": (
            "neg_risk_candidate_success_receipts",
            "SELECT id FROM neg_risk_candidate_success_receipts WHERE id=?",
            "candidate-success-",
        ),
        "discovery": (
            "neg_risk_discovery_batches",
            "SELECT id FROM neg_risk_discovery_batches WHERE id=?",
            "discovery-batch-",
        ),
        "reconciliation": (
            "neg_risk_reconciliation_windows",
            "SELECT id FROM neg_risk_reconciliation_windows WHERE id=?",
            "reconciliation-window-",
        ),
        "notification": (
            "neg_risk_opportunity_notification_attempts",
            "SELECT id FROM neg_risk_opportunity_notification_attempts WHERE id=?",
            "telegram-delivery-",
        ),
    }
    table, query, prefix = recovery_queries[history.intent.runtime.component]
    if not recovery_id.startswith(prefix):
        raise ValueError("fault-recovery-source-invalid")
    writer_id: object = recovery_id[len(prefix):]
    if history.intent.runtime.component != "reconciliation":
        try:
            writer_id = int(str(writer_id))
        except ValueError as exc:
            raise ValueError("fault-recovery-source-invalid") from exc
    writer_row = con.execute(query.replace("SELECT id", "SELECT *"), (writer_id,)).fetchone()
    if writer_row is None:
        raise ValueError("fault-recovery-source-missing")
    occurred_column = {
        "candidate": "observed_at_ms",
        "discovery": "finished_at_ms",
        "reconciliation": "checkpoint_at_ms",
        "notification": "attempted_at_ms",
    }[history.intent.runtime.component]

    candidate_mismatches = int(
        con.execute(
            "SELECT COUNT(*) FROM neg_risk_candidate_success_receipts r "
            "LEFT JOIN neg_risk_group_revisions g ON g.id=r.group_revision_row_id "
            "LEFT JOIN neg_risk_group_quote_batches q "
            "ON q.rowid=r.quote_batch_row_id AND q.id=r.quote_batch_id "
            "LEFT JOIN neg_risk_candidate_watch_facts f "
            "ON f.id=r.candidate_fact_row_id "
            "WHERE g.id IS NULL OR q.rowid IS NULL OR f.id IS NULL "
            "OR r.group_id!=g.group_id OR r.group_id!=q.group_id "
            "OR r.group_id!=f.group_id OR r.membership_hash!=g.membership_hash "
            "OR r.membership_hash!=q.membership_hash "
            "OR r.membership_hash!=f.membership_hash "
            "OR q.group_revision!=g.revision OR f.quote_batch_id!=q.id"
        ).fetchone()[0]
    )
    legacy_mismatches = int(
        con.execute(
            "SELECT COUNT(DISTINCT l.quote_run_id) "
            "FROM neg_risk_quote_run_legs l JOIN neg_risk_quotes q "
            "ON q.quote_run_id=l.quote_run_id AND q.yes_token_id=l.yes_token_id "
            "WHERE trim(l.event_id)='' OR trim(l.membership_hash)='' "
            "OR trim(q.event_id)='' OR trim(q.membership_hash)='' "
            "OR q.event_id!=l.event_id OR q.membership_hash!=l.membership_hash "
            "OR q.neg_risk_market_id!=l.neg_risk_market_id "
            "OR q.market_id!=l.market_id OR q.condition_id!=l.condition_id"
        ).fetchone()[0]
    )
    freshness = con.execute(
        "WITH current AS (SELECT r.* FROM neg_risk_group_revisions r JOIN "
        "(SELECT group_id,MAX(revision) revision FROM neg_risk_group_revisions "
        "GROUP BY group_id) c ON c.group_id=r.group_id AND c.revision=r.revision) "
        "SELECT COUNT(*) AS candidates,SUM(CASE WHEN NOT EXISTS "
        "(SELECT 1 FROM neg_risk_group_quote_batches q WHERE q.group_id=current.group_id "
        "AND q.membership_hash=current.membership_hash AND q.status='complete' "
        "AND q.quoted_at_ms BETWEEN ? AND ?) THEN 1 ELSE 0 END) AS missing "
        "FROM current WHERE current.status='certified'",
        (now_ms - freshness_limit_ms, now_ms),
    ).fetchone()
    latest_reconciliation = con.execute(
        "SELECT status FROM neg_risk_reconciliation_windows "
        "ORDER BY started_at_ms DESC,rowid DESC LIMIT 1"
    ).fetchone()
    return {
        "detection_receipt": {
            "detection_id": detection_id,
            "kind": history.intent.kind.value,
            "call_class": history.intent.call_class.value,
            "target_key": history.intent.target_key,
            "runtime": {
                "component": history.intent.runtime.component,
                "release_id": history.intent.runtime.release_id,
                "machine_id": history.intent.runtime.machine_id,
                "boot_id": str(history.intent.runtime.boot_id),
            },
            "source_kind": source_kind,
        },
        "recovery_writer_receipt": {
            "table": table,
            "row_id": writer_row["id"],
            "component": history.intent.runtime.component,
            "occurred_at_ms": int(writer_row[occurred_column]),
        },
        "open_incident_count": int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_incident_open_authority"
            ).fetchone()[0]
        ),
        "cross_membership_quote_batches": candidate_mismatches + legacy_mismatches,
        "partial_publication_count": int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_discovery_batches WHERE completed=0 "
                "AND next_cursor IS requested_cursor"
            ).fetchone()[0]
        ),
        "orphan_collecting_runs": int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_quote_runs "
                "WHERE status='collecting' AND lease_expires_at_ms<=?",
                (now_ms,),
            ).fetchone()[0]
        ),
        "freshness_gate": int(freshness["missing"] or 0) == 0,
        "reconciliation_gate": (
            latest_reconciliation is None
            or latest_reconciliation["status"] in {"complete", "applied"}
        ),
    }


def _mapping(value: object, reason: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(reason)
    return value


def _number(value: object, reason: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(reason)
    return float(value)


def _integer(value: object, reason: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(reason)
    return value


def _nearest_rank_p95(values: Sequence[float]) -> float:
    if not values:
        raise ValueError("http-latency-evidence-missing")
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * 0.95) - 1]


def build_evidence(
    rounds: Sequence[Mapping[str, object]],
    *,
    expected_release: str,
) -> dict[str, object]:
    if len(rounds) < 5:
        raise ValueError("insufficient-readonly-samples")

    identities: set[tuple[str, str, str, str]] = set()
    latencies: list[float] = []
    checkpoints: list[int] = []
    quote_p95_values: list[float] = []
    normalized_rounds: list[Mapping[str, object]] = []

    for sample in rounds:
        health = _mapping(sample.get("health"), "health-evidence-invalid")
        identity = (
            str(health.get("serviceId")),
            str(health.get("releaseId")),
            str(health.get("machineId")),
            str(health.get("bootId")),
        )
        identities.add(identity)
        if identity[0] != "polyarb-l1" or identity[1] != expected_release:
            raise ValueError("runtime-identity-mismatch")

        observed_at_ms = _integer(
            sample.get("observed_at_ms"),
            "sample-time-invalid",
        )
        if normalized_rounds and observed_at_ms < _integer(
            normalized_rounds[-1].get("observed_at_ms"),
            "sample-time-invalid",
        ):
            raise ValueError("sample-time-order-invalid")
        normalized_rounds.append(sample)

        raw_latencies = sample.get("latencies_s")
        if not isinstance(raw_latencies, list) or not raw_latencies:
            raise ValueError("http-latency-evidence-missing")
        latencies.extend(
            _number(value, "http-latency-evidence-invalid")
            for value in raw_latencies
        )

        for envelope_name in (
            "discovery",
            "reconciliation",
            "resources",
            "incidents",
            "qualification",
        ):
            envelope = _mapping(
                sample.get(envelope_name),
                f"{envelope_name}-evidence-invalid",
            )
            if envelope.get("status") != "available":
                raise ValueError(f"{envelope_name}-unavailable")

        reconciliation = _mapping(
            _mapping(
                sample["reconciliation"],
                "reconciliation-evidence-invalid",
            ).get("reconciliation"),
            "reconciliation-evidence-missing",
        )
        checkpoints.append(
            _integer(
                reconciliation.get("checkpoint_at_ms"),
                "reconciliation-checkpoint-invalid",
            )
        )

        resource_items = _mapping(
            sample["resources"],
            "resource-evidence-invalid",
        ).get("items")
        if not isinstance(resource_items, list) or not resource_items:
            raise ValueError("resource-sample-missing")
        resource_sample = _mapping(
            _mapping(resource_items[0], "resource-item-invalid").get("sample"),
            "resource-sample-invalid",
        )
        quote_p95_values.append(
            _number(
                resource_sample.get("candidate_quote_p95_ms"),
                "candidate-quote-p95-missing",
            )
        )

    if len(identities) != 1:
        raise ValueError("runtime-identity-changed")
    service_id, release_id, machine_id, boot_id = next(iter(identities))
    latest = rounds[-1]
    health = _mapping(latest["health"], "health-evidence-invalid")
    policy = _mapping(
        health.get("qualificationPolicy"),
        "qualification-policy-missing",
    )
    discovery = _mapping(
        _mapping(latest["discovery"], "discovery-evidence-invalid").get(
            "discovery"
        ),
        "discovery-evidence-missing",
    )
    coverage = _mapping(discovery.get("coverage"), "coverage-evidence-missing")
    by_minutes = _mapping(coverage.get("by_minutes"), "coverage-evidence-missing")
    coverage_15 = _mapping(by_minutes.get("15"), "coverage-15m-missing")
    admission = _mapping(
        discovery.get("admission_proof"),
        "admission-proof-missing",
    )
    reconciliation = _mapping(
        _mapping(
            latest["reconciliation"],
            "reconciliation-evidence-invalid",
        ).get("reconciliation"),
        "reconciliation-evidence-missing",
    )
    reconciliation_status = reconciliation.get("status")
    if reconciliation_status not in {"open", "complete", "applied"}:
        raise ValueError("reconciliation-status-invalid")
    incidents = _mapping(latest["incidents"], "incident-evidence-invalid")
    qualification = _mapping(
        latest["qualification"],
        "qualification-evidence-invalid",
    )

    result: dict[str, object] = {
        "evidence_schema_version": 1,
        "scope": "production-readonly",
        "app_id": service_id,
        "release_id": release_id,
        "machine_id": machine_id,
        "boot_id": boot_id,
        "window_started_at_ms": _integer(
            rounds[0].get("observed_at_ms"),
            "sample-time-invalid",
        ),
        "window_ended_at_ms": _integer(
            latest.get("observed_at_ms"),
            "sample-time-invalid",
        ),
        "sample_count": len(rounds),
        "http_p95_s": _nearest_rank_p95(latencies),
        "candidate_quote_p95_s": max(quote_p95_values) / 1_000,
        "candidate_stale_before_s": _number(
            policy.get("candidateQuoteHardStaleS"),
            "candidate-stale-policy-missing",
        ),
        "normal_quote_stale_before_s": _number(
            policy.get("candidateLowerLaneMaxWaitS"),
            "normal-stale-policy-missing",
        ),
        "liquidity_weighted_active_known_coverage": _number(
            coverage_15.get("liquidity_weighted_fraction"),
            "coverage-15m-invalid",
        ),
        "coverage_window_s": 900,
        "oldest_known_group_visit_s": _number(
            discovery.get("oldest_visit_age_ms"),
            "oldest-visit-missing",
        )
        / 1_000,
        "promotion_to_watch_s": _number(
            admission.get("effective_start_bound_ms"),
            "promotion-bound-missing",
        )
        / 1_000,
        "reconciliation_complete": reconciliation_status
        in {"complete", "applied"},
        "reconciliation_advancing": checkpoints[-1] > checkpoints[0],
        "open_incident_count": _integer(
            incidents.get("open_count"),
            "open-incident-count-missing",
        ),
        "cross_membership_quote_batches": _integer(
            qualification.get("cross_membership_quote_batches"),
            "cross-membership-quote-batches-missing",
        ),
        "orphan_collecting_runs": _integer(
            qualification.get("orphan_collecting_runs"),
            "orphan-collecting-runs-missing",
        ),
        "incidents": [],
        "source_rounds": list(normalized_rounds),
    }
    if result["reconciliation_complete"]:
        result["reconciliation_closure_s"] = (
            _number(
                reconciliation.get("duration_ms"),
                "reconciliation-duration-missing",
            )
            / 1_000
        )
    return result


def _fetch_json(base_url: str, path: str) -> tuple[object, float]:
    request = Request(f"{base_url.rstrip('/')}{path}", method="GET")
    started = time.monotonic()
    with urlopen(request, timeout=10) as response:  # noqa: S310
        payload = response.read(_MAX_RESPONSE_BYTES + 1)
    elapsed = time.monotonic() - started
    if len(payload) > _MAX_RESPONSE_BYTES:
        raise ValueError("readonly-response-too-large")
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise ValueError("readonly-response-not-object")
    return decoded, elapsed


def collect_rounds(
    base_url: str,
    *,
    sample_count: int,
    interval_s: float,
    fetch_json: Callable[[str, str], tuple[object, float]] = _fetch_json,
    clock_ms: Callable[[], int] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> list[dict[str, object]]:
    if sample_count < 5 or sample_count > 120:
        raise ValueError("samples-must-be-between-5-and-120")
    if not math.isfinite(interval_s) or interval_s < 0 or interval_s > 60:
        raise ValueError("interval-must-be-between-0-and-60")
    effective_clock = (
        (lambda: int(time.time() * 1_000)) if clock_ms is None else clock_ms
    )
    rounds: list[dict[str, object]] = []
    for sequence in range(sample_count):
        sample: dict[str, object] = {"latencies_s": []}
        latencies = sample["latencies_s"]
        assert isinstance(latencies, list)
        for path, field in _READ_PATHS:
            body, elapsed_s = fetch_json(base_url, path)
            sample[field] = body
            latencies.append(elapsed_s)
        sample["observed_at_ms"] = effective_clock()
        rounds.append(sample)
        print(
            f"readonly sample {sequence + 1}/{sample_count} complete",
            file=sys.stderr,
            flush=True,
        )
        if sequence + 1 < sample_count:
            sleeper(interval_s)
    return rounds


def _validate_base_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("base-url-must-be-an-https-origin")
    return value.rstrip("/")


def _write_exclusive(path: Path, evidence: Mapping[str, object]) -> None:
    payload = (
        json.dumps(
            evidence,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    )
    with path.open("x") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--expected-release", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--interval-s", type=float, default=1.0)
    args = parser.parse_args(argv)
    try:
        base_url = _validate_base_url(args.base_url)
        if re.fullmatch(r"[0-9a-f]{40}", args.expected_release) is None:
            raise ValueError("expected-release-must-be-a-40-character-sha")
        rounds = collect_rounds(
            base_url,
            sample_count=args.samples,
            interval_s=args.interval_s,
        )
        evidence = build_evidence(
            rounds,
            expected_release=args.expected_release,
        )
        _write_exclusive(args.output, evidence)
    except FileExistsError:
        print(f"evidence output already exists: {args.output}", file=sys.stderr)
        return 2
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(f"readonly qualification collection failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
