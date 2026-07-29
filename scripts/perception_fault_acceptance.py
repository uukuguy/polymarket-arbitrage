"""Deterministic observer-only M1 perception qualification evaluator."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class QualificationVerdict:
    status: str
    reasons: tuple[str, ...]


_MAXIMUMS = (
    ("http_p95_s", 2.0, "http-p95"),
    ("candidate_quote_p95_s", 30.0, "candidate-quote-p95"),
    ("candidate_stale_before_s", 90.0, "candidate-stale"),
    ("normal_quote_stale_before_s", 120.0, "normal-stale"),
    ("coverage_window_s", 900.0, "coverage-window"),
    ("oldest_known_group_visit_s", 21_600.0, "oldest-known-group-visit"),
    ("promotion_to_watch_s", 60.0, "promotion-to-watch"),
    ("mttd_s", 30.0, "mttd"),
    ("containment_s", 60.0, "containment"),
)


def _validated_number(
    evidence: Mapping[str, Any],
    field: str,
    reasons: list[str],
) -> float | None:
    label = field.replace("_", "-")
    if field not in evidence:
        reasons.append(f"missing-{label}")
        return None
    value = evidence[field]
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        reasons.append(f"invalid-{label}")
        return None
    return float(value)


def _validated_bool(
    evidence: Mapping[str, Any],
    field: str,
    reasons: list[str],
) -> bool | None:
    label = field.replace("_", "-")
    if field not in evidence:
        reasons.append(f"missing-{label}")
        return None
    value = evidence[field]
    if type(value) is not bool:
        reasons.append(f"invalid-{label}")
        return None
    return value


def _validated_count(
    evidence: Mapping[str, Any],
    field: str,
    reasons: list[str],
) -> int | None:
    label = field.replace("_", "-")
    if field not in evidence:
        reasons.append(f"missing-{label}")
        return None
    value = evidence[field]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        reasons.append(f"invalid-{label}")
        return None
    return value


def _has_recovery_writer_receipt(incident: Mapping[str, Any]) -> bool:
    receipt = incident.get("recovery_writer_receipt")
    if not isinstance(receipt, Mapping):
        return False
    incident_component = incident.get("component")
    component = receipt.get("component")
    row_id = receipt.get("receipt_row_id")
    return (
        isinstance(incident_component, str)
        and bool(incident_component)
        and component == incident_component
        and isinstance(row_id, int)
        and not isinstance(row_id, bool)
        and row_id > 0
    )


def evaluate(evidence: Mapping[str, Any]) -> QualificationVerdict:
    reasons: list[str] = []
    for field, maximum, reason in _MAXIMUMS:
        value = _validated_number(evidence, field, reasons)
        if value is not None and value > maximum:
            reasons.append(reason)

    coverage = _validated_number(
        evidence,
        "liquidity_weighted_active_known_coverage",
        reasons,
    )
    if coverage is not None:
        if coverage > 1:
            reasons.append("invalid-liquidity-weighted-active-known-coverage")
        elif coverage < 0.9:
            reasons.append("active-known-coverage")

    reconciliation_complete = _validated_bool(
        evidence,
        "reconciliation_complete",
        reasons,
    )
    reconciliation_advancing = _validated_bool(
        evidence,
        "reconciliation_advancing",
        reasons,
    )
    if reconciliation_complete is True:
        reconciliation_closure = _validated_number(
            evidence,
            "reconciliation_closure_s",
            reasons,
        )
        if reconciliation_closure is not None and reconciliation_closure > 86_400:
            reasons.append("reconciliation-closure")
    elif reconciliation_complete is False and reconciliation_advancing is False:
        reasons.append("reconciliation-not-advancing")

    cross_membership = _validated_count(
        evidence,
        "cross_membership_quote_batches",
        reasons,
    )
    if cross_membership:
        reasons.append("cross-membership-quote")
    orphan_collecting = _validated_count(
        evidence,
        "orphan_collecting_runs",
        reasons,
    )
    if orphan_collecting:
        reasons.append("orphan-collecting-run")

    incidents = evidence.get("incidents")
    if isinstance(incidents, list):
        for incident in incidents:
            if not isinstance(incident, Mapping):
                reasons.append("invalid-incidents")
                break
            state = incident.get("state")
            incident_id = incident.get("incident_id")
            component = incident.get("component")
            if (
                state
                not in {
                    "detected",
                    "classified",
                    "contained",
                    "recovering",
                    "verified",
                    "escalated",
                }
                or not isinstance(incident_id, int)
                or isinstance(incident_id, bool)
                or incident_id <= 0
                or not isinstance(component, str)
                or not component
            ):
                reasons.append("invalid-incidents")
                break
            if state == "verified" and not _has_recovery_writer_receipt(incident):
                reasons.append("missing-recovery-writer-evidence")
                break
    elif "incidents" not in evidence:
        reasons.append("missing-incidents")
    else:
        reasons.append("invalid-incidents")

    return QualificationVerdict(
        status="PASS" if not reasons else "FAIL",
        reasons=tuple(reasons),
    )


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_evidence(path: Path) -> dict[str, Any]:
    payload = json.loads(
        path.read_text(),
        parse_constant=_reject_json_constant,
        object_pairs_hook=_reject_duplicate_keys,
    )
    if type(payload) is not dict:
        raise ValueError("evidence root must be an object")
    return payload


def _write_exclusive(path: Path, payload: Mapping[str, Any]) -> None:
    serialized = _canonical_json(payload).decode() + "\n"
    with path.open("x") as stream:
        stream.write(serialized)
        stream.flush()
        os.fsync(stream.fileno())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        evidence = _read_evidence(args.evidence)
        verdict = evaluate(evidence)
        canonical_evidence = _canonical_json(evidence)
        output = {
            "evidence_sha256": (
                f"sha256:{hashlib.sha256(canonical_evidence).hexdigest()}"
            ),
            "reasons": list(verdict.reasons),
            "schema_version": 1,
            "status": verdict.status,
        }
        _write_exclusive(args.output, output)
    except FileExistsError:
        print(f"verdict output already exists: {args.output}", file=sys.stderr)
        return 2
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(f"invalid evidence or output: {exc}", file=sys.stderr)
        return 2
    return 0 if verdict.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
