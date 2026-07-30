"""Deterministic observer-only M1 perception qualification evaluator."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from polyarb.perception.evaluator_signing import (
    SIGNATURE_VERSION,
    load_private_key,
    load_public_key,
    sign_digest,
    verify_digest,
)
from polyarb.perception.fault_control import (
    FaultEventState,
    FaultRuntimeIdentity,
    fault_call_binding_digest,
    normalize_evidence,
)
from polyarb.safe_artifact import read_stable_bytes, write_exclusive_bytes


@dataclass(frozen=True)
class QualificationVerdict:
    status: str
    reasons: tuple[str, ...]


def canonical_digest(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


_RECOVERY_TABLES = {
    "candidate": "neg_risk_candidate_success_receipts",
    "discovery": "neg_risk_discovery_batches",
    "reconciliation": "neg_risk_reconciliation_windows",
    "notification": "neg_risk_opportunity_notification_attempts",
}
_RECOVERY_PREFIXES = {
    "candidate": "candidate-success-",
    "discovery": "discovery-batch-",
    "reconciliation": "reconciliation-window-",
    "notification": "telegram-delivery-",
}
_FAULT_CONTRACTS = {
    "gamma-timeout": ("discovery", "gamma-discovery-event-page"),
    "gamma-partial": ("discovery", "gamma-discovery-event-page"),
    "gamma-malformed": ("discovery", "gamma-discovery-event-page"),
    "gamma-cursor": ("reconciliation", "gamma-reconciliation-event-page"),
    "clob-missing-leg": ("candidate", "clob-candidate-book-batch"),
    "clob-429": ("candidate", "clob-candidate-book-batch"),
    "clob-latency": ("candidate", "clob-candidate-book-batch"),
    "telegram-failure": ("notification", "telegram-opportunity-card"),
}
_SOURCE_AUTHORITY_DOMAIN = "polyarb-upstream-fault-source-envelope-v1"
_ENVELOPE_FIELDS = frozenset(
    {
        "evidence_schema_version", "scope", "mode", "app_id", "release_id",
        "machine_id", "boot_id", "fault_intent", "fault_intent_digest",
        "target_digest", "parameter_digest", "nonce_digest", "fault_history",
        "fault_history_tail_hash", "recovery_writer_receipt",
        "open_injection_fault_count", "pending_verification_fault_count",
        "source_projection_active", "open_incident_count",
        "cross_membership_quote_batches", "partial_publication_count",
        "orphan_collecting_runs", "freshness_gate", "reconciliation_gate",
        "detection_receipt", "source_authority", "source_valid_until_ms",
    }
)
_SOURCE_AUTHORITY_FIELDS = frozenset(
    {
        "domain",
        "envelope_digest",
        "signature",
        "signature_kid",
        "signature_version",
        "source_facts_digest",
    }
)
_EVENT_FIELDS = frozenset(
    {
        "fault_id", "sequence", "state", "action", "occurred_at_ms",
        "evidence", "previous_hash", "event_hash",
    }
)
_INTENT_FIELDS = frozenset(
    {
        "fault_id", "kind", "call_class", "target_key", "parameters",
        "nonce_digest", "runtime",
    }
)
_RUNTIME_FIELDS = frozenset(
    {"component", "release_id", "machine_id", "boot_id"}
)
_PRODUCTION_EVIDENCE_FIELDS = {
    "authorized": frozenset({"reason"}),
    "armed": frozenset({"runtime_identity_digest", "ownership_digest"}),
    "injected": frozenset({"call_id", "call_binding_digest"}),
    "detected": None,
    "contained": frozenset({"containment_id"}),
    "cleaned": frozenset(
        {"cleanup_id", "memory_cleared_at_ms", "receipt_persisted_at_ms"}
    ),
    "recovered": frozenset({"recovery_id"}),
    "verified": frozenset({"verdict_id", "verdict_digest"}),
}
_INCIDENT_SOURCE_FIELDS = frozenset(
    {
        "event_hash", "event_id", "evidence_json", "incident_id", "kind",
        "occurred_at_ms", "previous_hash", "scope", "sequence", "state",
    }
)
_INCIDENT_TRANSITIONS = {
    "detected": {"classified"},
    "classified": {"contained", "escalated"},
    "contained": {"recovering", "escalated"},
    "recovering": {"verified", "contained", "escalated"},
    "verified": set(),
    "escalated": {"recovering"},
}
_COVERAGE_SOURCE_FIELDS = frozenset(
    {
        "boot_id", "call_class", "component", "coverage_id", "fault_id",
        "injected_call_id", "kept_count", "machine_id", "next_cursor_digest",
        "original_count", "recorded_at_ms", "release_id", "requested_cursor_digest",
        "source_hash", "target_key",
    }
)
_INCIDENT_CHECKPOINT_FIELDS = frozenset(
    {
        "checkpoint_hash",
        "compacted_event_count",
        "generation",
        "prefix_hash",
        "scope_floor_count",
        "through_event_id",
    }
)


def _source_history_valid(
    receipt: Mapping[str, Any],
    *,
    intent: Mapping[str, Any],
    runtime: Mapping[str, Any],
    injected_call_id: object,
) -> bool:
    history = receipt.get("source_history")
    if not isinstance(history, list) or not history:
        return False
    if receipt.get("source_kind") == "coverage:partial-or-rejected-page":
        if (
            receipt.get("source_checkpoint") is not None
            or len(history) != 1
            or not isinstance(history[0], Mapping)
        ):
            return False
        row = history[0]
        payload = {key: row.get(key) for key in _COVERAGE_SOURCE_FIELDS - {"source_hash"}}
        expected_id = "coverage-" + canonical_digest(
            {
                "kept_count": row.get("kept_count"),
                "next_cursor_digest": row.get("next_cursor_digest"),
                "original_count": row.get("original_count"),
                "requested_cursor_digest": row.get("requested_cursor_digest"),
            }
        )
        return (
            set(row) == _COVERAGE_SOURCE_FIELDS
            and type(row.get("original_count")) is int
            and type(row.get("kept_count")) is int
            and 0 <= row["kept_count"] < row["original_count"]
            and type(row.get("recorded_at_ms")) is int
            and row["recorded_at_ms"] >= 0
            and all(
                isinstance(row.get(key), str)
                and re.fullmatch(r"[0-9a-f]{64}", row[key]) is not None
                for key in ("requested_cursor_digest", "next_cursor_digest")
            )
            and row.get("source_hash") == canonical_digest(payload)
            and row.get("coverage_id") == expected_id == receipt.get("detection_id")
            and row.get("fault_id") == intent.get("fault_id")
            and row.get("injected_call_id") == injected_call_id
            and row.get("call_class") == intent.get("call_class")
            and row.get("target_key") == intent.get("target_key")
            and all(row.get(key) == runtime.get(key) for key in (
                "component", "release_id", "machine_id", "boot_id"
            ))
        )

    checkpoint = receipt.get("source_checkpoint")
    if not isinstance(checkpoint, Mapping) or set(checkpoint) != _INCIDENT_CHECKPOINT_FIELDS:
        return False
    try:
        checkpoint_payload = {
            "compacted_event_count": int(checkpoint["compacted_event_count"]),
            "generation": int(checkpoint["generation"]),
            "prefix_hash": str(checkpoint["prefix_hash"]),
            "scope_floor_count": int(checkpoint["scope_floor_count"]),
            "through_event_id": int(checkpoint["through_event_id"]),
        }
    except (KeyError, TypeError, ValueError):
        return False
    if (
        any(
            isinstance(checkpoint[key], bool) or not isinstance(checkpoint[key], int)
            for key in (
                "compacted_event_count",
                "generation",
                "scope_floor_count",
                "through_event_id",
            )
        )
        or any(checkpoint_payload[key] < 0 for key in (
            "compacted_event_count",
            "generation",
            "scope_floor_count",
            "through_event_id",
        ))
        or re.fullmatch(r"sha256:[0-9a-f]{64}", checkpoint_payload["prefix_hash"])
        is None
        or checkpoint.get("checkpoint_hash")
        != "sha256:" + hashlib.sha256(_canonical_json(checkpoint_payload)).hexdigest()
        or (
            checkpoint_payload["generation"] == 0
            and checkpoint_payload
            != {
                "compacted_event_count": 0,
                "generation": 0,
                "prefix_hash": "sha256:" + "0" * 64,
                "scope_floor_count": 0,
                "through_event_id": 0,
            }
        )
    ):
        return False

    component = runtime.get("component")
    expected_scope = (
        f"candidate:{intent.get('target_key')}"
        if component == "candidate"
        else f"notification:{intent.get('target_key')}"
        if component == "notification"
        else component
    )
    previous_hash: object = checkpoint_payload["prefix_hash"]
    target: list[tuple[Mapping[str, Any], object]] = []
    previous_event_id = checkpoint_payload["through_event_id"]
    incident_sequences: dict[str, int] = {}
    for row in history:
        if not isinstance(row, Mapping) or set(row) != _INCIDENT_SOURCE_FIELDS:
            return False
        try:
            evidence = json.loads(str(row["evidence_json"]))
            canonical_evidence = json.dumps(
                evidence, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            )
            payload = {
                "evidence_json": str(row["evidence_json"]),
                "event_id": int(row["event_id"]),
                "incident_id": str(row["incident_id"]),
                "kind": str(row["kind"]),
                "occurred_at_ms": int(row["occurred_at_ms"]),
                "previous_hash": str(row["previous_hash"]),
                "scope": str(row["scope"]),
                "sequence": int(row["sequence"]),
                "state": str(row["state"]),
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return False
        expected_hash = "sha256:" + hashlib.sha256(_canonical_json(payload)).hexdigest()
        if (
            canonical_evidence != row["evidence_json"]
            or row["event_hash"] != expected_hash
            or row["previous_hash"] != previous_hash
            or int(row["event_id"]) != previous_event_id + 1
            or int(row["sequence"])
            != incident_sequences.get(str(row["incident_id"]), 0) + 1
        ):
            return False
        previous_hash = row["event_hash"]
        previous_event_id = int(row["event_id"])
        incident_sequences[str(row["incident_id"])] = int(row["sequence"])
        if row["incident_id"] == receipt.get("detection_id"):
            target.append((row, evidence))
    if (
        not target
        or target[0][0].get("state") != "detected"
        or not isinstance(target[0][1], Mapping)
        or target[0][1].get("fault_call_id") != injected_call_id
    ):
        return False
    for index, (row, _evidence) in enumerate(target, start=1):
        if (
            row.get("sequence") != index
            or row.get("kind") != receipt.get("source_kind")
            or row.get("scope") != expected_scope
        ):
            return False
        if index > 1 and row.get("state") not in _INCIDENT_TRANSITIONS.get(
            str(target[index - 2][0].get("state")), set()
        ):
            return False
    return target[-1][0].get("state") == "verified"


def _event_digest(event: Mapping[str, Any]) -> str:
    return canonical_digest(
        {
            "fault_id": event.get("fault_id"),
            "sequence": event.get("sequence"),
            "state": event.get("state"),
            "action": event.get("action"),
            "occurred_at_ms": event.get("occurred_at_ms"),
            "evidence": event.get("evidence"),
            "previous_hash": event.get("previous_hash"),
        }
    )


def evaluate_fault_envelope(
    evidence: Mapping[str, Any],
    *,
    mode: str,
    candidate_artifact: Mapping[str, Any] | None = None,
    expected_release: str | None = None,
) -> QualificationVerdict:
    """Evaluate one immutable exported fault envelope without any mutation."""
    reasons: list[str] = []
    if set(evidence) != _ENVELOPE_FIELDS:
        reasons.append("invalid-envelope-fields")
    source_authority = evidence.get("source_authority")
    unsigned_evidence = {
        key: value for key, value in evidence.items() if key != "source_authority"
    }
    source_digest = canonical_digest(unsigned_evidence)
    source_facts_digest = canonical_digest(
        {
            key: value
            for key, value in unsigned_evidence.items()
            if key not in {"freshness_gate", "orphan_collecting_runs"}
        }
    )
    source_public_key = os.getenv(
        "POLYARB_UPSTREAM_FAULT_SOURCE_PUBLIC_KEY", ""
    )
    if (
        not isinstance(source_authority, Mapping)
        or set(source_authority) != _SOURCE_AUTHORITY_FIELDS
        or source_authority.get("domain") != _SOURCE_AUTHORITY_DOMAIN
        or source_authority.get("envelope_digest") != source_digest
        or source_authority.get("source_facts_digest") != source_facts_digest
        or not source_public_key
        or not verify_digest(
            source_public_key,
            kid=source_authority.get("signature_kid"),
            version=source_authority.get("signature_version"),
            digest=canonical_digest(
                {
                    "domain": _SOURCE_AUTHORITY_DOMAIN,
                    "envelope_digest": source_digest,
                }
            ),
            signature=source_authority.get("signature"),
        )
    ):
        reasons.append("source-authority-signature-mismatch")
    if evidence.get("scope") != "production-fault":
        reasons.append("scope-mismatch")
    if evidence.get("mode") != mode:
        reasons.append("evidence-mode-mismatch")
    if evidence.get("evidence_schema_version") != 2:
        reasons.append("invalid-evidence-schema-version")
    intent = evidence.get("fault_intent")
    if not isinstance(intent, Mapping):
        reasons.append("missing-fault-intent")
        return QualificationVerdict("FAIL", tuple(reasons))
    if set(intent) != _INTENT_FIELDS:
        reasons.append("invalid-fault-intent-fields")
    if evidence.get("fault_intent_digest") != canonical_digest(intent):
        reasons.append("fault-intent-digest-mismatch")
    runtime = intent.get("runtime")
    if not isinstance(runtime, Mapping):
        reasons.append("missing-runtime")
        runtime = {}
    elif set(runtime) != _RUNTIME_FIELDS:
        reasons.append("invalid-runtime-fields")
    for field in ("release_id", "machine_id", "boot_id"):
        if evidence.get(field) != runtime.get(field):
            reasons.append(f"runtime-{field.replace('_', '-')}-mismatch")
    try:
        FaultRuntimeIdentity(
            component=runtime.get("component"),
            release_id=runtime.get("release_id"),
            machine_id=runtime.get("machine_id"),
            boot_id=UUID(str(runtime.get("boot_id"))),
        )
    except (TypeError, ValueError):
        reasons.append("runtime-identity-invalid")
    if expected_release is not None and runtime.get("release_id") != expected_release:
        reasons.append("expected-release-mismatch")
    if intent.get("nonce_digest") is None:
        reasons.append("missing-nonce-digest")
    if evidence.get("nonce_digest") != intent.get("nonce_digest"):
        reasons.append("nonce-digest-mismatch")
    if evidence.get("target_digest") != canonical_digest(intent.get("target_key")):
        reasons.append("target-digest-mismatch")
    if evidence.get("parameter_digest") != canonical_digest(intent.get("parameters")):
        reasons.append("parameter-digest-mismatch")
    contract = _FAULT_CONTRACTS.get(str(intent.get("kind")))
    if (
        contract is None
        or runtime.get("component") != contract[0]
        or intent.get("call_class") != contract[1]
    ):
        reasons.append("fault-contract-mismatch")

    history = evidence.get("fault_history")
    if not isinstance(history, list):
        reasons.append("missing-fault-history")
        history = []
    state_counts: dict[str, int] = {}
    previous = "0" * 64
    previous_time = -1
    for index, raw in enumerate(history, start=1):
        if not isinstance(raw, Mapping):
            reasons.append("invalid-fault-history")
            continue
        if set(raw) != _EVENT_FIELDS:
            reasons.append("invalid-event-fields")
        state = raw.get("state")
        action = raw.get("action")
        if not (
            (isinstance(state, str) and action is None)
            or (state is None and action in {"cleanup-requested", "cleanup-confirmed"})
        ):
            reasons.append("invalid-event-state-action")
        if isinstance(state, str):
            event_evidence = (
                raw.get("evidence") if isinstance(raw.get("evidence"), Mapping) else {}
            )
            try:
                normalize_evidence(
                    FaultEventState(state),
                    event_evidence,
                )
            except (TypeError, ValueError):
                reasons.append("invalid-state-evidence")
            expected_evidence = _PRODUCTION_EVIDENCE_FIELDS.get(state)
            if expected_evidence is not None and set(event_evidence) != expected_evidence:
                reasons.append("invalid-state-evidence-fields")
        elif action == "cleanup-requested":
            event_evidence = raw.get("evidence")
            digest_fields = (
                "authorization_digest",
                "nonce_digest",
                "request_digest",
            )
            if (
                not isinstance(event_evidence, Mapping)
                or set(event_evidence)
                != {
                    *digest_fields,
                    "reservation_id",
                    "attempt_id",
                }
                or any(
                    not isinstance(event_evidence.get(key), str)
                    or re.fullmatch(r"[0-9a-f]{64}", event_evidence[key]) is None
                    for key in digest_fields
                )
                or any(
                    not isinstance(event_evidence.get(key), int)
                    or isinstance(event_evidence.get(key), bool)
                    or event_evidence[key] <= 0
                    for key in ("reservation_id", "attempt_id")
                )
            ):
                reasons.append("invalid-action-evidence")
        elif action == "cleanup-confirmed":
            event_evidence = raw.get("evidence")
            if (
                not isinstance(event_evidence, Mapping)
                or set(event_evidence)
                != {
                    "cleaned_event_hash",
                    "cleanup_id",
                    "memory_cleared_at_ms",
                    "receipt_commit_confirmed_at_ms",
                }
                or not isinstance(event_evidence.get("cleaned_event_hash"), str)
                or re.fullmatch(
                    r"[0-9a-f]{64}", event_evidence["cleaned_event_hash"]
                )
                is None
                or not isinstance(event_evidence.get("cleanup_id"), str)
                or not event_evidence["cleanup_id"]
                or any(
                    not isinstance(event_evidence.get(key), int)
                    or isinstance(event_evidence.get(key), bool)
                    or event_evidence[key] < 0
                    for key in (
                        "memory_cleared_at_ms",
                        "receipt_commit_confirmed_at_ms",
                    )
                )
            ):
                reasons.append("invalid-action-evidence")
        if raw.get("fault_id") != intent.get("fault_id"):
            reasons.append("event-fault-id-mismatch")
        if isinstance(state, str):
            state_counts[state] = state_counts.get(state, 0) + 1
        if raw.get("sequence") != index:
            reasons.append("event-sequence-mismatch")
        if raw.get("previous_hash") != previous or raw.get("event_hash") != _event_digest(raw):
            reasons.append("event-hash-mismatch")
        occurred = raw.get("occurred_at_ms")
        if not isinstance(occurred, int) or isinstance(occurred, bool) or occurred < previous_time:
            reasons.append("event-time-invalid")
        else:
            previous_time = occurred
        previous = str(raw.get("event_hash", ""))
    if evidence.get("fault_history_tail_hash") != previous:
        reasons.append("history-tail-hash-mismatch")

    cleanup_confirmations = [
        item
        for item in history
        if isinstance(item, Mapping) and item.get("action") == "cleanup-confirmed"
    ]
    cleaned_events = [
        item
        for item in history
        if isinstance(item, Mapping) and item.get("state") == "cleaned"
    ]
    if len(cleanup_confirmations) != 1 or len(cleaned_events) != 1:
        reasons.append("cleanup-confirmation-missing")
    else:
        confirmation = cleanup_confirmations[0]
        confirmation_evidence = confirmation.get("evidence")
        cleaned = cleaned_events[0]
        cleaned_evidence = cleaned.get("evidence")
        if (
            not isinstance(confirmation_evidence, Mapping)
            or not isinstance(cleaned_evidence, Mapping)
            or set(confirmation_evidence)
            != {
                "cleaned_event_hash",
                "cleanup_id",
                "memory_cleared_at_ms",
                "receipt_commit_confirmed_at_ms",
            }
            or confirmation_evidence.get("cleaned_event_hash")
            != cleaned.get("event_hash")
            or confirmation_evidence.get("cleanup_id")
            != cleaned_evidence.get("cleanup_id")
            or str(confirmation_evidence.get("memory_cleared_at_ms"))
            != str(cleaned_evidence.get("memory_cleared_at_ms"))
            or confirmation_evidence.get("receipt_commit_confirmed_at_ms")
            != confirmation.get("occurred_at_ms")
        ):
            reasons.append("cleanup-confirmation-invalid")

    expected_states = (
        "authorized",
        "armed",
        "injected",
        "detected",
        "contained",
        "cleaned",
        "recovered",
    )
    exact_states = [
        item.get("state")
        for item in history
        if isinstance(item, Mapping) and item.get("state") is not None
    ]
    required_states = list(expected_states) + (["verified"] if mode == "final" else [])
    if exact_states != required_states:
        reasons.append("lifecycle-state-machine-invalid")
    for state in expected_states:
        count = state_counts.get(state, 0)
        state_label = {
            "injected": "injection",
            "detected": "detection",
            "cleaned": "cleanup",
            "recovered": "recovery",
        }.get(state, state)
        if count == 0:
            reasons.append(f"missing-{state_label}")
        elif count > 1:
            reasons.append(f"duplicate-{state_label}")
    positions = {
        state: next(
            (index for index, item in enumerate(history) if isinstance(item, Mapping)
             and item.get("state") == state),
            -1,
        )
        for state in expected_states
    }
    if all(positions[state] >= 0 for state in expected_states):
        if [positions[state] for state in expected_states] != sorted(
            positions[state] for state in expected_states
        ):
            reasons.append("lifecycle-order-invalid")
        injection_time = history[positions["injected"]].get("occurred_at_ms")
        cleanup_time = history[positions["cleaned"]].get("occurred_at_ms")
        recovery_time = history[positions["recovered"]].get("occurred_at_ms")
        if not (
            isinstance(injection_time, int)
            and isinstance(cleanup_time, int)
            and isinstance(recovery_time, int)
            and injection_time < cleanup_time < recovery_time
        ):
            reasons.append("cleanup-recovery-order-invalid")
    detected_events = [
        item for item in history
        if isinstance(item, Mapping) and item.get("state") == "detected"
    ]
    if len(detected_events) == 1:
        detection_evidence = detected_events[0].get("evidence")
        expected_detection_key = (
            "coverage_id" if intent.get("kind") == "gamma-partial" else "incident_id"
        )
        if (
            not isinstance(detection_evidence, Mapping)
            or set(detection_evidence) != {expected_detection_key}
            or not isinstance(detection_evidence.get(expected_detection_key), str)
            or not detection_evidence.get(expected_detection_key)
        ):
            reasons.append("detection-identity-mismatch")
    armed_events = [
        item for item in history
        if isinstance(item, Mapping) and item.get("state") == "armed"
    ]
    if len(armed_events) == 1:
        armed_evidence = armed_events[0].get("evidence")
        if (
            not isinstance(armed_evidence, Mapping)
            or armed_evidence.get("runtime_identity_digest")
            != canonical_digest(runtime)
            or not isinstance(armed_evidence.get("ownership_digest"), str)
            or len(armed_evidence.get("ownership_digest", "")) != 64
        ):
            reasons.append("armed-runtime-digest-mismatch")
    injected_events = [
        item for item in history
        if isinstance(item, Mapping) and item.get("state") == "injected"
    ]
    call_id: object = None
    if len(injected_events) == 1:
        injected_evidence = injected_events[0].get("evidence")
        call_id = (
            injected_evidence.get("call_id")
            if isinstance(injected_evidence, Mapping)
            else None
        )
        if (
            not isinstance(call_id, str)
            or set(injected_evidence) != {"call_id", "call_binding_digest"}
            or injected_evidence.get("call_binding_digest")
            != fault_call_binding_digest(
                fault_id=str(intent.get("fault_id")),
                kind=str(intent.get("kind")),
                call_class=str(intent.get("call_class")),
                target_key=str(intent.get("target_key")),
                runtime=runtime,
                call_id=call_id,
            )
        ):
            reasons.append("injected-call-binding-mismatch")

    detection_receipt = evidence.get("detection_receipt")
    detected_evidence = (
        detected_events[0].get("evidence") if len(detected_events) == 1 else None
    )
    detection_id = None
    if isinstance(detected_evidence, Mapping):
        detection_id = next(iter(detected_evidence.values()), None)
    if (
        not isinstance(detection_receipt, Mapping)
        or set(detection_receipt)
        != {
            "detection_id", "kind", "call_class", "target_key", "runtime",
            "source_kind", "source_checkpoint", "source_history",
        }
        or detection_receipt.get("detection_id") != detection_id
        or detection_receipt.get("kind") != intent.get("kind")
        or detection_receipt.get("call_class") != intent.get("call_class")
        or detection_receipt.get("target_key") != intent.get("target_key")
        or detection_receipt.get("runtime") != runtime
        or detection_receipt.get("source_kind")
        != (
            "coverage:partial-or-rejected-page"
            if intent.get("kind") == "gamma-partial"
            else intent.get("kind")
        )
    ):
        reasons.append("detection-source-binding-mismatch")
    elif not _source_history_valid(
        detection_receipt,
        intent=intent,
        runtime=runtime,
        injected_call_id=call_id,
    ):
        reasons.append("detection-source-history-invalid")

    component = runtime.get("component")
    receipt = evidence.get("recovery_writer_receipt")
    expected_table = _RECOVERY_TABLES.get(str(component))
    recovered_events = [
        item for item in history
        if isinstance(item, Mapping) and item.get("state") == "recovered"
    ]
    recovered_event = recovered_events[0] if len(recovered_events) == 1 else None
    recovered_evidence = (
        recovered_event.get("evidence")
        if isinstance(recovered_event, Mapping)
        and isinstance(recovered_event.get("evidence"), Mapping)
        else {}
    )
    expected_recovery_id = recovered_evidence.get("recovery_id")
    expected_prefix = _RECOVERY_PREFIXES.get(str(component))
    if not isinstance(receipt, Mapping):
        reasons.append("missing-recovery-writer")
    elif (
        set(receipt)
        != {
            "component",
            "fault_id",
            "occurred_at_ms",
            "recovery_id",
            "row_id",
            "runtime",
            "table",
            "target_key",
        }
        or receipt.get("component") != component
        or receipt.get("table") != expected_table
        or receipt.get("fault_id") != intent.get("fault_id")
        or receipt.get("target_key") != intent.get("target_key")
        or receipt.get("runtime") != runtime
        or receipt.get("recovery_id") != expected_recovery_id
        or not (
            (
                isinstance(receipt.get("row_id"), int)
                and not isinstance(receipt.get("row_id"), bool)
                and receipt.get("row_id", 0) > 0
            )
            or (isinstance(receipt.get("row_id"), str) and bool(receipt.get("row_id")))
        )
        or not isinstance(expected_recovery_id, str)
        or not isinstance(expected_prefix, str)
        or expected_recovery_id
        != f"{expected_prefix}{receipt.get('row_id')}"
        or type(receipt.get("occurred_at_ms")) is not int
        or (
            isinstance(recovered_event, Mapping)
            and receipt["occurred_at_ms"] > recovered_event.get("occurred_at_ms", -1)
        )
    ):
        reasons.append("recovery-family-mismatch")
    if isinstance(receipt, Mapping) and history:
        cleanup_time = next(
            (item.get("occurred_at_ms") for item in history
             if isinstance(item, Mapping) and item.get("state") == "cleaned"),
            None,
        )
        if not isinstance(cleanup_time, int) or receipt.get("occurred_at_ms", -1) <= cleanup_time:
            reasons.append("recovery-not-newer-than-cleanup")

    for field, reason in (
        ("open_injection_fault_count", "open-injection-fault"),
        ("open_incident_count", "open-incident"),
        ("cross_membership_quote_batches", "cross-membership-quote"),
        ("partial_publication_count", "partial-publication"),
        ("orphan_collecting_runs", "orphan-collecting-run"),
    ):
        if evidence.get(field) != 0:
            reasons.append(reason)
    expected_pending = 1 if mode == "candidate" else 0
    if evidence.get("pending_verification_fault_count") != expected_pending:
        reasons.append("pending-verification-fault")
    if evidence.get("source_projection_active") is not (mode == "candidate"):
        reasons.append("source-projection-active-mismatch")
    for field, reason in (
        ("freshness_gate", "freshness-gate"),
        ("reconciliation_gate", "reconciliation-gate"),
    ):
        if evidence.get(field) is not True:
            reasons.append(reason)
    if (
        type(evidence.get("source_valid_until_ms")) is not int
        or evidence["source_valid_until_ms"] <= 0
    ):
        reasons.append("source-valid-until-invalid")

    if mode not in {"candidate", "final"}:
        reasons.append("invalid-evaluator-mode")
    if mode == "final":
        verified = [
            item for item in history
            if isinstance(item, Mapping) and item.get("state") == "verified"
        ]
        if not verified:
            reasons.append("missing-verified")
        elif len(verified) != 1:
            reasons.append("duplicate-verified")
        if not isinstance(candidate_artifact, Mapping):
            reasons.append("missing-candidate-verdict")
        else:
            public_key = os.getenv(
                "POLYARB_UPSTREAM_FAULT_EVALUATOR_PUBLIC_KEY", ""
            )
            try:
                _source_kid, source_key = load_public_key(source_public_key)
                _evaluator_kid, evaluator_key = load_public_key(public_key)
                if source_key.public_bytes(
                    Encoding.Raw, PublicFormat.Raw
                ) == evaluator_key.public_bytes(Encoding.Raw, PublicFormat.Raw):
                    reasons.append("authority-role-collision")
            except ValueError:
                pass
            unsigned = {
                key: value
                for key, value in candidate_artifact.items()
                if key not in {"artifact_digest", "signature"}
            }
            artifact_digest = canonical_digest(unsigned)
            if (
                not public_key
                or candidate_artifact.get("artifact_digest") != artifact_digest
                or not verify_digest(
                    public_key,
                    kid=candidate_artifact.get("signature_kid"),
                    version=candidate_artifact.get("signature_version"),
                    digest=artifact_digest,
                    signature=candidate_artifact.get("signature"),
                )
            ):
                reasons.append("candidate-verdict-signature-mismatch")
            if (
                candidate_artifact.get("fault_id") != intent.get("fault_id")
                or candidate_artifact.get("runtime") != runtime
                or candidate_artifact.get("source_tail_hash")
                != (
                    verified[0].get("previous_hash")
                    if len(verified) == 1 and isinstance(verified[0], Mapping)
                    else None
                )
            ):
                reasons.append("candidate-verdict-source-mismatch")
        if isinstance(candidate_artifact, Mapping) and verified:
            verified_evidence = verified[0].get("evidence")
            if (
                not isinstance(verified_evidence, Mapping)
                or verified_evidence.get("verdict_id") != candidate_artifact.get("verdict_id")
                or verified_evidence.get("verdict_digest")
                != candidate_artifact.get("artifact_digest")
            ):
                reasons.append("verified-verdict-mismatch")
    return QualificationVerdict("PASS" if not reasons else "FAIL", tuple(dict.fromkeys(reasons)))


def build_candidate_artifact(
    evidence: Mapping[str, Any],
    *,
    source_bytes: bytes | None = None,
) -> dict[str, object]:
    """Create a signed PASS candidate; local fixtures can never be signed."""
    verdict = evaluate_fault_envelope(evidence, mode="candidate")
    if verdict.status != "PASS":
        raise ValueError("candidate-evidence-failed")
    private_key = os.getenv(
        "POLYARB_UPSTREAM_FAULT_EVALUATOR_PRIVATE_KEY", ""
    )
    if not private_key:
        raise ValueError("evaluator-authority-unavailable")
    source_public_key = os.getenv(
        "POLYARB_UPSTREAM_FAULT_SOURCE_PUBLIC_KEY", ""
    )
    try:
        _source_kid, source_key = load_public_key(source_public_key)
        _evaluator_kid, evaluator_key = load_private_key(private_key)
    except ValueError as exc:
        raise ValueError("evaluator-authority-unavailable") from exc
    if source_key.public_bytes(
        Encoding.Raw, PublicFormat.Raw
    ) == evaluator_key.public_key().public_bytes(
        Encoding.Raw, PublicFormat.Raw
    ):
        raise ValueError("authority-role-collision")
    source_digest = hashlib.sha256(
        _canonical_json(evidence) if source_bytes is None else source_bytes
    ).hexdigest()
    intent = evidence["fault_intent"]
    source_authority = evidence["source_authority"]
    assert isinstance(intent, Mapping)
    assert isinstance(source_authority, Mapping)
    unsigned: dict[str, object] = {
        "schema_version": 1,
        "scope": "production-fault",
        "mode": "candidate",
        "status": "PASS",
        "source_evidence_sha256": f"sha256:{source_digest}",
        "source_envelope_digest": source_authority["source_facts_digest"],
        "source_valid_until_ms": evidence["source_valid_until_ms"],
        "fault_id": intent["fault_id"],
        "runtime": intent["runtime"],
        "source_tail_hash": evidence["fault_history_tail_hash"],
    }
    unsigned["verdict_id"] = f"verdict-{canonical_digest(unsigned)[:32]}"
    kid, _ = load_private_key(private_key)
    unsigned["signature_version"] = SIGNATURE_VERSION
    unsigned["signature_kid"] = kid
    artifact_digest = canonical_digest(unsigned)
    unsigned["artifact_digest"] = artifact_digest
    _signed_kid, signature = sign_digest(private_key, artifact_digest)
    assert _signed_kid == kid
    unsigned["signature"] = signature
    return unsigned


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


def _validate_provenance(
    evidence: Mapping[str, Any],
    *,
    required_scope: str,
    expected_release: str | None,
    reasons: list[str],
) -> None:
    if evidence.get("evidence_schema_version") != 1:
        reasons.append("invalid-evidence-schema-version")
    if evidence.get("scope") != required_scope:
        reasons.append("scope-mismatch")
    if evidence.get("app_id") != "polyarb-l1":
        reasons.append("invalid-app-id")
    if not required_scope.startswith("production-"):
        return

    release_id = evidence.get("release_id")
    if not isinstance(release_id, str) or re.fullmatch(r"[0-9a-f]{40}", release_id) is None:
        reasons.append("invalid-release-id")
    elif expected_release is not None and release_id != expected_release:
        reasons.append("release-mismatch")

    machine_id = evidence.get("machine_id")
    if not isinstance(machine_id, str) or not machine_id or machine_id == "local":
        reasons.append("invalid-machine-id")

    boot_id = evidence.get("boot_id")
    try:
        parsed_boot = UUID(str(boot_id))
        if parsed_boot.version != 4:
            raise ValueError
    except (AttributeError, TypeError, ValueError):
        reasons.append("invalid-boot-id")

    started_at_ms = evidence.get("window_started_at_ms")
    ended_at_ms = evidence.get("window_ended_at_ms")
    if (
        not isinstance(started_at_ms, int)
        or isinstance(started_at_ms, bool)
        or not isinstance(ended_at_ms, int)
        or isinstance(ended_at_ms, bool)
        or started_at_ms < 0
        or ended_at_ms < started_at_ms
    ):
        reasons.append("invalid-evidence-window")

    sample_count = evidence.get("sample_count")
    if (
        not isinstance(sample_count, int)
        or isinstance(sample_count, bool)
        or sample_count < 5
    ):
        reasons.append("invalid-sample-count")


def evaluate(
    evidence: Mapping[str, Any],
    *,
    required_scope: str | None = None,
    expected_release: str | None = None,
) -> QualificationVerdict:
    reasons: list[str] = []
    if required_scope is not None:
        _validate_provenance(
            evidence,
            required_scope=required_scope,
            expected_release=expected_release,
            reasons=reasons,
        )
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
    open_incidents = _validated_count(
        evidence,
        "open_incident_count",
        reasons,
    )
    if open_incidents:
        reasons.append("open-incident")

    incidents = evidence.get("incidents")
    if isinstance(incidents, list):
        for incident in incidents:
            if not isinstance(incident, Mapping):
                reasons.append("invalid-incidents")
                break
            state = incident.get("state")
            incident_id = incident.get("incident_id")
            component = incident.get("component")
            valid_incident_id = (
                isinstance(incident_id, int)
                and not isinstance(incident_id, bool)
                and incident_id > 0
            ) or (
                isinstance(incident_id, str)
                and re.fullmatch(r"[0-9a-f]{32}", incident_id) is not None
            )
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
                or not valid_incident_id
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
        read_stable_bytes(path).decode("utf-8"),
        parse_constant=_reject_json_constant,
        object_pairs_hook=_reject_duplicate_keys,
    )
    if type(payload) is not dict:
        raise ValueError("evidence root must be an object")
    return payload


def _write_exclusive(path: Path, payload: Mapping[str, Any]) -> None:
    write_exclusive_bytes(path, _canonical_json(payload) + b"\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--require-scope",
        choices=("local-conformance", "production-readonly", "production-fault"),
        required=True,
    )
    parser.add_argument("--expected-release")
    parser.add_argument("--fault-mode", choices=("candidate", "final"))
    parser.add_argument("--candidate-artifact", type=Path)
    args = parser.parse_args(argv)
    if args.require_scope.startswith("production-") and args.expected_release is None:
        parser.error("--expected-release is required for production evidence")
    try:
        evidence_bytes = read_stable_bytes(args.evidence)
        evidence = json.loads(
            evidence_bytes.decode("utf-8"),
            parse_constant=_reject_json_constant,
            object_pairs_hook=_reject_duplicate_keys,
        )
        if type(evidence) is not dict:
            raise ValueError("evidence root must be an object")
        if args.fault_mode is not None:
            if args.require_scope != "production-fault":
                raise ValueError("fault mode requires production-fault scope")
            if args.fault_mode == "candidate":
                if evaluate_fault_envelope(
                    evidence,
                    mode="candidate",
                    expected_release=args.expected_release,
                ).status != "PASS":
                    raise ValueError("candidate-evidence-failed")
                output = build_candidate_artifact(
                    evidence,
                    source_bytes=evidence_bytes,
                )
                _write_exclusive(args.output, output)
                return 0
            if args.candidate_artifact is None:
                raise ValueError("final mode requires candidate artifact")
            candidate = _read_evidence(args.candidate_artifact)
            fault_verdict = evaluate_fault_envelope(
                evidence,
                mode="final",
                candidate_artifact=candidate,
                expected_release=args.expected_release,
            )
            output = {
                "candidate_verdict_id": candidate.get("verdict_id"),
                "evidence_sha256": f"sha256:{canonical_digest(evidence)}",
                "mode": "final",
                "reasons": list(fault_verdict.reasons),
                "schema_version": 1,
                "status": fault_verdict.status,
            }
            _write_exclusive(args.output, output)
            return 0 if fault_verdict.status == "PASS" else 1
        verdict = evaluate(
            evidence,
            required_scope=args.require_scope,
            expected_release=args.expected_release,
        )
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
