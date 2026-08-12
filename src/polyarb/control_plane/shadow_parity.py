"""Offline, fail-closed acceptance verifier for transactional shadow parity."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence


class ShadowParityError(ValueError):
    """Shadow evidence is insufficient for a reversible pointer authorization."""


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMPONENTS = frozenset(
    {"events", "event_tags", "memberships", "group_truth", "markets", "issues"}
)


def verify_shadow_parity(evidence: Mapping[str, object]) -> dict[str, object]:
    """Verify exactly three complete, non-mutating Structure/Quote shadow runs."""
    runs = evidence.get("runs")
    if not isinstance(runs, Sequence) or isinstance(runs, (str, bytes)) or len(runs) != 3:
        raise ShadowParityError("shadow parity requires exactly three runs")
    verified: list[str] = []
    for run in runs:
        if not isinstance(run, Mapping):
            raise ShadowParityError("shadow parity run must be an object")
        run_id = _string(run.get("run_id"), "run_id")
        if run_id in verified:
            raise ShadowParityError("shadow parity run_id must be unique")
        legacy = _mapping(run.get("legacy"), "legacy")
        transactional = _mapping(run.get("transactional"), "transactional")
        _verify_run(legacy=legacy, transactional=transactional)
        verified.append(run_id)
    return {
        "status": "PASS",
        "required_runs": 3,
        "verified_runs": verified,
        "legacy_pointer_mutations": 0,
    }


def _verify_run(*, legacy: Mapping[str, object], transactional: Mapping[str, object]) -> None:
    legacy_source = _mapping(legacy.get("source_identity"), "legacy source_identity")
    transactional_source = _mapping(
        transactional.get("source_identity"), "transactional source_identity"
    )
    _verify_source_identity(legacy_source)
    _verify_source_identity(transactional_source)
    if legacy_source != transactional_source:
        raise ShadowParityError("source identity mismatch")
    legacy_bundle = _digest(legacy.get("bundle_digest"), "legacy bundle_digest")
    transactional_bundle = _digest(
        transactional.get("bundle_digest"), "transactional bundle_digest"
    )
    if legacy_bundle != transactional_bundle:
        raise ShadowParityError("bundle digest mismatch")
    _digest(transactional.get("manifest_digest"), "transactional manifest_digest")
    if _counts(legacy.get("component_counts")) != _counts(transactional.get("component_counts")):
        raise ShadowParityError("component count mismatch")
    legacy_universe = _digest(legacy.get("quote_universe_hash"), "legacy quote_universe_hash")
    transactional_universe = _digest(
        transactional.get("quote_universe_hash"), "transactional quote_universe_hash"
    )
    if legacy_universe != transactional_universe:
        raise ShadowParityError("Quote universe mismatch")
    mutations = transactional.get("legacy_pointer_mutations")
    if mutations != 0:
        raise ShadowParityError("legacy pointer mutation is forbidden")


def _verify_source_identity(value: Mapping[str, object]) -> None:
    for field in ("publication_id", "window_id"):
        _string(value.get(field), f"source_identity {field}")
    snapshot_id = value.get("snapshot_id")
    if isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or snapshot_id < 0:
        raise ShadowParityError("source_identity snapshot_id is invalid")
    _digest(value.get("comparison_receipt_digest"), "source_identity comparison_receipt_digest")


def _counts(value: object) -> dict[str, int]:
    counts = _mapping(value, "component_counts")
    if set(counts) != _COMPONENTS:
        raise ShadowParityError("component_counts does not cover the six Structure components")
    normalized: dict[str, int] = {}
    for component, count in counts.items():
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ShadowParityError("component_counts contains invalid count")
        normalized[component] = count
    return normalized


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ShadowParityError(f"{name} must be an object")
    return value  # type: ignore[return-value]


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ShadowParityError(f"{name} must be non-empty")
    return value


def _digest(value: object, name: str) -> str:
    digest = _string(value, name)
    if not _SHA256.fullmatch(digest):
        raise ShadowParityError(f"{name} must be a sha256 digest")
    return digest
