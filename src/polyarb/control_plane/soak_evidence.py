"""Append-only, fail-closed evidence for transactional M1 soak windows."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any


class SoakEvidenceError(ValueError):
    """A saved soak window cannot prove continuous healthy operation."""


_KIND = "m1-transactional-soak-v1"


def _canonical_bytes(payload: Mapping[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _observed_at(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise SoakEvidenceError("observed_at is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SoakEvidenceError("observed_at must be timezone-aware")
    return parsed.astimezone(UTC)


def create_record(
    *,
    observed_at: str,
    control_api_url: str,
    machine_states: Mapping[str, str],
    control_snapshot: Mapping[str, object],
) -> dict[str, object]:
    """Create one canonical, self-authenticating read-only observation."""
    _observed_at(observed_at)
    if not control_api_url or not machine_states or len(set(machine_states)) != len(machine_states):
        raise SoakEvidenceError("control API URL and unique machine states are required")
    if any(not machine_id or not state for machine_id, state in machine_states.items()):
        raise SoakEvidenceError("machine states must have non-empty identities")
    payload: dict[str, object] = {
        "kind": _KIND,
        "observed_at": observed_at,
        "control_api_url": control_api_url,
        "machine_states": dict(sorted(machine_states.items())),
        "control_api_status": control_snapshot.get("status"),
        "queue_health": control_snapshot.get("queue_health"),
        "expired_leases": control_snapshot.get("expired_leases"),
        "open_circuit_count": control_snapshot.get("open_circuit_count"),
    }
    return {**payload, "snapshot_sha256": sha256(_canonical_bytes(payload)).hexdigest()}


def append_record(path: Path, record: Mapping[str, object], *, exclusive: bool = False) -> None:
    """Append one validated record, creating a new evidence file only once."""
    _validated(record)
    mode = "x" if exclusive else "a"
    with path.open(mode, encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def read_records(path: Path) -> tuple[dict[str, object], ...]:
    """Read every JSONL observation and reject blank or malformed evidence."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise SoakEvidenceError("soak evidence is unreadable") from error
    if not lines:
        raise SoakEvidenceError("soak evidence is empty")
    records: list[dict[str, object]] = []
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise SoakEvidenceError("soak evidence contains invalid JSON") from error
        if not isinstance(record, dict):
            raise SoakEvidenceError("soak evidence record must be an object")
        _validated(record)
        records.append(record)
    return tuple(records)


def verify_soak(
    records: Sequence[Mapping[str, object]],
    *,
    minimum_seconds: int = 86_400,
    max_gap_seconds: int = 900,
) -> dict[str, int | str]:
    """Verify one immutable baseline and an uninterrupted healthy window."""
    if minimum_seconds <= 0 or max_gap_seconds <= 0 or len(records) < 2:
        raise SoakEvidenceError("soak needs at least two records and positive bounds")
    validated = [_validated(record) for record in records]
    baseline = validated[0]
    times = [_observed_at(str(record["observed_at"])) for record in validated]
    for previous, current in zip(times, times[1:]):
        gap = (current - previous).total_seconds()
        if gap <= 0:
            raise SoakEvidenceError("sample times must be strictly increasing")
        if gap > max_gap_seconds:
            raise SoakEvidenceError("sample gap exceeds configured maximum")
    machine_states = baseline["machine_states"]
    for record in validated:
        if record["control_api_url"] != baseline["control_api_url"]:
            raise SoakEvidenceError("control API identity changed")
        if record["machine_states"].keys() != machine_states.keys():
            raise SoakEvidenceError("machine identity changed")
        if record["control_api_status"] != "available":
            raise SoakEvidenceError("control API is unavailable")
        if any(state != "started" for state in record["machine_states"].values()):
            raise SoakEvidenceError("all Machines must remain started")
        if int(record["expired_leases"]) > int(baseline["expired_leases"]):
            raise SoakEvidenceError("expired lease count increased")
        if int(record["open_circuit_count"]) > int(baseline["open_circuit_count"]):
            raise SoakEvidenceError("open circuit count increased")
    duration = int((times[-1] - times[0]).total_seconds())
    if duration < minimum_seconds:
        raise SoakEvidenceError("soak must cover at least 24 hours")
    return {
        "status": "PASS",
        "duration_seconds": duration,
        "scheduler_ticks": len(validated),
        "machine_count": len(machine_states),
    }


def _validated(record: Mapping[str, object]) -> dict[str, Any]:
    payload = dict(record)
    digest = payload.pop("snapshot_sha256", None)
    required = {
        "kind", "observed_at", "control_api_url", "machine_states", "control_api_status",
        "queue_health", "expired_leases", "open_circuit_count",
    }
    if set(payload) != required or payload.get("kind") != _KIND or not isinstance(digest, str):
        raise SoakEvidenceError("soak record shape is invalid")
    if sha256(_canonical_bytes(payload)).hexdigest() != digest:
        raise SoakEvidenceError("soak record digest is invalid")
    _observed_at(str(payload["observed_at"]))
    states = payload["machine_states"]
    if not isinstance(states, dict) or not states or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in states.items()
    ):
        raise SoakEvidenceError("machine states are invalid")
    for field in ("expired_leases", "open_circuit_count"):
        if (
            isinstance(payload[field], bool)
            or not isinstance(payload[field], int)
            or payload[field] < 0
        ):
            raise SoakEvidenceError(f"{field} is invalid")
    return payload
