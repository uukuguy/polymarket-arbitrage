"""Offline, fail-closed verification of cloud transactional fault/soak evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence


class FaultSoakError(ValueError):
    """Cloud acceptance evidence is not sufficient for M1 production readiness."""


_MIN_SOAK_SECONDS = 86_400
_MAX_LEASE_RECLAIM_SLA_SECONDS = 180
_WORKERS = frozenset({"structure", "quote"})
_CIRCUIT_PROBE_DELAYS = (15, 30, 60)
_RECOVERY_DELIVERY_CHANNELS = frozenset({"dashboard", "telegram"})


def verify_fault_soak(evidence: Mapping[str, object]) -> dict[str, object]:
    """Require both crash-takeovers and one uninterrupted 24-hour evidence window."""
    takeovers = evidence.get("takeovers")
    if not isinstance(takeovers, Sequence) or isinstance(takeovers, (str, bytes)):
        raise FaultSoakError("takeovers must be an array")
    verified_workers: set[str] = set()
    recovered_circuit_workers: set[str] = set()
    for takeover in takeovers:
        record = _mapping(takeover, "takeover")
        worker = _string(record.get("worker"), "takeover worker")
        if worker not in _WORKERS:
            raise FaultSoakError("takeover worker is invalid")
        if worker in verified_workers:
            raise FaultSoakError("takeover worker must be unique")
        verified_workers.add(worker)
        if record.get("crash_boundary") != "r2-upload-before-receipt":
            raise FaultSoakError("takeover must prove r2-upload-before-receipt")
        reclaimed = _positive_int(record.get("lease_reclaimed_seconds"), "lease reclaim")
        reclaim_sla = _positive_int(record.get("lease_reclaim_sla_seconds"), "lease reclaim SLA")
        if reclaim_sla > _MAX_LEASE_RECLAIM_SLA_SECONDS or reclaimed > reclaim_sla:
            raise FaultSoakError("lease reclaim exceeds bounded SLA")
        if record.get("old_certified_truth_available") is not True:
            raise FaultSoakError("old certified truth must remain available")
        if record.get("control_api_readable") is not True:
            raise FaultSoakError("control API must remain readable during worker loss")
        _verify_circuit_recovery(record.get("circuit"), worker)
        recovered_circuit_workers.add(worker)
    if verified_workers != _WORKERS:
        raise FaultSoakError("fault acceptance requires Structure and Quote takeovers")
    if recovered_circuit_workers != _WORKERS:
        raise FaultSoakError("fault acceptance requires Structure and Quote circuit recoveries")
    soak = _mapping(evidence.get("soak"), "soak")
    duration = _positive_int(soak.get("duration_seconds"), "soak duration")
    if duration < _MIN_SOAK_SECONDS:
        raise FaultSoakError("soak must cover at least 24 hours")
    if _positive_int(soak.get("scheduler_ticks"), "scheduler_ticks") <= 0:
        raise FaultSoakError("soak must contain scheduler ticks")
    if soak.get("control_api_readable") is not True:
        raise FaultSoakError("control API must remain readable throughout soak")
    for field in ("manual_unlocks", "silent_stops", "permanent_degradations"):
        if soak.get(field) != 0:
            raise FaultSoakError(f"soak {field} must be zero")
    if soak.get("circuit_recoveries") != len(_WORKERS):
        raise FaultSoakError("soak must include both circuit recoveries")
    return {
        "status": "PASS",
        "takeover_workers": sorted(verified_workers),
        "minimum_soak_seconds": _MIN_SOAK_SECONDS,
        "observed_soak_seconds": duration,
        "manual_unlocks": 0,
        "circuit_recovery_workers": sorted(recovered_circuit_workers),
    }


def _verify_circuit_recovery(value: object, worker: str) -> None:
    circuit = _mapping(value, "circuit")
    job_key = _string(circuit.get("job_key"), "circuit job key")
    if (
        not job_key.startswith(f"{worker}:")
        and not (worker == "structure" and job_key.startswith("structure:"))
        and not (worker == "quote" and job_key.startswith("quote:"))
    ):
        raise FaultSoakError("circuit job key does not match worker")
    if circuit.get("opened_after_failures") != 3:
        raise FaultSoakError("circuit must open after three failures")
    delays = circuit.get("probe_delays_seconds")
    if not isinstance(delays, list) or tuple(delays) != _CIRCUIT_PROBE_DELAYS:
        raise FaultSoakError("circuit probes must be 15/30/60 seconds")
    _string(circuit.get("replacement_worker"), "replacement worker")
    if circuit.get("recovery_event_kind") != "recovered":
        raise FaultSoakError("circuit recovery event must be recovered")
    if circuit.get("incident_resolved") is not True:
        raise FaultSoakError("circuit incident resolved evidence is required")
    receipts = circuit.get("delivery_receipts")
    if not isinstance(receipts, list) or set(receipts) != _RECOVERY_DELIVERY_CHANNELS:
        raise FaultSoakError("circuit recovery delivery receipts must cover dashboard and telegram")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise FaultSoakError(f"{name} must be an object")
    return value  # type: ignore[return-value]


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise FaultSoakError(f"{name} must be non-empty")
    return value


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FaultSoakError(f"{name} must be positive")
    return value
