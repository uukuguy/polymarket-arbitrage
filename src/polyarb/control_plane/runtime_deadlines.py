"""Single-source lifecycle deadlines for every transactional M1 job type."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from .runtime_contract import RUNTIME_STAGE_REGISTRY
from .runtime_models import RuntimeDeadlineProfile


@dataclass(frozen=True, slots=True)
class _RuntimePolicySpec:
    attempt_multiplier: int | None
    retry_budget: int
    checkpoint_interval: int
    retry_backoff_base_seconds: int = 15
    retry_backoff_cap_seconds: int = 300
    attempt_ceiling_seconds: int | None = None

    def __post_init__(self) -> None:
        if (self.attempt_multiplier is None) == (self.attempt_ceiling_seconds is None):
            raise ValueError("runtime policy must choose one attempt authority")
        attempt_value = (
            self.attempt_multiplier
            if self.attempt_multiplier is not None
            else self.attempt_ceiling_seconds
        )
        assert attempt_value is not None
        if min(attempt_value, self.retry_budget, self.checkpoint_interval) <= 0:
            raise ValueError("runtime policy spec values must be positive")


@dataclass(frozen=True, slots=True)
class RuntimeRetryPolicy:
    """Lease-independent durable retry/circuit policy for one job type."""

    job_type: str
    retry_budget: int
    retry_backoff_base_seconds: int
    retry_backoff_cap_seconds: int

    def __post_init__(self) -> None:
        if self.job_type not in RUNTIME_STAGE_REGISTRY:
            raise ValueError("unknown runtime job type")
        if (
            min(
                self.retry_budget,
                self.retry_backoff_base_seconds,
                self.retry_backoff_cap_seconds,
            )
            <= 0
        ):
            raise ValueError("runtime retry policy values must be positive")
        if self.retry_backoff_base_seconds > self.retry_backoff_cap_seconds:
            raise ValueError("retry backoff base cannot exceed its cap")

    def retry_backoff_seconds(self, failure_count: int) -> int:
        """Return the sole durable exponential backoff for a failed attempt."""
        if failure_count <= 0:
            raise ValueError("failure_count must be positive")
        exponent = min(failure_count - 1, 62)
        return min(
            self.retry_backoff_base_seconds * (2**exponent),
            self.retry_backoff_cap_seconds,
        )


@dataclass(frozen=True, slots=True)
class RuntimePolicy:
    """Resolved lifecycle policy persisted and consumed by one job attempt."""

    job_type: str
    policy_version: str
    deadlines: RuntimeDeadlineProfile
    io_timeout_seconds: int
    terminal_grace_seconds: int
    retry_budget: int
    checkpoint_interval: int
    provider_attempts: int
    provider_timeout_seconds: float
    retry_backoff_base_seconds: int
    retry_backoff_cap_seconds: int

    def __post_init__(self) -> None:
        if self.job_type not in RUNTIME_STAGE_REGISTRY:
            raise ValueError("unknown runtime job type")
        if self.policy_version != self.deadlines.policy_version:
            raise ValueError("runtime policy versions disagree")
        if self.io_timeout_seconds >= self.deadlines.progress_seconds:
            raise ValueError("I/O timeout must be below progress deadline")
        if self.io_timeout_seconds >= self.terminal_grace_seconds:
            raise ValueError("I/O timeout must be below terminal grace")
        if self.provider_attempts != 1:
            raise ValueError("formal runtime provider calls must use one inner attempt")
        if not 0 < self.provider_timeout_seconds < self.io_timeout_seconds:
            raise ValueError("provider timeout must be inside the worker I/O envelope")
        if self.retry_backoff_base_seconds > self.retry_backoff_cap_seconds:
            raise ValueError("retry backoff base cannot exceed its cap")
        if (
            min(
                self.io_timeout_seconds,
                self.terminal_grace_seconds,
                self.retry_budget,
                self.checkpoint_interval,
                self.retry_backoff_base_seconds,
                self.retry_backoff_cap_seconds,
            )
            <= 0
        ):
            raise ValueError("runtime policy values must be positive")

    def retry_backoff_seconds(self, failure_count: int) -> int:
        """Return the single durable exponential backoff for one failed attempt."""
        return RuntimeRetryPolicy(
            job_type=self.job_type,
            retry_budget=self.retry_budget,
            retry_backoff_base_seconds=self.retry_backoff_base_seconds,
            retry_backoff_cap_seconds=self.retry_backoff_cap_seconds,
        ).retry_backoff_seconds(failure_count)


_POLICY_VERSION: Final = "runtime-v2"
_DEFAULT_SPEC = _RuntimePolicySpec(
    attempt_multiplier=10,
    retry_budget=3,
    checkpoint_interval=25,
)
_POLICY_SPECS = MappingProxyType(
    {
        "structure-fetch": _DEFAULT_SPEC,
        "structure-materialize": _DEFAULT_SPEC,
        "structure-normalize": _DEFAULT_SPEC,
        "structure-certify": _RuntimePolicySpec(
            None,
            3,
            25,
            attempt_ceiling_seconds=3_600,
        ),
        "quote-admit": _RuntimePolicySpec(10, 3, 10),
        "quote-batch": _DEFAULT_SPEC,
        "quote-certify": _DEFAULT_SPEC,
        "opportunity-certify": _DEFAULT_SPEC,
    }
)

RUNTIME_JOB_SUCCESSORS = MappingProxyType(
    {
        "structure-fetch": ("structure-materialize",),
        "structure-materialize": ("structure-normalize",),
        "structure-normalize": ("structure-certify",),
        "structure-certify": ("quote-admit",),
        "quote-admit": ("quote-batch",),
        "quote-batch": ("quote-certify",),
        "quote-certify": ("opportunity-certify",),
        "opportunity-certify": (),
    }
)

if tuple(_POLICY_SPECS) != tuple(RUNTIME_STAGE_REGISTRY):
    raise RuntimeError("runtime policy registry must exactly match the stage registry")
if tuple(RUNTIME_JOB_SUCCESSORS) != tuple(RUNTIME_STAGE_REGISTRY):
    raise RuntimeError("runtime DAG must exactly match the stage registry")


def runtime_job_order() -> tuple[str, ...]:
    """Return the validated topological order of the durable runtime DAG."""
    incoming = {job_type: 0 for job_type in RUNTIME_JOB_SUCCESSORS}
    for successors in RUNTIME_JOB_SUCCESSORS.values():
        for successor in successors:
            if successor not in incoming:
                raise RuntimeError(f"runtime DAG names unknown successor {successor!r}")
            incoming[successor] += 1
    ready = [job_type for job_type, count in incoming.items() if count == 0]
    order: list[str] = []
    while ready:
        job_type = ready.pop(0)
        order.append(job_type)
        for successor in RUNTIME_JOB_SUCCESSORS[job_type]:
            incoming[successor] -= 1
            if incoming[successor] == 0:
                ready.append(successor)
    if len(order) != len(incoming):
        raise RuntimeError("runtime DAG contains a cycle")
    return tuple(order)


RUNTIME_JOB_ORDER = runtime_job_order()


def runtime_retry_policy(job_type: str) -> RuntimeRetryPolicy:
    """Resolve retry/circuit authority without inventing a placeholder lease."""
    try:
        spec = _POLICY_SPECS[job_type]
    except KeyError as error:
        raise ValueError(f"unknown runtime job type: {job_type}") from error
    return RuntimeRetryPolicy(
        job_type=job_type,
        retry_budget=spec.retry_budget,
        retry_backoff_base_seconds=spec.retry_backoff_base_seconds,
        retry_backoff_cap_seconds=spec.retry_backoff_cap_seconds,
    )


def runtime_policy(job_type: str, lease_seconds: int) -> RuntimePolicy:
    """Resolve the only accepted lifecycle policy for one claimed job."""
    try:
        spec = _POLICY_SPECS[job_type]
    except KeyError as error:
        raise ValueError(f"unknown runtime job type: {job_type}") from error
    retry_policy = runtime_retry_policy(job_type)
    bounded_lease = max(3, int(lease_seconds))
    heartbeat = max(1, min(30, bounded_lease // 3))
    progress = max(bounded_lease, heartbeat * 3)
    if spec.attempt_ceiling_seconds is not None:
        if progress > spec.attempt_ceiling_seconds:
            raise ValueError("runtime lease exceeds the absolute attempt ceiling")
        attempt = spec.attempt_ceiling_seconds
    else:
        assert spec.attempt_multiplier is not None
        attempt = max(progress, bounded_lease * spec.attempt_multiplier)
    deadlines = RuntimeDeadlineProfile(
        policy_version=_POLICY_VERSION,
        lease_seconds=bounded_lease,
        heartbeat_seconds=heartbeat,
        progress_seconds=progress,
        attempt_seconds=attempt,
    )
    terminal_grace = max(3, min(30, bounded_lease // 2))
    io_timeout = max(1, min(90, progress - 1, terminal_grace - 1))
    return RuntimePolicy(
        job_type=job_type,
        policy_version=_POLICY_VERSION,
        deadlines=deadlines,
        io_timeout_seconds=io_timeout,
        terminal_grace_seconds=terminal_grace,
        retry_budget=retry_policy.retry_budget,
        checkpoint_interval=spec.checkpoint_interval,
        provider_attempts=1,
        provider_timeout_seconds=max(0.5, min(15.0, io_timeout - 0.5)),
        retry_backoff_base_seconds=retry_policy.retry_backoff_base_seconds,
        retry_backoff_cap_seconds=retry_policy.retry_backoff_cap_seconds,
    )


def runtime_deadline_profile(job_type: str, lease_seconds: int) -> RuntimeDeadlineProfile:
    """Return the persisted deadline subset of the resolved runtime policy."""
    return runtime_policy(job_type, lease_seconds).deadlines


__all__ = [
    "RUNTIME_JOB_ORDER",
    "RUNTIME_JOB_SUCCESSORS",
    "RuntimePolicy",
    "RuntimeRetryPolicy",
    "runtime_deadline_profile",
    "runtime_job_order",
    "runtime_policy",
    "runtime_retry_policy",
]
