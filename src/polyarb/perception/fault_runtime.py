"""Fail-open bridge from durable fault authority to producer-local memory."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loguru import logger

from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import (
    FaultCall,
    FaultController,
    FaultDecision,
    FaultEventState,
    FaultRuntimeIdentity,
)


@dataclass(frozen=True, slots=True)
class CleanupResult:
    memory_cleared: bool
    receipt_persisted: bool
    degraded: bool = False
    terminal_state: FaultEventState | None = None


class FaultRuntimeProtocol(Protocol):
    degraded: bool

    @property
    def active_fault_id(self) -> str | None: ...

    async def sync_before_batch(self) -> None: ...

    async def cleanup(self, fault_id: str, reason: str) -> CleanupResult: ...

    def consume(self, call: FaultCall) -> FaultDecision: ...


class FaultRuntime:
    """Claim only at safe boundaries and consume without persistence reads."""

    degraded = False

    def __init__(
        self,
        *,
        identity: FaultRuntimeIdentity,
        authority: FaultAuthorityStore,
        clock_ms=None,
        monotonic=None,
    ) -> None:
        self.identity = identity
        self._authority = authority
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1_000))
        self._monotonic = monotonic or time.monotonic
        self._controller = FaultController(
            runtime=identity,
            monotonic=self._monotonic,
        )

    @property
    def active_fault_id(self) -> str | None:
        active = self._controller.active
        return None if active is None else active.intent.fault_id

    async def sync_before_batch(self) -> None:
        """Claim at most one intent; store failure leaves controller unchanged."""
        if self._controller.frozen:
            return
        active = self._controller.active
        if active is not None:
            if self._monotonic() < active.expires_monotonic:
                return
            await self.cleanup(active.intent.fault_id, "intent-expired")
            if self._controller.frozen:
                return
        claimed_at_ms = self._clock_ms()
        claim_task = asyncio.create_task(
            asyncio.to_thread(
                self._authority.claim_pending,
                self.identity,
                claimed_at_ms=claimed_at_ms,
            )
        )
        try:
            intent = await asyncio.shield(claim_task)
            if intent is not None:
                self._controller.admit(intent, claimed_at_ms=claimed_at_ms)
        except asyncio.CancelledError as cancellation:
            try:
                intent = await asyncio.shield(claim_task)
            except Exception as error:
                logger.warning(
                    "fault control cancelled claim unavailable "
                    f"component={self.identity.component} "
                    f"kind={type(error).__name__}"
                )
            else:
                if intent is not None:
                    self._controller.admit(intent, claimed_at_ms=claimed_at_ms)
                    await self.cleanup(intent.fault_id, "claim-cancelled")
            raise cancellation
        except Exception as error:
            logger.warning(
                "fault control claim unavailable "
                f"component={self.identity.component} kind={type(error).__name__}"
            )

    async def cleanup(self, fault_id: str, reason: str) -> CleanupResult:
        """Clear memory first, append cleanup second."""
        active = self._controller.active
        if active is None or active.intent.fault_id != fault_id:
            return CleanupResult(False, False)
        ownership = active.intent.ownership_capability
        terminal_state: FaultEventState | None = None

        def persist_cleanup_receipt(_: str) -> None:
            nonlocal terminal_state
            event = self._authority.relinquish_claim(
                fault_id,
                occurred_at_ms=self._clock_ms(),
                ownership=ownership,
            )
            terminal_state = event.state

        try:
            await asyncio.to_thread(
                self._controller.clear,
                fault_id,
                receipt_writer=persist_cleanup_receipt,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning(
                "fault control cleanup receipt unavailable "
                f"component={self.identity.component} reason={reason} "
                f"kind={type(error).__name__}"
            )
            return CleanupResult(True, False, degraded=True)
        return CleanupResult(
            True,
            True,
            terminal_state=terminal_state,
        )

    def consume(self, call: FaultCall) -> FaultDecision:
        """Pure in-memory hot-path decision."""
        try:
            return self._controller.consume(call)
        except Exception:
            return FaultDecision(False)


class PassThroughFaultRuntime:
    """Dormant or degraded control seam that never blocks producer work."""

    def __init__(self, *, degraded: bool = False) -> None:
        self.degraded = degraded

    @property
    def active_fault_id(self) -> None:
        return None

    async def sync_before_batch(self) -> None:
        return None

    async def cleanup(self, fault_id: str, reason: str) -> CleanupResult:
        return CleanupResult(False, False, degraded=self.degraded)

    def consume(self, call: FaultCall) -> FaultDecision:
        return FaultDecision(False)


async def cleanup_active_fault(
    runtime: FaultRuntimeProtocol,
    *,
    reason: str,
) -> CleanupResult:
    fault_id = runtime.active_fault_id
    if fault_id is None:
        return CleanupResult(False, False, degraded=runtime.degraded)
    return await runtime.cleanup(fault_id, reason)


def build_fault_runtime(
    *,
    enabled: bool,
    db_path: Path,
    identity: FaultRuntimeIdentity,
    supervisor_run_id: str,
    attempt: int,
    started_at_ms: int,
) -> FaultRuntimeProtocol:
    """Register exact boot identity without making producer startup depend on it."""
    if not enabled:
        return PassThroughFaultRuntime()
    try:
        authority = FaultAuthorityStore(db_path)
        authority.register_runtime_start(
            identity,
            supervisor_run_id=supervisor_run_id,
            attempt=attempt,
            started_at_ms=started_at_ms,
        )
    except Exception as error:
        logger.warning(
            "fault control registration unavailable "
            f"component={identity.component} kind={type(error).__name__}"
        )
        return PassThroughFaultRuntime(degraded=True)
    return FaultRuntime(identity=identity, authority=authority)


__all__ = [
    "CleanupResult",
    "FaultRuntime",
    "FaultRuntimeProtocol",
    "PassThroughFaultRuntime",
    "build_fault_runtime",
    "cleanup_active_fault",
]
