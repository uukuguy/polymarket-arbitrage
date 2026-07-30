"""Fail-open bridge from durable fault authority to producer-local memory."""

from __future__ import annotations

import asyncio
import secrets
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
    FaultKind,
    FaultOwnershipCapability,
    FaultRuntimeIdentity,
)


@dataclass(frozen=True, slots=True)
class CleanupResult:
    memory_cleared: bool
    receipt_persisted: bool
    degraded: bool = False
    terminal_state: FaultEventState | None = None


@dataclass(frozen=True, slots=True)
class FaultInjectionReceipt:
    fault_id: str
    call_id: str
    occurred_at_ms: int


class FaultRuntimeProtocol(Protocol):
    degraded: bool

    @property
    def active_fault_id(self) -> str | None: ...

    @property
    def pending_recovery_fault_id(self) -> str | None: ...

    async def sync_before_batch(self) -> None: ...

    async def cleanup(self, fault_id: str, reason: str) -> CleanupResult: ...

    def consume(self, call: FaultCall) -> FaultDecision: ...

    async def record_injection(
        self,
        fault_id: str,
    ) -> FaultInjectionReceipt | None: ...

    async def link_detection(
        self,
        fault_id: str,
        *,
        kind: FaultKind,
        detection_id: str,
    ) -> bool: ...

    async def record_recovery(self, recovery_id: str) -> bool: ...


class FaultRuntime:
    """Claim only at safe boundaries and consume without persistence reads."""

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
        self.degraded = False
        self._evidence_frozen = False
        self._injected_fault_id: str | None = None
        self._pending_recovery: tuple[str, FaultOwnershipCapability] | None = None

    @staticmethod
    async def _settle_evidence_write(call) -> None:
        task = asyncio.create_task(asyncio.to_thread(call))
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                await asyncio.shield(task)
                break
            except asyncio.CancelledError as error:
                cancellation = cancellation or error
                continue
            except BaseException as error:
                if cancellation is not None:
                    raise cancellation from error
                raise
        if cancellation is not None:
            raise cancellation

    @property
    def active_fault_id(self) -> str | None:
        active = self._controller.active
        return None if active is None else active.intent.fault_id

    @property
    def pending_recovery_fault_id(self) -> str | None:
        return None if self._pending_recovery is None else self._pending_recovery[0]

    async def sync_before_batch(self) -> None:
        """Claim at most one intent; store failure leaves controller unchanged."""
        if self._controller.frozen or self._evidence_frozen:
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
        if terminal_state is FaultEventState.CLEANED and ownership is not None:
            self._pending_recovery = (fault_id, ownership)
        self._injected_fault_id = None
        return CleanupResult(
            True,
            True,
            terminal_state=terminal_state,
        )

    def consume(self, call: FaultCall) -> FaultDecision:
        """Pure in-memory hot-path decision."""
        if self._evidence_frozen:
            return FaultDecision(False)
        try:
            return self._controller.consume(call)
        except Exception:
            return FaultDecision(False)

    async def record_injection(
        self,
        fault_id: str,
    ) -> FaultInjectionReceipt | None:
        """Append the process-owned injection receipt before applying a fault."""
        active = self._controller.active
        if (
            self._evidence_frozen
            or active is None
            or not active.consumed
            or active.intent.fault_id != fault_id
            or active.intent.ownership_capability is None
        ):
            return None
        occurred_at_ms = self._clock_ms()
        call_id = secrets.token_hex(16)
        try:
            await self._settle_evidence_write(
                lambda: self._authority.append_event(
                    fault_id,
                    FaultEventState.INJECTED,
                    occurred_at_ms=occurred_at_ms,
                    evidence={"call_id": call_id},
                    ownership=active.intent.ownership_capability,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._freeze_evidence(error)
            return None
        self._injected_fault_id = fault_id
        return FaultInjectionReceipt(
            fault_id=fault_id,
            call_id=call_id,
            occurred_at_ms=occurred_at_ms,
        )

    async def link_detection(
        self,
        fault_id: str,
        *,
        kind: FaultKind,
        detection_id: str,
    ) -> bool:
        """Link one exact detection and containment to the injected intent."""
        active = self._controller.active
        if (
            self._evidence_frozen
            or active is None
            or active.intent.fault_id != fault_id
            or active.intent.kind is not kind
            or active.intent.ownership_capability is None
            or self._injected_fault_id != fault_id
        ):
            return False
        try:
            evidence_key = (
                "coverage_id"
                if kind is FaultKind.GAMMA_PARTIAL
                else "incident_id"
            )

            def persist_detection() -> None:
                self._authority.append_event(
                    fault_id,
                    FaultEventState.DETECTED,
                    occurred_at_ms=self._clock_ms(),
                    evidence={evidence_key: detection_id},
                )
                self._authority.append_event(
                    fault_id,
                    FaultEventState.CONTAINED,
                    occurred_at_ms=self._clock_ms(),
                    evidence={"containment_id": secrets.token_hex(16)},
                    ownership=active.intent.ownership_capability,
                )

            await self._settle_evidence_write(
                persist_detection,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._freeze_evidence(error)
            return False
        return True

    async def record_recovery(self, recovery_id: str) -> bool:
        """Append one writer-owned recovery fact after successful cleanup."""
        pending = self._pending_recovery
        if self._evidence_frozen or pending is None:
            return False
        fault_id, ownership = pending
        try:
            await self._settle_evidence_write(
                lambda: self._authority.append_event(
                    fault_id,
                    FaultEventState.RECOVERED,
                    occurred_at_ms=self._clock_ms(),
                    evidence={"recovery_id": recovery_id},
                    ownership=ownership,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._freeze_evidence(error)
            return False
        self._pending_recovery = None
        return True

    def _freeze_evidence(self, error: BaseException) -> None:
        self._evidence_frozen = True
        self.degraded = True
        logger.warning(
            "fault control evidence unavailable "
            f"component={self.identity.component} kind={type(error).__name__}"
        )


class PassThroughFaultRuntime:
    """Dormant or degraded control seam that never blocks producer work."""

    def __init__(self, *, degraded: bool = False) -> None:
        self.degraded = degraded

    @property
    def active_fault_id(self) -> None:
        return None

    @property
    def pending_recovery_fault_id(self) -> None:
        return None

    async def sync_before_batch(self) -> None:
        return None

    async def cleanup(self, fault_id: str, reason: str) -> CleanupResult:
        return CleanupResult(False, False, degraded=self.degraded)

    def consume(self, call: FaultCall) -> FaultDecision:
        return FaultDecision(False)

    async def record_injection(
        self,
        fault_id: str,
    ) -> FaultInjectionReceipt | None:
        return None

    async def link_detection(
        self,
        fault_id: str,
        *,
        kind: FaultKind,
        detection_id: str,
    ) -> bool:
        return False

    async def record_recovery(self, recovery_id: str) -> bool:
        return False


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
    "FaultInjectionReceipt",
    "PassThroughFaultRuntime",
    "build_fault_runtime",
    "cleanup_active_fault",
]
