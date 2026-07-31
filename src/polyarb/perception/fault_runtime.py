"""Fail-open bridge from durable fault authority to producer-local memory."""

from __future__ import annotations

import asyncio
import secrets
import time
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from loguru import logger

from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import (
    FaultCall,
    FaultController,
    FaultDecision,
    FaultEventState,
    FaultIntent,
    FaultKind,
    FaultOwnershipCapability,
    FaultRecoveryReceipt,
    FaultRecoveryWriter,
    FaultRuntimeIdentity,
    fault_call_binding_digest,
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


class FaultRecoveryOutcome(StrEnum):
    RECORDED = "recorded"
    INVALID = "invalid"
    UNAVAILABLE = "unavailable"
    NOT_APPLICABLE = "not-applicable"


@dataclass(frozen=True, slots=True)
class _PendingRecovery:
    intent: FaultIntent
    ownership: FaultOwnershipCapability
    injection: FaultInjectionReceipt


@dataclass(frozen=True, slots=True)
class _CompletedRecovery:
    receipt: FaultRecoveryReceipt
    target_key: str


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

    def make_recovery_receipt(
        self,
        writer: FaultRecoveryWriter,
        *,
        writer_id: int | str,
        writer_occurred_at_ms: int,
    ) -> FaultRecoveryReceipt | None: ...

    async def record_recovery(self, receipt: FaultRecoveryReceipt) -> bool: ...

    async def record_recovery_outcome(
        self,
        receipt: FaultRecoveryReceipt,
    ) -> FaultRecoveryOutcome: ...

    async def record_writer_recovery_outcome(
        self,
        writer: FaultRecoveryWriter,
        *,
        target_key: str,
        writer_id: int | str,
        writer_occurred_at_ms: int,
    ) -> FaultRecoveryOutcome: ...

    async def invalidate_evidence(
        self,
        fault_id: str,
        reason: str,
    ) -> CleanupResult: ...

    async def evidence_unavailable(
        self,
        fault_id: str,
        reason: str,
    ) -> CleanupResult: ...


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
        self.degraded_reason: str | None = None
        self._evidence_frozen = False
        self._injected_fault_id: str | None = None
        self._pending_recovery: _PendingRecovery | None = None
        self._last_injection: FaultInjectionReceipt | None = None
        self._completed_recovery: _CompletedRecovery | None = None

    @staticmethod
    async def _settle_evidence_write(call, *, settled=None):
        task = asyncio.create_task(asyncio.to_thread(call))
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                result = await asyncio.shield(task)
                break
            except asyncio.CancelledError as error:
                cancellation = cancellation or error
                continue
            except BaseException as error:
                if cancellation is not None:
                    raise cancellation from error
                raise
        if settled is not None:
            settled(result)
        if cancellation is not None:
            raise cancellation
        return result

    @staticmethod
    async def _settle_cancelled_operation(operation) -> None:
        task = asyncio.create_task(operation)
        while True:
            try:
                await asyncio.shield(task)
                return
            except asyncio.CancelledError:
                continue
            except BaseException:
                return

    @property
    def active_fault_id(self) -> str | None:
        active = self._controller.active
        return None if active is None else active.intent.fault_id

    @property
    def pending_recovery_fault_id(self) -> str | None:
        return None if self._pending_recovery is None else self._pending_recovery.intent.fault_id

    async def sync_before_batch(self) -> None:
        """Claim at most one intent; store failure leaves controller unchanged."""
        if self._controller.frozen or self._evidence_frozen:
            return
        active = self._controller.active
        if active is not None:
            ownership = active.intent.ownership_capability
            if ownership is not None:
                try:
                    cleanup_requested = await asyncio.to_thread(
                        self._authority.owner_cleanup_requested,
                        active.intent.fault_id,
                        ownership=ownership,
                    )
                except Exception as error:
                    logger.warning(
                        "fault control cleanup request unavailable "
                        f"component={self.identity.component} "
                        f"kind={type(error).__name__}"
                    )
                    try:
                        self._controller.clear(
                            active.intent.fault_id,
                            receipt_writer=lambda _fault_id: (_ for _ in ()).throw(
                                RuntimeError("cleanup-truth-unavailable")
                            ),
                        )
                    except BaseException:
                        pass
                    self._freeze_evidence(
                        error,
                        reason="cleanup-truth-unavailable",
                    )
                    return
                if cleanup_requested:
                    await self.cleanup(active.intent.fault_id, "cleanup-requested")
                    if self._controller.frozen:
                        return
                    active = self._controller.active
                    if active is None:
                        return
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
            memory_cleared_at_ms = self._clock_ms()
            persisted_at_ms = self._clock_ms()
            event = self._authority.relinquish_claim(
                fault_id,
                occurred_at_ms=persisted_at_ms,
                ownership=ownership,
                memory_cleared_at_ms=memory_cleared_at_ms,
            )
            confirm = getattr(self._authority, "confirm_cleanup_commit", None)
            if callable(confirm) and event.state is FaultEventState.CLEANED:
                confirm(
                    fault_id,
                    cleaned=event,
                    memory_cleared_at_ms=memory_cleared_at_ms,
                    confirmed_at_ms=self._clock_ms(),
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
            self._freeze_evidence(error)
            logger.warning(
                "fault control cleanup receipt unavailable "
                f"component={self.identity.component} reason={reason} "
                f"kind={type(error).__name__}"
            )
            return CleanupResult(True, False, degraded=True)
        if (
            terminal_state is FaultEventState.CLEANED
            and ownership is not None
            and self._last_injection is not None
            and self._last_injection.fault_id == fault_id
        ):
            self._pending_recovery = _PendingRecovery(
                intent=active.intent,
                ownership=ownership,
                injection=self._last_injection,
            )
        self._injected_fault_id = None
        self._last_injection = None
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
        receipt = FaultInjectionReceipt(
            fault_id=fault_id,
            call_id=call_id,
            occurred_at_ms=occurred_at_ms,
        )

        def install_receipt(_: object) -> None:
            self._injected_fault_id = fault_id
            self._last_injection = receipt
            self._completed_recovery = None

        try:
            await self._settle_evidence_write(
                lambda: self._authority.append_event(
                    fault_id,
                    FaultEventState.INJECTED,
                    occurred_at_ms=occurred_at_ms,
                    evidence={
                        "call_id": call_id,
                        "call_binding_digest": fault_call_binding_digest(
                            fault_id=fault_id,
                            kind=active.intent.kind.value,
                            call_class=active.intent.call_class.value,
                            target_key=active.intent.target_key,
                            runtime={
                                "component": active.intent.runtime.component,
                                "release_id": active.intent.runtime.release_id,
                                "machine_id": active.intent.runtime.machine_id,
                                "boot_id": str(active.intent.runtime.boot_id),
                            },
                            call_id=call_id,
                        ),
                    },
                    ownership=active.intent.ownership_capability,
                ),
                settled=install_receipt,
            )
        except asyncio.CancelledError as cancellation:
            if self._last_injection is receipt:
                await self._settle_cancelled_operation(
                    self.cleanup(fault_id, "injection-commit-cancelled")
                )
            raise cancellation
        except Exception as error:
            self._freeze_evidence(error)
            return None
        return receipt

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
            evidence_key = "coverage_id" if kind is FaultKind.GAMMA_PARTIAL else "incident_id"

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

    async def record_partial_coverage_source(
        self,
        fault_id: str,
        *,
        coverage_id: str,
        original_count: int,
        kept_count: int,
        requested_cursor_digest: str,
        next_cursor_digest: str,
    ) -> bool:
        """Persist the source-owned rejected-page fact before lifecycle linking."""
        active = self._controller.active
        if (
            active is None
            or active.intent.fault_id != fault_id
            or active.intent.kind is not FaultKind.GAMMA_PARTIAL
        ):
            return False
        await self._settle_evidence_write(
            lambda: self._authority.record_partial_coverage_rejection(
                fault_id,
                coverage_id=coverage_id,
                original_count=original_count,
                kept_count=kept_count,
                requested_cursor_digest=requested_cursor_digest,
                next_cursor_digest=next_cursor_digest,
                recorded_at_ms=self._clock_ms(),
            )
        )
        return True

    def make_recovery_receipt(
        self,
        writer: FaultRecoveryWriter,
        *,
        writer_id: int | str,
        writer_occurred_at_ms: int,
    ) -> FaultRecoveryReceipt | None:
        pending = self._pending_recovery
        if self._evidence_frozen or pending is None:
            return None
        try:
            return FaultRecoveryReceipt(
                fault_id=pending.intent.fault_id,
                kind=pending.intent.kind,
                call_class=pending.intent.call_class,
                component=pending.intent.runtime.component,
                runtime=pending.intent.runtime,
                writer=writer,
                writer_id=writer_id,
                writer_occurred_at_ms=writer_occurred_at_ms,
            )
        except (TypeError, ValueError):
            return None

    async def record_recovery(self, receipt: FaultRecoveryReceipt) -> bool:
        return (
            await self.record_recovery_outcome(receipt)
            is FaultRecoveryOutcome.RECORDED
        )

    async def record_recovery_outcome(
        self,
        receipt: FaultRecoveryReceipt,
    ) -> FaultRecoveryOutcome:
        """Append one writer-owned recovery fact after successful cleanup."""
        pending = self._pending_recovery
        if (
            pending is None
            and self._completed_recovery is not None
            and receipt == self._completed_recovery.receipt
        ):
            return FaultRecoveryOutcome.RECORDED
        if (
            self._evidence_frozen
            or pending is None
            or not isinstance(receipt, FaultRecoveryReceipt)
            or receipt.fault_id != pending.intent.fault_id
            or receipt.kind is not pending.intent.kind
            or receipt.call_class is not pending.intent.call_class
            or receipt.component != pending.intent.runtime.component
            or receipt.runtime != pending.intent.runtime
        ):
            return FaultRecoveryOutcome.INVALID

        def install_recovery(written: object | None) -> None:
            if written is None:
                return
            self._pending_recovery = None
            self._completed_recovery = _CompletedRecovery(
                receipt=receipt,
                target_key=pending.intent.target_key,
            )

        try:
            written = await self._settle_evidence_write(
                lambda: self._authority.append_recovery_event(
                    receipt,
                    injected_at_ms=pending.injection.occurred_at_ms,
                    occurred_at_ms=self._clock_ms(),
                    ownership=pending.ownership,
                ),
                settled=install_recovery,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._freeze_evidence(error)
            return FaultRecoveryOutcome.UNAVAILABLE
        if written is None:
            return FaultRecoveryOutcome.INVALID
        return FaultRecoveryOutcome.RECORDED

    async def record_writer_recovery_outcome(
        self,
        writer: FaultRecoveryWriter,
        *,
        target_key: str,
        writer_id: int | str,
        writer_occurred_at_ms: int,
    ) -> FaultRecoveryOutcome:
        """Validate writer family and target before touching recovery authority."""
        pending = self._pending_recovery
        if self._evidence_frozen:
            return FaultRecoveryOutcome.UNAVAILABLE
        if pending is None:
            completed = self._completed_recovery
            if (
                completed is not None
                and completed.target_key == target_key
                and completed.receipt.writer is writer
                and completed.receipt.writer_id == writer_id
                and completed.receipt.writer_occurred_at_ms == writer_occurred_at_ms
            ):
                return FaultRecoveryOutcome.RECORDED
            return FaultRecoveryOutcome.NOT_APPLICABLE
        if pending.intent.target_key != target_key:
            return FaultRecoveryOutcome.NOT_APPLICABLE
        expected_writer = {
            FaultKind.GAMMA_TIMEOUT: FaultRecoveryWriter.DISCOVERY_BATCH,
            FaultKind.GAMMA_PARTIAL: FaultRecoveryWriter.DISCOVERY_BATCH,
            FaultKind.GAMMA_MALFORMED: FaultRecoveryWriter.DISCOVERY_BATCH,
            FaultKind.GAMMA_CURSOR: FaultRecoveryWriter.RECONCILIATION_CHECKPOINT,
            FaultKind.CLOB_MISSING_LEG: FaultRecoveryWriter.CANDIDATE_SUCCESS,
            FaultKind.CLOB_429: FaultRecoveryWriter.CANDIDATE_SUCCESS,
            FaultKind.CLOB_LATENCY: FaultRecoveryWriter.CANDIDATE_SUCCESS,
            FaultKind.TELEGRAM_FAILURE: FaultRecoveryWriter.TELEGRAM_DELIVERY,
        }.get(pending.intent.kind)
        invalid_reason = f"{pending.intent.runtime.component}-recovery-evidence-invalid"
        if expected_writer is None or writer is not expected_writer:
            return await self._record_owned_invalidity(
                pending.intent.fault_id,
                invalid_reason,
            )
        receipt = self.make_recovery_receipt(
            writer,
            writer_id=writer_id,
            writer_occurred_at_ms=writer_occurred_at_ms,
        )
        if receipt is None:
            return await self._record_owned_invalidity(
                pending.intent.fault_id,
                invalid_reason,
            )
        outcome = await self.record_recovery_outcome(receipt)
        if outcome is FaultRecoveryOutcome.INVALID:
            return await self._record_owned_invalidity(
                pending.intent.fault_id,
                invalid_reason,
            )
        return outcome

    async def _record_owned_invalidity(
        self,
        fault_id: str,
        reason: str,
    ) -> FaultRecoveryOutcome:
        result = await self.invalidate_evidence(fault_id, reason)
        if (
            isinstance(result, CleanupResult)
            and result.receipt_persisted is True
            and result.terminal_state is FaultEventState.EVIDENCE_INVALID
        ):
            return FaultRecoveryOutcome.INVALID
        if not self.degraded:
            self._freeze_evidence(RuntimeError("fault-invalidity-evidence-unavailable"))
        return FaultRecoveryOutcome.UNAVAILABLE

    async def invalidate_evidence(
        self,
        fault_id: str,
        reason: str,
    ) -> CleanupResult:
        """Persist a proven semantic invalidity, then freeze qualification."""
        active = self._controller.active
        pending = self._pending_recovery
        ownership = (
            active.intent.ownership_capability
            if active is not None and active.intent.fault_id == fault_id
            else (
                pending.ownership
                if pending is not None and pending.intent.fault_id == fault_id
                else None
            )
        )
        if ownership is None:
            self._freeze_evidence(ValueError("fault-evidence-invalid"))
            return CleanupResult(False, False, degraded=True)

        memory_cleared = active is not None

        def persist_invalidity(_: str | None = None) -> None:
            self._authority.append_event(
                fault_id,
                FaultEventState.EVIDENCE_INVALID,
                occurred_at_ms=self._clock_ms(),
                evidence={"reason": "evidence-invalid"},
                ownership=ownership,
            )

        try:
            if active is not None:
                await self._settle_evidence_write(
                    lambda: self._controller.clear(
                        fault_id,
                        receipt_writer=persist_invalidity,
                    )
                )
            else:
                await self._settle_evidence_write(persist_invalidity)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._freeze_evidence(error)
            return CleanupResult(
                memory_cleared=memory_cleared,
                receipt_persisted=False,
                degraded=True,
            )
        self._freeze_evidence(ValueError("fault-evidence-invalid"))
        return CleanupResult(
            memory_cleared=memory_cleared,
            receipt_persisted=True,
            degraded=True,
            terminal_state=FaultEventState.EVIDENCE_INVALID,
        )

    async def evidence_unavailable(
        self,
        fault_id: str,
        reason: str,
    ) -> CleanupResult:
        """Restore pass-through, but do not label unavailable proof invalid."""
        result = await self.cleanup(fault_id, reason)
        self._freeze_evidence(RuntimeError("fault-evidence-unavailable"))
        return replace(result, degraded=True)

    def _freeze_evidence(
        self,
        error: BaseException,
        *,
        reason: str = "fault-evidence-unavailable",
    ) -> None:
        self._evidence_frozen = True
        self.degraded = True
        self.degraded_reason = reason
        self._injected_fault_id = None
        self._last_injection = None
        self._pending_recovery = None
        self._completed_recovery = None
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

    def make_recovery_receipt(
        self,
        writer: FaultRecoveryWriter,
        *,
        writer_id: int | str,
        writer_occurred_at_ms: int,
    ) -> FaultRecoveryReceipt | None:
        return None

    async def record_recovery(self, receipt: FaultRecoveryReceipt) -> bool:
        return False

    async def record_recovery_outcome(
        self,
        receipt: FaultRecoveryReceipt,
    ) -> FaultRecoveryOutcome:
        return (
            FaultRecoveryOutcome.UNAVAILABLE
            if self.degraded
            else FaultRecoveryOutcome.INVALID
        )

    async def record_writer_recovery_outcome(
        self,
        writer: FaultRecoveryWriter,
        *,
        target_key: str,
        writer_id: int | str,
        writer_occurred_at_ms: int,
    ) -> FaultRecoveryOutcome:
        return (
            FaultRecoveryOutcome.UNAVAILABLE
            if self.degraded
            else FaultRecoveryOutcome.NOT_APPLICABLE
        )

    async def invalidate_evidence(
        self,
        fault_id: str,
        reason: str,
    ) -> CleanupResult:
        return CleanupResult(False, False, degraded=self.degraded)

    async def evidence_unavailable(
        self,
        fault_id: str,
        reason: str,
    ) -> CleanupResult:
        return CleanupResult(False, False, degraded=self.degraded)


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
    "FaultInjectionReceipt",
    "FaultRecoveryOutcome",
    "FaultRuntime",
    "FaultRuntimeProtocol",
    "PassThroughFaultRuntime",
    "build_fault_runtime",
    "cleanup_active_fault",
]
