"""Typed, exact-call upstream fault adapters.

The adapter never inspects URLs or response bodies.  Invalid or unavailable
control evidence falls through to the real page call exactly once.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, replace

import httpx
from py_clob_client.exceptions import PolyApiException

from polyarb.clients.gamma_client import EventPage
from polyarb.perception.fault_control import (
    FaultCall,
    FaultCallClass,
    FaultDecision,
    FaultKind,
    canonical_digest,
    normalize_fault_id,
)
from polyarb.perception.fault_runtime import FaultRuntimeProtocol


@dataclass(frozen=True, slots=True)
class CandidateBooksDecision:
    decision: FaultDecision
    receipt: object | None = None


class CandidateBooksFault:
    """Exact-group seam around the Candidate watcher's selected books call."""

    def __init__(self, *, runtime: FaultRuntimeProtocol) -> None:
        self._runtime = runtime

    async def before_books(self, group_id: str) -> CandidateBooksDecision:
        try:
            decision = self._runtime.consume(
                FaultCall(
                    FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
                    group_id,
                )
            )
        except Exception:
            return CandidateBooksDecision(FaultDecision(False))
        if not self._qualified(decision):
            return CandidateBooksDecision(FaultDecision(False))
        assert decision.fault_id is not None
        receipt = await self._runtime.record_injection(decision.fault_id)
        if receipt is None:
            return CandidateBooksDecision(FaultDecision(False))
        return CandidateBooksDecision(decision, receipt)

    async def after_books(
        self,
        selected: CandidateBooksDecision,
        *,
        token_ids: Sequence[str],
        books: Sequence[dict],
    ) -> Sequence[dict]:
        if selected.receipt is None or not selected.decision.inject:
            return books
        decision = selected.decision
        assert decision.kind is not None
        if decision.kind is FaultKind.CLOB_429:
            error = PolyApiException(error_msg="qualified-clob-429")
            error.status_code = 429
            _tag_error(error, selected.receipt)
            error._polyarb_fault_kind = decision.kind
            raise error
        if decision.kind is FaultKind.CLOB_LATENCY:
            await asyncio.sleep(decision.parameters["delay_ms"] / 1_000)
            return books
        leg_index = decision.parameters["leg_index"]
        if leg_index >= len(token_ids) or leg_index >= len(books):
            assert decision.fault_id is not None
            await self._runtime.cleanup(
                decision.fault_id,
                "missing-leg-not-applicable",
            )
            return books
        return tuple((*books[:leg_index], *books[leg_index + 1 :]))

    async def settle_inner_failure(
        self,
        selected: CandidateBooksDecision,
    ) -> None:
        if selected.receipt is None or selected.decision.fault_id is None:
            return
        task = asyncio.create_task(
            self._runtime.cleanup(
                selected.decision.fault_id,
                "injected-books-call-failed",
            )
        )
        while True:
            try:
                await asyncio.shield(task)
                return
            except asyncio.CancelledError:
                continue
            except BaseException:
                return

    @staticmethod
    def tag_error(
        error: BaseException,
        selected: CandidateBooksDecision,
    ) -> None:
        if selected.receipt is None:
            return
        _tag_error(error, selected.receipt)
        error._polyarb_fault_kind = selected.decision.kind

    @staticmethod
    def _qualified(decision: FaultDecision) -> bool:
        if (
            not isinstance(decision, FaultDecision)
            or not decision.inject
            or decision.fault_id is None
            or decision.kind
            not in {
                FaultKind.CLOB_MISSING_LEG,
                FaultKind.CLOB_429,
                FaultKind.CLOB_LATENCY,
            }
        ):
            return False
        expected = {
            FaultKind.CLOB_MISSING_LEG: {"leg_index"},
            FaultKind.CLOB_429: set(),
            FaultKind.CLOB_LATENCY: {"delay_ms"},
        }[decision.kind]
        return set(decision.parameters) == expected


class _QualifiedCursor(str):
    def __new__(
        cls,
        *,
        fault_id: str,
        injected_at_ms: int,
        call_id: str,
    ):
        value = super().__new__(cls, "qualified-gamma-cursor-mismatch")
        value.fault_id = fault_id
        value.injected_at_ms = injected_at_ms
        value.call_id = call_id
        return value


def _cursor_digest(cursor: str | None) -> str:
    value = "<none>" if cursor is None else cursor
    return hashlib.sha256(value.encode()).hexdigest()


class PartialGammaPageError(RuntimeError):
    """A real Gamma page whose requested truncation would reject coverage."""

    def __init__(
        self,
        *,
        original_count: int,
        kept_count: int,
        requested_cursor_digest: str,
        next_cursor_digest: str,
        fault_id: str,
        injected_at_ms: int,
    ) -> None:
        self.original_count = original_count
        self.kept_count = kept_count
        self.requested_cursor_digest = requested_cursor_digest
        self.next_cursor_digest = next_cursor_digest
        self.fault_id = normalize_fault_id(fault_id)
        self.injected_at_ms = injected_at_ms
        self.coverage_id = "coverage-" + canonical_digest(
            {
                "kept_count": kept_count,
                "next_cursor_digest": next_cursor_digest,
                "original_count": original_count,
                "requested_cursor_digest": requested_cursor_digest,
            }
        )
        super().__init__(
            "qualified-gamma-partial "
            f"original_count={original_count} kept_count={kept_count} "
            f"requested_cursor_digest={requested_cursor_digest} "
            f"next_cursor_digest={next_cursor_digest}"
        )


class FaultingGammaPageClient:
    """Wrap only ``fetch_active_event_page`` for one exact producer scope."""

    def __init__(
        self,
        *,
        inner: object,
        runtime: FaultRuntimeProtocol,
        call_class: FaultCallClass,
        target_key: str,
    ) -> None:
        self._inner = inner
        self._runtime = runtime
        self._call_class = FaultCallClass(call_class)
        self._target_key = target_key
        self._call = FaultCall(self._call_class, target_key)

    async def fetch_active_event_page(
        self,
        cursor: str | None,
        limit: int,
    ) -> EventPage:
        try:
            decision = self._runtime.consume(self._call)
        except Exception:
            return await self._fetch_real(cursor, limit)
        if not self._qualified(decision):
            return await self._fetch_real(cursor, limit)
        assert decision.fault_id is not None
        receipt = await self._runtime.record_injection(decision.fault_id)
        if receipt is None:
            return await self._fetch_real(cursor, limit)
        if decision.kind is FaultKind.GAMMA_TIMEOUT:
            await asyncio.sleep(decision.parameters["delay_ms"] / 1_000)
            error = httpx.ReadTimeout("qualified-gamma-timeout")
            _tag_error(error, receipt)
            raise error
        if decision.kind is FaultKind.GAMMA_MALFORMED:
            error = json.JSONDecodeError("qualified-gamma-malformed", "", 0)
            _tag_error(error, receipt)
            raise error
        try:
            page = await self._fetch_real(cursor, limit)
        except BaseException:
            await self._settle_cleanup(
                decision.fault_id,
                "injected-transform-fetch-failed",
            )
            raise
        if decision.kind is FaultKind.GAMMA_CURSOR:
            return replace(
                page,
                requested_cursor=_QualifiedCursor(
                    fault_id=decision.fault_id,
                    injected_at_ms=receipt.occurred_at_ms,
                    call_id=receipt.call_id,
                ),
            )
        keep_events = decision.parameters["keep_events"]
        if keep_events < len(page.events):
            raise PartialGammaPageError(
                original_count=len(page.events),
                kept_count=keep_events,
                requested_cursor_digest=_cursor_digest(page.requested_cursor),
                next_cursor_digest=_cursor_digest(page.next_cursor),
                fault_id=decision.fault_id,
                injected_at_ms=receipt.occurred_at_ms,
            )
        await self._runtime.cleanup(
            decision.fault_id,
            "partial-not-applicable",
        )
        return page

    async def _settle_cleanup(self, fault_id: str, reason: str) -> None:
        task = asyncio.create_task(self._runtime.cleanup(fault_id, reason))
        while True:
            try:
                await asyncio.shield(task)
                return
            except asyncio.CancelledError:
                continue
            except BaseException:
                return

    async def _fetch_real(self, cursor: str | None, limit: int) -> EventPage:
        fetch = getattr(self._inner, "fetch_active_event_page")
        return await fetch(cursor, limit)

    def _qualified(self, decision: FaultDecision) -> bool:
        if (
            not isinstance(decision, FaultDecision)
            or not decision.inject
            or decision.fault_id is None
        ):
            return False
        allowed = {
            FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE: {
                FaultKind.GAMMA_TIMEOUT,
                FaultKind.GAMMA_PARTIAL,
                FaultKind.GAMMA_MALFORMED,
            },
            FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE: {
                FaultKind.GAMMA_CURSOR,
            },
        }[self._call_class]
        if decision.kind not in allowed:
            return False
        expected_parameters = {
            FaultKind.GAMMA_TIMEOUT: {"delay_ms"},
            FaultKind.GAMMA_PARTIAL: {"keep_events"},
            FaultKind.GAMMA_MALFORMED: set(),
            FaultKind.GAMMA_CURSOR: set(),
        }[decision.kind]
        return set(decision.parameters) == expected_parameters


def gamma_fault_id(error: BaseException) -> str | None:
    value = (
        error.fault_id
        if isinstance(error, PartialGammaPageError)
        else getattr(error, "_polyarb_fault_id", None)
    )
    try:
        return normalize_fault_id(value)
    except (TypeError, ValueError):
        return None


def gamma_injected_at_ms(error: BaseException) -> int | None:
    value = (
        error.injected_at_ms
        if isinstance(error, PartialGammaPageError)
        else getattr(error, "_polyarb_injected_at_ms", None)
    )
    return value if type(value) is int and value >= 0 else None


def gamma_cursor_error(page: EventPage, message: str) -> ValueError:
    error = ValueError(message)
    cursor = page.requested_cursor
    if isinstance(cursor, _QualifiedCursor):
        error._polyarb_fault_id = cursor.fault_id
        error._polyarb_injected_at_ms = cursor.injected_at_ms
        error._polyarb_fault_call_id = cursor.call_id
    return error


def _tag_error(error: BaseException, receipt: object) -> None:
    error._polyarb_fault_id = getattr(receipt, "fault_id", None)
    error._polyarb_injected_at_ms = getattr(receipt, "occurred_at_ms", None)
    error._polyarb_fault_call_id = getattr(receipt, "call_id", None)


__all__ = [
    "CandidateBooksDecision",
    "CandidateBooksFault",
    "FaultingGammaPageClient",
    "PartialGammaPageError",
    "gamma_fault_id",
    "gamma_injected_at_ms",
    "gamma_cursor_error",
]
