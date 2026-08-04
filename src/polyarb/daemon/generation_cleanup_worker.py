"""Resident, low-priority owner for bounded Structure evidence cleanup."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from collections.abc import Callable


class StructureGenerationCleanupWorker:
    """Own one bounded cleanup chunk at a time below Quote priority."""

    def __init__(
        self,
        *,
        settings: object,
        sqlite_store: object,
        producer_lock: asyncio.Lock,
        quote_worker_runtime: object | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._settings = settings
        self._store = sqlite_store
        self._producer_lock = producer_lock
        self._quote_runtime = quote_worker_runtime
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1_000))

    def _interval_ms(self, setting: str, default_s: float) -> int:
        return max(1, int(float(getattr(self._settings, setting, default_s)) * 1_000))

    def _quote_priority_reason(self) -> str | None:
        runtime = self._quote_runtime
        if runtime is None:
            return None
        if runtime.pipeline_active():
            return "quote-pipeline-active"
        interval_s = float(
            getattr(self._settings, "neg_risk_quote_interval_s", 120.0)
        )
        if runtime.pipeline_due(interval_s):
            return "quote-pipeline-due"
        return None

    async def _defer(self, *, now_ms: int, error_kind: str) -> None:
        delay_ms = self._interval_ms(
            "structure_generation_cleanup_writer_busy_interval_s", 5.0
        )
        await asyncio.to_thread(
            self._store.defer_structure_generation_cleanup_runtime,
            now_ms=now_ms,
            next_attempt_at_ms=now_ms + delay_ms,
            error_kind=error_kind,
        )

    async def _finish_error(
        self,
        *,
        now_ms: int,
        error: Exception,
    ) -> None:
        writer_busy = isinstance(error, sqlite3.OperationalError) and any(
            marker in str(error).lower() for marker in ("locked", "busy")
        )
        if writer_busy:
            delay_ms = self._interval_ms(
                "structure_generation_cleanup_writer_busy_interval_s", 5.0
            )
            error_kind = "writer-busy"
        else:
            try:
                runtime = await asyncio.to_thread(
                    self._store.structure_generation_cleanup_runtime_status
                )
                failures = int(runtime["consecutive_failures"])
            except (AttributeError, OSError, sqlite3.Error, TypeError, ValueError):
                failures = 0
            initial_ms = self._interval_ms(
                "structure_generation_cleanup_retry_initial_s", 1.0
            )
            maximum_ms = self._interval_ms(
                "structure_generation_cleanup_retry_max_s", 30.0
            )
            delay_ms = min(maximum_ms, initial_ms * (2 ** min(failures, 10)))
            error_kind = type(error).__name__[:64]
        await asyncio.to_thread(
            self._store.finish_structure_generation_cleanup_attempt,
            state="backoff",
            now_ms=now_ms,
            next_attempt_at_ms=now_ms + delay_ms,
            generation_snapshot_id=None,
            phase=None,
            rows_deleted=0,
            error_kind=error_kind,
            increment_failure=not writer_busy,
        )

    async def _tick(self) -> None:
        """Advance at most one authenticated cleanup transaction."""
        now_ms = self._clock_ms()
        if self._quote_priority_reason() is not None:
            await self._defer(now_ms=now_ms, error_kind="quote-priority")
            return

        async with self._producer_lock:
            if self._quote_priority_reason() is not None:
                await self._defer(now_ms=now_ms, error_kind="quote-priority")
                return
            admitted = await asyncio.to_thread(
                self._store.begin_structure_generation_cleanup_attempt,
                now_ms=now_ms,
            )
            if not admitted:
                return
            cleanup_task = asyncio.create_task(
                asyncio.to_thread(
                    self._store.cleanup_structure_generation_evidence,
                    retain_generations=int(
                        getattr(
                            self._settings,
                            "structure_generation_retention_floor",
                            2,
                        )
                    ),
                    max_rows=int(
                        getattr(
                            self._settings,
                            "structure_generation_cleanup_max_rows",
                            500,
                        )
                    ),
                    now_ms=now_ms,
                )
            )
            cancelled = False
            try:
                result = await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                cancelled = True
                result = await cleanup_task
            except Exception as error:  # noqa: BLE001 - durable bounded retry
                await self._finish_error(now_ms=now_ms, error=error)
                return

            if bool(result["blocked"]):
                retry_ms = self._interval_ms(
                    "structure_generation_cleanup_retry_initial_s", 1.0
                )
                await asyncio.to_thread(
                    self._store.finish_structure_generation_cleanup_attempt,
                    state="blocked",
                    now_ms=now_ms,
                    next_attempt_at_ms=now_ms + retry_ms,
                    generation_snapshot_id=result["generation_snapshot_id"],
                    phase=result["phase"],
                    rows_deleted=int(result["rows_deleted"]),
                    error_kind=str(result["blocked_reason"]),
                    increment_failure=True,
                )
            else:
                status = await asyncio.to_thread(
                    self._store.structure_generation_status,
                    retain_generations=int(
                        getattr(
                            self._settings,
                            "structure_generation_retention_floor",
                            2,
                        )
                    ),
                )
                active = int(
                    status["reclaimable_generation_count_lower_bound"]
                ) > 0
                delay_ms = self._interval_ms(
                    (
                        "structure_generation_cleanup_active_interval_s"
                        if active
                        else "structure_generation_cleanup_idle_interval_s"
                    ),
                    0.05 if active else 30.0,
                )
                await asyncio.to_thread(
                    self._store.finish_structure_generation_cleanup_attempt,
                    state="idle",
                    now_ms=now_ms,
                    next_attempt_at_ms=now_ms + delay_ms,
                    generation_snapshot_id=result["generation_snapshot_id"],
                    phase=result["phase"],
                    rows_deleted=int(result["rows_deleted"]),
                    error_kind=None,
                    increment_failure=False,
                )
            if cancelled:
                raise asyncio.CancelledError

    async def run(self, stop_event: asyncio.Event) -> None:
        """Recover durable ownership, then execute due chunks until shutdown."""
        retry_ms = self._interval_ms(
            "structure_generation_cleanup_retry_initial_s", 1.0
        )
        await asyncio.to_thread(
            self._store.recover_structure_generation_cleanup_runtime,
            now_ms=self._clock_ms(),
            retry_delay_ms=retry_ms,
        )
        while not stop_event.is_set():
            try:
                runtime = await asyncio.to_thread(
                    self._store.structure_generation_cleanup_runtime_status
                )
                delay_s = max(
                    0.0,
                    (int(runtime["next_attempt_at_ms"]) - self._clock_ms()) / 1_000,
                )
                if delay_s > 0:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=delay_s)
                    except TimeoutError:
                        pass
                    continue
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - health exposes persistent failures
                try:
                    await asyncio.to_thread(
                        self._store.recover_structure_generation_cleanup_runtime,
                        now_ms=self._clock_ms(),
                        retry_delay_ms=retry_ms,
                    )
                except Exception:  # noqa: BLE001 - retry after bounded delay
                    pass
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=retry_ms / 1_000,
                    )
                except TimeoutError:
                    pass
