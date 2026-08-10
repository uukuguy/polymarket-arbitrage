"""SnapshotScheduler — bounded retry and self-recovery state machine.

Phase 02 Plan 02 — D-13 / T-02-04.
Phase 03.1-04 — D-02: threshold raised 3 → 5 to give tenacity DNS retry (D-01 A)
room to absorb transient EAI_NODATA without prematurely entering recovery.
At ~37s snapshot cadence, 5 consecutive failures = ~3min observation window;
the producer enters RECOVERING rather than permanently disabling itself.

State machine:
  RUNNING → tick OK/DEGRADED → reset counter, stay RUNNING
  RUNNING → tick FAILED/exception → counter += 1
  RUNNING + counter >= FAILURE_THRESHOLD → transition to RECOVERING
  RECOVERING → bounded tick → retain retry evidence and continue
  RECOVERING + successful certified snapshot → RUNNING, counter = 0

Design decisions:
- DEGRADED is NOT a failure (D-12 amendment): 3x DEGRADED does NOT pause.
  Only SnapshotStatus.FAILED and uncaught exceptions count as failures.
- Counter persists to SQLite (scheduler_state table, singleton row) so a
  restart after 2 failures still knows it's at counter=2.
- _run_snapshot is async and injectable (tests replace it with AsyncMock).
- _on_recovering() alerts Sentry/Better Stack/Telegram without stopping retries.
- run() is a placeholder loop for local testing; Plan 04 uses Fly scheduled
  machines for real prod cron (not this loop).

Source: RESEARCH.md §Architecture Patterns §2.5, CONTEXT.md D-13
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import signal
import sqlite3
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

from loguru import logger

from polyarb.daemon.producer_arbitration import ProducerArbitrator, ProducerLease
from polyarb.daemon.structure_schedule import derive_structure_schedule
from polyarb.perception.structure_contract import (
    STRUCTURE_DRIFT_CLASSIFIER_V4,
    STRUCTURE_DRIFT_SOURCE_EVENT_MAX_ROWS,
    STRUCTURE_GENERATION_CHILD_HARD_LIMIT_S,
    valid_structure_publication_checkpoint,
)
from polyarb.validator.category import SnapshotStatus


class SchedulerState(StrEnum):
    """Producer lifecycle; RECOVERING remains active but is health-visible."""

    RUNNING = "RUNNING"
    RECOVERING = "RECOVERING"
    # Kept only to read historical state written before H-011.  New code must
    # migrate it to RECOVERING rather than preserve a terminal producer stop.
    PAUSED = "PAUSED"


def _structure_drift_defer_contract(status: dict[str, object]) -> str:
    contract = status.get("classifier_contract_version")
    return contract if isinstance(contract, str) and contract else STRUCTURE_DRIFT_CLASSIFIER_V4


class SnapshotSubprocessError(RuntimeError):
    """The isolated snapshot process did not return one bounded result."""

    def __init__(
        self,
        reason: str,
        *,
        last_stage: str | None = None,
        elapsed_ms: int = 0,
        chunks_processed: int | None = None,
        rows_processed: int | None = None,
        stderr: bytes = b"",
    ) -> None:
        super().__init__(f"snapshot-subprocess-{reason}")
        self.last_stage = last_stage
        self.elapsed_ms = max(0, elapsed_ms)
        self.chunks_processed = chunks_processed
        self.rows_processed = rows_processed
        self.stderr_bytes = len(stderr)
        self.stderr_sha256 = hashlib.sha256(stderr).hexdigest()
        self.stderr_tail = _safe_stderr_tail(stderr)


class StructureDriftCancelled(asyncio.CancelledError):
    """Cancellation that retains only bounded parent-observed commit evidence."""

    def __init__(
        self,
        *,
        last_stage: str | None,
        chunks_processed: int,
        rows_processed: int,
        stderr: bytes,
    ) -> None:
        super().__init__()
        self.last_stage = last_stage
        self.chunks_processed = chunks_processed
        self.rows_processed = rows_processed
        self.stderr_bytes = len(stderr)
        self.stderr_sha256 = hashlib.sha256(stderr).hexdigest()
        self.stderr_tail: str | None = None


SNAPSHOT_SUBPROCESS_TIMEOUT_S = 240.0
# Structure shares one memory/SQLite-heavy producer lane with the Quote feed.
# A longer adaptive snapshot timeout may improve completion probability, but
# it must never let one Structure child monopolize that lane past the amount
# production can absorb without violating the 300-second Quote hard SLA.
STRUCTURE_SLICE_MAX_PAGES = 40
STRUCTURE_SLICE_MAX_ELAPSED_S = 45.0
STRUCTURE_DEFER_RETRY_DELAY_S = 5.0
RECOVERY_RETRY_DELAY_S = 5.0
MAX_RECOVERY_RETRY_DELAY_S = 60.0


def recovery_retry_delay_s(failure_counter: int) -> float:
    """Back off repeated recovery collisions without ever disabling retries."""
    exponent = max(0, min(failure_counter - 1, 4))
    return min(
        MAX_RECOVERY_RETRY_DELAY_S,
        RECOVERY_RETRY_DELAY_S * (2**exponent),
    )


def structure_attempt_slot_budget_s(publication_status: object) -> float:
    """Bound the complete child; pointer transaction timing is store-owned."""
    return STRUCTURE_GENERATION_CHILD_HARD_LIMIT_S


_SNAPSHOT_STAGE_MARKER_RE = re.compile(
    rb"^snapshot-stage stage="
    rb"(gamma-events|gamma-markets|membership-recheck|validate|persist) "
    rb"state=(?:start|complete) elapsed_ms=(?:0|[1-9][0-9]*)$",
    re.MULTILINE,
)
_STRUCTURE_PAGE_BOUNDARY_RE = re.compile(
    rb"^structure-page-boundary stage=(gamma-events|gamma-markets) "
    rb"operation=(fetch|commit) state=(?:start|complete) "
    rb"elapsed_ms=(?:0|[1-9][0-9]*)$",
    re.MULTILINE,
)
_STRUCTURE_PROGRESS_MARKER_RE = re.compile(
    rb"^structure-publication-progress "
    rb"stage=(?:normalizing|certifying|ready) "
    rb"component=(?:[a-z][a-z_-]{0,31}|none) "
    rb"chunks=(100|[1-9][0-9]?) rows=(?:0|[1-9][0-9]*)$",
    re.MULTILINE,
)
_STRUCTURE_FAILURE_MARKER_RE = re.compile(
    rb"^structure-sync-failure failure_kind="
    rb"(?:membership-invalid(?: membership_kind=(?:active-market-missing|group-truth|"
    rb"market-identity|terminal-invariant) key_sha256=[0-9a-f]{64})?|"
    rb"generation-count-mismatch|generation-incomplete|generation-validation-issues|"
    rb"pointer-switch-deadline|source-truth-invalid|sqlite-busy|structure-child-error|"
    rb"structure-page-deadline|"
    rb"structure-publication-not-writing)$",
    re.MULTILINE,
)
_STRUCTURE_SUPERSESSION_MARKER_RE = re.compile(
    rb"^structure-publication-superseded publication_id=[0-9a-f]{32}$",
    re.MULTILINE,
)
_STRUCTURE_DRIFT_MARKER_RE = re.compile(
    rb"^structure-drift stage=(?P<phase>source-events|source-markets|generation-members|"
    rb"legacy-members|fresh-group-truth|sealed|stale|exact|none) "
    rb"chunks=(?P<chunks>0|[1-9][0-9]*) rows=(?P<rows>0|[1-9][0-9]*)$",
    re.MULTILINE,
)


def _parse_last_snapshot_stage(stderr: bytes) -> str | None:
    """Extract only the final fixed-vocabulary stage marker from child stderr."""
    last_stage: str | None = None
    for marker in _SNAPSHOT_STAGE_MARKER_RE.finditer(stderr):
        last_stage = marker.group(1).decode("ascii")
    return last_stage


def _parse_last_structure_chunks(stderr: bytes) -> int | None:
    """Extract the last fully flushed committed-chunk marker from child stderr."""
    chunks_processed: int | None = None
    for marker in _STRUCTURE_PROGRESS_MARKER_RE.finditer(stderr):
        chunks_processed = int(marker.group(1))
    return chunks_processed


def _safe_stderr_tail(stderr: bytes) -> str | None:
    """Retain only the final allowlisted marker, never arbitrary child output."""
    matches = [*_SNAPSHOT_STAGE_MARKER_RE.finditer(stderr)]
    matches.extend(_STRUCTURE_PAGE_BOUNDARY_RE.finditer(stderr))
    matches.extend(_STRUCTURE_PROGRESS_MARKER_RE.finditer(stderr))
    matches.extend(_STRUCTURE_FAILURE_MARKER_RE.finditer(stderr))
    matches.extend(_STRUCTURE_SUPERSESSION_MARKER_RE.finditer(stderr))
    matches.extend(_STRUCTURE_DRIFT_MARKER_RE.finditer(stderr))
    if not matches:
        return None
    tail = max(matches, key=lambda match: match.start()).group(0).decode("ascii")
    return tail if len(tail) <= 256 else None


def _parse_last_structure_drift_marker(
    stderr: bytes,
    *,
    max_rows: int,
    max_chunks: int,
) -> tuple[str | None, int, int, str | None]:
    """Return only the last post-CAS drift marker as committed parent evidence."""
    matches = [*_STRUCTURE_DRIFT_MARKER_RE.finditer(stderr)]
    if not matches:
        return None, 0, 0, None
    marker = matches[-1]
    phase = marker.group("phase").decode("ascii")
    chunks = int(marker.group("chunks"))
    rows = int(marker.group("rows"))
    phase_row_limit = (
        STRUCTURE_DRIFT_SOURCE_EVENT_MAX_ROWS if phase == "source-events" else max_rows
    )
    if (
        chunks == 0
        or chunks > max_chunks
        or rows > max_rows * chunks
        or rows > phase_row_limit * chunks
    ):
        return None, 0, 0, None
    return phase, chunks, rows, marker.group(0).decode("ascii")


@dataclass(frozen=True)
class IsolatedSnapshotResult:
    status: SnapshotStatus
    snapshot_id: int
    market_count: int
    issue_count: int
    last_stage: str | None
    elapsed_ms: int


@dataclass(frozen=True)
class IsolatedStructureCheckpoint:
    window_id: str
    stage: str
    pages_processed: int
    elapsed_ms: int


@dataclass(frozen=True)
class IsolatedStructurePublicationCheckpoint:
    stage: str
    component: str | None
    rows_processed: int
    cursor: str | None
    publication_id: str
    elapsed_ms: int
    chunks_processed: int = 1


@dataclass(frozen=True)
class IsolatedStructureDriftCheckpoint:
    phase: str | None
    rows_processed: int
    chunks_processed: int
    ready: bool
    deferred: bool
    defer_reason: str | None
    stop_reason: str
    elapsed_ms: int
    stderr: bytes = b""


@dataclass(frozen=True)
class IsolatedStructureEventMemberCheckpoint:
    window_id: str
    rows_processed: int
    chunks_processed: int
    sealed: bool
    deferred: bool
    defer_reason: str | None
    stop_reason: str
    elapsed_ms: int


async def run_structure_event_members_in_subprocess(
    *,
    db_path: object,
    window_id: str,
    max_rows: int = 500,
    max_chunks: int = 100,
    max_elapsed_s: float = 45.0,
    spawn: Callable[..., Awaitable[asyncio.subprocess.Process]] = asyncio.create_subprocess_exec,
    timeout_s: float = 75.0,
    terminate_timeout_s: float = 15.0,
) -> IsolatedStructureEventMemberCheckpoint:
    """Run one bounded event-member slice outside the resident scheduler."""
    process = await spawn(
        sys.executable,
        "-m",
        "polyarb.snapshot",
        "structure-event-members-advance",
        "--db-path",
        str(db_path),
        "--window-id",
        window_id,
        "--max-rows",
        str(max_rows),
        "--max-chunks",
        str(max_chunks),
        "--max-elapsed-seconds",
        str(max_elapsed_s),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    started = time.monotonic()
    communicate_task = asyncio.create_task(process.communicate())

    async def terminate_then_kill() -> tuple[bytes, bytes]:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            return await asyncio.wait_for(
                asyncio.shield(communicate_task), timeout=terminate_timeout_s
            )
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            return await asyncio.shield(communicate_task)

    try:
        stdout, stderr = await asyncio.wait_for(asyncio.shield(communicate_task), timeout=timeout_s)
    except asyncio.CancelledError:
        await terminate_then_kill()
        raise
    except TimeoutError as error:
        await terminate_then_kill()
        raise SnapshotSubprocessError(
            "structure-event-members-timeout",
            last_stage="structure-event-members",
            elapsed_ms=max(0, int((time.monotonic() - started) * 1_000)),
        ) from error
    try:
        payload = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        reason = "sqlite-busy" if b"database is locked" in stderr.lower() else "invalid-json"
        raise SnapshotSubprocessError(f"structure-event-members-{reason}") from error
    expected_keys = {
        "checkpointed",
        "chunks_processed",
        "defer_reason",
        "deferred",
        "elapsed_ms",
        "kind",
        "rows_processed",
        "sealed",
        "stop_reason",
        "window_id",
    }
    defer_reason = payload.get("defer_reason") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or set(payload) != expected_keys
        or payload["checkpointed"] is not True
        or payload["kind"] != "structure-event-members"
        or payload["window_id"] != window_id
        or type(payload["rows_processed"]) is not int
        or not 0 <= payload["rows_processed"] <= max_rows * max_chunks
        or type(payload["chunks_processed"]) is not int
        or not 0 <= payload["chunks_processed"] <= max_chunks
        or type(payload["elapsed_ms"]) is not int
        or payload["elapsed_ms"] < 0
        or type(payload["sealed"]) is not bool
        or type(payload["deferred"]) is not bool
        or (defer_reason is not None and not isinstance(defer_reason, str))
        or bool(payload["deferred"]) != (defer_reason == "writer-busy")
        or payload["stop_reason"]
        not in {"complete", "max-chunks", "max-elapsed-seconds", "writer-busy"}
        or process.returncode != 0
    ):
        raise SnapshotSubprocessError("structure-event-members-invalid-json")
    return IsolatedStructureEventMemberCheckpoint(
        window_id=window_id,
        rows_processed=int(payload["rows_processed"]),
        chunks_processed=int(payload["chunks_processed"]),
        sealed=bool(payload["sealed"]),
        deferred=bool(payload["deferred"]),
        defer_reason=defer_reason,
        stop_reason=str(payload["stop_reason"]),
        elapsed_ms=int(payload["elapsed_ms"]),
    )


async def run_structure_drift_in_subprocess(
    *,
    db_path: object,
    max_rows: int,
    max_chunks: int,
    max_elapsed_s: float,
    spawn: Callable[..., Awaitable[asyncio.subprocess.Process]] = (asyncio.create_subprocess_exec),
    timeout_s: float = 75.0,
    terminate_timeout_s: float = 15.0,
) -> IsolatedStructureDriftCheckpoint:
    """Run one cooperative drift slice outside the scheduler event loop."""
    process = await spawn(
        sys.executable,
        "-m",
        "polyarb.snapshot",
        "structure-generation-drift-advance",
        "--db-path",
        str(db_path),
        "--max-rows",
        str(max_rows),
        "--max-chunks",
        str(max_chunks),
        "--max-elapsed-seconds",
        str(max_elapsed_s),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    started = time.monotonic()
    communicate_task = asyncio.create_task(process.communicate())

    async def terminate_then_kill() -> tuple[bytes, bytes]:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            return await asyncio.wait_for(
                asyncio.shield(communicate_task),
                timeout=terminate_timeout_s,
            )
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            return await asyncio.shield(communicate_task)

    def elapsed_ms() -> int:
        return max(0, int((time.monotonic() - started) * 1_000))

    def drift_error(reason: str, stderr: bytes) -> SnapshotSubprocessError:
        phase, chunks, rows, safe_marker = _parse_last_structure_drift_marker(
            stderr,
            max_rows=max_rows,
            max_chunks=max_chunks,
        )
        result = SnapshotSubprocessError(
            reason,
            last_stage=phase or "structure-drift",
            elapsed_ms=elapsed_ms(),
            chunks_processed=chunks,
            rows_processed=rows,
            stderr=stderr,
        )
        result.stderr_tail = safe_marker
        return result

    try:
        stdout, stderr = await asyncio.wait_for(
            asyncio.shield(communicate_task),
            timeout=timeout_s,
        )
    except asyncio.CancelledError as error:
        _, stderr = await terminate_then_kill()
        phase, chunks, rows, safe_marker = _parse_last_structure_drift_marker(
            stderr,
            max_rows=max_rows,
            max_chunks=max_chunks,
        )
        cancelled = StructureDriftCancelled(
            last_stage=phase,
            chunks_processed=chunks,
            rows_processed=rows,
            stderr=stderr,
        )
        cancelled.stderr_tail = safe_marker
        raise cancelled from error
    except TimeoutError as error:
        _, stderr = await terminate_then_kill()
        raise drift_error("structure-drift-timeout", stderr) from error
    if process.returncode is not None and process.returncode < 0:
        signal_number = -process.returncode
        try:
            signal_name = signal.Signals(signal_number).name.lower()
        except ValueError:
            signal_name = str(signal_number)
        suffix = "-possible-oom" if signal_number == signal.SIGKILL else ""
        raise drift_error(f"structure-drift-signal-{signal_name}{suffix}", stderr)
    try:
        payload = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        reason = "sqlite-busy" if b"database is locked" in stderr.lower() else "invalid-json"
        raise drift_error(f"structure-drift-{reason}", stderr) from error
    if (
        payload
        == {
            "failed": True,
            "failure_kind": "source-event-workload-oversized",
        }
        and process.returncode == 1
    ):
        raise drift_error(
            "structure-drift-source-event-workload-oversized",
            stderr,
        )
    expected_keys = {
        "checkpointed",
        "chunks_processed",
        "defer_reason",
        "deferred",
        "elapsed_ms",
        "kind",
        "phase",
        "ready",
        "rows_processed",
        "stop_reason",
    }
    if not isinstance(payload, dict) or set(payload) != expected_keys:
        raise SnapshotSubprocessError("structure-drift-invalid-json")
    phase = payload["phase"]
    defer_reason = payload["defer_reason"]
    allowed_phases = {
        None,
        "source-events",
        "source-markets",
        "fresh-projection-members",
        "generation-members",
        "legacy-members",
        "fresh-group-truth",
        "sealed",
        "stale",
        "exact",
    }
    allowed_stop_reasons = {
        "complete",
        "identity-stale",
        "max-chunks",
        "max-elapsed-seconds",
        "not-pending",
        "stale",
        "writer-busy",
    }
    if (
        payload["checkpointed"] is not True
        or payload["kind"] != "structure-drift"
        or phase not in allowed_phases
        or isinstance(payload["rows_processed"], bool)
        or not isinstance(payload["rows_processed"], int)
        or payload["rows_processed"] < 0
        or payload["rows_processed"] > max_rows * max_chunks
        or isinstance(payload["chunks_processed"], bool)
        or not isinstance(payload["chunks_processed"], int)
        or not 0 <= payload["chunks_processed"] <= max_chunks
        or not isinstance(payload["ready"], bool)
        or not isinstance(payload["deferred"], bool)
        or (defer_reason is not None and not isinstance(defer_reason, str))
        or payload["stop_reason"] not in allowed_stop_reasons
        or isinstance(payload["elapsed_ms"], bool)
        or not isinstance(payload["elapsed_ms"], int)
        or payload["elapsed_ms"] < 0
        or (bool(payload["deferred"]) != (defer_reason in {"writer-busy", "identity-stale"}))
        or process.returncode != 0
    ):
        raise SnapshotSubprocessError("structure-drift-invalid-json")
    return IsolatedStructureDriftCheckpoint(
        phase=phase,
        rows_processed=int(payload["rows_processed"]),
        chunks_processed=int(payload["chunks_processed"]),
        ready=bool(payload["ready"]),
        deferred=bool(payload["deferred"]),
        defer_reason=defer_reason,
        stop_reason=str(payload["stop_reason"]),
        elapsed_ms=int(payload["elapsed_ms"]),
        stderr=stderr,
    )


async def run_snapshot_in_subprocess(
    *,
    spawn: Callable[..., Awaitable[asyncio.subprocess.Process]] = (asyncio.create_subprocess_exec),
    timeout_s: float = SNAPSHOT_SUBPROCESS_TIMEOUT_S,
    terminate_timeout_s: float = 3.0,
) -> IsolatedSnapshotResult:
    """Run the CPU/GIL-heavy snapshot pipeline outside the HTTP process."""
    process = await spawn(
        sys.executable,
        "-m",
        "polyarb.snapshot",
        "structure-sync",
        "--json",
        "--low-priority",
        "--schema-ready",
        "--max-pages",
        str(STRUCTURE_SLICE_MAX_PAGES),
        "--max-elapsed-seconds",
        str(STRUCTURE_SLICE_MAX_ELAPSED_S),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    started = time.monotonic()
    logger.info(f"isolated snapshot started pid={getattr(process, 'pid', None)}")
    # Own one pipe-reader task for the child lifetime.  A timeout/cancellation
    # must not cancel the reader and then attempt a second communicate(), which
    # can leave the original child alive after the scheduler records failure.
    communicate_task = asyncio.create_task(process.communicate())

    async def terminate_then_kill() -> tuple[bytes, bytes]:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            return await asyncio.wait_for(
                asyncio.shield(communicate_task),
                timeout=terminate_timeout_s,
            )
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            return await asyncio.shield(communicate_task)

    def elapsed_ms() -> int:
        return max(0, int((time.monotonic() - started) * 1000))

    def subprocess_error(
        reason: str,
        *,
        stderr: bytes,
    ) -> SnapshotSubprocessError:
        return SnapshotSubprocessError(
            reason,
            last_stage=_parse_last_snapshot_stage(stderr),
            elapsed_ms=elapsed_ms(),
            chunks_processed=_parse_last_structure_chunks(stderr),
            stderr=stderr,
        )

    try:
        stdout, stderr = await asyncio.wait_for(
            asyncio.shield(communicate_task),
            timeout=timeout_s,
        )
    except asyncio.CancelledError:
        await terminate_then_kill()
        raise
    except TimeoutError as error:
        logger.error(
            f"isolated snapshot timed out pid={getattr(process, 'pid', None)} timeout_s={timeout_s}"
        )
        _, stderr = await terminate_then_kill()
        raise subprocess_error("timeout", stderr=stderr) from error

    last_stage = _parse_last_snapshot_stage(stderr)
    process_elapsed_ms = elapsed_ms()

    if process.returncode is not None and process.returncode < 0:
        signal_number = -process.returncode
        try:
            signal_name = signal.Signals(signal_number).name.lower()
        except ValueError:
            signal_name = str(signal_number)
        possible_oom = signal_number == signal.SIGKILL
        logger.error(
            "isolated snapshot terminated by signal "
            f"pid={getattr(process, 'pid', None)} "
            f"exit_class=signal signal={signal_name.upper()} "
            f"oom_hint={'possible-cgroup-oom' if possible_oom else 'none'} "
            f"stderr_bytes={len(stderr)}"
        )
        suffix = "-possible-oom" if possible_oom else ""
        raise SnapshotSubprocessError(
            f"signal-{signal_name}{suffix}",
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
            chunks_processed=_parse_last_structure_chunks(stderr),
            stderr=stderr,
        )

    try:
        payload = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        reason = "sqlite-busy" if b"database is locked" in stderr.lower() else "invalid-json"
        logger.warning(
            "isolated snapshot returned invalid output "
            f"returncode={process.returncode} failure_kind={reason} "
            f"stderr_bytes={len(stderr)}"
        )
        raise SnapshotSubprocessError(
            reason,
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
            chunks_processed=_parse_last_structure_chunks(stderr),
            stderr=stderr,
        ) from error
    if not isinstance(payload, dict):
        raise SnapshotSubprocessError(
            "invalid-json",
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
            stderr=stderr,
        )
    if payload.get("failed") is True:
        failure_kind = payload.get("failure_kind")
        if (
            set(payload) != {"failed", "failure_kind"}
            or failure_kind
            not in {
                "generation-count-mismatch",
                "generation-incomplete",
                "generation-validation-issues",
                "membership-invalid",
                "pointer-switch-deadline",
                "source-truth-invalid",
                "sqlite-busy",
                "structure-page-deadline",
                "structure-child-error",
                "structure-publication-not-writing",
            }
            or process.returncode != 1
        ):
            failure_kind = "invalid-json"
        raise SnapshotSubprocessError(
            str(failure_kind),
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
            chunks_processed=_parse_last_structure_chunks(stderr),
            stderr=stderr,
        )
    if payload.get("checkpointed") is True:
        publication_id = payload.get("publication_id")
        if publication_id is not None:
            stage = payload.get("stage")
            component = payload.get("component")
            rows_processed = payload.get("rows_processed")
            cursor = payload.get("cursor")
            chunks_processed = payload.get("chunks_processed", 1)
            child_elapsed_ms = payload.get("elapsed_ms", 0)
            if (
                not isinstance(publication_id, str)
                or not publication_id
                or not valid_structure_publication_checkpoint(stage, component)
                or isinstance(rows_processed, bool)
                or not isinstance(rows_processed, int)
                or rows_processed < 0
                or isinstance(chunks_processed, bool)
                or not isinstance(chunks_processed, int)
                or not 0 <= chunks_processed <= 100
                or isinstance(child_elapsed_ms, bool)
                or not isinstance(child_elapsed_ms, int)
                or child_elapsed_ms < 0
                or (cursor is not None and not isinstance(cursor, str))
                or (
                    stage == "superseded"
                    and (
                        rows_processed != 0
                        or cursor is not None
                        or chunks_processed != 1
                        or re.fullmatch(r"[0-9a-f]{32}", publication_id) is None
                    )
                )
                or process.returncode != 0
            ):
                raise SnapshotSubprocessError(
                    "invalid-json",
                    last_stage=last_stage,
                    elapsed_ms=process_elapsed_ms,
                    stderr=stderr,
                )
            logger.info(
                "isolated Structure publication checkpointed "
                f"pid={getattr(process, 'pid', None)} elapsed_ms={process_elapsed_ms} "
                f"stage={stage} component={component} rows={rows_processed} "
                f"chunks={chunks_processed} publication_id={publication_id}"
            )
            return IsolatedStructurePublicationCheckpoint(
                stage=str(stage),
                component=None if component is None else str(component),
                rows_processed=rows_processed,
                cursor=None if cursor is None else str(cursor),
                publication_id=publication_id,
                elapsed_ms=process_elapsed_ms,
                chunks_processed=chunks_processed,
            )
        window_id = payload.get("window_id")
        stage = payload.get("stage")
        pages_processed = payload.get("pages_processed")
        if (
            not isinstance(window_id, str)
            or not window_id
            or stage not in {"events", "markets", "complete", "bootstrap"}
            or isinstance(pages_processed, bool)
            or not isinstance(pages_processed, int)
            or pages_processed < 1
            or process.returncode != 0
        ):
            raise SnapshotSubprocessError(
                "invalid-json",
                last_stage=last_stage,
                elapsed_ms=process_elapsed_ms,
                stderr=stderr,
            )
        logger.info(
            "isolated snapshot checkpointed "
            f"pid={getattr(process, 'pid', None)} "
            f"elapsed_ms={process_elapsed_ms} stage={stage} "
            f"pages_processed={pages_processed} window_id={window_id}"
        )
        return IsolatedStructureCheckpoint(
            window_id=window_id,
            stage=stage,
            pages_processed=pages_processed,
            elapsed_ms=process_elapsed_ms,
        )
    try:
        status = SnapshotStatus(str(payload.get("status", "")).lower())
    except ValueError as error:
        raise SnapshotSubprocessError(
            "invalid-json",
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
            stderr=stderr,
        ) from error
    is_valid = payload.get("is_valid")
    snapshot_id = payload.get("snapshot_id")
    market_count = payload.get("market_count")
    issue_count = payload.get("issue_count")
    if (
        not isinstance(is_valid, bool)
        or isinstance(snapshot_id, bool)
        or not isinstance(snapshot_id, int)
        or snapshot_id <= 0
        or isinstance(market_count, bool)
        or not isinstance(market_count, int)
        or market_count < 0
        or isinstance(issue_count, bool)
        or not isinstance(issue_count, int)
        or issue_count < 0
        or (status == SnapshotStatus.FAILED) == is_valid
        or (process.returncode == 0) != is_valid
    ):
        raise SnapshotSubprocessError(
            "invalid-json",
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
            stderr=stderr,
        )
    logger.info(
        "isolated snapshot complete "
        f"pid={getattr(process, 'pid', None)} "
        f"elapsed_ms={process_elapsed_ms} "
        f"last_stage={last_stage} "
        f"status={status.value} "
        f"snapshot_id={snapshot_id} "
        f"market_count={market_count} "
        f"issue_count={issue_count}"
    )
    return IsolatedSnapshotResult(
        status=status,
        snapshot_id=snapshot_id,
        market_count=market_count,
        issue_count=issue_count,
        last_stage=last_stage,
        elapsed_ms=process_elapsed_ms,
    )


class SnapshotScheduler:
    """Manages snapshot scheduling with 5-failure self-recovery protection.

    Usage:
        scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
        await scheduler._tick()       # unit-testable single tick
        await scheduler.run(stop_ev)  # long-running loop (Plan 02 placeholder)
    """

    # Phase 03.1-04 D-02: 3 → 5. Combined with DNS retry (D-01 A) the threshold
    # tolerates ~3min of bursty failure before pausing; healthz-watcher cron
    # (15-min) auto-unpauses well within human-response cadence.
    FAILURE_THRESHOLD = 5

    def __init__(
        self,
        settings: object,
        sqlite_store: object,
        *,
        producer_lock: asyncio.Lock | None = None,
        on_snapshot_published: Callable[[], object] | None = None,
        quote_worker_runtime: object | None = None,
        producer_arbitrator: ProducerArbitrator | None = None,
        on_structure_failure: Callable[[str, int | None, str | None], object] | None = None,
        on_structure_success: Callable[[int], object] | None = None,
    ) -> None:
        self._settings = settings
        self._sqlite_store = sqlite_store
        self._producer_lock = producer_lock
        self._on_snapshot_published = on_snapshot_published
        self._quote_worker_runtime = quote_worker_runtime
        self._producer_arbitrator = producer_arbitrator
        self._producer_lease: ProducerLease | None = None
        self._on_structure_failure = on_structure_failure
        self._on_structure_success = on_structure_success
        self._checkpoint_pending = False
        self._producer_slot_owned = False
        self._admitted_timeout_s: float | None = None
        self._tick_lock = asyncio.Lock()

        # Restore state from DB (test_counter_persists_across_restart)
        self._failure_counter = 0
        self.state = SchedulerState.RUNNING
        self._request_now_event = asyncio.Event()
        self._restore_state()
        self._recover_structure_drift_attempts()
        self._effective_timeout_s = int(SNAPSHOT_SUBPROCESS_TIMEOUT_S)
        self._effective_cadence_s = int(getattr(settings, "scheduler_interval_s", 300))
        self._restore_effective_schedule()

    def _recover_structure_drift_attempts(self) -> None:
        """Terminalize parent-owned drift children left running by a restart."""
        try:
            recovered = self._sqlite_store.recover_orphaned_structure_drift_attempts(
                recovered_at_ms=int(time.time() * 1_000)
            )
            if recovered:
                logger.warning(f"recovered orphaned structure drift attempts count={recovered}")
        except (AttributeError, OSError, sqlite3.Error, TypeError, ValueError):
            logger.warning("could not recover structure drift attempt evidence")

    def _finish_structure_drift_attempt(self, **evidence: object) -> bool:
        """Bound terminal evidence without leaking ledger faults to snapshot state."""
        try:
            self._sqlite_store.finish_structure_drift_attempt(
                **evidence,
                writer_timeout_s=0.25,
            )
            return True
        except (OSError, sqlite3.Error, TypeError, ValueError) as error:
            logger.error(
                "structure drift terminal evidence unavailable "
                f"kind={type(error).__name__} attempt_id={evidence.get('attempt_id')}"
            )
            return False

    @property
    def effective_timeout_s(self) -> int:
        return self._effective_timeout_s

    @property
    def effective_cadence_s(self) -> int:
        return self._effective_cadence_s

    @property
    def legacy_reconciliation_enabled(self) -> bool:
        """Whether the universe-sized legacy Structure loop may run."""
        return bool(
            getattr(
                self._settings,
                "legacy_structure_reconciliation_enabled",
                False,
            )
        )

    @property
    def structure_sync_enabled(self) -> bool:
        """Enable resumable Structure; honor the old flag during migration."""
        return bool(
            getattr(self._settings, "structure_sync_enabled", False)
            or self.legacy_reconciliation_enabled
        )

    @property
    def structure_drift_compare_enabled(self) -> bool:
        """Keep the drift-safe maintenance writer explicitly default-off."""
        return bool(
            getattr(
                self._settings,
                "structure_generation_drift_compare_enabled",
                False,
            )
        )

    def _restore_effective_schedule(self) -> None:
        """Restore or derive one bounded schedule from durable attempt truth."""
        try:
            latest_adjustment = self._sqlite_store.get_latest_structure_schedule_adjustment()
            if latest_adjustment is not None:
                self._effective_timeout_s = int(latest_adjustment["timeout_s"])
                self._effective_cadence_s = int(latest_adjustment["cadence_s"])

            attempts = self._sqlite_store.get_snapshot_attempts(limit=30)
            if not attempts:
                return
            source_attempt_id = max(int(row["id"]) for row in attempts)
            latest_source_id = (
                int(latest_adjustment["source_attempt_id"])
                if latest_adjustment is not None
                else None
            )
            if latest_source_id == source_attempt_id:
                return

            attempts_since_adjustment = (
                max(0, source_attempt_id - latest_source_id) if latest_source_id is not None else 3
            )
            decision = derive_structure_schedule(
                attempts,
                configured_timeout_s=int(SNAPSHOT_SUBPROCESS_TIMEOUT_S),
                configured_cadence_s=int(getattr(self._settings, "scheduler_interval_s", 300)),
                previous_timeout_s=self._effective_timeout_s,
                previous_cadence_s=self._effective_cadence_s,
                attempts_since_adjustment=attempts_since_adjustment,
            )
            changed = (
                decision.timeout_s != self._effective_timeout_s
                or decision.cadence_s != self._effective_cadence_s
            )
            if not changed:
                return
            previous_timeout_s = self._effective_timeout_s
            previous_cadence_s = self._effective_cadence_s
            self._sqlite_store.append_structure_schedule_adjustment(
                source_attempt_id=source_attempt_id,
                decided_at_ms=int(time.time() * 1_000),
                success_sample_count=decision.success_sample_count,
                success_p95_s=decision.success_p95_s,
                previous_timeout_s=previous_timeout_s,
                previous_cadence_s=previous_cadence_s,
                timeout_s=decision.timeout_s,
                cadence_s=decision.cadence_s,
                reason=decision.reason,
            )
            self._effective_timeout_s = decision.timeout_s
            self._effective_cadence_s = decision.cadence_s
            logger.info(
                "structure schedule adjusted "
                f"timeout_s={decision.timeout_s} cadence_s={decision.cadence_s} "
                f"reason={decision.reason} source_attempt_id={source_attempt_id}"
            )
        except Exception as error:  # noqa: BLE001 - static config remains safe fallback
            logger.warning(
                f"could not restore adaptive structure schedule kind={type(error).__name__}"
            )

    def _restore_state(self) -> None:
        """Read scheduler_state from SQLite and restore counter + state."""
        try:
            row = self._sqlite_store.get_scheduler_state()
            if row:
                self._failure_counter = int(row.get("failure_counter", 0))
                state_str = row.get("state", "RUNNING").upper()
                self.state = (
                    SchedulerState(state_str)
                    if state_str in SchedulerState.__members__
                    else SchedulerState.RUNNING
                )
                if self.state == SchedulerState.PAUSED:
                    # A persisted pre-H-011 pause must not leave production
                    # unable to make a recovery attempt after a restart.
                    self.state = SchedulerState.RECOVERING
                    self._persist_counter()
                logger.debug(
                    f"scheduler state restored: state={self.state} "
                    f"failure_counter={self._failure_counter}"
                )
        except Exception:
            logger.warning("could not restore scheduler state from DB, starting fresh")

    def _persist_counter(self) -> None:
        """Write current state + counter to scheduler_state SQLite table."""
        try:
            self._sqlite_store.upsert_scheduler_state(
                state=self.state.value,
                failure_counter=self._failure_counter,
            )
        except Exception:
            logger.warning("could not persist scheduler state to DB")

    def _record_durable_progress(self) -> None:
        """Break a failure streak without certifying full scheduler recovery."""
        if self._failure_counter == 0:
            return
        self._failure_counter = 0
        self._persist_counter()

    async def _resolve_structure_timeout_s(self) -> float:
        publication = await asyncio.to_thread(self._sqlite_store.get_latest_structure_publication)
        try:
            publication_status = publication.status if publication is not None else None
        except AttributeError:
            publication_status = None
        return min(
            self._effective_timeout_s,
            structure_attempt_slot_budget_s(publication_status),
        )

    async def _run_snapshot(self) -> object:
        """Run a snapshot and return a result with .status attribute.

        This method is injectable — tests replace it with AsyncMock.
        Real prod wires it to the orchestrator in Plan 04.
        """

        async def run_in_slot() -> object:
            timeout_s = self._admitted_timeout_s
            if timeout_s is None:
                timeout_s = await self._resolve_structure_timeout_s()
            return await run_snapshot_in_subprocess(timeout_s=timeout_s)

        if self._producer_lock is None or self._producer_slot_owned:
            return await run_in_slot()
        async with self._producer_lock:
            return await run_in_slot()

    def _quote_priority_reason(self) -> str | None:
        # The in-parent Quote runtime is not authoritative when Quote is
        # supervised in another process; arbitration then happens in SQLite.
        if self._producer_arbitrator is not None:
            return None
        runtime = self._quote_worker_runtime
        if runtime is None:
            return None
        if runtime.pipeline_active():
            return "quote-pipeline-active"
        interval_s = float(getattr(self._settings, "neg_risk_quote_interval_s", 120.0))
        if runtime.pipeline_due(interval_s):
            return "quote-pipeline-due"
        return None

    async def _record_structure_defer(
        self,
        *,
        reason: str,
        queued_at_ms: int,
        initialized_comparison_id: str | None = None,
        current_comparison_id: str | None = None,
        classifier_contract_version: str | None = None,
    ) -> None:
        try:
            await asyncio.to_thread(
                self._sqlite_store.record_structure_defer,
                reason,
                queued_at_ms,
                int(time.time() * 1_000),
                initialized_comparison_id=initialized_comparison_id,
                current_comparison_id=current_comparison_id,
                classifier_contract_version=classifier_contract_version,
            )
        except Exception as error:  # noqa: BLE001 - admission still stays fail-safe
            logger.warning(
                "could not persist Structure defer receipt "
                f"kind={type(error).__name__} reason={reason}"
            )

    def _release_producer_slot(self) -> None:
        if not self._producer_slot_owned and self._producer_lease is None:
            return
        self._producer_slot_owned = False
        if self._producer_lock is not None:
            self._producer_lock.release()
        if self._producer_lease is not None:
            try:
                self._producer_arbitrator.release(self._producer_lease)
            finally:
                self._producer_lease = None

    async def _admit_snapshot(self, *, queued_at_ms: int) -> int | None:
        """Return one attempt id while retaining ownership of its producer slot."""
        reason = self._quote_priority_reason()
        if reason is not None:
            await self._record_structure_defer(
                reason=reason,
                queued_at_ms=queued_at_ms,
            )
            return None

        if self._producer_arbitrator is not None:
            try:
                lease = await asyncio.to_thread(
                    self._producer_arbitrator.acquire,
                    owner="structure",
                    lease_s=STRUCTURE_GENERATION_CHILD_HARD_LIMIT_S,
                )
            except sqlite3.OperationalError as error:
                if not any(token in str(error).lower() for token in ("locked", "busy")):
                    raise
                await self._record_structure_defer(
                    reason="producer-arbitration-writer-busy",
                    queued_at_ms=queued_at_ms,
                )
                self._checkpoint_pending = True
                logger.warning(
                    "Structure producer arbitration deferred for SQLite writer contention"
                )
                return None
            if lease is None:
                await self._record_structure_defer(
                    reason="producer-lease-held",
                    queued_at_ms=queued_at_ms,
                )
                # The durable arbitrator also yields a just-released
                # Structure checkpoint to Quote. Retry at a bounded cadence
                # rather than sleeping the full Structure interval.
                self._checkpoint_pending = True
                return None
            self._producer_lease = lease
        if self._producer_lock is not None:
            await self._producer_lock.acquire()
            self._producer_slot_owned = True
        admitted = False
        try:
            reason = self._quote_priority_reason()
            if reason is None:
                timeout_s = await self._resolve_structure_timeout_s()
                # Quote can become active while publication state is read.
                # This final check is the admission boundary: the attempt
                # insert below is synchronous and the caller's next await is
                # the already-budgeted child itself.
                reason = self._quote_priority_reason()
            if reason is None:
                self._admitted_timeout_s = (
                    min(timeout_s, STRUCTURE_GENERATION_CHILD_HARD_LIMIT_S)
                    if self._producer_arbitrator is not None
                    else timeout_s
                )
                attempt_id = self._sqlite_store.begin_snapshot_attempt(
                    started_at_ms=int(time.time() * 1_000),
                )
                admitted = True
                return attempt_id
            await self._record_structure_defer(
                reason=reason,
                queued_at_ms=queued_at_ms,
            )
            return None
        finally:
            if not admitted:
                self._release_producer_slot()

    async def _maybe_advance_structure_drift(
        self,
        *,
        queued_at_ms: int,
    ) -> bool | None:
        """Own one pending drift chunk before starting a new Structure child."""
        if not self.structure_drift_compare_enabled:
            return None
        try:
            status = await asyncio.to_thread(self._sqlite_store.structure_generation_drift_status)
        except (OSError, sqlite3.Error, TypeError, ValueError) as error:
            await self._record_structure_defer(
                reason="structure-drift-status-unavailable",
                queued_at_ms=queued_at_ms,
                classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V4,
            )
            logger.warning(f"structure drift status unavailable kind={type(error).__name__}")
            return True
        if status.get("authorized") is True or status.get("phase") == "stale":
            return None
        if status.get("reason") not in {
            "structure-drift-progress-missing",
            "structure-drift-incomplete",
        }:
            return None
        window_id = status.get("window_id")
        if isinstance(window_id, str) and window_id:
            try:
                member_status = await asyncio.to_thread(
                    self._sqlite_store.structure_event_member_status,
                    window_id=window_id,
                )
            except (OSError, sqlite3.Error, TypeError, ValueError) as error:
                await self._record_structure_defer(
                    reason="structure-drift-status-unavailable",
                    queued_at_ms=queued_at_ms,
                    current_comparison_id=(
                        str(status["progress_id"])
                        if status.get("progress_id") is not None
                        else None
                    ),
                    classifier_contract_version=_structure_drift_defer_contract(status),
                )
                logger.warning(
                    f"structure drift member authority unavailable kind={type(error).__name__}"
                )
                return True
            if (
                member_status.get("state") == "waiting-natural-window"
                and member_status.get("authenticated") is True
                and member_status.get("reason") == "structure-event-source-receipt-unavailable"
            ):
                await self._record_structure_defer(
                    reason="structure-drift:waiting-natural-window",
                    queued_at_ms=queued_at_ms,
                    current_comparison_id=(
                        str(status["progress_id"])
                        if status.get("progress_id") is not None
                        else None
                    ),
                    classifier_contract_version=_structure_drift_defer_contract(status),
                )
                return None
        reason = self._quote_priority_reason()
        if reason is not None:
            await self._record_structure_defer(
                reason=f"structure-drift:{reason}",
                queued_at_ms=queued_at_ms,
                current_comparison_id=status.get("progress_id"),
                classifier_contract_version=_structure_drift_defer_contract(status),
            )
            return False
        if self._producer_lock is not None:
            await self._producer_lock.acquire()
            self._producer_slot_owned = True
        try:
            reason = self._quote_priority_reason()
            if reason is not None:
                await self._record_structure_defer(
                    reason=f"structure-drift:{reason}",
                    queued_at_ms=queued_at_ms,
                    current_comparison_id=status.get("progress_id"),
                    classifier_contract_version=_structure_drift_defer_contract(status),
                )
                return False
            initialized_comparison_id: str | None = None
            try:
                status = await asyncio.to_thread(
                    self._sqlite_store.structure_generation_drift_status
                )
                if status.get("authorized") is True or status.get("phase") == "stale":
                    return None
                if status.get("reason") not in {
                    "structure-drift-progress-missing",
                    "structure-drift-incomplete",
                }:
                    return None
                # Initialization is the writer-side revalidation boundary: it
                # supersedes only an older active contract and deterministically
                # inserts-or-finds classifier v2 before attempt admission.
                initialized_comparison_id = await asyncio.to_thread(
                    self._sqlite_store.initialize_structure_drift_comparison,
                    now_ms=int(time.time() * 1_000),
                )
                status = await asyncio.to_thread(
                    self._sqlite_store.structure_generation_drift_status
                )
                if status.get("progress_id") != initialized_comparison_id:
                    await self._record_structure_defer(
                        reason="structure-drift-identity-stale",
                        queued_at_ms=queued_at_ms,
                        initialized_comparison_id=initialized_comparison_id,
                        current_comparison_id=status.get("progress_id"),
                        classifier_contract_version=_structure_drift_defer_contract(status),
                    )
                    logger.warning(
                        "structure drift current identity changed after "
                        "classifier initialization; child not spawned"
                    )
                    return True
                if status.get("authorized") is True or status.get("phase") == "stale":
                    return None
                if status.get("reason") != "structure-drift-incomplete" or status.get(
                    "phase"
                ) not in {
                    "source-events",
                    "source-markets",
                    "fresh-projection-members",
                    "generation-members",
                    "legacy-members",
                    "fresh-group-truth",
                }:
                    await self._record_structure_defer(
                        reason="structure-drift-status-unavailable",
                        queued_at_ms=queued_at_ms,
                        initialized_comparison_id=initialized_comparison_id,
                        current_comparison_id=status.get("progress_id"),
                        classifier_contract_version=_structure_drift_defer_contract(status),
                    )
                    logger.warning(
                        "structure drift post-initialization status invalid; child not spawned"
                    )
                    return True
            except (OSError, sqlite3.Error, TypeError, ValueError) as error:
                await self._record_structure_defer(
                    reason="structure-drift-status-unavailable",
                    queued_at_ms=queued_at_ms,
                    initialized_comparison_id=initialized_comparison_id,
                    classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V4,
                )
                logger.warning(
                    "structure drift current-contract initialization unavailable "
                    f"kind={type(error).__name__}; child not spawned"
                )
                return True
            max_rows = int(getattr(self._settings, "structure_generation_drift_max_rows", 500))
            max_chunks = int(
                getattr(
                    self._settings,
                    "structure_generation_drift_max_chunks_per_tick",
                    100,
                )
            )
            slice_s = float(getattr(self._settings, "structure_generation_drift_slice_s", 45.0))
            # Final admission check immediately precedes the only writer call.
            reason = self._quote_priority_reason()
            if reason is not None:
                await self._record_structure_defer(
                    reason=f"structure-drift:{reason}",
                    queued_at_ms=queued_at_ms,
                )
                return False
            identity_keys = (
                "legacy_snapshot_id",
                "generation_snapshot_id",
                "publication_id",
                "window_id",
                "normalization_contract_version",
                "exact_receipt_digest",
                "pointer_validation_hash",
                "generation_certification_hash",
            )
            attempt_started_at_ms = int(time.time() * 1_000)
            try:
                attempt_id = self._sqlite_store.begin_structure_drift_attempt(
                    identity={key: status.get(key) for key in identity_keys},
                    progress_id=(
                        str(status["progress_id"])
                        if status.get("progress_id") is not None
                        else None
                    ),
                    started_at_ms=attempt_started_at_ms,
                    stale_before_ms=attempt_started_at_ms - 90_000,
                )
            except (OSError, sqlite3.Error, TypeError, ValueError) as error:
                logger.warning(
                    "structure drift admission evidence unavailable "
                    f"kind={type(error).__name__}; child not spawned"
                )
                return True
            try:
                checkpoint = await run_structure_drift_in_subprocess(
                    db_path=self._settings.db_path,
                    max_rows=max_rows,
                    max_chunks=max_chunks,
                    max_elapsed_s=slice_s,
                    timeout_s=75.0,
                    terminate_timeout_s=15.0,
                )
            except asyncio.CancelledError as error:
                self._finish_structure_drift_attempt(
                    attempt_id=attempt_id,
                    outcome="cancelled",
                    finished_at_ms=int(time.time() * 1_000),
                    last_phase=getattr(error, "last_stage", None),
                    chunks_processed=getattr(error, "chunks_processed", 0),
                    rows_processed=getattr(error, "rows_processed", 0),
                    elapsed_ms=max(0, int(time.time() * 1_000) - attempt_started_at_ms),
                    failure_kind="scheduler-cancelled",
                    stderr_bytes=getattr(error, "stderr_bytes", 0),
                    stderr_sha256=getattr(error, "stderr_sha256", hashlib.sha256(b"").hexdigest()),
                    stderr_safe_marker=getattr(error, "stderr_tail", None),
                )
                raise
            except SnapshotSubprocessError as error:
                error_text = str(error)
                reason = (
                    "structure-drift-writer-busy"
                    if "sqlite-busy" in error_text
                    else "structure-drift-child-failed"
                )
                self._finish_structure_drift_attempt(
                    attempt_id=attempt_id,
                    outcome="failed",
                    finished_at_ms=int(time.time() * 1_000),
                    last_phase=error.last_stage,
                    chunks_processed=error.chunks_processed or 0,
                    rows_processed=error.rows_processed or 0,
                    elapsed_ms=error.elapsed_ms,
                    failure_kind=error_text.removeprefix("snapshot-subprocess-")[:64],
                    stderr_bytes=error.stderr_bytes,
                    stderr_sha256=error.stderr_sha256,
                    stderr_safe_marker=error.stderr_tail,
                )
                await self._record_structure_defer(
                    reason=reason,
                    queued_at_ms=queued_at_ms,
                )
                # A hard timeout is not itself progress, but the child can
                # commit one or more durable chunks before its parent kills
                # it. Re-read the same authenticated comparison so that only
                # demonstrated forward movement selects the 100ms follow-up
                # cadence. The failed attempt remains immutable evidence.
                if error_text == "snapshot-subprocess-structure-drift-timeout":
                    try:
                        advanced_status = await asyncio.to_thread(
                            self._sqlite_store.structure_generation_drift_status
                        )
                        prior_checkpoint = status.get("checkpoint_at_ms")
                        advanced_checkpoint = advanced_status.get("checkpoint_at_ms")
                        advanced = (
                            advanced_status.get("progress_id") == initialized_comparison_id
                            and advanced_status.get("reason") == "structure-drift-incomplete"
                            and advanced_status.get("phase")
                            in {
                                "source-events",
                                "source-markets",
                                "fresh-projection-members",
                                "generation-members",
                                "legacy-members",
                                "fresh-group-truth",
                            }
                            and type(prior_checkpoint) is int
                            and type(advanced_checkpoint) is int
                            and advanced_checkpoint > prior_checkpoint
                        )
                    except (OSError, sqlite3.Error, TypeError, ValueError):
                        advanced = False
                    if advanced:
                        self._record_durable_progress()
                        self._checkpoint_pending = True
                logger.warning(
                    f"structure drift child failed kind={error_text} elapsed_ms={error.elapsed_ms}"
                )
                return True
            except OSError as error:
                self._finish_structure_drift_attempt(
                    attempt_id=attempt_id,
                    outcome="failed",
                    finished_at_ms=int(time.time() * 1_000),
                    last_phase=None,
                    chunks_processed=0,
                    rows_processed=0,
                    elapsed_ms=max(0, int(time.time() * 1_000) - attempt_started_at_ms),
                    failure_kind=f"spawn-{type(error).__name__}"[:64],
                )
                logger.warning(f"structure drift child spawn failed kind={type(error).__name__}")
                return True
            if checkpoint.deferred:
                defer_reason = (
                    "structure-drift-writer-busy"
                    if checkpoint.defer_reason == "writer-busy"
                    else "structure-drift-identity-stale"
                )
                self._finish_structure_drift_attempt(
                    attempt_id=attempt_id,
                    outcome="deferred",
                    finished_at_ms=int(time.time() * 1_000),
                    last_phase=checkpoint.phase,
                    chunks_processed=checkpoint.chunks_processed,
                    rows_processed=checkpoint.rows_processed,
                    elapsed_ms=checkpoint.elapsed_ms,
                    failure_kind=checkpoint.defer_reason,
                    stderr=checkpoint.stderr,
                )
                await self._record_structure_defer(
                    reason=defer_reason,
                    queued_at_ms=queued_at_ms,
                    initialized_comparison_id=initialized_comparison_id,
                    current_comparison_id=initialized_comparison_id,
                    classifier_contract_version=_structure_drift_defer_contract(status),
                )
                return True
            self._finish_structure_drift_attempt(
                attempt_id=attempt_id,
                outcome="succeeded" if checkpoint.ready else "checkpointed",
                finished_at_ms=int(time.time() * 1_000),
                last_phase=checkpoint.phase,
                chunks_processed=checkpoint.chunks_processed,
                rows_processed=checkpoint.rows_processed,
                elapsed_ms=checkpoint.elapsed_ms,
                failure_kind=None,
                stderr=checkpoint.stderr,
            )
            self._record_durable_progress()
            logger.info(
                "structure drift slice checkpointed "
                f"phase={checkpoint.phase} rows={checkpoint.rows_processed} "
                f"chunks={checkpoint.chunks_processed} stop={checkpoint.stop_reason}"
            )
            if (
                not checkpoint.ready
                and checkpoint.stop_reason in {"max-chunks", "max-elapsed-seconds"}
                and checkpoint.chunks_processed > 0
                and checkpoint.phase
                in {
                    "source-events",
                    "source-markets",
                    "fresh-projection-members",
                    "generation-members",
                    "legacy-members",
                    "fresh-group-truth",
                }
            ):
                # Match event-member and Structure publication continuations:
                # durable non-terminal progress resumes after 100ms, while the
                # next admission still rechecks every Quote-priority gate.
                self._checkpoint_pending = True
            return True
        finally:
            self._release_producer_slot()

    async def _maybe_advance_structure_event_members(self, *, queued_at_ms: int) -> bool | None:
        """Advance one isolated member slice under the Quote-priority lock."""
        if not self.structure_sync_enabled:
            return None
        window = await asyncio.to_thread(self._sqlite_store.get_latest_structure_sync)
        if window is None or window.get("status") not in {"complete", "published"}:
            return None
        window_id = str(window["id"])
        status = await asyncio.to_thread(
            self._sqlite_store.structure_event_member_status, window_id=window_id
        )
        if status.get("sealed") is True:
            return None
        if status.get("reason") == "structure-event-source-receipt-unavailable":
            # Historical windows predate natural source authority.  They are
            # intentionally not backfilled; allow the normal producer to open
            # a fresh authoritative window instead of retrying this one.
            return None
        if (
            status.get("state") == "waiting-event-market-backfill"
            and status.get("authenticated") is True
        ):
            return None
        if status.get("failure_reason") is not None or status.get("reason") is not None:
            logger.error(
                "structure event member derivation unavailable "
                f"reason={status.get('failure_reason') or status.get('reason')}"
            )
            return True
        reason = self._quote_priority_reason()
        if reason is not None:
            await self._record_structure_defer(
                reason=f"structure-event-members:{reason}", queued_at_ms=queued_at_ms
            )
            return False
        if self._producer_lock is not None:
            await self._producer_lock.acquire()
            self._producer_slot_owned = True
        try:
            for _ in range(2):
                reason = self._quote_priority_reason()
                if reason is not None:
                    await self._record_structure_defer(
                        reason=f"structure-event-members:{reason}",
                        queued_at_ms=queued_at_ms,
                    )
                    return False
                if _ == 0:
                    status = await asyncio.to_thread(
                        self._sqlite_store.structure_event_member_status,
                        window_id=window_id,
                    )
                    if status.get("sealed") is True:
                        return None
                    if (
                        status.get("state") == "waiting-event-market-backfill"
                        and status.get("authenticated") is True
                    ):
                        return None
            try:
                checkpoint = await run_structure_event_members_in_subprocess(
                    db_path=self._settings.db_path,
                    window_id=window_id,
                    max_rows=500,
                    max_chunks=100,
                    max_elapsed_s=45.0,
                    timeout_s=75.0,
                    terminate_timeout_s=15.0,
                )
            except SnapshotSubprocessError as error:
                reason = (
                    "structure-event-members:child-timeout"
                    if "timeout" in str(error)
                    else "structure-event-members:child-failed"
                )
                await self._record_structure_defer(reason=reason, queued_at_ms=queued_at_ms)
                raise
            if checkpoint.deferred:
                await self._record_structure_defer(
                    reason="structure-event-members:writer-busy",
                    queued_at_ms=queued_at_ms,
                )
            else:
                self._record_durable_progress()
            logger.info(
                "structure event-member slice checkpointed "
                f"rows={checkpoint.rows_processed} chunks={checkpoint.chunks_processed} "
                f"sealed={checkpoint.sealed} stop={checkpoint.stop_reason}"
            )
            if not checkpoint.sealed and not checkpoint.deferred:
                # The outer resident loop uses this flag for a 100ms
                # continuation instead of the ordinary production cadence.
                # The next call re-enters every Quote-priority admission check.
                self._checkpoint_pending = True
            return True
        finally:
            self._release_producer_slot()

    async def _on_recovering(self) -> None:
        """Alert once when repeated failures require active recovery.

        Plan 02: stub (logs only).
        Plan 05: wires to alerts.send_recovering_alert (Sentry + Better Stack +
            Telegram fallback). We reference via the module attribute
            ``alerts.send_recovering_alert`` (not a from-import) so tests can
            monkeypatch the function on the module.
        """
        logger.error(
            "SCHEDULER_RECOVERING: consecutive failure threshold reached "
            f"(counter={self._failure_counter}). Bounded retries remain active."
        )
        try:
            from polyarb.daemon import alerts as _alerts

            await _alerts.send_recovering_alert(
                self._settings,
                reason=f"{self._failure_counter} consecutive FAILED snapshots",
            )
        except Exception as e:  # noqa: BLE001
            # Alerts are fail-soft: if every channel is unreachable, the
            # daemon should still retain recovery state — losing the notification
            # is bad, losing the recovery path is worse.
            logger.warning(f"send_recovering_alert failed: {e!r}")

    async def _finish_attempt(
        self,
        *,
        attempt_id: int,
        outcome: str,
        snapshot_id: int | None,
        failure_kind: str | None,
        last_stage: str | None = None,
        elapsed_ms: int | None = None,
        chunks_processed: int | None = None,
        stderr_bytes: int | None = None,
        stderr_sha256: str | None = None,
        stderr_tail: str | None = None,
    ) -> None:
        """Best-effort terminal record; scheduler behavior remains primary truth."""
        try:
            await asyncio.to_thread(
                self._sqlite_store.finish_snapshot_attempt,
                attempt_id=attempt_id,
                outcome=outcome,
                finished_at_ms=int(time.time() * 1000),
                snapshot_id=snapshot_id,
                failure_kind=failure_kind,
                last_stage=last_stage,
                elapsed_ms=elapsed_ms,
                chunks_processed=chunks_processed,
                stderr_bytes=stderr_bytes,
                stderr_sha256=stderr_sha256,
                stderr_tail=stderr_tail,
            )
        except Exception as error:  # noqa: BLE001 - operational evidence is fail-soft
            logger.warning(
                "could not finish snapshot attempt "
                f"attempt_id={attempt_id} kind={type(error).__name__}"
            )

    async def _tick(self) -> None:
        """Serialize scheduler entry so one task owns attempt truth at a time."""
        queued_at_ms = int(time.time() * 1_000)
        while True:
            async with self._tick_lock:
                try:
                    completed = await self._tick_once(queued_at_ms=queued_at_ms)
                finally:
                    self._release_producer_slot()
                    self._admitted_timeout_s = None
            if completed:
                return
            await asyncio.sleep(STRUCTURE_DEFER_RETRY_DELAY_S)

    async def _tick_once(self, *, queued_at_ms: int) -> bool:
        """Execute one scheduler tick.

        If RECOVERING: run the same bounded producer step, update evidence, and
        return to RUNNING only after a certified successful result.

        F-04 (Plan 02-08): cancellation propagates. asyncio.CancelledError
        is NOT caught by the generic Exception handler — we re-raise so
        run() can unwind. Wave 5 chaos test gates on this.
        """
        if self.state == SchedulerState.PAUSED:
            # Defensive compatibility for an in-memory legacy caller.  The
            # durable restore path already migrates this value.
            self.state = SchedulerState.RECOVERING

        self._checkpoint_pending = False
        attempt_id: int | None = None

        try:
            member_completed = await self._maybe_advance_structure_event_members(
                queued_at_ms=queued_at_ms
            )
            if member_completed is not None:
                return member_completed
            drift_completed = await self._maybe_advance_structure_drift(queued_at_ms=queued_at_ms)
            if drift_completed is not None:
                return drift_completed
            attempt_id = await self._admit_snapshot(queued_at_ms=queued_at_ms)
            if attempt_id is None:
                return False
            try:
                result = await self._run_snapshot()
            finally:
                self._release_producer_slot()
            if isinstance(
                result,
                (IsolatedStructureCheckpoint, IsolatedStructurePublicationCheckpoint),
            ):
                if isinstance(result, IsolatedStructurePublicationCheckpoint):
                    last_stage = "persist"
                    pages_or_rows = result.rows_processed
                    chunks_processed = result.chunks_processed
                else:
                    last_stage = (
                        "gamma-events"
                        if result.stage == "events"
                        else "persist"
                        if result.stage == "bootstrap"
                        else "gamma-markets"
                    )
                    pages_or_rows = result.pages_processed
                    chunks_processed = None
                await self._finish_attempt(
                    attempt_id=attempt_id,
                    outcome="cancelled",
                    snapshot_id=None,
                    failure_kind=(
                        "structure-contract-superseded"
                        if result.stage == "superseded"
                        else "structure-checkpoint"
                    ),
                    last_stage=last_stage,
                    elapsed_ms=result.elapsed_ms,
                    chunks_processed=chunks_processed,
                )
                if result.stage != "superseded":
                    self._record_durable_progress()
                self._checkpoint_pending = True
                message = (
                    "snapshot tick checkpointed: "
                    f"stage={result.stage} rows_or_pages={pages_or_rows} "
                    f"failure_counter={self._failure_counter}"
                )
                if result.stage == "superseded":
                    logger.warning(
                        "publication contract superseded: "
                        f"publication_id={result.publication_id} "
                        f"failure_counter={self._failure_counter}"
                    )
                else:
                    logger.info(message)
                if result.stage == "superseded":
                    self._persist_counter()
                return True
            result_status = getattr(result, "status", None)

            if result_status in (SnapshotStatus.OK, SnapshotStatus.DEGRADED):
                snapshot_id = getattr(result, "snapshot_id", None)
                if not isinstance(snapshot_id, int) or snapshot_id <= 0:
                    last_stage = getattr(result, "last_stage", None)
                    elapsed_ms = getattr(result, "elapsed_ms", 0)
                    raise SnapshotSubprocessError(
                        "missing-snapshot-id",
                        last_stage=last_stage if isinstance(last_stage, str) else None,
                        elapsed_ms=elapsed_ms if isinstance(elapsed_ms, int) else 0,
                    )
                await self._finish_attempt(
                    attempt_id=attempt_id,
                    outcome="succeeded",
                    snapshot_id=snapshot_id,
                    failure_kind=None,
                    last_stage=getattr(result, "last_stage", None),
                    elapsed_ms=getattr(result, "elapsed_ms", None),
                )
                # DEGRADED is NOT a failure (D-12 amendment)
                self._failure_counter = 0
                if self.state == SchedulerState.RECOVERING:
                    logger.info("structure recovery confirmed by certified snapshot")
                self.state = SchedulerState.RUNNING
                if self._on_structure_success is not None:
                    try:
                        self._on_structure_success(snapshot_id)
                    except Exception as error:
                        logger.warning(
                            "structure recovery incident recording failed "
                            f"kind={type(error).__name__}"
                        )
                logger.info(
                    f"snapshot tick success: status={result_status} failure_counter reset to 0"
                )
                # Publish recovery to /health before any bounded retention.
                # Large staging deletes can take tens of seconds on the
                # production volume and must not keep stale failure evidence
                # visible while certified truth is already online.
                self._persist_counter()
                if self._on_snapshot_published is not None:
                    try:
                        self._on_snapshot_published()
                    except Exception as error:  # quote wake-up is retried periodically
                        logger.warning(
                            "could not request Quote after Structure publication "
                            f"kind={type(error).__name__}"
                        )
                # Plan 02-05 fix-up: Better Stack heartbeat OK pulse.
                # Reference via the module attribute (not from-import) so tests
                # can monkeypatch alerts.send_heartbeat_ok. Fail-soft already
                # encapsulated inside send_heartbeat_ok itself.
                try:
                    from polyarb.daemon import alerts as _alerts

                    await _alerts.send_heartbeat_ok(self._settings)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"send_heartbeat_ok failed: {e!r}")
                retention_defer_reason = self._quote_priority_reason()
                if retention_defer_reason is not None:
                    # Retention uses large SQLite write transactions.  It is
                    # lower priority than the newly woken Quote producer and
                    # must never turn a fresh Structure publication into a
                    # 120-second Quote persist timeout.
                    logger.info(f"snapshot retention deferred reason={retention_defer_reason}")
                else:
                    try:
                        deleted, _ = await asyncio.to_thread(
                            self._sqlite_store.purge_old_snapshots,
                            older_than_days=7,
                            keep_last=5,
                            max_snapshots_per_run=10,
                            parquet_root=self._settings.parquet_root,
                        )
                        if deleted:
                            logger.info(f"snapshot retention deleted {deleted} expired snapshots")
                    except Exception as e:  # noqa: BLE001
                        # Retention is fail-soft relative to a valid fresh snapshot,
                        # but its failure remains visible in production logs.
                        logger.warning(f"snapshot retention failed: {e!r}")
                    try:
                        reclaimed_failed, _ = await asyncio.to_thread(
                            self._sqlite_store.purge_failed_structure_sync_windows,
                            max_windows_per_run=1,
                        )
                        if reclaimed_failed:
                            logger.info(
                                "structure staging retention reclaimed payload for "
                                f"{reclaimed_failed} failed window"
                            )
                    except Exception as e:  # noqa: BLE001
                        logger.warning(f"failed structure staging retention failed: {e!r}")
                    try:
                        reclaimed_windows, _ = await asyncio.to_thread(
                            self._sqlite_store.purge_published_structure_sync_windows,
                            keep_last=1,
                            max_windows_per_run=1,
                        )
                        if reclaimed_windows:
                            logger.info(
                                "structure staging retention reclaimed payload for "
                                f"{reclaimed_windows} expired published window"
                            )
                    except Exception as e:  # noqa: BLE001
                        # The certified snapshot remains valid; the next successful
                        # cycle retries this bounded disk-reclamation transaction.
                        logger.warning(f"structure staging retention failed: {e!r}")
            else:
                snapshot_id = getattr(result, "snapshot_id", None)
                await self._finish_attempt(
                    attempt_id=attempt_id,
                    outcome="failed",
                    snapshot_id=(snapshot_id if isinstance(snapshot_id, int) else None),
                    failure_kind="snapshot-status-failed",
                    last_stage=getattr(result, "last_stage", None),
                    elapsed_ms=getattr(result, "elapsed_ms", None),
                )
                # FAILED status
                self._failure_counter += 1
                if self._on_structure_failure is not None:
                    try:
                        self._on_structure_failure(
                            "snapshot-status-failed",
                            getattr(result, "elapsed_ms", None),
                            getattr(result, "last_stage", None),
                        )
                    except Exception:
                        logger.warning("structure incident recording failed")
                logger.warning(
                    f"snapshot tick FAILED: status={result_status} "
                    f"failure_counter={self._failure_counter}/{self.FAILURE_THRESHOLD}"
                )

        except asyncio.CancelledError:
            # F-04: cancellation must propagate so run() can stop in <1s.
            # Do NOT count as a failure — this is a graceful shutdown signal.
            if attempt_id is not None:
                await self._finish_attempt(
                    attempt_id=attempt_id,
                    outcome="cancelled",
                    snapshot_id=None,
                    failure_kind="scheduler-cancelled",
                )
            logger.info("scheduler tick cancelled mid-flight; propagating CancelledError")
            raise
        except Exception as error:
            if attempt_id is not None:
                await self._finish_attempt(
                    attempt_id=attempt_id,
                    outcome="failed",
                    snapshot_id=None,
                    failure_kind=str(error),
                    last_stage=getattr(error, "last_stage", None),
                    elapsed_ms=getattr(error, "elapsed_ms", None),
                    chunks_processed=getattr(error, "chunks_processed", None),
                    stderr_bytes=getattr(error, "stderr_bytes", None),
                    stderr_sha256=getattr(error, "stderr_sha256", None),
                    stderr_tail=getattr(error, "stderr_tail", None),
                )
            self._failure_counter += 1
            if self._on_structure_failure is not None:
                try:
                    self._on_structure_failure(
                        str(error),
                        getattr(error, "elapsed_ms", None),
                        getattr(error, "last_stage", None),
                    )
                except Exception:
                    logger.warning("structure incident recording failed")
            logger.exception(
                f"snapshot tick raised exception "
                f"failure_counter={self._failure_counter}/{self.FAILURE_THRESHOLD}"
            )

        # Persist counter before recovery-state check
        self._persist_counter()
        self._restore_effective_schedule()

        # Transition once; later failed recovery ticks keep trying and do not
        # repeatedly emit the first-incident alert.
        if (
            self._failure_counter >= self.FAILURE_THRESHOLD
            and self.state != SchedulerState.RECOVERING
        ):
            self.state = SchedulerState.RECOVERING
            self._persist_counter()
            await self._on_recovering()
        return True

    def unpause(self) -> None:
        """Manually unpause the scheduler (called via /scan or SSH).

        Resets failure counter. Plan 04 exposes this via daemon management endpoint.
        """
        self.state = SchedulerState.RUNNING
        self._failure_counter = 0
        self._persist_counter()
        logger.info("scheduler unpaused manually, failure_counter reset to 0")

    def request_now(self) -> bool:
        """Wake the single scheduler loop for one normal tick, if available.

        This never calls ``_run_snapshot`` directly.  A paused scheduler is a
        safety boundary, and a set event records one pending request while an
        existing child owns the producer.
        """
        if self.state == SchedulerState.PAUSED or self._request_now_event.is_set():
            return False
        self._request_now_event.set()
        return True

    async def _wait_for_next_tick(self, stop_event: asyncio.Event, delay_s: float) -> bool:
        """Wait for stop, cadence, or one coalesced requested normal cycle."""
        if self._request_now_event.is_set():
            self._request_now_event.clear()
            return False
        stop_task = asyncio.create_task(stop_event.wait())
        request_task = asyncio.create_task(self._request_now_event.wait())
        try:
            done, pending = await asyncio.wait(
                (stop_task, request_task),
                timeout=delay_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if stop_task in done and stop_task.result():
                return True
            if request_task in done and request_task.result():
                self._request_now_event.clear()
            return False
        except asyncio.CancelledError:
            stop_task.cancel()
            request_task.cancel()
            await asyncio.gather(stop_task, request_task, return_exceptions=True)
            raise

    async def run(self, stop_event: asyncio.Event) -> None:
        """Long-running scheduler loop (Plan 02 placeholder).

        Plan 02: sleeps between ticks using settings interval (default 1 hour).
        Plan 04: real prod uses Fly scheduled machines (not this loop).

        F-04 (Plan 02-08): inner sleep granularity dropped from 10s → 1s so
        a SIGINT-triggered stop_event.set() is observed within 1s, satisfying
        the Wave 5 chaos test "< 1s graceful shutdown" gate. Also use
        asyncio.wait_for on stop_event so cancellation interrupts the wait
        immediately rather than after the next 1s tick.
        """
        logger.info(
            "scheduler loop started, "
            f"tick interval={self._effective_cadence_s}s "
            f"snapshot timeout={self._effective_timeout_s}s"
        )

        try:
            # Delay first tick 10s so uvicorn fully starts and Fly's health
            # check sees a live /health before the first Gamma fetch ties up
            # the event loop for 30-120s. Use wait_for(stop_event) so SIGINT
            # during startup delay is still <1s responsive.
            if await self._wait_for_next_tick(stop_event, 10):
                # stop_event was set during delay — exit immediately
                logger.info("scheduler: stop_event during startup delay, exiting")
                return

            while not stop_event.is_set():
                await self._tick()
                if await self._wait_for_next_tick(
                    stop_event,
                    (
                        (2.0 if self._producer_arbitrator is not None else 0.1)
                        if self._checkpoint_pending
                        else recovery_retry_delay_s(self._failure_counter)
                        if self._failure_counter > 0
                        else self._effective_cadence_s
                    ),
                ):
                    break
        except asyncio.CancelledError:
            # F-04: graceful cancellation path. main.py may cancel this task
            # explicitly to interrupt an in-flight tick.
            logger.info("scheduler loop received CancelledError, exiting")
            raise

        logger.info("scheduler loop stopped")
