"""Shell-free subprocess isolation and bounded restart policy."""

from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import uuid
from dataclasses import dataclass

from polyarb.perception.incidents import (
    Incident,
    IncidentManager,
    RecoveryEvidenceRequiredError,
)
from polyarb.perception.store import OpportunityPerceptionStore, ProducerReceipt

PRODUCER_COMMANDS = {
    "candidate": (
        sys.executable,
        "-m",
        "polyarb.perception.worker_cli",
        "candidate",
    ),
    "discovery": (
        sys.executable,
        "-m",
        "polyarb.perception.worker_cli",
        "discovery",
    ),
    "reconciliation": (
        sys.executable,
        "-m",
        "polyarb.perception.worker_cli",
        "reconciliation",
    ),
}

_AUTH_RE = re.compile(r"(?i)(authorization\s*:\s*(?:bearer|basic)\s+)[^\s\\]+")
_COOKIE_RE = re.compile(r"(?i)(cookie\s*:)[^\r\n\\]*")
_JSON_SECRET_RE = re.compile(
    r'(?i)("(?:token|secret|password|api[_-]?key|authorization|cookie)"\s*:\s*")'
    r'[^"]*(")'
)
_KEY_VALUE_RE = re.compile(
    r"(?i)((?:^|[?&;\s])(?:token|secret|password|api[_-]?key|authorization|cookie)"
    r"\s*=\s*)[^&;\s\\]+"
)
_URI_USERINFO_RE = re.compile(r"(?i)([a-z][a-z0-9+.-]*://)[^/@\s]+@")


@dataclass(frozen=True)
class ProducerSpec:
    component: str
    timeout_s: float
    terminate_grace_s: float = 1.0
    max_restarts: int = 3
    backoff_initial_s: float = 1.0
    backoff_max_s: float = 30.0
    output_limit_bytes: int = 16_384

    def __post_init__(self) -> None:
        if (
            self.component not in PRODUCER_COMMANDS
            or self.timeout_s <= 0
            or self.terminate_grace_s <= 0
            or self.max_restarts < 0
            or self.backoff_initial_s < 0
            or self.backoff_max_s < self.backoff_initial_s
            or not 1 <= self.output_limit_bytes <= 16_384
        ):
            raise ValueError("invalid-producer-spec")


class ProducerSupervisor:
    def __init__(
        self,
        *,
        store: OpportunityPerceptionStore,
        incidents: IncidentManager,
        clock_ms=None,
        _test_commands: dict[str, tuple[str, ...]] | None = None,
    ) -> None:
        self._store = store
        self._incidents = incidents
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1_000))
        self._commands = PRODUCER_COMMANDS if _test_commands is None else _test_commands

    async def run(self, spec: ProducerSpec, stop_event: asyncio.Event) -> None:
        attempt = len(self._store.producer_receipts(spec.component))
        supervisor_run_id = uuid.uuid4().hex
        incident: Incident | None = None
        retries = 0
        while not stop_event.is_set():
            attempt += 1
            started_at_ms = self._clock_ms()
            process = None
            stdout_task = None
            stderr_task = None
            outcome = "spawn-error"
            exit_code = None
            child_nonce = uuid.uuid4().hex
            try:
                child_env = os.environ.copy()
                child_env["POLYARB_PRODUCER_SUPERVISOR_RUN_ID"] = supervisor_run_id
                child_env["POLYARB_PRODUCER_CHILD_NONCE"] = child_nonce
                process = await asyncio.create_subprocess_exec(
                    *self._commands[spec.component],
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=child_env,
                )
                self._store.record_producer_child_start(
                    spec.component,
                    supervisor_run_id=supervisor_run_id,
                    child_nonce=child_nonce,
                    attempt=attempt,
                    started_at_ms=started_at_ms,
                )
                assert process.stdout is not None and process.stderr is not None
                stdout_task = asyncio.create_task(
                    self._drain(process.stdout, spec.output_limit_bytes)
                )
                stderr_task = asyncio.create_task(
                    self._drain(process.stderr, spec.output_limit_bytes)
                )
                exit_code, signal = await self._wait(
                    process,
                    stop_event,
                    spec.timeout_s,
                    spec.component,
                    incident,
                    supervisor_run_id,
                    child_nonce,
                )
                if signal == "stop":
                    outcome = "cancelled"
                    await self._terminate(process, spec.terminate_grace_s)
                elif signal == "timeout":
                    outcome = "timeout"
                    await self._terminate(process, spec.terminate_grace_s)
                else:
                    outcome = "success" if exit_code == 0 else "nonzero"
            except asyncio.CancelledError:
                outcome = "cancelled"
                if process is not None:
                    await asyncio.shield(self._terminate(process, spec.terminate_grace_s))
                raise
            except OSError:
                outcome = "spawn-error"
            finally:
                stdout = await self._drain_result(stdout_task)
                stderr = await self._drain_result(stderr_task)
                self._store.record_producer_receipt(
                    ProducerReceipt(
                        component=spec.component,
                        attempt=attempt,
                        started_at_ms=started_at_ms,
                        finished_at_ms=max(started_at_ms, self._clock_ms()),
                        outcome=outcome,
                        exit_code=exit_code,
                        stdout_tail=self._safe_text(stdout),
                        stderr_tail=self._safe_text(stderr),
                        supervisor_run_id=supervisor_run_id,
                        child_nonce=child_nonce,
                    )
                )

            if outcome == "cancelled":
                return

            incident = self._record_failure(
                incident,
                component=spec.component,
                outcome=("unexpected-exit" if outcome == "success" else outcome),
                attempt=attempt,
            )
            if retries >= spec.max_restarts:
                self._escalate(incident, retries=retries)
                return
            retries += 1
            self._begin_recovery(incident, retries=retries)
            delay = min(
                spec.backoff_max_s,
                spec.backoff_initial_s * (2 ** (retries - 1)),
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
                return
            except TimeoutError:
                pass

    async def _wait(
        self,
        process,
        stop_event,
        timeout_s,
        component,
        incident,
        supervisor_run_id,
        child_nonce,
    ):
        wait_task = asyncio.create_task(process.wait())
        stop_task = asyncio.create_task(stop_event.wait())
        marker = self._progress_marker(component, supervisor_run_id, child_nonce)
        deadline = time.monotonic() + timeout_s
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None, "timeout"
                done, _ = await asyncio.wait(
                    {wait_task, stop_task},
                    timeout=min(1.0, remaining),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if wait_task in done:
                    return wait_task.result(), "exit"
                if stop_task in done and stop_task.result():
                    return None, "stop"
                current = self._progress_marker(component, supervisor_run_id, child_nonce)
                if current != marker:
                    marker = current
                    deadline = time.monotonic() + timeout_s
                    if incident is not None:
                        self._attempt_verify(incident)
        finally:
            for task in (wait_task, stop_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(wait_task, stop_task, return_exceptions=True)

    def _progress_marker(
        self,
        component: str,
        supervisor_run_id: str,
        child_nonce: str,
    ) -> tuple:
        try:
            con = sqlite3.connect(
                f"file:{self._store.db_path}?mode=ro",
                uri=True,
                timeout=0.25,
            )
            try:
                row = con.execute(
                    "SELECT COUNT(*),COALESCE(MAX(sequence),0),"
                    "COALESCE(MAX(observed_at_ms),0) "
                    "FROM neg_risk_producer_heartbeats "
                    "WHERE component=? AND supervisor_run_id=? AND child_nonce=?",
                    (component, supervisor_run_id, child_nonce),
                ).fetchone()
                if row[0] != row[1]:
                    return ("invalid-sequence",)
                return tuple(row)
            finally:
                con.close()
        except sqlite3.Error:
            return ("unavailable",)

    @staticmethod
    async def _terminate(process, grace_s: float) -> None:
        if process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=grace_s)
        except TimeoutError:
            process.kill()
            await process.wait()

    @staticmethod
    async def _drain(reader: asyncio.StreamReader, limit: int) -> bytes:
        tail = bytearray()
        while chunk := await reader.read(4_096):
            tail.extend(chunk)
            if len(tail) > limit:
                del tail[:-limit]
        return bytes(tail)

    @staticmethod
    async def _drain_result(task: asyncio.Task[bytes] | None) -> bytes:
        if task is None:
            return b""
        try:
            return await asyncio.shield(task)
        except Exception:
            return b"[output-read-error]"

    @staticmethod
    def _safe_text(value: bytes) -> str:
        text = value.decode("utf-8", "replace")
        # Decode percent-encoding before matching so `token%3Dsecret` cannot
        # bypass the same policy. A second pass covers encoded percent signs.
        for _ in range(2):
            text = urllib.parse.unquote(text)
        text = _URI_USERINFO_RE.sub(r"\1[REDACTED]@", text)
        text = _AUTH_RE.sub(r"\1[REDACTED]", text)
        text = _COOKIE_RE.sub(r"\1 [REDACTED]", text)
        text = _JSON_SECRET_RE.sub(r"\1[REDACTED]\2", text)
        redacted = _KEY_VALUE_RE.sub(r"\1[REDACTED]", text)
        return redacted.encode("utf-8")[:16_384].decode("utf-8", "ignore")

    def _record_failure(
        self,
        incident: Incident | None,
        *,
        component: str,
        outcome: str,
        attempt: int,
    ) -> Incident:
        if incident is not None and all(
            item.id != incident.id for item in self._incidents.open_incidents()
        ):
            incident = None
        if incident is None:
            incident = self._incidents.detect(component, f"child-{outcome}", {"attempt": attempt})
            incident = self._incidents.transition(
                incident.id, "classified", {"class": "producer-process"}
            )
            return self._incidents.transition(incident.id, "contained", {"attempt": attempt})
        if incident.state == "recovering":
            return self._incidents.transition(
                incident.id, "contained", {"attempt": attempt, "outcome": outcome}
            )
        return incident

    def _begin_recovery(self, incident: Incident, *, retries: int) -> None:
        self._incidents.transition(
            incident.id,
            "recovering",
            self._recovery_anchor(incident.scope, retries),
        )

    def _escalate(self, incident: Incident, *, retries: int) -> None:
        current = self._incidents.open_incidents()
        latest = next(item for item in current if item.id == incident.id)
        if latest.state in {"classified", "contained", "recovering"}:
            self._incidents.transition(latest.id, "escalated", {"retry_limit": retries})

    def _attempt_verify(self, incident: Incident) -> None:
        latest = next(item for item in self._incidents.open_incidents() if item.id == incident.id)
        if latest.state != "recovering":
            return
        try:
            pointer = self._verification_pointer(latest.scope)
            self._incidents.transition(latest.id, "verified", pointer)
        except (RecoveryEvidenceRequiredError, ValueError):
            return

    def _recovery_anchor(self, scope: str, retries: int) -> dict:
        if scope == "reconciliation":
            try:
                window = self._store.current_reconciliation()
                pages = window.pages_completed if window else 0
            except (TypeError, ValueError):
                pages = 0
            return {"retry": retries, "pages_completed": pages}
        return {"retry": retries}

    def _verification_pointer(self, scope: str) -> dict:
        con = sqlite3.connect(self._store.db_path, timeout=0.25)
        con.row_factory = sqlite3.Row
        try:
            if scope == "candidate":
                row = con.execute(
                    "SELECT q.id,q.group_id,q.membership_hash "
                    "FROM neg_risk_group_quote_batches q "
                    "WHERE q.status='complete' ORDER BY q.quoted_at_ms DESC,q.id "
                    "DESC LIMIT 1"
                ).fetchone()
                return (
                    {}
                    if row is None
                    else {
                        "quote_batch_id": row["id"],
                        "group_id": row["group_id"],
                        "membership_hash": row["membership_hash"],
                    }
                )
            if scope == "discovery":
                row = con.execute(
                    "SELECT id FROM neg_risk_discovery_batches ORDER BY id DESC LIMIT 1"
                ).fetchone()
                return {} if row is None else {"batch_id": row["id"]}
            if scope == "reconciliation":
                row = con.execute(
                    "SELECT id FROM neg_risk_reconciliation_windows "
                    "ORDER BY started_at_ms DESC,id DESC LIMIT 1"
                ).fetchone()
                return {} if row is None else {"window_id": row["id"]}
            return {}
        finally:
            con.close()


__all__ = [
    "PRODUCER_COMMANDS",
    "ProducerSpec",
    "ProducerSupervisor",
]
