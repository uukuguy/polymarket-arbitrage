"""Bounded, read-only HTTP projections for durable M1 perception facts."""

# ruff: noqa: E501

from __future__ import annotations

import asyncio
import base64
import binascii
import contextvars
import json
import math
import re
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse

from polyarb.daemon.producer_arbitration import ProducerArbitrator
from polyarb.http.opportunity_read_health import ReadLaneSaturatedError
from polyarb.perception.models import GroupLeg, GroupRevision
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.routing.neg_risk_quote_store import NegRiskQuoteStore
from polyarb.storage.sqlite_store import SQLiteStore

_MAX_LIMIT = 500
_HISTORY_CAP = 500
_LEGS_JSON_MAX_BYTES = 65_536
_TIMEOUT_S = 1.0
_BUSY_TIMEOUT_MS = 250
_READ_SQL_DEADLINE_S = 0.8
_INCIDENT_READ_TIMEOUT_S = 3.0
_INCIDENT_READ_SQL_DEADLINE_S = 2.5
_INCIDENT_EDGES = {
    "detected": {"classified"},
    "classified": {"contained", "escalated"},
    "contained": {"recovering", "escalated"},
    "recovering": {"verified", "contained", "escalated"},
    "verified": set(),
    "escalated": {"recovering"},
}
_SECRET_KEYS = {
    "secret",
    "password",
    "passwd",
    "token",
    "authorization",
    "auth",
    "api_key",
    "apikey",
    "cookie",
    "session",
    "credential",
    "dsn",
    "traceback",
    "path",
}
_INLINE_SECRET_RE = re.compile(
    r"(?i)(?:password|passwd|secret|api[_-]?key|token|authorization|cookie|session)"
    r"\s*[:=]\s*[^\s,;&]+|(?:bearer|basic)\s+[a-z0-9._~+/=-]+"
)
_READ_EXECUTION: contextvars.ContextVar[_ReadExecution | None] = contextvars.ContextVar(
    "perception_read_execution",
    default=None,
)


class _ReadExecution:
    """One absolute request deadline plus every SQLite handle it owns."""

    def __init__(self, deadline_monotonic: float) -> None:
        self.deadline_monotonic = deadline_monotonic
        self._lock = threading.Lock()
        self._connections: list[sqlite3.Connection] = []

    def register(self, con: sqlite3.Connection) -> None:
        with self._lock:
            self._connections.append(con)

    def interrupt(self) -> None:
        with self._lock:
            connections = tuple(self._connections)
        for con in connections:
            try:
                con.interrupt()
            except sqlite3.Error:
                pass

    def check(self) -> None:
        if time.monotonic() >= self.deadline_monotonic:
            raise TimeoutError("perception-read-deadline")


class _IncidentNotFoundError(RuntimeError):
    pass


async def producer_arbitration_status(request: Request) -> JSONResponse:
    """Expose the cross-process producer handoff with an operator action."""
    try:
        arbitrator = ProducerArbitrator(request.app.state.settings.db_path)
        current, receipts = await asyncio.to_thread(
            lambda: (arbitrator.current(), arbitrator.receipts(limit=10))
        )
    except (OSError, sqlite3.Error, ValueError) as error:
        return JSONResponse(
            {
                "status": "unavailable",
                "reason": type(error).__name__,
                "operator_action": "Inspect /healthz and Fly volume/SQLite availability; retry this view.",
            },
            status_code=503,
        )
    now_ms = int(time.time() * 1_000)
    lease = None if current is None else asdict(current)
    if current is None:
        action = (
            "No producer owns the slot; the next scheduled Quote or Structure cycle may acquire it."
        )
    elif current.expires_at_ms <= now_ms:
        action = "Lease is expired and will be atomically reclaimed by the next producer; inspect recent receipts if it does not clear."
    elif current.owner == "structure":
        action = (
            "Structure has a bounded 45-second window; Quote retries automatically after release."
        )
    else:
        action = "Quote owns its bounded collection window; Structure records a defer and retries on its next tick."
    return JSONResponse(
        {
            "status": "available",
            "now_ms": now_ms,
            "current_lease": lease,
            "recent_receipts": [asdict(item) for item in receipts],
            "automatic_action": "SQLite BEGIN IMMEDIATE enforces one owner; expiry reclaims a crashed owner without manual intervention.",
            "operator_action": action,
        }
    )


def _producer_progress(db_path: Path, quote_worker_runtime: Any | None = None) -> dict[str, Any]:
    """Return only the newest durable producer checkpoints for the console."""
    quote_attempt = NegRiskQuoteStore(db_path).latest_collection_attempt()
    structure_attempt = SQLiteStore(db_path).get_latest_snapshot_attempt()
    snapshot = (
        quote_worker_runtime.snapshot()
        if quote_worker_runtime is not None
        and callable(getattr(quote_worker_runtime, "snapshot", None))
        else None
    )
    hydration = {
        "consecutive_failures": (
            0 if snapshot is None else snapshot.hydration_consecutive_failures
        ),
        "last_error_kind": (
            None if snapshot is None else snapshot.hydration_last_error_kind
        ),
        "last_attempt_at_s": (
            None if snapshot is None else snapshot.hydration_last_attempt_at_s
        ),
    }
    return {
        "status": "available",
        "quote": {"attempt": quote_attempt, "hydration": hydration},
        "structure": {"attempt": structure_attempt},
        "automatic_action": (
            "Each producer persists a bounded checkpoint before its next expensive stage; "
            "a timeout is terminalized and the next eligible cycle retries automatically."
        ),
        "operator_action": (
            "Compare checkpoint phase and elapsed timing with the configured child budget; "
            "a checkpoint that stops advancing identifies the stalled stage."
        ),
    }


async def producer_progress(request: Request) -> JSONResponse:
    """Expose current Quote and Structure stage evidence without SSH access."""
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(
        request,
        lambda: _producer_progress(
            db_path,
            getattr(request.app.state, "quote_worker_runtime", None),
        ),
        lane_name="incident_read_lane",
        timeout_s=_INCIDENT_READ_TIMEOUT_S,
        sql_deadline_s=_INCIDENT_READ_SQL_DEADLINE_S,
    )


def perception_console(_request: Request) -> HTMLResponse:
    """Serve a Fly-native, credential-free view of the public incident model."""
    return HTMLResponse(
        """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>M1 incident console</title>
<style>
body{background:#0b0d10;color:#e6edf3;font:15px system-ui,sans-serif;margin:0}main{max-width:1100px;margin:auto;padding:24px}
h1{margin:0 0 6px} .muted{color:#9aa4b2}.row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}.card{border:1px solid #30363d;border-radius:8px;padding:16px;margin:12px 0;background:#11161c}.p1{border-color:#f85149}.p2{border-color:#d29922}button,a{background:#21262d;color:#58a6ff;border:1px solid #30363d;border-radius:6px;padding:7px 10px;text-decoration:none;cursor:pointer}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#090c10;padding:10px;border-radius:5px}.label{color:#9aa4b2}.error{color:#ff7b72}
</style></head><body><main>
<div class="row"><div><h1>M1 incident console</h1><div class="muted">Fly-native, read-only operator view. It never treats an unavailable read as no incident.</div></div><button id="refresh">Refresh now</button><a href="/healthz">Health JSON</a><a href="/perception/incidents?limit=100">Incident JSON</a><a href="https://polyarb-l2.fly.dev/console">L2 operations console</a></div>
<p id="status" class="muted">Loading durable incident evidence…</p><p class="muted">`read-model-unavailable` or `read-model-saturated` is itself an operator-visibility failure, never an all-clear.</p><section><h2>Producer handoff</h2><p class="muted">The live cross-process ownership proof for Quote and Structure. A stale owner is automatically reclaimed.</p><div id="handoff"></div></section><section><h2>Current producer checkpoints</h2><p class="muted">The newest durable stage receipt reveals where an active or failed producer stopped; it is not inferred from process logs.</p><div id="progress"></div></section><section id="incidents"></section><section><h2>Recent recovered severe incidents</h2><p class="muted">Last 24 hours. Recovery remains inspectable; it is not erased from the operator record.</p><div id="recent"></div></section>
<script>
const endpoint="/perception/incidents?limit=100";
const recentQuoteEndpoint="/perception/incidents/recent?scope=quote-collection";
const recentQuoteSupervisorEndpoint="/perception/incidents/recent?scope=quote";
const recentCapacityEndpoint="/perception/incidents/recent?scope=capacity";
const handoffEndpoint="/perception/producer-arbitration";
const progressEndpoint="/perception/producer-progress";
const status=document.getElementById("status"), root=document.getElementById("incidents"), recent=document.getElementById("recent"), handoff=document.getElementById("handoff"), progress=document.getElementById("progress");
function field(card,label,value){const row=document.createElement("div");const key=document.createElement("strong");key.textContent=label+": ";row.append(key,document.createTextNode(value ?? "not recorded"));card.append(row)}
function renderHandoff(body){handoff.replaceChildren();const card=document.createElement("article");card.className="card "+(body.status==="available"?"":"p1");const lease=body.current_lease||{};const title=document.createElement("h3");title.textContent=body.status==="available"?`Current owner: ${lease.owner||"none"}`:"Producer handoff read unavailable";card.append(title);field(card,"Lease expiry",lease.expires_at_ms?new Date(lease.expires_at_ms).toISOString():null);field(card,"Automatic action",body.automatic_action);field(card,"Operator action",body.operator_action);const detail=document.createElement("pre");detail.textContent="Durable handoff evidence\n"+JSON.stringify(body.recent_receipts||[],null,2);card.append(detail);handoff.append(card)}
function renderProgress(body){progress.replaceChildren();const card=document.createElement("article");card.className="card "+(body.status==="available"?"":"p1");const title=document.createElement("h3");title.textContent=body.status==="available"?"Latest durable producer stages":"Producer checkpoint read unavailable";card.append(title);field(card,"Automatic action",body.automatic_action);field(card,"Operator action",body.operator_action);const detail=document.createElement("pre");detail.textContent="Quote checkpoint\n"+JSON.stringify(body.quote?.attempt??null,null,2)+"\n\nQuote feed hydration\n"+JSON.stringify(body.quote?.hydration??null,null,2)+"\n\nStructure checkpoint\n"+JSON.stringify(body.structure?.attempt??null,null,2);card.append(detail);progress.append(card)}
function render(body){root.replaceChildren();const items=Array.isArray(body.items)?body.items:[];status.textContent=`${body.open_count ?? "unknown"} open incident(s) · refreshed ${new Date().toISOString()}`;if(!items.length){const empty=document.createElement("p");empty.className="muted";empty.textContent="No open incident rows were returned. This is not proof of health when the read model is unavailable.";root.append(empty);return}for(const incident of items){const diagnosis=incident.diagnosis||{};const evidence=incident.recovery_start_evidence||incident.evidence||{};const card=document.createElement("article");card.className="card "+(diagnosis.severity||"");const title=document.createElement("h2");title.textContent=`${(diagnosis.severity||"incident").toUpperCase()} · ${incident.kind} · ${incident.state}`;card.append(title);field(card,"Scope",incident.scope);field(card,"Impact",diagnosis.impact);field(card,"Automatic action",diagnosis.automatic_action);field(card,"Next operator action",diagnosis.next_action);field(card,"Failure reason",diagnosis.failure_reason);field(card,"Failed attempt",evidence.attempt_id);field(card,"Retry count",incident.retry_count);field(card,"Next automatic retry",incident.next_retry_at_ms?new Date(incident.next_retry_at_ms).toISOString():null);field(card,"Lifecycle age",incident.lifecycle_age_ms==null?null:`${Math.round(incident.lifecycle_age_ms/1000)}s`);const detail=document.createElement("pre");detail.textContent="Recovery / current evidence\n"+JSON.stringify(evidence,null,2);card.append(detail);root.append(card)}}
function renderRecent(histories){recent.replaceChildren();if(!histories.length){const empty=document.createElement("p");empty.className="muted";empty.textContent="No recovered Quote or capacity incidents in this 24-hour window.";recent.append(empty);return}for(const history of histories){const events=Array.isArray(history.items)?history.items:[];const first=events[0]||{}, last=events.at(-1)||{}, evidence=first.evidence||{}, recovered=last.evidence||{};const card=document.createElement("article");card.className="card "+(evidence.severity||"");const title=document.createElement("h3");title.textContent=`${(evidence.severity||"incident").toUpperCase()} · ${history.kind} · recovered as ${last.state||"unknown"}`;card.append(title);field(card,"Scope",history.scope);field(card,"Automatic action",evidence.automatic_action);field(card,"Next operator action",evidence.next_action);field(card,"Failure reason",evidence.failure_reason);field(card,"Failed attempt",evidence.attempt_id);field(card,"Recovery time",last.occurred_at_ms?new Date(last.occurred_at_ms).toISOString():null);const link=document.createElement("a");link.href=`/perception/incidents/${history.incident_id}/history`;link.textContent="Open complete lifecycle JSON";card.append(link);const proof=document.createElement("pre");proof.textContent="Recovery evidence\n"+JSON.stringify(recovered,null,2);card.append(proof);recent.append(card)}}
async function loadRecent(){const after=Date.now()-24*60*60*1000;const responses=await Promise.all([fetch(`${recentQuoteEndpoint}&after_ms=${after}&limit=5`,{cache:"no-store"}),fetch(`${recentQuoteSupervisorEndpoint}&after_ms=${after}&limit=5`,{cache:"no-store"}),fetch(`${recentCapacityEndpoint}&after_ms=${after}&limit=5`,{cache:"no-store"})]);const bodies=await Promise.all(responses.map(async response=>{const body=await response.json();if(!response.ok||body.status!=="available")throw new Error(body.reason||`HTTP ${response.status}`);return body}));const ids=[...new Set(bodies.flatMap(body=>Array.isArray(body.items)?body.items.map(item=>item.incident_id):[]))].slice(0,10);const histories=await Promise.all(ids.map(async id=>{const response=await fetch(`/perception/incidents/${id}/history`,{cache:"no-store"});const body=await response.json();if(!response.ok||body.status!=="available")throw new Error(body.reason||`HTTP ${response.status}`);return body}));renderRecent(histories)}
async function refresh(){status.className="muted";status.textContent="Loading durable incident evidence…";try{const [response,handoffResponse,progressResponse]=await Promise.all([fetch(endpoint,{cache:"no-store"}),fetch(handoffEndpoint,{cache:"no-store"}),fetch(progressEndpoint,{cache:"no-store"})]);const body=await response.json(),handoffBody=await handoffResponse.json(),progressBody=await progressResponse.json();if(!response.ok||body.status!=="available")throw new Error(body.reason||`HTTP ${response.status}`);render(body);renderHandoff(handoffBody);renderProgress(progressBody);await loadRecent()}catch(error){root.replaceChildren();recent.replaceChildren();handoff.replaceChildren();progress.replaceChildren();status.className="error";status.textContent=`Incident read unavailable: ${error.message}. This is a production visibility fault, not zero incidents. Check /healthz and retry.`}}
document.getElementById("refresh").addEventListener("click",refresh);refresh();setInterval(refresh,30000);
</script></main></body></html>"""
    )


def _check_read_deadline() -> None:
    execution = _READ_EXECUTION.get()
    if execution is not None:
        execution.check()


def _read_store(db_path: Path) -> OpportunityPerceptionStore:
    execution = _READ_EXECUTION.get()
    return OpportunityPerceptionStore(
        db_path,
        read_only=True,
        busy_timeout_ms=_BUSY_TIMEOUT_MS,
        deadline_monotonic=(None if execution is None else execution.deadline_monotonic),
    )


def _validate_recovery_batch(
    con: sqlite3.Connection,
    db_path: Path,
    proofs: list[tuple[str, int, int, dict[str, Any], dict[str, Any]]],
) -> None:
    """Validate all verified incidents with fixed-count bulk reads."""
    if not proofs:
        return
    store = _read_store(db_path)
    scopes = {scope for scope, *_ in proofs}
    candidate_receipts: dict[str, sqlite3.Row] = {}
    if any(scope == "candidate" or scope.startswith("candidate:") for scope in scopes):
        store.validated_candidate_opportunity_count(_connection=con)
        candidate_receipts = {
            str(row["quote_batch_id"]): row
            for row in con.execute("SELECT * FROM neg_risk_candidate_success_receipts").fetchall()
        }
    discovery_batches: dict[int, sqlite3.Row] = {}
    latest_discovery_id: int | None = None
    if "discovery" in scopes:
        discovery_rows = con.execute(
            "SELECT * FROM neg_risk_discovery_batches ORDER BY id"
        ).fetchall()
        discovery_batches = {int(row["id"]): row for row in discovery_rows}
        latest_discovery_id = None if not discovery_rows else int(discovery_rows[-1]["id"])
        if discovery_rows:
            store.discovery_status(
                max(int(row["finished_at_ms"]) for row in discovery_rows),
                _connection=con,
            )
    windows: dict[str, sqlite3.Row] = {}
    current_window = None
    if "reconciliation" in scopes:
        window_rows = con.execute(
            "SELECT * FROM neg_risk_reconciliation_windows ORDER BY rowid"
        ).fetchall()
        windows = {str(row["id"]): row for row in window_rows}
        current_window = store.current_reconciliation(_connection=con)
    probes: dict[tuple[str, str], sqlite3.Row] = {}
    if "http" in scopes:
        for row in con.execute(
            "SELECT rowid probe_row_id,* FROM neg_risk_http_probe_receipts ORDER BY id"
        ).fetchall():
            probes[(str(row["release_id"]), str(row["probe_nonce"]))] = row
    resource_rows: dict[int, sqlite3.Row] = {}
    resource_decision = None
    if "resource" in scopes:
        from polyarb.perception.resource_controller import validate_resource_history

        resource_decision = validate_resource_history(con)
        resource_rows = {
            int(row["id"]): row
            for row in con.execute("SELECT * FROM neg_risk_resource_decisions").fetchall()
        }
    for scope, recovery_at, verified_at, recovery, verification in proofs:
        _check_read_deadline()
        valid = False
        if scope == "candidate" or scope.startswith("candidate:"):
            group_id = verification.get("group_id")
            receipt = candidate_receipts.get(str(verification.get("quote_batch_id")))
            anchor = recovery.get("candidate_success_receipt_row_id")
            valid = bool(
                isinstance(group_id, str)
                and group_id
                and (not scope.startswith("candidate:") or group_id == scope.split(":", 1)[1])
                and type(anchor) is int
                and receipt is not None
                and int(receipt["id"]) > anchor
                and receipt["group_id"] == group_id
                and receipt["membership_hash"] == verification.get("membership_hash")
                and receipt["quote_batch_id"] == verification.get("quote_batch_id")
                and recovery_at <= int(receipt["observed_at_ms"]) <= verified_at
            )
        elif scope == "discovery":
            batch_id = verification.get("batch_id")
            row = discovery_batches.get(batch_id) if type(batch_id) is int else None
            valid = bool(
                row is not None
                and int(row["id"]) == latest_discovery_id
                and recovery_at < int(row["finished_at_ms"]) <= verified_at
                and (row["completed"] or row["next_cursor"] != row["requested_cursor"])
            )
        elif scope == "reconciliation":
            row = windows.get(str(verification.get("window_id")))
            valid = bool(
                row is not None
                and current_window is not None
                and current_window.id == row["id"]
                and recovery_at < int(row["checkpoint_at_ms"]) <= verified_at
                and int(row["pages_completed"]) > int(recovery.get("pages_completed", -1))
            )
        elif scope == "http":
            release = recovery.get("release_id")
            nonce = recovery.get("probe_nonce")
            anchor = recovery.get("http_probe_row_id")
            row = probes.get((str(release), str(nonce)))
            valid = bool(
                isinstance(release, str)
                and isinstance(nonce, str)
                and type(anchor) is int
                and row is not None
                and row["probe_row_id"] > anchor
                and row["responsive"]
                and row["observed_release_id"] == release
                and verification.get("release_id") == release
                and verification.get("probe_nonce") == nonce
                and recovery_at <= row["started_at_ms"]
                and row["finished_at_ms"] <= verified_at
                and row["finished_at_ms"] - row["started_at_ms"] <= 2_000
            )
        elif scope == "resource":
            row_id = verification.get("decision_id")
            row = resource_rows.get(row_id) if type(row_id) is int else None
            valid = bool(
                row is not None
                and resource_decision is not None
                and resource_decision.sequence == row["sequence"]
                and resource_decision.decided_at_ms == row["decided_at_ms"]
                and recovery_at < row["decided_at_ms"] <= verified_at
            )
        if not valid:
            raise ValueError("invalid-incident-recovery-proof")


def _safe_evidence(value: Any, *, depth: int = 0) -> Any:
    if depth > 5:
        return "[redacted]"
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in list(value.items())[:100]:
            rendered_key = str(key)[:128]
            normalized = rendered_key.lower().replace("-", "_")
            result[rendered_key] = (
                "[redacted]"
                if any(marker in normalized for marker in _SECRET_KEYS)
                else _safe_evidence(item, depth=depth + 1)
            )
        return result
    if isinstance(value, list):
        return [_safe_evidence(item, depth=depth + 1) for item in value[:100]]
    if isinstance(value, str):
        if (
            "://" in value
            or _INLINE_SECRET_RE.search(value)
            or value.lower().startswith(("bearer ", "basic ", "sha256:", "/users/", "/home/"))
        ):
            return "[redacted]"
        return value[:1_024]
    if value is None or isinstance(value, (int, float, bool)):
        return value
    return "[redacted]"


def _connect(db_path: Path) -> sqlite3.Connection:
    execution = _READ_EXECUTION.get()
    con = sqlite3.connect(
        f"file:{db_path.resolve()}?mode=ro",
        uri=True,
        isolation_level=None,
        timeout=_BUSY_TIMEOUT_MS / 1_000,
    )
    con.row_factory = sqlite3.Row
    con.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    con.execute("PRAGMA query_only=ON")
    if execution is not None:
        execution.register(con)
        con.set_progress_handler(
            lambda: 1 if time.monotonic() >= execution.deadline_monotonic else 0,
            1_000,
        )
    return con


def _limit(request: Request, default: int = 100) -> int:
    raw = request.query_params.get("limit")
    if raw is None:
        return default
    try:
        value = int(raw, 10)
    except (TypeError, ValueError):
        raise ValueError("limit-must-be-an-integer-from-1-to-500") from None
    if str(value) != raw or not 1 <= value <= _MAX_LIMIT:
        raise ValueError("limit-must-be-an-integer-from-1-to-500")
    return value


def _validate_revision(row: sqlite3.Row) -> dict[str, Any]:
    try:
        raw_legs = json.loads(row["legs_json"])
        if not isinstance(raw_legs, list) or len(raw_legs) < 2:
            raise ValueError
        legs = tuple(
            GroupLeg(
                market_id=str(item[0]),
                condition_id=str(item[1]),
                yes_token_id=str(item[2]),
                title=str(item[3]),
            )
            for item in raw_legs
            if isinstance(item, list) and len(item) == 4
        )
        if len(legs) != len(raw_legs):
            raise ValueError
        if (
            not row["group_id"]
            or not row["event_id"]
            or len(str(row["group_id"])) > 256
            or len(str(row["event_id"])) > 256
            or len(str(row["source_cursor"])) > 2_048
            or len(legs) > 500
            or any(
                max(
                    len(leg.market_id),
                    len(leg.condition_id),
                    len(leg.yes_token_id),
                    len(leg.title),
                )
                > 512
                for leg in legs
            )
            or int(row["revision"]) < 1
            or int(row["started_at_ms"]) > int(row["observed_at_ms"])
            or GroupRevision.membership_digest(legs) != str(row["membership_hash"])
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("invalid-group-history") from error
    return {
        "group_id": str(row["group_id"]),
        "event_id": str(row["event_id"]),
        "revision": int(row["revision"]),
        "membership_hash": str(row["membership_hash"]),
        "status": str(row["status"]),
        "started_at_ms": int(row["started_at_ms"]),
        "observed_at_ms": int(row["observed_at_ms"]),
        "source_cursor": str(row["source_cursor"]),
        "leg_count": len(legs),
    }


def _validate_group_history(
    rows: list[sqlite3.Row],
    *,
    allow_prefix: bool = False,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    previous_by_group: dict[str, dict[str, Any]] = {}
    allowed_transitions = {
        "discovered": {"certified", "invalidated", "closed"},
        "certified": {"certified", "stale", "invalidated", "closed"},
        "stale": {"certified", "stale", "invalidated", "closed"},
        "invalidated": {"certified", "invalidated", "closed"},
        "closed": {"closed"},
    }
    for row in rows:
        _check_read_deadline()
        item = _validate_revision(row)
        previous = previous_by_group.get(item["group_id"])
        if (previous is None and item["revision"] != 1 and not allow_prefix) or (
            previous is not None
            and (
                item["event_id"] != previous["event_id"]
                or item["revision"] != previous["revision"] + 1
                or item["started_at_ms"] < previous["started_at_ms"]
                or item["observed_at_ms"] < previous["observed_at_ms"]
                or item["status"] not in allowed_transitions[previous["status"]]
            )
        ):
            raise ValueError("invalid-group-history")
        previous_by_group[item["group_id"]] = item
        items.append(item)
    return items


def _read_incident_history(con: sqlite3.Connection, db_path: Path) -> list[dict[str, Any]]:
    count = int(con.execute("SELECT COUNT(*) FROM neg_risk_incident_events").fetchone()[0])
    evidence_size = con.execute(
        "SELECT COALESCE(MAX(length(evidence_json)),0),"
        "COALESCE(SUM(length(evidence_json)),0) "
        "FROM neg_risk_incident_events"
    ).fetchone()
    max_evidence = int(evidence_size[0])
    total_evidence = int(evidence_size[1])
    if count > _HISTORY_CAP or max_evidence > 4_096 or total_evidence > 1_048_576:
        raise ValueError("incident-history-too-large")
    rows = con.execute(
        "SELECT incident_id,sequence,scope,kind,state,occurred_at_ms,evidence_json "
        "FROM neg_risk_incident_events ORDER BY incident_id,sequence LIMIT ?",
        (_HISTORY_CAP + 1,),
    ).fetchall()
    previous: dict[str, dict[str, Any]] = {}
    recovering: dict[str, tuple[int, dict[str, Any]]] = {}
    proofs: list[tuple[str, int, int, dict[str, Any], dict[str, Any]]] = []
    for row in rows:
        try:
            evidence = json.loads(row["evidence_json"])
            if (
                not isinstance(evidence, dict)
                or len(str(row["evidence_json"]).encode("utf-8")) > 4_096
            ):
                raise ValueError
            item = {
                "incident_id": str(row["incident_id"]),
                "sequence": int(row["sequence"]),
                "scope": str(row["scope"]),
                "kind": str(row["kind"]),
                "state": str(row["state"]),
                "occurred_at_ms": int(row["occurred_at_ms"]),
                "evidence": _safe_evidence(evidence),
            }
            prior = previous.get(item["incident_id"])
            if (prior is None and (item["sequence"] != 1 or item["state"] != "detected")) or (
                prior is not None
                and (
                    item["sequence"] != prior["sequence"] + 1
                    or item["scope"] != prior["scope"]
                    or item["kind"] != prior["kind"]
                    or item["state"] not in _INCIDENT_EDGES[prior["state"]]
                    or item["occurred_at_ms"] < prior["occurred_at_ms"]
                )
            ):
                raise ValueError
            if item["state"] == "recovering":
                recovering[item["incident_id"]] = (
                    item["occurred_at_ms"],
                    evidence,
                )
            if item["state"] == "verified":
                recovery = recovering.get(item["incident_id"])
                if recovery is None:
                    raise ValueError
                proofs.append(
                    (
                        item["scope"],
                        recovery[0],
                        item["occurred_at_ms"],
                        recovery[1],
                        evidence,
                    )
                )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("invalid-incident-history") from error
        previous[item["incident_id"]] = item
    _validate_recovery_batch(con, db_path, proofs)
    return list(previous.values())


def _status(db_path: Path) -> dict[str, Any]:
    from polyarb.perception.incidents import IncidentManager

    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        store = _read_store(db_path)
        (
            open_incident_count,
            candidate_incident_open,
            _http_incident_open,
            _other_incident_open,
        ) = IncidentManager(store).open_incident_status(_connection=con)
        if candidate_incident_open:
            raise ValueError("candidate-worker-unavailable")
        summary = store.candidate_current_summary(_connection=con)
        count = summary.opportunity_count
        con.execute("COMMIT")
        return {
            "status": "available",
            "server_time_ms": int(time.time() * 1_000),
            "candidate_authority_hash": summary.authority_hash,
            "current_candidate_group_count": summary.current_group_count,
            "candidate_state_counts": summary.state_counts,
            "opportunities": {
                "status": "available",
                "count": count,
                "reason": "certified-edge" if count else "no-certified-edge",
            },
            "open_incident_count": open_incident_count,
        }
    finally:
        con.close()


def _opportunities(
    db_path: Path,
    limit: int,
    after_group_id: str,
) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        store = _read_store(db_path)
        summary = store.candidate_current_summary(_connection=con)
        items, next_after = store.current_opportunities(
            after_group_id=after_group_id,
            limit=limit,
            _connection=con,
        )
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    return {
        "status": "available",
        "server_time_ms": int(time.time() * 1_000),
        "candidate_authority_hash": summary.authority_hash,
        "current_opportunity_count": summary.opportunity_count,
        "items": [
            {
                "group_id": item.group_id,
                "event_id": item.event_id,
                "group_revision": item.group_revision,
                "membership_hash": item.membership_hash,
                "quote_batch_id": item.quote_batch_id,
                "fact_id": item.fact_id,
                "bundle_cost": float(item.bundle_cost),
                "gross_edge_bps": float(item.gross_edge_bps),
                "max_bundle_size": float(item.max_bundle_size),
                "structure_observed_at_ms": item.structure_observed_at_ms,
                "quote_started_at_ms": item.quote_started_at_ms,
                "quote_quoted_at_ms": item.quote_quoted_at_ms,
            }
            for item in items
        ],
        "limit": limit,
        "next_after_group_id": next_after,
    }


def _groups(db_path: Path, limit: int, after: str) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        current_meta = con.execute(
            "SELECT r.id,r.group_id,length(r.legs_json) AS legs_bytes "
            "FROM neg_risk_group_revisions r JOIN "
            "(SELECT group_id,MAX(revision) revision FROM neg_risk_group_revisions "
            "GROUP BY group_id) x ON x.group_id=r.group_id AND x.revision=r.revision "
            "WHERE r.group_id>? ORDER BY r.group_id LIMIT ?",
            (after, limit + 1),
        ).fetchall()
        if not current_meta:
            con.execute("COMMIT")
            return {
                "status": "available",
                "items": [],
                "limit": limit,
                "next_after": None,
            }
        if any(int(row["legs_bytes"] or 0) > _LEGS_JSON_MAX_BYTES for row in current_meta):
            raise ValueError("group-legs-json-too-large")
        has_more = len(current_meta) > limit
        current_meta = current_meta[:limit]
        current_ids = tuple(int(row["id"]) for row in current_meta)
        current_marks = ",".join("?" for _ in current_ids)
        current = con.execute(
            f"SELECT * FROM neg_risk_group_revisions "  # noqa: S608
            f"WHERE id IN ({current_marks}) ORDER BY group_id",
            current_ids,
        ).fetchall()
        for row in current:
            revision = int(row["revision"])
            previous = (
                None
                if revision == 1
                else con.execute(
                    "SELECT * FROM neg_risk_group_revisions WHERE group_id=? AND revision=?",
                    (row["group_id"], revision - 1),
                ).fetchone()
            )
            if revision > 1 and previous is None:
                raise ValueError("invalid-group-history")
            chain = [row] if previous is None else [previous, row]
            _validate_group_history(chain, allow_prefix=previous is not None)
        con.execute("COMMIT")
        return {
            "status": "available",
            "items": [_validate_revision(row) for row in current],
            "limit": limit,
            "next_after": (str(current_meta[-1]["group_id"]) if has_more else None),
        }
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _history(
    db_path: Path,
    group_id: str,
    limit: int,
    before_revision: int | None,
) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        upper_revision = 9_223_372_036_854_775_807 if before_revision is None else before_revision
        rows = con.execute(
            "SELECT * FROM neg_risk_group_revisions "
            "WHERE group_id=? AND revision<? "
            "ORDER BY revision DESC LIMIT ?",
            (group_id, upper_revision, limit + 1),
        ).fetchall()
        if not rows:
            con.execute("COMMIT")
            return {
                "status": "available",
                "group_id": group_id,
                "items": [],
                "limit": limit,
                "next_before_revision": None,
            }
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        oldest_revision = int(page_rows[-1]["revision"])
        anchor = (
            None
            if oldest_revision == 1
            else con.execute(
                "SELECT * FROM neg_risk_group_revisions WHERE group_id=? AND revision=?",
                (group_id, oldest_revision - 1),
            ).fetchone()
        )
        if oldest_revision > 1 and anchor is None:
            raise ValueError("invalid-group-history")
        ascending = list(reversed(page_rows))
        chain = ascending if anchor is None else [anchor, *ascending]
        validated = _validate_group_history(
            chain,
            allow_prefix=anchor is not None,
        )
        items = validated if anchor is None else validated[1:]
        items.reverse()
        con.execute("COMMIT")
        return {
            "status": "available",
            "group_id": group_id,
            "items": items,
            "limit": limit,
            "next_before_revision": (items[-1]["revision"] if has_more else None),
        }
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _discovery(db_path: Path) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        if con.execute("SELECT 1 FROM neg_risk_discovery_state WHERE id=1").fetchone() is None:
            con.execute("COMMIT")
            return {"status": "available", "discovery": None}
        status = _read_store(db_path).discovery_status(int(time.time() * 1_000), _connection=con)
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    return {
        "status": "available",
        "discovery": {
            "next_cursor": status.next_cursor,
            "completed": status.completed,
            "last_started_at_ms": status.last_started_at_ms,
            "last_finished_at_ms": status.last_finished_at_ms,
            "page_event_count": status.page_event_count,
            "groups_seen": status.groups_seen,
            "promoted_count": status.promoted_count,
            "queue_depth_by_class": status.queue_depth_by_class,
            "oldest_visit_age_ms": status.oldest_visit_age_ms,
            "promotion_queue_depth": status.promotion_queue_depth,
            "outstanding_admitted_count": status.outstanding_admitted_count,
            "candidate_attempt_start_count": status.candidate_attempt_start_count,
            "candidate_start_deadline_breach_count": (status.candidate_start_deadline_breach_count),
            "candidate_start_ready": status.candidate_start_ready,
            "coverage": {
                "known_groups": status.coverage.known_groups,
                "total_liquidity_weight": float(status.coverage.total_liquidity_weight),
                "by_minutes": {
                    str(minutes): {
                        "visited_groups": window.visited_groups,
                        "raw_fraction": float(window.raw_fraction),
                        "liquidity_weighted_fraction": float(window.liquidity_weighted_fraction),
                    }
                    for minutes, window in sorted(status.coverage.by_minutes.items())
                },
            },
            "load_state": {
                "degraded_streak": status.load_state.degraded_streak,
                "last_reason": status.load_state.last_reason,
                "last_decision": status.load_state.last_decision,
                "probe_every_cycles": status.load_state.probe_every_cycles,
                "updated_at_ms": status.load_state.updated_at_ms,
            },
            "admission_proof": (
                None
                if status.admission_proof is None
                else {
                    "effective_capacity": status.admission_proof.effective_capacity,
                    "candidate_max_wait_ms": (status.admission_proof.candidate_max_wait_ms),
                    "selection_budget_ms": status.admission_proof.selection_budget_ms,
                    "poll_interval_ms": status.admission_proof.poll_interval_ms,
                    "group_timeout_ms": status.admission_proof.group_timeout_ms,
                    "terminal_write_budget_ms": (status.admission_proof.terminal_write_budget_ms),
                    "attempt_start_write_budget_ms": (
                        status.admission_proof.attempt_start_write_budget_ms
                    ),
                    "high_burst_groups": status.admission_proof.high_burst_groups,
                    "reserved_non_high_slots": (status.admission_proof.reserved_non_high_slots),
                    "effective_start_bound_ms": (status.admission_proof.effective_start_bound_ms),
                }
            ),
        },
    }


def _reconciliation(db_path: Path) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        window = _read_store(db_path).current_reconciliation(_connection=con)
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    if window is None:
        return {"status": "available", "reconciliation": None}
    return {
        "status": "available",
        "reconciliation": {
            "id": window.id,
            "status": window.status,
            "failure_reason": window.failure_reason,
            "next_cursor": window.next_cursor,
            "started_at_ms": window.started_at_ms,
            "checkpoint_at_ms": window.checkpoint_at_ms,
            "finished_at_ms": window.finished_at_ms,
            "pages_completed": window.pages_completed,
            "events_seen": window.events_seen,
            "groups_staged": window.groups_staged,
            "rejected_count": window.rejected_count,
            "duration_ms": max(
                0,
                (window.finished_at_ms or window.checkpoint_at_ms) - window.started_at_ms,
            ),
            "observations_count": window.observations_count,
            "baseline_count": window.baseline_count,
            "added_count": window.added_count,
            "changed_count": window.changed_count,
            "closed_count": window.closed_count,
            "unchanged_count": window.unchanged_count,
            "applied_rejected_count": window.applied_rejected_count,
        },
    }


def _encode_incident_cursor(value: tuple[int, str]) -> str:
    payload = json.dumps(
        [value[0], value[1]],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _decode_incident_cursor(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    if not value or len(value) > 256:
        raise ValueError("before-must-be-an-opaque-incident-cursor")
    try:
        payload = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
        decoded = json.loads(payload)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("before-must-be-an-opaque-incident-cursor") from error
    if (
        not isinstance(decoded, list)
        or len(decoded) != 2
        or type(decoded[0]) is not int
        or decoded[0] < 0
        or not isinstance(decoded[1], str)
        or not decoded[1]
    ):
        raise ValueError("before-must-be-an-opaque-incident-cursor")
    return decoded[0], decoded[1]


def _encode_group_timeline_cursor(
    group_id: str,
    value: tuple[int, int, int],
) -> str:
    payload = json.dumps(
        [1, group_id, value[0], value[1], value[2]],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _decode_group_timeline_cursor(
    value: str | None,
    *,
    group_id: str,
) -> tuple[int, int, int] | None:
    if value is None:
        return None
    if not value or len(value) > 512 or "=" in value:
        raise ValueError("invalid-group-timeline-cursor")
    try:
        payload = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
        decoded = json.loads(payload)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("invalid-group-timeline-cursor")
    if (
        not isinstance(decoded, list)
        or len(decoded) != 5
        or decoded[0] != 1
        or decoded[1] != group_id
        or type(decoded[2]) is not int
        or decoded[2] < 0
        or type(decoded[3]) is not int
        or decoded[3] not in range(4)
        or type(decoded[4]) is not int
        or decoded[4] <= 0
    ):
        raise ValueError("invalid-group-timeline-cursor")
    result = (decoded[2], decoded[3], decoded[4])
    if _encode_group_timeline_cursor(group_id, result) != value:
        raise ValueError("invalid-group-timeline-cursor")
    return result


def _timeline(
    db_path: Path,
    group_id: str,
    limit: int,
    before: tuple[int, int, int] | None,
) -> dict[str, Any]:
    from polyarb.perception.incidents import IncidentManager

    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        store = _read_store(db_path)
        sources = store.validated_group_timeline_sources(
            group_id,
            limit=limit,
            before=before,
            _connection=con,
        )
        incident_before: tuple[int, int] | None = None
        incident_include_equal_ms = False
        if before is not None:
            if before[1] == 3:
                incident_before = (before[0], before[2])
            else:
                incident_before = (before[0] + 1, 1)
                incident_include_equal_ms = True
        incident_page = IncidentManager(store).group_incident_history(
            group_id,
            limit=limit,
            before_order_key=incident_before,
            _connection=con,
        )
        con.execute("COMMIT")

        class_names = (
            "membership_revision",
            "quote_batch",
            "opportunity_transition",
        )
        merged: list[dict[str, Any]] = []
        for class_order, source_name in enumerate(("membership", "quote", "opportunity")):
            for source_item in sources[source_name]:
                merged.append(
                    {
                        "class": class_names[class_order],
                        "class_order": class_order,
                        **source_item,
                    }
                )
        for event in incident_page.items:
            if (
                before is not None
                and incident_include_equal_ms
                and event.incident.occurred_at_ms > before[0]
            ):
                continue
            merged.append(
                {
                    "class": "incident_event",
                    "class_order": 3,
                    "stable_id": event.event_id,
                    "occurred_at_ms": event.incident.occurred_at_ms,
                    "incident_id": event.incident.id,
                    "sequence": event.incident.sequence,
                    "scope": event.incident.scope,
                    "kind": event.incident.kind,
                    "state": event.incident.state,
                    "evidence": _safe_evidence(event.incident.evidence),
                }
            )
        merged.sort(
            key=lambda item: (
                -int(item["occurred_at_ms"]),
                int(item["class_order"]),
                -int(item["stable_id"]),
            )
        )
        page_items = merged[:limit]
        candidate_has_more = any(
            len(sources[name]) > limit for name in ("membership", "quote", "opportunity")
        )
        has_more = (
            len(merged) > limit
            or candidate_has_more
            or incident_page.next_before_event_id is not None
        )
        next_before = (
            None
            if not has_more or not page_items
            else _encode_group_timeline_cursor(
                group_id,
                (
                    int(page_items[-1]["occurred_at_ms"]),
                    int(page_items[-1]["class_order"]),
                    int(page_items[-1]["stable_id"]),
                ),
            )
        )
        for item in page_items:
            item.pop("class_order")
        incident_floor = {
            "scope": f"candidate:{group_id}",
            "through_id": incident_page.floor_event_id or 0,
            "compacted_count": incident_page.floor_compacted_count or 0,
        }
        floors = {**sources["history_floor"], "incident": incident_floor}
        return {
            "status": "available",
            "group_id": group_id,
            "items": page_items,
            "limit": limit,
            "next_before": next_before,
            "history_floor": floors,
            "history_complete": {
                "membership": floors["membership"]["compacted_count"] == 0,
                "quote": floors["quote"]["compacted_count"] == 0,
                "opportunity": (floors["opportunity"]["source_rows_compacted"] == 0),
                "incident": incident_floor["compacted_count"] == 0,
            },
        }
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _quote_incident_diagnosis(evidence: dict[str, object]) -> dict[str, object] | None:
    """Expose only a complete, credential-free Quote failure disposition."""
    required_strings = ("impact", "automatic_action", "next_action")
    if any(not isinstance(evidence.get(key), str) for key in required_strings):
        return None
    deadline_s = evidence.get("deadline_s")
    failures = evidence.get("consecutive_failures")
    age = evidence.get("last_success_age_s")
    if (
        (deadline_s is not None and (
            isinstance(deadline_s, bool)
            or not isinstance(deadline_s, (int, float))
            or not math.isfinite(float(deadline_s))
            or deadline_s <= 0
        ))
        or isinstance(failures, bool)
        or not isinstance(failures, int)
        or failures < 1
        or (
            age is not None
            and (isinstance(age, bool) or not isinstance(age, (int, float)) or age < 0)
        )
    ):
        return None
    severity = evidence.get("severity")
    reminder_interval_s = evidence.get("reminder_interval_s")
    if (
        severity not in {"p1", "p2"}
        or isinstance(reminder_interval_s, bool)
        or not isinstance(reminder_interval_s, int)
        or reminder_interval_s <= 0
    ):
        return None
    return {
        "severity": severity,
        "reminder_interval_s": reminder_interval_s,
        "impact": evidence["impact"],
        "automatic_action": evidence["automatic_action"],
        "next_action": evidence["next_action"],
        "deadline_s": None if deadline_s is None else float(deadline_s),
        "consecutive_failures": failures,
        "last_success_age_s": None if age is None else float(age),
        "free_percent": None,
        "failure_reason": (
            evidence["failure_reason"] if isinstance(evidence.get("failure_reason"), str) else None
        ),
    }


def _capacity_incident_diagnosis(evidence: dict[str, object]) -> dict[str, object] | None:
    """Expose a complete, credential-free disposition for storage pressure."""
    if (
        evidence.get("severity") not in {"p1", "p2"}
        or evidence.get("impact") != "storage-exhaustion-risk"
        or evidence.get("automatic_action") != "reclaim-bounded-history"
        or evidence.get("next_action") != "inspect-capacity-receipts"
    ):
        return None
    reminder = evidence.get("reminder_interval_s")
    free_percent = evidence.get("free_percent")
    failures = evidence.get("consecutive_failures")
    if (
        isinstance(reminder, bool)
        or not isinstance(reminder, int)
        or reminder <= 0
        or isinstance(free_percent, bool)
        or not isinstance(free_percent, (int, float))
        or not 0.0 <= float(free_percent) <= 100.0
        or isinstance(failures, bool)
        or not isinstance(failures, int)
        or failures < 0
    ):
        return None
    failure_reason = evidence.get("failure_reason")
    if failure_reason is not None and not isinstance(failure_reason, str):
        return None
    return {
        "severity": evidence["severity"],
        "reminder_interval_s": reminder,
        "impact": evidence["impact"],
        "automatic_action": evidence["automatic_action"],
        "next_action": evidence["next_action"],
        "deadline_s": None,
        "free_percent": float(free_percent),
        "consecutive_failures": failures,
        "last_success_age_s": None,
        "failure_reason": failure_reason,
    }


def _quote_supervisor_incident_diagnosis(
    *,
    kind: str,
    state: str,
    evidence: dict[str, object],
) -> dict[str, object] | None:
    """Make failed Quote supervision an actionable P1, not opaque evidence."""
    if not kind.startswith("child-"):
        return None
    retries = evidence.get("retry_count")
    if type(retries) is not int or retries < 0:
        return None
    exhausted = state == "escalated"
    return {
        "severity": "p1",
        "reminder_interval_s": 300,
        "impact": "feed-unavailable",
        "automatic_action": (
            "automatic-retries-exhausted" if exhausted else "retry-supervised-producer"
        ),
        "next_action": "inspect-producer-receipt-and-restart",
        "deadline_s": None,
        "consecutive_failures": retries + 1,
        "last_success_age_s": None,
        "free_percent": None,
        "failure_reason": kind,
    }


def _structure_incident_diagnosis(
    evidence: dict[str, object],
) -> dict[str, object] | None:
    """Expose the complete, bounded disposition for a Structure outage."""
    if (
        evidence.get("severity") not in {"p1", "p2"}
        or evidence.get("impact") != "market-map-stale"
        or evidence.get("automatic_action") != "retry-bounded-structure-child"
        or evidence.get("next_action") != "inspect-stage-checkpoint-and-child-budget"
    ):
        return None
    failure_reason = evidence.get("failure_reason")
    elapsed_ms = evidence.get("elapsed_ms")
    last_stage = evidence.get("last_stage")
    if (
        not isinstance(failure_reason, str)
        or not failure_reason
        or (
            elapsed_ms is not None
            and (isinstance(elapsed_ms, bool) or not isinstance(elapsed_ms, int) or elapsed_ms < 0)
        )
        or (last_stage is not None and not isinstance(last_stage, str))
    ):
        return None
    return {
        "severity": evidence["severity"],
        "reminder_interval_s": 300,
        "impact": evidence["impact"],
        "automatic_action": evidence["automatic_action"],
        "next_action": evidence["next_action"],
        "deadline_s": None,
        "consecutive_failures": 1,
        "last_success_age_s": None,
        "free_percent": None,
        "failure_reason": failure_reason,
        "elapsed_ms": elapsed_ms,
        "last_stage": last_stage,
    }


def _incidents(
    db_path: Path,
    limit: int,
    before: tuple[int, str] | None,
) -> dict[str, Any]:
    from polyarb.perception.incidents import IncidentManager

    canonical_actions = {
        "classify-producer-failure",
        "operator-intervention",
        "restart-producer",
        "retry-producer",
    }
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        page = IncidentManager(_read_store(db_path)).open_incident_page(
            limit=limit,
            before=before,
            _connection=con,
        )
        con.execute("COMMIT")
        now_ms = int(time.time() * 1_000)
        return {
            "status": "available",
            "items": [
                {
                    "incident_id": item.incident.id,
                    "sequence": item.incident.sequence,
                    "scope": item.incident.scope,
                    "kind": item.incident.kind,
                    "state": item.incident.state,
                    "detected_at_ms": item.detected_at_ms,
                    "occurred_at_ms": item.incident.occurred_at_ms,
                    "lifecycle_age_ms": max(0, now_ms - item.detected_at_ms),
                    "action": (
                        item.incident.evidence.get("action")
                        if item.incident.evidence.get("action") in canonical_actions
                        else None
                    ),
                    "retry_count": (
                        item.incident.evidence.get("retry_count")
                        if type(item.incident.evidence.get("retry_count")) is int
                        and item.incident.evidence["retry_count"] >= 0
                        else (
                            item.incident.evidence.get("retry")
                            if type(item.incident.evidence.get("retry")) is int
                            and item.incident.evidence["retry"] >= 0
                            else None
                        )
                    ),
                    "next_retry_at_ms": (
                        item.incident.evidence.get("next_retry_at_ms")
                        if type(item.incident.evidence.get("next_retry_at_ms")) is int
                        and item.incident.evidence["next_retry_at_ms"] >= 0
                        else None
                    ),
                    "recovery_start_evidence": _safe_evidence(item.recovery_evidence),
                    "recovery_occurred_at_ms": item.recovery_occurred_at_ms,
                    "history_floor": (
                        None
                        if item.history_floor_event_id is None
                        else {
                            "through_event_id": item.history_floor_event_id,
                            "compacted_event_count": item.history_floor_compacted_count,
                        }
                    ),
                    "notification_delivery_tracked": False,
                    "diagnosis": (
                        _quote_incident_diagnosis(item.incident.evidence)
                        if item.incident.scope == "quote-collection"
                        else _quote_supervisor_incident_diagnosis(
                            kind=item.incident.kind,
                            state=item.incident.state,
                            evidence=item.incident.evidence,
                        )
                        if item.incident.scope == "quote"
                        else _capacity_incident_diagnosis(item.incident.evidence)
                        if item.incident.scope == "capacity"
                        else _structure_incident_diagnosis(item.incident.evidence)
                        if item.incident.scope == "structure"
                        else None
                    ),
                    "evidence": _safe_evidence(item.incident.evidence),
                }
                for item in page.items
            ],
            "limit": limit,
            "open_count": page.open_count,
            "next_before": (
                None if page.next_before is None else _encode_incident_cursor(page.next_before)
            ),
        }
    finally:
        con.close()


def _incident_history(db_path: Path, incident_id: str) -> dict[str, Any]:
    from polyarb.perception.incidents import IncidentManager

    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        history = IncidentManager(_read_store(db_path)).incident_history(
            incident_id,
            _connection=con,
        )
        con.execute("COMMIT")
        if history is None:
            raise _IncidentNotFoundError
        latest = history.items[-1].incident
        receipt = None
        if latest.state == "verified":
            component = latest.scope.split(":", 1)[0]
            pointer_fields = {
                "candidate": "candidate_success_receipt_id",
                "discovery": "batch_id",
                "reconciliation": "window_id",
            }
            pointer = latest.evidence.get(pointer_fields.get(component, ""))
            if type(pointer) is int and pointer > 0:
                receipt = {
                    "component": component,
                    "receipt_row_id": pointer,
                }
        return {
            "status": "available",
            "incident_id": incident_id,
            "scope": latest.scope,
            "kind": latest.kind,
            "history_complete": history.history_complete,
            "recovery_writer_receipt": receipt,
            "items": [
                {
                    "event_id": item.event_id,
                    "sequence": item.incident.sequence,
                    "state": item.incident.state,
                    "occurred_at_ms": item.incident.occurred_at_ms,
                    "evidence": _safe_evidence(item.incident.evidence),
                }
                for item in history.items
            ],
        }
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _recent_incidents(
    db_path: Path,
    *,
    scope: str,
    after_ms: int,
    limit: int,
) -> dict[str, Any]:
    from polyarb.perception.incidents import IncidentManager

    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        items = IncidentManager(_read_store(db_path)).recent_incidents(
            scope,
            after_ms=after_ms,
            limit=limit,
            _connection=con,
        )
        con.execute("COMMIT")
        return {
            "status": "available",
            "scope": scope,
            "after_ms": after_ms,
            "limit": limit,
            "items": [
                {
                    "incident_id": item.id,
                    "sequence": item.sequence,
                    "kind": item.kind,
                    "state": item.state,
                    "occurred_at_ms": item.occurred_at_ms,
                    "evidence": _safe_evidence(item.evidence),
                }
                for item in items
            ],
        }
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _qualification(db_path: Path) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        store = _read_store(db_path)
        store.candidate_current_summary(_connection=con)
        candidate_mismatches = int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_candidate_success_receipts r "
                "LEFT JOIN neg_risk_group_revisions g "
                "ON g.id=r.group_revision_row_id "
                "LEFT JOIN neg_risk_group_quote_batches q "
                "ON q.rowid=r.quote_batch_row_id AND q.id=r.quote_batch_id "
                "LEFT JOIN neg_risk_candidate_watch_facts f "
                "ON f.id=r.candidate_fact_row_id "
                "WHERE g.id IS NULL OR q.rowid IS NULL OR f.id IS NULL "
                "OR r.group_id!=g.group_id OR r.group_id!=q.group_id "
                "OR r.group_id!=f.group_id "
                "OR r.membership_hash!=g.membership_hash "
                "OR r.membership_hash!=q.membership_hash "
                "OR r.membership_hash!=f.membership_hash "
                "OR q.group_revision!=g.revision "
                "OR f.quote_batch_id!=q.id"
            ).fetchone()[0]
        )
        legacy_mismatches = int(
            con.execute(
                "SELECT COUNT(DISTINCT l.quote_run_id) "
                "FROM neg_risk_quote_run_legs l "
                "JOIN neg_risk_quotes q "
                "ON q.quote_run_id=l.quote_run_id "
                "AND q.yes_token_id=l.yes_token_id "
                "WHERE trim(l.event_id)='' OR trim(l.membership_hash)='' "
                "OR trim(q.event_id)='' OR trim(q.membership_hash)='' "
                "OR q.event_id!=l.event_id "
                "OR q.membership_hash!=l.membership_hash "
                "OR q.neg_risk_market_id!=l.neg_risk_market_id "
                "OR q.market_id!=l.market_id "
                "OR q.condition_id!=l.condition_id"
            ).fetchone()[0]
        )
        orphan_collecting = int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_quote_runs "
                "WHERE status='collecting' AND lease_expires_at_ms<=?",
                (int(time.time() * 1_000),),
            ).fetchone()[0]
        )
        con.execute("COMMIT")
        return {
            "status": "available",
            "cross_membership_quote_batches": (candidate_mismatches + legacy_mismatches),
            "orphan_collecting_runs": orphan_collecting,
        }
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _resources(
    db_path: Path,
    limit: int,
    before_sequence: int | None,
) -> dict[str, Any]:
    from polyarb.perception.resource_controller import (
        resource_history_page,
        validate_resource_evidence_failure,
    )

    store = _read_store(db_path)
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        store._assert_owner_journal_clean(con)
        validate_resource_evidence_failure(con, require_resolved=True)
        page = resource_history_page(
            con,
            limit=limit,
            before_sequence=before_sequence,
        )
        con.execute("COMMIT")
        return {
            "status": "available",
            "current": None if page.current is None else asdict(page.current),
            "items": [
                {
                    "sample": asdict(item.sample),
                    "decision": asdict(item.decision),
                }
                for item in page.items
            ],
            "limit": limit,
            "next_before_sequence": page.next_before_sequence,
            "history_floor": (None if page.history_floor is None else asdict(page.history_floor)),
        }
    finally:
        con.close()


async def _serve(
    request: Request,
    reader: Callable[[], dict[str, Any]],
    *,
    lane_name: str = "perception_read_lane",
    timeout_s: float = _TIMEOUT_S,
    sql_deadline_s: float = _READ_SQL_DEADLINE_S,
) -> JSONResponse:
    execution = _ReadExecution(time.monotonic() + sql_deadline_s)
    token = _READ_EXECUTION.set(execution)
    try:
        task = asyncio.create_task(
            getattr(request.app.state, lane_name).run(
                reader,
                timeout_s=timeout_s,
            )
        )
    finally:
        _READ_EXECUTION.reset(token)
    try:
        try:
            body = await task
        except TimeoutError:
            execution.interrupt()
            raise
        if (
            len(
                json.dumps(
                    body,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            )
            > 1_048_576
        ):
            raise ValueError("response-output-too-large")
        return JSONResponse(body)
    except _IncidentNotFoundError:
        return JSONResponse(
            {
                "status": "unavailable",
                "reason": "incident-not-found-or-retained",
            },
            status_code=404,
        )
    except ValueError as error:
        if str(error) == "limit-must-be-an-integer-from-1-to-500":
            return JSONResponse(
                {"status": "invalid-request", "reason": str(error)}, status_code=400
            )
        return JSONResponse(
            {"status": "unavailable", "reason": "durable-evidence-invalid"},
            status_code=503,
        )
    except ReadLaneSaturatedError:
        return JSONResponse(
            {"status": "unavailable", "reason": "read-model-saturated"},
            status_code=503,
        )
    except (TimeoutError, sqlite3.Error):
        return JSONResponse(
            {"status": "unavailable", "reason": "read-model-unavailable"},
            status_code=503,
        )


async def perception_status(request: Request) -> JSONResponse:
    db_path = Path(request.app.state.sqlite_store.db_path)
    response = await _serve(request, lambda: _status(db_path))
    if response.status_code == 503:
        return JSONResponse(
            {
                "status": "unavailable",
                "opportunities": {
                    "status": "unavailable",
                    "count": None,
                    "reason": "worker-or-evidence-unavailable",
                },
            },
            status_code=503,
        )
    return response


async def perception_groups(request: Request) -> JSONResponse:
    try:
        limit = _limit(request)
    except ValueError as error:
        return JSONResponse({"status": "invalid-request", "reason": str(error)}, status_code=400)
    after = request.query_params.get("after", "")
    if len(after) > 256 or "\x00" in after:
        return JSONResponse(
            {"status": "invalid-request", "reason": "invalid-after-cursor"},
            status_code=400,
        )
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _groups(db_path, limit, after))


async def perception_opportunities(request: Request) -> JSONResponse:
    try:
        limit = _limit(request)
    except ValueError as error:
        return JSONResponse(
            {"status": "invalid-request", "reason": str(error)},
            status_code=400,
        )
    after_group_id = request.query_params.get("after_group_id", "")
    if len(after_group_id) > 256 or "\x00" in after_group_id:
        return JSONResponse(
            {
                "status": "invalid-request",
                "reason": "invalid-after-group-id-cursor",
            },
            status_code=400,
        )
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(
        request,
        lambda: _opportunities(db_path, limit, after_group_id),
    )


async def perception_group_history(request: Request) -> JSONResponse:
    try:
        limit = _limit(request)
    except ValueError as error:
        return JSONResponse({"status": "invalid-request", "reason": str(error)}, status_code=400)
    group_id = unquote(str(request.path_params["group_id"]))
    if not group_id or len(group_id) > 256 or "\x00" in group_id:
        return JSONResponse(
            {"status": "invalid-request", "reason": "invalid-group-id"},
            status_code=400,
        )
    raw_before = request.query_params.get("before_revision")
    try:
        before_revision = None if raw_before is None else int(raw_before)
        if before_revision is not None and (
            before_revision < 1 or str(before_revision) != raw_before
        ):
            raise ValueError
    except ValueError:
        return JSONResponse(
            {
                "status": "invalid-request",
                "reason": "before-revision-must-be-a-positive-integer",
            },
            status_code=400,
        )
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(
        request,
        lambda: _history(db_path, group_id, limit, before_revision),
    )


async def perception_group_timeline(request: Request) -> JSONResponse:
    try:
        limit = _limit(request)
    except ValueError as error:
        return JSONResponse(
            {"status": "invalid-request", "reason": str(error)},
            status_code=400,
        )
    group_id = unquote(str(request.path_params["group_id"]))
    if not group_id or len(group_id) > 256 or "\x00" in group_id:
        return JSONResponse(
            {"status": "invalid-request", "reason": "invalid-group-id"},
            status_code=400,
        )
    try:
        before = _decode_group_timeline_cursor(
            request.query_params.get("before"),
            group_id=group_id,
        )
    except ValueError as error:
        return JSONResponse(
            {"status": "invalid-request", "reason": str(error)},
            status_code=400,
        )
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(
        request,
        lambda: _timeline(db_path, group_id, limit, before),
    )


async def perception_discovery(request: Request) -> JSONResponse:
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _discovery(db_path))


async def perception_reconciliation(request: Request) -> JSONResponse:
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _reconciliation(db_path))


async def perception_incidents(request: Request) -> JSONResponse:
    try:
        limit = _limit(request)
        before = _decode_incident_cursor(request.query_params.get("before"))
    except ValueError as error:
        return JSONResponse({"status": "invalid-request", "reason": str(error)}, status_code=400)
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(
        request,
        lambda: _incidents(db_path, limit, before),
        lane_name="incident_read_lane",
        timeout_s=_INCIDENT_READ_TIMEOUT_S,
        sql_deadline_s=_INCIDENT_READ_SQL_DEADLINE_S,
    )


async def perception_incident_history(request: Request) -> JSONResponse:
    incident_id = str(request.path_params["incident_id"])
    if len(incident_id) != 32 or any(
        character not in "0123456789abcdef" for character in incident_id
    ):
        return JSONResponse(
            {"status": "invalid-request", "reason": "invalid-incident-id"},
            status_code=400,
        )
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(
        request,
        lambda: _incident_history(db_path, incident_id),
        lane_name="incident_read_lane",
        timeout_s=_INCIDENT_READ_TIMEOUT_S,
        sql_deadline_s=_INCIDENT_READ_SQL_DEADLINE_S,
    )


async def perception_recent_incidents(request: Request) -> JSONResponse:
    scope = request.query_params.get("scope", "")
    if re.fullmatch(r"[a-z][a-z0-9:_-]{0,127}", scope) is None or "\x00" in scope:
        return JSONResponse(
            {"status": "invalid-request", "reason": "invalid-incident-scope"},
            status_code=400,
        )
    raw_after = request.query_params.get("after_ms", "")
    try:
        after_ms = int(raw_after, 10)
        if after_ms < 0 or str(after_ms) != raw_after:
            raise ValueError
    except ValueError:
        return JSONResponse(
            {"status": "invalid-request", "reason": "invalid-after-ms"},
            status_code=400,
        )
    try:
        limit = _limit(request)
    except ValueError as error:
        return JSONResponse(
            {"status": "invalid-request", "reason": str(error)},
            status_code=400,
        )
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(
        request,
        lambda: _recent_incidents(
            db_path,
            scope=scope,
            after_ms=after_ms,
            limit=limit,
        ),
        lane_name="incident_read_lane",
        timeout_s=_INCIDENT_READ_TIMEOUT_S,
        sql_deadline_s=_INCIDENT_READ_SQL_DEADLINE_S,
    )


async def perception_qualification(request: Request) -> JSONResponse:
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _qualification(db_path))


async def perception_resources(request: Request) -> JSONResponse:
    try:
        limit = _limit(request)
    except ValueError as error:
        return JSONResponse(
            {"status": "invalid-request", "reason": str(error)},
            status_code=400,
        )
    try:
        raw_before = request.query_params.get("before_sequence")
        before_sequence = None if raw_before is None else int(raw_before, 10)
        if before_sequence is not None and (
            before_sequence <= 0 or str(before_sequence) != raw_before
        ):
            raise ValueError
    except ValueError:
        return JSONResponse(
            {
                "status": "invalid-request",
                "reason": "before-sequence-must-be-a-positive-integer",
            },
            status_code=400,
        )
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(
        request,
        lambda: _resources(db_path, limit, before_sequence),
    )
