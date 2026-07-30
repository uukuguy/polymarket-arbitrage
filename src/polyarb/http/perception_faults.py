"""Dedicated, disabled-by-default upstream fault authority HTTP boundary."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import sqlite3
import time
from typing import Any
from uuid import UUID

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import (
    FaultAuthorization,
    FaultEventAction,
    FaultIntentRequest,
    FaultRuntimeIdentity,
    canonical_digest,
)

_MAX_BODY_BYTES = 65_536
_AUTH_SKEW_SECONDS = 300
_STORE_BUDGET_SECONDS = 0.75
_CLEANUP_WAIT_SECONDS = 0.2
_TERMINAL_CLEANUP_STATES = frozenset(
    {
        "cleaned",
        "abandoned",
        "expired",
        "recovered",
        "verified",
        "rejected",
        "cleanup-failed",
        "recovery-timeout",
        "evidence-invalid",
        "escalated",
    }
)
_ARM_FIELDS = frozenset(
    {
        "fault_id",
        "kind",
        "call_class",
        "target_key",
        "parameters",
        "ttl_ms",
        "runtime",
    }
)
_RUNTIME_FIELDS = frozenset({"component", "release_id", "machine_id", "boot_id"})


def _json_no_duplicates(raw: bytes) -> object:
    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise ValueError("duplicate-field")
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid-json") from exc


def _fault_auth(request: Request, body: bytes) -> FaultAuthorization | None:
    timestamp = request.headers.get("X-Fault-Timestamp", "")
    nonce = request.headers.get("X-Fault-Nonce", "")
    received = request.headers.get("X-Fault-Signature", "")
    try:
        timestamp_s = int(timestamp)
    except ValueError:
        return None
    if (
        str(timestamp_s) != timestamp
        or abs(int(time.time()) - timestamp_s) > _AUTH_SKEW_SECONDS
        or not 16 <= len(nonce) <= 128
        or not nonce.isalnum()
        or not received
    ):
        return None
    canonical = b"\n".join(
        (
            b"polyarb-fault-v1",
            timestamp.encode(),
            nonce.encode(),
            request.method.encode(),
            request.url.path.encode(),
            body,
        )
    )
    secret = request.app.state.settings.upstream_fault_control_secret.get_secret_value()
    expected = hmac.new(secret.encode(), canonical, hashlib.sha256).hexdigest()
    if received.startswith("sha256="):
        received = received[7:]
    if not hmac.compare_digest(received, expected):
        return None
    return FaultAuthorization(
        nonce_digest=hashlib.sha256(nonce.encode()).hexdigest(),
        authorization_digest=hashlib.sha256(canonical).hexdigest(),
    )


async def _bounded_body(request: Request) -> bytes | JSONResponse:
    body = getattr(request.state, "perception_control_body", None)
    if body is None:
        body = await request.body()
    if len(body) > _MAX_BODY_BYTES:
        return JSONResponse({"error": "request body too large"}, status_code=413)
    return body


def _store(request: Request, *, read_only: bool = False) -> FaultAuthorityStore:
    return FaultAuthorityStore(
        request.app.state.sqlite_store.db_path,
        read_only=read_only,
        busy_timeout_ms=250,
    )


async def _run_mutation(function: Any, *args: Any, **kwargs: Any) -> Any:
    deadline = time.monotonic() + _STORE_BUDGET_SECONDS
    kwargs["deadline_monotonic"] = deadline
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.wait_for(
            asyncio.shield(task), timeout=_STORE_BUDGET_SECONDS + 0.1
        )
    except TimeoutError:
        # The store checks the same deadline before every mutation and COMMIT.
        # Settle the worker so an unavailable response can never race a late commit.
        return await asyncio.shield(task)


async def _read_snapshot(request: Request, **selectors: Any) -> Any:
    deadline = time.monotonic() + _STORE_BUDGET_SECONDS
    return await asyncio.wait_for(
        asyncio.to_thread(
            _store(request, read_only=True).read_snapshot,
            now_ms=int(time.time() * 1_000),
            deadline_monotonic=deadline,
            **selectors,
        ),
        timeout=_STORE_BUDGET_SECONDS + 0.1,
    )


async def _audit_invalid_request(
    request: Request,
    *,
    auth: FaultAuthorization,
    operation: str,
    body: bytes,
    fault_id: str | None,
) -> JSONResponse | None:
    try:
        await _run_mutation(
            _store(request).reject_control_attempt,
            auth=auth,
            operation=operation,
            fault_id=fault_id,
            request_digest=hashlib.sha256(body).hexdigest(),
            reason="invalid-request",
            occurred_at_ms=int(time.time() * 1_000),
        )
        return None
    except (TimeoutError, sqlite3.Error):
        return JSONResponse(
            {"status": "unavailable", "reason": "fault-control-store-unavailable"},
            status_code=409,
        )


async def arm_fault(request: Request) -> JSONResponse:
    if not request.app.state.settings.upstream_fault_control_enabled:
        return JSONResponse(
            {"status": "unavailable", "reason": "fault-control-disabled"},
            status_code=409,
        )
    body = await _bounded_body(request)
    if isinstance(body, JSONResponse):
        return body
    authorized = _fault_auth(request, body)
    if authorized is None:
        return JSONResponse({"error": "invalid fault authentication"}, status_code=401)
    auth = authorized
    try:
        payload = _json_no_duplicates(body)
        if not isinstance(payload, dict) or set(payload) != _ARM_FIELDS:
            raise ValueError("invalid-fields")
        runtime_value = payload["runtime"]
        if not isinstance(runtime_value, dict) or set(runtime_value) != _RUNTIME_FIELDS:
            raise ValueError("invalid-runtime-fields")
        runtime = FaultRuntimeIdentity(
            component=runtime_value["component"],
            release_id=runtime_value["release_id"],
            machine_id=runtime_value["machine_id"],
            boot_id=UUID(runtime_value["boot_id"]),
        )
        intent = FaultIntentRequest(
            fault_id=payload["fault_id"],
            kind=payload["kind"],
            call_class=payload["call_class"],
            target_key=payload["target_key"],
            parameters=payload["parameters"],
            ttl_ms=payload["ttl_ms"],
            runtime=runtime,
        )
        if intent.ttl_ms > request.app.state.settings.upstream_fault_control_max_ttl_ms:
            raise ValueError("invalid-ttl")
    except (KeyError, TypeError, ValueError):
        audit_failure = await _audit_invalid_request(
            request,
            auth=auth,
            operation="arm",
            body=body,
            fault_id=None,
        )
        if audit_failure is not None:
            return audit_failure
        return JSONResponse({"error": "invalid fault request"}, status_code=400)
    try:
        admission = await _run_mutation(
            _store(request).accept_intent,
            intent,
            auth=auth,
            accepted_at_ms=int(time.time() * 1_000),
            request_digest=hashlib.sha256(body).hexdigest(),
        )
    except sqlite3.IntegrityError:
        return JSONResponse({"error": "invalid fault authentication"}, status_code=401)
    except ValueError:
        return JSONResponse(
            {"status": "unavailable", "reason": "fault-control-store-unavailable"},
            status_code=409,
        )
    except (TimeoutError, sqlite3.Error):
        return JSONResponse(
            {"status": "unavailable", "reason": "fault-control-store-unavailable"},
            status_code=409,
        )
    if not admission.accepted:
        status = 401 if admission.reason == "nonce-replay" else 409
        return JSONResponse(
            {"status": "rejected", "reason": admission.reason},
            status_code=status,
        )
    return JSONResponse(
        {
            "status": "accepted",
            "fault_id": intent.fault_id,
            "parameter_digest": canonical_digest(dict(intent.parameters)),
            "authorization_digest": auth.authorization_digest,
        },
        status_code=202,
    )


async def cleanup_fault(request: Request) -> JSONResponse:
    if not request.app.state.settings.upstream_fault_control_enabled:
        return JSONResponse(
            {"status": "unavailable", "reason": "fault-control-disabled"},
            status_code=409,
        )
    body = await _bounded_body(request)
    if isinstance(body, JSONResponse):
        return body
    authorized = _fault_auth(request, body)
    if authorized is None:
        return JSONResponse({"error": "invalid fault authentication"}, status_code=401)
    auth = authorized
    try:
        payload = _json_no_duplicates(body)
        if (
            not isinstance(payload, dict)
            or set(payload) != {"fault_id"}
            or not isinstance(payload["fault_id"], str)
        ):
            raise ValueError("invalid-fields")
        fault_id = payload["fault_id"]
    except (KeyError, TypeError, ValueError):
        audit_failure = await _audit_invalid_request(
            request,
            auth=auth,
            operation="cleanup",
            body=body,
            fault_id=None,
        )
        if audit_failure is not None:
            return audit_failure
        return JSONResponse({"error": "invalid cleanup request"}, status_code=400)
    try:
        await _run_mutation(
            _store(request).request_cleanup,
            fault_id,
            auth=auth,
            requested_at_ms=int(time.time() * 1_000),
            request_digest=hashlib.sha256(body).hexdigest(),
        )
    except ValueError as exc:
        if str(exc) == "nonce-replay":
            return JSONResponse({"error": "invalid fault authentication"}, status_code=401)
        if str(exc) in {"fault-auth-history-invalid", "fault-history-invalid"}:
            return JSONResponse(
                {
                    "status": "unavailable",
                    "reason": "fault-control-store-unavailable",
                },
                status_code=409,
            )
        return JSONResponse({"error": "invalid cleanup request"}, status_code=400)
    except (TimeoutError, sqlite3.Error):
        return JSONResponse(
            {"status": "unavailable", "reason": "fault-control-store-unavailable"},
            status_code=409,
        )
    deadline = time.monotonic() + _CLEANUP_WAIT_SECONDS
    while time.monotonic() < deadline:
        try:
            snapshot = await _read_snapshot(request, fault_id=fault_id)
        except TimeoutError:
            snapshot = None
        if (
            snapshot is None
            or not snapshot.available
            or snapshot.history is None
            or snapshot.projection is None
        ):
            return JSONResponse(
                {
                    "status": "unavailable",
                    "reason": "fault-control-store-unavailable",
                },
                status_code=409,
            )
        state = snapshot.projection.state
        if state is not None and state.value in _TERMINAL_CLEANUP_STATES:
            return JSONResponse(
                {
                    "status": "already-terminal",
                    "fault_id": fault_id,
                    "current_state": state.value,
                },
                status_code=200,
            )
        await asyncio.sleep(0.02)
    return JSONResponse(
        {"status": FaultEventAction.CLEANUP_REQUESTED.value, "fault_id": fault_id},
        status_code=202,
    )


def _runtime_json(runtime: FaultRuntimeIdentity) -> dict[str, str]:
    return {
        "component": runtime.component,
        "release_id": runtime.release_id,
        "machine_id": runtime.machine_id,
        "boot_id": str(runtime.boot_id),
    }


async def fault_runtime(request: Request) -> JSONResponse:
    component = request.query_params.get("component", "")
    if component not in {"candidate", "discovery", "reconciliation", "notification"}:
        return JSONResponse({"error": "invalid component"}, status_code=400)
    try:
        snapshot = await _read_snapshot(request, component=component)
    except TimeoutError:
        snapshot = None
    if snapshot is None or not snapshot.available or snapshot.runtime is None:
        return JSONResponse(
            {"status": "unavailable", "reason": "runtime-evidence-unavailable"},
            status_code=503,
        )
    return JSONResponse(
        {"status": "available", "runtime": _runtime_json(snapshot.runtime)}
    )


async def fault_status(request: Request) -> JSONResponse:
    fault_id = request.path_params["fault_id"]
    try:
        snapshot = await _read_snapshot(request, fault_id=fault_id)
    except TimeoutError:
        snapshot = None
    if (
        snapshot is None
        or not snapshot.available
        or snapshot.projection is None
        or snapshot.history is None
        or snapshot.history.intent is None
    ):
        return JSONResponse(
            {
                "status": "unavailable",
                "reason": snapshot.reason if snapshot is not None else "authority-unavailable",
            },
            status_code=503,
        )
    projection = snapshot.projection
    history = snapshot.history
    lifecycle_values = [
        event.state.value for event in history.events if event.state is not None
    ]
    actions = [
        event.action.value
        for event in history.events
        if event.action is not None
    ]
    return JSONResponse(
        {
            "status": "available",
            "fault_id": fault_id,
            "active": projection.active,
            "state": projection.state.value if projection.state else None,
            "complete_history": history.valid,
            "event_count": len(history.events),
            "lifecycle": lifecycle_values[-32:],
            "actions": actions[-8:],
            "intent": {
                "kind": history.intent.kind.value,
                "call_class": history.intent.call_class.value,
                "target_key": history.intent.target_key,
                "parameter_digest": canonical_digest(dict(history.intent.parameters)),
                "ttl_ms": history.intent.ttl_ms,
                "runtime": _runtime_json(history.intent.runtime),
            },
        }
    )
