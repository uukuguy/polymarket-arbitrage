"""Evidence harness for Phase 05.1 production listener recovery.

This command never restarts or stops an application machine.  Listener mode
targets one PostgreSQL backend whose last statement is exactly
``LISTEN snapshot_complete``.  Poll mode is an assertion harness around two
operator-controlled L1 configuration changes; it never changes Fly secrets.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

import asyncpg

from polyarb.config import load_settings

L1_APP = "polyarb-l1"
L2_APP = "polyarb-l2"
L2_HEALTH_URL = "https://polyarb-l2.fly.dev/health"
MAX_RECOVERY_SECONDS = 180
POLL_PROOF_SECONDS = 60
SAMPLE_SECONDS = 5


class ProofFailure(RuntimeError):
    """An acceptance assertion failed without exposing credentials."""


@dataclass(frozen=True)
class MachineIdentity:
    machine_id: str
    instance_id: str | None
    created_at: str | None
    state: str

    def stable_key(self) -> tuple[str, str | None, str | None]:
        return self.machine_id, self.instance_id, self.created_at


@dataclass(frozen=True)
class Position:
    latest_snapshot: int
    committed_cursor: int


def _fly(*args: str) -> str:
    """Run flyctl through keychain auth; returned output must be non-secret."""
    env = dict(__import__("os").environ)
    env.pop("FLY_API_TOKEN", None)
    result = subprocess.run(
        ["flyctl", *args],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        raise ProofFailure(f"flyctl failed with exit code {result.returncode}")
    return result.stdout


def _machine_identity() -> MachineIdentity:
    rows = json.loads(_fly("machines", "list", "-a", L2_APP, "--json"))
    running = [row for row in rows if row.get("state") in {"started", "starting"}]
    if len(running) != 1:
        raise ProofFailure(f"expected one running L2 machine, observed {len(running)}")
    row = running[0]
    return MachineIdentity(
        machine_id=str(row["id"]),
        instance_id=row.get("instance_id"),
        created_at=row.get("created_at"),
        state=str(row["state"]),
    )


def _health() -> dict[str, Any]:
    request = urllib.request.Request(L2_HEALTH_URL, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        # Strict health may intentionally be 503 during the fault; its JSON is
        # still the evidence surface.
        body = exc.read()
    try:
        return json.loads(body)
    except (json.JSONDecodeError, UnboundLocalError) as exc:
        raise ProofFailure("L2 health did not return JSON") from exc


def _observed(health: dict[str, Any], key: str) -> Any:
    try:
        return health["checks"][key][0]["observedValue"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ProofFailure(f"health is missing required check {key}") from exc


def _notification_anchor(health: dict[str, Any]) -> float:
    value = _observed(health, "event_bus:last_notification_at")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProofFailure("last_notification_at has no numeric baseline")
    return float(value)


def _print_machine(prefix: str, machine: MachineIdentity) -> None:
    print(
        f"{prefix}_machine_id={machine.machine_id} "
        f"instance_id={machine.instance_id or 'unavailable'} "
        f"created_at={machine.created_at or 'unavailable'} state={machine.state}"
    )


async def _position(conn: asyncpg.Connection) -> Position:
    row = await conn.fetchrow(
        "SELECT "
        "(SELECT COALESCE(MAX(id), 0) FROM snapshots) AS latest_snapshot, "
        "COALESCE((SELECT last_snapshot_id FROM l2_event_cursor "
        "WHERE consumer=$1), 0) AS committed_cursor",
        "l2-candidate-refresh",
    )
    return Position(int(row["latest_snapshot"]), int(row["committed_cursor"]))


def _assert_same_machine(before: MachineIdentity, after: MachineIdentity) -> None:
    if before.stable_key() != after.stable_key():
        raise ProofFailure(
            "L2 machine identity/start anchor changed; no-restart proof is invalid"
        )


def _l1_event_bus_value() -> str:
    command = (
        "python -c \"import os; "
        "print(os.environ.get('POLYARB_EVENT_BUS_ENABLED', '<unset>'))\""
    )
    output = _fly("ssh", "console", "-a", L1_APP, "-C", command)
    values = [line.strip() for line in output.splitlines() if line.strip()]
    for value in reversed(values):
        if value in {"0", "1", "<unset>"}:
            return value
    raise ProofFailure("could not verify the L1 event-bus setting")


async def _listener_mode(conn: asyncpg.Connection) -> None:
    before_machine = _machine_identity()
    before_health = _health()
    reconnect_before = int(_observed(before_health, "event_bus:reconnect_count"))
    listener_before = _observed(before_health, "event_bus:connection_state")
    _print_machine("before", before_machine)
    print(f"before_listener={listener_before} reconnect_count={reconnect_before}")
    if listener_before != "listening":
        raise ProofFailure("listener baseline is not connected")

    rows = await conn.fetch(
        "SELECT pid FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() AND backend_type='client backend' "
        "AND query ~* '^\\s*LISTEN\\s+\"?snapshot_complete\"?\\s*;?\\s*$'"
    )
    if len(rows) != 1:
        raise ProofFailure(
            f"expected exactly one snapshot_complete LISTEN backend, found {len(rows)}"
        )
    listener_pid = int(rows[0]["pid"])
    print(f"identified_listener_backend_pid={listener_pid}")
    confirmation = input(f"Type TERMINATE {listener_pid} to terminate only this backend: ")
    if confirmation.strip() != f"TERMINATE {listener_pid}":
        raise ProofFailure("listener termination was not confirmed")

    try:
        terminated = await conn.fetchval("SELECT pg_terminate_backend($1)", listener_pid)
    except asyncpg.InsufficientPrivilegeError:
        print("HUMAN ACTION REQUIRED in the authenticated Supabase SQL editor:")
        print(f"  SELECT pg_terminate_backend({listener_pid});")
        input("Press Enter only after that single statement reports true: ")
        terminated = True
    if not terminated:
        raise ProofFailure("Postgres did not terminate the identified listener backend")

    deadline = time.monotonic() + MAX_RECOVERY_SECONDS
    while time.monotonic() < deadline:
        current = _health()
        listener = _observed(current, "event_bus:connection_state")
        reconnect = int(_observed(current, "event_bus:reconnect_count"))
        if listener == "listening" and reconnect > reconnect_before:
            after_machine = _machine_identity()
            _assert_same_machine(before_machine, after_machine)
            _print_machine("after", after_machine)
            print(f"after_listener={listener} reconnect_count={reconnect}")
            print("PASS listener: reconnect increased on the same L2 machine")
            return
        await asyncio.sleep(SAMPLE_SECONDS)
    raise ProofFailure("listener did not reconnect within 180 seconds")


async def _wait_for_poll_transition(
    conn: asyncpg.Connection,
    baseline: Position,
    notification_before: float,
) -> Position:
    snapshot_deadline = time.monotonic() + MAX_RECOVERY_SECONDS
    new_snapshot: int | None = None
    catchup_deadline: float | None = None
    while time.monotonic() < snapshot_deadline:
        current = await _position(conn)
        if new_snapshot is None and current.latest_snapshot > baseline.latest_snapshot:
            new_snapshot = current.latest_snapshot
            catchup_deadline = time.monotonic() + POLL_PROOF_SECONDS
            print(f"new_snapshot={new_snapshot}")
        if new_snapshot is not None and current.committed_cursor >= new_snapshot:
            notification_after_poll = _notification_anchor(_health())
            if notification_after_poll != notification_before:
                raise ProofFailure(
                    "last_notification_at changed during poll proof; timer causality unproven"
                )
            print(
                f"poll_cursor={current.committed_cursor} "
                f"last_notification_at={notification_after_poll!r}"
            )
            return current
        if catchup_deadline is not None and time.monotonic() > catchup_deadline:
            raise ProofFailure("cursor did not catch the new snapshot within 60 seconds")
        await asyncio.sleep(SAMPLE_SECONDS)
    raise ProofFailure("no newer normal L1 snapshot appeared within 180 seconds")


async def _poll_mode(conn: asyncpg.Connection) -> None:
    before_machine = _machine_identity()
    baseline = await _position(conn)
    notification_before = _notification_anchor(_health())
    _print_machine("before", before_machine)
    print(
        f"baseline_latest_snapshot={baseline.latest_snapshot} "
        f"baseline_cursor={baseline.committed_cursor} "
        f"last_notification_at={notification_before!r}"
    )
    if baseline.latest_snapshot != baseline.committed_cursor:
        raise ProofFailure("poll baseline is not caught up")
    if _l1_event_bus_value() != "0":
        raise ProofFailure(
            "L1 must report POLYARB_EVENT_BUS_ENABLED=0 before the poll proof"
        )

    print("L1 publish is disabled. Trigger one normal L1 snapshot without restarting L2.")
    input("Press Enter immediately after triggering that snapshot: ")
    recovered = await _wait_for_poll_transition(conn, baseline, notification_before)
    after_poll_machine = _machine_identity()
    _assert_same_machine(before_machine, after_poll_machine)
    print(
        f"PASS poll: cursor={recovered.committed_cursor}; notification anchor unchanged; "
        "no L2 restart"
    )

    print("RESTORE REQUIRED: set L1 POLYARB_EVENT_BUS_ENABLED=1 now.")
    print("Then trigger or wait for one later normal snapshot so NOTIFY can be observed.")
    input("Press Enter after restoration and the later snapshot trigger: ")
    if _l1_event_bus_value() != "1":
        raise ProofFailure("L1 publish restoration is not observable as value 1")

    restore_deadline = time.monotonic() + MAX_RECOVERY_SECONDS
    while time.monotonic() < restore_deadline:
        restored_anchor = _notification_anchor(_health())
        if restored_anchor != notification_before:
            final_machine = _machine_identity()
            _assert_same_machine(before_machine, final_machine)
            print(f"restored_last_notification_at={restored_anchor!r}")
            print("PASS restore: L1 publish is enabled and a later notification arrived")
            return
        await asyncio.sleep(SAMPLE_SECONDS)
    raise ProofFailure("no later notification arrived after restoring L1 publish")


async def _run(mode: str) -> None:
    settings = load_settings()
    dsn = settings.supabase_db_dsn.get_secret_value()
    if not dsn:
        raise ProofFailure("POLYARB_SUPABASE_DB_DSN is required")
    conn = await asyncpg.connect(dsn=dsn, command_timeout=15)
    try:
        if mode == "listener":
            await _listener_mode(conn)
        else:
            await _poll_mode(conn)
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("listener", "poll"))
    args = parser.parse_args()
    try:
        asyncio.run(_run(args.mode))
    except ProofFailure as exc:
        print(f"FAIL {args.mode}: {type(exc).__name__}: {exc}", file=sys.stderr)
        if args.mode == "poll":
            print(
                "SAFETY: verify L1 POLYARB_EVENT_BUS_ENABLED=1 before leaving "
                "this checkpoint.",
                file=sys.stderr,
            )
        return 1
    except (asyncpg.PostgresError, OSError) as exc:
        # External exception messages may contain host or database identifiers.
        # The type is enough to route diagnosis without risking credential output.
        print(f"FAIL {args.mode}: {type(exc).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
