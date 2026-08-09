"""External watchdog for the Fly resident Polywatch process group.

This program deliberately runs outside the ``cron`` machine it supervises.
It is used both by GitHub Actions and as the post-deploy gate.  A stopped
resident watcher cannot report its own absence, so a healthy L1 HTTP endpoint
is not sufficient evidence that alerting is alive.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

DEFAULT_APP = "polyarb-l1"
FLYCTL_TIMEOUT_S = 30


class FlyctlError(RuntimeError):
    """The independent control-plane probe or repair did not complete."""


@dataclass(frozen=True)
class CronAssessment:
    healthy: bool
    reason: str
    repair_ids: tuple[str, ...] = ()


def _run_flyctl(argv: list[str], **_kwargs: object) -> str:
    """Run flyctl without exposing its output or any credential to logs."""
    completed = subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        timeout=FLYCTL_TIMEOUT_S,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise FlyctlError(detail or f"flyctl exited {completed.returncode}")
    return completed.stdout


def _cron_machines(machines: Sequence[object]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for machine in machines:
        if not isinstance(machine, dict):
            continue
        config = machine.get("config")
        metadata = config.get("metadata") if isinstance(config, dict) else None
        if isinstance(metadata, dict) and metadata.get("fly_process_group") == "cron":
            result.append(machine)
    return result


def inspect_cron_machines(machines: Sequence[object]) -> CronAssessment:
    """Require exactly one cron machine and require it to be started."""
    cron = _cron_machines(machines)
    if len(cron) != 1:
        return CronAssessment(
            healthy=False,
            reason=f"expected exactly one cron machine; observed={len(cron)}",
        )
    machine = cron[0]
    machine_id = machine.get("id")
    state = machine.get("state")
    if not isinstance(machine_id, str) or not machine_id:
        return CronAssessment(False, "cron machine missing id")
    if state == "started":
        return CronAssessment(True, f"cron machine {machine_id} is started")
    return CronAssessment(
        healthy=False,
        reason=f"cron machine {machine_id} is {state!r}, expected 'started'",
        repair_ids=(machine_id,),
    )


def _list_cron_assessment(app: str) -> CronAssessment:
    raw = _run_flyctl(["flyctl", "machines", "list", "-a", app, "--json"])
    try:
        machines = json.loads(raw)
    except json.JSONDecodeError as error:
        raise FlyctlError(f"invalid flyctl machines JSON: {error.msg}") from error
    if not isinstance(machines, list):
        raise FlyctlError("flyctl machines JSON is not a list")
    return inspect_cron_machines(machines)


def _send_telegram(text: str) -> bool:
    """Best-effort incident delivery; caller treats failure as a failed gate."""
    token = os.environ.get("POLYARB_TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("POLYARB_TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("[resident-watchdog] Telegram credentials missing", file=sys.stderr)
        return False
    body = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage", data=body, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300
    except Exception as error:
        print(f"[resident-watchdog] Telegram failed: {error!r}", file=sys.stderr)
        return False


def _incident_message(*, app: str, assessment: CronAssessment, repair: str) -> str:
    return (
        "[L1 CRITICAL] Polywatch resident watchdog incident\n"
        f"app={app}\n"
        f"reason={assessment.reason}\n"
        f"start issued={repair}\n"
        "impact=resident monitoring may have been silent; external watchdog is active\n"
        "next=inspect Fly machine and Polywatch logs; verify the next 2-minute tick"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app", default=os.environ.get("POLYWATCH_FLY_APP", DEFAULT_APP))
    parser.add_argument("--repair", action="store_true", help="start a stopped cron machine")
    args = parser.parse_args(argv)

    try:
        before = _list_cron_assessment(args.app)
    except (FlyctlError, subprocess.TimeoutExpired) as error:
        message = (
            "[L1 CRITICAL] Polywatch external watchdog control-plane failure\n"
            f"app={args.app}\nerror={error!r}"
        )
        _send_telegram(message)
        print(message, file=sys.stderr)
        return 1

    if before.healthy:
        print(f"[resident-watchdog] healthy: {before.reason}")
        return 0

    repair = "not-attempted"
    if args.repair and before.repair_ids:
        try:
            for machine_id in before.repair_ids:
                _run_flyctl(["flyctl", "machines", "start", machine_id, "-a", args.app])
            after = _list_cron_assessment(args.app)
            repair = "ok" if after.healthy else f"issued-but-unverified ({after.reason})"
        except (FlyctlError, subprocess.TimeoutExpired) as error:
            repair = f"failed ({error!r})"

    delivered = _send_telegram(_incident_message(app=args.app, assessment=before, repair=repair))
    print(f"[resident-watchdog] incident: {before.reason}; start issued={repair}", file=sys.stderr)
    return 0 if repair == "ok" and delivered else 1


if __name__ == "__main__":
    raise SystemExit(main())
