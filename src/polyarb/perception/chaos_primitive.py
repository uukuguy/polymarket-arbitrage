"""Image-safe, narrowly scoped M1 perception fault primitives."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

_RELEASE_RE = re.compile(r"[0-9a-f]{40}")
_WORKER_COMPONENTS = frozenset({"candidate", "discovery", "reconciliation"})
_TERMINATABLE_FAULTS = {
    "candidate": "candidate-exit",
    "discovery": "discovery-exit",
}


class PrimitiveRefusedError(RuntimeError):
    """The requested primitive is not proven to have exactly one safe target."""


@dataclass(frozen=True)
class WorkerProcess:
    pid: int
    ppid: int
    component: str


def _argv(path: Path) -> tuple[str, ...]:
    try:
        payload = path.read_bytes()
        return tuple(
            item.decode("utf-8", "strict")
            for item in payload.split(b"\0")
            if item
        )
    except (OSError, UnicodeDecodeError):
        return ()


def _ppid(path: Path) -> int:
    try:
        payload = path.read_text()
        suffix = payload[payload.rindex(")") + 1 :].split()
        if len(suffix) < 2:
            raise ValueError
        return int(suffix[1])
    except (OSError, ValueError) as exc:
        raise PrimitiveRefusedError("invalid-worker-parent") from exc


def _is_module_command(argv: tuple[str, ...], module: str, *args: str) -> bool:
    return len(argv) == 3 + len(args) and argv[1:] == ("-m", module, *args)


def locate_worker(proc_root: Path, component: str) -> WorkerProcess:
    """Resolve one exact direct child of the PID-1 M1 daemon."""
    if component not in _WORKER_COMPONENTS:
        raise PrimitiveRefusedError("unsupported-component")
    daemon_argv = _argv(proc_root / "1" / "cmdline")
    if not _is_module_command(daemon_argv, "polyarb.daemon.main"):
        raise PrimitiveRefusedError("daemon-command-drift")

    matches: list[int] = []
    try:
        entries = tuple(proc_root.iterdir())
    except OSError as exc:
        raise PrimitiveRefusedError("proc-unavailable") from exc
    for entry in entries:
        if not entry.name.isdigit():
            continue
        argv = _argv(entry / "cmdline")
        if _is_module_command(
            argv,
            "polyarb.perception.worker_cli",
            component,
        ):
            matches.append(int(entry.name))
    if len(matches) != 1:
        raise PrimitiveRefusedError(f"worker-count:{len(matches)}")

    pid = matches[0]
    ppid = _ppid(proc_root / str(pid) / "stat")
    if ppid != 1:
        raise PrimitiveRefusedError(f"worker-parent:{ppid}")
    return WorkerProcess(pid=pid, ppid=ppid, component=component)


def terminate_worker(
    proc_root: Path,
    *,
    component: str,
    expected_pid: int,
    expected_release: str,
    authorization: str,
    environ: Mapping[str, str],
    kill: Callable[[int, signal.Signals], None] = os.kill,
) -> WorkerProcess:
    """SIGTERM only an exact authorized producer worker."""
    fault_id = _TERMINATABLE_FAULTS.get(component)
    if fault_id is None:
        raise PrimitiveRefusedError("unsupported-component")
    if _RELEASE_RE.fullmatch(expected_release) is None:
        raise PrimitiveRefusedError("invalid-release")
    if environ.get("POLYARB_RELEASE_ID") != expected_release:
        raise PrimitiveRefusedError("release-drift")
    expected_authorization = (
        f"fault:{fault_id}:{expected_release}:{expected_pid}"
    )
    if authorization != expected_authorization:
        raise PrimitiveRefusedError("invalid-authorization")

    worker = locate_worker(proc_root, component)
    if worker.pid != expected_pid:
        raise PrimitiveRefusedError(f"pid-drift:{worker.pid}")
    kill(worker.pid, signal.SIGTERM)
    return worker


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    locate = subparsers.add_parser("locate")
    locate.add_argument("--component", required=True, choices=sorted(_WORKER_COMPONENTS))
    terminate = subparsers.add_parser("terminate")
    terminate.add_argument("--component", required=True)
    terminate.add_argument("--expected-pid", required=True, type=int)
    terminate.add_argument("--expected-release", required=True)
    terminate.add_argument("--authorization", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "locate":
            worker = locate_worker(Path("/proc"), args.component)
            payload = {"action": "locate", **asdict(worker)}
        else:
            worker = terminate_worker(
                Path("/proc"),
                component=args.component,
                expected_pid=args.expected_pid,
                expected_release=args.expected_release,
                authorization=args.authorization,
                environ=os.environ,
            )
            payload = {
                "action": "sigterm",
                "signal": signal.SIGTERM.name,
                **asdict(worker),
            }
        print(json.dumps(payload, sort_keys=True))
        return 0
    except PrimitiveRefusedError as exc:
        print(f"primitive-refused: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
