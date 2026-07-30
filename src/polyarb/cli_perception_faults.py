"""CLI for the separately authorized, disabled-by-default fault API."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import secrets
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

_DEFAULT_BASE_URL = "https://polyarb-l1.fly.dev"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scoped upstream fault controls")
    parser.add_argument(
        "--base-url", default=os.getenv("POLYARB_DAEMON_URL", _DEFAULT_BASE_URL)
    )
    commands = parser.add_subparsers(dest="command", required=True)
    runtime = commands.add_parser("runtime")
    runtime.add_argument(
        "--component",
        required=True,
        choices=("candidate", "discovery", "reconciliation", "notification"),
    )
    arm = commands.add_parser("arm")
    arm.add_argument("--intent", type=Path, required=True)
    cleanup = commands.add_parser("cleanup")
    cleanup.add_argument("--fault-id", required=True)
    finalize = commands.add_parser("finalize")
    finalize.add_argument("--fault-id", required=True)
    finalize.add_argument("--artifact", type=Path, required=True)
    finalize.add_argument("--expected-release", required=True)
    status = commands.add_parser("status")
    status.add_argument("--fault-id", required=True)
    return parser


def _signed_headers(path: str, body: bytes) -> dict[str, str]:
    ordinary_secret = os.getenv("POLYARB_SCAN_SHARED_SECRET", "")
    fault_secret = os.getenv("POLYARB_UPSTREAM_FAULT_CONTROL_SECRET", "")
    if not ordinary_secret or not fault_secret:
        raise ValueError(
            "POLYARB_SCAN_SHARED_SECRET and "
            "POLYARB_UPSTREAM_FAULT_CONTROL_SECRET are required"
        )
    timestamp = str(int(time.time()))
    ordinary_nonce = secrets.token_hex(16)
    fault_nonce = secrets.token_hex(16)
    ordinary = b"\n".join(
        (timestamp.encode(), ordinary_nonce.encode(), b"POST", path.encode(), body)
    )
    fault = b"\n".join(
        (
            b"polyarb-fault-v1",
            timestamp.encode(),
            fault_nonce.encode(),
            b"POST",
            path.encode(),
            body,
        )
    )
    return {
        "Content-Type": "application/json",
        "X-Perception-Timestamp": timestamp,
        "X-Perception-Nonce": ordinary_nonce,
        "X-Signature": hmac.new(
            ordinary_secret.encode(), ordinary, hashlib.sha256
        ).hexdigest(),
        "X-Fault-Timestamp": timestamp,
        "X-Fault-Nonce": fault_nonce,
        "X-Fault-Signature": hmac.new(
            fault_secret.encode(), fault, hashlib.sha256
        ).hexdigest(),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    base = args.base_url.rstrip("/")
    if args.command == "runtime":
        path = "/perception/faults/runtime?" + urlencode({"component": args.component})
        body = None
        headers: dict[str, str] = {}
        method = "GET"
    elif args.command == "status":
        path = f"/perception/faults/{args.fault_id}"
        body = None
        headers = {}
        method = "GET"
    elif args.command == "arm":
        path = "/control/perception/faults/arm"
        try:
            body = args.intent.read_bytes()
        except OSError as exc:
            print(f"ERROR: cannot read intent: {exc}", file=sys.stderr)
            return 2
        method = "POST"
        try:
            headers = _signed_headers(path, body)
        except ValueError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
    elif args.command == "cleanup":
        path = "/control/perception/faults/cleanup"
        body = json.dumps(
            {"fault_id": args.fault_id}, separators=(",", ":")
        ).encode()
        method = "POST"
        try:
            headers = _signed_headers(path, body)
        except ValueError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
    else:
        path = f"/control/perception/faults/{args.fault_id}/finalize"
        try:
            artifact = json.loads(args.artifact.read_text())
            if not isinstance(artifact, dict):
                raise ValueError("artifact root must be an object")
            runtime = artifact.get("runtime")
            if (
                not isinstance(runtime, dict)
                or runtime.get("release_id") != args.expected_release
                or len(args.expected_release) != 40
                or any(character not in "0123456789abcdef" for character in args.expected_release)
            ):
                raise ValueError("artifact release does not match expected release")
            body = json.dumps(
                {"artifact": artifact},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode()
            headers = _signed_headers(path, body)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            print(f"ERROR: cannot read finalizer artifact: {exc}", file=sys.stderr)
            return 2
        method = "POST"
    request = Request(base + path, data=body, method=method, headers=headers)
    try:
        with urlopen(request, timeout=10) as response:  # noqa: S310 - operator URL
            payload = json.loads(response.read())
    except HTTPError as exc:
        print(exc.read().decode(errors="replace"), file=sys.stderr)
        return 1
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"ERROR: fault control request failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
