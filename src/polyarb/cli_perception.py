"""Fail-closed cloud control client for read-model producer wake-ups."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import secrets
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_DEFAULT_BASE_URL = "https://polyarb-l1.fly.dev"
_ROUTES = {
    "build-market-map": "/control/market-map/build",
    "scan-neg-risk-map": "/control/neg-risk/scan",
    "queue-discovery": "/control/perception/discovery",
    "queue-reconciliation": "/control/perception/reconciliation",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Queue one normal cloud perception cycle")
    parser.add_argument("command", choices=sorted(_ROUTES))
    parser.add_argument("--base-url", default=os.getenv("POLYARB_DAEMON_URL", _DEFAULT_BASE_URL))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    secret = os.getenv("POLYARB_SCAN_SHARED_SECRET", "")
    if not secret:
        print("ERROR: POLYARB_SCAN_SHARED_SECRET is required for cloud controls", file=sys.stderr)
        return 2
    body = b"{}"
    path = _ROUTES[args.command]
    headers = {"Content-Type": "application/json"}
    if args.command.startswith("queue-"):
        timestamp = str(int(time.time()))
        nonce = secrets.token_hex(16)
        canonical = b"\n".join(
            (timestamp.encode(), nonce.encode(), b"POST", path.encode(), body)
        )
        signature = hmac.new(
            secret.encode("utf-8"), canonical, hashlib.sha256
        ).hexdigest()
        headers.update(
            {
                "X-Perception-Timestamp": timestamp,
                "X-Perception-Nonce": nonce,
                "X-Signature": f"sha256={signature}",
            }
        )
    else:
        signature = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        headers["X-Signature"] = signature
    request = Request(
        f"{args.base_url.rstrip('/')}{path}",
        data=body,
        method="POST",
        headers=headers,
    )
    try:
        with urlopen(request, timeout=15) as response:  # noqa: S310 - fixed configured daemon URL
            sys.stdout.write(response.read().decode("utf-8"))
            sys.stdout.write("\n")
            return 0
    except HTTPError as error:
        print(error.read().decode("utf-8", errors="replace"), file=sys.stderr)
        return 1
    except URLError as error:
        print(f"ERROR: cloud control request failed: {error.reason}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
