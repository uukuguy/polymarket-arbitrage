"""Fail-closed cloud control client for read-model producer wake-ups."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_DEFAULT_BASE_URL = "https://polyarb-l1.fly.dev"
_ROUTES = {
    "build-market-map": "/control/market-map/build",
    "scan-neg-risk-map": "/control/neg-risk/scan",
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
    signature = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    request = Request(
        f"{args.base_url.rstrip('/')}{_ROUTES[args.command]}",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "X-Signature": signature},
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
