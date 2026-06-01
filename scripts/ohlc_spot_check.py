"""Print L3 freshness anchors from L2 daemon /health (Phase 05 Plan 05-05).

Used by `make ohlc-spot-check`. Reads JSON on stdin and surfaces the 3
critical L3 anchors so the operator can confirm the promoter is alive
and OHLC views have fresh top-of-book source data.

Usage (inside Makefile):
    curl -sS $URL/health | uv run python scripts/ohlc_spot_check.py
"""

from __future__ import annotations

import json
import sys
from typing import Any


def _extract(checks: dict[str, Any], key: str, field: str) -> Any:
    """Return checks[key][0][field] or '?' if missing."""
    entry = checks.get(key)
    if not isinstance(entry, list) or not entry:
        return "?"
    first = entry[0]
    if not isinstance(first, dict):
        return "?"
    return first.get(field, "?")


def main() -> int:
    try:
        d = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(f"ABORT: not valid JSON on stdin ({e})", file=sys.stderr)
        return 1

    checks = d.get("checks", {})
    print(f"l3:active_count = {_extract(checks, 'l3:active_count', 'observedValue')}")
    print(f"l3:last_promote_at_s = {_extract(checks, 'l3:last_promote_at_s', 'output')}")
    print(
        "l3:last_book_levels_write_at_s = "
        f"{_extract(checks, 'l3:last_book_levels_write_at_s', 'output')}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
