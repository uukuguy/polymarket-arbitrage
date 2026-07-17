from __future__ import annotations

import json
from pathlib import Path
import re


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    content = (root / "docs/status/climb/session-target.md").read_text()
    match = re.search(r"^target_value:\s*(.*)$", content, re.MULTILINE)
    raw = match.group(1).strip() if match else ""
    result = {
        "has_target": bool(raw),
        "met": False,
        "reason": "best-effort mode" if not raw else "target evaluation not configured",
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
