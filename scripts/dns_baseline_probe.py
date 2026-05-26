"""DNS baseline probe — Fly support ticket evidence collection.

Phase 03.1-04 D-01 modify C — collect node-level baseline data on the
EAI_NODATA / EAI_AGAIN chronic DNS failures (Sentry issue 121111789,
6 days / 3 occurrences). Outputs JSON-lines to stdout (one per probe
attempt) so ``flyctl logs`` aggregation is trivial.

Local smoke:
    make dns-baseline-probe

Production cron (future Polywatch plan):
    Launch via Fly scheduled machines or a sidecar process inside polyarb-l1.

Design notes:
- Pure stdlib (socket / json / time) — no project imports, so the same script
  can run inside the polyarb-l1 container image without pulling polyarb.
- One JSON-line per attempt + one summary JSON-line at the end. Failure mode
  classified by errno when available (EAI_NODATA = -5, EAI_AGAIN = -3).
- Exit code: 0 if all probes succeed, 1 if any fail. Cron-friendly.
- Default cadence: 30 probes × 3 hosts × 2s interval = ~3 min total runtime.
  Tunable via env vars POLYARB_DNS_PROBE_N / POLYARB_DNS_PROBE_INTERVAL_S.
"""
from __future__ import annotations

import json
import os
import socket
import sys
import time
from datetime import datetime, timezone

HOSTS = [
    "gamma-api.polymarket.com",
    "clob.polymarket.com",
    "data-api.polymarket.com",
]
N_PROBES = int(os.environ.get("POLYARB_DNS_PROBE_N", "30"))
INTERVAL_S = float(os.environ.get("POLYARB_DNS_PROBE_INTERVAL_S", "2.0"))


def probe(host: str) -> dict:
    """Resolve hostname once; return JSON-serializable result dict."""
    t0 = time.monotonic()
    try:
        ip = socket.gethostbyname(host)
        latency_ms = (time.monotonic() - t0) * 1000
        return {
            "ts": datetime.now(timezone.utc).isoformat(),
            "host": host,
            "ok": True,
            "ip": ip,
            "latency_ms": round(latency_ms, 2),
        }
    except OSError as e:
        return {
            "ts": datetime.now(timezone.utc).isoformat(),
            "host": host,
            "ok": False,
            "errno": getattr(e, "errno", None),
            "msg": str(e)[:200],
        }


def main() -> int:
    total = 0
    failed = 0
    by_host_failed: dict[str, int] = {h: 0 for h in HOSTS}
    for _ in range(N_PROBES):
        for host in HOSTS:
            result = probe(host)
            print(json.dumps(result), flush=True)
            total += 1
            if not result["ok"]:
                failed += 1
                by_host_failed[host] += 1
        time.sleep(INTERVAL_S)
    rate = failed / total if total else 0.0
    summary = {
        "summary": True,
        "total": total,
        "failed": failed,
        "failure_rate": round(rate, 4),
        "failed_by_host": by_host_failed,
    }
    print(json.dumps(summary), flush=True)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
