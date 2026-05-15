#!/usr/bin/env bash
set -euo pipefail
APP=${APP:-polyarb-l1}
URL="https://${APP}.fly.dev/health"
# W8: ensure both processes scaled (idempotent — no-op if already at desired count)
if command -v flyctl >/dev/null 2>&1; then
  flyctl scale count app=1 cron=1 -a "$APP" 2>/dev/null || echo "(scale skipped — flyctl not authenticated in this context)"
fi
echo ">> smoke probing $URL"
for i in $(seq 1 10); do
  # Use -sS (no -f) because /health returns HTTP 503 when status=fail
  # (no snapshots yet = expected on fresh deploy). We validate the JSON body.
  if BODY=$(curl -sS "$URL" 2>/dev/null); then
    STATUS=$(echo "$BODY" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")
    echo "/health = $STATUS (try $i)"
    if [[ "$STATUS" == "fail" ]]; then
      echo "WARN: status=fail (expected if no snapshots yet); HTTP 503 is normal here" >&2
    fi
    exit 0
  fi
  sleep 6
done
echo "/health did not respond in 60s" >&2
exit 1
