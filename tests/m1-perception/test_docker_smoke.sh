#!/usr/bin/env bash
set -euo pipefail
# Docker smoke test: build image + import check + /health probe + non-root verify
# Plan 04 Wave 0 — RED before Dockerfile exists, GREEN after Task 2.
#
# Exit codes:
#   0  = all checks passed (docker smoke contract satisfied)
#   1  = a check failed (contract violated)
#   77 = skip (docker not available or not running — autotools convention)

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not available; skipping" >&2
  exit 77
fi
if ! docker info >/dev/null 2>&1; then
  echo "docker daemon not running; skipping" >&2
  exit 77
fi

IMG=polyarb-l1-test:$$
trap "docker rmi -f $IMG >/dev/null 2>&1 || true" EXIT

# 1. Build
docker build -t "$IMG" . || { echo "FAIL: docker build" >&2; exit 1; }

# 2. Import smoke
docker run --rm "$IMG" python -c "import polyarb; print('import OK')" || { echo "FAIL: import polyarb" >&2; exit 1; }

# 3. Run + health probe
CID=$(docker run -d -p 8088:8080 -e POLYARB_ALLOW_EMPTY_SECRET=1 -e POLYARB_ALLOW_EXTERNAL_PATHS=1 "$IMG")
trap "docker rm -f $CID >/dev/null 2>&1; docker rmi -f $IMG >/dev/null 2>&1 || true" EXIT
sleep 8
curl -fsS http://127.0.0.1:8088/health | python -c "import json,sys; d=json.load(sys.stdin); assert d['status'] in ('pass','warn','fail'), f'bad status: {d}'; print('health OK', d['status'])" || { echo "FAIL: /health" >&2; exit 1; }

# 4. Non-root verify
UID_OUT=$(docker exec "$CID" id -u)
[[ "$UID_OUT" == "10001" ]] || { echo "FAIL: not UID 10001 (got $UID_OUT)" >&2; exit 1; }

echo "docker smoke OK"
