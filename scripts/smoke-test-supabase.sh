#!/usr/bin/env bash
# Smoke test: POST /scan with HMAC signature against running daemon.
# Note: filename has historical "supabase" prefix but this script tests the
# /scan HMAC + recipe engine, not Supabase directly. Renaming deferred to
# avoid breaking any in-flight shell history pointing here.
#
# Prerequisites:
# - daemon running:        make daemon-run-local
# - .env has POLYARB_SCAN_SHARED_SECRET
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if [ ! -f .env ]; then
  echo "ERROR: .env not found at $PROJECT_ROOT/.env" >&2
  exit 1
fi
set -a; . ./.env; set +a

if [ -z "${POLYARB_SCAN_SHARED_SECRET:-}" ]; then
  echo "ERROR: POLYARB_SCAN_SHARED_SECRET empty in .env" >&2
  exit 1
fi

PORT="${POLYARB_HTTP_PORT:-19080}"
BODY='{"recipe_name":"thick-but-slippery","params":{}}'
SIG="$(printf "%s" "$BODY" | openssl dgst -sha256 -hmac "$POLYARB_SCAN_SHARED_SECRET" -hex | awk '{print $NF}')"

echo "SIG computed (first 8 chars): ${SIG:0:8}..."
curl -i -X POST "http://localhost:${PORT}/scan" \
  -H "X-Signature: sha256=$SIG" \
  -H "Content-Type: application/json" \
  -d "$BODY"
