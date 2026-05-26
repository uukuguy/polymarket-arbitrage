#!/usr/bin/env bash
# fly_secrets_sync.sh — Push .env to BOTH polyarb-l1 + polyarb-l2 idempotently.
#
# Phase 03 Plan 02 — Phase 02.1 D-22 invariant: same SCAN_SHARED_SECRET on
# both apps (single shared secret, not separate L1/L2 minting).
#
# Threat: T-03-02-01 — bash xtrace MUST stay disabled (it would leak secret
# values to terminal/log). We only echo KEY names, never values.
#
# Threat: T-03-02-05 — repudiation. We echo timestamp + KEY names + apps
# touched; user can tee to scripts/.secrets-sync-log (gitignored) for audit.
#
# Usage:
#   bash scripts/fly_secrets_sync.sh              # sync .env → both apps
#   ENV_FILE=.env.staging bash scripts/...        # use a different env file
#   DRY_RUN=1 bash scripts/fly_secrets_sync.sh    # preview, no flyctl side effect

set -euo pipefail

# Phase 03.1 Plan 03 (GAP-4): prevent .env-shadowing of keychain Fly token.
# Lesson from Phase 03 Inj L2-2 cleanup: when .env contains an L1-only
# FLY_API_TOKEN (or stale token), flyctl picks it up via process env and
# silently shadows the correct keychain credential, producing misleading
# "App not found" errors against sibling apps (polyarb-l2 in the precedent).
# Force flyctl to fall back to the keychain by unsetting any inherited
# token at the very top of this script. See feedback memory
# `feedback_fly-api-token-shadowing-2026-05.md` for the lived precedent.
unset FLY_API_TOKEN

ENV_FILE="${ENV_FILE:-.env}"
APPS=("polyarb-l1" "polyarb-l2")
DRY_RUN="${DRY_RUN:-0}"

if [ ! -f "${ENV_FILE}" ]; then
  echo "ERROR: ${ENV_FILE} not found" >&2
  echo "Hint: ensure you are in the repo root, or set ENV_FILE=<path>" >&2
  exit 1
fi

# Filter comments (^#) + blank lines + lines missing = (malformed) from .env.
# This is the 03-PATTERNS.md File 5 Gotcha — flyctl secrets set would otherwise
# treat "# comment" as a literal KEY and fail with a confusing parse error.
# Also skip FLY_API_TOKEN (host-side only, container does not need it; its value
# can contain commas/spaces that confuse downstream tr-based splitters).
SECRETS_RAW="$(grep -v '^#' "${ENV_FILE}" | grep -v '^$' | grep '=' | grep -v '^FLY_API_TOKEN=' || true)"

if [ -z "${SECRETS_RAW}" ]; then
  echo "ERROR: no secrets found in ${ENV_FILE} after comment/blank filter" >&2
  exit 1
fi

SECRET_KEYS="$(echo "${SECRETS_RAW}" | awk -F= '{print $1}')"
SECRET_COUNT="$(echo "${SECRET_KEYS}" | wc -l | tr -d ' ')"

echo "=== fly_secrets_sync — $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo "Source:  ${ENV_FILE}"
echo "Apps:    ${APPS[*]}"
echo "Count:   ${SECRET_COUNT} secrets"
echo "Keys (values redacted — T-03-02-01):"
echo "${SECRET_KEYS}" | sed 's/^/  - /'
echo ""

if [ "${DRY_RUN}" = "1" ]; then
  echo "DRY_RUN=1 — skipping actual flyctl secrets set"
  exit 0
fi

# Require flyctl on PATH (avoid cryptic "command not found" mid-sync).
if ! command -v flyctl >/dev/null 2>&1; then
  echo "ERROR: flyctl not on PATH. Install via 'brew install flyctl' or curl -L https://fly.io/install.sh | sh" >&2
  exit 1
fi

for APP in "${APPS[@]}"; do
  echo "=== Syncing → ${APP} ==="
  # --stage = no machine restart per secret; final `secrets deploy` triggers
  # a single atomic restart at the end (vs N restarts mid-sync).
  # Pipe KEY=VALUE pairs via stdin (one per line) — flyctl reads them safely
  # even when values contain whitespace, commas, or quoted multiline content.
  # (Prior tr '\n' ' ' word-split broke on values like FLY_API_TOKEN='FlyV1 fm2_...,fm2_...'.)
  printf '%s\n' "${SECRETS_RAW}" | flyctl secrets import -a "${APP}" --stage
  echo "Staged ${SECRET_COUNT} secrets. Applying…"
  flyctl secrets deploy -a "${APP}"
  echo "${APP}: ${SECRET_COUNT} secrets applied."
  echo ""
done

echo "=== Done — ${SECRET_COUNT} secrets synced to all ${#APPS[@]} apps ==="
