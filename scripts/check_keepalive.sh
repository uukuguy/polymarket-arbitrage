#!/usr/bin/env bash
# check_keepalive.sh — list last N runs of supabase-keepalive workflow.
#
# Surfaces silent failures + drift (Phase 02 L8 precedent: a GHA workflow
# can fail silently for days if nobody eyeballs the runs list).
#
# Usage:
#   bash scripts/check_keepalive.sh         # last 7 runs (default)
#   bash scripts/check_keepalive.sh 14      # last 14 runs
#
# Exit codes:
#   0 = ≤1 failure in window (healthy)
#   1 = ≥2 failures in window (investigate)
#   2 = gh CLI missing / not authenticated

set -euo pipefail

WORKFLOW="supabase-keepalive.yml"
LIMIT="${1:-7}"

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: gh CLI not installed (https://cli.github.com/)" >&2
  exit 2
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "ERROR: gh CLI not authenticated — run 'gh auth login'" >&2
  exit 2
fi

echo "=== Last ${LIMIT} runs of ${WORKFLOW} ==="
gh run list --workflow "${WORKFLOW}" --limit "${LIMIT}" \
  --json databaseId,conclusion,createdAt,displayTitle \
  --jq '.[] | "\(.createdAt) \(.conclusion // "in_progress") #\(.databaseId) — \(.displayTitle)"'

echo ""
echo "=== Summary ==="
failure_count=$(gh run list --workflow "${WORKFLOW}" --limit "${LIMIT}" \
  --json conclusion --jq '[.[] | select(.conclusion == "failure")] | length')
success_count=$(gh run list --workflow "${WORKFLOW}" --limit "${LIMIT}" \
  --json conclusion --jq '[.[] | select(.conclusion == "success")] | length')

echo "failures: ${failure_count} / successes: ${success_count} / window: ${LIMIT}"

if [ "${failure_count}" -gt "1" ]; then
  echo "::warning:: ${failure_count} failures in last ${LIMIT} runs — investigate before assuming Supabase healthy"
  echo "Phase 03 D-01 trigger: if Supabase pauses, consider upgrading to Pro \$25/mo"
  exit 1
fi

echo "OK: keepalive healthy"
