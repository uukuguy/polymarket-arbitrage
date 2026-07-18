#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^H-[0-9]{3}$ ]]; then
  echo "usage: tools/climb/cycle.sh H-NNN" >&2
  exit 2
fi

ROOT="$(git rev-parse --show-toplevel)"
RUN_DIR="$(tools/climb/train.sh "$1")"

set +e
tools/climb/eval-local.sh "$RUN_DIR"
EVAL_STATUS=$?
set -e

if [[ $EVAL_STATUS -eq 0 ]]; then
  DECISION="PUSH"
  REASON="all local gates passed"
  # This script is only reached after eval-local exits 0. The recorder is a
  # no-op for legacy paradigms and performs exactly one read-only GET for H-008.
  uv run python tools/climb/record-production-evidence.py "$RUN_DIR"
  tools/climb/push.sh "$RUN_DIR"
else
  DECISION="SKIP"
  REASON="one or more local gates failed"
fi

uv run python -m tools.climb.cycle \
  --state-dir "$ROOT/docs/status/climb" \
  --run-dir "$RUN_DIR" \
  --decision "$DECISION" \
  --reason "$REASON"

uv run python tools/climb/check-target.py
