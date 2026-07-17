#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: tools/climb/eval-local.sh <run_dir>" >&2
  exit 2
fi

exec uv run python -m tools.climb.eval_local "$1"
