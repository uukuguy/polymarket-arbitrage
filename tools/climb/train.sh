#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^H-[0-9]{3}$ ]]; then
  echo "usage: tools/climb/train.sh H-NNN" >&2
  exit 2
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
HYPOTHESIS_ID="$1"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
ARTIFACT_ROOT="${CLIMB_ARTIFACT_DIR:-$REPO_ROOT/runs/climb}"
HYPOTHESIS_SLUG="$(printf '%s' "$HYPOTHESIS_ID" | tr '[:upper:]' '[:lower:]')"
RUN_DIR="$ARTIFACT_ROOT/${STAMP}-${HYPOTHESIS_SLUG}"

mkdir -p "$RUN_DIR"
uv run python - "$REPO_ROOT" "$HYPOTHESIS_ID" "$RUN_DIR" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import yaml

root = Path(sys.argv[1])
hypothesis_id = sys.argv[2]
run_dir = Path(sys.argv[3])
state = yaml.safe_load((root / "docs/status/climb/hypotheses.yaml").read_text())
hypothesis = next(
    (item for item in state["hypotheses"] if item["id"] == hypothesis_id),
    None,
)
if hypothesis is None:
    raise SystemExit(f"unknown hypothesis: {hypothesis_id}")
git_head = subprocess.check_output(
    ["git", "rev-parse", "HEAD"], cwd=root, text=True
).strip()
manifest = {
    "hypothesis_id": hypothesis_id,
    "paradigm": hypothesis["parent_paradigm"],
    "git_head": git_head,
    "status": "ready-for-eval",
}
(run_dir / "manifest.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n"
)
PY

printf '%s\n' "$RUN_DIR"
