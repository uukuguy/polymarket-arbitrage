#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: tools/climb/push.sh <run_dir>" >&2
  exit 2
fi

uv run python - "$1" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys

run_dir = Path(sys.argv[1]).resolve()
manifest = json.loads((run_dir / "manifest.json").read_text())
evaluation = json.loads((run_dir / "local-eval.json").read_text())
artifact = {
    "external_submission": False,
    "git_head": manifest["git_head"],
    "hypothesis_id": manifest["hypothesis_id"],
    "local_score": evaluation["total"],
}
path = run_dir / "verification-artifact.json"
path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
print(path)
PY
