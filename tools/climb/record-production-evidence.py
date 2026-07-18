#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.climb.cycle import record_production_evidence_after_gates


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Record the single post-gate opportunity-feed diagnostic."
    )
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    manifest = json.loads((run_dir / "manifest.json").read_text())
    record_production_evidence_after_gates(
        run_dir,
        manifest,
    )


if __name__ == "__main__":
    main()
