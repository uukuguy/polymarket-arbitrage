from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import yaml

from tools.climb.regen_tree import regenerate


RUN_FIELDS = [
    "run_id",
    "cycle",
    "session",
    "hypothesis_id",
    "paradigm",
    "parent_run",
    "pushed_at",
    "local_score",
    "planning",
    "unit",
    "integration",
    "cli",
    "restart",
    "push_decision",
    "decision_reason",
    "verdict",
    "cost_h",
    "manifest_path",
]


def _atomic_write(path: Path, content: str) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(content)
    temp.replace(path)


def sync_cycle(state_dir: Path, completed_run: dict) -> None:
    manifest = json.loads(Path(completed_run["manifest_path"]).read_text())
    evaluation = json.loads(Path(completed_run["local_eval_path"]).read_text())
    session_path = state_dir / "session-state.json"
    session = json.loads(session_path.read_text())
    hypotheses_path = state_dir / "hypotheses.yaml"
    hypotheses = yaml.safe_load(hypotheses_path.read_text())

    runs_path = state_dir / "runs.csv"
    with runs_path.open(newline="") as handle:
        existing = list(csv.DictReader(handle))
    if any(row["run_id"] == completed_run["run_id"] for row in existing):
        raise ValueError(f"duplicate run_id: {completed_run['run_id']}")

    cycle = int(session.get("last_cycle", 0)) + 1
    subscores = evaluation["subscores"]
    verdict = (
        "confirmed"
        if completed_run["decision"] == "PUSH" and evaluation["total"] == 100.0
        else "falsified"
    )
    row = {
        "run_id": completed_run["run_id"],
        "cycle": cycle,
        "session": session["session"],
        "hypothesis_id": manifest["hypothesis_id"],
        "paradigm": manifest["paradigm"],
        "parent_run": completed_run.get("parent_run", ""),
        "pushed_at": completed_run.get("pushed_at", ""),
        "local_score": evaluation["total"],
        "planning": subscores.get("planning", 0.0),
        "unit": subscores.get("unit", 0.0),
        "integration": subscores.get("integration", 0.0),
        "cli": subscores.get("cli", 0.0),
        "restart": subscores.get("restart", 0.0),
        "push_decision": completed_run["decision"],
        "decision_reason": completed_run["reason"],
        "verdict": verdict,
        "cost_h": completed_run["cost_h"],
        "manifest_path": completed_run["manifest_path"],
    }
    with runs_path.open("a", newline="") as handle:
        csv.DictWriter(handle, fieldnames=RUN_FIELDS).writerow(row)

    hypothesis = next(
        item
        for item in hypotheses["hypotheses"]
        if item["id"] == manifest["hypothesis_id"]
    )
    hypothesis["status"] = verdict
    hypothesis.setdefault("results", []).append(
        {
            "session": session["session"],
            "cycle": cycle,
            "run": completed_run["run_id"],
            "local": evaluation["total"],
            "local_per_task": subscores,
            "online": None,
            "verdict": verdict,
            "decision_reason": completed_run["reason"],
        }
    )
    _atomic_write(
        hypotheses_path,
        yaml.safe_dump(hypotheses, sort_keys=False, allow_unicode=True),
    )

    session.update(
        {
            "last_cycle": cycle,
            "in_flight": None,
            "next_action": "rank next pending hypothesis",
        }
    )
    _atomic_write(
        session_path,
        json.dumps(session, indent=2, sort_keys=True) + "\n",
    )
    regenerate(state_dir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--decision", choices=("PUSH", "SKIP"), required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--cost-h", type=float, default=0.0)
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    sync_cycle(
        args.state_dir.resolve(),
        {
            "run_id": run_dir.name,
            "run_dir": str(run_dir),
            "manifest_path": str(run_dir / "manifest.json"),
            "local_eval_path": str(run_dir / "local-eval.json"),
            "decision": args.decision,
            "reason": args.reason,
            "cost_h": args.cost_h,
        },
    )


if __name__ == "__main__":
    main()
