from __future__ import annotations

import csv
import json
from pathlib import Path

import yaml


def regenerate(state_dir: Path) -> str:
    with (state_dir / "runs.csv").open(newline="") as handle:
        runs = list(csv.DictReader(handle))
    hypotheses = yaml.safe_load((state_dir / "hypotheses.yaml").read_text())
    session = json.loads((state_dir / "session-state.json").read_text())
    evidence_by_run = {
        result["run"]: result["production_evidence"]["digest"]
        for hypothesis in hypotheses.get("hypotheses", [])
        for result in hypothesis.get("results", [])
        if isinstance(result, dict)
        and isinstance(result.get("run"), str)
        and isinstance(result.get("production_evidence"), dict)
        and isinstance(result["production_evidence"].get("digest"), str)
    }

    lines = [
        "# Climb Research Tree",
        "",
        "> Generated deterministically from tracked climb state. Do not edit.",
        "",
        "## Session",
        "",
        f"- Last cycle: {session.get('last_cycle', 0)}",
        f"- Next action: {session.get('next_action') or 'none'}",
        "",
        "## In flight",
        "",
    ]
    in_flight = session.get("in_flight")
    if in_flight:
        lines.append(f"- {in_flight.get('hypothesis_id', 'unknown')}")
    else:
        lines.append("- None")

    lines.extend(["", "## Hypothesis pool", ""])
    for hypothesis in sorted(
        hypotheses.get("hypotheses", []),
        key=lambda item: (-float(item.get("ranking", 0)), item["id"]),
    ):
        lines.append(
            f"- **{hypothesis['id']}** [{hypothesis.get('status', 'unknown')}]: "
            f"{hypothesis.get('description', '')}"
        )

    lines.extend(["", "## Runs", ""])
    if not runs:
        lines.append("- None")
    else:
        for run in runs:
            line = f"- {run['run_id']}: {run['local_score']} ({run['verdict']})"
            digest = evidence_by_run.get(run["run_id"])
            if digest:
                line += f" evidence={digest[:12]}"
            lines.append(line)

    rendered = "\n".join(lines) + "\n"
    (state_dir / "research-tree.md").write_text(rendered)
    projection = {
        "runs": [run["run_id"] for run in runs],
        "hypotheses": [item["id"] for item in hypotheses.get("hypotheses", [])],
        "last_cycle": session.get("last_cycle", 0),
        "production_evidence": evidence_by_run,
    }
    (state_dir / "research-tree.json").write_text(
        json.dumps(projection, indent=2, sort_keys=True) + "\n"
    )
    return rendered


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    print(regenerate(root / "docs/status/climb"), end="")


if __name__ == "__main__":
    main()
