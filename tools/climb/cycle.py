from __future__ import annotations

import argparse
import csv
import io
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from collections.abc import Callable

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

OPPORTUNITY_FEED_CHAIN_TRUTH = "opportunity-feed-chain-truth"
PRODUCTION_EVIDENCE_FILENAME = "production-evidence.json"
DIAGNOSE_FEED_COMMAND = ["make", "diagnose-arb-feed-prod"]
ROOT = Path(__file__).resolve().parents[2]


def _atomic_write(path: Path, content: str) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(content)
    temp.replace(path)


def _require_opportunity_feed_evidence(run_dir: Path, manifest: dict) -> None:
    """Reject a local-only opportunity-feed result before it can mutate state."""
    if manifest.get("paradigm") != OPPORTUNITY_FEED_CHAIN_TRUTH:
        return

    evidence_path = run_dir / PRODUCTION_EVIDENCE_FILENAME
    if not evidence_path.is_file():
        raise ValueError("opportunity-feed production evidence is required")
    try:
        evidence = json.loads(evidence_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid opportunity-feed production evidence: {exc}") from exc

    response = evidence.get("response")
    command = evidence.get("command")
    if not isinstance(response, dict) or not isinstance(command, dict):
        raise ValueError("invalid opportunity-feed production evidence structure")
    if evidence.get("hypothesis_id") != manifest.get("hypothesis_id"):
        raise ValueError("opportunity-feed production evidence hypothesis mismatch")
    if evidence.get("paradigm") != OPPORTUNITY_FEED_CHAIN_TRUTH:
        raise ValueError("opportunity-feed production evidence paradigm mismatch")
    if not isinstance(evidence.get("observed_at"), str) or not evidence["observed_at"]:
        raise ValueError("opportunity-feed production evidence timestamp is required")
    if command.get("argv") != DIAGNOSE_FEED_COMMAND or command.get("count") != 1:
        raise ValueError("opportunity-feed production evidence must record one diagnostic")
    if not isinstance(response.get("kind"), str) or not response["kind"]:
        raise ValueError("opportunity-feed production classification is required")
    if not isinstance(response.get("reason"), str) or not response["reason"]:
        raise ValueError("opportunity-feed production reason is required")
    if evidence.get("classification") != response["kind"]:
        raise ValueError("opportunity-feed production classification disagrees with response")
    if evidence.get("reason") != response["reason"]:
        raise ValueError("opportunity-feed production reason disagrees with response")
    if not isinstance(response.get("http_status"), int):
        raise ValueError("opportunity-feed production evidence HTTP status is required")


def collect_opportunity_feed_evidence(
    run_dir: Path,
    manifest: dict,
    *,
    runner: Callable[[list[str]], object] | None = None,
    observed_at: str | None = None,
) -> Path:
    """Run the single read-only diagnostic and persist its unmodified response."""
    if manifest.get("paradigm") != OPPORTUNITY_FEED_CHAIN_TRUTH:
        raise ValueError("production evidence only applies to opportunity-feed-chain-truth")

    if runner is None:
        def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                command,
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

    result = runner(list(DIAGNOSE_FEED_COMMAND))
    try:
        response = json.loads(str(result.stdout))
    except json.JSONDecodeError as exc:
        raise ValueError(f"diagnostic did not produce JSON evidence: {exc}") from exc
    if not isinstance(response, dict):
        raise ValueError("diagnostic JSON response must be an object")

    evidence_path = run_dir / PRODUCTION_EVIDENCE_FILENAME
    evidence = {
        "hypothesis_id": manifest.get("hypothesis_id"),
        "paradigm": manifest.get("paradigm"),
        "observed_at": observed_at
        or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "command": {
            "argv": DIAGNOSE_FEED_COMMAND,
            "count": 1,
            "returncode": result.returncode,
        },
        "classification": response.get("kind"),
        "reason": response.get("reason"),
        "response": response,
    }
    _atomic_write(evidence_path, json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    _require_opportunity_feed_evidence(run_dir, manifest)
    return evidence_path


def record_production_evidence_after_gates(
    run_dir: Path,
    manifest: dict,
    *,
    local_gates_passed: bool,
    runner: Callable[[list[str]], object] | None = None,
    observed_at: str | None = None,
) -> Path | None:
    """Allow the one production read only after all local gates succeed."""
    if manifest.get("paradigm") != OPPORTUNITY_FEED_CHAIN_TRUTH:
        return None
    if not local_gates_passed:
        raise ValueError("opportunity-feed production evidence requires local gates")
    return collect_opportunity_feed_evidence(
        run_dir,
        manifest,
        runner=runner,
        observed_at=observed_at,
    )


def sync_cycle(state_dir: Path, completed_run: dict) -> None:
    manifest = json.loads(Path(completed_run["manifest_path"]).read_text())
    evaluation = json.loads(Path(completed_run["local_eval_path"]).read_text())
    _require_opportunity_feed_evidence(Path(completed_run["run_dir"]), manifest)
    decision_reason = completed_run["reason"]
    if manifest.get("paradigm") == OPPORTUNITY_FEED_CHAIN_TRUTH:
        decision_reason += "; production evidence recorded"
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
        "decision_reason": decision_reason,
        "verdict": verdict,
        "cost_h": completed_run["cost_h"],
        "manifest_path": completed_run["manifest_path"],
    }
    existing.append(row)
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=RUN_FIELDS,
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(existing)
    _atomic_write(runs_path, output.getvalue())

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
            "decision_reason": decision_reason,
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
