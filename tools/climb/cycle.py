from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
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


def _canonical_evidence_digest(evidence: dict) -> str:
    """Return the stable digest for the artifact, excluding its own digest field."""
    unsigned = {key: value for key, value in evidence.items() if key != "digest"}
    encoded = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _is_finite_number(value: object) -> bool:
    return type(value) in {int, float} and math.isfinite(float(value))


def _require_verified_local_gates(run_dir: Path, manifest: dict) -> dict:
    """Independently validate every configured local evaluator gate for this run."""
    from tools.climb.eval_local import gate_commands_for

    local_eval_path = run_dir / "local-eval.json"
    try:
        evaluation = json.loads(local_eval_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"opportunity-feed local evaluation is required: {exc}") from exc
    if not isinstance(evaluation, dict):
        raise ValueError("opportunity-feed local evaluation must be an object")

    expected_commands = gate_commands_for(manifest)
    commands = evaluation.get("commands")
    subscores = evaluation.get("subscores")
    if not isinstance(commands, dict) or not isinstance(subscores, dict):
        raise ValueError("opportunity-feed local evaluation gate records are required")
    if evaluation.get("disaster_pattern") is not False:
        raise ValueError("opportunity-feed local gates did not pass")
    if not _is_finite_number(evaluation.get("total")) or evaluation["total"] != 100.0:
        raise ValueError("opportunity-feed local evaluation total must be 100")

    for name, expected_argv in expected_commands.items():
        result = commands.get(name)
        if not isinstance(result, dict):
            raise ValueError(f"opportunity-feed local gate {name} is missing")
        if result.get("argv") != expected_argv:
            raise ValueError(f"opportunity-feed local gate {name} command mismatch")
        returncode = result.get("returncode")
        if type(returncode) is not int or returncode != 0:
            raise ValueError(f"opportunity-feed local gate {name} did not pass")
        score = subscores.get(name)
        if not _is_finite_number(score) or score != 100.0:
            raise ValueError(f"opportunity-feed local gate {name} score did not pass")
    return evaluation


def _require_opportunity_feed_evidence(run_dir: Path, manifest: dict) -> dict | None:
    """Reject a local-only opportunity-feed result before it can mutate state."""
    if manifest.get("paradigm") != OPPORTUNITY_FEED_CHAIN_TRUTH:
        return None

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
    if type(command.get("returncode")) is not int:
        raise ValueError("opportunity-feed production evidence returncode is required")
    if not isinstance(response.get("kind"), str) or not response["kind"]:
        raise ValueError("opportunity-feed production classification is required")
    if not isinstance(response.get("reason"), str) or not response["reason"]:
        raise ValueError("opportunity-feed production reason is required")
    if evidence.get("classification") != response["kind"]:
        raise ValueError("opportunity-feed production classification disagrees with response")
    if evidence.get("reason") != response["reason"]:
        raise ValueError("opportunity-feed production reason disagrees with response")
    if type(response.get("http_status")) is not int:
        raise ValueError("opportunity-feed production evidence HTTP status is required")
    digest = evidence.get("digest")
    if not isinstance(digest, str) or len(digest) != 64:
        raise ValueError("opportunity-feed production evidence digest is required")
    if digest != _canonical_evidence_digest(evidence):
        raise ValueError("opportunity-feed production evidence digest mismatch")
    if response["kind"] == "stale-snapshot":
        for field in ("snapshot_age_seconds", "max_snapshot_age_seconds"):
            if not _is_finite_number(response.get(field)):
                raise ValueError(
                    f"opportunity-feed stale evidence {field} is required"
                )
    return evidence


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

    evidence_path = run_dir / PRODUCTION_EVIDENCE_FILENAME
    if evidence_path.exists():
        raise ValueError("opportunity-feed production evidence already exists")

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
    evidence["digest"] = _canonical_evidence_digest(evidence)
    _atomic_write(evidence_path, json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    _require_opportunity_feed_evidence(run_dir, manifest)
    return evidence_path


def record_production_evidence_after_gates(
    run_dir: Path,
    manifest: dict,
    *,
    runner: Callable[[list[str]], object] | None = None,
    observed_at: str | None = None,
) -> Path | None:
    """Allow the one production read only after all local gates succeed."""
    if manifest.get("paradigm") != OPPORTUNITY_FEED_CHAIN_TRUTH:
        return None
    if (run_dir / PRODUCTION_EVIDENCE_FILENAME).exists():
        raise ValueError("opportunity-feed production evidence already exists")
    _require_verified_local_gates(run_dir, manifest)
    return collect_opportunity_feed_evidence(
        run_dir,
        manifest,
        runner=runner,
        observed_at=observed_at,
    )


def sync_cycle(state_dir: Path, completed_run: dict) -> None:
    manifest = json.loads(Path(completed_run["manifest_path"]).read_text())
    evaluation = json.loads(Path(completed_run["local_eval_path"]).read_text())
    evidence = _require_opportunity_feed_evidence(Path(completed_run["run_dir"]), manifest)
    if manifest.get("paradigm") == OPPORTUNITY_FEED_CHAIN_TRUTH:
        _require_verified_local_gates(Path(completed_run["run_dir"]), manifest)
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
    result = {
            "session": session["session"],
            "cycle": cycle,
            "run": completed_run["run_id"],
            "local": evaluation["total"],
            "local_per_task": subscores,
            "online": None,
            "verdict": verdict,
            "decision_reason": decision_reason,
        }
    if evidence is not None:
        response = evidence["response"]
        production_evidence = {
            "digest": evidence["digest"],
            "observed_at": evidence["observed_at"],
            "argv": evidence["command"]["argv"],
            "count": evidence["command"]["count"],
            "returncode": evidence["command"]["returncode"],
            "http_status": response["http_status"],
            "classification": evidence["classification"],
            "reason": evidence["reason"],
        }
        if response["kind"] == "stale-snapshot":
            production_evidence["snapshot_age_seconds"] = response[
                "snapshot_age_seconds"
            ]
            production_evidence["max_snapshot_age_seconds"] = response[
                "max_snapshot_age_seconds"
            ]
        result["production_evidence"] = production_evidence
    hypothesis.setdefault("results", []).append(result)
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
