from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class GateResult:
    passed: bool
    returncode: int
    output: str


def build_score(results: Mapping[str, GateResult]) -> dict:
    if not results:
        raise ValueError("at least one gate is required")
    subscores = {name: 100.0 if result.passed else 0.0 for name, result in results.items()}
    total = sum(subscores.values()) / len(subscores)
    return {
        "total": total,
        "subscores": subscores,
        "disaster_pattern": any(score == 0.0 for score in subscores.values()),
    }


def evaluate_gates(
    commands: Mapping[str, list[str]],
    *,
    runner: Callable[[list[str]], GateResult],
    output_path: Path,
) -> dict:
    results = {name: runner(command) for name, command in commands.items()}
    payload = build_score(results)
    payload["commands"] = {
        name: {
            "argv": commands[name],
            "returncode": result.returncode,
            "output": result.output[-8_000:],
        }
        for name, result in results.items()
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


ROOT = Path(__file__).resolve().parents[2]
GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/routing/test_position_repository.py",
        "tests/routing/test_position_tracker.py",
        "-q",
    ],
    "integration": ["uv", "run", "pytest", "tests/execution", "-q"],
    "cli": ["uv", "run", "pytest", "tests/cli", "-q"],
    "restart": [
        "uv",
        "run",
        "pytest",
        "tests/cli/test_arbitrage_cli_process.py",
        "-q",
    ],
}
LIVING_DOC_CONTRACT_GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_m1_manual_contract.py",
        "-q",
    ],
    "integration": ["make", "docs-m1-check"],
    "cli": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_makefile_contract.py",
        "tests/test_makefile.py",
        "-q",
    ],
    "restart": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_m1_manual_contract.py",
        "-k",
        "precommit",
        "-q",
    ],
}
OPPORTUNITY_FEED_CHAIN_TRUTH_GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/routing/test_opportunity_diagnosis.py",
        "-q",
    ],
    "integration": [
        "uv",
        "run",
        "pytest",
        "tests/cli/test_arbitrage_cli_process.py",
        "-k",
        "diagnose_feed",
        "-q",
    ],
    "cli": ["make", "docs-m1-check"],
    "restart": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_m1_manual_contract.py",
        "-k",
        "opportunity_diagnosis",
        "-q",
    ],
}
OPPORTUNITY_FEED_CADENCE_SLA_GATE_COMMANDS = {
    "collect-dry-run": ["make", "-n", "collect-neg-risk-quotes"],
    "quote-store-collector": [
        "uv",
        "run",
        "pytest",
        "tests/routing/test_neg_risk_quote_store.py",
        "tests/routing/test_neg_risk_quote_collector.py",
        "-q",
    ],
    "scanner-http": [
        "uv",
        "run",
        "pytest",
        "tests/routing/test_opportunity_scanner.py",
        "tests/m1-perception/test_arbitrage_opportunities_http.py",
        "-q",
    ],
    "quote-cli": [
        "uv",
        "run",
        "pytest",
        "tests/cli/test_arbitrage_cli_process.py",
        "-k",
        "collect_neg_risk_quotes or scan_quotes",
        "-q",
    ],
    "scan-dry-run": ["make", "-n", "scan-arb-quotes"],
}
L3_PREREQUISITE_CHAIN_TRUTH_GATE_COMMANDS = {
    "planning": ["make", "planning-status"],
    "unit": [
        "uv",
        "run",
        "pytest",
        "tests/alembic/test_006.py",
        "tests/storage/test_supabase_mirror.py",
        "tests/observation/test_l2_temp_db.py",
        "-q",
    ],
    "integration": [
        "uv",
        "run",
        "pytest",
        "tests/observation/test_l2_candidate_refresh.py",
        "tests/m1-perception/test_l3_promoter.py",
        "tests/m1-perception/test_l2_health_l3_subchecks.py",
        "-q",
    ],
    "cli": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_l3_promote_dry_run.py",
        "tests/test_makefile.py",
        "-k",
        "dry_run or l3_seed",
        "-q",
    ],
    "restart": [
        "uv",
        "run",
        "pytest",
        "tests/m1-perception/test_candidate_refresh_l3_protection.py",
        "-q",
    ],
}


def gate_commands_for(manifest: Mapping[str, object]) -> Mapping[str, list[str]]:
    if manifest.get("paradigm") == "living-doc-contract":
        commands = LIVING_DOC_CONTRACT_GATE_COMMANDS
    elif manifest.get("paradigm") == "opportunity-feed-chain-truth":
        commands = OPPORTUNITY_FEED_CHAIN_TRUTH_GATE_COMMANDS
    elif manifest.get("paradigm") == "opportunity-feed-cadence-sla":
        commands = OPPORTUNITY_FEED_CADENCE_SLA_GATE_COMMANDS
    elif manifest.get("paradigm") == "l3-prerequisite-chain-truth":
        commands = L3_PREREQUISITE_CHAIN_TRUTH_GATE_COMMANDS
    else:
        commands = GATE_COMMANDS
    return {name: list(command) for name, command in commands.items()}


def run_command(command: list[str]) -> GateResult:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return GateResult(
        passed=completed.returncode == 0,
        returncode=completed.returncode,
        output=completed.stdout,
    )


def load_manifest(run_dir: Path) -> Mapping[str, object]:
    path = run_dir / "manifest.json"
    if not path.exists():
        # Backward compatibility: old/direct evaluator invocations predate
        # manifests and always used the repository gate profile.
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid climb manifest {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"invalid climb manifest {path}: expected a JSON object")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = load_manifest(args.run_dir)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    output_path = args.run_dir / "local-eval.json"
    payload = evaluate_gates(
        gate_commands_for(manifest),
        runner=run_command,
        output_path=output_path,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not payload["disaster_pattern"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
