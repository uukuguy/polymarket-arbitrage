from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
from typing import Callable, Mapping


@dataclass(frozen=True)
class GateResult:
    passed: bool
    returncode: int
    output: str


def build_score(results: Mapping[str, GateResult]) -> dict:
    if not results:
        raise ValueError("at least one gate is required")
    subscores = {
        name: 100.0 if result.passed else 0.0
        for name, result in results.items()
    }
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
        "uv", "run", "pytest",
        "tests/routing/test_position_repository.py",
        "tests/routing/test_position_tracker.py", "-q",
    ],
    "integration": ["uv", "run", "pytest", "tests/execution", "-q"],
    "cli": ["uv", "run", "pytest", "tests/cli", "-q"],
    "restart": [
        "uv", "run", "pytest",
        "tests/cli/test_arbitrage_cli_process.py", "-q",
    ],
}


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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args()
    output_path = args.run_dir / "local-eval.json"
    payload = evaluate_gates(
        GATE_COMMANDS,
        runner=run_command,
        output_path=output_path,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not payload["disaster_pattern"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
