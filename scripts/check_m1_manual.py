from __future__ import annotations

import argparse
import os
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path

MANUAL = Path("docs/M1-市场感知平台使用手册.md")
ALLOWED_LABELS = {"已验证可用", "有条件可用", "尚不可用"}
CAPABILITY_HEADER = ("能力", "状态", "用途", "数据源", "验证方法", "已知限制", "禁止用途")
MARKER_RE = re.compile(
    r"<!-- m1-contract: (health|route)=([^ ]+) file=([^ ]+) -->"
)
MAKE_RE = re.compile(r"`make ([a-z0-9][a-z0-9-]*)")
LINK_RE = re.compile(r"\[[^]]+\]\(([^)]+)\)")
M1_TARGET_RE = re.compile(
    r"^[+-](?![+-])## (?:snapshot|scan|show|track|watchlist|overview|daemon|smoke|"
    r"dashboard|fly-l2|verify-keepalive|l3|ohlc|chaos-l2|docs-m1)[a-z0-9-]*:",
    re.MULTILINE,
)
HEALTH_RE = re.compile(
    r'^[+-](?![+-])[ \t]*checks\["[a-z0-9_:-]+"\][ \t]*=', re.MULTILINE
)
CLI_RE = re.compile(
    r"^[+-](?![+-])[ \t]*(?:"
    r"@app\.command[ \t]*(?:\(|$)|"
    r"[a-zA-Z_]\w*(?:[ \t]*:[ \t]*[^=\n#'\"]+)?[ \t]*=[ \t]*"
    r"typer\.(?:Option|Argument)[ \t]*\(|"
    r'"-{1,2}[a-zA-Z0-9][a-zA-Z0-9-]*"[ \t]*[,)])',
    re.MULTILINE,
)
ROUTE_RE = re.compile(
    r'^[+-](?![+-])[ \t]*(?:Route[ \t]*\([ \t]*"/[a-z0-9_/{}/.-]+"|'
    r'"/[a-z0-9_/{}/.-]+"[ \t]*[,)])',
    re.MULTILINE,
)
DIFF_HEADER_RE = re.compile(r"^diff --git a/(.+) b/(.+)$")
HEALTH_PATHS = {
    "src/polyarb/http/health.py",
    "src/polyarb/http/l2_health.py",
}
M1_CLI_PATHS = {
    "src/polyarb/cli_observation.py",
    "src/polyarb/cli_translation.py",
    "src/polyarb/snapshot/cli.py",
}
ROUTE_PATHS = {
    "src/polyarb/http/app.py",
    "src/polyarb/http/l2_app.py",
}


def _make_targets(root: Path) -> set[str]:
    text = (root / "Makefile").read_text()
    return set(re.findall(r"^([a-zA-Z0-9_.-]+):(?:\s|$)", text, re.MULTILINE))


def _table_rows(text: str) -> list[list[str]]:
    rows = []
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells == list(CAPABILITY_HEADER) or all(
            set(cell) <= {"-", ":"} for cell in cells
        ):
            continue
        if len(cells) == len(CAPABILITY_HEADER) and cells[1] in ALLOWED_LABELS:
            rows.append(cells)
        elif len(cells) == len(CAPABILITY_HEADER) and cells[0] != "Fact type":
            rows.append(cells)
    return rows


def validate_manual(root: Path, text: str) -> list[str]:
    errors: list[str] = []
    for number in range(1, 11):
        if not re.search(rf"^## {number}\. ", text, re.MULTILINE):
            errors.append(f"required section {number} is missing")
    if "最后核验：" not in text or "`.planning/CURRENT.md`" not in text:
        errors.append("required verification/current-state metadata is missing")

    header = "| " + " | ".join(CAPABILITY_HEADER) + " |"
    if header not in text:
        errors.append("capability matrix header is missing or incomplete")
    rows = _table_rows(text)
    if not rows:
        errors.append("capability matrix has no data rows")
    for row in rows:
        if row[1] not in ALLOWED_LABELS:
            errors.append(f"unknown readiness label: {row[1]}")
        if any(not cell for cell in row):
            errors.append(f"capability row has an empty required field: {row[0]}")

    targets = _make_targets(root)
    for target in sorted(set(MAKE_RE.findall(text))):
        if target not in targets:
            errors.append(f"Make target {target} does not exist")

    for raw_link in LINK_RE.findall(text):
        link = raw_link.split("#", 1)[0]
        if not link or link.startswith(("http://", "https://", "mailto:")):
            continue
        destination = (root / MANUAL.parent / link).resolve()
        if not destination.exists():
            errors.append(f"local link does not resolve: {raw_link}")

    for kind, name, source in MARKER_RE.findall(text):
        source_path = root / source
        if not source_path.is_file():
            errors.append(f"{kind} {name} source does not exist: {source}")
            continue
        if kind == "health" and name not in source_path.read_text():
            errors.append(f"health {name} is absent from {source}")
        if kind == "route":
            parts = Path(source).parts
            discovered = None
            if parts[:2] == ("dashboard", "app") and parts[-1] == "page.tsx":
                discovered = "/" + "/".join(parts[2:-1])
            if discovered != name:
                errors.append(
                    f"route {name} does not match filesystem route {discovered}: {source}"
                )
    return errors


def _diff_by_path(paths: list[str], diff: str) -> dict[str, str]:
    chunks: dict[str, list[str]] = {}
    current_path: str | None = None
    for line in diff.splitlines(keepends=True):
        header = DIFF_HEADER_RE.match(line.rstrip("\n"))
        if header:
            current_path = header.group(2)
            chunks.setdefault(current_path, [])
        elif current_path is not None:
            chunks[current_path].append(line)
    if not chunks and len(paths) == 1:
        return {paths[0]: diff}
    return {path: "".join(lines) for path, lines in chunks.items()}


def classify_staged_impact(paths: list[str], diff: str) -> bool:
    if any(path.startswith("alembic/versions/") for path in paths):
        return True
    if any(
        path.startswith("dashboard/app/") and path.endswith("/page.tsx")
        for path in paths
    ):
        return True
    changed = _diff_by_path(paths, diff)
    return any(
        (path == "Makefile" and M1_TARGET_RE.search(chunk))
        or (path in HEALTH_PATHS and HEALTH_RE.search(chunk))
        or (path in M1_CLI_PATHS and CLI_RE.search(chunk))
        or (path in ROUTE_PATHS and ROUTE_RE.search(chunk))
        for path, chunk in changed.items()
    )


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, text=True, capture_output=True
    ).stdout


def _decode_nul_paths(data: bytes) -> list[str]:
    return [os.fsdecode(path) for path in data.split(b"\0") if path]


def _git_paths(*args: str) -> list[str]:
    result = subprocess.run(["git", *args], check=True, capture_output=True)
    return _decode_nul_paths(result.stdout)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staged", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    manual = root / MANUAL

    if args.staged:
        paths = _git_paths("diff", "--cached", "--name-only", "-z")
        diff = _git("diff", "--cached", "--unified=0")
        self_changed = any(
            path in {str(MANUAL), "scripts/check_m1_manual.py"} for path in paths
        )
        if classify_staged_impact(paths, diff) and str(MANUAL) not in paths:
            print(
                "M1 operator contract changed; update the manual or append an "
                "auditable no-operator-impact entry to its sync log."
            )
            return 1
        if not self_changed and not classify_staged_impact(paths, diff):
            return 0

    if not manual.is_file():
        print(f"manual missing: {MANUAL}")
        return 1
    errors = validate_manual(root, manual.read_text())
    for error in errors:
        print(f"ERROR: {error}")
    if errors:
        return 1
    print("M1 manual contract: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
