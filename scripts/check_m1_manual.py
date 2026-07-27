from __future__ import annotations

import argparse
import os
import re
import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path

MANUAL = Path("docs/M1-市场感知平台使用手册.md")
ALLOWED_LABELS = {"已验证可用", "有条件可用", "尚不可用"}
CAPABILITY_HEADER = ("能力", "状态", "用途", "数据源", "验证方法", "已知限制", "禁止用途")
MARKER_RE = re.compile(r"<!-- m1-contract: (health|route)=([^ ]+) file=([^ ]+) -->")
MAKE_RE = re.compile(r"`make ([a-z0-9][a-z0-9-]*)")
LINK_RE = re.compile(r"\[[^]]+\]\(([^)]+)\)")
L3_READ_ONLY_MAKE_TARGETS = (
    "l3-evidence-status",
    "l3-soak-checkpoint",
    "l3-soak-verify",
    "l3-evidence-retention-check",
)
L3_LOCAL_MUTATION_MAKE_TARGETS = ("l3-soak-manifest",)
L3_MUTATION_MAKE_TARGETS = (
    "l3-soak-manifest-bind",
    "l3-evidence-retention-cleanup",
)
L3_CREDENTIAL_PROOF_TARGETS = (
    "l3-runtime-credential-check",
    "l3-retention-operator-check",
    "supabase-prod-revision",
)
M1_MAKE_TARGETS = {
    "snapshot-markets-v",
    "snapshot-status",
    "snapshot-attempt-status",
    "overview",
    "list-recipes",
    "scan-l3-seed",
    "daemon-run-local",
    "daemon-l2-run-local",
    "smoke-health-prod",
    "smoke-market-truth-prod",
    "smoke-l2-health-prod",
    "smoke-l2-health-strict-prod",
    "scan-arb-live",
    "diagnose-arb-feed-prod",
    "collect-neg-risk-quotes",
    "scan-arb-quotes",
    "eval-local",
    "l3-promote-dry-run",
    "ohlc-spot-check",
    "dashboard-dev",
    "smoke-l2-dashboard",
    "smoke-l3-dashboard",
    "polywatch-healthz-dry",
    "polywatch-healthz",
    "polywatch-resident-status",
    "fly-l2-status",
    "fly-l2-logs",
    "docs-m1-check",
    "status",
    "smoke-test",
    "deploy",
    "dashboard-deploy",
    "unpause-prod",
    "chaos-l2-baseline",
    "chaos-l2-fly-image-check",
    "chaos-l2-inj1",
    "chaos-l2-cleanup",
    *L3_READ_ONLY_MAKE_TARGETS,
    *L3_LOCAL_MUTATION_MAKE_TARGETS,
    *L3_MUTATION_MAKE_TARGETS,
    *L3_CREDENTIAL_PROOF_TARGETS,
}
HEALTH_RE = re.compile(r'^[+-](?![+-])[ \t]*checks\["[a-z0-9_:-]+"\][ \t]*=', re.MULTILINE)
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
M1_DASHBOARD_PAGES = {
    "dashboard/app/status/page.tsx",
    "dashboard/app/candidates/page.tsx",
    "dashboard/app/signals/page.tsx",
    "dashboard/app/asset/[id]/tob/page.tsx",
    "dashboard/app/asset/[id]/trades/page.tsx",
    "dashboard/app/l3/[asset_id]/page.tsx",
}
M1_MIGRATION_RE = re.compile(
    r"\b(?:markets|snapshots|market_candidates|l2_top_of_books|l2_trades|"
    r"l2_book_levels|l3_|ohlc|signals)\b",
    re.IGNORECASE,
)
REQUIRED_MARKERS = {
    ("health", "snapshot:last_success_age_seconds", "src/polyarb/http/health.py"),
    ("health", "event_bus:cursor_lag", "src/polyarb/http/l2_health.py"),
    ("health", "ws:last_event_age_seconds", "src/polyarb/http/l2_health.py"),
    ("health", "mirror:l2_tob_age_seconds", "src/polyarb/http/l2_health.py"),
    ("health", "l3:active_count", "src/polyarb/http/l2_health.py"),
    ("route", "/status", "dashboard/app/status/page.tsx"),
    ("route", "/candidates", "dashboard/app/candidates/page.tsx"),
    ("route", "/signals", "dashboard/app/signals/page.tsx"),
    ("route", "/l3/[asset_id]", "dashboard/app/l3/[asset_id]/page.tsx"),
}
SYNC_LOG_RE = re.compile(
    r"^- `(?P<date>\d{4}-\d{2}-\d{2}) \| (?P<change>[^|`]+) \| "
    r"(?P<contract>[^|`]+) \| (?P<impact>[^|`]+) \| "
    r"(?P<evidence>[^|`]+) \| (?P<owner>[^|`]+)`$",
    re.MULTILINE,
)


def _make_targets(text: str) -> set[str]:
    return set(re.findall(r"^([a-zA-Z0-9_.-]+):(?:\s|$)", text, re.MULTILINE))


def _table_rows(text: str) -> list[list[str]]:
    rows = []
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if cells == list(CAPABILITY_HEADER) or all(set(cell) <= {"-", ":"} for cell in cells):
            continue
        if len(cells) == len(CAPABILITY_HEADER) and cells[1] in ALLOWED_LABELS:
            rows.append(cells)
        elif len(cells) == len(CAPABILITY_HEADER) and cells[0] != "Fact type":
            rows.append(cells)
    return rows


def validate_manual(
    root: Path,
    text: str,
    *,
    read_repo_text: Callable[[Path], str | None] | None = None,
    repo_path_exists: Callable[[Path], bool] | None = None,
) -> list[str]:
    if read_repo_text is None:

        def filesystem_read_text(path: Path) -> str | None:
            return (root / path).read_text() if (root / path).is_file() else None

        read_repo_text = filesystem_read_text
    if repo_path_exists is None:

        def filesystem_path_exists(path: Path) -> bool:
            return (root / path).exists()

        repo_path_exists = filesystem_path_exists
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

    makefile_text = read_repo_text(Path("Makefile"))
    targets = _make_targets(makefile_text or "")
    for target in sorted(set(MAKE_RE.findall(text))):
        if target not in targets:
            errors.append(f"Make target {target} does not exist")

    for raw_link in LINK_RE.findall(text):
        link = raw_link.split("#", 1)[0]
        if not link or link.startswith(("http://", "https://", "mailto:")):
            continue
        destination = (root / MANUAL.parent / link).resolve()
        try:
            relative_destination = destination.relative_to(root)
        except ValueError:
            errors.append(f"local link escapes repository: {raw_link}")
            continue
        if not repo_path_exists(relative_destination):
            errors.append(f"local link does not resolve: {raw_link}")

    markers = set(MARKER_RE.findall(text))
    for missing in sorted(REQUIRED_MARKERS - markers):
        errors.append(
            f"required contract marker is missing: {missing[0]}={missing[1]} file={missing[2]}"
        )
    for kind, name, source in markers:
        source_path = Path(source)
        source_text = read_repo_text(source_path)
        if source_text is None:
            errors.append(f"{kind} {name} source does not exist: {source}")
            continue
        if kind == "health" and name not in source_text:
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
    changed = _diff_by_path(paths, diff)
    return any(
        (path == "Makefile" and _make_contract_changed(chunk))
        or (path in HEALTH_PATHS and HEALTH_RE.search(chunk))
        or (path in M1_CLI_PATHS and CLI_RE.search(chunk))
        or (path in ROUTE_PATHS and ROUTE_RE.search(chunk))
        or (path in M1_DASHBOARD_PAGES and _has_code_change(chunk))
        or (path.startswith("alembic/versions/") and M1_MIGRATION_RE.search(_changed_lines(chunk)))
        for path, chunk in changed.items()
    )


def _changed_lines(chunk: str) -> str:
    return "".join(
        line
        for line in chunk.splitlines(keepends=True)
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))
    )


def _has_code_change(chunk: str) -> bool:
    return any(
        line[1:].strip() and not line[1:].lstrip().startswith(("//", "/*", "*"))
        for line in _changed_lines(chunk).splitlines()
    )


def _make_contract_changed(chunk: str) -> bool:
    current_target: str | None = None
    for line in chunk.splitlines():
        body = line[1:] if line[:1] in {"+", "-", " "} else line
        target = re.match(r"^([a-zA-Z0-9_.-]+):(?:\s|$)", body)
        if target:
            current_target = target.group(1)
            if line[:1] in {"+", "-"} and current_target in M1_MAKE_TARGETS:
                return True
            continue
        if current_target in M1_MAKE_TARGETS and line[:1] in {"+", "-"} and body.startswith("\t"):
            return True
    return False


def _section(text: str, number: int) -> str:
    match = re.search(rf"^## {number}\. .*?(?=^## \d+\. |\Z)", text, re.MULTILINE | re.DOTALL)
    return match.group(0) if match else ""


def _semantic_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def manual_sync_is_meaningful(before: str, after: str) -> bool:
    if any(
        _semantic_text(_section(before, number)) != _semantic_text(_section(after, number))
        for number in range(2, 10)
    ):
        return True
    old_records = set(SYNC_LOG_RE.findall(before))
    new_records = set(SYNC_LOG_RE.findall(after))
    return bool(new_records - old_records)


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, text=True, capture_output=True).stdout


def _decode_nul_paths(data: bytes) -> list[str]:
    return [os.fsdecode(path) for path in data.split(b"\0") if path]


def _git_paths(*args: str) -> list[str]:
    result = subprocess.run(["git", *args], check=True, capture_output=True)
    return _decode_nul_paths(result.stdout)


def _index_view() -> tuple[Callable[[Path], str | None], Callable[[Path], bool]]:
    index_paths = set(_git_paths("ls-files", "--cached", "-z"))

    def read_text(path: Path) -> str | None:
        name = path.as_posix()
        if name not in index_paths:
            return None
        try:
            return _git("show", f":{name}")
        except subprocess.CalledProcessError:
            return None

    def exists(path: Path) -> bool:
        name = path.as_posix().rstrip("/")
        return name in index_paths or any(item.startswith(f"{name}/") for item in index_paths)

    return read_text, exists


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staged", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    manual = root / MANUAL

    if args.staged:
        paths = _git_paths("diff", "--cached", "--name-only", "-z")
        diff = _git("diff", "--cached", "--unified=10000")
        self_changed = any(path in {str(MANUAL), "scripts/check_m1_manual.py"} for path in paths)
        impacted = classify_staged_impact(paths, diff)
        if impacted:
            if str(MANUAL) not in paths:
                print(
                    "M1 operator contract changed; update the manual or append an "
                    "auditable no-operator-impact entry to its sync log."
                )
                return 1
            try:
                before = _git("show", f"HEAD:{MANUAL}")
            except subprocess.CalledProcessError:
                before = ""
            try:
                after = _git("show", f":{MANUAL}")
            except subprocess.CalledProcessError:
                print(f"manual missing from staged tree: {MANUAL}")
                return 1
            if not manual_sync_is_meaningful(before, after):
                print(
                    "M1 operator contract changed; the staged manual edit is only "
                    "whitespace/unowned text. Update sections 2-9 or add a valid "
                    "new six-field sync-log record."
                )
                return 1
        if not self_changed and not classify_staged_impact(paths, diff):
            return 0

        read_repo_text, repo_path_exists = _index_view()
        staged_manual = read_repo_text(MANUAL)
        if staged_manual is None:
            print(f"manual missing from staged tree: {MANUAL}")
            return 1
        errors = validate_manual(
            root,
            staged_manual,
            read_repo_text=read_repo_text,
            repo_path_exists=repo_path_exists,
        )
        for error in errors:
            print(f"ERROR: {error}")
        if errors:
            return 1
        print("M1 manual contract: OK")
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
