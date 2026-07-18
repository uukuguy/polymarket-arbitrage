# M1 Market Perception Platform Living Manual Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a Chinese product-and-operations manual that truthfully explains M1 completion, usage, practical boundaries, and stays synchronized with user-visible repository contracts.

**Architecture:** Keep narrative and readiness judgments in one human-authored manual while treating `.planning/CURRENT.md` and runtime `/health` as volatile truth. A Python standard-library checker validates stable Markdown references and exposes a pure staged-impact classifier used by the pre-commit hook; no documentation test performs network I/O.

**Tech Stack:** Markdown, Python 3.12 standard library, pytest, Make, Bash pre-commit hook.

## Global Constraints

- The canonical manual path is `docs/M1-市场感知平台使用手册.md`.
- Readiness labels are exactly `已验证可用`, `有条件可用`, and `尚不可用`.
- Runtime ages, counts, incidents, and deployment state remain authoritative in `.planning/CURRENT.md` and L1/L2 `/health`; the manual must not freeze them as evergreen facts.
- `make docs-m1-check` is offline and requires no credentials.
- Every executable workflow uses an existing Makefile target; no new operational bypass command is introduced.
- Local, production, mutating-production, and chaos operations are visibly distinguished.
- M1 output does not authorize real-money trading or assert sustainable profitability.
- The documentation gate reacts only to user-visible M1 contract changes, not internal refactors, test-only edits, or log wording.
- Do not weaken Phase 05/05.1 production gates or alter M1 runtime behavior in this work.

---

## File map

- Create `scripts/check_m1_manual.py`: pure manual-contract validation, staged-impact classification, and CLI entry point.
- Create `tests/m1-perception/test_m1_manual_contract.py`: checker unit tests, repository-manual acceptance test, Make target test, and hook contract tests.
- Create `docs/M1-市场感知平台使用手册.md`: product/operations narrative, capability matrix, workflows, troubleshooting, command safety classes, and sync log.
- Modify `Makefile`: expose `docs-m1-check` and list it in `.PHONY`/`make help`.
- Modify `.githooks/pre-commit`: run the staged documentation guard before the existing plan-SUMMARY subject logic.
- Modify `README.md`: replace stale M1 prose with the manual entry point and preserve CURRENT as volatile truth.
- Modify `docs/learning/00-INDEX.md`: distinguish the operations manual from the learning sequence.
- Modify `.planning/CURRENT.md`: link stable operational interpretation to the manual without moving current status out of CURRENT.
- Modify `.planning/JOURNAL.md`: record the delivered manual, synchronization contract, verification, and next command.
- Create `.planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/` with five lightweight GSD PLAN pointers and matching SUMMARY artifacts; the full executable detail remains in this canonical implementation plan.

## Execution registration

Before Task 1, register this independent delivery without changing the still-open Phase 05.1 runtime verdict:

- Add Roadmap Phase `05.2: M1 platform living manual`, depending only on the already-delivered M1 L1/L2/L3 interfaces and containing Plans `05.2-01` through `05.2-05` corresponding to Tasks 1–5 below.
- Create `.planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-CONTEXT.md` linking the approved design at `docs/superpowers/specs/2026-07-18-m1-market-perception-manual-design.md`.
- Create `05.2-01-PLAN.md`, `05.2-02-PLAN.md`, `05.2-03-PLAN.md`, `05.2-04-PLAN.md`, and `05.2-05-PLAN.md` in that directory. Each file states its matching Task title, files, acceptance command, and links this implementation plan for the complete step sequence.
- Run `make planning-status`; expected result is five `NOT-STARTED` plans and no `DRIFT`.
- Commit only those planning artifacts as `docs(05.2): register M1 living manual delivery`. A SUMMARY is not created in this registration commit because no plan implementation has shipped.

### Task 1: Build the offline contract checker with test-first fixtures

**Files:**
- Create: `scripts/check_m1_manual.py`
- Create: `tests/m1-perception/test_m1_manual_contract.py`

**Interfaces:**
- Consumes: repository root `Path`, manual Markdown, optional staged path/diff inputs.
- Produces: `validate_manual(root: Path, text: str) -> list[str]`, `classify_staged_impact(paths: list[str], diff: str) -> bool`, and `main(argv: Sequence[str] | None = None) -> int`.
- Marker contract: `<!-- m1-contract: health=<name> file=<path> -->` and `<!-- m1-contract: route=<route> file=<path> -->`.

- [ ] **Step 1: Write failing unit tests for headings, labels, Make targets, links, routes, health names, and staged impact**

Create the test module with temporary-repository helpers and these concrete cases:

```python
from __future__ import annotations

import subprocess
from pathlib import Path

from scripts.check_m1_manual import classify_staged_impact, validate_manual

ROOT = Path(__file__).parents[2]
HEADINGS = tuple(f"## {n}. " for n in range(1, 11))


def _valid_manual() -> str:
    sections = "\n".join(f"## {n}. section-{n}\nbody" for n in range(1, 11))
    return f"""# M1 市场感知平台使用手册

> 最后核验：2026-07-18
> 动态状态：`.planning/CURRENT.md`

{sections}

| 能力 | 状态 | 用途 | 数据源 | 验证方法 | 已知限制 | 禁止用途 |
|---|---|---|---|---|---|---|
| L1 | 已验证可用 | snapshot | Gamma+CLOB | `make snapshot-status` | local only | live order |

[CURRENT](../.planning/CURRENT.md)
<!-- m1-contract: health=snapshot:last_success_age_seconds file=src/polyarb/http/health.py -->
<!-- m1-contract: route=/status file=dashboard/app/status/page.tsx -->
"""


def _repo(tmp_path: Path) -> Path:
    (tmp_path / "src/polyarb/http").mkdir(parents=True)
    (tmp_path / "dashboard/app/status").mkdir(parents=True)
    (tmp_path / "docs").mkdir()
    (tmp_path / ".planning").mkdir()
    (tmp_path / "Makefile").write_text("snapshot-status:\n\t@true\n")
    (tmp_path / ".planning/CURRENT.md").write_text("current\n")
    (tmp_path / "src/polyarb/http/health.py").write_text(
        'checks["snapshot:last_success_age_seconds"] = []\n'
    )
    (tmp_path / "dashboard/app/status/page.tsx").write_text("export default 1\n")
    return tmp_path


def test_valid_manual_has_no_errors(tmp_path: Path) -> None:
    assert validate_manual(_repo(tmp_path), _valid_manual()) == []


def test_rejects_missing_section_and_unknown_label(tmp_path: Path) -> None:
    text = _valid_manual().replace("## 10. section-10\nbody\n", "")
    text = text.replace("已验证可用", "大概可用")
    errors = validate_manual(_repo(tmp_path), text)
    assert any("required section 10" in error for error in errors)
    assert any("大概可用" in error for error in errors)


def test_rejects_missing_make_link_route_and_health(tmp_path: Path) -> None:
    text = _valid_manual().replace("snapshot-status", "missing-target")
    text = text.replace("../.planning/CURRENT.md", "missing.md")
    text = text.replace("snapshot:last_success_age_seconds", "missing:health")
    text = text.replace("dashboard/app/status/page.tsx", "dashboard/app/missing/page.tsx")
    errors = validate_manual(_repo(tmp_path), text)
    assert any("Make target missing-target" in error for error in errors)
    assert any("link" in error for error in errors)
    assert any("health missing:health" in error for error in errors)
    assert any("route /status" in error for error in errors)


def test_rejects_empty_capability_field(tmp_path: Path) -> None:
    text = _valid_manual().replace("| local only |", "|  |")
    errors = validate_manual(_repo(tmp_path), text)
    assert any("empty required field" in error for error in errors)


def test_staged_classifier_is_narrow() -> None:
    assert classify_staged_impact(
        ["Makefile"], "+## snapshot-status: changed operator contract\n"
    )
    assert classify_staged_impact(
        ["src/polyarb/http/l2_health.py"],
        '+    checks["event_bus:cursor_lag"] = []\n',
    )
    assert classify_staged_impact(
        ["dashboard/app/new-view/page.tsx"], "diff --git a/x b/x\n"
    )
    assert not classify_staged_impact(
        ["src/polyarb/http/l2_health.py"], "+    logger.info('refactor only')\n"
    )
    assert not classify_staged_impact(
        ["tests/m1-perception/test_health_endpoint.py"], "+def test_internal(): pass\n"
    )
```

- [ ] **Step 2: Run the focused tests and confirm the import fails**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py -q
```

Expected: collection fails with `ModuleNotFoundError: No module named 'scripts.check_m1_manual'`.

- [ ] **Step 3: Implement the minimal offline validator and staged classifier**

Create `scripts/check_m1_manual.py` with these exact public contracts and behavior:

```python
from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path
from typing import Sequence

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
HEALTH_RE = re.compile(r'^[+-](?![+-]).*checks\["[a-z0-9_:-]+"\]', re.MULTILINE)
CLI_RE = re.compile(
    r"^[+-](?![+-]).*(@app\.command|typer\.(Option|Argument))", re.MULTILINE
)
ROUTE_RE = re.compile(
    r'^[+-](?![+-]).*Route\("/[a-z0-9_/{}/.-]+"', re.MULTILINE
)


def _make_targets(root: Path) -> set[str]:
    text = (root / "Makefile").read_text()
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


def classify_staged_impact(paths: list[str], diff: str) -> bool:
    if any(path.startswith("alembic/versions/") for path in paths):
        return True
    if any(path.startswith("dashboard/app/") and path.endswith("/page.tsx") for path in paths):
        return True
    patterns = (M1_TARGET_RE, HEALTH_RE, CLI_RE, ROUTE_RE)
    return any(pattern.search(diff) for pattern in patterns)


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, text=True, capture_output=True
    ).stdout


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staged", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    manual = root / MANUAL

    if args.staged:
        paths = _git("diff", "--cached", "--name-only").splitlines()
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
```

- [ ] **Step 4: Run the checker unit tests and fix only contract-level failures**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py -q
```

Expected: `5 passed` after adding a separate assertion that an empty capability field is rejected. Do not create the real manual in this task.

- [ ] **Step 5: Write the Plan 05.2-01 SUMMARY and commit the checker slice**

Create `.planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-01-SUMMARY.md` recording the pure interfaces, the six focused tests, offline/no-credential constraint, and staged-classifier patterns. Then commit:

```bash
git add scripts/check_m1_manual.py tests/m1-perception/test_m1_manual_contract.py \
  .planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-01-SUMMARY.md
git commit -m "test(05.2-01): add living manual contract checker"
```

### Task 2: Write the product-and-operations manual against verified repository truth

**Files:**
- Create: `docs/M1-市场感知平台使用手册.md`
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_m1_manual_contract.py`

**Interfaces:**
- Consumes: the Task 1 marker/table contracts, `.planning/CURRENT.md`, Makefile, L1/L2 health implementations, dashboard filesystem routes, and learning documents 07–11.
- Produces: the stable M1 operator entry point; later tasks depend on its exact path.

- [ ] **Step 1: Add a failing repository acceptance test**

Append:

```python
def test_repository_m1_manual_passes_contract() -> None:
    manual = ROOT / "docs/M1-市场感知平台使用手册.md"
    assert manual.is_file(), "living M1 manual must exist"
    assert validate_manual(ROOT, manual.read_text()) == []


def test_manual_keeps_real_money_boundary_explicit() -> None:
    text = (ROOT / "docs/M1-市场感知平台使用手册.md").read_text()
    assert "不构成真实资金下单授权" in text
    assert "`.planning/CURRENT.md`" in text
    assert "本地数据不代表生产状态" in text


def test_docs_m1_check_make_target() -> None:
    result = subprocess.run(
        ["make", "docs-m1-check"], cwd=ROOT, text=True, capture_output=True
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "M1 manual contract: OK" in result.stdout
```

- [ ] **Step 2: Run the acceptance tests and confirm the manual is missing**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py \
  -k 'repository_m1_manual or real_money_boundary' -q
```

Expected: both tests fail because `docs/M1-市场感知平台使用手册.md` does not exist.

- [ ] **Step 3: Create the manual with the approved ten-section contract**

Write the document in Chinese with these exact top-level headings and content requirements:

```markdown
# M1 市场感知平台使用手册

> 最后核验：2026-07-18
> 动态状态：以 [`.planning/CURRENT.md`](../.planning/CURRENT.md) 和生产 L1/L2 `/health` 为准。
> 定位：产品说明 + 日常运维；设计学习请进入 [`docs/learning/`](learning/00-INDEX.md)。

## 1. 30 秒心智模型

市场发现 → L1 快照 → 候选集 → L2 实时订单簿 → L3 观察证据 → 人工/paper 决策。
M1 的职责是看清市场和暴露数据质量，不构成真实资金下单授权。

## 2. 功能完成度矩阵

| 能力 | 状态 | 用途 | 数据源 | 验证方法 | 已知限制 | 禁止用途 |
|---|---|---|---|---|---|---|
| L1 市场发现与快照 | 已验证可用 | 建立市场全景和历史切片 | Gamma + CLOB | `make snapshot-status`; 生产看 L1 `/health` | R2 可独立 warn；动态状态看 CURRENT | 把本地行数当生产状态 |
| 观察配方与单市场追踪 | 已验证可用 | 筛候选、看漂移、管理 watchlist | 本地 SQLite/Parquet | `make overview`; `make list-recipes` | 结果依赖最近一次本地快照 | 把筛选命中当可成交机会 |
| L1→L2 候选链 | 有条件可用 | 将最新候选送入实时跟踪 | Supabase + durable cursor + NOTIFY/poll | `make smoke-l2-health-prod` 后读取 strict `/health` | Phase 05.1 quiet-edge 证据仍以 CURRENT 为准 | 忽略 cursor lag 或 freshness fail |
| L2 WebSocket 与 top-of-book mirror | 有条件可用 | 观察实时盘口和镜像新鲜度 | Polymarket WS + Supabase | L2 `/health` 的 `ws:*` 与 `mirror:*` | 安静市场需区分活连接和业务帧新鲜度 | 把连接在线等同盘口可成交 |
| L3 深度与 OHLC | 尚不可用 | 为单市场深度/K 线提供证据 | `l2_book_levels` + OHLC views | `make ohlc-spot-check URL=https://polyarb-l2.fly.dev` | 严格 N=5/soak 门未关闭 | 用不足样本推断稳定策略 |
| Dashboard | 有条件可用 | 浏览候选、盘口、成交、L3 和状态 | Supabase read models | `make smoke-l2-dashboard` | Vercel Auth；页面在线不等于数据新鲜 | 仅凭 UI 绿色绕过 `/health` |
| Neg-risk 可成交 ask 机会输入 | 已验证可用 | 为 M2 提供真实市场监控输入 | L1 `/arbitrage/opportunities` | `make scan-arb-live min_edge_bps=0` | gross-before-fees；可能长期为 0 | 把 gross edge 当净收益或下单指令 |
| 自动真实资金执行 | 尚不可用 | 交易所下单、成交和风控 | 尚无 live adapter | 无 | 认证、allowance、限额、kill switch 未完成 | 任何真实资金运行 |

## 3. 五分钟日常巡检
## 4. 三种使用方式：生产巡检、本地验证、Dashboard
## 5. L1/L2/L3 数据与新鲜度契约
## 6. 如何解读结果与实战边界
## 7. 故障排查地图
## 8. 运维与恢复
## 9. 命令安全分级索引
## 10. 持续同步协议与变更记录
```

Write Sections 3–10 with the following exact coverage contract:

- Section 3 reads strict `/health`, not only always-200 `/healthz`, and explains pass/warn/fail/insufficient evidence.
- Section 4 separates production read-only (`make smoke-test`, `make smoke-l2-health-prod`, `make scan-arb-live`), local (`make snapshot-markets-v`, `make overview`, `make daemon-run-local`, `make daemon-l2-run-local`), and dashboard (`make dashboard-dev`, `make smoke-l2-dashboard`) flows. Include the literal sentence `本地数据不代表生产状态`.
- Section 5 explains NOTIFY as a doorbell and the durable cursor as the ledger; distinguish WS transport liveness from business frame freshness.
- Section 6 lists fee, slippage, fill, oracle, risk/capital approval, and sustainable-return gaps and repeats `不构成真实资金下单授权`.
- Section 7 covers local zero rows, L1 warn/fail, `WAITING_FOR_EVENT`, cursor lag, stale mirror, L3 `0/10`, zero opportunities, dashboard auth, and R2 warning.
- Section 8 starts with read-only diagnosis and marks restart/deploy/secrets/chaos actions as production mutations.
- Section 9 groups commands into daily read-only, local mutation, production mutation, and chaos; state prerequisites and success interpretation.
- Section 10 states the truth hierarchy, `make docs-m1-check`, the pre-commit rule, and an auditable sync-log format.

Add contract markers beside the corresponding explanations:

```markdown
<!-- m1-contract: health=snapshot:last_success_age_seconds file=src/polyarb/http/health.py -->
<!-- m1-contract: health=event_bus:cursor_lag file=src/polyarb/http/l2_health.py -->
<!-- m1-contract: health=ws:last_event_age_seconds file=src/polyarb/http/l2_health.py -->
<!-- m1-contract: health=mirror:l2_tob_age_seconds file=src/polyarb/http/l2_health.py -->
<!-- m1-contract: health=l3:active_count file=src/polyarb/http/l2_health.py -->
<!-- m1-contract: route=/status file=dashboard/app/status/page.tsx -->
<!-- m1-contract: route=/candidates file=dashboard/app/candidates/page.tsx -->
<!-- m1-contract: route=/signals file=dashboard/app/signals/page.tsx -->
<!-- m1-contract: route=/l3/[asset_id] file=dashboard/app/l3/[asset_id]/page.tsx -->
```

Link, rather than duplicate, `docs/learning/07-观察市场.md`, `08-生产化部署.md`, `09-生产化运维.md`, `10-L2-跟踪.md`, `11-L3-K线.md`, `docs/E2E_ACCEPTANCE_GUIDE.md`, and `.planning/CURRENT.md`.

In the Makefile meta section, add the target at the same time so every `make docs-m1-check` reference is valid in this slice:

```make
.PHONY: docs-m1-check

## docs-m1-check: Offline verification of the M1 platform manual's commands, links, routes, health names, and readiness matrix
docs-m1-check:
	@uv run python scripts/check_m1_manual.py
```

- [ ] **Step 4: Run the manual contract and inspect every reported mismatch**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py -q
make docs-m1-check
```

Expected: all focused tests pass and the checker prints `M1 manual contract: OK`. Fix stale command, link, route, or health references in the manual; do not weaken checker assertions to accept a false reference.

- [ ] **Step 5: Write the Plan 05.2-02 SUMMARY and commit the manual slice**

Create `.planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-02-SUMMARY.md` recording the ten sections, readiness matrix, verified references, real-money boundary, and focused acceptance results. Then commit:

```bash
git add docs/M1-市场感知平台使用手册.md Makefile \
  tests/m1-perception/test_m1_manual_contract.py \
  .planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-02-SUMMARY.md
git commit -m "docs(05.2-02): add market perception platform manual"
```

### Task 3: Enforce scoped staged synchronization

**Files:**
- Modify: `.githooks/pre-commit`
- Modify: `tests/m1-perception/test_m1_manual_contract.py`

**Interfaces:**
- Consumes: `scripts/check_m1_manual.py --staged` from Task 1.
- Produces: `make docs-m1-check`; commits touching public M1 contracts require a staged manual sync entry.

- [ ] **Step 1: Add the failing hook-order contract test**

Append:

```python
def test_precommit_invokes_staged_m1_manual_check_before_summary_exit() -> None:
    text = (ROOT / ".githooks/pre-commit").read_text()
    check = "uv run python scripts/check_m1_manual.py --staged"
    assert check in text
    assert text.index(check) < text.index("PHASE_PLAN=")
```

- [ ] **Step 2: Run the hook-order test and confirm the missing invocation failure**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py \
  -k precommit_invokes -q
```

Expected: one failure because the hook lacks the staged invocation.

- [ ] **Step 3: Add the staged checker hook call**

In `.githooks/pre-commit`, immediately after `set -euo pipefail` and before commit-subject parsing, add:

```bash
# Keep the M1 product/operations manual synchronized with public contracts.
# The checker is offline and exits immediately for unrelated staged changes.
uv run python scripts/check_m1_manual.py --staged
```

The auditable no-impact path is a dated line in the manual's Section 10 sync log explaining why the detected public-contract change does not alter an operator workflow. Staging that line satisfies the gate and preserves review evidence.

- [ ] **Step 4: Run focused tests, the Make target, and a hook-safe no-op check**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py -q
make docs-m1-check
bash -n .githooks/pre-commit
make help | rg 'docs-m1-check:'
```

Expected: tests pass, checker prints `OK`, Bash syntax passes, and help lists the new target.

- [ ] **Step 5: Write the Plan 05.2-03 SUMMARY and commit the integration slice**

Create `.planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-03-SUMMARY.md` recording the Make target, pre-commit placement, narrow triggers, auditable no-impact path, and hook/test evidence. Then commit:

```bash
git add .githooks/pre-commit tests/m1-perception/test_m1_manual_contract.py \
  .planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-03-SUMMARY.md
git commit -m "chore(05.2-03): guard the living manual contract"
```

### Task 4: Wire the manual into all canonical entry points

**Files:**
- Modify: `README.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `.planning/CURRENT.md`
- Modify: `tests/m1-perception/test_m1_manual_contract.py`

**Interfaces:**
- Consumes: the stable manual path from Task 2.
- Produces: product, learning, and live-status navigation with non-overlapping authority statements.

- [ ] **Step 1: Add failing entry-point link tests**

Append:

```python
def test_canonical_entry_points_link_the_manual() -> None:
    expected = "M1-市场感知平台使用手册.md"
    assert expected in (ROOT / "README.md").read_text()
    assert "../M1-市场感知平台使用手册.md" in (
        ROOT / "docs/learning/00-INDEX.md"
    ).read_text()
    assert "../docs/M1-市场感知平台使用手册.md" in (
        ROOT / ".planning/CURRENT.md"
    ).read_text()
```

- [ ] **Step 2: Run the entry-point test and confirm all three assertions fail**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py \
  -k canonical_entry_points -q
```

Expected: failure at the first missing entry-point link.

- [ ] **Step 3: Add concise authority-aware links**

Replace README's stale statement `M1 生产数据目前 unhealthy` with:

```markdown
先读 [M1 市场感知平台使用手册](docs/M1-市场感知平台使用手册.md)，了解功能完成度、
日常巡检、故障排查和真实资金边界。部署状态会变化，当前事实以
[`.planning/CURRENT.md`](.planning/CURRENT.md) 为准。
```

Add before `docs/learning/00-INDEX.md`'s reading order:

```markdown
要运行或巡检平台，请先用 [M1 市场感知平台使用手册](../M1-市场感知平台使用手册.md)。
本索引负责建立代码心智模型，不承担实时生产状态或操作手册职责。
```

Add after `.planning/CURRENT.md`'s opening blockquote:

```markdown
稳定的使用流程、健康语义和命令安全分级见
[M1 市场感知平台使用手册](../docs/M1-市场感知平台使用手册.md)；本文继续只维护动态状态。
```

- [ ] **Step 4: Run entry-point and full contract checks**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py -q
make docs-m1-check
git diff --check
```

Expected: all pass with no whitespace errors and no stale README claim.

- [ ] **Step 5: Write the Plan 05.2-04 SUMMARY and commit the navigation slice**

Create `.planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-04-SUMMARY.md` recording the three entry points, removal of the stale README statement, authority boundaries, and passing link checks. Then commit:

```bash
git add README.md docs/learning/00-INDEX.md .planning/CURRENT.md \
  tests/m1-perception/test_m1_manual_contract.py \
  .planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-04-SUMMARY.md
git commit -m "docs(05.2-04): make the manual the operator entry point"
```

### Task 5: Close documentation delivery with project evidence and planning hygiene

**Files:**
- Modify: `.planning/JOURNAL.md`
- Create: `.planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-05-SUMMARY.md`
- Verify: all files changed in Tasks 1–4

**Interfaces:**
- Consumes: passing checker, tests, and committed manual slices.
- Produces: durable session/plan recovery evidence and an explicit next command without closing Phase 05.1's production gate.

- [ ] **Step 1: Run the proportional verification suite**

Run:

```bash
make docs-m1-check
uv run pytest tests/m1-perception/test_m1_manual_contract.py \
  tests/m1-perception/test_makefile_contract.py tests/test_makefile.py -q
bash -n .githooks/pre-commit
make planning-status
git diff --check
```

Expected: documentation checker and focused tests pass, hook syntax is valid, and planning status shows no shipped plan without a SUMMARY.

- [ ] **Step 2: Perform a human operator walkthrough without mutating production**

Follow the manual's five-minute workflow using only read-only commands:

```bash
make status
make smoke-test
make smoke-l2-health-prod
make scan-arb-live min_edge_bps=0
```

Record actual results only in `.planning/CURRENT.md` or the execution evidence/SUMMARY, not as evergreen manual values. A production warning is a truthful walkthrough result and does not fail the documentation deliverable if the manual interprets it correctly.

- [ ] **Step 3: Append the JOURNAL session record**

Record:

- the living manual path and its product/operations role;
- capability labels and the real-money prohibition;
- `make docs-m1-check` and scoped staged guard;
- verification commands and outcomes;
- Phase 05.1 remains open unless its independent quiet-edge evidence passed;
- next session starts from `/gsd-resume-work --ws m1-perception`.

- [ ] **Step 4: Write the Plan 05.2-05 SUMMARY before the final plan-scoped commit**

Create `.planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-05-SUMMARY.md` from the repository summary template. Include files changed, tests, operator walkthrough evidence, design decisions, limitations, the manual maintenance protocol, and the explicit statement that Phase 05.1 remains independently open.

- [ ] **Step 5: Commit closure artifacts and re-run guards**

```bash
git add .planning/JOURNAL.md \
  .planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-05-SUMMARY.md
git commit -m "docs(05.2-05): close M1 manual delivery"
make planning-status
git status --short
```

Expected: final planning status reports all five Phase 05.2 plans with SUMMARY files, no drift, and a clean worktree.

## Execution notes

- This plan is independent of the open Phase 05.1-04 natural quiet-edge observation. It documents that boundary but cannot close it.
- At execution kickoff, register the exact Phase 05.2 paths in the Execution registration section before the first implementation commit.
- If the runtime walkthrough reveals a defect, record and route it separately. Do not broaden this documentation plan into a production repair.
- No Fly deploy, service restart, secret mutation, schema migration, or chaos command is authorized by this plan.
