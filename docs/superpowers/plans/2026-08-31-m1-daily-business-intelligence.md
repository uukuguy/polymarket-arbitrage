# M1 日常业务情报 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide a read-only production opportunity command and a business-facing daily M1 intelligence guide with an append-only log template.

**Architecture:** Add a Makefile façade for the public control-plane transactional opportunity projection; it has no local database or credential dependency and preserves HTTP failure semantics. Put interpretation, decision boundaries, and the daily record template in documentation rather than adding shell-side scoring or automatic reporting.

**Tech Stack:** GNU Make, curl, Python `json.tool`, pytest, Markdown.

## Global Constraints

- The only production opportunity URL is `https://polyarb-control-api.fly.dev/perception/opportunities`.
- The endpoint accepts `limit=1..500` and `after_group_id=`; 503/unavailable is not zero opportunities.
- All commands are Makefile targets and are read-only: no deploy, scheduler, wallet, order, trade, local SQLite, secret, DSN, or Fly credential.
- M1 is observe-only: current opportunities are research candidates, never executions, fills, P&L, or trading authorization.
- The old `perception-opportunities` target remains untouched in this plan because it may retain historical/local compatibility semantics; the new target is the current production business entrypoint.
- Daily observations append to their log; historical records are never rewritten.

---

## File structure

| File | Responsibility |
| --- | --- |
| `Makefile` | Exposes discoverable, bounded, production read-only opportunity command. |
| `tests/m1-perception/test_makefile_contract.py` | Locks the command's URL, pagination interface, and non-mutating/no-secret contract. |
| `docs/learning/106-M1日常业务情报操作指南.md` | Explains daily business interpretation and decision boundaries. |
| `docs/learning/00-INDEX.md` | Adds the guide to the learning path. |
| `docs/ops/m1-daily-business-intelligence-log.md` | Carries a reusable, append-only daily observation template. |
| `tests/m1-perception/test_m1_manual_contract.py` | Ensures the business guide contains executable command and safety wording. |

### Task 1: Publish the production opportunity reader

**Files:**
- Modify: `Makefile:15,772-778`
- Modify: `tests/m1-perception/test_makefile_contract.py:3450-3510`

**Interfaces:**
- Produces: `make control-plane-opportunities [limit=1..500] [after_group_id=<opaque-id>]`.
- Consumes: `GET /perception/opportunities` from `src/polyarb/control_plane/api.py:61-98`, whose JSON response is an authenticated `status="available"` projection or whose error remains a failed command.
- Produces: pretty-printed raw JSON so business users can audit fields without shell-side inference.

- [ ] **Step 1: Write the failing Makefile contract test**

Add this test after `test_make_help_exposes_control_plane_production_smoke`:

```python
def test_control_plane_opportunities_is_current_read_only_business_entrypoint() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
    match = re.search(
        r"(?m)^control-plane-opportunities:\n(?P<recipe>(?:\\t.*\\n)+)", makefile
    )
    assert match is not None
    recipe = match.group("recipe")

    assert "https://polyarb-control-api.fly.dev/perception/opportunities" in recipe
    assert "limit=$(or $(limit),50)" in recipe
    assert "after_group_id=$(after_group_id)" in recipe
    assert "curl --disable" in recipe
    assert "--connect-timeout 3" in recipe
    assert "--max-time 10" in recipe
    assert "-f" in recipe
    assert "python -m json.tool" in recipe
    assert not any(
        re.search(rf"\\b{token}\\b", recipe.lower())
        for token in ("flyctl", "deploy", "post", "secret", "dsn", "sqlite", "wallet", "order", "trade")
    )

    result = subprocess.run(
        ["make", "help"], cwd=PROJECT_ROOT, text=True, capture_output=True, timeout=5
    )
    assert result.returncode == 0, result.stderr
    assert "control-plane-opportunities:" in result.stdout
```

- [ ] **Step 2: Run the red test**

Run:

```bash
uv run pytest tests/m1-perception/test_makefile_contract.py \
  -k control_plane_opportunities_is_current_read_only_business_entrypoint -q
```

Expected: FAIL because the target does not yet exist.

- [ ] **Step 3: Add the production reader**

Add `control-plane-opportunities` to the control-plane `.PHONY` declaration that includes `control-plane-status`. Directly after `control-plane-status`, add:

```make
## control-plane-opportunities: Read current certified M1 business opportunities from production; optional limit=1..500 and after_group_id=.
control-plane-opportunities:
	@curl --disable --connect-timeout 3 --max-time 10 --retry 0 -fsS "https://polyarb-control-api.fly.dev/perception/opportunities?limit=$(or $(limit),50)&after_group_id=$(after_group_id)" | python -m json.tool
```

Do not add `|| true`, fallback JSON, or an `items: []` synthesis: curl `-f` must retain 503 as a nonzero command failure.

- [ ] **Step 4: Run contract and live read-only verification**

Run:

```bash
uv run pytest tests/m1-perception/test_makefile_contract.py \
  -k control_plane_opportunities_is_current_read_only_business_entrypoint -q
make -n control-plane-opportunities limit=5 after_group_id=example
make control-plane-opportunities limit=5
```

Expected: the test passes; dry run contains the exact control-plane URL and supplied pagination values; live command returns a JSON projection or exits nonzero rather than presenting an empty successful page.

- [ ] **Step 5: Commit the command contract**

```bash
git add Makefile tests/m1-perception/test_makefile_contract.py
git commit -m "feat(m1): add production opportunity reader"
```

### Task 2: Write the business operating guide and daily log

**Files:**
- Create: `docs/learning/106-M1日常业务情报操作指南.md`
- Create: `docs/ops/m1-daily-business-intelligence-log.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `tests/m1-perception/test_m1_manual_contract.py:760-850`

**Interfaces:**
- Consumes: `make smoke-control-plane-prod`, `make control-plane-status limit=20`, and Task 1's `make control-plane-opportunities limit=50`.
- Produces: a three-step business review and a fixed append-only daily record schema.
- Produces: four explicit conclusions: candidates present, authenticated zero, qualification paused, and business data unavailable.

- [ ] **Step 1: Write the failing documentation contract test**

Add this test before `test_docs_m1_check_make_target`:

```python
def test_daily_business_intelligence_guide_keeps_business_truth_boundaries() -> None:
    guide = (ROOT / "docs/learning/106-M1日常业务情报操作指南.md").read_text()
    log = (ROOT / "docs/ops/m1-daily-business-intelligence-log.md").read_text()
    index = (ROOT / "docs/learning/00-INDEX.md").read_text()

    for command in (
        "`make smoke-control-plane-prod`",
        "`make control-plane-status limit=20`",
        "`make control-plane-opportunities limit=50`",
    ):
        assert command in guide
    for conclusion in ("认证机会", "暂无认证机会", "资格暂停", "业务数据不可用"):
        assert conclusion in guide
    assert "不代表成交、收益或 P&L" in guide
    assert "追加" in log
    assert "北京时间" in log
    assert "106-M1日常业务情报操作指南" in index
```

- [ ] **Step 2: Run the red documentation test**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py \
  -k daily_business_intelligence_guide_keeps_business_truth_boundaries -q
```

Expected: FAIL with a missing-guide-file error.

- [ ] **Step 3: Create the guide and log**

Create the guide with these required sections and literal command blocks:

```markdown
# M1 日常业务情报操作指南

## 30 秒业务心智模型

M1 只观察，不交易：`Structure → Quote → 认证机会 → 资格/异常证据`。
每天先证明读数可信，再判断有没有值得交给 M2 研究的候选；认证机会不代表成交、收益或 P&L。

## 每日三步

1. `make smoke-control-plane-prod`：公共入口是否能读取权威；200/available 不是机会结论。
2. `make control-plane-status limit=20`：市场覆盖、报价推进、qualification、incident 与 watchdog 的业务可信度。
3. `make control-plane-opportunities limit=50`：当前认证机会投影；分页时传 `after_group_id=`。

## 结论矩阵

| 看到的状态 | 日报结论 | 下一步 |
| --- | --- | --- |
| `status=available` 且 `current_opportunity_count>0` | 有认证机会 | 记录 group、edge、容量；仅作为 M2 研究候选 |
| `status=available` 且 count=0 | 暂无认证机会 | 正常记录，不报故障 |
| `eligibility_state=paused` | 资格暂停 | 记录 `eligibility_reason`，观察 Structure/Quote 恢复 |
| HTTP 503 或 `status=unavailable` | 业务数据不可用 | 不写零机会，按 incident/watchdog 排障 |
```

Then add sections titled `可记录的业务数据`, `不要据此下的结论`, `代码地图`,
`设计取舍`, `自检题`, and `FAQ 增量`. The business-data section must name Structure
coverage/generation, Quote pointer/freshness, count plus group/event/edge/size from
opportunities, qualification reason, and incident/watchdog lifecycle. The code map
must cite `src/polyarb/control_plane/api.py:61` and
`src/polyarb/control_plane/postgres.py:8809`.

Create `docs/ops/m1-daily-business-intelligence-log.md` with an explicit append-only
rule and this blank record schema:

```markdown
## YYYY-MM-DD（北京时间）

- 采集时间（北京时间）：
- API 可达性（`smoke-control-plane-prod`）：
- 市场覆盖与报价推进（`control-plane-status`）：
- 资格状态与原因：
- 认证机会（count；最多列出值得跟进的 group/event/edge/size）：
- 异常与恢复（incident/watchdog）：
- 当日业务结论：
- 下一次观察 / 升级动作：
- 证据：命令输出摘要或已保存链接；不得写入密钥。
```

Add the guide as row 106 in the Phase 1 index immediately after row 105, with a
summary stating the reader can distinguish business availability, zero certified
opportunities, paused qualification, and unavailable authority.

- [ ] **Step 4: Run documentation verification**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py \
  -k daily_business_intelligence_guide_keeps_business_truth_boundaries -q
uv run pytest tests/m1-perception/test_m1_manual_contract.py -q
make docs-m1-check
```

Expected: all tests pass and `M1 manual contract: OK` is printed.

- [ ] **Step 5: Commit the business documentation slice**

```bash
git add docs/learning/106-M1日常业务情报操作指南.md \
  docs/learning/00-INDEX.md docs/ops/m1-daily-business-intelligence-log.md \
  tests/m1-perception/test_m1_manual_contract.py
git commit -m "docs(m1): add daily business intelligence guide"
```

### Task 3: Record delivery state and verify the complete business path

**Files:**
- Modify: `.planning/CURRENT.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/JOURNAL.md`
- Create: `docs/superpowers/plans/2026-08-31-m1-daily-business-intelligence-TASK-1-SUMMARY.md`
- Create: `docs/superpowers/plans/2026-08-31-m1-daily-business-intelligence-TASK-2-SUMMARY.md`

**Interfaces:**
- Consumes: the two delivered commits and their passed evidence.
- Produces: recoverable project state naming the business-intelligence entrypoints and plan summaries required by the repository process.

- [ ] **Step 1: Update state and write factual summaries**

Update current M1 state to name the three-command business evidence path and the
production opportunity URL. Append a JOURNAL session entry with the command result
and the distinction between `available + 0` and `unavailable`. Write one summary per
Task 1 and Task 2 that contains: changed files, command/test evidence, non-goals, and
the commit SHA. Do not claim a live opportunity count unless it was observed in this
execution session.

- [ ] **Step 2: Run final verification**

Run:

```bash
git diff --check HEAD~2..HEAD
uv run pytest tests/m1-perception/test_makefile_contract.py \
  -k control_plane_opportunities_is_current_read_only_business_entrypoint -q
uv run pytest tests/m1-perception/test_m1_manual_contract.py -q
make docs-m1-check
make planning-status
make smoke-control-plane-prod
make control-plane-opportunities limit=5
git status --short
```

Expected: no whitespace errors; tests and manual checker pass; planning status reports
no drift; readiness returns 200/available; opportunity command yields a projection or
fails nonzero as unavailable; worktree is clean after the final commit.

- [ ] **Step 3: Commit state and plan summaries**

```bash
git add .planning/CURRENT.md .planning/workstreams/m1-perception/STATE.md \
  .planning/JOURNAL.md \
  docs/superpowers/plans/2026-08-31-m1-daily-business-intelligence-TASK-1-SUMMARY.md \
  docs/superpowers/plans/2026-08-31-m1-daily-business-intelligence-TASK-2-SUMMARY.md
git commit -m "docs(m1): record daily intelligence delivery"
```

## Self-review

- Spec coverage: Task 1 implements the official, bounded public opportunity reader and its failure semantics. Task 2 supplies the learning guide, index, and append-only log; it covers business data, decision distinctions, and non-execution limits. Task 3 preserves planning/JOURNAL/SUMMARY discipline and verifies the production path.
- Completeness scan: every implementation and documentation task includes literal required content and exact commands.
- Type/interface consistency: `control-plane-opportunities` uses the API's `limit` and `after_group_id` names everywhere; the guide uses the same target and `current_opportunity_count` return field.
