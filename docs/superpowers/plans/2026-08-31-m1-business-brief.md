# M1 业务情报简报 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one read-only command that turns existing M1 durable status and certified opportunities into a human business brief or stable JSON summary.

**Architecture:** A small CLI module reads the existing scoped-DSN status authority and the public certified opportunity projection, validates each response, then creates one canonical brief dict. Text and JSON render only that dict; the Makefile is a narrow safe command façade.

**Tech Stack:** Python 3.12, urllib, GNU Make, pytest, Markdown.

## Global Constraints

- No schema, scheduler, deployment, secret, wallet, order, trade, or database write.
- M1 is observe-only; opportunity/edge/size never imply fill, P&L, or execution authority.
- Authority failure is `业务数据不可用` and a nonzero process exit, never zero opportunities.
- Default text and `format=json` derive from one summary; opportunities are capped at five in the brief.
- The public endpoint remains `https://polyarb-control-api.fly.dev/perception/opportunities` with bounded GET semantics.

---

### Task 1: Canonical brief reader and renderer

**Files:**
- Create: `src/polyarb/control_plane/business_brief.py`
- Create: `tests/m1-perception/test_business_brief.py`

**Interfaces:**
- Produces: `build_business_brief(status: Mapping[str, object], opportunities: Mapping[str, object]) -> dict[str, object]`.
- Produces: `render_business_brief(brief: Mapping[str, object]) -> str`.
- Produces: `BusinessBriefUnavailable` for malformed/unavailable authority.

- [ ] **Step 1: Write failing fixture tests**

Create a status fixture containing `.structure`, `.quote`, `.qualification`, `.open_incidents`, `.runtime_incidents`, `.recovery_actions`, `.runtime_watchdog` and an available opportunity fixture with six items. Require:

```python
brief = build_business_brief(status, opportunities)
assert brief["status"] == "available"
assert brief["opportunities"]["count"] == 6
assert len(brief["opportunities"]["items"]) == 5
assert "今日结论" in render_business_brief(brief)
assert "市场覆盖（Structure）" in render_business_brief(brief)
assert "异常与恢复" in render_business_brief(brief)
```

Add unavailable cases for opportunity `status="unavailable"` and missing qualification, asserting `BusinessBriefUnavailable`.

- [ ] **Step 2: Run red tests**

```bash
uv run pytest tests/m1-perception/test_business_brief.py -q
```

Expected: import failure because the module does not exist.

- [ ] **Step 3: Implement minimal canonical mapping**

Implement strict mapping access helpers. Emit exactly:

```python
{
  "status": "available",
  "conclusion": {"eligibility_state": ..., "eligibility_reason": ..., "escalate": bool},
  "structure": status["structure"], "quote": status["quote"],
  "opportunities": {"count": opportunities["current_opportunity_count"], "items": opportunities["items"][:5]},
  "incidents": {"open": status["open_incidents"], "runtime": status["runtime_incidents"], "recovery_actions": status["recovery_actions"], "watchdog": status["runtime_watchdog"]},
}
```

`escalate` is true when qualification is paused or any open/runtime incident is present. Render five labelled text sections without calculating edge totals or P&L.

- [ ] **Step 4: Run green tests and commit**

```bash
uv run pytest tests/m1-perception/test_business_brief.py -q
git add src/polyarb/control_plane/business_brief.py tests/m1-perception/test_business_brief.py
git commit -m "feat(m1): add canonical business brief"
```

### Task 2: Safe command and Makefile entrypoint

**Files:**
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Test: `tests/m1-perception/test_business_brief.py`

**Interfaces:**
- Produces: `make control-plane-business-brief [format=text|json]`.
- Consumes: Task 1 mapping plus scoped DSN `control-plane-status` authority and public opportunity GET.

- [ ] **Step 1: Write failing CLI/Makefile tests**

Require parser rejection for `format=xml`, Make help exposure, exact target name, and recipe prohibition of Fly deploy/secret/order/trade tokens. Mock the status reader and urllib request; require JSON mode to `json.loads(stdout)` equal the canonical brief and unavailable response to return nonzero with `业务数据不可用`.

- [ ] **Step 2: Run red tests**

```bash
uv run pytest tests/m1-perception/test_business_brief.py tests/m1-perception/test_makefile_contract.py -k business_brief -q
```

Expected: missing CLI subcommand and Make target.

- [ ] **Step 3: Implement command**

Add `business-brief` parser arguments `--format` choices `text,json` and `--limit` fixed default `50`. Reuse `PostgresControlPlane.status(limit=20)` with scoped DSN. Fetch opportunities through `Request`/`urlopen` using a 10-second timeout, validate HTTP/JSON, build Task 1 brief, print text or sorted JSON. Convert all authority/read failures to a redacted `业务数据不可用` stderr message and exit 2.

Add:

```make
## control-plane-business-brief: Read a one-screen M1 business brief; optional format=text|json.
control-plane-business-brief:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	@test -n "$$POLYARB_SUPABASE_DB_DSN" || (echo "ERROR: POLYARB_SUPABASE_DB_DSN is required" >&2; exit 2); \
	uv run python -m polyarb.cli_control_plane business-brief --format "$(or $(format),text)"
```

Include target in `.PHONY`; retain the existing `control-plane-status` and `control-plane-opportunities` audit readers unchanged.

- [ ] **Step 4: Verify and commit**

```bash
uv run pytest tests/m1-perception/test_business_brief.py tests/m1-perception/test_makefile_contract.py -k business_brief -q
make -n control-plane-business-brief format=json
git add src/polyarb/cli_control_plane.py Makefile tests/m1-perception/test_makefile_contract.py tests/m1-perception/test_business_brief.py
git commit -m "feat(m1): expose business intelligence brief"
```

### Task 3: Explain the reading layers and record delivery

**Files:**
- Modify: `docs/learning/106-M1日常业务情报操作指南.md`
- Modify: `tests/m1-perception/test_m1_manual_contract.py`
- Modify: `.planning/CURRENT.md`, `.planning/workstreams/m1-perception/STATE.md`, `.planning/JOURNAL.md`
- Create: `docs/superpowers/plans/2026-08-31-m1-business-brief-TASK-1-SUMMARY.md`
- Create: `docs/superpowers/plans/2026-08-31-m1-business-brief-TASK-2-SUMMARY.md`

- [ ] **Step 1: Write a failing guide contract**

Require the guide to contain `make control-plane-business-brief`, `format=json`, `最多 5`, `control-plane-status`, `control-plane-opportunities`, and `不代表成交、收益或 P&L`.

- [ ] **Step 2: Update guide and state**

Make the brief the daily default. Explain default text, JSON automation, and audit drill-down. State that nonzero means `业务数据不可用`. Append a JOURNAL entry with factual test/live evidence and write plan summaries including changed files, verification, non-goals, and commit SHA.

- [ ] **Step 3: Verify and commit**

```bash
uv run pytest tests/m1-perception/test_business_brief.py tests/m1-perception/test_m1_manual_contract.py -q
make docs-m1-check
make planning-status
git diff --check
git add docs/learning/106-M1日常业务情报操作指南.md tests/m1-perception/test_m1_manual_contract.py .planning docs/superpowers/plans/2026-08-31-m1-business-brief-TASK-1-SUMMARY.md docs/superpowers/plans/2026-08-31-m1-business-brief-TASK-2-SUMMARY.md
git commit -m "docs(m1): explain business intelligence brief"
```

## Self-review

- Task 1 supplies one strict canonical shape; Task 2 transports it without reinterpreting it; Task 3 teaches and records it.
- Every spec requirement maps to one task, including unavailable semantics, five-item cap, JSON mode, Make entrypoint, and guide boundary.
- No open-ended implementation markers or unspecified test steps remain.
