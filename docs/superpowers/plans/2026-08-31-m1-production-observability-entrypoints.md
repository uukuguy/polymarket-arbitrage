# M1 Production Observability Entrypoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace dead L1/L2 production-health Makefile probes with an explicit,
read-only control-plane probe and fail-loud retired aliases.

**Architecture:** The public probe calls only the current control API strict
`/health`, while the existing DSN-backed `control-plane-status` remains the
separate business-truth command. Retired L1/L2 target names remain discoverable
but exit nonzero with both replacement commands; they do not silently acquire
different semantics.

**Tech Stack:** GNU Make, curl, jq, pytest, Markdown.

## Global Constraints

- The public endpoint is `https://polyarb-control-api.fly.dev/health`.
- A public-health success requires HTTP `200` and JSON
  `status="ok", control_plane="available"`.
- No target in this change may deploy, restart, scale, mutate secrets, write the
  database, or require a DSN/Fly credential.
- `make control-plane-status` remains the authenticated durable business-truth
  reader and is not a substitute for public API reachability.
- Historical JOURNAL entries remain append-only; only current operator guidance
  is changed.

---

### Task 1: Make the production-health contract fail loud and testable

**Files:**
- Modify: `Makefile:492-590,1646-1652`
- Modify: `tests/m1-perception/test_m1_manual_contract.py:786-850`
- Modify: `tests/m1-perception/test_makefile_contract.py:3469-3495`

**Interfaces:**
- Produces: `make smoke-control-plane-prod` — unauthenticated public strict
  readiness probe.
- Produces: six retained retired targets that exit `2` and print both
  `make smoke-control-plane-prod` and `make control-plane-status`.
- Consumes: the stable `GET /health → {"status":"ok","control_plane":"available"}`
  contract from `src/polyarb/control_plane/api.py`.

- [ ] **Step 1: Write failing Makefile contract tests**

Replace the assertions that require old Fly hostnames with tests that extract
`smoke-control-plane-prod` and assert all of:

```python
assert "curl --disable" in recipe
assert re.search(r"--request\\s+GET\\b", recipe)
assert "https://polyarb-control-api.fly.dev/health" in recipe
assert 'jq -e \' .status == "ok" and .control_plane == "available" \' not in recipe
assert "control_plane" in recipe
assert "/healthz" not in recipe
assert not any(re.search(rf"\\b{token}\\b", recipe.lower()) for token in forbidden)
```

Use a simpler robust assertion for the jq gate in the actual test:

```python
assert '.status == "ok" and .control_plane == "available"' in recipe
```

For each retired target, extract its recipe and require a nonzero `exit 2` plus
both replacement command strings. Update the Make help dry-run test to require
`smoke-control-plane-prod` and to assert that `make -n smoke-health-prod`
contains no `fly.dev` hostname.

- [ ] **Step 2: Run the focused tests and observe failure**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py \
  -k 'smoke_health_prod or smoke_l2_health_strict_prod' -q
uv run pytest tests/m1-perception/test_makefile_contract.py \
  -k 'market_truth_production_smoke or make_help_exposes_market_truth' -q
```

Expected: failures because the new target and retired-target contract do not
yet exist, while tests still expect the removed L1/L2 endpoints.

- [ ] **Step 3: Implement the targets**

Add this target beside existing production health targets and include it in the
corresponding `.PHONY` declaration:

```make
## smoke-control-plane-prod: Read-only strict public readiness of the current M1 control API.
smoke-control-plane-prod:
	@BODY=$$(mktemp); trap 'rm -f "$$BODY"' EXIT; \
	URL="https://polyarb-control-api.fly.dev/health"; \
	echo ">> smoke-control-plane-prod — GET $$URL"; \
	HTTP_STATUS=$$(curl --disable --request GET -sS -o "$$BODY" -w "%{http_code}" "$$URL") || { rc=$$?; echo "FAIL: request error" >&2; exit $$rc; }; \
	echo "HTTP $$HTTP_STATUS"; \
	python3 -m json.tool < "$$BODY" || cat "$$BODY"; \
	if [ "$$HTTP_STATUS" != "200" ] || ! jq -e '.status == "ok" and .control_plane == "available"' "$$BODY" >/dev/null; then \
		echo "FAIL: control-plane strict readiness is unavailable" >&2; exit 1; \
	fi; \
	echo "PASS: control-plane strict readiness returned 200/available"
```

Replace each old production target recipe with this exact fail-loud pattern,
preserving its name only for operator migration:

```make
## smoke-health-prod: Retired L1 probe; use smoke-control-plane-prod plus control-plane-status.
smoke-health-prod:
	@echo "RETIRED: polyarb-l1 no longer exists." >&2; \
	 echo "Use: make smoke-control-plane-prod  # public API readiness" >&2; \
	 echo "Use: make control-plane-status       # durable business truth" >&2; \
	 exit 2
```

Apply the same form to `smoke-market-truth-prod`, `smoke-healthz`,
`smoke-l2-health-prod`, `smoke-l2-health-strict-prod`, and `fly-l2-status`.
Do not edit local daemon targets or chaos targets in this task.

- [ ] **Step 4: Run focused regression tests**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py \
  -k 'smoke_health_prod or smoke_l2_health_strict_prod' -q
uv run pytest tests/m1-perception/test_makefile_contract.py \
  -k 'market_truth_production_smoke or make_help_exposes_market_truth' -q
make -n smoke-control-plane-prod
make -n smoke-health-prod
```

Expected: all selected tests pass; the new dry run contains only GET/curl/jq
logic, and the retired dry run contains only migration messages and `exit 2`.

- [ ] **Step 5: Commit the contract slice**

```bash
git add Makefile tests/m1-perception/test_m1_manual_contract.py \
  tests/m1-perception/test_makefile_contract.py
git commit -m "fix(m1): replace retired production health probes"
```

### Task 2: Align operator guidance and current state

**Files:**
- Modify: `docs/M1-市场感知平台使用手册.md:170-171,700-715,750-751`
- Modify: `scripts/check_m1_manual.py:35-100`
- Modify: `tests/m1-perception/test_m1_manual_contract.py:830-855`
- Modify: `.planning/CURRENT.md`
- Modify: `.planning/JOURNAL.md`

**Interfaces:**
- Consumes: `smoke-control-plane-prod` from Task 1 and unchanged
  `control-plane-status`.
- Produces: a daily read-only runbook that distinguishes public readiness from
  durable business state, with no current instructions targeting retired apps.

- [ ] **Step 1: Write failing documentation-contract tests**

Replace current-manual assertions for old smoke targets with assertions that
the daily and read-only sections contain both commands and their distinct
meanings:

```python
for section in (daily, read_only):
    assert "`make smoke-control-plane-prod`" in section
    assert "`make control-plane-status`" in section
assert "公开控制 API" in text
assert "durable business truth" in text
assert "polyarb-l1.fly.dev/health" not in daily
assert "polyarb-l2.fly.dev/health" not in daily
```

Update `M1_MAKE_TARGETS` in `scripts/check_m1_manual.py` to include
`smoke-control-plane-prod`; keep retired targets listed only while the manual
mentions their migration status.

- [ ] **Step 2: Run the documentation contract red test**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py \
  -k 'manual_routes_l2_strict_health or manual_keeps_reviewed_operator_safety_facts' -q
make docs-m1-check
```

Expected: failure because current operator instructions still advertise L1/L2
health endpoints as daily production checks.

- [ ] **Step 3: Update current guidance and state**

Replace the daily inspection instructions with this sequence:

```markdown
1. `make smoke-control-plane-prod` proves the public strict control API can
   read its durable authority. It does not prove qualification or Opportunity
   freshness.
2. `make control-plane-status` reads durable task, lease, circuit, incident,
   watchdog and qualification facts. `qualification.paused(freshness.opportunity)`
   is a business non-readiness result, not a transport failure.
3. The former `smoke-*-prod` L1/L2 targets are retired because their Fly apps
   no longer exist; they fail loud and print these replacements.
```

Change all current daily/read-only references to the retired targets. Preserve
historical release notes and append-only Journal history. Update `.planning/CURRENT.md`
to name the two-command read-only evidence pair. Append a JOURNAL entry that
records the retired DNS names, current control API identity, and the semantic
split.

- [ ] **Step 4: Run documentation and planning verification**

Run:

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py -q
make docs-m1-check
make help | rg 'smoke-control-plane-prod|smoke-health-prod'
make planning-status
git diff --check
```

Expected: manual contract and checker pass; help lists the new command and
marks old targets retired; planning status reports no drift.

- [ ] **Step 5: Record live read-only evidence and commit**

Run:

```bash
FLY_API_TOKEN= flyctl status -a polyarb-control-api --json
make smoke-control-plane-prod
git add docs/M1-市场感知平台使用手册.md scripts/check_m1_manual.py \
  tests/m1-perception/test_m1_manual_contract.py .planning/CURRENT.md \
  .planning/JOURNAL.md
git commit -m "docs(m1): align production observability runbook"
```

Expected: Fly reports the control API started with `/healthz` passing. The
public smoke either passes on a normal external path or records a local proxy
transport failure without being misclassified as a service outage.

## Plan Self-Review

- Spec coverage: Task 1 implements the explicit public probe, fail-loud retired
  names and automated safety contract; Task 2 updates discoverability, current
  state and append-only history.
- Scope: local-daemon, deployment, chaos and historical-JOURNAL machinery are
  deliberately unchanged.
- Consistency: all tasks use the sole new target name
  `smoke-control-plane-prod` and the stable `GET /health` response contract.
