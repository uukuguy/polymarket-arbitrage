# M1 Runtime Recovery Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use test-driven development task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Make M1’s declared health depend on fresh business work, proven alert delivery, and bounded database capacity, then provide a safe production restoration sequence.

**Architecture:** Keep the existing database-free watchdog as the break-glass pager; deploy the existing transactional outbox consumer as its own Fly app. Add one bounded capacity/readiness projection to the control-plane snapshot and an explicitly approved retention operator for only expired operational rows. Dashboard reads typed aggregate facts, never raw audit tables.

**Tech Stack:** Python 3.12, psycopg/PostgreSQL 16, Alembic, Starlette, Fly Machines, Next.js/TypeScript, pytest, Playwright.

## Global Constraints

- No DSN, token, password, SQL text, or provider response body reaches logs or Dashboard.
- No automated billing action, database reset, or unapproved deletion.
- Retention must preserve current/previous published generations, unresolved incidents, pending alerts, and qualification certificates.
- A liveness HTTP 200 or Fly `started` state is never sufficient for business health.
- All executable operator entrypoints are Makefile targets.

---

### Task 1: Make watchdog paging independent of the writer acknowledgement

**Files:**
- Modify: `src/polyarb/control_plane/watchdog.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Test: `tests/m1-perception/test_control_plane_watchdog.py`

**Interfaces:**
- Produces `RuntimeObservation` transition delivery with a redacted `pager_outcome` of `direct-delivered`, `writer-rejected`, or `pager-failed`.
- Preserves `run_watchdog_service(observe, send, persist_transition, ...)` as the public service entrypoint.

- [ ] **Step 1: Write failing tests** asserting that an HTTP 500-equivalent writer error sends one direct Telegram page on the first unhealthy tick, that identical subsequent unhealthy ticks do not storm Telegram, and that recovery sends one recovery page.
- [ ] **Step 2: Run** `uv run pytest tests/m1-perception/test_control_plane_watchdog.py -k 'writer or pager' -q` and confirm the new expectations fail.
- [ ] **Step 3: Implement** a typed writer-rejection exception path in `cli_control_plane.py` and make `run_watchdog_service()` record/emit the direct page when the writer cannot acknowledge a transition.
- [ ] **Step 4: Run** the focused watchdog test command and confirm it passes.
- [ ] **Step 5: Commit** `fix(m1): page directly when runtime writer rejects alerts`.

### Task 2: Make transactional outbox delivery a deployable, observable service

**Files:**
- Modify: `src/polyarb/control_plane/rollout.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `deploy/control-plane/fly-control-alert-delivery.toml.template`
- Modify: `Makefile`
- Test: `tests/m1-perception/test_control_plane_deployment_templates.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`
- Test: `tests/m1-perception/test_control_plane_api.py`

**Interfaces:**
- Produces a rendered `alert_delivery_config` and requires its app identity in the staged rollout manifest.
- Adds `alert_delivery` to `operational_snapshot()` with `pending_count`, `oldest_pending_age_seconds`, `latest_delivery_at`, and `state`.
- Adds `make control-plane-alert-delivery-preflight expected_database=postgres` as a read-only topology/queue gate.

- [ ] **Step 1: Write failing deployment and snapshot tests** proving a rollout that omits the alert-delivery app is rejected, and that a two-minute-old pending Telegram entry yields a critical delivery state.
- [ ] **Step 2: Run** the three focused pytest modules and confirm the assertions fail because rollout and snapshot do not expose this service.
- [ ] **Step 3: Implement** the seventh rendered app input, its strict secret-name/topology checks, snapshot aggregation, API serialization, and Makefile read-only preflight target.
- [ ] **Step 4: Run** the focused pytest modules plus `make -n control-plane-alert-delivery-preflight expected_database=postgres`; confirm all pass and the target is discoverable through `make help`.
- [ ] **Step 5: Commit** `feat(m1): close alert delivery deployment and visibility gap`.

### Task 3: Add capacity observations and a protected retention operator

**Files:**
- Create: `alembic/versions/041_m1_runtime_capacity_retention.py`
- Create: `src/polyarb/control_plane/capacity.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/api.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `Makefile`
- Test: `tests/alembic/test_041.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`
- Test: `tests/m1-perception/test_control_plane_api.py`

**Interfaces:**
- `measure_database_capacity(control_plane, now) -> CapacityObservation` returns only aggregate bytes, state, and ten capped relation summaries.
- `plan_runtime_retention(control_plane, cutoff, protected_generation_keys) -> RetentionPlan` is read-only.
- `execute_runtime_retention(..., approval='DELETE_EXPIRED_M1_RUNTIME_EVIDENCE') -> RetentionReceipt` is the only destructive path.

- [ ] **Step 1: Write failing tests** for 60/75/85 percent thresholds, provider-read-only mapping to `exhausted`, capped relation summaries, protected row survival, dry-run immutability, and rejection of a missing approval token.
- [ ] **Step 2: Run** the new Alembic and control-plane test selectors and confirm failures from absent capacity/retention interfaces.
- [ ] **Step 3: Implement** revision 041, aggregate capacity query, allowlisted retention classes, protected-generation query, receipt storage, typed API fields, and Make targets `control-plane-capacity-status`, `control-plane-retention-plan`, and `control-plane-retention-execute`.
- [ ] **Step 4: Run** all Task 3 tests, `make -n` for each target, and a local disposable-Postgres migration upgrade/downgrade test.
- [ ] **Step 5: Commit** `feat(m1): add capacity budget and protected runtime retention`.

### Task 4: Surface the verdict without mixing operations into business research

**Files:**
- Modify: `dashboard/lib/control-plane.ts`
- Modify: `dashboard/app/control-plane/RuntimeOverview.tsx`
- Modify: `dashboard/app/control-plane/page.tsx`
- Modify: `dashboard/lib/business-overview.ts`
- Modify: `dashboard/app/business/business-ui.tsx`
- Test: `tests/m1-perception/test_business_dashboard_contract.py`
- Test: `dashboard/lib/control-plane.test.ts` or the repository’s existing dashboard test location

**Interfaces:**
- `RuntimeVerdict` has independent `business_freshness`, `alert_delivery`, `capacity`, and `qualification` states.
- Any unavailable/degraded state is rendered as status text and evidence age; it cannot become a numeric zero or an opportunity claim.

- [ ] **Step 1: Write failing decoder/component tests** for healthy, stale business work, alert delivery backlog, critical capacity, and unavailable control-plane responses.
- [ ] **Step 2: Run** the focused TypeScript tests and confirm the new states are rejected or absent.
- [ ] **Step 3: Implement** strict decoder validation and a compact control-plane verdict panel; link business-page blockers to the control-plane evidence without rendering raw audit events in the business page.
- [ ] **Step 4: Run** `make dashboard-typecheck` and the focused tests; use Playwright to capture desktop and narrow-screen error/degraded states.
- [ ] **Step 5: Commit** `feat(m1): expose business, delivery and capacity verdicts`.

### Task 5: Make production restoration an executable gate

**Files:**
- Modify: `Makefile`
- Modify: `docs/dev/control-plane-runbook.md`
- Modify: `docs/learning/00-INDEX.md`
- Create: `docs/learning/NN-m1-runtime-recovery-closure.md`
- Modify: `tools/climb/eval_local.py`
- Test: `tests/climb/test_eval_local.py`
- Test: `tests/m1-perception/test_makefile_contract.py`

**Interfaces:**
- `make m1-runtime-recovery-preflight expected_database=postgres` runs only reads and fails on read-only database, missing delivery app, stale business work, pending delivery breach, or capacity warning.
- `make m1-runtime-recovery-verify` requires an explicit release identity and proves migration 040+, delivered Telegram/outbox receipts, fresh Structure/Quote lineage, and capacity headroom.

- [ ] **Step 1: Write failing Makefile and Climb evaluator tests** for missing delivery/capacity gates and for H-066’s gate profile.
- [ ] **Step 2: Run** those tests and confirm H-066 is unsupported.
- [ ] **Step 3: Implement** the fail-closed Make targets, runbook sequence, teaching guide, H-066 evaluator profile, and state-machine evidence requirements.
- [ ] **Step 4: Run** `make planning-status`, Task 1–5 suites, `make dashboard-typecheck`, and `tools/climb/cycle.sh H-066`.
- [ ] **Step 5: Commit** `docs(m1): operationalize runtime recovery closure` and create the required plan SUMMARY.

### Task 6: Restore production after provider write access returns

**Files:**
- No source edits required; use Task 5 Makefile targets and rendered Fly payloads.

**Interfaces:**
- Accepts the restored Supabase write state and the Task 5 release artifact.
- Produces an immutable, redacted recovery evidence directory and a 24-hour fresh-business soak baseline.

- [ ] **Step 1: Run** `make m1-runtime-recovery-preflight expected_database=postgres`; stop if the provider still reports read-only or capacity is not below the configured warning budget.
- [ ] **Step 2: Deploy** the isolated alert-delivery app through the rendered optimistic Machine payload; verify its exact process group and secret names without exposing values.
- [ ] **Step 3: Run** `make supabase-migrate`, role preflight, and `make m1-runtime-recovery-verify release_id=<revision>`; do not update worker Machines unless all gates pass.
- [ ] **Step 4: If capacity remains critical, run** the approved retention plan and then the exact approved retention execution; save the receipt and rerun preflight.
- [ ] **Step 5: Deploy** the new M1 image, prove one direct Telegram and durable outbox delivery, then require a 24-hour passing fresh-business/delivery/capacity soak before declaring M1 normal.

## Plan self-review

- The plan covers all four design guarantees: business freshness, direct and durable alert delivery, capacity observation, and bounded retention.
- Destructive retention is isolated behind an explicit approval token and is not a prerequisite for code/test work.
- Production execution is separate from implementation and is blocked by the provider’s current read-only state.
