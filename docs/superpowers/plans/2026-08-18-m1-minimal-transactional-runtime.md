# M1 Minimal Transactional Runtime Implementation Plan

> **For agentic workers:** Execute task-by-task with TDD; preserve unrelated dirty files.

**Goal:** Make the transactional-control-plane rollout render a resource-bounded, identity-safe production topology before deploying collection.

**Architecture:** The worker template has three business process groups, not historical duplicate pools or a soak sampler. The renderer accepts no machine identity because those are created by Fly; a deploy verification command renders the alert config only after exact current IDs are inserted by the operator/deploy step. The existing watchdog remains the independent API-plus-machine monitor.

**Tech Stack:** Python 3.12, pytest, Fly TOML templates, Makefile.

## Global Constraints

- No legacy SQLite or `polyarb-l1` runtime reuse.
- API and watchdog remain 256MB; worker roles start one-per-group at 1024MB.
- No secret is written to source, test output, or logs.
- No 24-hour baseline begins until API, watchdog, and durable forward progress pass.

### Task 1: Bound the rendered worker topology

**Files:**
- Modify: `deploy/control-plane/fly-control-worker.toml.template`
- Modify: `tests/m1-perception/test_control_plane_rollout.py`

- [ ] Add a failing contract asserting rendered worker config contains exactly `coordinator`, `structure_range`, and `quote_batch`, has no `soak_sampler`, has no literal 16-character machine IDs, and uses `memory = "1024mb"`.
- [ ] Run `uv run pytest tests/m1-perception/test_control_plane_rollout.py -q`; expect failure because the template still contains five workers plus sampler and 2GB allocation.
- [ ] Replace the process map with:
  ```toml
  coordinator = "python -m polyarb.cli_control_plane serve --enable --worker-id fly-control-plane-coordinator --worker-role coordinator --max-turns 4 --interval-seconds 5 --json"
  structure_range = "python -m polyarb.cli_control_plane serve --enable --worker-id fly-control-plane-structure-range --worker-role structure-range --pool-turns 1 --interval-seconds 5 --json"
  quote_batch = "python -m polyarb.cli_control_plane serve --enable --worker-id fly-control-plane-quote-batch --worker-role quote-batch --pool-turns 1 --interval-seconds 5 --json"
  ```
  and restrict its VM processes to those exact names at 1024MB.
- [ ] Re-run the focused test; expect PASS. Commit `fix(m1): bound transactional worker topology`.

### Task 2: Remove stale machine identities from deploy artifacts

**Files:**
- Modify: `deploy/control-plane/fly-control-alert.toml.template`
- Modify: `deploy/control-plane/fly-control-worker.toml.template`
- Modify: `tests/m1-perception/test_control_plane_rollout.py`

- [ ] Add a failing test that renders all artifacts and asserts none contains the deleted IDs `3d8d0e29c7d589`, `080d3ddbe66068`, `4d895231f7d987`, `85e990c43533e8`, or `86ed91bee33608`.
- [ ] Run the focused test; expect failure from the stale worker sampler and alert watchdog commands.
- [ ] Change template watchdog invocation to require runtime-injected `POLYARB_CONTROL_API_URL`, `POLYARB_WORKER_APP`, and `POLYARB_WORKER_MACHINE_IDS` rather than embedding historical app/IDs; add a small shell-free Python CLI parsing contract if existing CLI does not already support comma-separated IDs.
- [ ] Re-run focused rollout and watchdog tests; expect PASS. Commit `fix(m1): remove stale control plane identities`.

### Task 3: Verify rendering and prepare the formal deployment gate

**Files:**
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_make_perception_contract.py`
- Modify: `docs/learning/64-M1事务控制面.md`

- [ ] Add a failing Make contract for `make control-plane-render-rollout` and a new read-only `make control-plane-watchdog-verify` target that requires explicit API URL, Fly app, and comma-separated machine IDs; it must not deploy, migrate, or access secrets.
- [ ] Run its test; expect failure because the target is absent.
- [ ] Implement the target using `cli_control_plane watchdog-serve` in one-shot/read-only verification mode, document the API→watchdog→workers chain and failure meanings, then run focused tests plus `make planning-status`.
- [ ] Commit `feat(m1): expose control plane watchdog verification` and write/update its mandatory phase SUMMARY before deployment.

## Verification

1. `uv run pytest tests/m1-perception/test_control_plane_rollout.py tests/m1-perception/test_control_plane_watchdog.py tests/m1-perception/test_make_perception_contract.py -q`
2. `uv run ruff check deploy/control-plane src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_rollout.py`
3. Render artifacts to a fresh temporary directory; verify app names resolve and zero stale IDs remain.
4. Only then create the three Fly apps, migrate the new authority, install least-privilege secrets, start watchdog, and require a healthy transition before workers.
