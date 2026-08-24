# Task 5 Report — Cross-job transactional runtime coverage

## Status

Implemented, verified, summarized, and committed locally on
`feat/m1-self-healing`. No production deployment, Fly command, live control-plane
mutation, wallet/signing, or trading action was performed.

## Scope delivered

- Created `tests/m1-perception/test_transactional_runtime_coverage.py`.
- Created
  `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-202-SUMMARY.md`.
- Added a minimal runtime reporter guard rejecting secret-like progress detail
  keys before any store call.
- Performed mechanical Ruff-only line wrapping in the specified Ruff scope so
  the exact brief command passes.

## RED evidence

Command:

```bash
uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py -q
```

Result: exit 1.

Expected failure reason:

- The new coverage file collected and ran.
- The persisted cross-job fixture and registry-shape checks were not setup
  failures.
- The RED was specifically the missing coverage contract for secret-like
  runtime detail keys:

```text
Failed: DID NOT RAISE <class 'ValueError'>
```

This occurred for the seven bound key shapes:

- `api_key`
- `apikey`
- `authorization`
- `credential`
- `password`
- `secret`
- `token`

The fake store remained callable before the fix, proving the reporter did not
reject those keys before persistence.

## GREEN and gate evidence

After the minimal runtime reporter guard:

```bash
uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py -q
```

Result: exit 0; 9 tests passed.

Complete task gate after commits:

```bash
uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py tests/m1-perception/test_transactional_* -q
```

Result: exit 0.

Ruff gate after commits:

```bash
uv run ruff check src/polyarb/control_plane tests/m1-perception
```

Result:

```text
All checks passed!
```

Planning gate after summary:

```bash
make planning-status
```

Result: exit 0; final line:

```text
✓ no drift detected — every shipped plan has a SUMMARY.
```

## Runtime coverage contract

The Task 5 test asserts the real `RUNTIME_STAGE_REGISTRY` has exactly these
eight job types:

1. `structure-fetch`
2. `structure-materialize`
3. `structure-normalize`
4. `structure-certify`
5. `quote-admit`
6. `quote-batch`
7. `quote-certify`
8. `opportunity-certify`

For each job type, the test uses a real Postgres-backed `PostgresControlPlane`
fixture to enqueue and claim one job, persist every registered progress stage,
append one terminal `job.succeeded`, and assert:

- exactly one `job.started`;
- exactly one stage-progress event per registered stage;
- exactly one terminal success event;
- contiguous event sequences;
- terminal stage/progress equals the final registered stage;
- no secret-like detail keys are present in persisted runtime events.

The fixture uses the same default production-derived deadline profile for a
120-second lease:

- `policy_version = runtime-v1`
- `lease_seconds = 120`
- `heartbeat_seconds = 30`
- `progress_seconds = 120`
- `attempt_seconds = 1200`

## 207-second regression coverage

The full transactional gate includes
`test_quote_admitter_long_runtime_keeps_lease_live_for_207_simulated_seconds`
from `tests/m1-perception/test_transactional_quote_admission.py`. That
regression remains in the passing gate and covers at least six fenced
heartbeats, monotonic shard/batch progress, no expired lease observation, and
unchanged terminal Quote identities across 207 simulated seconds.

## Files changed

Committed implementation/test files:

- `tests/m1-perception/test_transactional_runtime_coverage.py` — new cross-job
  registry and persisted-event coverage gate.
- `src/polyarb/control_plane/runtime_contract.py` — minimal pre-persistence
  secret-like detail-key rejection.
- `src/polyarb/control_plane/postgres.py` — mechanical Ruff line wrapping only.
- `src/polyarb/control_plane/watchdog.py` — mechanical Ruff line wrapping only.
- `tests/m1-perception/test_control_plane_postgres.py` — mechanical Ruff line
  wrapping only.
- `tests/m1-perception/test_control_plane_watchdog.py` — mechanical Ruff line
  wrapping only.
- `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-202-SUMMARY.md`
  — Plan 02 closure summary.

Report file:

- `.superpowers/sdd/task-5-report.md` — this report; stale unrelated content
  was fully replaced.

## Commits

- `3fada81c` — `test(05.6-202): cover transactional runtime jobs`
- `f611eb0b` — `docs(05.6-202): close runtime instrumentation plan`
- This report commit — `docs(05.6-202): record runtime coverage task report`

## Self-review

- TDD order was preserved: the coverage test was written and run RED before the
  runtime reporter implementation change.
- The RED was not a syntax or fixture setup error; it identified a real contract
  gap at the reporter boundary.
- The production change is narrowly scoped to rejecting secret-like detail keys
  before persistence.
- The cross-job test uses the real closed runtime registry and real persisted
  event rows rather than mocking the database contract.
- Existing user/controller changes were not staged or committed.
- No climb state or adapter files were staged or committed, despite concurrent
  dirty climb status files appearing in the shared worktree.

## Deviations / concerns

- The exact Ruff command initially failed on pre-existing line-length findings
  outside the new test file. I applied behavior-preserving formatting-only wraps
  in the specified Ruff scope so the required gate passes.
- `make planning-status` still does not list plan `05.6-202` in the displayed
  workstream table, but it exits 0 and reports no drift after the new summary
  exists.
- The worktree still contains unrelated uncommitted changes owned by the
  controller/user or concurrent agents:
  `.planning/JOURNAL.md`,
  `.planning/threads/market-observation-architecture.md`,
  `.superpowers/sdd/progress.md`, and `docs/status/climb/*`.
  They were preserved and not committed by this task.

## Review fix — real terminal boundaries and bounded guards

Review result: Needs fixes. I fixed the Important finding and both Minors.

### Review-fix RED evidence

Command:

```bash
uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py -q
```

Result: exit 1 with two expected failures:

- `test_runtime_coverage_gate_uses_real_terminal_boundaries_and_fails_closed`
  failed because the coverage file still contained the private
  `_append_job_succeeded_cursor` terminal bypass.
- `test_runtime_reporter_rejects_unbounded_detail_before_persistence` failed
  with `Failed: DID NOT RAISE <class 'ValueError'>`, proving 21 runtime detail
  keys could reach the fake store before bounded validation.

### Fixes applied

- Replaced the generic fake terminal completion with real public/specialized
  terminal boundaries for all eight job types:
  - `structure-fetch` → `record_structure_source_page`
  - `structure-materialize` → `admit_structure_source_bundle`
  - `structure-normalize` → `complete_structure_range`
  - `structure-certify` → `certify_structure_generation`
  - `quote-admit` → `admit_quote_generation`
  - `quote-batch` → `record_quote_batch(..., terminal=True)`
  - `quote-certify` → `certify_quote_generation`
  - `opportunity-certify` → `publish_opportunity_projection(..., lease=...)`
- Kept exact eight-type registry coverage and the one
  start/progress-chain/terminal/no-secret assertions, now filtered by each real
  terminal job key so prerequisite jobs cannot satisfy the gate.
- Replaced Docker absence skip with fail-closed `pytest.fail(...)` carrying an
  actionable Docker/Testcontainers message.
- Extracted shared bounded runtime detail validation into
  `runtime_models.validate_runtime_detail_bounds(...)`; the reporter invokes it
  before scanning secret-like keys, preventing unbounded key iteration. Existing
  `RuntimeEvent` specialized error semantics are preserved by deferring encoded
  byte-size validation until after per-field normalization.

### Review-fix GREEN evidence

Focused coverage:

```bash
uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py -q
```

Result: exit 0; 18 tests passed.

Focused runtime/model/Postgres compatibility:

```bash
uv run pytest tests/m1-perception/test_control_plane_runtime_contract.py tests/m1-perception/test_control_plane_runtime_models.py tests/m1-perception/test_control_plane_postgres.py -k 'runtime_detail or runtime_event or runtime_progress or runtime' -q
```

Result: exit 0.

Complete transactional gate:

```bash
uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py tests/m1-perception/test_transactional_* -q
```

Result: exit 0.

Ruff:

```bash
uv run ruff check src/polyarb/control_plane tests/m1-perception
```

Result:

```text
All checks passed!
```

Planning status:

```bash
make planning-status
```

Result: exit 0; no drift detected.
