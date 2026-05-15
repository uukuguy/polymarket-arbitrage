# Plan 02-09 Verification

**Checked:** 2026-05-15
**Plan path:** `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-09-PLAN.md`
**Verdict:** NEEDS-REVISION (2 blockers, 5 warnings, 3 nits)

---

## Goal alignment

Goal-backward check against D-23's five acceptance criteria:

| Criterion | Addressed? | Where | Confidence |
|---|---|---|---|
| 1. Survive 7-day soak on 256MB Fly VM | Partial — Task 6 verifies ≥2 cron ticks / ≥1 hour, NOT 7 days | T6 step 4 | Medium — soak target is Plan 02-07 per `<dependencies_satisfied>` |
| 2. `AsyncIterator[dict]` paginator (never accumulate list internally) | Yes — T3 explicit | gamma_client.py refactor | High |
| 3. Streaming consumer in orchestrator (per-page normalize → write → discard) | **Partial — see Blocker B-1** | T4 phase 2 + 7 | **Low** — phase 2 collects target_markets fully, phase 7 streams an already-materialized list |
| 4. NOT upgrade VM memory | Yes — explicit in `<constraint>` + `<out_of_scope>` #5 | — | High |
| 5. Apply to both `/markets` and `/events` | Defers `/events` per Decision A (justified ~5MB peak) | gamma_client.py + decision A | High — explicit justified deferral, matches "though plan may justify deferring" in goal |

**Concrete acceptance — peak RSS ≤ 130MB on 20k markets:** Task 5 implements regression test asserting `peak_abs < 130MB` (`PEAK_RSS_BUDGET_BYTES = 130 * 1024 * 1024`). Implementation specified completely. ✓

**Triggering incident addressed?** Yes — Plan correctly identifies root cause (`out: list[dict]` in `_paginate`) and the architectural fix removes the full-buffer pattern. Task 1-4 commit messages reference D-23 amendment.

---

## Findings

### BLOCKERS (must fix before execute)

#### B-1: Memory budget table arithmetic is internally inconsistent and probably wrong

`02-09-PLAN.md:264-277` — the memory budget recap claims:

| Component | Plan estimate |
|---|---|
| raw_events | ~5MB |
| market_to_event_map | ~2MB |
| seen_ids (50k strs) | ~5MB |
| target_markets | <1MB |
| Current Gamma page | ~1MB |
| Parquet write batch buffer | ~1MB |
| validation issues | <1MB |
| CLOB books_by_token | ~5MB |
| **Subtotal** | ~20MB |
| **Plan claim** | "**~25MB above baseline Python runtime (~80MB)**" → "Total expected peak RSS: ~105MB" |

**Problems:**

1. **Baseline is wrong.** Plan says "baseline Python runtime ~80MB". The empirical baseline from `02-04-SUMMARY.md:49-87` and the prior synthetic-vs-real profiling lesson (`feedback_profile-with-real-data-2026-05`) does not support 80MB. The actual fly machine pre-snapshot RSS observed at first deploy was higher — the test asserts `peak_abs < 130MB` (absolute), so if the real Python baseline + httpx + pyarrow + sqlite + uvicorn imports is ~100-118MB (typical for this stack with pyarrow loaded), then the budget is `≤30MB transient over baseline`, not `~25MB`.

2. **target_markets size estimate undercounts.** Plan says "<1MB" assuming ~500 dicts × ~2KB. But each `target_markets` dict in phase 5 gets `best_bid_price`/`best_ask_price`/`best_bid_size`/`best_ask_size`/`fetched_at_ms` attached **plus** the entire CLOB book as a transient reference during attach. Actual size in subset mode is in the right ballpark, but the plan does not back this with empirical data — see also B-2.

3. **No accounting for the parquet `pa.Table.from_pylist(batch)` intermediate.** Each 500-row batch in `write_parquet_streaming` constructs a transient `pa.Table` whose Arrow C-side allocation is not counted in the budget table. For SNAPSHOT_SCHEMA (22 fields) × 500 rows, expect ~1-3MB transient — small, but the plan budget makes no room for Arrow's allocator overhead, and `tracemalloc` would not see it.

4. **No accounting for httpx HTTP/2 connection state and pyarrow's module-init heap.** Both are non-trivial for a 256MB box.

**Why this is a blocker (not a warning):** The whole point of the plan is to fit in 150MB usable. If the arithmetic table is wrong by 30MB, the regression test will fail in T5 and the executor will not know whether to (a) loosen the budget, (b) hunt down more savings, or (c) declare the architecture wrong. The plan must show a budget table grounded in measurable numbers from prior runs, not handwaved estimates.

**Suggested resolution:**
- Replace the memory budget table with a calibration step at the START of Task 5: measure `psutil.Process().memory_info().rss` after `import polyarb.snapshot.orchestrator` + `Settings(...)` + before any `run_snapshot` call. Pin that as `baseline_rss`. State the budget as `peak_delta = peak_abs - baseline_rss MUST be < (130MB - measured_baseline_rss)`.
- Add a calibration test that fails fast with a useful number, instead of asserting an absolute 130MB that nobody knows the slack on.

#### B-2: Plan handwaves the subset-mode target_markets size with no empirical anchor

`02-09-PLAN.md:222-226` (Decision G) and risk R5: "subset mode typically produces a few hundred target_markets". `02-09-PLAN.md:1607` says "$1k filters to <1k markets today".

But:
- Default `liquidity_threshold_usd = 1000.0` (config.py:44) — NOT $10k as Task 5 test uses (`liquidity_threshold_usd=10000.0` on line 1184).
- The plan never cites the actual production observation. `01.1-LEARNINGS.md` mentions 20353 markets total at LIVE-RUN-005 but no post-filter count.
- If the prod threshold is $1k and real Polymarket has, say, 3000 markets with liquidity > $1k, then target_markets is **6x the "few hundred" estimate** — and at ~2KB each that's still only ~6MB, fine. But: the **test in T5 uses $10k** so it filters out ~80% (a few hundred target_markets). The test does not exercise the production threshold of $1k.

**Why blocker:** the test does not validate the actual production memory profile. A passing T5 will deploy to prod where target_markets is 5-10x larger, potentially pushing past 130MB after the CLOB phase attaches books to all of them.

**Suggested resolution:**
- T5 must run with `liquidity_threshold_usd=1000.0` (production default) OR
- T5 must include a second test variant at the production threshold OR
- The plan must record an explicit observation from a prior live snapshot (`make snapshot-markets` output of `target_markets` count) and adjust the test threshold to produce a comparable count.
- T6 step 1 (docker --memory=256m smoke) MUST use the production threshold, not the test threshold, to close the gap.

---

### WARNINGS (should fix but not blocking)

#### W-1: `GammaClient` is opened twice in Task 4's refactored orchestrator — inconsistent with current code

Current orchestrator opens ONE `async with GammaClient(settings) as gamma:` in phase 1 covering both events and markets fetch (`orchestrator.py:190`). Task 4's outline at lines 866-883 opens a fresh `GammaClient` in phase 2 for the markets stream — meaning the events fetch in phase 1 also needs a Gamma client. The plan says "Events: unchanged" but the existing client context manager spans both 1a and 1b. If phase 2 reopens a Gamma client, that's two `httpx.AsyncClient` instances created sequentially (~minor memory + connection re-establishment cost). The plan does not specify whether the phase-1 client is kept open until phase-2 ends, or two clients are sequentially created. It matters for HTTP/2 keepalive and for clean shutdown order.

**Fix:** Task 4 outline at lines 870-885 should explicitly show the single `async with GammaClient(settings) as gamma:` enclosing both events fetch and markets stream, OR justify why two sequential clients is acceptable.

#### W-2: Task 4 does not show a complete diff for orchestrator.py — large refactor underspecified

`02-09-PLAN.md:863-1018` is the orchestrator refactor (~150 lines of code change in the actual file). The plan provides:
- Phase 2 outline (good)
- Phase 7 outline (good)
- "unchanged" labels for phases 3-6

But it does not show: how `gamma_count_reported` flows from phase 2 to phase 6; what happens to `del raw_markets` lines (line 244 currently); what happens to the original phase 2's `del raw_events` line (228) — both `del` lines should be removed/relocated. The full `markets` list variable (currently line 237) should not exist after refactor. The plan does not explicitly say "delete line 237-263" type instructions.

**Fix:** Add an explicit before/after block for the orchestrator changes at the level of the existing source line numbers, or accept that the executor will re-derive — but warn that the current spec invites drift.

#### W-3: Memory regression test imports `tests.m1_perception.fixtures...` — but directory is `tests/m1-perception/` (hyphen, not underscore)

`02-09-PLAN.md:1158-1160`:
```python
from tests.m1_perception.fixtures.gamma_streaming_payload import (
    make_realistic_event, make_realistic_market,
)
```
The directory is `tests/m1-perception/` (hyphen). Python cannot import a module name containing `-`. The existing tests do not import via package syntax from `tests.m1-perception.*` — `grep -rn "from tests" tests/m1-perception/` would confirm this. The fixture file at `tests/m1-perception/fixtures/gamma_streaming_payload.py` must be imported via a sys.path manipulation, a relative import (if there's an `__init__.py`), or by placing the fixture under a non-hyphen package name.

**Fix:** Either use `pathlib.Path` + `importlib.util` to load, or rename to `tests/m1-perception/fixtures/` with `from tests.fixtures...` (probably needs `conftest.py` registration), or import via `sys.path.insert(0, ...)` then `from gamma_streaming_payload import ...`. The plan needs to specify which.

#### W-4: Makefile targets not added per CLAUDE.md "命令入口约定"

CLAUDE.md (line 191-196) requires: "任何 Phase 实现新功能 → 同步在 Makefile 加 target". Plan 02-09 introduces new operations:
- "20k-market memory regression test" — should have `make memory-budget-test` target
- "docker --memory=256m smoke" — should have `make docker-smoke-256mb` target (the existing `make docker-smoke` does NOT pin memory)

Task 6 step 1 uses an inline 4-line `docker run` command — exactly the kind of thing CLAUDE.md says to wrap in a Makefile. Task 5's slow test would benefit from `make memory-budget-test` so the user can re-run it without remembering the pytest path.

**Fix:** Task 5 acceptance criteria adds Makefile entry. Task 6 step 1 wraps the docker run in a target. Both updated in `files_modified` (Makefile is already listed there but no acceptance criterion mentions it).

#### W-5: Atomicity test for `write_snapshot_streaming` does not test post-INSERT pre-COMMIT crash

Task 2 test 3 (`02-09-PLAN.md:640`) tests mid-batch crash by generator raising at row 750. This proves rollback works during the executemany loop. But it does NOT test:
- Crash AFTER `con.execute("UPDATE snapshots SET market_count=?...")` but BEFORE `con.execute("COMMIT")` — would prove the snapshots row is also rolled back, not just the markets.
- Crash inside the issues `executemany` (after market_count update, before COMMIT) — same.

For per-snapshot atomicity to be proven, one of these post-markets-pre-commit crash tests is needed.

**Fix:** Add a fourth test variant: monkeypatch `con.execute` on the COMMIT call to raise; assert no snapshots row, no markets rows, exception propagates.

---

### NITS (cosmetic / nice-to-have)

#### N-1: Decision E claims pyarrow ParquetWriter has no context-manager support
`02-09-PLAN.md:185` says "No context-manager support guaranteed across versions". Actually pyarrow >= 0.15 has supported `with pq.ParquetWriter(...) as writer:` for ~5 years. The plan's try/finally pattern works fine but the disclaimer is outdated. Not load-bearing.

#### N-2: `test_streaming_memory_budget` adds `psutil` to dev deps
`02-09-PLAN.md:1280` — `psutil` is already a transitive dep of some testing libs but not pinned. The plan correctly adds it to `[project.optional-dependencies.dev]`. Acceptance criterion is clear. Just noting that `uv add --dev psutil` is the canonical command (per CLAUDE.md technical stack) and the plan does not say so.

#### N-3: Task 7 references `docs/learning/07-生产化部署.md`
`02-09-PLAN.md:1378` — assumes 07 exists. Currently `docs/learning/` may have 01-06 per CLAUDE.md "Phase 1（market snapshot）的 6 篇教学文档（01-06）已落库". If 07 doesn't exist yet, the executor should use 06 as the体例 reference instead. Minor — the read_first hint can defensively say "highest-numbered existing".

---

## Memory budget sanity check

Plan claim: `~105MB peak` (`~25MB above baseline Python runtime (~80MB)`).

Reality check using first-principles:
- Python 3.12 + import polyarb stack (loguru, httpx, pyarrow, pandas if transitive, sqlite3, supabase, boto3) is more like **100-130MB baseline RSS** on Linux. pyarrow alone is ~40MB module load. The "80MB baseline" in the plan is what `python -c "pass"` shows, NOT what `python -m polyarb.snapshot snapshot` starts at.
- Phase 02-04 commit `1a97200` brought peak from 480MB → ~160MB on synthetic profiling. Real-world deploy hit the 256MB cap. So even with field-stripping, real prod baseline + working set was approaching 256MB.
- After D-23 fix: removing the 20k full-list (~80-100MB at peak) brings real prod working set down by roughly that amount → expected real peak ~80-100MB transient over actual baseline → if baseline is 100-118MB, total peak is **180-218MB on a real fly machine**.

**218MB > 130MB budget.** The plan's regression test will likely fail on first run, OR pass in the test harness (which has a low pyarrow+httpx baseline) but the real fly deploy still ekes close to 256MB.

This is exactly why **B-1 calibration step matters** — without measuring baseline empirically, the executor cannot know if the design works.

**Math summary:**
```
Plan claim:              80 (baseline) + 25 (working) = 105 MB
Plausible reality:      110 (baseline) + 30 (working) = 140 MB  ← FAILS 130MB test
Real-fly-VM reality:    120 (baseline) + 40 (working) = 160 MB  ← FAILS 130MB test and 150MB cap
```

**Fits in 150MB usable?** Only if (a) real Python baseline on fly is ~95MB AND (b) working set stays under 35MB. Both are plausible but neither is verified by the plan. Task 5 must report `baseline_rss` in the commit body so the executor and future readers know what slack actually exists.

---

## Atomicity preservation check

| Property | Current code | Plan T2/T4 design | Preserved? |
|---|---|---|---|
| One BEGIN IMMEDIATE around all market inserts | Yes (`sqlite_store.py:184`) | Yes (T2 outline line 562-619) | ✓ |
| DELETE FROM markets inside same tx | Yes | Yes (line 565) | ✓ |
| `snapshots` row + `markets` rows + `validation_issues` rows all-or-nothing | Yes | Yes via single tx | ✓ |
| Parquet tmp + os.replace | Yes (`parquet_writer.py`) | Yes (T1 outline 436-466) | ✓ |
| Dedup via seen_ids set | Yes (`orchestrator.py:251`) | Yes (T4 outline line 876, 893-896) | ✓ |
| Layer 1 count using full Gamma count | `len(raw_markets)` vs `len(markets)` | Running counters `raw_market_count` vs `normalized_count` (T4 line 887, 898, 944-946) | ✓ |
| Layer 2/4 on persisted markets only | `target_markets` | `target_markets` (unchanged) | ✓ |

**Subtle issue (see W-5):** the atomicity TEST only proves mid-batch rollback, not post-markets-pre-commit rollback. The DESIGN is correct (single tx); the TEST is incomplete proof.

**Subtle issue (B-2 underlies this):** if target_markets in production is 5-10x the test fixture size due to threshold mismatch ($1k prod vs $10k test), the SQLite batched insert is still atomic, but the memory profile during phase 5 (CLOB attach + book payload references) is untested.

---

## Test methodology check

Per the task spec:

| Requirement | T5 design | Verdict |
|---|---|---|
| Realistic fixtures (real-length strings, 15 fields post-strip) | Yes — `make_realistic_market` uses 77-char token IDs, full question strings, 64-char hex condition IDs | ✓ Strong |
| Use `psutil.Process().memory_info().rss` not tracemalloc | Yes — `psutil.Process(os.getpid())` + sidecar thread polling 50ms | ✓ Strong |
| Assert peak RSS not allocated objects | Yes — `peak[0]` is poll-max RSS, asserted absolute | ✓ |
| Real `run_snapshot()` integration with mocked Gamma via respx | Yes — respx router for /markets + /events with offset paging | ✓ |
| Use production-equivalent threshold | **NO — uses $10k instead of $1k default** | ✗ (see B-2) |
| Baseline isolation | Partial — `baseline_rss` is captured but assertion is on absolute, not delta | Warning |

**Trap 1 from `02-04-SUMMARY.md`:** synthetic 20-field dicts profiled 170MB while real 50+ field dicts profiled 482MB. Plan T5 addresses this by including realistic fields. ✓

**Trap 2 (new):** mocking CLOB to return empty hides the phase 5 book-attach memory cost. In prod, target_markets dicts get books attached with `book.get("asks") / book.get("bids")` referenced. If books are non-trivial (real CLOB books have 50+ levels per side), this matters. T5 mocks CLOB to empty dicts (`_empty_books` returns `[]`). **This means T5 does not exercise the real memory cost of phase 5.**

**Suggested resolution to trap 2:** T5 mock should return ~realistic CLOB books (10-50 bid/ask levels per book) for target_markets-equivalent number of tokens, OR add a comment acknowledging that phase 5 memory cost is observed-but-not-bounded by this test, and Task 6 (live fly deploy) is the actual phase-5 gate.

**Test framework correctness:**
- The `@pytest.mark.asyncio` decorator is required. The plan uses it. ✓
- The respx mock pattern is correct.
- The `os.environ.setdefault` calls at module-import time are correct (they need to run before `from polyarb.config import Settings`).
- W-3 (import path with hyphen) is the only mechanical blocker.

---

## Recommendation

**Address 2 blockers before execute:**

1. **B-1:** Replace handwaved memory budget with calibrated baseline measurement at start of Task 5. State budget as delta-over-baseline. This is the single most important change — without it, the executor cannot interpret a failed regression test.

2. **B-2:** Either configure T5 to use the production liquidity threshold ($1k, not $10k), OR add an explicit second test at the production threshold, OR record an empirical observation of `target_markets` count from a current live `make snapshot-markets` run and adjust the test threshold to produce a comparable count. T6 step 1 (docker --memory=256m) must use the production threshold.

**Address 5 warnings as opportunistic fixes** while doing the blocker work — W-3 (hyphen import path) is mechanical and trivial; W-1 (GammaClient context manager) is a one-line clarification; W-4 (Makefile targets) is straightforward; W-5 (post-INSERT-pre-COMMIT atomicity test) is one extra test method; W-2 (orchestrator full diff) can be deferred to a TODO comment.

**Nits are optional.**

After revision, the plan is structurally sound: tasks are additive (T1+T2 land alongside legacy paths, T3 keeps backward-compat wrappers, T4 swaps in the new APIs, T5 enforces budget, T6 is the prod gate, T7 closes the trail with SUMMARY + teaching doc). The 7-task split is well-bounded for context budget. Dependency graph is correct (`depends_on: [02-04]`, wave 3.5).

The critical insight the plan is missing: **the 130MB target is not derivable from the budget table as written, and the test as written may pass under unrealistic conditions while real fly still OOMs.** The calibration fix in B-1 turns this from "hope the design works" into "measure whether the design works".


---

## Round 2 Verification — Post-Revision Check

**Re-checked:** 2026-05-15
**Plan length:** 1694 → 1934 lines (planner reports +18 edits)
**Verdict:** PASS-WITH-WARNINGS

### B-1 (memory budget) — fix verification

**Structural, not cosmetic.** The revised `<design_decisions>` block at `02-09-PLAN.md:263-292` replaces the old static budget table with three concrete mechanisms:

1. **T5.0 calibration test** (`02-09-PLAN.md:1119-1170`) — separate test file `test_streaming_memory_calibration.py`, imports the full stack (`polyarb.snapshot.orchestrator`, `polyarb.clients.gamma_client`, `polyarb.storage.parquet_writer`, `polyarb.storage.sqlite_store`) + instantiates `Settings(...)`, then captures `psutil.Process(os.getpid()).memory_info().rss`. **Does NOT call `run_snapshot()`** — confirmed by reading line 1158 (the rss reading happens immediately after touching settings; no orchestrator entry point invoked). ✓ Baseline is not polluted.

2. **Sanity floor** (`02-09-PLAN.md:1166-1169`) — calibration asserts `baseline > 50MB` to catch the failure mode where imports didn't actually load (e.g. test discovery without full stack). This is the right kind of guard.

3. **Dual assertion in T5.1** (`02-09-PLAN.md:1397-1426`):
   - `peak_delta < 30MB` — hard assertion, the architectural claim
   - `peak_abs >= 130MB` → WARNING print (not fail)
   - `peak_abs < 140MB` — hard assertion, the OOM-relevance check with 10MB jitter band

   **Logic is correct:** 130MB is not "soft pass"; it's "warn for diagnostic, fail at 140MB". So a 131MB peak prints a warning AND passes (because delta is the dominant claim). A 141MB peak fails. This matches the user's intended semantics ("130MB hard cap + 10MB jitter band").

**Grounding of the 30MB number** (`02-09-PLAN.md:273-286`): the budget table explicitly enumerates 8 components summing to ~26-30MB. Honest about the tightness ("leaves ~0-4MB slack to the 30MB delta target"). target_markets line correctly cross-references B-2's threshold change (6-8MB at $1k threshold, vs <1MB at the old $10k threshold). The math is plausible — see sanity check below.

**Fly VM worst-case scenario (user's question):**
The plan's risk note at line 290 says "if baseline_rss is unexpectedly high on the Fly VM (>110MB), T6 step 4 will discover this — at which point the absolute-ceiling 130MB may need revision upward (e.g. to 140MB)".

This is **partially correct but misframed.** If real Fly baseline is 130MB and peak_delta is 30MB, peak_abs = 160MB > 150MB usable → OOM. The plan's response "raise the ceiling to 140MB" does not solve the OOM; it just makes the test pass while the deploy still fails. The actual fix in that scenario would be to **shrink the working set further** (reduce batch_size, defer events streaming, etc.) — but the plan does not articulate that. **This is a Warning, not a Blocker** — the regression test still catches the architectural claim, and T6 (real fly deploy) is the actual safety net (machine `stopped` = visible OOM). The plan does correctly say "Document the live Fly baseline in T7 SUMMARY so the next memory plan starts from real data" — so escalation path exists.

**B-1 fix verdict: ACCEPTED.** Calibration is real, dual-assert is correctly framed, budget table is grounded. Residual concern (Fly baseline > 110MB scenario) is acknowledged in risks but underdeveloped — flagged below as W-9.

### B-2 (threshold mismatch) — fix verification

**Structural, not cosmetic.** Five concrete changes:

1. `02-09-PLAN.md:1332` — `liquidity_threshold_usd=1000.0` (production default per `config.py:44`). ✓
2. `02-09-PLAN.md:1198-1202` — log-normal liquidity distribution with `μ=log(500), σ=2`. Sanity-checked: P(X > 1000) ≈ 36.4%, P(X > 10000) ≈ 6.7%, P(X > 50000) ≈ 1.07%. Plan claims "~35% > $1k, ~12% > $10k, ~3% > $50k". The $1k figure is dead-on; $10k and $50k claims are slight over-estimates but right order of magnitude. Sufficient. ✓
3. `02-09-PLAN.md:1333-1337` — explicit comment: "target_markets ≈ 6000-8000" at $1k threshold. **This is 12-16x the prior 'few hundred' Decision G claim.** The plan acknowledges this and rolls the cost (~6-8MB) into the budget table at line 280. ✓ Internally consistent.
4. `02-09-PLAN.md:1515` — T6 step 1 docker run includes `-e POLYARB_LIQUIDITY_THRESHOLD_USD=1000.0`. ✓ Matches Makefile target at `02-09-PLAN.md:1791`. The env var is plausible (config.py reads from `POLYARB_*` env vars via pydantic_settings) but **note: this assumes `liquidity_threshold_usd` field has env-var binding configured.** Worth a quick code check at execute time (W-10 below).
5. `02-09-PLAN.md:1522-1526` — T6 extracts `target_markets` count via `grep -E "(target after mode-filter|target_markets)" /tmp/02-09-step1-snapshot.log`. This works because the new orchestrator logs `"target after mode-filter (mode={mode})"` at line 960 (matches the grep regex). ✓ Empirical anchor will land in SUMMARY.

**B-2 fix verdict: ACCEPTED.** Threshold matches production. Distribution is real. Empirical observation will be captured. Decision G's "few hundred" claim is silently superseded by the 6000-8000 reality — plan acknowledges this in the budget table but does not update Decision G text (cosmetic; flagged below).

### W-1..W-5 — fix verification

| Warning | Fix location | Status |
|---|---|---|
| W-1: Single GammaClient session | `02-09-PLAN.md:883-953` — explicit single `async with GammaClient(settings) as gamma:` enclosing phase 1a (events) AND phase 2 (markets stream) | ✓ Fixed correctly |
| W-2: Orchestrator full diff | `02-09-PLAN.md:1037-1062` — explicit delete-line table with current line numbers (170, 189-204, 207-222, 225-233, 236-244, 251-263, 266-285, 432-445, 456-466) AND add-line section AND survives-across-phases section | ✓ Fixed thoroughly — possibly the best edit in this revision |
| W-3: Hyphen-dir import | `02-09-PLAN.md:1293-1302` — switch to pytest fixture `gamma_payload_factory` defined in `conftest.py`; test receives via injection (`02-09-PLAN.md:1314, 1317`). `tests/m1-perception/conftest.py` already exists with the right patterns; `tests/m1-perception/fixtures/` already has `__init__.py`. The conftest can `from .fixtures.gamma_streaming_payload import make_realistic_market, make_realistic_event` then expose them as a fixture. | ✓ Mechanism is correct — but plan does not actually write the conftest snippet, just describes it. Minor under-specification (flagged as N-4). |
| W-4: Makefile targets | `02-09-PLAN.md:1774-1816` — `memory-budget-test` + `docker-smoke-256mb` targets added; `.PHONY` declared; both targets shell out the documented invocations; `test_makefile_contract.py` extended with dry-run tests asserting the env var + `--memory=256m` flag appear in the make recipe | ✓ Fixed — and the Makefile-contract test is a nice belt-and-suspenders touch |
| W-5: Post-commit atomicity | `02-09-PLAN.md:657-658` — `test_write_snapshot_streaming_atomicity_on_commit_failure` added; monkeypatch `sqlite3.Connection.execute` to raise on literal `"COMMIT"` argument. **Pattern verified against existing code**: `sqlite_store.py:184, 245, 521, 544` all use `con.execute("BEGIN IMMEDIATE")` / `con.execute("COMMIT")` (string-based, NOT `con.commit()`). So the monkeypatch on `execute` matching the `"COMMIT"` string argument WILL fire correctly. | ✓ Fixed — and the user's concern (con.commit vs execute("COMMIT")) is correctly resolved in the direction the plan chose. |

### New issues introduced (if any)

**W-6 (warning):** Decision G text at `02-09-PLAN.md:218-261` still says "target_markets ~few hundred" / "~500 markets in subset mode". B-2 fix establishes that at the $1k production threshold, target_markets is actually 6000-8000. The budget table at line 280 acknowledges this but Decision G prose was not updated. Cosmetic inconsistency — executor reading Decision G in isolation gets a wrong number. **Fix:** add one-line note in Decision G referencing the budget-table empirical correction.

**W-7 (warning):** `02-09-PLAN.md:1108` and frontmatter line 19 list `tests/m1-perception/test_parquet_sqlite_consistency.py` as a created file, but `tests/m1-perception/test_parquet_sqlite_consistency.py` **already exists** (139 lines from Phase 02-01, Wave 0 RED test). The plan likely means "extend" not "create". **Fix:** change `files_modified` frontmatter wording or have T5 explicitly say "append new test functions to existing file".

**W-8 (warning):** The plan's risk R5 at `02-09-PLAN.md:1847` still says "subset threshold $1k filters to <1k markets today" — contradicts the log-normal distribution claim (6000-8000) and the actual prod count that T6 will record. Same root cause as W-6 (stale "few hundred" estimate). **Fix:** update R5 text to match the new empirical anchor.

**W-9 (warning):** `02-09-PLAN.md:290` says "if baseline_rss is unexpectedly high on the Fly VM (>110MB), ... the absolute-ceiling 130MB may need revision upward (e.g. to 140MB)". This **misframes** the OOM-relevance check: if Fly baseline IS 130MB, raising the ceiling to 140MB makes the test pass but the deploy still OOMs (160MB peak > 150MB usable). The actual fix in that scenario is to shrink the working set, not relax the test. **Fix:** rewrite line 290 to say "if Fly baseline > 110MB, the working-set budget itself must shrink (smaller batch_size, defer events streaming earlier, etc.); raising the test ceiling without shrinking working set just hides the OOM." Not a blocker because T6 (real fly deploy) catches it regardless — the test ceiling adjustment doesn't break the safety net.

**W-10 (warning, executor-time check):** `02-09-PLAN.md:1515, 1791` use `POLYARB_LIQUIDITY_THRESHOLD_USD=1000.0` env var. This assumes `Settings.liquidity_threshold_usd` reads from `POLYARB_LIQUIDITY_THRESHOLD_USD`. The pydantic_settings convention is `env_prefix="POLYARB_"` + field name uppercase — so this should work, but the plan does not verify it. **Fix:** executor must verify at T6 step 1 that the env var is actually picked up (e.g. log the threshold value or read `Settings()` in a tiny pre-script). If `Settings` requires the var to be set differently, the plan's T6 test instrument fails silently and uses default ($1k, which happens to be the same number — so this might pass even if env-var binding is broken).

**N-4 (nit):** W-3 fix describes the conftest fixture mechanism in prose but doesn't write the literal `@pytest.fixture` snippet. Concrete diff for `tests/m1-perception/conftest.py` would prevent ambiguity. Trivial to fix; trivial to live with.

**N-5 (nit):** `02-09-PLAN.md:1334-1335` comment says "~35% of 20k markets exceed $1k → target_markets ≈ 6000-8000". Strict arithmetic: 35% × 20000 = 7000, so range "6000-8000" gives ±15% slack which is reasonable (variance + dedup ~4%). Numbers are coherent. Not an issue, just confirming.

### Round 2 verdict reasoning

The two blockers are genuinely fixed, not cosmetic-patched. B-1's calibration test is real (separate file, runs before T5.1, writes to a gitignored fixture, asserts a sanity floor). B-1's dual-assert is correctly framed (delta hard-fail, absolute soft-warning + jitter-band hard-fail). B-2 swaps in the production threshold ($1k) end-to-end (test + docker smoke + Makefile target) and acknowledges the threshold-driven explosion of target_markets in the budget table.

The five warnings (W-1..W-5) from Round 1 are all genuinely addressed — W-2 (orchestrator full diff) is especially thorough.

The five new issues (W-6..W-10) are all warnings, none are blockers. Most are stale-prose inconsistencies (Decision G, risk R5, mitigation framing) where the math/code is right but the surrounding text wasn't updated to match. W-10 is a sanity check the executor must do at T6 time but is not architecturally broken. W-9 is a misframing of a worst-case scenario; the actual safety net (T6 fly deploy machine-state observation) catches the failure mode regardless.

The plan is now executable. The executor should be aware of W-9 (Fly worst-case interpretation) and W-10 (env var binding sanity check) but does not need plan revision to handle them — both surface during T6 and have clear escalation paths.

**Recommendation:** approve for execute. Optional polish on W-6/W-7/W-8 (stale prose) and W-9 (worst-case framing) is fine to defer to a quick edit before commit; W-10 should be added as a manual gate in T6's checklist if it's not already implicit. Do not invoke Round 3 — the remaining issues do not block the phase goal.

