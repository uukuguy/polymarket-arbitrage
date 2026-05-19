---
phase: "02"
plan: "07"
subsystem: m1-perception
status: "TASKS 1-3 COMPLETE — TASK 4 (SOAK) PENDING USER START"
tags: [chaos-engineering, soak-monitor, testing, teaching-doc, production]
dependency_graph:
  requires: [02-04, 02-05, 02-06]
  provides: [chaos-test-suite, soak-monitor, production-teaching-doc]
  affects: [tests/m1-perception/, scripts/, docs/learning/]
tech_stack:
  added: [respx>=0.21]
  patterns: [chaos-engineering, fail-soft, state-machine-testing, concurrent-sqlite]
key_files:
  created:
    - tests/m1-perception/test_chaos_gamma_5xx.py
    - tests/m1-perception/test_chaos_clob.py
    - tests/m1-perception/test_chaos_supabase.py
    - tests/m1-perception/test_chaos_r2.py
    - tests/m1-perception/test_chaos_3failures_pause.py
    - tests/m1-perception/test_chaos_scan_flood.py
    - tests/m1-perception/test_sqlite_concurrency.py
    - scripts/soak_monitor.py
    - .planning/workstreams/m1-perception/phases/02-l1-production-grade/02-SOAK-LOG.md
    - docs/learning/08-生産化部署.md
  modified:
    - src/polyarb/validator/layers.py
    - src/polyarb/snapshot/orchestrator.py
    - Makefile
    - tests/m1-perception/test_makefile_contract.py
    - docs/learning/00-INDEX.md
    - pyproject.toml
decisions:
  - "D-12 confirmed: DEGRADED does NOT increment failure_counter — mirror/R2 failure ≠ data loss"
  - "Chaos test patch targets: module-level imports vs function-scope imports require different patch paths"
  - "respx>=0.21,<0.22 pinned for httpx mock compatibility"
metrics:
  duration: "~3 hours (Tasks 1-3)"
  completed_date: "2026-05-19"
  tasks_completed: 3
  tasks_pending: 2
  files_created: 10
  files_modified: 6
---

# Phase 02 Plan 07: Wave 5 Chaos + Soak Summary

**Status: TASKS 1-3 COMPLETE — TASK 4 (SOAK) PENDING USER START**

> Task 4 (7-day Better Stack soak) cannot be executed by this agent — it requires
> 7 calendar days of real cloud monitoring. User must start the soak by running
> `make soak-status` after Day 7. Task 5 (final SUMMARY update) will be written
> at that time.

One-liner: Chaos test suite (20 passing tests, 8 fault scenarios) + Better Stack soak monitor CLI + Phase 02 production teaching doc.

## Tasks Completed

| Task | Description | Commit | Status |
|------|-------------|--------|--------|
| 1 | Chaos engineering test suite (8 scenarios, 7 files) | `8ccd604` | DONE |
| 2 | `scripts/soak_monitor.py` + `make soak-*` targets + `02-SOAK-LOG.md` | `2fbfd32` | DONE |
| 3 | `docs/learning/08-生産化部署.md` + 00-INDEX update | `522ea56` | DONE |
| 4 | 7-day Better Stack soak (cloud, user checkpoint) | — | PENDING |
| 5 | Final SUMMARY update (after soak passes) | — | PENDING |

## Task 1: Chaos Engineering Test Suite

### Files Created

| File | Scenarios Covered |
|------|-------------------|
| `test_chaos_gamma_5xx.py` | Gamma 503 exhaustion → FAILED; mid-pagination timeout → DEGRADED or FAILED |
| `test_chaos_clob.py` | Malformed CLOB book (dict instead of list) → F-1 capture, no crash |
| `test_chaos_supabase.py` | Supabase `push_snapshot` raises 500 → DEGRADED, not FAILED |
| `test_chaos_r2.py` | R2UploadError → DEGRADED + Issue; R2 retry config 3 attempts |
| `test_chaos_3failures_pause.py` | 3 FAILED → PAUSED; alert called once; PAUSED skips tick; unpause resumes; counter persists restart |
| `test_chaos_scan_flood.py` | 100 concurrent /scan requests → no 500; HMAC validation holds |
| `test_sqlite_concurrency.py` | WAL mode concurrent reader + writer → no crash, monotonic counts; `SQLiteStore.init_schema()` enables WAL |

**Result: 20 tests passing, all GREEN.** (1 slow test marked `@pytest.mark.slow`; 7 standard tests run in ~1.70s)

### Auto-Fixed Bugs (Rule 1)

**[Rule 1 - Bug] F-1 defense incomplete in `layers.py` and `orchestrator.py`**

The chaos test for malformed CLOB books exposed a real production bug:

- **Found during**: Task 1 (test_chaos_clob.py development)
- **Issue**: `book.get("asks")` returning a `dict` (not a list) was truthy, so `asks = book.get("asks") or []` kept the dict. Then `asks[0]` raised `KeyError`, which was NOT in the except clause `(AttributeError, IndexError, TypeError)`. The F-1 invariant claimed to protect against malformed book data, but `KeyError` escaped.
- **Fix**: Normalize to list type before indexing:
  ```python
  _raw_asks = book.get("asks")
  asks = _raw_asks if isinstance(_raw_asks, (list, tuple)) else []
  ```
  Also added `KeyError` to the except clause. Applied to both `layers.py:208` and `orchestrator.py` Phase 5 stamp section.
- **Files modified**: `src/polyarb/validator/layers.py`, `src/polyarb/snapshot/orchestrator.py`
- **Commits**: part of `8ccd604`

### Key Implementation Decisions

**Mock patch paths**: Supabase and R2 are imported locally inside `run_snapshot()`, not at module level. Must patch at source module:
- `polyarb.storage.supabase_mirror.SupabaseMirror` (not `polyarb.snapshot.orchestrator.SupabaseMirror`)
- `polyarb.storage.r2_sync.upload_parquet_to_r2` (not `polyarb.snapshot.orchestrator.upload_parquet_to_r2`)

**Post-SQLite issues**: Mirror/R2 issues (steps 7.5/7.6) run AFTER `store.write_snapshot_streaming()`. They only flow into `SnapshotResult.issue_categories`, not into SQLite `validation_issues`. Tests must assert on `result.issue_categories`, not DB rows.

**D-12 confirmation**: Tests verify DEGRADED does NOT increment `_failure_counter`. Supabase or R2 failure → DEGRADED → counter stays 0 → never reaches PAUSED threshold.

## Task 2: Soak Monitor

### Files Created

**`scripts/soak_monitor.py`**: typer CLI with two commands:
- `status`: fetches Better Stack 7-day SLA, exits 0 if uptime ≥ 99%, else 1
- `export`: appends timestamped audit section to `02-SOAK-LOG.md`

Both require `BETTERSTACK_API_TOKEN` + `BETTERSTACK_MONITOR_ID` env vars.

**`.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-SOAK-LOG.md`**: Initial scaffold with:
- Pass criteria checklist (8 items)
- Fault injection plan (3 scenarios: scale-to-0, break R2 creds, HMAC flood)
- `<!-- BEGIN SOAK EVENTS -->` marker for `soak_monitor.py export` append target

**Makefile targets added**:
```makefile
make soak-status       # check current uptime % (exit 0 = gate PASS)
make soak-export       # dump 7-day history to 02-SOAK-LOG.md
make soak-fault-inject # print fault injection instructions
```

## Task 3: Teaching Doc

**`docs/learning/08-生産化部署.md`**: Phase 02 production deployment teaching doc (Chinese, 251 lines).

Sections:
1. **30 秒心智模型**: 3-layer diagram (Fly daemon, state machine, alert paths)
2. **用户工作流**: 7-step morning check + emergency commands
3. **关键代码片段**: 7 snippets with exact file:line references
4. **设计取舍**: 5 documented trade-offs
5. **自检题**: 5 questions with detailed answers
6. **FAQ 增量区**: empty, ready for user Q&A

`docs/learning/00-INDEX.md` updated with doc 08 entry.

## Task 4: 7-Day Soak (PENDING)

**How to start**: The soak is already running — Better Stack monitor was configured in Plan 05 and pings `/health` every 30s from the cloud. No action needed to "start" it.

**Day 7 completion steps**:
```bash
# Check 7-day uptime (exit 0 = PASS)
make soak-status

# Export audit trail to 02-SOAK-LOG.md
make soak-export

# Verify fault injection (optional, Day 3-4)
make soak-fault-inject
```

**Pass criteria** (from `02-SOAK-LOG.md`):
- Uptime ≥ 99% (Better Stack 7-day SLA)
- Cron 14/14 subset fires (7 days × 2/day)
- Cron 1/1 full fires (Sunday 04:00 UTC)
- OK + DEGRADED ≥ 95% of snapshot attempts
- ≥ 1 natural failure → Telegram alert received
- Self-healing after failure OR correct PAUSED (3x consecutive)
- SQLite volume ≤ 4GB at day 7
- Sentry errors < 5/day

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] F-1 defense incomplete — KeyError not caught in layers.py and orchestrator.py**
- **Found during**: Task 1 (writing test_chaos_clob.py)
- **Issue**: Non-list book data (e.g., a dict for `asks`) bypassed the `or []` guard and reached `asks[0]`, raising `KeyError` outside the except clause
- **Fix**: `isinstance` check before indexing; added `KeyError` to except tuple
- **Files modified**: `src/polyarb/validator/layers.py`, `src/polyarb/snapshot/orchestrator.py`
- **Commit**: `8ccd604`

No other deviations — plan executed as written for Tasks 1-3.

## Self-Check: PASSED

Files verified:
- `tests/m1-perception/test_chaos_gamma_5xx.py` — FOUND
- `tests/m1-perception/test_chaos_clob.py` — FOUND
- `tests/m1-perception/test_chaos_supabase.py` — FOUND
- `tests/m1-perception/test_chaos_r2.py` — FOUND
- `tests/m1-perception/test_chaos_3failures_pause.py` — FOUND
- `tests/m1-perception/test_chaos_scan_flood.py` — FOUND
- `tests/m1-perception/test_sqlite_concurrency.py` — FOUND
- `scripts/soak_monitor.py` — FOUND
- `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-SOAK-LOG.md` — FOUND
- `docs/learning/08-生産化部署.md` — FOUND

Commits verified:
- `8ccd604` — chaos test suite (Task 1)
- `2fbfd32` — soak monitor (Task 2)
- `522ea56` — teaching doc (Task 3)
