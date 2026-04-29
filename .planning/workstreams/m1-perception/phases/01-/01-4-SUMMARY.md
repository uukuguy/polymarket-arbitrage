---
phase: 01
plan: 4
wave: 3
type: execute
status: complete
subsystem: snapshot-orchestrator
tags: [m1-perception, orchestrator, cli, normalizer, makefile]
started_at: 2026-04-29T16:25:00Z
completed_at: 2026-04-29T16:36:00Z
duration_min: 11
tasks_completed: 6
tests_added: 6
tests_passing_in_plan: 6
tests_passing_full_m1: 57
dependency_graph:
  requires:
    - "01-1: polyarb skeleton (config.py, pyproject.toml, src/polyarb tree)"
    - "01-2: GammaClient + ClobReaderClient async clients"
    - "01-3: SQLiteStore + parquet_writer + Category/Issue + layer1/2/4 validators"
  provides:
    - "polyarb.snapshot.normalizer.normalize_market — Gamma raw dict → MARKETS_COLUMN_ORDER row"
    - "polyarb.snapshot.orchestrator.run_snapshot — async 7-step pipeline coordinator"
    - "polyarb.snapshot.orchestrator.SnapshotResult — return dataclass for CLI/tests"
    - "polyarb.snapshot.cli.app — typer app with `snapshot` subcommand"
    - "polyarb.cli.app — top-level shim for `[project.scripts] polyarb = polyarb.cli:app`"
    - "Makefile targets: snapshot-markets, snapshot-markets-full"
  affects:
    - "Plan 5 (integration): exercises this pipeline end-to-end against fixtures"
    - "Future m1-perception phases (Phase 2 WS): will register additional commands on the same Typer app"
tech_stack:
  added: []
  pinned:
    - "click>=8.1,<8.2 (Rule 3 retro-fix; click 8.2 changed Parameter.make_metavar() signature, breaking typer 0.12.x --help rendering)"
  patterns:
    - "Async context manager wrapping for both clients (`async with GammaClient(s) as g`)"
    - "All API failures categorized as Issues, never propagated — D-D3 (snapshot persists with is_valid flag)"
    - "Parquet write FIRST then SQLite (Plan 3 contract — parquet_path known when snapshot row inserted)"
    - "F-1: every float() coercion of CLOB book fields wrapped in try/except (KeyError, TypeError, ValueError, IndexError)"
    - "F-5: exception details capped at 200 chars, raw_payload at 500 bytes"
    - "F-8: naive datetimes from endDate parsing treated as UTC, not local time"
key_files:
  created:
    - src/polyarb/snapshot/normalizer.py
    - src/polyarb/snapshot/orchestrator.py
    - src/polyarb/snapshot/cli.py
    - src/polyarb/snapshot/__main__.py
    - src/polyarb/cli.py
    - tests/m1-perception/test_orchestrator.py
  modified:
    - Makefile
    - pyproject.toml (Rule 3 click<8.2 pin; flagged for retro-add to 01-1 scope tracking)
decisions:
  - "Top-of-book attached only for yes_token_id (Phase-1 simplification — both sides symmetric on Polymarket; Layer 4 ghost-book validator still checks both yes+no tokens)"
  - "fetched_at_ms is stamped on ALL normalized markets including those filtered out of the subset mode — semantic gap from F-1 review noted as Phase 2 cleanup"
  - "Orchestrator NEVER raises for transport failures (Gamma/CLOB unreachable → Layer 1/4 API_UNREACHABLE Issue + persist snapshot with is_valid=false). The CLI sets the process exit code based on result.is_valid"
  - "Single-command typer app: registered_commands has name=None and uses callback.__name__ ('snapshot') as the implicit name. Smoke check uses callback.__name__ fallback"
  - "click pinned <8.2 because typer 0.12.x calls Parameter.make_metavar() without ctx; click 8.2 made ctx required. Will revisit when typer 0.13 lands (currently constraint-pinned <0.13)"
  - "Layer 4 prices_combined was rebuilt to UNWRAP the SDK-side dict ({tid: {'BUY': '0.46'}} → tid: '0.46') before passing to layer4_cross_source. The validator's tests confirm the flat str-or-num shape; the orchestrator was originally passing the nested form which silently disabled ghost-book detection"
---

# Phase 01 Plan 4: Orchestrator + CLI — Summary

Wired all subsystems together into a single executable pipeline. `make
snapshot-markets` is now a real command. End-to-end:
**Gamma fetch → normalize → mode filter → CLOB fetch → stamp+attach → validate
→ Parquet (atomic) → SQLite (single-tx)**. The orchestrator never raises on
transport failure — every failure mode (Gamma down, CLOB down, malformed book
prices) is recorded as a categorized `Issue` and the snapshot is persisted with
`is_valid` set accordingly.

## Per-task table

| # | Task | Commit | Files | Status |
|---|------|--------|-------|--------|
| T1 | Gamma raw → storage row normalizer | `8672a41` | `src/polyarb/snapshot/normalizer.py` | done |
| T2 | 7-step async orchestrator + SnapshotResult | `0edfeaa` | `src/polyarb/snapshot/orchestrator.py` | done |
| T3 | typer CLI (--full / --verbose / --config) | `0eca344` | `src/polyarb/snapshot/cli.py` | done |
| T4 | `__main__.py` + top-level cli shim + click<8.2 pin | `864bd63` | `src/polyarb/snapshot/__main__.py`, `src/polyarb/cli.py`, `pyproject.toml` | done |
| T5 | Makefile snapshot-markets + snapshot-markets-full | `e4c7b54` | `Makefile` | done |
| T6a | [Rule 1] Layer 4 prices shape unwrap fix | `c76bb9f` | `src/polyarb/snapshot/orchestrator.py` | done |
| T6b | Orchestrator end-to-end tests (6 tests) | `900ca32` | `tests/m1-perception/test_orchestrator.py` | done |

## Pipeline trace — what `make snapshot-markets` actually does

End-to-end audit trail for a future operator who wants to reason about a
specific run:

1. `make snapshot-markets` invokes `python -m polyarb.snapshot` (Makefile:
   tab-indented recipe; targets registered in `.PHONY`).
2. `polyarb/snapshot/__main__.py` imports the typer `app` from
   `polyarb.snapshot.cli` and calls it. Same `app` is also reachable via the
   `polyarb` console script through `src/polyarb/cli.py` re-export
   (pyproject.toml `[project.scripts]`).
3. The `snapshot` typer command (default mode = subset) parses
   `--full / --verbose / --config`, configures loguru level, calls
   `load_settings()` (which reads YAML at `config/snapshot.yaml` if present),
   and runs `asyncio.run(run_snapshot(settings, mode=mode))`.
4. `run_snapshot` (orchestrator):
   - **Step 1** (`async with GammaClient`): paginate `/markets?active=true...`
     up to `MAX_PAGES=1000`. On any exception → `Issue(layer=1, category=API_UNREACHABLE,
     detail="Gamma unreachable: …"[:200])` and proceed with `[]`.
   - **Step 2**: `[normalize_market(r) for r in raw]` — drops rows missing `id`
     (returns `None`), JSON-decodes `clobTokenIds` and `outcomePrices`, parses
     `endDate` (F-8 UTC), keeps token IDs as `str` (Pitfall 3).
   - **Step 3**: subset mode keeps `liquidity_usd > liquidity_threshold_usd`
     (default $1000); full mode keeps all. Concatenates `yes_token_id` +
     `no_token_id` → token list for CLOB.
   - **Step 4** (`ClobReaderClient`): `get_books(token_ids)` (chunked at 500)
     + `get_prices_buy_sell(token_ids)` (BUY + SELL, separate calls). On
     exception → `Issue(layer=4, category=API_UNREACHABLE)`. Books indexed by
     `asset_id` (with fallback to `market` / `token_id`).
   - **Step 5**: stamp every market with `fetched_at_ms = int(time.time() * 1000)`
     (Pitfall 6 best-effort consistency). Attach `best_ask_*` / `best_bid_*`
     for `yes_token_id`'s book — F-1 try/except on each `float()` call →
     unparseable books recorded as `Issue(layer=4, category=UNKNOWN)` with
     `raw_payload` truncated to 500 bytes.
   - **Step 6**: run validators. Layer 1 (`reported_total != fetched`),
     Layer 2 (mark missing-field rows as `incomplete=True`, in-place mutation),
     Layer 4 (per-token ghost-book check via `prices_combined` mapping —
     orchestrator unwraps the SDK side-keyed dict before passing).
     `is_valid_overall(issues)` → True iff no Layer 1 issue.
   - **Step 7**: compute Parquet path
     `{parquet_root}/YYYY/MM/DD/HH-MM-SS.parquet`; build rows with the two
     parquet-only fields (`snapshot_taken_at_ms`, `snapshot_id=0` placeholder);
     `write_parquet_atomic` (tmp + os.replace). Then `SQLiteStore.write_snapshot`
     (BEGIN IMMEDIATE → DELETE FROM markets → INSERT snapshot meta →
     executemany markets → executemany issues → COMMIT) returns `snapshot_id`.
5. CLI prints the D-F1 single-line summary to stdout. If `is_valid=False`,
   stderr prints the failure breakdown and the process exits 1; otherwise 0.

## Documented limitations (carry forward to Phase 2)

1. **`fetched_at_ms` stamped on ALL normalized markets including filtered-out
   ones.** Markets that weren't in the subset (their tokens never went to the
   CLOB) still receive `fetched_at_ms = clob_done_ms`, which technically lies
   about when their CLOB data was retrieved (since it wasn't). This was raised
   in the F-1 review as a semantic gap. Phase 2 cleanup: split into
   `metadata_fetched_at_ms` (from Gamma completion) vs `book_fetched_at_ms`
   (from CLOB completion, only set for the subset).
2. **Top-of-book attached only for `yes_token_id`.** Polymarket's NO side
   prices are arithmetically `1 - YES`, so storing both columns is redundant
   for the binary case. The Layer 4 ghost-book validator still checks both
   tokens independently (the `book_by_token` index is full). Phase 3 may add
   `best_no_*` columns when proper neg-risk multi-leg pricing arrives.
3. **No retry on CLOB.** Inherited from Plan 2 (no async retry hook on the
   sync py-clob-client SDK). Plan 5 should benchmark to see whether transient
   CLOB failures are common enough to warrant a manual retry loop.

## Empirical confirmations (relative to plan assumptions)

- **CLOB book token-id key** (Plan 2 SUMMARY claim): confirmed —
  `OrderBookSummary` exposes the token id as `asset_id` (not `market`, which
  is the conditionId). Orchestrator uses `b.get("asset_id") or b.get("market")
  or b.get("token_id")` defensive lookup; T6.1 verifies this against the
  recorded fixture.
- **`get_prices` return shape**: confirmed nested
  `{tid: {"BUY"|"SELL": "<price-str>"}}`. The orchestrator's
  `prices_combined` UNWRAPS the side-keyed inner dict before passing to
  Layer 4 — this was a Rule 1 bug fix during T6 (see "Deviations" below).
- **typer single-command `app.registered_commands`**: confirmed that a single
  `@app.command()` produces an entry with `name=None` and uses
  `callback.__name__ == "snapshot"` as the implicit command name. The smoke
  check uses a `name or callback.__name__` fallback.
- **`polyarb` console script picks up edits without re-running `pip install -e .`**:
  partially. Source-only edits to `src/polyarb/snapshot/cli.py` are visible
  immediately (entry point shim re-imports each invocation). But the click
  pin in pyproject.toml DID require a fresh `pip install -e .` to resolve the
  new constraint. Future plans that touch pyproject.toml must remember this.

## Deviations from plan

### [Rule 3 — Blocking dependency issue] click<8.2 pin

- **Found during:** T4 entry-point verification (`python -m polyarb.snapshot --help`)
- **Issue:** `TypeError: Parameter.make_metavar() missing 1 required positional argument: 'ctx'`. click 8.2.0 changed the signature of `Parameter.make_metavar()` to require a `ctx` argument; typer 0.12.5 still calls it without ctx, so all `--help` rendering crashes.
- **Fix:** Added `"click>=8.1,<8.2"` to `[project] dependencies` in `pyproject.toml`. typer 0.13+ would have the fix but our constraint pins typer `<0.13`.
- **Files modified:** `pyproject.toml`
- **Commit:** `864bd63` (bundled with T4)
- **Retro-add note:** This was a transitive-dep issue not anticipated in Plan 01-1's pyproject.toml. Plan 01-1's scope-tracking should record this as an addition.

### [Rule 1 — Bug] Layer 4 prices shape mismatch (T6 discovery)

- **Found during:** T6.3 ghost-book test (`test_ghost_book_detected_in_validation_issues`)
- **Issue:** Orchestrator was passing `prices_combined = {tid: {"buy": {"BUY": "0.46"}, "sell": {"SELL": "0.47"}}}` to `layer4_cross_source`, but the validator (per its own tests at `test_validator.py::test_layer4_ghost_book_detected`) expects `{tid: {"buy": "0.46", "sell": "0.47"}}`. `_safe_float({"BUY": "0.46"})` returns `None`, silently disabling ALL ghost-book detection in production runs.
- **Fix:** Added `_unwrap_side` helper in the orchestrator's prices_combined construction to extract the inner side-key (BUY/SELL) value.
- **Files modified:** `src/polyarb/snapshot/orchestrator.py`
- **Commit:** `c76bb9f`
- **Severity:** This is the kind of bug that would have shipped silently — pre-T6 the orchestrator imported and "worked" but never produced a single ghost_book Issue from real data. Test T6.3 catches it.

### Bonus test (not a deviation)

T6 added a 6th orchestrator test (`test_clob_unreachable_records_issue_but_persists_snapshot`) covering the D-E2/D-D3 invariant that CLOB failures don't abort the snapshot. The plan listed 5 minimum; we have 6.

## Authentication gates

None — pure-Python implementation with mocked clients in tests. Live snapshots
hit Polymarket's public Gamma + CLOB endpoints which require no auth at the
read-only L0 surface.

## Known stubs

None. Every output is fully wired:
- normalizer's CLOB-derived placeholders (`best_*`, `fetched_at_ms`) are
  systematically overwritten by the orchestrator before persistence.
- The CLI summary line is real and tested.
- Both Makefile targets are real shell commands, not placeholders.

## Threat flags

None. This plan adds:
- One new outbound HTTP path (Gamma `/markets` paginate) — already in the
  Plan 02 threat surface.
- One new sqlite path (writes `state.db` under `data/`) — already in Plan 03
  threat surface and gated by F-3 path validator.
- One new local-filesystem write path (`data/snapshots/YYYY/MM/DD/...parquet`)
  — already in Plan 03 threat surface.

No new auth paths, no new external surface, no schema changes at trust
boundaries.

## Open items for Plan 5 (integration)

1. **Live-API smoke test** — Plan 5 should run `make snapshot-markets` against
   the real Polymarket APIs (rate-limited, ~5-10 markets) at least once and
   assert: (a) result.is_valid → True; (b) Parquet file is readable by DuckDB;
   (c) `duckdb -c "SELECT COUNT(*) FROM 'data/snapshots/.../...parquet'"`
   matches result.market_count.
2. **typer + click compat watch** — Drop the click<8.2 pin once typer ≥0.13
   is released and verified compat. Track via a CHANGELOG line.
3. **Per-row `metadata_fetched_at_ms` vs `book_fetched_at_ms` split** — see
   Documented Limitations §1. Filed as Phase 2 cleanup.

## Self-Check

Verified post-write:

- [x] `src/polyarb/snapshot/normalizer.py` — FOUND
- [x] `src/polyarb/snapshot/orchestrator.py` — FOUND
- [x] `src/polyarb/snapshot/cli.py` — FOUND
- [x] `src/polyarb/snapshot/__main__.py` — FOUND
- [x] `src/polyarb/cli.py` — FOUND
- [x] `tests/m1-perception/test_orchestrator.py` — FOUND
- [x] T1 commit `8672a41` (normalizer) — FOUND in git log
- [x] T2 commit `0edfeaa` (orchestrator) — FOUND in git log
- [x] T3 commit `0eca344` (CLI) — FOUND in git log
- [x] T4 commit `864bd63` (entry-points + click pin) — FOUND in git log
- [x] T5 commit `e4c7b54` (Makefile) — FOUND in git log
- [x] T6a commit `c76bb9f` (Rule 1 prices unwrap) — FOUND in git log
- [x] T6b commit `900ca32` (orchestrator tests) — FOUND in git log
- [x] All 6 plan tests pass (`pytest tests/m1-perception/test_orchestrator.py` → 6 passed)
- [x] All 57 m1-perception tests pass (no regression in Plan 01-1/01-2/01-3)
- [x] `make help` lists snapshot-markets + snapshot-markets-full
- [x] `python -m polyarb.snapshot --help` and `polyarb --help` both render typer help

## Self-Check: PASSED
