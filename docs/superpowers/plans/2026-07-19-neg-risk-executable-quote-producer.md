# Neg-Risk Executable Quote Producer — Implementation Plan

> **For implementation:** use the preselected subagent-driven workflow. Work in
> an isolated worktree; implement and review each task separately, with tests
> written first. Do not deploy, alter Fly machines, alter cron, or claim the
> production feed is ready as part of this plan.

**Goal:** Replace the structurally stale snapshot-ask opportunity input with a
known-universe, atomic, read-only CLOB quote-run input. The endpoint remains
`/arbitrage/opportunities`, but it returns a valid zero only when both a recent
membership universe and one complete dedicated quote run pass their respective
freshness contracts.

**Architecture:** A snapshot remains the authority for active neg-risk group
membership. A dedicated collector derives every eligible YES token from that
one snapshot, fetches CLOB books using the existing batched read-only client,
and writes one all-or-nothing quote run to SQLite. The scanner selects exactly
one latest `complete` run and the snapshot it references; it never combines
rows from runs. The HTTP surface fails closed for missing/stale quote runs and
for stale universe membership.

**Tech stack:** Python 3.12, stdlib `sqlite3`, existing `ClobReaderClient`,
Typer, Starlette, pytest, Makefile.

**Design source:**
`docs/superpowers/specs/2026-07-19-neg-risk-executable-quote-producer-design.md`

## Immutable contracts

| Contract | Implementation consequence |
|---|---|
| Snapshot is membership authority | Quote collection reads exactly one latest snapshot and only its `active=1`, `closed=0`, non-empty group/token rows. It does not discover markets. |
| Quote run is atomic | A run becomes `complete` only after it has exactly one terminal quote record for every requested token. A failed/partial run cannot be selected. |
| Group completeness is atomic | A group with any terminal non-executable sibling, missing response, bad price, or non-positive size is excluded as a whole. |
| No mixed data | Scanner loads one complete `run_id`, joins quote rows on that same ID, and verifies its `universe_snapshot_id`; no `MAX()` per token/group query is allowed. |
| Freshness is two-clock | Quote clock: 300 seconds. Universe clock: 50,400 seconds (14 h). Neither threshold is relaxed. |
| No prod change in this phase | New local commands may make read-only CLOB GETs and write their explicitly chosen local DB. No Fly deploy, cron edit, scheduler wiring, CLOB order, wallet, or authenticated call. |

## Task 1 — Persist an atomic quote-run sidecar

**Files:**

- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Create: `src/polyarb/routing/neg_risk_quote_store.py`
- Create: `tests/routing/test_neg_risk_quote_store.py`

### Step 1: Write failing storage tests

Create a temporary SQLite database through `SQLiteStore(path).init_schema()`.
Seed `snapshots` and the latest snapshot's `markets` rows directly where a
test needs membership. Write tests for the storage API described below.

1. A started run is invisible to `latest_complete_run()`.
2. A run can become complete only after exactly every requested token has one
   terminal row. Attempting completion with fewer rows raises a bounded
   `QuoteRunStateError` and leaves status `collecting`.
3. Terminal records use a constraint-backed bounded reason vocabulary:
   `executable`, `missing-book`, `missing-ask`, `invalid-ask-price`,
   `invalid-ask-size`, `collector-error`. `executable` requires a finite price
   in `(0, 1]` and positive finite size; all other states require null price and
   size. Validate this both in Python and by tested table constraints where
   feasible.
4. A failed run never displaces a prior complete run: create complete run A,
   begin then fail run B, and assert the selected run is still A with only A's
   rows.
5. `begin_run()` implements a no-overlap lock. While A is `collecting`, a
   second begin raises `QuoteRunBusyError`; after A is marked failed or complete,
   B may begin. Use two connections/API instances to prove it is database state,
   not an object-local flag.
6. A latest-complete projection returns `run_id`, the parent universe snapshot
   ID and taken time, quote time, requested/response count, and every row from
   that one run. It must return `None` when no complete run exists.

Use a fixed `now_ms` in every test; no test sleeps or contacts CLOB.

### Step 2: Add DDL and a focused store API

Append the following tables and indexes to `DDL` in
`src/polyarb/storage/schemas.py` (the current `SQLiteStore.init_schema()`
already runs `DDL` idempotently):

```sql
CREATE TABLE IF NOT EXISTS neg_risk_quote_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  universe_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  universe_taken_at_ms INTEGER NOT NULL,
  quoted_at_ms INTEGER NOT NULL,
  requested_token_count INTEGER NOT NULL CHECK(requested_token_count >= 0),
  successful_response_count INTEGER NOT NULL DEFAULT 0
      CHECK(successful_response_count >= 0),
  status TEXT NOT NULL CHECK(status IN ('collecting', 'complete', 'failed')),
  failure_reason TEXT,
  completed_at_ms INTEGER,
  CHECK((status = 'complete' AND failure_reason IS NULL AND completed_at_ms IS NOT NULL)
     OR (status = 'failed' AND failure_reason IS NOT NULL)
     OR (status = 'collecting' AND failure_reason IS NULL AND completed_at_ms IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_quote_runs_select
  ON neg_risk_quote_runs(status, quoted_at_ms DESC, id DESC);

CREATE TABLE IF NOT EXISTS neg_risk_quotes (
  quote_run_id INTEGER NOT NULL REFERENCES neg_risk_quote_runs(id),
  neg_risk_market_id TEXT NOT NULL,
  market_id TEXT NOT NULL,
  condition_id TEXT NOT NULL,
  slug TEXT,
  yes_token_id TEXT NOT NULL,
  terminal_state TEXT NOT NULL CHECK(terminal_state IN (
    'executable', 'missing-book', 'missing-ask', 'invalid-ask-price',
    'invalid-ask-size', 'collector-error'
  )),
  best_ask_price REAL,
  best_ask_size REAL,
  PRIMARY KEY(quote_run_id, yes_token_id),
  CHECK((terminal_state = 'executable' AND best_ask_price > 0
      AND best_ask_price <= 1 AND best_ask_size > 0)
    OR (terminal_state != 'executable' AND best_ask_price IS NULL
      AND best_ask_size IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_quotes_run_group
  ON neg_risk_quotes(quote_run_id, neg_risk_market_id, market_id);
```

Keep the migration add-only: existing databases receive these tables through
`CREATE TABLE IF NOT EXISTS`; no destructive migrations are needed.

Create `neg_risk_quote_store.py` rather than expanding snapshot writer
responsibilities. It owns small frozen dataclasses (`UniverseLeg`,
`PersistedQuote`, `QuoteRun`, `CompleteQuoteProjection`) and these methods:

```python
class NegRiskQuoteStore:
    def __init__(self, db_path: Path | str) -> None: ...
    def latest_universe(self) -> tuple[int, int, tuple[UniverseLeg, ...]] | None: ...
    def begin_run(self, *, universe_snapshot_id: int, universe_taken_at_ms: int,
                  legs: tuple[UniverseLeg, ...], quoted_at_ms: int) -> int: ...
    def record_terminal_quotes(self, run_id: int, quotes: tuple[PersistedQuote, ...]) -> None: ...
    def complete_run(self, run_id: int, *, completed_at_ms: int) -> QuoteRun: ...
    def fail_run(self, run_id: int, *, failure_reason: str) -> None: ...
    def latest_complete_projection(self) -> CompleteQuoteProjection | None: ...
```

`latest_universe()` queries only `MAX(snapshots.id)` and selects rows whose
`snapshot_id` equals that ID, `active=1`, `closed=0`, and whose group/token are
non-empty. It returns deterministic `(group, market)` order and preserves
`condition_id`/`slug` for route output. De-duplicate token IDs before begin;
reject duplicate token IDs that map to inconsistent market identity rather than
silently overwriting them.

Every state-changing method opens its own `sqlite3` connection with
`isolation_level=None`, enables WAL/foreign keys, and brackets the operation in
`BEGIN IMMEDIATE`/`COMMIT`, using the existing rollback helper pattern. In
`begin_run`, first reject any `collecting` row before inserting. In
`record_terminal_quotes`, require the incoming token set to be a subset of the
run's requested set and use plain `INSERT`, never `INSERT OR REPLACE`.
In `complete_run`, count requested tokens versus persisted rows and require each
token exactly once; set `successful_response_count` to count terminal rows that
are `executable` and then update status in the same transaction. `fail_run`
only transitions `collecting -> failed`; it cannot alter a complete run.

### Step 3: Run focused tests

Run:

```bash
uv run pytest -q tests/routing/test_neg_risk_quote_store.py
```

Expected: all storage invariants pass. Commit this task only after the focused
test is green.

## Task 2 — Collect one read-only, complete-or-failed CLOB quote run

**Files:**

- Create: `src/polyarb/routing/neg_risk_quote_collector.py`
- Modify: `src/polyarb/cli_arbitrage.py`
- Create: `tests/routing/test_neg_risk_quote_collector.py`
- Modify: `tests/cli/test_arbitrage_cli_process.py`

### Step 1: Write failing collector and CLI tests

Use a fake async reader with `get_books(token_ids)` that records requested
tokens and returns fixture-like objects/dicts containing `asset_id`, `asks`,
where asks use the CLOB `{price, size}` shape. Do not mock internals of
`ClobReaderClient`.

Cover:

1. The collector reads every eligible token from one latest universe and calls
   `get_books()` once with the complete de-duplicated token list.
2. It selects the lowest valid ask by numeric price, carries its matching size,
   and produces `executable` only for a finite price `(0, 1]` and positive
   finite size.
3. Missing book, no asks, malformed price, and malformed/non-positive size
   each become their corresponding terminal non-executable reason, so a normal
   partial API response can still make a *complete* run with a visible
   non-executable sibling.
4. A transport exception marks the newly started run failed with bounded
   reason `clob-fetch-failed`; the prior complete run remains selectable.
5. An unexpected duplicate/mismatched CLOB asset ID or unusable payload marks
   the new run failed, never complete.
6. A busy store surfaces `QuoteRunBusyError` without making a network call.
7. The Typer process command emits a JSON summary with `run_id`, `status`,
   `universe_snapshot_id`, `requested_token_count`,
   `successful_response_count`, `quote_taken_at_ms`, and `elapsed_ms`; its
   non-success paths exit 2 and do not claim a successful collection.

### Step 2: Implement collector and local CLI command

Implement `collect_neg_risk_quotes()` in
`neg_risk_quote_collector.py`. Inject `quote_store`, `reader`, and a `now_ms`
callable for deterministic tests. Its return type is a frozen
`QuoteCollectionResult` with the summary fields above.

Algorithm:

1. Call `quote_store.latest_universe()`. If absent or it has zero eligible
   legs, return/raise a bounded `QuoteUniverseUnavailableError` before any CLOB
   request.
2. Start a run before contacting CLOB. This establishes the overlap lock and
   records the snapshot membership version.
3. Await `reader.get_books(token_ids)` exactly once. Index returned objects by
   `asset_id`; unknown/duplicate response assets are collection integrity
   failures, not ignored data.
4. Build exactly one `PersistedQuote` per requested universe leg. A missing
   requested asset becomes `missing-book`. For a returned book, inspect asks
   with a small defensive accessor supporting fixture dicts and SDK objects;
   use the best valid ask after numeric validation. If a response has asks but
   none can form a valid price/size pair, choose the precise bounded terminal
   reason.
5. Write all terminal rows, complete the run, and return result. If fetching,
   parsing, or persistence fails after begin, best-effort `fail_run()` with a
   bounded failure reason, preserve the original exception for CLI reporting,
   and never promote a partial run.

Add Typer command `collect-neg-risk-quotes` to `cli_arbitrage.py`:

```text
--db-path PATH       default data/state.db
--verbose / -v       existing log convention
```

It first calls `SQLiteStore(db_path).init_schema()` so a local operator can use
an initialized sidecar safely, builds `Settings()` and `ClobReaderClient`, then
uses `asyncio.run(collect_neg_risk_quotes(...))`. It prints a stable JSON result
on success. It performs read-only CLOB calls but deliberately writes **only the
explicit local `--db-path`**. Help text must say this is one local collection,
not a production scheduler and not an order command.

### Step 3: Run focused tests

```bash
uv run pytest -q tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py \
  tests/cli/test_arbitrage_cli_process.py
```

Expected: fixture-only tests prove no partial quote run can become selected.

## Task 3 — Scan one complete run and fail closed at the HTTP boundary

**Files:**

- Modify: `src/polyarb/routing/opportunity_scanner.py`
- Modify: `src/polyarb/http/arbitrage.py`
- Modify: `src/polyarb/routing/opportunity_diagnosis.py`
- Modify: `tests/routing/test_opportunity_scanner.py`
- Modify: `tests/m1-perception/test_arbitrage_opportunities_http.py`
- Modify: `tests/routing/test_opportunity_diagnosis.py`

### Step 1: Write failing scanner, HTTP, and diagnosis tests

Extend the existing scanner fixture to use `SQLiteStore.init_schema()` and seed
one universe snapshot plus quote-run rows. Add tests proving:

1. A fresh complete run yields the same gross-before-fees buy-all arithmetic:
   lowest group sum, minimum leg size, strict positive edge, deterministic
   `(-edge, group_id)` order.
2. A group is absent if any one sibling's terminal state is non-executable,
   even when the other sibling asks imply an apparent edge.
3. A failed/collecting newer run does not suppress an older fresh complete run.
4. A quote age exactly 300 seconds passes and greater than 300 seconds raises
   `StaleQuoteRunError` with exactly
   `quote age {age:.1f}s exceeds {limit:.1f}s`.
5. A universe age exactly 50,400 seconds passes and greater than 50,400 seconds
   raises `StaleUniverseError` with exactly
   `universe age {age:.1f}s exceeds {limit:.1f}s`.
6. No complete run raises `QuoteRunUnavailableError("quote run unavailable")`.
7. The scanner's result includes `quote_run_id`, `quote_age_seconds`,
   `universe_snapshot_id`, and `universe_age_seconds` so consumers can see its
   known-universe bound. It contains no snapshot best-ask fallback.
8. HTTP returns 200 only for fresh scanner output and returns the exact bounded
   503 error for each of the three precondition exceptions. Invalid numeric
   query behavior remains 400.
9. `diagnose_opportunity_feed()` maps only the exact stale strings to new
   `stale-quote-run` and `stale-universe` kinds, retaining parsed age/limit in
   generic fields renamed from snapshot-specific names. Existing exact snapshot
   matching remains backward compatible. Unknown 503 remains `feed-unavailable`.

### Step 2: Implement scanner and route

Keep `scan_neg_risk_buy_all()` as a compatibility entry point for existing
offline/snapshot callers, but add `scan_neg_risk_quote_run()` as the endpoint
source of truth. Its explicit API is:

```python
def scan_neg_risk_quote_run(
    db_path: Path | str, *, min_edge_bps: float = 0, max_quote_age_s: float = 300,
    max_universe_age_s: float = 50_400, limit: int = 50,
    now_s: Callable[[], float] = time.time,
) -> list[NegRiskOpportunity]: ...
```

Validate finite/non-negative thresholds and reject `bool` limits/thresholds
where ambiguity matters. Use `NegRiskQuoteStore.latest_complete_projection()`.
Check quote age first, then universe age; if both fail, report stale quote first
because it is the newest execution-critical observation. Build legs only from
the selected run's `terminal_state='executable'` records. Group expected legs
by the projection's complete membership rows; any non-executable row makes the
whole group invalid. Preserve `Decimal(str(value))` arithmetic and add the new
run/universe fields to `NegRiskOpportunity.to_dict()`.

In `http/arbitrage.py`, call `scan_neg_risk_quote_run(...,
max_quote_age_s=300, max_universe_age_s=50_400)`. Catch the three bounded
precondition errors separately and return their exact public messages with 503;
keep database/validation failures generic (`snapshot database: ...`) and do not
leak tracebacks. Add response metadata:

```json
{
  "coverage": "known-universe",
  "quote_sla_seconds": 300,
  "universe_sla_seconds": 50400
}
```

The successful response must still include `strategy`, `profit_basis`, `count`,
and `opportunities`. Update `_is_valid_success_payload()` to require the new
fixed coverage and integer SLA fields so H-008 cannot interpret an arbitrary
200 as a valid executable feed.

Update `opportunity_diagnosis.py` with a common bounded `age_seconds` and
`max_age_seconds` representation. To avoid breaking current consumers,
continue emitting `snapshot_age_seconds`/`max_snapshot_age_seconds` for
`stale-snapshot`, emit `quote_age_seconds`/`max_quote_age_seconds` for
`stale-quote-run`, and emit `universe_age_seconds`/`max_universe_age_seconds`
for `stale-universe`. Never include raw server body/error text.

### Step 3: Run focused tests

```bash
uv run pytest -q tests/routing/test_opportunity_scanner.py \
  tests/m1-perception/test_arbitrage_opportunities_http.py \
  tests/routing/test_opportunity_diagnosis.py
```

Expected: fresh and only fresh atomic data reaches HTTP 200; no stale/missing
condition can masquerade as an empty result.

## Task 4 — Expose operator commands, capability evidence, and the living manual

**Files:**

- Modify: `Makefile`
- Modify: `tests/test_makefile.py`
- Modify: `tools/climb/eval_local.py`
- Modify: `tests/climb/test_eval_local.py`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Modify: `docs/status/climb/CURRENT.md`
- Modify: `docs/status/climb/hypotheses.yaml`
- Modify: `.planning/JOURNAL.md`
- Create: `docs/learning/19-独立报价运行与已知市场覆盖.md`
- Modify: `docs/learning/00-INDEX.md`

### Step 1: Write failing Make/evaluator tests

Add static/dry-run tests requiring these Make targets and safe properties:

1. `collect-neg-risk-quotes` forwards `db=<path>` to
   `cli_arbitrage collect-neg-risk-quotes --db-path`, has no `flyctl`,
   deploy/restart/secret/schema migration, order, wallet, or write HTTP verb.
2. `scan-arb-quotes` forwards `db`, `min_edge_bps`, `max_quote_age_s`, and
   `max_universe_age_s` to the quote-run scanner CLI (add a quote scan command
   if Task 3 has not made the current `scan` command selectable).
3. Both targets appear in `make help` and `make -n` passes with default and
   overridden values.
4. `eval_local` has an `opportunity-feed-cadence-sla` profile whose command
   is a fixture-only pytest selection plus `make -n` checks; it makes no
   network call and requires the quote-store/collector/scanner/HTTP tests.

### Step 2: Implement local-only entry points and documentation

Add concise Make targets near the M2 section:

```make
## collect-neg-risk-quotes: One local read-only CLOB quote collection for the known latest snapshot universe; never deploys or orders.
collect-neg-risk-quotes:
	@uv run python -m polyarb.cli_arbitrage collect-neg-risk-quotes --db-path "$(if $(strip $(db)),$(db),data/state.db)"

## scan-arb-quotes: Inspect the latest complete known-universe quote run from a local SQLite DB; returns nonzero if unavailable/stale.
scan-arb-quotes:
	@uv run python -m polyarb.cli_arbitrage scan --db-path "$(if $(strip $(db)),$(db),data/state.db)" --min-edge-bps "$(or $(min_edge_bps),0)" --max-quote-age-s "$(or $(max_quote_age_s),300)" --max-universe-age-s "$(or $(max_universe_age_s),50400)"
```

If preserving the old `scan` defaults would be unsafe/confusing, instead name
the quote command `scan-quotes` and point the second target to it. Do not change
an existing operator command's source semantics silently.

Add the H-009 local evaluator profile to `tools/climb/eval_local.py`. It must
only use offline test commands and Make dry-runs. It is evidence of code
correctness, **not** confirmation of H-009 and must not update hypothesis status
to confirmed.

Update the manual's opportunity-feed row and first-run path: current production
status remains **conditional/not ready** until a separately authorized deploy,
capacity observation, repeated complete runs, and immutable evidence exist.
Document:

- `make collect-neg-risk-quotes db=data/state.db` is a local read-only CLOB
  collector with a local SQLite write; it is not a production cron job;
- `make scan-arb-quotes` is valid only when both clocks pass and still reports
  gross-before-fees, not an order instruction;
- known-universe coverage excludes newly listed groups until the next membership
  snapshot;
- HTTP 503 (stale quote/universe/no run) is never a zero result;
- production rollout has an explicit later gate and is not performed here.

Create learning document 19 in the existing 30-second mental-model format:
model, source-file links/line pointers after implementation, run state diagram,
why atomically complete groups matter, practical commands, trade-offs, a small
worked price/size example, 3–5 decision questions, and FAQ increment section.
Link it from `00-INDEX.md` after document 18.

Update `CURRENT.md`, `hypotheses.yaml`, and JOURNAL accurately: H-009 remains
`pending` after local implementation because live capacity/deployment evidence
was intentionally not performed. Record the exact remaining authorization:
production deployment/scheduling and timestamped read-only capacity observation.
Do not fabricate run results, response coverage, or readiness.

### Step 3: Run final local verification

```bash
uv run pytest -q tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py \
  tests/routing/test_opportunity_scanner.py \
  tests/m1-perception/test_arbitrage_opportunities_http.py \
  tests/routing/test_opportunity_diagnosis.py \
  tests/cli/test_arbitrage_cli_process.py \
  tests/climb/test_eval_local.py tests/test_makefile.py
make eval-local profile=opportunity-feed-cadence-sla
make docs-m1-check
make planning-status
git diff --check
```

Expected: all named local gates pass. The final output must explicitly say this
proves an implementation and local contract only; production readiness remains
unproven and `H-009` stays pending.

## Cross-task review checklist

Before merging, independently review the final diff for:

1. No new authenticated CLOB client, user key, order endpoint, signed request,
   Fly deployment, cron modification, or scheduler change.
2. No route fallback to `markets.best_ask_*` or a partial/mixed quote run.
3. Every run status transition is transactionally valid, and failed runs cannot
   become complete later.
4. Exact age error strings match diagnostic regular expressions and 503 tests.
5. A valid 200 has the complete coverage/SLA metadata; zero is never emitted
   for missing/stale quote data.
6. Make targets are discoverable with `make help`, have safe default local DB
   paths, and preserve existing command compatibility.
7. Manual/current state truthfully remains conditional and H-009 pending.

## Completion evidence

Implementation is complete only after every focused test and the final local
verification block above passes, documentation is current, and the plan summary
records the production boundary. It does **not** authorize or imply deployment,
scheduling, production database writes, or real-money execution.
