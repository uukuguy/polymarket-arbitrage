# Task 3 Summary — Bounded Discovery and Promotion

## Delivered

- Added a one-page Gamma event API with a bounded limit, opaque cursor, explicit
  terminal proof, and shared keyset shape/cursor validation.
- Kept the legacy streaming event projection unchanged while retaining the
  additional nested identity fields only for bounded Discovery certification.
- Reused `normalize_events()` for group truth and `normalize_market()` for every
  active named leg. A group is promoted only after a real certified revision
  contains every market ID, condition ID, Yes token, and title.
- Added deterministic Decimal priority with persisted edge/activity/liquidity/
  change/age inputs, explicit weights, output, and reason. Bounded score age plus
  a durable maximum-wait deadline provides runtime anti-starvation.
- Added one `BEGIN IMMEDIATE` publication boundary for certified revisions,
  group schedules, promotions, per-group coverage samples, and the next cursor.
- Added restart-safe terminal-cursor semantics and a cancellation barrier that
  finishes one started commit before propagating cancellation.
- Review remediation revokes prior certified/Quote authority in the same page
  transaction when new truth is incomplete, unsupported, or identity-incomplete.
- Candidate freshness now comes from one durable full-set snapshot of current
  certified groups and exact matching complete batches; missing Quote degrades.
- Status now uses one validated read transaction for cursor, schedule,
  promotion/revision, Decimal/rank, count, time, and coverage chains.
- Event pages reject malformed members rather than making their cursor durable;
  legacy market-stream compatibility remains unchanged.
- Runtime anti-starvation uses a configurable maximum-wait deadline recomputed
  from persisted anchors on every selection and after restart.
- Persisted degraded duty-cycle state yields N-1 Candidate-priority cycles and
  then permits one bounded Discovery page without restart reset.
- Immutable batch receipts and per-group sample/promotion proofs bind latest
  status counts to exact writer facts; stored scores/reasons are recomputed.
- Factless/overdue promotions use reserved lower-lane capacity after a genuine
  Candidate high burst; they never become global high.
- Group authority binds `event_id` as well as membership; attempted event
  migration rejects and rolls back the whole page.
- Composed durable Discovery promotions with the legacy candidate seed; no
  current hot candidate is dropped. Before a first Candidate fact exists, its
  scheduler consumes the persisted Discovery score/class.
- Added a testable freshness controller: Discovery yields before Gamma work
  when current candidate Quote evidence crosses its hard-stale input.
- Added default-off daemon wiring and explicit page-limit/interval settings.
- Added read-only `make perception-discovery-status db_path=...`, reporting the
  exact Discovery cursor/state, queue depths, oldest visit, and 15/30/60-minute
  raw plus liquidity-weighted active-known coverage.
- Added learning document 32 and updated the index and M1 operator manual.

## Correctness and Safety Boundaries

- A malformed page, normalization conflict, duplicate group identity, or any
  transaction failure leaves the cursor and all page facts unchanged.
- Incomplete/unsupported group membership is recorded with a bounded reason but
  is not certified or promoted. No missing condition/token identity is invented.
- Coverage is rolling and statistical over the known schedule. It makes no
  zero-miss universe claim.
- The feature flags remain off by default. This task does not implement Full
  Reconciliation, incidents, public API/Dashboard, deployment, or production
  enablement.
- M1 remains observer-only: no wallet, signing, balance, order placement, or
  real-money execution.

## TDD Evidence

```text
RED — bounded page:
AttributeError: GammaClient has no fetch_active_event_page

RED — priority/worker:
ModuleNotFoundError: polyarb.perception.priority / EventPage

RED — status:
ModuleNotFoundError: polyarb.cli_discovery

RED — operational promotion order:
expected z-high before a-low; scheduler returned lexical a-low first

RED — duplicate group:
expected duplicate-discovery-group; page was incorrectly committed

GREEN — Task 3 focused:
20 passed

GREEN — Task 1/2 + Gamma/legacy proportional regression:
241 passed

REVIEW RED → GREEN:
authority revocation/rollback; full-set freshness/missing/bootstrap/restart;
malformed event members; corrupt/concurrent status snapshot; bounded ranks and
runtime overdue ordering.

FINAL GREEN — Task 3 + proportional regression:
257 passed

SECOND RE-REVIEW RED → GREEN:
persisted degraded N-cycle probe phase; overdue reserved-lane isolation;
immutable batch receipts plus score/authority/count corruption matrix; exact
event identity conflict rollback.

FINAL GREEN AFTER SECOND RE-REVIEW:
266 passed

uv run ruff check <changed Python/test files>
All checks passed!

git diff --check
pass

make docs-m1-check
M1 manual contract: OK

make perception-discovery-status db_path=<valid fixture>
exit 0; bounded JSON read from exact Discovery tables
```

## Review Focus

- Pagination: opaque cursor preservation, terminal sweep restart, and no
  invocation of the unbounded iterator.
- Atomicity: certification/schedule/promotion/coverage/cursor rollback and
  commit-time cancellation.
- Coverage: distinct-group windows and liquidity-weighted denominator semantics.
- Concurrency: Candidate writes serialized with Discovery revision changes;
  all blocking DB/normalization work leaves the HTTP event loop.
- Scope: feature remains default-off and no later-slice API, incident,
  reconciliation, deployment, or execution behavior is included.
