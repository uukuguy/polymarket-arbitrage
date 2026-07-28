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
  change/age inputs, explicit weights, output, and reason. Unbounded elapsed-age
  rank provides eventual anti-starvation.
- Added one `BEGIN IMMEDIATE` publication boundary for certified revisions,
  group schedules, promotions, per-group coverage samples, and the next cursor.
- Added restart-safe terminal-cursor semantics and a cancellation barrier that
  finishes one started commit before propagating cancellation.
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
