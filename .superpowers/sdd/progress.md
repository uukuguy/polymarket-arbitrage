# M1 Verified Perception SDD Progress

Baseline: `76f9e75`

- Plan 1 / Task 1: complete (commits 76f9e75..6410157, review clean)
- Plan 1 / Task 2: complete (commits 6410157..50fed01, review clean)
- Plan 1 / Task 3: complete (commits cf94d99..8a876c2, review clean)
- Plan 1 / Task 4: complete (commits 8a876c2..539826d, review clean)
- Plan 1 / Task 5: complete (commits 539826d..9bf026a, review clean)
- Plan 1 / Task 6: pending
- Plan 2 / Task 1: pending
- Plan 2 / Task 2: pending
- Plan 2 / Task 3: pending
- Plan 2 / Task 4: pending
- Plan 2 / Task 5: pending
- Plan 3 / Task 1: pending
- Plan 3 / Task 2: pending
- Plan 3 / Task 3: pending
- Plan 3 / Task 4: pending
- Plan 3 / Task 5: pending
- Plan 3 / Task 6: pending

Minor review findings to revisit in final whole-branch review: none.

## M1 Event-Driven Self-Healing — Plan 01

Plan:
`docs/superpowers/plans/2026-08-24-m1-self-healing-01-runtime-evidence.md`
Baseline: `2d337d28`

- Pre-flight stale Quote supervision fixture: complete (commit `8e03acc4`, review clean)
- Baseline scheduler retention hang: complete (commit `7cdc5e25`, review clean)
- Baseline Quote child timeout fixture: complete (commit `b4c13a9a`, review clean)
- Baseline Dashboard retry source contract: complete (commit `6c662797`, review clean)
- Baseline L2 mirror upsert contract: complete (commit `15ed9764`, review approved with one Minor)
- Baseline isolated Quote topology fixture: complete (commit `b2ea4c9c`, review clean)
- Baseline returned-supervisor restart fixture: complete (commit `77a69bd9`, review clean)
- Baseline Structure current-contract/query-plan drift: complete (commit `d50feaea`, review clean; fast nodes 2 passed; current/V3 compatibility subset 38 passed; slow 120k gate 1 passed in 1267.30s)
- Baseline Candidate reserved-lane contract: complete (amended commit `15b295ee`, review clean; reviewer-style 10/10 and full file 37 passed)
- Baseline Quote wedged-pipes cleanup timing: complete (commit `054bc82d`, review clean; 10/10 and full file 46 passed)
- Baseline reconciliation cursor recovery clock: complete (commit `7eadb335`, review clean; 20/20 and full file 21 passed)
- Baseline reconciliation stall observation: complete (commit `410cd905`, review clean; 20/20 and supervisor file 55 passed)
- Task 1: complete (commit `43e2af46`, review approved; 34 focused tests)
- Task 2: complete (commit `afa55cd5`, independent review clean; 13 focused and 69 full Postgres tests)
- Task 3: complete (commit `be123ac2`, independent review clean; 35 focused tests)
- Task 4: complete (commit `fdd3ebcd`, independent review approved; 251 selected tests, real 191-sample replay matched)
- Post-review hardening: complete (`46215008`, `7139d61a`; both focused reviews clean)
- Final whole-branch review: approved at `0ee6c64c` (0 Critical / 0 Important / 0 Low; final 346-test gate passed)

## M1 Event-Driven Self-Healing — Plan 02

Plan:
`docs/superpowers/plans/2026-08-24-m1-self-healing-02-task-instrumentation.md`
Baseline: `0ee6c64c`

- Task 1: complete (amended commit `f8e8f0f8`; independent review approved with 0 findings; reporter + Postgres runtime gate 44 passed; Ruff passed; changed-file Pyright 0 errors)
- Task 2: complete (`208b8231`, `520bdbea`, `c485806c`, `03f2e77b`, `ee5f4290`, `90517113`, `745c4901`; final independent review approved with 0 Critical / 0 Important / 0 Low)
- Task 3: complete (`2ca127a8`, `e190d215`, `8d2b672d`, `4b4c4c65`, `e3b6edea`, `a0ec3de6`; final independent review approved with 0 Critical / 0 Important / 0 Low)
- Task 4: complete (`24c4c2e5`; independent review approved with 0 Critical / 0 Important / 0 Low; 22 worker tests and 69 focused Postgres tests passed)
- Task 5: complete (`3fada81c`, `f611eb0b`, `9da99183`, `46ecb773`; review approved; 18 focused coverage tests, full transactional gate, Ruff, and planning-status passed)

Task 5 minor review follow-up for closure docs: add `46ecb773` to the
`05.6-202-SUMMARY.md` Task 5 commit list.

## M1 Event-Driven Self-Healing — Plan 03

Plan:
`docs/superpowers/plans/2026-08-24-m1-self-healing-03-reconciler-recovery.md`
Baseline: `d3c9edd7`

- Task 1: complete (`555ecd23`, `fd7f611d`; review approved; 55 focused tests, Ruff, diff check)
- Task 2: complete (`2fbf7408`, `aebc6cd9`, `497dcfa5`, `089be676`; final independent review approved; 19 focused Postgres/migration tests, runtime/reconciler 55 tests, Ruff, diff check)
- Task 3: complete (`f1561e63`, `e7297ad8`, `9d7d790a`; final independent review approved; executor 15 tests, recovery Postgres 21 tests, migration 4 tests, owned-surface Pyright 0 errors, Ruff, diff check)
- Task 4: complete (`147082da`, `df3cf2d0`, `9617c72a`, `93869d3c`, `2cf29953`, `beb7fd6b`, `d587fe98`, `d5bb03ee`, `e5d422d6`, `91e50d32`, `db575070`, `1e761faf`, `9567c636`; final independent review approved; full Plan03 gate, 9 CLI fail-loud/service tests, 4 real Postgres race/status tests, Ruff, owned Pyright, diff check, planning-status)

## M1 Event-Driven Self-Healing — Plan 04

Plan:
`docs/superpowers/plans/2026-08-24-m1-self-healing-04-rolling-qualification.md`
Baseline: `9f450eab`

- Task 1: complete (`0b4eabf4`, `16b697a7`, `8167d08e`; final independent review approved; 37 policy tests, Ruff, Pyright, diff check)
- Task 2: complete (`3bc828e1`, `c758af01`, `314b888b`; final independent review approved; 8 focused Postgres/migration tests, 3 migration tests, 37 policy tests, Ruff, owned Pyright, diff check)
- Task 3: complete (`e6ecf98e`, `654e7df4`, `9caa957e`; final independent review approved; 72 selected qualification/migration nodes, real Postgres recovery/late-ingress/freshness probes, Ruff, owned Pyright, diff check, planning-status)
- Task 4: complete (`0828b884`, `bfd11e20`, `1f8463bd`; final independent review approved; 7 qualification Make contracts, 96 full Make contracts, replay digest `a0c5ab1f1bae4f7356a15430b0889b6a3de433654888f640a601b748ce878875`, planning-status clean)

## M1 Event-Driven Self-Healing — Plan 05

Plan:
`docs/superpowers/plans/2026-08-24-m1-self-healing-05-operator-surfaces.md`
Baseline: `b48a7f6e`

- Task 1: complete (`1845b30a`, `cbd52cbf`; final independent review approved; 7 focused runtime/qualification tests, full owned files, 58-test qualification sweep, real PostgreSQL redacted-503 probe, Ruff/diff check)
- Task 2: complete (`25c864d4`, `4ab417bd`, `971f4787`, `7634ceb8`; final independent review approved; strict decoder contract 2 nodes, TypeScript typecheck, production Dashboard build, malformed time/lease/qualification/action/fence facts fail closed)
- Task 3: complete (`5c80db4b`, `b0751378`; final independent review approved; 4 Dashboard contracts, TypeScript typecheck, production build; open incidents and independent completed/unlinked recovery actions remain visible)
- Task 4: complete (`e35ff79b`, `bec27869`; final independent review approved;
  81 alert/watchdog/CLI tests, real-PostgreSQL concurrency/rich-outbox/order
  probes, restart-safe reminder cadence, Ruff, owned Pyright, diff check)
- Task 5: complete (`4016aef2`, `8553e3b5`; authenticated-body smoke contracts,
  docs/teaching 87/build/typecheck/planning gates; H-017 profile `50d65016` and
  cycle 18 `20260825-074318-h-017` all five subscores 100)

## M1 Event-Driven Self-Healing — Plan 06

Plan:
`docs/superpowers/plans/2026-08-24-m1-self-healing-06-production-enablement.md`
Baseline: `ff43f93c`
Climb hypothesis: H-018 (`f296b9f7`, pending)

- Task 1: in progress (local deterministic real-PostgreSQL fault matrix)
- Task 2: in progress (least-privilege controller/qualification topology)
- Task 3: in progress (exact capability-limited Fly recovery adapter)
- Task 4: pending (observe-only production gate; deployment requires exact authorization)
- Task 5: pending (job/process production fault gates; exact mutation authorization required)

Task 1 lifecycle evidence:

- Normal stop and `__aexit__` drain an in-flight `asyncio.to_thread` heartbeat before returning; controlled `threading.Event` probes proved no lease renewal after exit.
- Heartbeat failure cancels the owning task and surfaces the original fenced-store error.
- Job type and stage names use a closed per-job registry; cross-job stages reject before persistence.
- Sync and async reporters fail before persistence when the wall clock regresses.

Task 2 lifecycle evidence:

- The 207-second virtual Quote admission regression records at least six fenced heartbeats, monotonic stage progress, no expired observation, and unchanged terminal Quote identities.
- All worker-side blocking calls shield and drain their executor thread; cancellation cannot return while R2 or Postgres work continues in the background.
- Terminal admission is a bounded point of no return: committed success wins over scheduler cancellation, while real terminal failure remains authoritative.
- Quote admission appends exactly one `job.succeeded` event in the same transaction as job/attempt success and Quote batch enqueue; injected event, lock, and statement failures roll the transaction back completely.
- Heartbeat, progress, retry, recovery, admission reads, production service connections, and watchdog incident ingress now have bounded connect/query/lock behavior.
- A recovery timeout after committed admission is explicitly reported as `admitted:recovery-pending` and leaves the durable incident visible instead of falsely reporting admission failure.
- A heartbeat renewal racing `stop()` is retained and exposed to the terminal fenced commit; external cancellation racing the drain is re-propagated after the in-flight call finishes.

Task 3 lifecycle evidence:

- `structure-fetch`, `structure-materialize`, `structure-normalize`, and `structure-certify` expose exact bounded stage chains with meaningful monotonic progress and renewed leases.
- Synchronous normalize/certify blocking calls run under heartbeat polling, attempt deadlines, underlying connection/read/query/lock timeouts, and drain semantics.
- Source page, source bundle, range completion, and certification append one success fact atomically with their receipt/pointer and job/attempt success.
- Retryable failure and retry scheduling facts commit atomically with job/attempt, circuit, incident, and alert-outbox transitions.
- Historical receipt-backed half-completions and succeeded rows missing runtime evidence are narrowly validated and repaired to exactly one `job.succeeded`; injected repair failures leave no partial event.
- Post-terminal recovery failures are reported as recovery-pending outcomes rather than falsely converting durable success into a retry failure.

Baseline gate after pre-flight correction:

- Final full `make test` at `410cd905`: 5291 passed, 8 skipped, 1 xfailed,
  0 failed in 2185.86s. Baseline gate green; Plan 01 Task 1 may begin.
- Current full `make test` at `15b295ee`: 5289 passed, 8 skipped, 1 xfailed,
  2 failed in 2697.67s; all previously unbounded scheduler/supervisor nodes
  completed. Remaining nodes: Quote killed-child wall-clock budget and
  reconciliation cursor recovery incident closure.
- An orphan full-suite process left by an out-of-scope agent run was discovered
  competing for one CPU during the early Quote failure and was terminated; the
  Quote node therefore requires isolated reproduction before classification.
- Original `test_perception_supervision.py` indefinite retry: fixed and bounded.
- Full `make test`: stopped at 44% after 636.69s; 2372 passed, 4 deterministic
  failures, and `test_successful_tick_defers_retention_while_quote_pipeline_active`
  hung indefinitely.
- The four failures reproduce in isolation: Quote hard-timeout wall-clock bound,
  Dashboard retry copy contract, and two L2 Supabase mirror insert/upsert contract
  mismatches.
- Plan 01 Task 1 has not been dispatched pending the baseline decision.

Minor review findings to revisit in final whole-branch review:

- `tests/m1-perception/test_l2_supabase_mirror_book_levels.py` retains the
  stale helper name `raise_on_insert` although the exercised verb is upsert.
- Generic upsert still permits same-key/different-value overwrite; this is a
  separate production truth-policy gap, not part of the baseline repair.
- Task 1 timestamp detail UTC boundary: resolved in Task 2 persistence (`afa55cd5`).
- Task 1 declared-code taxonomy boundary: resolved by Task 3 closed runtime
  reason vocabulary (`be123ac2`).

- Neg-risk opportunity watcher / Task 3: complete (commits 8bd6ce1..c2fda53, review clean)
- Neg-risk opportunity watcher / Task 4: complete (commits c2fda53..8b9200b, review clean)
- Structure stage diagnostics / Task 1: complete (commits 4212216..b5f9ccc, review clean)
- Structure stage diagnostics / Task 2: complete (commits b5f9ccc..3e0c604, review clean)

## M1 Runtime Scoped Database Roles — Plan 07

Plan:
`docs/superpowers/plans/2026-08-25-m1-runtime-scoped-database-roles.md`
Baseline: `e3293cbf`

- Task 1: complete (commits e3293cbf..06388e2b, review clean)
- Task 2: complete (commits 06388e2b..61762332, review clean)
- Task 3: complete (commits 61762332..504c188f, review clean)
- Task 4: complete (commits 504c188f..ae9fe081, review clean)
- Task 5: complete (commits ae9fe081..e3c1fc83, review clean)
- Task 6: complete (commits e3c1fc83..93580b81, review clean)

Approved PostgreSQL grammar resolution: role names use `sql.Identifier`,
passwords use `sql.Literal`, composed SQL is never logged, and secret leakage
plus transaction rollback are mandatory tests because PostgreSQL 16 rejects a
bind parameter in `ALTER ROLE ... PASSWORD $1`.

## Opportunity-First Perception Rollout

Plan: `docs/superpowers/plans/2026-07-28-m1-opportunity-first-rollout.md`
Baseline: `00bac7b`

- Task 1: complete (commits 00bac7b..c65770a, review clean; production-clone gate passed)
- Task 2: complete (commits c65770a..d4fbc8d, review clean)
- Task 3: complete (commits d4fbc8d..ebdb151, review clean; 113 focused + 2481 full-suite collected passed)
- Task 4: complete (commits ebdb151..114f30e, review clean; 47 focused + 213 proportional + 2507 full-suite collected passed)
- Task 5: complete (commits 114f30e..e03f795, review clean; 89 focused + 318 proportional + 2627 full-suite collected passed)
- Task 6: complete (commits e03f795..d8e2315, review clean; 235 focused + 438 expanded + 2938 full-suite collected, 0 failures)
- Task 7: complete (commits fd4c37d..472d8e2; full repository suite passed; UI gate 24/24 with 0 findings)
- Task 8: pending

Task 7 read-model remediation:

- Task 1: complete (commits 8ba84cb..b9b5afa, review clean; 37 focused contracts)
- Task 2: complete (commits 5d408ad..936133f, review clean; authenticated current opportunity authority)
- Task 3: complete (commits 39862ed..b22db52, review clean; bounded Incident lifecycle authority)
- Task 4: complete (commits 7debd9b..9c49789, review clean; bounded resource authority)
- Task 5: complete (commit 301fad1, review clean; authenticated four-class group timeline)
- Task 6: complete (commit 472d8e2; full gates green; six-pillar UI review 24/24)

Minor review findings to revisit in final whole-branch review: none.
