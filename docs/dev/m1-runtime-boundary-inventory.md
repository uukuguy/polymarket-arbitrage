# M1 Runtime Boundary Inventory

This is the production contract for timeout, ordering, interruption and
recovery boundaries. A number is valid only when its authority and outer
ordering are named here. Changing a number without updating its owning policy,
tests and this inventory is a contract violation.

## 30-second mental model

One durable attempt owns work lifetime. Provider and database bounds terminate
one subordinate I/O operation; the progress watchdog detects absence of
durable stage facts; the attempt deadline caps the complete attempt; the lease
fences writes. Scheduler cadence only decides when to look for more work and
never cancels an active lane. On shutdown, one centrally derived terminal
grace drains a point-of-no-return transaction before fencing owns any late
result.

The ordered relationship is:

```text
provider request < worker I/O < progress deadline <= attempt deadline
                                      |
                                      +-- durable stage/checkpoint evidence

scheduler cadence -- admits work only; it is not an attempt deadline
retry backoff      -- schedules a new durable attempt; it is not a sleep wrapper
terminal grace     -- applies only after stop; it is not an I/O retry budget
qualification 86,400 s -- acceptance observation window; it is not a process timeout
```

Before that window may start, the exact release/config must pass the closed
eight-node production commissioning contract. Every node needs a normal turn,
all assigned directed fault attacks, ordered detection/recovery evidence,
cleanup, its business postcondition, and a final Structure-to-Opportunity
lineage proof. Qualification then counts healthy eligible seconds rather than
wall time: an expected recoverable incident pauses or blocks accumulation in
the same epoch; only truth, fencing, progress-integrity or identity defects
invalidate the epoch.

## Authoritative boundaries

| Boundary | Sole authority | Cancellation / late result | Durable evidence | Verification |
|---|---|---|---|---|
| Lease, heartbeat, progress and attempt | `runtime_deadlines.py:runtime_policy` persisted in `m1_job_runtime_state` | heartbeat/watchdog cancels the owning task; stale lease rejects terminal writes | runtime events plus job attempt | `test_control_plane_runtime_contract.py`, `test_control_plane_runtime_policy.py` |
| Provider request | provider client configured from `RuntimePolicy.provider_timeout_seconds` | provider coroutine/socket is cancelled or closed | stage-start remains current | Gamma, Quote and R2 focused tests |
| Worker I/O | `RuntimePolicy.io_timeout_seconds` | cancels/detaches only the subordinate request | typed retryable result at current stage | source/quote worker tests and fault matrix |
| Database connect/statement/lock | `db_deadlines.py` named policy | PostgreSQL statement/lock timeout rolls back transaction | no partial event/state mutation | real PostgreSQL deadline tests |
| Worker claim bridge | `service_lifecycle.claim_worker_job()` delegates to database policy without another timer | first service cancellation drains; grace expiry detaches and the lease fence owns a late result | claimed lease and attempt row, or no mutation | blocking-claim event-loop test plus static async-worker audit |
| Terminal fan-in | predecessor terminal transaction plus certifier-row lock | concurrent siblings serialize their final eligibility read; claim-time repair advances at most one lost wakeup | every input has receipt and matching job is `succeeded` | concurrent PostgreSQL barrier and historical-repair tests |
| Durable retry | `RuntimeRetryPolicy` | no in-process retry sleep; writes `next_attempt_at` | attempt, circuit and incident rows | retry/circuit PostgreSQL tests |
| Circuit probe | `m1_job_circuits.next_probe_at` | a recovery action opens one bounded claim window; trusted service interruption renews that window without another action or defect | circuit row and action ledger | revision 034 and circuit/interruption PostgreSQL tests |
| Recovery action count | revision 035 episode row | no reset on controller reclaim; new episode gets a separate row | `(controller,target,episode)` budget | revision 035 and episode fault case |
| Scheduler cadence | caller `interval_seconds` | never cancels an active lane; recomputes remaining time after observer work | completed turn callback | scheduler service tests |
| Daemon startup and steady supervision | `daemon.lifecycle` startup policy plus owned task set | SIGTERM before bind is a clean stop; any task exit after bind stops and drains all siblings before nonzero process exit | process log and platform restart; job recovery remains lease-fenced | L1/L2 startup and daemon-shutdown tests |
| Terminal shutdown | `service_lifecycle.terminal_grace_seconds()` | first cancel requests drain; second cancel detaches; fence owns late result | `ServiceStopRequested` or terminal receipt | service lifecycle and interruption tests |
| Operator observation | watchdog/Fly read policy constants | daemon reader is abandoned after the derived observation round | watchdog transition, never a job mutation | CLI/watchdog tests |
| Control API readiness | `CONTROL_PLANE_HEALTH_DB_POLICY` below Fly's 5 s check | detach the read-only probe at 3.5 s; platform never owns the DB call | no mutation; typed 503 | API, deployment-contract and real PostgreSQL tests |
| Pre-qualification commissioning | `production_commissioning.py` owns the closed runtime-DAG/attack registries; `production_commissioning_runner.py` owns stage ordering; `production_commissioning_disposable.py` executes real transactional normal turns | disposable exact-image by default; exact production canary only for named provider boundaries; any post-injection failure enters cleanup; no all-experiment timeout | exclusive stage receipts plus exact release/config, terminal attempt, `job.succeeded` event, causally bound business postcondition, attack lifecycle and recovery facts | `m1-production-commissioning-plan`, `m1-production-commissioning-assemble`, `m1-production-commissioning-verify` and commissioning/runtime-coverage tests |
| Disposable stale-owner fence | `PreparedNormalTurn` captures each node's real terminal transaction; `claim_job` is the only lease takeover path | virtual API time crosses the persisted lease boundary; no direct row mutation; old epoch executes the same terminal call and must raise `StaleLeaseError` | zero old-epoch succeeded attempts/events; replacement attempt epoch, success event and business postcondition | eight-node stale-owner parametrized real-PostgreSQL contract |
| Qualification window | immutable qualification policy | service can restart and resume cursors; expected recovery pauses/blocks eligible seconds in the same epoch; no outer process timeout | normalized ingress, eligibility state/reason and epoch/certificate | qualification service and 86,400 healthy-effective-second certificate |

## Provider and external-I/O inventory

| Surface | Inner bound | Outer bound | Attempts | Notes |
|---|---:|---:|---:|---|
| Gamma keyset/exact-ID HTTP | `provider_timeout_seconds` in `GammaClient` | Structure `io_timeout_seconds` | 1 | A failed durable attempt replaces the HTTP transport generation but preserves the service limiter. Replacement, cancellation and normal service cleanup all use `GAMMA_CANCELLED_CLOSE_TIMEOUT_S`; cleanup cannot become another attempt deadline or outlive SIGTERM. |
| Structure R2 PUT+HEAD | botocore connect/read split derived by `control_plane_r2_config()` | `run_blocking_call_with_timeout(io_timeout_seconds)` | 1 | Timed-out daemon thread cannot publish a fenced receipt. |
| Quote CLOB books | client request inside `_await_reader()` | Quote `io_timeout_seconds` | 1 | cancellation drains the owned request task; no scheduler wrapper timeout. |
| Quote/Structure R2 reads and writes | botocore request bounds | remaining absolute attempt budget for synchronous bridge | 1 | periodic waits only drive lease heartbeat; they do not extend the absolute deadline. |
| Telegram outbox delivery | `ALERT_DELIVERY_POLICY.provider_timeout_seconds` | stop grace below alert lease | 1 | failure is a durable retryable outbox delivery. |
| Runtime watchdog control API / Fly API | 10 s individual read | derived 21 s parallel observation round | 1 | read-only, bounded target sets; a missed round becomes an observation failure, never a Machine mutation. |
| Runtime watchdog page / private event writer | 5 s request | blocking bridge obeys service cancellation | 1 | watchdog remains independent from the transactional DB path. |
| L2 top-of-book mirror | PostgREST client request bound | dispatcher owner drains after WS producer stops | 1 per coalesced batch | latest row per asset is coalesced; `wait_idle()` is deterministic evidence and `aclose()` prevents orphan tasks. |
| Control API `/healthz` | 1 s connect, 1 s bootstrap, one 1 s `SELECT 1` | 3.5 s application envelope below Fly 5 s | 1 | readiness never builds the operator snapshot; `/perception/control-plane` retains the full read model. |
| Local Fly CLI soak read | 30 s operator read | command process timeout | 1 | operator evidence only. Long-running deployment/certificate commands do not receive this bound. |
| Local Docker build/pull | fixed `orbstack` / `DOCKER_CONTEXT=orbstack` | owning CLI process only | 1 | M1 has no second Docker runtime dependency. The user-global default is never mutated; `docker-context-status` checks Docker accounting and the OrbStack filesystem before large transfers. |

The legacy snapshot-only R2 helper retains its historical standard retry
policy. Formal runtime-v2 paths must use `control_plane_r2_config()`, whose
`total_max_attempts=1` prevents hidden retries inside the durable attempt.

## Database inventory

| Policy | Connect | Statement | Lock | Consumers |
|---|---:|---:|---:|---|
| `CONTROL_PLANE_DB_POLICY` | 5 s | 5 s | 1 s | worker transactions, API reads, qualification facts |
| `CONTROL_PLANE_HEALTH_DB_POLICY` | 1 s | 1 s | 0.25 s | minimal control API readiness only |
| `RECOVERY_DB_POLICY` | 5 s | 2 s | 1 s | controller lease, observe ledger, action scheduling/execution |
| `MIGRATION_DB_POLICY` | 10 s | 30 s | 1 s | Alembic/preflight scans only |

Server-side statement/lock limits are the mutation authority. A fresh scoped
connection always performs `connect -> bounded bootstrap/readback -> data
statement`; HTTP and stop envelopes therefore include two statement budgets,
not one. They must not retry the transaction or claim a second mutation
deadline.

## Ordering and restart contract

The durable DAG is declared once in `RUNTIME_JOB_SUCCESSORS`:

```text
structure-fetch -> structure-materialize -> structure-normalize
-> structure-certify -> quote-admit -> quote-batch
-> quote-certify -> opportunity-certify
```

- Successor jobs are admitted only by their predecessor's fenced terminal
  receipt. Scheduler iteration order is throughput policy, not dependency
  truth.
- Fan-in successors require both receipt completeness and terminal producer
  states. Final siblings serialize on the successor row; each certifier turn
  repairs at most one historically ready `waiting` row from the same durable
  facts before claiming work.
- Same-name lanes are serial; sibling job-type lanes are independent. One lane
  failure cannot cancel siblings. Dedicated Quote and Structure-range roles
  execute bounded pools of independently fenced lanes; `pool-turns` counts
  waves and is not a concurrency control.
- Structure source admission stops while even one `structure-normalize` job is
  unfinished. Queue depth may not admit a second generation whose projected
  completion would violate the 15-minute publication freshness contract.
- A pooled worker must expose the single positive lease shared by its lanes;
  terminal grace is derived from that lease's runtime policy, never copied to
  the pool as another constant.
- Every asynchronous worker claim crosses `claim_worker_job()` before its
  attempt runtime starts. Database connection/query/row-lock work never runs
  on the shared event-loop thread and never receives a competing worker timer.
- Every external source boundary persists `current=0` before I/O. Completion
  persists `current=1` only after I/O. `commit-page=0` precedes the terminal
  transaction; the terminal job receipt, not a premature progress row, proves
  commit completion.
- Normal cancellation calls `finish_interrupted`, writes
  `ServiceStopRequested`, schedules immediate resume, and does not increment a
  circuit defect streak. If the attempt was an authorized half-open probe, the
  same transaction renews one bounded claim window while keeping the circuit
  open, its failure fingerprint/count unchanged, and its episode budget intact.
- Failed source attempts retire their Gamma transport generation before the
  next attempt. They keep the shared limiter, so replacement cannot evade rate
  policy.
- Job recovery episode identity is the exact attempt ID. Circuit episode
  identity is the exact failure fingerprint. Legacy target-only exhaustion
  stays under `legacy` and is never refilled or deleted.
- An execute-only exact recovery selector is applied by PostgreSQL before
  `LIMIT`; the normal bounded observation sample cannot hide the requested job.
- Structure certification has one absolute 3,600-second attempt ceiling. Lease
  changes may alter fencing cadence but cannot scale that lifetime.
- Process and Machine recovery remain explicit-enable actions. The normal
  controller is observe-only with an empty allowlist.

## Audit findings and resolution

| Finding | Failure mode | Resolution |
|---|---|---|
| Mixed error classes shared one circuit count | unrelated failures falsely opened a circuit | revision 033 fingerprints consecutive failures |
| Service stop counted as a defect | deployment interruption consumed retries | `finish_interrupted` preserves the defect streak |
| Recovery budget rebuilt a relative circuit clock | elapsed open age compounded into multi-hour lockout | revision 034 makes `next_probe_at` the only circuit clock |
| Long-lived Gamma pool survived failed attempts | resident coordinator repeatedly timed out while fresh clients succeeded | failed attempt replaces transport generation |
| Source stage appeared only after I/O | fetch and upload timeouts collapsed to `started` | persist 0/1 and 1/1 around each boundary; fingerprint current stage |
| Commit progress said 1/1 before the DB receipt | false completion survived a failed terminal commit | persist commit 0/1; terminal receipt is completion authority |
| Budget key was target-only forever | repaired/new failures inherited exhausted history | revision 035 episode-scoped primary key |
| Schema migrated to 035 while rollout/role gates expected 034 | release preflight stopped after a successful migration | one checked 035 head across role admin, rollout and matrix |
| Fault cases selected global `LIMIT 1` | an earlier resumable row changed later test decisions | select the case's exact durable `job_key` |
| Cadence reused time measured before observer callback | slow observers added the same wait twice and reduced progress | recompute monotonic remainder after callback |
| Async workers claimed synchronously on the event loop | live 8-lane source plus materializer turns starved a healthy 0.164-second Gamma read until the 29-second worker-I/O timer won | route all five async claim sites through the cancellable blocking bridge; static audit forbids regression |
| Quote capacity was a serial turn count | 148 batches required about 28 minutes and contradicted the 900-second freshness gate | independently fenced lanes derive capacity from the existing CLOB concurrency authority; lane failures remain local |
| Structure capacity was also a serial turn count while admission tolerated 2,000 unfinished ranges | one 1,115-range generation projected to about 136 minutes and a second generation entered behind it, contradicting the same 900-second freshness gate | a 12-lane independently fenced range pool owns execution capacity; the default Structure high-water is one unfinished range, so generations cannot overlap |
| Paginated Structure drift reads delegated connection close to cyclic GC | the 120k classifier accumulated about 600 simultaneous handles to one SQLite file before GC, risking descriptor exhaustion during a healthy long recovery | one context-managed drift-read owner explicitly closes every page connection on success or error; transaction and checkpoint semantics are unchanged |
| Concurrent final receipts could both decline a wakeup | every producer was terminal while its certifier remained waiting forever | terminal transition precedes one wake; sibling checks serialize on the certifier row; bounded claim-time repair closes historical gaps |
| Receipt count stood in for producer success | a checkpointed producer could make an incomplete generation claimable | eligibility joins each receipt to a `succeeded` producer job |
| Structure source pool hid its lanes' lease policy | SIGTERM drain could not resolve terminal grace and exited the coordinator with `ValueError` | validate one common lane lease and expose it on the pool, matching the Quote pool contract |
| Fast sub-calls never reached heartbeat polling | a long Opportunity/Structure sequence of healthy sub-30-second calls silently expired its cumulative lease and surfaced `stale-lease` | check the runtime's due heartbeat after every successful nonterminal call boundary as well as while one call blocks |
| Fly readiness ran the full operator snapshot | a 5 s platform check wrapped a 10.5 s application envelope whose formula also omitted session bootstrap, producing recurring 16–56 s health flaps | use a one-statement durable-authority probe with a dedicated 3.5 s full-sequence policy; derive default request/stop envelopes from connect + bootstrap + data statement |
| Local SSH waiter outlived an already completed remote Machine | operator process appeared hung despite completed remote work | exact waiter can be interrupted safely; remote durable state is re-read before retry |
| Operator snapshot `LIMIT` still scanned history | status crossed its five-second statement boundary as attempts/outbox grew | revision 036 adds concurrent latest-attempt and pending-outbox access paths; the deadline is unchanged |
| Exact recovery target was filtered after the 100-row sample | a valid pinned action disappeared behind unrelated candidates | apply exact `job_key` in SQL before ordering and limit, then verify target type/action in memory |
| Normal Gamma cleanup had no bound | durable stop could finish but `scheduler.aclose()` could hold the process until platform kill | explicit close uses the same two-second fail-soft transport cleanup boundary |
| Certifier's nominal absolute hour was `lease * 120` | a deployment lease change silently changed total attempt lifetime | represent the 3,600-second ceiling as a distinct policy authority and reject incompatible leases |
| Ad-hoc runtime build omitted the OCI revision label | correct bytes could be pushed under a plausible tag without an independently inspectable Git identity | one Make target derives full HEAD, rejects dirty image inputs, writes the OCI revision label and never deploys |
| A long half-open probe outlived its short claim window | a healthy probe interrupted by deployment became retryable behind an already-expired open circuit and required another recovery action, so repeated normal stops could exhaust the episode budget | the fenced interruption transaction renews the existing policy-derived claim window without closing the circuit, changing its defect history or consuming another action |
| Concurrent PostgreSQL tests used 5/10-second magic barriers, including one unbounded barrier | a healthy lost-wakeup test failed only under full-suite host load, while a missing peer could make another test hang forever | every real-PostgreSQL concurrency barrier uses one named diagnostic watchdog derived from the control-plane full transaction/stop envelope; wall time is not a product assertion |
| Runtime-authority test fixed the number of policy consumers at three | adding a legitimate interruption consumer failed the full suite even though it reused the sole policy and introduced no private clock | assert every backoff use has a central-policy lookup and forbid copied formulas, without constraining how many lifecycle paths consume the authority |
| L2 top-of-book writes ran in an anonymous background task | handler return raced the write under host load, tests passed by scheduler luck, and shutdown had no owner to drain the last coalesced batch | an explicit callable dispatcher owns pending rows and its one drain generation; tests wait for idle and daemon cleanup closes it after stopping producers on every exit path |
| L2 task drain used a private five-second default while Fly declared no stop contract | cooperative cleanup could be force-cancelled early or the platform could choose an implicit signal/window, making the actual ordering deployment-dependent | one daemon lifecycle policy owns a 30-second drain below an explicit `SIGTERM/40s` platform window; the executable config/signature contract rejects drift |
| L1/L2 copied HTTP startup loops and L1 stopped observing tasks after bind | loop count and sleep cadence formed a second nominal 10-second clock; a later uvicorn/producer exit left the process alive with missing capability | one monotonic stop-aware startup helper owns both entrypoints; L1 supervises every resident task and turns any unexpected exit into ordered sibling drain plus nonzero process exit |
| Real Structure drift child had a 45-second cooperative slice inside a test-local 10-second parent timeout | full-suite host load let the outer test clock kill a healthy child before it could report `writer-busy`; production separately copied `75.0` | derive the normal parent envelope as `child slice + 30-second owned TERM/KILL budget`; production and real-child tests no longer pass another number |
| Structure event-member child still copied `75.0` in both its helper default and production caller | the sibling slice path could drift from its 45-second cooperative window or from the shared TERM/KILL protocol despite the drift path being fixed | one Structure subprocess derivation now owns both slice parents; production passes work limits only and fake-process reap tests are the sole explicit timeout overrides |
| Starting a temporary Colima profile left it as Docker's user-global default | an unrelated repository silently pulled into the 59 GiB M1 VM; containerd content/snapshots filled it although `docker system df` under-reported the filesystem usage | remove the unnecessary profile/runtime, bind every project Docker entry to the existing OrbStack context, and expose one read-only Docker plus OrbStack filesystem status target |
| L1/L2 declared a 120-second readiness grace that Fly silently capped at 60 seconds | source review and platform behavior described different startup contracts even though the daemon itself binds within a named 10-second budget | declare the platform-exact 60 seconds in both deployable TOMLs and assert `startup budget < readiness grace`; config validation must emit no normalization warning |
| Exact image build read `fly.toml` but its dirty-input guard omitted that file | an uncommitted build/platform config could affect the pushed artifact request while the OCI label still named the previous HEAD | include `fly.toml` in unstaged, staged and untracked release-input checks before deriving the revision label |

## Prohibited patterns

- Wrapping `run_once()` or the full M1 test suite in a new arbitrary timeout.
- Treating scheduler cadence, retry backoff, circuit probe time or the 86,400 s
  qualification window as interchangeable clocks.
- Retrying provider calls inside a durable attempt unless the central policy
  explicitly changes from one attempt and the fault matrix is updated.
- Writing stage completion before the external effect or terminal receipt.
- Resetting/deleting an exhausted recovery row to make a new incident run.
- Waiting on a local Fly/SSH process without re-reading the exact remote
  Machine/action state after interruption.
- Calling synchronous `.claim_job()` directly from an `async run_once()`.
- Renewing a repeated blocking sequence only from the single-call timeout branch;
  cumulative lease age must also be checked when each fast call returns.
- Reusing an operator/report query as a platform readiness probe, or comparing
  an outer timeout against a budget that omits mandatory bootstrap work.
- Filtering an exact mutation/recovery target after applying a generic sample
  limit.
- Expressing an absolute deadline as a multiplier of lease, cadence or retry
  settings.
- Calling an unbounded provider/client close after the worker stop grace has
  already completed.
- Building a production runtime image with an ad-hoc `flyctl deploy` command;
  the exact Make entrypoint owns source cleanliness, build-only mode and the
  full OCI revision label.
- Spending a second recovery action merely because an authorized half-open
  attempt ran longer than its original claim window and then received a trusted
  service stop.
- Encoding ad-hoc wall-clock assertions in concurrency test barriers; diagnostic
  watchdogs must reuse the named database envelope and must not be unbounded.
- Creating fire-and-forget mirror tasks from a frame handler without an owner
  that can expose idle state and drain after producers stop.
- Giving a daemon a private shutdown timeout without an explicit, longer
  platform termination window and a tested ordering relationship.
- Encoding startup duration as `loop count * sleep`, or waiting only for a
  signal after readiness without supervising every long-lived resident task.
- Giving a real subprocess less parent time than its cooperative slice; its
  normal envelope must be derived from child work plus the named reap budget.
- Starting or restarting a qualification window before the exact release/config
  has complete eight-node normal-turn, directed-attack, cleanup, postcondition
  and end-to-end lineage evidence.
- Invalidating an epoch for an expected recoverable incident, or counting its
  detection/recovery interval as healthy eligible time.

## Verification map

- Transport generation: `test_gamma_client.py` and
  `test_transactional_structure_source_worker.py`.
- Stage start and current-stage identity: `test_control_plane_runtime_contract.py`
  and Structure source worker tests.
- Episode budgets and schema round trip: `test_035.py`, recovery PostgreSQL
  tests and runtime observe replay tests.
- Scheduling, cancellation and cadence: shared runtime contract, blocking
  bridge, blocking-claim liveness/static audit, service lifecycle and
  transactional scheduler tests.
- Integrated durable effects: fault matrix v3 contains 16 cases, including
  transport replacement, pre-I/O timeout, service interruption and recovery
  episode isolation.
- Climb orchestration: every explicit pytest node selector across all gate
  profiles must pass a repository-wide `--collect-only` contract before a
  renamed test can become a late single-point gate failure.
- Half-open interruption continuity:
  `test_retry_circuit_opens_on_third_failure_with_bounded_probe_delay` proves
  immediate replacement claim, preserved open-circuit identity and unchanged
  episode budget after a probe runs beyond its original window.
