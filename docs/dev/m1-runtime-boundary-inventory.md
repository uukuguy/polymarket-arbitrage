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
| Circuit probe | `m1_job_circuits.next_probe_at` | no recovery-budget-derived clock | circuit row and action ledger | revision 034 and circuit-clock tests |
| Recovery action count | revision 035 episode row | no reset on controller reclaim; new episode gets a separate row | `(controller,target,episode)` budget | revision 035 and episode fault case |
| Scheduler cadence | caller `interval_seconds` | never cancels an active lane; recomputes remaining time after observer work | completed turn callback | scheduler service tests |
| Terminal shutdown | `service_lifecycle.terminal_grace_seconds()` | first cancel requests drain; second cancel detaches; fence owns late result | `ServiceStopRequested` or terminal receipt | service lifecycle and interruption tests |
| Operator observation | watchdog/Fly read policy constants | daemon reader is abandoned after the derived observation round | watchdog transition, never a job mutation | CLI/watchdog tests |
| Qualification window | immutable qualification policy | service can restart and resume cursors; no outer process timeout | normalized ingress and epoch/certificate | qualification service and 86,400-second certificate |

## Provider and external-I/O inventory

| Surface | Inner bound | Outer bound | Attempts | Notes |
|---|---:|---:|---:|---|
| Gamma keyset/exact-ID HTTP | `provider_timeout_seconds` in `GammaClient` | Structure `io_timeout_seconds` | 1 | A failed durable attempt replaces the HTTP transport generation but preserves the service limiter. Cleanup uses `GAMMA_CANCELLED_CLOSE_TIMEOUT_S`; it cannot become another attempt deadline. |
| Structure R2 PUT+HEAD | botocore connect/read split derived by `control_plane_r2_config()` | `run_blocking_call_with_timeout(io_timeout_seconds)` | 1 | Timed-out daemon thread cannot publish a fenced receipt. |
| Quote CLOB books | client request inside `_await_reader()` | Quote `io_timeout_seconds` | 1 | cancellation drains the owned request task; no scheduler wrapper timeout. |
| Quote/Structure R2 reads and writes | botocore request bounds | remaining absolute attempt budget for synchronous bridge | 1 | periodic waits only drive lease heartbeat; they do not extend the absolute deadline. |
| Telegram outbox delivery | `ALERT_DELIVERY_POLICY.provider_timeout_seconds` | stop grace below alert lease | 1 | failure is a durable retryable outbox delivery. |
| Runtime watchdog control API / Fly API | 10 s individual read | derived 21 s parallel observation round | 1 | read-only, bounded target sets; a missed round becomes an observation failure, never a Machine mutation. |
| Runtime watchdog page / private event writer | 5 s request | blocking bridge obeys service cancellation | 1 | watchdog remains independent from the transactional DB path. |
| Local Fly CLI soak read | 30 s operator read | command process timeout | 1 | operator evidence only. Long-running deployment/certificate commands do not receive this bound. |

The legacy snapshot-only R2 helper retains its historical standard retry
policy. Formal runtime-v2 paths must use `control_plane_r2_config()`, whose
`total_max_attempts=1` prevents hidden retries inside the durable attempt.

## Database inventory

| Policy | Connect | Statement | Lock | Consumers |
|---|---:|---:|---:|---|
| `CONTROL_PLANE_DB_POLICY` | 5 s | 5 s | 1 s | worker transactions, API reads, qualification facts |
| `RECOVERY_DB_POLICY` | 5 s | 2 s | 1 s | controller lease, observe ledger, action scheduling/execution |
| `MIGRATION_DB_POLICY` | 10 s | 30 s | 1 s | Alembic/preflight scans only |

Server-side statement/lock limits are the mutation authority. HTTP or thread
request envelopes may contain connection plus statement transfer time, but
must not retry the transaction or claim a second mutation deadline.

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
  failure cannot cancel siblings.
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
  circuit defect streak.
- Failed source attempts retire their Gamma transport generation before the
  next attempt. They keep the shared limiter, so replacement cannot evade rate
  policy.
- Job recovery episode identity is the exact attempt ID. Circuit episode
  identity is the exact failure fingerprint. Legacy target-only exhaustion
  stays under `legacy` and is never refilled or deleted.
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
| Concurrent final receipts could both decline a wakeup | every producer was terminal while its certifier remained waiting forever | terminal transition precedes one wake; sibling checks serialize on the certifier row; bounded claim-time repair closes historical gaps |
| Receipt count stood in for producer success | a checkpointed producer could make an incomplete generation claimable | eligibility joins each receipt to a `succeeded` producer job |
| Structure source pool hid its lanes' lease policy | SIGTERM drain could not resolve terminal grace and exited the coordinator with `ValueError` | validate one common lane lease and expose it on the pool, matching the Quote pool contract |
| Fast sub-calls never reached heartbeat polling | a long Opportunity/Structure sequence of healthy sub-30-second calls silently expired its cumulative lease and surfaced `stale-lease` | check the runtime's due heartbeat after every successful nonterminal call boundary as well as while one call blocks |
| Local SSH waiter outlived an already completed remote Machine | operator process appeared hung despite completed remote work | exact waiter can be interrupted safely; remote durable state is re-read before retry |

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
