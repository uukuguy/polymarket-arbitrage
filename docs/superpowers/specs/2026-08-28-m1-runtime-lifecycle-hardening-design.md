# M1 Runtime Lifecycle Hardening Design

## Problem statement

M1 currently has several independent clocks that can terminate or classify the
same leased job: the durable runtime profile, module-local worker profiles, the
coordinator turn timeout, role-loop turn timeouts, network/client timeouts, and
recovery-controller deadlines. They are not derived from one contract.

This has produced two repeatable production failures:

- `structure-certify` was durable-deadlined before a production-sized parity
  pass could finish and restarted from range one 98 times.
- after that repair, `quote-admit` was cancelled by the coordinator at 105
  seconds despite owning a 1,200-second durable attempt. Each retry restarted
  from shard one and left the cancelled attempt recorded as `running`.

The defect class is therefore not "a timeout is too small". It is competing
ownership of attempt lifetime plus missing durable resume and cancellation
facts.

## Lifecycle authority

One claimed attempt has exactly one lifecycle authority:

1. PostgreSQL grants ownership with `(job_key, lease_epoch, worker_id)`.
2. The claim persists the exact runtime policy version and deadlines.
3. The attempt runtime renews the lease and enforces progress/absolute
   deadlines.
4. A worker records progress and either commits one fenced terminal transition
   or records a typed retryable/terminal failure.
5. Reclaim atomically closes the expired attempt before opening the next epoch.

The scheduler and role loop own only cadence, fairness, and concurrency. They
must not invent a shorter timeout around an already claimed attempt.

## Single policy registry

Create a closed `RuntimePolicyRegistry` covering exactly the eight job types in
`RUNTIME_STAGE_REGISTRY`. A policy derives from the requested lease and owns:

- heartbeat cadence;
- progress-stall deadline;
- absolute attempt deadline;
- terminal/recovery grace;
- maximum non-terminal I/O duration;
- retry/circuit budget;
- checkpoint cadence;
- immutable policy version.

Unknown job types fail closed. Workers, claim persistence, recovery observation,
CLI wiring, and tests consume this registry; module-local `_runtime_profile`
functions are forbidden.

Required invariants:

```text
3 * heartbeat <= lease
io_timeout < progress_timeout <= attempt_timeout
scheduler_timeout is absent for claimed work
terminal_statement_timeout < remaining_lease
policy job types == runtime-stage job types
```

## Cancellation and effect classes

Cancellation handling follows the effect being executed:

| Effect | Cancellation rule | Recovery rule |
|---|---|---|
| Pure compute/read | client-bounded and cancellable | discard late result |
| Content-addressed R2 write | bounded client call | HEAD and digest verification |
| Fenced terminal PostgreSQL transaction | shield only the transaction | statement timeout plus read-after-ambiguity |

Thread draining is not a timeout. A wrapper that waits indefinitely for an
executor thread may preserve effect ordering, but it cannot claim a wall-clock
bound. Every external client must therefore have connect/read/statement bounds
strictly inside the runtime policy.

`asyncio.CancelledError` must never silently bypass attempt finalization. A
typed cancellation reason (`attempt-deadline`, `progress-stalled`,
`lease-lost`, `service-stop`) is converted to an authoritative runtime event
and job transition unless a fenced terminal commit has already won.

## Durable recovery

Reclaiming an expired lease closes the previous `m1_job_attempts` row as
`retryable`, records `LeaseExpired`, and appends a `job.retryable-failed` fact
before the replacement attempt is inserted. There may be at most one `running`
attempt per job.

Production-sized reducers must not restart from zero:

- Structure certification checkpoints the last verified range and an immutable
  rolling proof tied to the generation manifest.
- Quote admission checkpoints the last consumed shard and immutable partial
  universe chunks, or is split into durable per-shard fan-in jobs.

Checkpoint identity includes input generation and policy version. A new epoch
may resume only when both match; otherwise it fails closed and starts a new
generation-specific job.

Repeated identical failure signatures open the existing job circuit and stop
automatic hot-looping. Recovery becomes explicit and observable.

## Task sequencing and shutdown

The durable DAG remains the source of dependency truth:

```text
source-admit -> fetch -> materialize -> normalize -> structure-certify
             -> quote-admit -> quote-batch -> quote-certify
             -> opportunity-certify
```

Execution is partitioned into bounded job-type lanes. A slow synchronous
certifier runs off the event loop and cannot starve unrelated source admission
or service signal handling. Downstream work may begin only from durable
predecessor commits, never from in-memory scheduler ordering.

Graceful stop stops new claims immediately, lets the current fenced operation
reach a safe boundary, persists a checkpoint/failure fact, and exits within the
policy's shutdown grace. It does not abandon a terminal DB transaction.

## Qualification restart semantics

A new qualification release must not silently replay from ledger offset zero
while appearing to accumulate current evidence. Deployment records a verified
cursor handoff from the predecessor release, or creates a new epoch explicitly
marked as a backfill. Only current-cursor live observations count toward the
86,400-second certificate.

## Verification gates

- static registry and no-duplicate-profile invariant;
- scheduler plus real worker regression proving a valid 110-second operation is
  not cancelled by a 105-second supervisor;
- cancellation at each await and terminal boundary with no late terminal
  effect;
- expired reclaim closes the old attempt and appends the terminal fact in the
  same transaction;
- 1,117-range and 231-shard interruption/resume tests continue after the last
  durable checkpoint;
- DAG acyclicity and declared-successor tests;
- SIGINT during every job type reaches a bounded safe stop;
- qualification release handoff test rejects zero-cursor continuity claims;
- full M1 regression, migration upgrade/downgrade, lint, format, planning, and
  climb gates.

Production remains stopped until the policy/reclaim foundation and
production-size resume path pass. Rollout is image-only with topology and
configuration hashes preserved, followed by live proof through current Quote
and opportunity publication before qualification resumes.
