# M1 Event-Driven Self-Healing and Rolling Qualification Design

**Date:** 2026-08-24

**Status:** Approved interactively; awaiting written-spec review

**Scope:** M1 transactional production runtime, incident response, recovery, and qualification

## Problem

M1 currently treats production acceptance as a manually inspected, immutable
24-hour run. A sampler appends observations, but no continuously running
consumer evaluates those observations against the acceptance rules. The Fly
watchdog checks API availability, exact Machine state, progress, and evidence
freshness, while the external Cloudflare supervisor checks the watchdog. Neither
turns every qualification-breaking business fact into an immediate incident and
recovery decision.

The run `m1-formal-20260823T1335Z` exposed the consequence. All four
`quote-admit` attempts exceeded their 120-second leases, taking between 179 and
207 seconds. Three five-minute samples happened to observe an expired lease.
The attempts later succeeded, so the current API returned to
`expired_leases=0`, but the immutable run could never qualify. No task-local
event, immediate Dashboard incident, Telegram transition, or automatic rolling
qualification reset explained this while it happened. The operator learned the
run had failed only by invoking the verifier the next day.

Adding one heartbeat to `quote-admit` is necessary but insufficient. Similar
timeouts, silent tasks, process exits, and long acceptance restarts have
recurred. A production collector must detect known failures at their source,
recover within bounded authority, retain exact evidence, and keep operating.
Qualification must measure that behavior rather than require a day in which
nothing ever goes wrong.

## Goals

1. Make every task's lifecycle, stage, progress, deadlines, and outcome
   durably knowable.
2. Detect task-known failures immediately; use periodic reconciliation only for
   silence, hangs, crashes, and monitor failure.
3. Atomically connect job state, runtime events, incidents, alert outbox, and
   retry scheduling.
4. Execute only typed, fenced, allowlisted recovery actions with cooldowns and
   budgets.
5. Separate continuous production recovery from rolling qualification.
6. Preserve immutable raw evidence and failed intervals without stopping the
   service or requiring an operator to create a new run.
7. Treat bounded, successful recovery as positive qualification evidence;
   invalidate a window only when correctness, ownership, freshness,
   observability, or recovery SLOs are breached.
8. Make the current state, exact failure, attempted action, recovery outcome,
   and qualification impact visible on Dashboard and Telegram within explicit
   deadlines.

## Non-goals and authority boundary

The autonomous runtime may:

- heartbeat, retry, reclaim, or cancel a job under a current lease fence;
- probe and close a circuit according to policy;
- restart an exact allowlisted worker process;
- restart an exact allowlisted Machine only after job-level recovery and
  independent confirmation fail;
- close a failed qualification epoch and open a new epoch after recovery is
  confirmed.

The autonomous runtime may not deploy code, migrate a database, alter
configuration, rotate credentials, change capacity, change topology, modify or
delete evidence, or perform wallet, signing, order, or trade actions. Those
remain explicit human authority boundaries.

## Chosen architecture

The system uses an event-driven primary path plus periodic and independent
backstops:

```text
Task Runtime Contract
  -> runtime state + append-only event
  -> incident + alert outbox + retry schedule (same transaction)
  -> Recovery Executor
  -> recovery result + incident transition

Deadline Reconciler
  -> detects silent, hung, or abandoned attempts
  -> invokes the same incident and recovery contracts

Independent Watchdog
  -> verifies API, Machines, reconciler, event writer, sampler, and evidence
  -> detects failure of the normal detection/recovery chain

Qualification Engine
  -> evaluates correctness, freshness, evidence, and recovery SLOs
  -> accumulates rolling healthy eligibility
  -> seals immutable qualification certificates
```

The incident ledger is the single transition authority for Dashboard and
Telegram. Neither UI maintains a separate interpretation of runtime state.

## Components

### 1. Task Runtime Contract

Every transactional task follows the same lifecycle:

```text
claimed -> started -> progressing -> succeeded
                              \-> retryable-failed -> retry-scheduled
                              \-> terminal-failed
```

The task publishes meaningful events:

- `job.started`
- `job.stage-changed`
- `job.lease-at-risk`
- `job.progress-stalled`
- `job.retryable-failed`
- `job.retry-scheduled`
- `job.recovery-started`
- `job.recovered`
- `job.terminal-failed`
- `job.succeeded`

Heartbeat updates prove liveness but do not count as progress. Progress is a
monotonic task-specific sequence with optional `current` and `total` counts.
Long jobs expose bounded stages. For Quote admission, those stages include
manifest read, shard read/parse, batch construction, and batch upload. A
200-second opaque call is not an acceptable runtime surface.

When a task knows it has failed, one database transaction:

1. ends the attempt;
2. moves the job to retryable or terminal state;
3. appends the runtime event;
4. creates or updates the stable incident;
5. appends the alert outbox item;
6. persists the next retry time, if any.

If that transaction fails, none of the facts are claimed. The lease and
reconciler become the recovery authority.

### 2. Deadline Reconciler

The reconciler handles failures that a task cannot reliably report: a hung
coroutine, blocked network call, killed process, lost runtime, or abandoned
lease. It runs at most every 30 seconds and uses persisted facts, not process
memory.

It distinguishes:

- liveness: is the owner still heartbeating?
- progress: has the task advanced its monotonic progress proof?
- ownership: is the attempt still the current lease epoch and owner?
- bounded execution: has the total attempt deadline elapsed?
- business outcome: is the last certified publication still fresh?

The reconciler can run in multiple replicas. A database controller lease gives
each owner a monotonically increasing epoch. Every decision and action carries
that epoch; stale controllers cannot mutate current work.

### 3. Incident and Alert Ledger

The existing `m1_incidents`, `m1_incident_events`, and transactional alert
outbox remain the single incident authority. Runtime transitions follow:

```text
detected -> recovery-started -> recovery-attempted -> recovered
                                                \-> escalated
```

An incident detail contains only bounded, non-secret facts:

- component and failure signature;
- job key, attempt ID, worker ID, and lease epoch;
- stage and progress;
- last heartbeat and last progress time;
- relevant deadlines;
- affected data product and freshness;
- chosen recovery policy and next decision time;
- qualification impact.

DSNs, tokens, headers, arbitrary response bodies, and unbounded exception text
are prohibited.

### 4. Recovery Executor

The executor accepts typed commands only:

- `heartbeat-job`
- `cancel-job`
- `retry-job`
- `reclaim-job`
- `probe-circuit`
- `restart-worker-process`
- `restart-machine`

Each command names an incident, target, expected controller epoch, expected
attempt, expected lease epoch, cooldown, and recovery budget. It records an
immutable action outcome. If any precondition no longer matches, the command
returns `stale-noop` and changes nothing.

Machine restart is the final automatic tier. It requires all of the following:

- job-level retry or reclaim did not restore progress;
- the exact worker heartbeat is absent;
- the independent watchdog confirms the target is unhealthy;
- no other active restart action exists for the target;
- the target is in the configured allowlist;
- hourly and daily restart budgets remain available.

### 5. Independent Watchdog and Supervisor

The Fly watchdog remains independent of Postgres credentials and checks the
control API, exact required roles, reconciler, event writer, evidence sampler,
and current qualification evidence. The Cloudflare supervisor continues to
check the Fly watchdog itself.

These monitors are backstops, not the primary detector for task-known failure.
Their role is to expose a failed runtime contract, reconciler, database/API,
Machine, or monitoring chain.

### 6. Qualification Engine

Qualification continuously evaluates the same durable facts but never owns
business recovery. Its state machine is:

```text
ACCUMULATING -> QUALIFIED
      |
      v
INVALIDATED -> RECOVERING -> ACCUMULATING
```

An invalidated epoch remains immutable. Recovery confirmation automatically
opens a new epoch; no operator-created run ID is required. Once a complete
rolling interval satisfies every rule, the engine seals a qualification
certificate over exact evidence bounds and digests.

## Data model

### `m1_job_runtime_state`

One current, fast-read row per active attempt:

- `job_key`
- `attempt_id`
- `lease_epoch`
- `worker_id`
- `stage`
- `started_at`
- `last_heartbeat_at`
- `last_progress_at`
- `progress_sequence`
- `progress_current`
- `progress_total`
- `lease_deadline_at`
- `heartbeat_deadline_at`
- `progress_deadline_at`
- `attempt_deadline_at`
- `recovery_state`
- `updated_at`

Updates require the current `(job_key, attempt_id, lease_epoch, worker_id)`.

### `m1_job_runtime_events`

Append-only lifecycle evidence:

- `event_id`
- `job_key`
- `attempt_id`
- `lease_epoch`
- `event_sequence`
- `kind`
- `stage`
- bounded progress and detail
- `occurred_at`
- `idempotency_key`

The unique keys are `(attempt_id, event_sequence)` and `idempotency_key`.
Heartbeat ticks update current state; only meaningful transitions enter this
table.

### `m1_recovery_actions`

Auditable recovery commands and outcomes:

- `action_id`
- `incident_key`
- `target_type`
- `target_id`
- `action_type`
- `expected_controller_epoch`
- `expected_attempt_id`
- `expected_lease_epoch`
- `requested_at`
- `started_at`
- `finished_at`
- `state`
- `result_code`
- `next_allowed_at`
- bounded detail and idempotency key

Only one active action of a given conflicting class may exist per target.

### `m1_runtime_controller_leases`

A fenced singleton lease for reconciliation and action scheduling. Multiple
replicas may observe, but only the current epoch schedules recovery.

### `m1_qualification_epochs`

- generated epoch ID;
- exact start and end bounds;
- state;
- release/config/role identity;
- recovery confirmation bound;
- invalidation time and reason, if any;
- raw observation and incident bounds;
- created and updated timestamps.

### `m1_qualification_certificates`

Immutable certificate containing:

- exact 24-hour interval;
- release/config/role identity;
- observation, incident, action, and data-product evidence digests;
- sample coverage and maximum gap;
- correctness and freshness SLO results;
- all contained incidents and bounded recoveries;
- successful progress counters;
- signature/version of the qualification policy;
- canonical certificate digest.

Update and delete are rejected. Re-verification is pure and deterministic.

## Deadline semantics

One timeout cannot represent every failure boundary.

1. **Lease deadline** fences ownership and result publication.
2. **Heartbeat deadline** proves that the executor remains alive.
3. **Progress deadline** bounds time without business progress.
4. **Attempt deadline** bounds total attempt duration even with progress.
5. **Business freshness deadline** bounds the age of certified output.

Each job type has a versioned deadline profile. The initial common rules are:

- heartbeat interval is no greater than one third of the lease duration and no
  greater than 30 seconds;
- two missed heartbeats make the attempt suspect;
- three missed heartbeats or lease expiry create an incident;
- heartbeat may renew ownership only while attempt and lease fences match;
- heartbeat never advances `last_progress_at`;
- a progress deadline initiates cooperative cancellation and retry even when
  heartbeat remains healthy;
- every external request has its own timeout shorter than the remaining
  attempt deadline;
- recovery cannot extend an attempt beyond its hard deadline.

The concrete per-job durations are configuration contracts with unit and
production-shaped tests. They are selected from bounded stage behavior and
measured production percentiles, not by increasing a global timeout until a
soak happens to pass.

## Detection, alert, and recovery SLOs

| Failure source | Durable detection | Dashboard | Telegram |
| --- | ---: | ---: | ---: |
| Task-known failure | same transaction | 5 seconds | 10 seconds |
| Heartbeat/progress deadline | 30 seconds | 35 seconds | 40 seconds |
| Process/Machine loss | 60 seconds | 65 seconds | 60 seconds |
| Recovery | same recovery transaction | 5 seconds | 10 seconds |
| Event writer/database loss | external supervisor | explicit unavailable | 60 seconds |

Telegram sends only state transitions: `DETECTED`, `RECOVERY STARTED`,
`RECOVERED`, and `ESCALATED`. An unresolved incident receives one reminder
after 15 minutes and then hourly reminders. Idempotency prevents duplicate
delivery.

## Recovery policy

### Retryable exception

Atomically record failure and schedule jittered backoff at approximately 5,
15, 30, and 60 seconds. A versioned job policy caps attempts. Exhaustion opens
a circuit and escalates.

### Lease at risk with current progress

Perform a fenced heartbeat and record `lease-at-risk`. This is not
qualification-breaking if the lease never expires, progress remains within its
deadline, and business freshness is preserved.

### Heartbeat without progress

Mark the attempt stuck, request cooperative cancellation, end the attempt, and
retry under a new lease. Heartbeat cannot keep a stalled attempt alive forever.

### Missing heartbeat

Prevent the old owner from publishing, wait for the fence to expire, and
reclaim. Repeated silent attempts permit a process restart only within budget.

### Open circuit

After cooldown, admit exactly one probe. Success closes the incident; failure
extends backoff. The entire queued workload is never released at once.

### Process or Machine failure

Use exact allowlisted identities and independent confirmation. Exceeding the
restart budget escalates to P1 and blocks further automatic restart.

### Integrity, authentication, schema, credential, or capacity failure

Isolate and alert. Do not automatically change deployment, data, credentials,
schema, topology, or capacity.

## Qualification semantics

Qualification proves correctness, freshness, observability, and bounded
recovery. It does not require zero incidents.

### Contained events that do not invalidate an epoch

- a retryable upstream error recovered within policy;
- a retry or reclaim with no concurrent fenced commit;
- a process replacement that preserves data-product SLOs;
- a circuit probe that restores service within its recovery SLO;
- any recovery whose action, target, budget, and postcondition are proven.

These events are included in the certificate as self-healing evidence.

### Events that invalidate an epoch

- an expired lease that creates an ownership or concurrent-commit risk;
- receipt, digest, pointer, or data-integrity conflict;
- Structure, Quote, opportunity, or required control evidence exceeding its
  freshness SLO;
- an observation, incident-ledger, watchdog, or supervisor gap beyond policy;
- an unresolved P1 beyond its recovery SLO;
- a repeated failure signature exceeding the stability budget;
- recovery requiring deployment, migration, credential, configuration,
  topology, or capacity intervention;
- a stale or mis-targeted automatic recovery action that mutates state;
- loss of monotonic successful work or required correctness evidence.

Machine IDs are diagnostic instances, not eternal qualification identities.
The certificate binds required roles plus release and config identities. A
fenced replacement that preserves correctness and SLOs is successful recovery,
not an automatic failure.

## Dashboard contract

The Dashboard presents four simultaneous views.

### Runtime overview

- `healthy`, `degraded`, `recovering`, or `critical`;
- correctness and freshness for Structure, Quote, and opportunity outputs;
- worker, reconciler, watchdog, supervisor, event writer, and sampler health;
- open incidents and remaining recovery budgets.

### Task visibility

Every active task shows job, attempt, owner, lease epoch, stage, progress,
heartbeat age, progress age, deadlines, retry/circuit status, and latest
recovery action.

### Incident timeline

The timeline answers what failed, who detected it, what was affected, what the
system tried, whether it recovered, whether qualification was invalidated, and
what human action remains.

### Qualification

Show current epoch start, accumulated eligible duration, SLO budget, contained
recoveries, last breaker, and immutable certificate history. Any unreadable
source renders `unavailable`; it never becomes empty or green.

## Verification strategy

The 24-hour certificate is sustained production evidence, not the development
feedback loop.

### Fast deterministic gates

1. State-machine tests cover every legal and illegal event transition.
2. Real-Postgres tests prove atomic job/event/incident/outbox/retry commits.
3. Fencing tests prove stale controller, lease, attempt, and duplicate actions
   become `stale-noop`.
4. Virtual-time tests simulate days of heartbeat, progress, failure, recovery,
   invalidation, and certification in minutes.
5. Historical replay feeds `m1-formal-20260823T1335Z` to the new Rule Engine.
   It must identify the first expired lease immediately and produce the exact
   incident and qualification decision.
6. A fault matrix covers task exception, R2 timeout/hang, heartbeat loss,
   progress stall, stale owner, circuit probe, process exit, Machine restart,
   database/event-writer failure, watchdog failure, and duplicate delivery.

Every fault verifies detection latency, durable events, alert transitions,
action fencing, recovery outcome, Dashboard state, and qualification impact.

### Production enablement gates

1. **Replay/local:** all historical replay, Postgres, virtual-time, and fault
   matrix gates pass.
2. **Observe-only:** production policy evaluates but schedules no actions; its
   decisions match bounded manual read-only evidence.
3. **Job recovery:** enable heartbeat, retry, reclaim, and circuit probe, then
   prove one controlled job-level fault end to end.
4. **Process recovery:** enable allowlisted process/Machine restart only after
   independent confirmation and prove restart budgets and stale-action fences.

These gates provide fast implementation feedback. Once enabled, rolling
qualification accumulates automatically and seals a certificate after a full
eligible interval; no operator babysitting or manually restarted run is
required.

## Makefile and operator surface

All executable commands have documented Makefile targets and appear in
`make help`:

- `make runtime-policy-replay`
- `make runtime-reconcile-once`
- `make runtime-controller-status`
- `make runtime-fault-matrix`
- `make qualification-status`
- `make qualification-certificates`

Mutation-capable targets require explicit enablement and exact target
identities. Read-only status and replay targets never fall back to live cloud
mutation.

## Rollout and compatibility

- Existing `m1_soak_runs` and `m1_soak_observations` remain immutable evidence
  and are available to the replay gate.
- The old manual verifier remains a historical evidence reader but is no
  longer the live failure detector or qualification coordinator.
- Existing incidents and alert delivery tables are reused rather than creating
  a second notification truth.
- Runtime tables and qualification tables are additive migrations. Destructive
  cleanup is outside this scope.
- Controller policy starts observe-only. Automatic action classes are enabled
  independently after their gates pass.
- The current failed run is never relabeled. Its three observed expired leases
  become a permanent regression fixture for the new design.

## Documentation and learning

Implementation adds a `docs/learning/` chapter covering:

- event-driven failure detection versus watchdog reconciliation;
- lease, heartbeat, progress, attempt, and freshness deadlines;
- incident and recovery state machines;
- fencing and `stale-noop` behavior;
- rolling qualification and certificate interpretation;
- adversarial operator questions and FAQ increments.

## Implementation decomposition

This is one architectural contract delivered through ordered, independently
verifiable work packages rather than one large code change:

1. **Runtime evidence foundation:** additive schema, typed event/deadline
   contracts, policy versioning, historical replay, and read-only status.
2. **Task-local instrumentation:** instrument every production job type with
   lifecycle, stage, heartbeat, progress, deadline, and atomic failure facts.
   `quote-admit` is the first production regression proof, not the terminal
   scope.
3. **Reconciliation and recovery:** controller fencing, silent-attempt
   detection, typed actions, budgets, and job-level fault matrix.
4. **Rolling qualification:** epoch state machine, breaking/contained policy,
   virtual-time verification, and immutable certificates.
5. **Operator surfaces:** Dashboard task/incident/qualification views,
   Telegram transitions, and unavailable-state behavior.
6. **Production enablement:** observe-only, job recovery, process recovery,
   controlled faults, and automatic rolling accumulation.

Each package has its own tests, Makefile entry points, plan SUMMARY, and
rollback or disable boundary. No package may claim the overall acceptance
criteria before all dependent packages and production gates pass.

## Acceptance criteria

The design is delivered only when:

1. Every production job type implements the runtime lifecycle and versioned
   deadline profile.
2. Task-known failures atomically produce retry, incident, and alert facts.
3. Silent attempts are detected and recovered within the declared SLO.
4. Stale or duplicate recovery commands cannot mutate current work.
5. Dashboard and Telegram meet their transition deadlines and share one
   incident truth.
6. Historical replay identifies the current failed run at its first invalid
   observation.
7. The full deterministic fault matrix passes without a 24-hour wait.
8. Observe-only, job-recovery, and process-recovery production gates pass.
9. Production continues through contained faults without operator-created run
   IDs.
10. A later complete rolling interval produces a reproducible immutable
    qualification certificate containing correctness, freshness, incident,
    recovery, and evidence digests.
