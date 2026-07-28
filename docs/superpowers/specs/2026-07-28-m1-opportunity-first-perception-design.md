# M1 Opportunity-First Market Perception Design

**Date:** 2026-07-28  
**Status:** Approved in discussion; pending written-spec review  
**Scope:** Observer-only M1 market perception and arbitrage-opportunity watching  
**Safety boundary:** No wallet, signing, balance, order placement, or real-money execution

## 1. Decision

M1 optimizes for the discovery and continuous tracking of actionable neg-risk
arbitrage opportunities under a finite resource budget. It does not promise that
every newly created Polymarket group is discovered without delay or omission.

A complete market crawl currently takes five-to-ten minutes or more and has a
variable duration. It therefore represents an observation window, not an
instantaneous world state. Completion of that crawl must not be a global
prerequisite for serving independently certified groups.

M1 will use three cooperating loops:

1. **Candidate Watcher** — high-frequency Structure and Quote verification for
   promoted neg-risk groups.
2. **Discovery** — bounded rolling exploration that finds and ranks groups.
3. **Full Reconciliation** — low-priority, checkpointed universe calibration.

Correctness is atomic at group level. Universe coverage is statistical,
time-windowed evidence.

## 2. Goals and Non-Goals

### Goals

- Maximize useful opportunity capture while keeping already discovered
  candidates fresh.
- Publish a group only when all of its Structure members and Quote legs share a
  verifiable identity.
- Make exploration coverage, staleness, resource allocation, incidents, and
  recovery actions measurable.
- Keep HTTP, history, and Dashboard reads responsive while background work runs.
- Continue operating through upstream failures, process failures, restarts, and
  partial degradation.

### Non-goals

- Zero-miss discovery of every new Polymarket market or group.
- Treating a ten-minute crawl as a simultaneous snapshot.
- Blocking all opportunity reads because an unrelated group or full crawl is
  incomplete.
- Combining observation with any trading authority.
- Migrating to a distributed database before production measurements require it.

## 3. Core Model: Exploration and Exploitation

M1 divides its resource budget between:

- **Exploitation:** refresh groups that are already candidates, have shown edge,
  have material capacity, or are changing rapidly.
- **Exploration:** revisit the broader known universe and discover new groups.

An initial operating point may devote roughly 70% of useful collection capacity
to candidates and 30% to exploration. This is not a permanent configuration.
The controller changes the effective allocation from observed Quote age, queue
lag, candidate count, collection duration, and failure rate.

At least 20% of the exploration budget is reserved for age-based anti-starvation.
No region of the known universe may be permanently excluded merely because it
has historically produced no edge.

Candidate scheduling enforces that reservation in both count and time. At
least `ceil(20% * cycle_max_groups)` slots are reserved for normal/explore
work. Each cycle executes one configured high burst first, then the reserved
lanes, then the remaining selected work. The configured worst-case
`high_burst_groups * group_timeout_s` must remain strictly below the explicit
normal-candidate maximum wait, which may be tightened but never raised above
the 120-second production acceptance boundary. The high burst cannot exceed
the bounded high CLOB worker capacity, so queued high calls cannot consume the
lower-lane wait budget.

Discovery certification does not imply immediate Candidate admission. The
outstanding factless promotion capacity is derived from the stricter bound:
`poll + bounded_selection + attempt_start_sqlite +
(high_burst + capacity - 1) * (group_timeout + terminal_write) <= 60s`.
Every duration is conservatively rounded up to milliseconds, and capacity
cannot exceed the reserved lower-lane slots. Source enumeration runs on one
isolated bounded executor and facts/schedules are read in one bulk SQLite
snapshot. Excess certified groups remain durably queued; groups with prior
Candidate facts remain actual candidates even when unpromoted. Before an
admitted watcher call, an immutable start receipt proves the deadline or
records an unavailable breach. Process-level kill isolation remains Task 5.

## 4. Group-Level Data Contract

The unit of online certification is one neg-risk group.

Each group revision records:

| Field | Meaning |
|---|---|
| `group_id` / `event_id` | Durable group identity |
| `membership_hash` | Hash of the complete ordered market/outcome/token set |
| `structure_revision` | Monotonic revision for this group |
| `structure_started_at_ms` | Beginning of the Structure observation window |
| `structure_observed_at_ms` | Time the complete membership was certified |
| `source_cursor` | Gamma page/cursor evidence that produced the revision |
| `status` | `discovered`, `certified`, `stale`, `invalidated`, or `closed` |

Each complete Quote observation records:

| Field | Meaning |
|---|---|
| `group_id` | Referenced group |
| `membership_hash` | Exact Structure identity used for collection |
| `quote_batch_id` | Atomic all-leg Quote batch |
| `quote_started_at_ms` / `quoted_at_ms` | Quote observation window |
| `legs` | Complete expected legs, including token, ask, size, and terminal state |

An opportunity is current only when:

1. the group has a complete certified membership;
2. every expected leg has a valid token identity;
3. every Quote leg belongs to one `membership_hash` and one `quote_batch_id`;
4. all legs satisfy the configured within-batch skew bound;
5. Structure and Quote ages satisfy their separate freshness gates; and
6. bundle cost, gross edge, and capacity are computed from that same batch.

The lifecycle is:

```text
discovered
  -> structure-certified
  -> quote-complete
  -> watching
  -> stale | invalidated | closed
```

A membership change immediately invalidates prior Quote evidence. A missing or
invalid leg makes only that group unavailable. Old and new legs are never
combined.

## 5. Candidate Watcher

The Candidate Watcher is the highest-priority data path.

It:

- consumes promoted group identities;
- revalidates current group membership;
- fetches only the complete set of tokens for that group;
- writes one atomic Quote batch;
- computes observer-only edge and capacity;
- records state transitions and notification intents; and
- changes the next refresh time from observed opportunity value and volatility.

Priority considers:

- current and historical gross edge;
- executable bundle capacity;
- recent membership or terminal-state change;
- Quote and Structure age;
- market activity/liquidity;
- distance to resolution; and
- time since last visit.

High-priority candidates receive the shortest interval. Normal candidates may be
slower, but their status becomes stale/unavailable before the freshness gate is
crossed silently.

## 6. Discovery

Discovery does bounded work per cycle. It never starts an uninterruptible
universe-sized transaction.

Each cycle:

1. reads one bounded Gamma page or cursor range;
2. normalizes events and neg-risk membership;
3. creates or refreshes group scheduling records;
4. promotes promising or changed groups to Candidate Watcher;
5. updates rolling coverage evidence; and
6. persists its next cursor/checkpoint.

Discovery priority combines activity, liquidity, recent updates, proximity to
resolution, known edge, staleness, and an age-based anti-starvation term.

Gamma update timestamps may improve priority, but they are not accepted as a
complete lossless change stream. Cursor advancement and periodic reconciliation
remain necessary.

Coverage is reported over 15-, 30-, and 60-minute windows as:

- groups visited;
- activity/liquidity-weighted universe fraction;
- queue depth by priority;
- oldest unvisited group age; and
- skipped/failed work by bounded reason.

## 7. Full Reconciliation

Full Reconciliation is a calibration job, not the online publication gate.

It:

- scans the known upstream universe into a staging reconciliation window;
- persists a checkpoint after every bounded batch;
- resumes from the last durable checkpoint after restart;
- records the window start/end rather than claiming a single `as_of` instant;
- compares staged groups with online group revisions;
- applies additions, changes, closures, and invalidations group by group; and
- publishes a coverage/difference report when the window closes.

An incomplete reconciliation never replaces online certified groups wholesale.
The last good group revision remains readable with its real age.

The deployed adaptive attempt-duration controller remains useful for measuring
and bounding reconciliation batches/windows. It is no longer a global
opportunity-feed readiness gate.

## 8. Runtime and Resource Isolation

The initial production topology keeps one Fly volume and SQLite/WAL, while
separating CPU/GIL-heavy work from the HTTP process:

```text
HTTP / read-model process
        |
        +-- Candidate Watcher subprocess       highest data priority
        +-- Discovery subprocess               bounded normal/low priority
        +-- Full Reconciliation subprocess     low priority + checkpoint
```

The initial machine target is at least two vCPUs:

- one capacity lane protects HTTP and the hot candidate path;
- one lane permits bounded discovery or reconciliation progress.

Only one expensive background batch runs at a time. The resource controller
sheds work in this order:

1. pause Full Reconciliation;
2. reduce Discovery batch size/duty cycle;
3. slow normal candidates;
4. preserve high-priority candidates and HTTP for as long as possible.

When Candidate Quote p95 age approaches its SLA, background work yields.
When candidates are few or empty, exploration expands. When production
measurements prove that a two-vCPU/SQLite deployment cannot satisfy the gates,
the workers may move to separate Fly applications and a shared PostgreSQL
control/read model. That migration is explicitly deferred until measured.

## 9. Failure Handling and Continuous Operation

Production stability means timely detection, containment, recovery, verification,
and escalation. It does not mean an absence of failures.

Every incident follows a durable lifecycle:

```text
detected -> classified -> contained -> recovering -> verified | escalated
```

The incident record includes:

- component and bounded impact scope;
- first/last observed time;
- triggering evidence and classification;
- automatic actions and parameter adjustments;
- retry count;
- recovery proof; and
- closure or escalation reason.

### Isolation Rules

- One group failure affects only that group.
- Candidate Watcher failure degrades current opportunity output but not HTTP,
  Discovery, or history reads.
- Discovery failure reduces new-group discovery but not known-candidate tracking.
- Reconciliation failure retains checkpoints and old certified groups.
- Telegram failure retains a durable outbox and never changes market facts.
- Storage failure stops unsafe writes and makes the affected write path fail
  visibly while preserving safe reads where possible.

### Automatic Actions

- Transient upstream failures use bounded exponential backoff with jitter.
- Sustained source/group failures open a scoped circuit breaker.
- A stuck child is terminated at its measured deadline and leaves a terminal
  attempt record.
- CPU/memory pressure sheds low-priority work before the candidate path.
- Membership change invalidates old Quote evidence immediately.
- A failed worker is restarted under supervision and restores durable
  queue/checkpoint/outbox state.
- Repeated unsuccessful recovery transitions to `escalated`; it does not create
  an infinite restart or notification storm.

Recovery is proved from fresh writer-side mutations: advancing queues, valid
group revisions, Quote freshness, responsive HTTP, and a post-recovery successful
attempt. Elapsed time alone cannot close an incident.

Initial hot-path targets are:

- MTTD at most 30 seconds;
- containment at most 60 seconds; and
- recovery targets defined per failure class rather than one misleading global
  MTTR.

## 10. Observability and Dashboard

The Dashboard and APIs expose:

- counts of `watching`, `stale`, `unavailable`, `invalidated`, and `closed`
  groups;
- current opportunities with edge, capacity, Structure age, and Quote age;
- group Structure/Quote transition history;
- Discovery queue depth, priority, cursor, and rolling weighted coverage;
- Reconciliation window progress, checkpoints, duration distribution, and
  differences;
- effective resource allocation and adjustment history;
- open incidents, lifecycle stage, automated actions, retries, and recovery
  evidence; and
- notification outbox/delivery status.

The read model distinguishes:

- a valid zero-current-opportunity result;
- no promoted candidates;
- stale/unavailable candidate evidence;
- a stopped or failed producer; and
- an unreachable API.

## 11. Production Acceptance

M1 may be called production-usable only when the following are proven.

### Hot Path

- API p95 latency remains at most two seconds during background work.
- High-priority candidate Quote age p95 is at most 30 seconds and never silently
  exceeds 90 seconds.
- Normal candidate Quote age is at most 120 seconds or explicitly stale.
- The Candidate scheduler exposes this 120-second normal-candidate boundary as
  configuration, reserves at least 20% of every cycle for normal/explore, and
  proves the pre-lower high timeout budget is strictly smaller.
- Every watching opportunity passes the group identity and all-leg batch gates.
- A valid empty candidate/opportunity result is distinguishable from failure.

### Exploration and Calibration

- At least 90% of the liquidity-weighted active known neg-risk universe is
  visited within a 15-minute rolling window.
- Every known neg-risk group is revisited within six hours.
- A group promoted by Discovery enters Candidate Watcher within 60 seconds.
- A Full Reconciliation window closes within 24 hours or remains visibly open
  with an advancing checkpoint and bounded incident.

These are initial production gates and must be recalibrated from observed
capacity without weakening fail-closed semantics.

### Resilience

Controlled tests cover:

- Gamma timeout, partial pagination, malformed response, and cursor failure;
- CLOB missing legs, 429, partial response, and long latency;
- Candidate, Discovery, and Reconciliation process exit/stall;
- SQLite busy and disk-pressure paths;
- Telegram failure;
- daemon restart and interrupted deployment; and
- background/hot-path resource contention.

Each test verifies the full chain:

```text
detect -> contain -> act -> expose -> prove recovery -> close/escalate
```

A continuous soak then verifies that, after incidents and recovery, queues,
checkpoints, group state, Quote freshness, and HTTP keep advancing. Soak duration
is supporting evidence, not the definition of stability.

## 12. Consequences for the Current System

- The current universe-sized Structure snapshot becomes reconciliation input,
  not the opportunity feed's single global gate.
- The current all-known-token Quote collection is replaced on the hot path by
  complete per-group batches.
- Existing Structure attempt history and adaptive timing remain valuable
  operational evidence for background calibration.
- Existing watcher ledger/outbox concepts remain useful, but opportunity identity
  moves from a global snapshot dependency to group revision plus membership hash.
- The previously considered design in which Structure globally preempts Quote is
  rejected: background completeness must not starve the opportunity-first path.
