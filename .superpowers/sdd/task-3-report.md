# Task 3 Implementer Report

Status: DONE

## Scope

Task 3 only: bounded Discovery, durable group identity, certified scheduling,
capacity-proven Candidate admission, actual-candidate freshness, bounded bulk
selection, durable start/deadline evidence, complete receipt-chain validation,
and default-off wiring. No Task 4, deployment, public Dashboard/API, process
isolation, or trading.

## Commit Chain

- `046cef1` — initial bounded Discovery.
- `83515f9` — authority/freshness/status hardening.
- `436c389` — continuity/receipt/load-control hardening.
- `bcc30db` — first-sight identity and capacity admission.
- Current commit — final re-review remediation below.

## Final Re-review RED → GREEN

### 1. Actual Candidate authority

The durable Candidate set is now every current certified group that either has
a Candidate fact or is capacity-admitted. A watched group is not dropped merely
because its Discovery schedule is unpromoted. Never-watched excess queue rows
remain excluded, and an unavailable fact cannot refresh matching Quote age.
The composed source uses this same authority, so freshness and execution cannot
disagree.

### 2. Complete ≤60-second start proof

Settings derive factless admission capacity from conservative ceil-ms budgets:

```text
poll
+ bounded source/bulk selection
+ attempt-start SQLite busy/write
+ (high_burst + capacity - 1)
   * (group_timeout + timeout-terminal-write budget)
<= 60 seconds
```

The default proof is capacity 1 with a 47-second worst-case start bound. Source
enumeration and the single bulk facts/schedules read run on a dedicated
one-thread executor with a six-second controller budget and a 500-ID hard cap.
No per-group SQLite reads remain in selection.

Immediately before an admitted watcher call, one `BEGIN IMMEDIATE` transaction
records an immutable attempt-start timestamp against the persisted deadline.
A delayed/restarted scheduler does not call the watcher: it records
`candidate-start-deadline-breached` as an unavailable Candidate fact, frees the
factless slot, atomically admits the next queue row, and leaves status non-ready.
The normal path proves start receipt ≤ deadline. Existing cancellation-safe
terminal writes remain intact.

The mathematical boundary covers source selection, SQLite busy/write, group
timeout, and timeout-terminal-write budgets. Killing a stuck process or sync SDK
thread is explicitly deferred to Task 5 process isolation.

### 3. Complete historical receipt proof

Status reads one SQLite snapshot and requires the first receipt to be sweep 1,
sequence 1. Every historical receipt must have nonnegative ordered timestamps,
`promoted_count <= groups_seen <= page_event_count`, exact sample and promotion
counts, valid cursor continuity, and exact sweep/sequence transitions. Orphan
samples, corrupt old receipts, and a forged first sweep fail safely with CLI
exit 2 and no path/traceback leakage.

Attempt-start receipts are also checked so `deadline_breached` is true exactly
when `started_at > persisted_deadline`.

### 4. Policy-free migration, explicit active configuration

Generic `init_schema()` now performs only additive/idempotent column/table work
and historical receipt sequence derivation. It does not seed capacity, fill
policy deadlines, or demote rows.

Active Discovery/Candidate wiring explicitly calls
`configure_discovery_admission(proof)`. That transaction persists the real
Settings proof, fills queue/deadline evidence, retains every fact-backed
Candidate, deterministically keeps the top configured factless promotions,
demotes only excess factless legacy rows, and admits queue capacity. Repeated
configuration is idempotent. Timing-policy changes are rejected while factless
work is outstanding; capacity-only changes reconcile safely.

## TDD Evidence

Observed RED:

- watched current-certified/unpromoted group absent from source and freshness;
- 200 IDs caused sequential per-group reads and a slow source had no bound;
- late restart called the watcher without durable deadline evidence;
- old receipt count/time corruption passed status;
- generic schema initialization seeded capacity 1 and demoted before Settings.

Focused final-review suite:

```text
105 passed
```

Task 3 + Task 1/2 + Gamma/routing/daemon proportional suite:

```text
289 passed
```

Additional gates:

- changed-file Ruff: pass;
- `git diff --check`: pass;
- `make docs-m1-check`: pass;
- `make planning-status`: no drift;
- active configured `make perception-discovery-status` fixture: exit 0.

## Remaining Boundaries

Default effective capacity is intentionally conservative at one. Deadline
breach readiness is local Task 3 evidence for a later health/acceptance gate;
this task does not add Task 4 incidents or Task 5 process isolation. Feature
flags remain off and nothing is deployed. M1 remains observer-only.
