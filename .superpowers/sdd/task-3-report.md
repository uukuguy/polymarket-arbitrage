# Task 3 Implementer Report

Status: DONE

## Scope

Task 3 only: bounded Discovery, durable group identity, certified scheduling,
capacity-proven Candidate admission, priority/freshness/load control, rolling
coverage, validated read-only status, and default-off daemon wiring. No Task 4,
deployment, production enablement, public Dashboard/API, or trading.

## Commit Chain

- `046cef1` — initial bounded Discovery implementation.
- `83515f9` — first authority/freshness/status hardening.
- `436c389` — second continuity/receipt/load-control hardening.
- Current commit — third re-review remediation described below.

## Third Re-review RED → GREEN

### 1. Identity binds at first sight

The durable schedule is now the first `group_id → event_id` identity anchor,
including incomplete or unsupported source truth that has no certified
revision. A later complete observation under another event rejects the whole
page before any cursor, schedule, coverage, or revision becomes durable.
Recovery under the original event certifies normally.

### 2. Promotion carries a real ≤60-second service proof

`discovery_candidate_max_wait_s` defaults to 60 and cannot exceed 60. Settings
derive the effective outstanding factless capacity from:

```text
poll
+ high_burst_groups * group_timeout
+ (capacity - 1) * group_timeout
<= candidate_max_wait
```

The capacity also cannot exceed Task 2's reserved lower-lane slots. With
production defaults, the effective capacity is one.

Complete-supported groups are certified immediately but enter a durable
promotion queue with eligibility and queue-deadline evidence. Only the
capacity-proven set receives admitted/promoted and Candidate-start deadline
timestamps. The scheduler runs a genuine high burst first and then admitted
promotions from reserved lower capacity; promotions are never relabelled
global high. Excess groups remain certified and unpromoted, ordered by queue
deadline, score, then group ID across restart.

A Candidate terminal fact and admission of the next queued group share one
`BEGIN IMMEDIATE` transaction. Queued-unpromoted groups are excluded from the
Candidate freshness p95/missing set, while admitted Discovery groups and
non-Discovery Candidate authority remain included.

### 3. Status proves load phase and the whole cursor chain

Every durable load-control row now stores its configured probe modulus. Status
accepts `probe` exactly when `streak % modulus == 0`, requires `yield`
otherwise, and proves fresh reset semantics.

Every immutable batch receipt stores sweep ID and within-sweep sequence. One
read transaction validates every historical transition:

- a nonterminal batch's `next_cursor` equals the next receipt's
  `requested_cursor`;
- a terminal `None` starts the next sweep with explicit `requested_cursor=None`;
- sweep and sequence numbers advance exactly;
- latest state still matches the latest receipt and per-group samples.

The same snapshot validates admission proof arithmetic, admitted factless
count, persisted eligibility/queue/admitted/start deadlines, current certified
event/membership authority, recomputed priority evidence, and coverage bounds.
CLI failures remain bounded exit 2 without traceback or database-path leakage.

## TDD Evidence

Observed RED failures included:

- incomplete `e1/g1` followed by complete `e2/g1` committed instead of failing;
- no persisted probe modulus or receipt sweep/sequence columns;
- no admission-capacity API, so every supported group promoted immediately;
- queued certified groups polluted durable freshness;
- status did not reject a wrong-streak probe or broken historical cursor link.

Focused GREEN:

```text
64 passed
```

Task 3 + Task 1/2 + Gamma/routing/daemon proportional GREEN:

```text
276 passed
```

Additional gates:

- changed-file Ruff: pass;
- `git diff --check`: pass;
- `make docs-m1-check`: pass;
- `make planning-status`: no drift;
- valid `make perception-discovery-status db_path=...`: exit 0.

## Compatibility and Boundaries

`init_schema()` performs idempotent migration for the new schedule, receipt,
and load columns; derives sweep/sequence for historical receipts; seeds the
conservative one-slot/60-second proof; fills legacy deadline evidence; and
demotes excess legacy factless promotions deterministically.

Rolling coverage and degraded probing remain statistical, not zero-miss
claims. The feature remains default-off and undeployed. M1 remains
observer-only: no wallet, signing, balances, order submission, or funds.
