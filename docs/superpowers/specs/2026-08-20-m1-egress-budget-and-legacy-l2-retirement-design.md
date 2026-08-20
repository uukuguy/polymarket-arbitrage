# M1 Egress Budget and Legacy L2 Retirement Design

**Status:** approved for implementation — operator authorization, 2026-08-20

## Problem

Supabase reports 24.48 GB of egress in the 2026-08-12 billing cycle, exceeding
the Free-plan 5 GB allowance by 19.48 GB. This is bandwidth already delivered
from Supabase; it is not the current database footprint.

The obsolete L2 candidate-refresh path is an unacceptable likely producer. It
downloads every `markets_latest` row using `SELECT *` (or its paginated REST
equivalent), materialises that data in a throwaway SQLite file for recipe
execution, then deletes the file. A 60-second debounce/maintenance cadence can
repeat this transfer without creating a durable M1 input artifact. The old L2
machine is stopped and must never be reintroduced as a production dependency.

The current M1 transactional control plane is pre-production: its prior formal
run was deliberately invalidated by the capacity cleanup. It must start a new
acceptance run only after this budget boundary is present.

## Alternatives considered

1. Raise the Supabase plan or accept the overage. Rejected: it hides an
   unbounded consumer and gives no attribution or emergency stop.
2. Keep L2 but lower its polling interval. Rejected: a full-table transfer
   remains unbounded and has no useful authority role in M1.
3. Retire L2 cloud consumption and make M1 accountable for every cloud input.
   Chosen: it removes the known unsafe path and makes costs observable before
   the next formal run.

## Architecture

### 1. Legacy L2 retirement boundary

The retired L2 candidate refresh must fail closed when configured for a cloud
market source. It must not make either REST or direct-Postgres full-table
requests. The retired Fly app remains stopped; M1 has no import or deployment
dependency on it.

This is intentionally a safety boundary, not an L2 rewrite. A future L2
program, if ever justified, needs a separate design based on a compact,
versioned R2 projection and cursor/digest reuse.

### 2. M1 egress ledger

M1 will record a bounded, append-only `cloud_usage_observations` fact for every
networked input producer. Each observation contains only non-secret metadata:
source name, operation, generation/digest or request identity, response bytes,
item count, timestamp, and whether the input was retained as a verified R2
artifact. A transfer with no retained authority artifact is rejected for formal
collection.

The initial scope is M1 structure and quote admission. The meter measures bytes
at the client boundary; it is a conservative operational control, not a claim
to replace Supabase's billing meter.

### 3. Budget decision and fail-closed behavior

A durable budget policy stores a daily byte ceiling and warning thresholds at
50%, 75%, and 90%. Every observation is evaluated transactionally:

- below 50%: admit normally;
- 50% and 75%: append a Dashboard-visible warning and alert intent once per
  threshold/day;
- 90%: open a critical incident, write Dashboard detail, enqueue Telegram, and
  refuse new non-essential collection admissions until the next UTC budget day
  or an explicit operator-authorized policy revision.

The response includes current used bytes, threshold, decision, and causal
observation identity, so the Dashboard can explain both detected and recovered
states. Failure to write/read the ledger fails collection closed; it may not
silently continue unmetered.

### 4. Dashboard and monitoring

The existing control-plane projection exposes current budget status plus the
latest bounded observations and incident links. The independent watchdog treats
an absent/stale budget observation during an active collection run as unhealthy.
Telegram remains delivery only; Postgres remains the durable source for the
Dashboard.

## Data flow

```text
bounded M1 source response
  -> measure bytes + verify R2 authority artifact
  -> transactional usage observation + budget decision
  -> allow admission OR refuse it
  -> Dashboard incident/history + outbox intent
  -> isolated Telegram delivery
```

No M1 consumer may read Supabase as a full-snapshot transport channel. Postgres
continues to hold transactional state, references, receipts, and small indexed
projections; R2 holds immutable larger input artifacts.

## Verification

Tests must first demonstrate each missing behavior:

1. retired L2 cloud candidate refresh fails before opening REST/DB clients;
2. an unretained input cannot create a usage observation or admission;
3. byte accumulation is UTC-day scoped and threshold transitions deduplicate;
4. 90% refuses a new admission and creates a Dashboard/Telegram intent;
5. the control-plane API renders current usage, thresholds, causal observation,
   and incident state without secrets;
6. a database failure prevents collection rather than bypassing the meter.

Acceptance additionally requires a deployment preflight that shows the legacy
L2 Fly machine stopped, no M1 template refers to it, and the budget gate is
healthy before a new 24-hour formal run starts.

## Non-goals

- Retroactive per-client attribution of the already accrued 24.48 GB (the
  provider UI exposes an aggregate, not a process-level ledger).
- Deleting billing-period bandwidth by deleting data (impossible).
- Restarting L2 or declaring any old formal run valid.
