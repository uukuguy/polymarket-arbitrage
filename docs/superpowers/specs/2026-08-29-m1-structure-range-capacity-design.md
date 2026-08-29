# M1 Structure Range Capacity Design

## Problem and measured constraint

Production qualification is permanently `recovering` on
`freshness.structure`. One generation contains 1,115 immutable range jobs. The
single range lane completed 123 jobs in 15 minutes, or about 8.2 jobs/minute;
one generation therefore needs about 136 minutes against a 15-minute
freshness contract. Admission also allows another generation whenever fewer
than 2,000 range jobs are unfinished, so inadequate capacity can become a
two-generation backlog.

The fix must not enlarge freshness, attempt, lease, or qualification clocks.
It must preserve per-range lease fencing, checkpoints, immutable artifacts and
terminal fan-in.

## Considered approaches

### A. Bounded parallel lanes plus single-generation backpressure — selected

Run 12 independently fenced Structure range workers in one pool. The lower
bound from production is `ceil(136 / 15) = 10` lanes; 12 provides two lanes of
headroom for PostgreSQL/R2 and scheduler overhead and matches the already
proven bounded worker-pool tier. Each lane has a distinct worker identity and
owns one lease. Set the admission high-water default to one unfinished range,
which means a new source window cannot start while any prior range remains.

### B. Merge source shards into larger range jobs

This would reduce claim and R2 round count, but changes range identity,
artifact digests, checkpoint semantics and certifier inputs. It is a later
optimization, not a safe production recovery.

### C. Add Machines or CPU without changing scheduling

Additional resources may improve throughput but do not remove the incorrect
two-generation admission rule. A transient slowdown would recreate the same
backlog, so infrastructure-only scaling is insufficient.

## Architecture

`Settings.structure_range_max_concurrency` is the sole capacity authority and
is bounded to 1..32 with default 12. `_transactional_structure_worker()` builds
that many `TransactionalStructureWorker` lanes over the existing shared R2
client and control plane. `TransactionalStructureRangePool` validates a common
positive lease, runs one turn per lane concurrently, drains all siblings before
reporting an error, and exposes the shared lease for terminal-grace derivation.

Every lane retains its current durable sequence:

```text
claim fenced job -> read spec/checkpoint -> normalize -> upload -> terminal receipt
```

No pool-level attempt, retry, timeout, heartbeat, or checkpoint is introduced.
SIGTERM cancels every active lane; each lane owns its existing
`finish_interrupted` transition and lease expiry remains the recovery authority.

Source admission changes its default `structure_high_water` from 2,000 to 1.
The existing query already counts unfinished `structure-normalize` jobs, so the
transactional advisory lock now admits a new source window only when that count
is zero. Quote high-water behavior is unchanged.

## Error handling and observability

- One lane failure remains attached to its exact durable attempt and cannot
  cancel or roll back healthy sibling receipts.
- The pool drains siblings, then returns one bounded aggregate result or raises
  the first classified error to the role loop.
- A heterogeneous/non-positive lane lease fails construction.
- Production acceptance uses the already queued generation: unfinished count
  must drain, certifier must publish, and the next generation must not overlap
  unfinished ranges from its predecessor.

## Verification and acceptance

1. RED tests prove 12 lanes are built from one Settings authority, claim
   concurrently with distinct IDs, propagate cancellation, and expose one
   lease policy.
2. RED PostgreSQL/unit admission tests prove one unfinished range returns
   `backpressured:structure` under the default.
3. Focused worker, CLI, scheduler, deployment, config, Ruff and Pyright gates
   pass.
4. An exact image rolls only the Structure range Machine first. Its current
   666-job backlog must show parallel leases and materially higher drain rate.
5. Full runtime rollout follows only after the canary preserves memory,
   terminal receipts and `SIGTERM/40s`.
6. The exact-head full M1 suite runs without an arbitrary outer timeout.

## Self-review

- No placeholder or optional fallback remains.
- Capacity, admission, attempt and freshness remain distinct authorities.
- The design changes no artifact schema, migration, publication pointer or
  qualification policy.
- The minimum measured lane requirement and chosen bounded headroom are
  explicit rather than copied from an unrelated timeout.
