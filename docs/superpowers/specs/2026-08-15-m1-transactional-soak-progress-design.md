# M1 Transactional Soak Progress Evidence Design

## Problem

The existing transactional soak record proves that the independent control API
remains readable, the five named Fly Machines retain their identities and
states, sampling remains continuous, and expired leases or open circuits do
not increase.  Those are necessary safety checks, but they alone cannot prove
that the transactional collector is still doing durable work: a healthy but
stalled scheduler would pass them.

## Decision

Introduce a new, self-authenticating `m1-transactional-soak-v2` record.  Every
read-only sample copies the control API's total successful-job counter.  The
verifier retains every v1 fail-closed invariant and additionally requires:

1. every sample has a non-negative integer `successful_job_count`;
2. the count never decreases; and
3. the final count is strictly greater than the immutable baseline.

The new evidence uses a separate JSONL file and a fresh 24-hour qualification
window.  The existing v1 evidence remains unchanged as historical proof of
the earlier automatic sampler.

## Data Flow and Safety

`control-plane-soak-sample` still makes only one HTTPS GET to the independent
operator API plus a read-only Fly Machine status query.  It derives
`successful_job_count` only from `job_counts.succeeded`; it does not use a
database DSN, a queue claim, a pointer write, or an alert row.  The record is
hashed with the existing chain scheme, so neither the baseline nor a later
success count can be silently changed.

The verifier fails closed if the counter is absent, non-integer, decreasing,
or unchanged across the complete window.  This intentionally qualifies the
current staging topology, where Structure work is continuously durable; it is
not a generic idle-environment health check.

## Validation

- Unit tests first demonstrate that a valid v2 stream requires forward
  progress, then demonstrate rejection for a stalled and a decreasing stream.
- Existing v1 tests remain valid and v1 records remain verifiable under their
  original contract.
- The named LaunchAgent switches only after the test suite is green, and the
  first v2 baseline and automatic follow-up sample prove its actual schedule.

## Non-goals

- No change to job leases, retries, circuits, R2 receipts, queue scheduling,
  production L1/L2, or publication pointers.
- No modification of the existing v1 JSONL evidence.
- No use of the blocked Fly alert-secret path.
