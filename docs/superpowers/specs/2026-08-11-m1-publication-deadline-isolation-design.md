# M1 Structure Publication Deadline Isolation Design

## Problem and evidence

The online Structure generation `892` is resumable, but is not production
recoverable.  Its `markets` normalization has committed data monotonically
across child attempts, while attempts `9369` and `9370` ended as
`snapshot-subprocess-timeout` at `persist` after roughly 75--82 seconds.
Their final durable progress marker reports seven 500-row chunks.  The child
should normally return after a 45-second cooperative slice; 75 seconds is
only the containment limit required to protect Quote's 300-second SLA.

`run_structure_publication_slice` checks the elapsed slice time only between
chunks.  A chunk currently has bounded writer lock acquisition but unbounded
SQLite source/anti-join reads and Python normalization.  It can therefore
start with enough apparent time, overrun the slice, and be killed by the
parent before emitting its next checkpoint.  Repeated writer pressure also
causes the single health read lane to remain occupied after the endpoint
budget expires, reporting `read-model-saturated`.

## Decision

Introduce one absolute monotonic deadline for each publication slice and pass
its remaining budget through every chunk boundary:

1. `run_structure_publication_slice` calculates a deadline once, preserves a
   commit reserve, and never starts a chunk when the remaining budget is below
   that reserve.
2. `run_structure_publication_step` and normalization helpers receive that
   deadline.  Every SQLite source read, bounded anti-join, writer transaction,
   certification read, and pointer transaction installs a SQLite progress
   handler bound to it.  A deadline interruption rolls back only the current
   transaction and becomes a normal checkpoint; prior chunks remain durable.
3. A chunk interrupted before its commit reports zero additional rows and
   retains its prior cursor.  The scheduler records `structure-checkpoint`,
   not `snapshot-subprocess-timeout`, and retries after the normal five-second
   defer delay.
4. Health reads use the same cancellation discipline: all authority queries
   register their read connections with the request deadline.  A timed-out
   request must release the lane after the SQLite interruption; it must not
   leave a worker consuming the next health request's capacity.

The 45-second cooperative slice and 75-second child hard limit are unchanged.
Increasing the hard limit or masking the health failure is explicitly out of
scope: both would extend contention and hide the actual production fault.

## Failure contract

The dashboard and `/healthz` retain the existing P1 incident throughout
recovery.  They expose the latest stage, elapsed time, automatic checkpoint
action, and next operator action.  A Structure incident can close only after
a new generation pointer is atomically published and a later Quote cycle is
certified against that pointer.  An interrupted chunk is not a successful
publication and cannot close the incident.

## Verification

- RED tests reproduce a source query that crosses the deadline and prove no
  new cursor/rows are committed for that chunk.
- GREEN tests prove prior chunks remain committed, the CLI emits a valid
  checkpoint JSON payload, and the scheduler does not classify the case as a
  subprocess timeout.
- A health-lane regression test proves an interrupted authority read releases
  the sole worker for the next request.
- Production acceptance requires a fresh published Structure snapshot, a
  subsequent certified Quote receipt bound to it, cleared P1 incidents, a
  readable dashboard/health response, and multi-cycle soak evidence.
