# M1 Transactional Soak Evidence Design

## Goal

Turn a live staging control-plane run into reviewable, fail-closed 24-hour
evidence. The recorder must not claim availability from an operator's memory or
from a hand-assembled JSON document.

## Constraints

- Read only the independent control API and Fly Machines API; never write M1
  Postgres, SQLite, R2, pointers, or alerts.
- The evidence window is staging-only and starts from an explicit immutable
  baseline. Historical quarantines, circuits, and outbox rows remain visible
  but do not retroactively fail a later window.
- Every sample is append-only JSONL, contains its collection timestamp and
  canonical snapshot digest, and is written with exclusive create semantics.
- Verification runs locally from saved evidence and fails closed for missing,
  reordered, malformed, stale, or unhealthy samples.
- Existing `verify-fault-soak` continues to require real Structure/Quote
  takeover and circuit evidence; this recorder supplies only the continuous
  soak portion.

## Alternatives considered

1. Hand-write the existing `soak` JSON. Rejected: it has no provenance and
   cannot prove an uninterrupted window.
2. Query Postgres after 24 hours. Rejected: it cannot prove that the
   independent API and all worker Machines remained observable throughout.
3. **Chosen: append-only observer evidence.** A small local command captures
   control API plus the five named Machine states at a fixed interval, and a
   pure verifier validates the saved chain against an initial baseline.

## Architecture

`polyarb.control_plane.soak_evidence` owns typed snapshots, canonical JSON
bytes, JSONL parsing, and a pure verifier. `cli_control_plane` exposes three
explicit commands:

1. `soak-start --output <path> --control-api-url <url> --machine-id ...`
   makes the initial baseline record with `O_EXCL` and refuses a pre-existing
   path.
2. `soak-sample` appends one record only to an already initialized file. It
   reads `/perception/control-plane` and each exact machine id using `flyctl
   machine status --json`; it rejects non-started Machines and unavailable API
   responses before appending.
3. `soak-verify` reads only the JSONL file. It requires an initial baseline,
   strictly increasing sample times, fixed API URL/Machine identity set,
   exactly started worker states, available API, a maximum sample gap, and a
   duration of at least 86,400 seconds. It reports duration and tick count for
   insertion into the existing fault-soak evidence.

The baseline includes historic job/circuit counts. Later records may retain or
reduce them, but verifier only rejects a newly higher `expired_leases` count or
new `open_circuit_count` above baseline. Queue depth is recorded as evidence,
not a pass/fail threshold: market arrivals can legitimately grow it.

## Data contract

Each JSON line has the following canonical structure:

```json
{
  "kind": "m1-transactional-soak-v1",
  "observed_at": "2026-08-15T13:00:00+00:00",
  "control_api_url": "https://.../perception/control-plane",
  "machine_ids": ["..."],
  "machine_states": {"...": "started"},
  "control_api_status": "available",
  "queue_health": {"structure-range": {"unfinished_count": 1}, "quote-batch": {"unfinished_count": 0}},
  "expired_leases": 0,
  "open_circuit_count": 0,
  "snapshot_sha256": "..."
}
```

`snapshot_sha256` hashes every preceding field in canonical key order. The
baseline is the first line; no separate mutable state file exists.

## Failure handling

- Network/parse/command failures do not append a record. The subsequent gap
  causes verification failure rather than fabricating continuity.
- Existing output, duplicate machine ids, an unexpected machine state, API
  status other than `available`, non-monotonic time, or a digest mismatch are
  immediate errors.
- A stopped/removed machine or clock gap is evidence of a failed soak, not an
  automatic repair request.

## Verification

- Unit tests cover canonical round-trip, exclusive start, malformed/digest
  rejection, baseline comparison, state/identity change, sample-gap, and the
  24-hour boundary.
- CLI tests monkeypatch HTTP/Fly reads and prove commands never call a M1
  mutator.
- The Makefile exposes `control-plane-soak-start`,
  `control-plane-soak-sample`, and `control-plane-soak-verify`.
- A staging window is valid only after natural samples cover 24 uninterrupted
  hours and its verifier output is incorporated into fault-soak evidence.
