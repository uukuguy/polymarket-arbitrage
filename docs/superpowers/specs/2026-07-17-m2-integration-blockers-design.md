# M2 Integration Blockers — Fail-Closed Design

## Context

Pre-merge review of `feat/m2-position-persistence` found three HIGH issues:
execution could report success after durable booking rejection, modeled fill identity
was not bound to its payload, and the climb post-commit hook could amend unrelated
staged changes while bypassing pre-commit verification.

## Decision

Use the smallest fail-closed correction:

1. `ExecutionEngine` treats position booking as part of leg success. A rejected
   `open_position` changes that leg result to failed, prevents its close path, and feeds
   the existing ABORTED/FAILED/PARTIAL status calculation. No separate booking-state
   model is introduced.
2. Every fill with immutable `fill_id` receives a versioned deterministic request
   fingerprint. Modeled fills bind market, exact quantity micros, and canonical exit
   price. Venue-settlement fills retain their stronger market/quantity/gross/fee/status/
   source fingerprint. Same identity plus changed payload conflicts atomically.
3. CLI fill-ID replay requires explicit `--size` and always reconstructs the fill and
   calls tracker/repository. Caller-owned legacy `--operation-id` replay remains
   compatible.
4. Remove the post-commit auto-amend hook. Research-tree regeneration must be explicit
   in the command that mutates climb state; no hook may sweep the staging index or use
   `--no-verify`.

## Alternatives Rejected

- Log booking rejection but keep execution successful: preserves the accounting lie.
- Add a new two-dimensional execution/booking state machine: useful eventually for live
  venue compensation, but unnecessary before a real adapter exists.
- Keep auto-amend and try to stage only named files: still rewrites history after the
  required verification boundary and complicates failure recovery.

## Acceptance

- Duplicate market with a new signal ID cannot report COMPLETED or expected PnL when
  durable booking is rejected.
- Same modeled fill ID with changed quantity or exit price raises identity conflict and
  leaves raw state unchanged; identical retry still replays.
- CLI rejects fill ID without explicit size before state mutation and detects changed
  modeled retry payload across processes.
- `.githooks/post-commit` performs no amend and contains no `--no-verify`.
- Targeted M2 regression, Ruff, planning-status, and diff checks pass before merge.
