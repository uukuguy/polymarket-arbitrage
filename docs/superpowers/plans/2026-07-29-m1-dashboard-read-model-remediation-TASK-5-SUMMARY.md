# Task 5 Summary — Four-Class Bounded Group Timeline

Task 5 replaces the Dashboard's two-request membership/incident merge with one
authenticated, bounded group operations timeline. It remains observer-only and
adds no producer invocation, deployment, wallet, order, or trading authority.

- `GET /perception/groups/{group_id}/timeline` reads membership revisions,
  Quote batches, opportunity transitions, and exact-scope incident events in
  one SQLite read transaction.
- Items use deterministic
  `(occurred_at_ms DESC, class_order, stable_id DESC)` ordering. Equal
  timestamps paginate without duplicates or omissions.
- The canonical versioned base64url cursor binds the group identity and full
  merge key. Cross-group replay, padding, malformed payloads, and alternate
  encodings fail with a bounded 400 response.
- Each of the four sources returns at most `limit + 1`; the response is capped
  at the requested limit and shares the existing absolute SQL deadline and
  1 MiB response limit.
- Candidate reads authenticate the rolling checkpoint, retained seeds, bounded
  suffix, success receipts, and current projection before returning history.
- Opportunity history emits only changes in normalized
  `(last_result, opportunity)` state. Candidate compaction now binds a
  per-group normalized-state seed into `seeds_json`, so the first suffix fact
  for a group whose physical predecessor was deleted compares against the
  authenticated floor state instead of being mislabeled as initial.
- Legacy compacted checkpoints without the new per-group seed do not invent an
  unprovable predecessor or transition.
- Candidate membership/Quote/opportunity floors are explicitly global and
  conservative. Incident floor remains exact for `candidate:<group_id>`.
  `history_complete` describes prefix retention; `next_before` alone describes
  page continuation.
- The Dashboard strictly validates the discriminated four-class union, exact
  group incident scope, ordering, state transitions, floor/completeness
  relationships, and bounded cursor semantics before rendering.
- The group page renders four semantic colors, 14 px metadata, safe wrapping,
  incident evidence, page counts, continuation, and honest floor copy.
- The living M1 manual and learning document 38 describe the operator contract,
  per-group checkpoint seed, and the difference between history completeness
  and pagination.

Commit:

- `301fad1 feat(m1): add bounded group operations timeline`

Verification:

- RED/GREEN HTTP cases cover four interleaved equal-timestamp classes,
  duplicate-free pagination, canonical/cross-group cursor rejection,
  cross-group checkpoint-floor transitions, conservative Candidate
  completeness, exact Incident completeness, shared SQL deadline, and the
  response-size cap.
- SQLite trace proves membership, Quote, and opportunity source queries use
  `limit + 1`; IncidentManager's exact-scope reader retains its own
  `limit + 1` contract.
- Candidate checkpoint/tamper/rollback regressions and the complete Incident
  authority suite passed after adding per-group timeline seeds.
- Store, Incident, perception HTTP, and Dashboard contract suites passed.
- Focused Ruff, Dashboard TypeScript checking, Next.js production build,
  M1 manual contract, pre-commit contract guard, and diff checks passed.

Task 6 is next: final Dashboard acceptance, documentation closure, and parent
Task 7 summary. Task 8 deployment remains blocked until that gate passes.
