# Task 1 Summary — Perception Progress Evidence

Task 1 closes the first Task 7 UI-audit gap by carrying already-authenticated
Discovery and Reconciliation facts through the bounded public API into the
Dashboard.

- `/perception/discovery` now exposes validated 15/30/60-minute raw and
  liquidity-weighted coverage, known-group/liquidity totals, load state,
  admission proof, queue state, attempt counts, and deadline-breach counts.
- `/perception/reconciliation` now exposes the current validated window
  duration, observations/baseline counts, and all five diff counts.
- The Dashboard renders those facts instead of amber placeholders. It states
  that historical Reconciliation duration distribution is not tracked; no
  unbounded historical scan or false distribution claim was added.
- TypeScript envelope validation mirrors backend invariants for finite
  non-negative integer counts, fractions, admission budgets and derived start
  bound, capacity, coverage cardinality, candidate readiness, and
  Reconciliation diff all-or-none/applied-state relations. Executable Node
  contract cases prove malformed HTTP 200 payloads fail validation.
- The living M1 manual describes the operator-visible fields and contract
  revision. No producer, deployment, control, or trading authority changed.

Commits:

- `8ba84cb feat(m1): expose perception progress evidence`
- `019150a fix(m1): reject impossible progress evidence`
- `b9b5afa fix(m1): bind progress evidence relations`

Verification:

- 37 focused perception HTTP/Dashboard contract tests passed.
- Executable malformed-envelope contract cases passed.
- Dashboard TypeScript check, focused Ruff, docs contract, planning status, and
  diff checks passed.
- Independent review approved with no remaining Critical, Important, or Minor
  findings.

Task 2 remains observer-only and will add authenticated current opportunity
details plus O(1) candidate state counts. Task 8 deployment remains blocked.
