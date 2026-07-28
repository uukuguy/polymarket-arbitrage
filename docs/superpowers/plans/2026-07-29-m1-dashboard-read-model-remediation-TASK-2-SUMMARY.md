# Task 2 Summary — Authenticated Current Opportunities

Task 2 closes the Dashboard current-opportunity gap with a bounded,
authenticated read model. It remains observer-only and adds no wallet, order,
control, deployment, or trading authority.

- Candidate current authority now carries fact/Structure observation times,
  bundle cost, gross edge, and max bundle size. Its aggregate maintains global
  `watching`, `no-edge`, and `unavailable` counts atomically under the existing
  owner journal.
- Owner authority v2 upgrades to v3 in one `BEGIN IMMEDIATE`: exact v2
  manifest/root/journal and Candidate replay are validated first; current rows
  are rebound to the current revision/latest fact/exact Quote; old triggers
  journal the backfill; canonical aggregate/guard/trigger/index objects are
  rebuilt; the v3 root is installed last.
- Migration tests cover non-empty and compacted retained-seed databases,
  coherent-but-stale rows, corruption, deadline rollback, a527 history,
  restart idempotency, and concurrent initializer convergence.
- `/perception/status` exposes O(1) authenticated Candidate state totals.
  `/perception/opportunities` uses the canonical
  `(opportunity, group_id)` keyset index and reads at most `limit + 1` result
  rows.
- Both envelopes expose the same authenticated Candidate authority hash.
  Dashboard rendering rejects hash, global-count, or first-page cursor drift
  before combining separate HTTP snapshots.
- The Dashboard separates global Candidate state from bounded Structure-page
  counts, renders edge/bundle/max-size/Structure age/Quote age, and states
  `Showing N of total` plus whether more rows exist.
- `make perception-opportunities` is the unified read-only operator entry.
  The living manual and learning document 35 describe the authority boundary.

Commits:

- `5d408ad feat(m1): authenticate current opportunity state`
- `936133f feat(m1): publish authenticated current opportunities`

Verification:

- Full `tests/perception/test_store.py` regression passed.
- 41 focused perception HTTP/Dashboard tests passed.
- Executable Node malformed/cross-envelope contract cases passed.
- Dashboard TypeScript check and production build passed.
- Focused Ruff, M1 manual/docs contract, planning status, and diff checks
  passed.
- Independent gate review approved with no remaining Critical, Important, or
  Minor findings after two Dashboard findings were remediated.

Task 3 is next: bounded incident lifecycle and operator actions. Task 8
deployment remains blocked until the final UI/acceptance gate.
