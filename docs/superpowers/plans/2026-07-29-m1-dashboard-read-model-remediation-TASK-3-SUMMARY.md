# Task 3 Summary — Bounded Incident Lifecycle and Operator Actions

Task 3 closes the Dashboard incident-evidence gap with a bounded,
owner-authenticated lifecycle authority. It remains observer-only and adds no
wallet, order, notification-delivery, deployment, or trading authority.

- Owner authority is now v5. Frozen v4 databases migrate atomically to v5;
  copied v2 and v3 databases pass through their canonical migrations, while
  partial, forged, or corrupt manifests fail closed.
- Open incidents live in normalized authority rows with an authenticated
  singleton aggregate. Open authority is capped at 4,096 rows, scope floors at
  8,192 rows, retained events at 512 rows, and replay anchors at 256 rows.
- Incident compaction publishes an authenticated prefix checkpoint, exact
  per-scope floors, replay anchors, and a suffix chain in the same owner
  transaction. Restored-trigger DELETE tests prove that leaf removal cannot be
  hidden by editing aggregate or checkpoint fields.
- Every authority-critical count and migration read is bounded with `cap + 1`
  sentinel work. Frozen-v4 trace tests verify actual SQL limits of 513, 4,097,
  and 8,193 before any in-memory processing.
- Evidence payloads use canonical JSON and are capped at 4,096 bytes. Failed
  post-append authority validation rolls back first, then records a bounded,
  owner-journaled incident breadcrumb; the next successful validated writer
  marks it recovered in a separate transaction.
- Operator control gates now validate normalized open authority and exact
  component scope. A regression compacts the active discovery event out of the
  suffix and proves that the retained open row still blocks control.
- `/perception/incidents` exposes bounded keyset pagination, lifecycle age,
  canonical actions, retry state, recovery-start evidence, and history floors.
  `/health` reads the same incident evidence chain and fails closed on
  corruption or unresolved breadcrumbs.
- The Dashboard renders current incident state and recovery details without
  claiming notification delivery. The living manual and learning document 36
  describe the authority and compaction model.

Commit:

- `b22db52 feat(m1): bound incident recovery evidence`

Verification:

- The final eight-suite regression passed:
  store, incidents, supervisor, resource controller, perception HTTP, health,
  Dashboard contract, and perception controls.
- Incident/store/control focused regressions and all migration, cap, restored
  trigger, compaction, and exact-scope tests passed.
- Dashboard TypeScript checking and the Next.js production build passed.
- Focused Ruff, M1 manual/docs contract, planning status, and diff checks
  passed.
- Independent gate review approved with no remaining correctness, security, or
  performance findings after cap-order and compacted-open control gaps were
  remediated.

Task 4 is next: bounded resource-decision history. Task 8 deployment remains
blocked until the final UI/acceptance gate.
