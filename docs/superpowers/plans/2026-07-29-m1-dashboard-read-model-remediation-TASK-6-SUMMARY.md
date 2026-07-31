# Task 6 Summary — Dashboard Acceptance and Closure

Task 6 closes the Dashboard acceptance gate for the M1 opportunity-first
read models. It remains observer-only and introduces no deployment, wallet,
order, balance, signing, or trading authority.

- The deterministic visual fixture uses the real SQLite store, reconciliation
  writer APIs, Candidate success writer, and Incident state machine. It refuses
  overwrite and validates project-root path policy before creating a database.
- The overview's current opportunity group ID links directly to its encoded
  four-class operations timeline.
- Reconciliation displays the current authenticated `duration_ms` and all diff
  counts. The historical-duration-not-tracked limitation is unconditional.
- Resource decision age and TTL use authenticated `status.server_time_ms`,
  never the browser clock.
- Known Incident action, retry, next-retry, success-receipt, and verification
  fields are promoted for scanning; complete raw evidence remains available in
  a collapsed disclosure.
- The opportunity table is contained by a local horizontal scroller, while
  long identities wrap safely across the rest of the 375 px layout.
- Six final screenshots cover desktop and 375 px overview, available long-ID
  group history, and unavailable long-ID group history.
- Formal six-pillar re-review scored 24/24 with zero Critical, Important, or
  Minor findings.
- Learning document 39 explains authenticated operator read models, zero versus
  unavailable, server-time TTL, real-store fixtures, and layered Incident
  evidence.

Commit:

- `472d8e2 fix(m1): pass dashboard acceptance gate`

Verification:

- `uv run pytest tests/perception tests/m1-perception -q` passed at 100% with
  only planned xfail/skip outcomes.
- `uv run pytest -q` passed at 100%; only existing deprecation/runtime warnings
  were emitted.
- 18 Dashboard contract tests passed after the final fixture/path-order fix.
- `make dashboard-typecheck` and `make dashboard-build` passed.
- A real-store local fixture backed
  `DASHBOARD_URL=http://127.0.0.1:3000 make smoke-perception-dashboard`; the
  route returned HTTP 200.
- `make docs-m1-check`, `make planning-status`, and `git diff --check` passed;
  planning reported 82 plans with no drift.

Task 7 is locally complete. Task 8 production qualification may begin with
local RED tests and read-only qualification, but deployment, feature-flag
changes, fault injection, and cutover remain separate authorized actions.
