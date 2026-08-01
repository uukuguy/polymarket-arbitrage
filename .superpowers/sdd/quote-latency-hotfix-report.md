# Quote Latency Hotfix Report

## Production root cause

Read-only incident evidence showed Quote freshness reaching 308 seconds. Every collection projected
roughly 35k eligible targets by first materializing about 116k Structure market rows and 116k
membership rows in Python. The same verified-universe work was repeated at admission and again when
serving the completed projection. The pre-fetch scans consumed most of the Quote freshness budget and
created shared SQLite/CPU pressure; repeated degradation did not repair that deterministic hot path.

No production deployment, restart, configuration change, or database mutation was performed by this
task.

## Fix

- Target-only indexed SQL joins `complete-supported` standard truth directly to its markets and
  memberships. Unsupported/augmented groups retain one rejection record without expanding members.
- A projection carries a Structure identity receipt. Generation mode binds it to the authenticated
  current pointer and sealed comparison receipt. Legacy mode uses a coalesced revision fence: the
  first source mutation dirties and increments once, bulk rows do not contend on a hot counter, and
  publication clears dirty only after all source writes, immediately before COMMIT.
- Admission validates that receipt in O(1) rather than rebuilding the universe. A source mutation
  between projection and admission fails closed.
- Each new quote run stores an immutable source receipt and rejection projection. Certification and
  serving reconstruct from run-bound legs plus that receipt, eliminating the third Structure scan.
  Historical runs without the new receipt retain a compatibility fallback; unreleased draft receipts
  that did not seal full leg/quote identity are quarantined and cannot regain trust through fallback.
- Durable attempts expose `universe → admission → fetch → transform → persist → certify → projection`
  checkpoints, phase timings, target count, Structure receipt, failure, and outcome. Parent and child
  exchange a strict attempt identity; cancel/nonzero exits record failure. Strict health warns after a
  phase is stalled for 45 seconds and fails after 120 seconds.
- CLOB fetch has a 100-second stage budget and the child has a 120-second absolute budget measured
  from attempt start. A stable exit code plus strict attempt-bound JSON error envelope carries the
  controlled fetch timeout to the parent; malformed or forged envelopes fail closed. The parent uses
  one monotonic terminate/kill/reap deadline, stops lease renewal, fails the collecting run and
  attempt, and retries timeouts immediately without clearing the old feed. Cancellation cleanup never
  masks the original `CancelledError`.
- Settings share one timing source of truth and reject any cadence + child + publish-reserve budget
  that is not strictly below the 300-second freshness SLA, or any fetch + shutdown budget that is not
  strictly below the child hard limit. The formerly legal 240-second cadence is rejected with the
  production defaults.
- Attempt admission closes parent-orphaned collecting attempts and bounds terminal attempt evidence
  even during continuous failure-only periods.
- The previous certified opportunity feed remains the serving authority while a new attempt collects;
  it is replaced only after certification/projection, while its existing freshness limits remain in
  force.

## TDD and bounded-plan evidence

- A 2,000-row augmented decoy fixture initially proved the old `fetchall()` materialization, then
  passed with target-only projection.
- `EXPLAIN QUERY PLAN` tests reject broad target-table scans and temporary B-trees.
- A monkeypatched forbidden-rescan test proves admission does not call universe projection again.
- A 2,000-row mutation test proves write amplification is exactly `rows + 2`: one revision update and
  one dirty insert for the whole bulk write.
- Parent/child tests prove spawn-time attempt durability, strict JSON/receipt/timing validation,
  cancellation/nonzero failure closure, direct collector checkpoints, and the 120-second health fail.
- The four full-suite failures found during verification exposed a real publication-boundary bug:
  snapshot metadata was inserted as published before truth and market rows. Both snapshot writers now
  clear the fence only after every source row is written and before the atomic COMMIT; all four focused
  regressions pass.

## Final verification

- Full M1 (`tests/perception/ tests/m1-perception/`): `3072 passed, 1 skipped, 1 xfailed`
  (exit 0).
- Focused publication-boundary regressions: 4 passed.
- Final expanded exact gate: 199 passed.
- Worker/collector/health expanded gate: 95 passed.
- CLI and Make contract tests: pass.
- Changed-file Ruff, `git diff --check`, and `make planning-status`: pass.

## Independent review remediation

The first independent review requested changes and found five missing closure paths. The follow-up
adds a retention-safe no-FK attempt identity (including legacy-FK detachment), a canonical digest over
snapshot identity plus every leg/quote identity, BUY side, terminal outcome, price and size, and DB
immutability guards for completed runs with an explicit bounded purge authority. Spawn, child protocol,
timeout, certification and projection failures now terminalize their attempt; `complete` occurs only
after the certified feed is published. Attempt evidence read failures make strict health fail rather
than masquerading as `never-attempted`. The broad snapshot publication trigger that could clear an
unrelated legacy dirty fence is removed and migrated away.

Later review rounds also closed failure-only attempt retention, orphan recovery, cleanup-error
masking, explicit reap, child fetch-timeout propagation, immediate retry, strict timing configuration,
and the shared shutdown deadline. Independent review of source SHA `674c4d3` returned
`APPROVE — no findings`. The final frozen-source exact gate and the final full M1 gate both pass after
these remediations. No production mutation or deployment was performed.
