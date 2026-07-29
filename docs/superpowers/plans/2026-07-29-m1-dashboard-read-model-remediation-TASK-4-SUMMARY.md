# Task 4 Summary — Bounded Resource Decision History

Task 4 replaces unbounded resource-policy replay with an authenticated
checkpoint and bounded suffix, then exposes the same evidence through health,
the public perception API, and the Dashboard. It remains observer-only and adds
no producer invocation, deployment, wallet, order, or trading authority.

- Every resource writer validates the owner manifest, authenticated checkpoint,
  retained sample/decision suffix, deterministic policy replay, and keyed
  failure breadcrumb before appending.
- The suffix compacts from 513 pairs to 256 pairs. Public validation rejects
  more than 1,024 pairs and uses actual `cap + 1` SQL limits for both counts and
  joined replay rows.
- The checkpoint stores the compacted floor identity and decision anchor,
  prefix digest, current suffix-tail digest, and owner-journaled checkpoint
  hash. Updating the tail digest on every decision detects deletion of an
  un-compacted terminal pair.
- Compaction, sample/decision append, checkpoint publication, and old-pair
  deletion share one `BEGIN IMMEDIATE`. Injected checkpoint failure rolls back
  every mutation; concurrent high-water writers serialize without loss.
- Authority and hard-limit failures roll back first, then independently publish
  `neg_risk_evidence_failures(component='resource')`. The next successful
  writer independently marks the row recovered before beginning its business
  transaction.
- Public `validate_resource_history()` centrally rejects unresolved resource
  breadcrumbs, so component controls, health mode projection, incident recovery
  proof, and store consumers cannot bypass the failure state.
- `perception:resource_evidence` has no feature/config gate. Existing corrupt
  resource evidence makes strict `/health` fail even when the resource
  controller is disabled.
- `/perception/resources` returns the current decision, a bounded descending
  sample/decision page, keyset cursor, and compaction floor. The matching
  `make perception-resources` command is read-only.
- The Dashboard strictly validates canonical policy v1 decisions and reason
  enums, then renders mode/reason, policy age/TTL, hot-path inputs, bounded
  recent transitions, cursor, and history floor. Null quote p95 is displayed as
  not observed, never as zero.
- The living manual and learning document 37 describe the operator contract and
  checkpoint/suffix mental model.

Commits:

- `7debd9b feat(m1): authenticate bounded resource history`
- `e03eec2 feat(m1): expose resource evidence health`
- `9c49789 feat(m1): publish bounded resource decisions`

Verification:

- Resource tests cover 513-pair compaction, 2,000 decisions, restart,
  checkpoint and tail tamper, `cap + 1` SQL trace, concurrent high-water
  writers, transaction rollback, owner-trigger loss, breadcrumb recovery, and
  component-control fail-closed behavior.
- Store, incident, supervisor, resource, perception HTTP, health, Dashboard
  contract, and component-control cross-domain regressions passed.
- Executable malformed JSON cases, focused Ruff, Dashboard TypeScript checking,
  Next.js production build, M1 docs contract, Make dry-run, planning status, and
  diff checks passed.
- Independent review approved with no remaining correctness, security, or
  performance findings after central breadcrumb, parameter-error, and strict
  policy-enum gaps were remediated.

Task 5 is next: four-class bounded group timeline. Task 8 deployment remains
blocked until the final UI/acceptance gate.
