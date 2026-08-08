# M1 Membership Conflict Recovery Summary

**Status:** Locally qualified; deployment requires a new exact-SHA approval.

- Preserves the strict event/member versus market identity check.
- Adds one idempotent, atomic retirement path for an unpublished publication
  after authenticated `membership-invalid`: publication/window become failed
  with `publication-membership-invalid`, the building snapshot becomes failed,
  and the serving pointer plus frozen source staging remain unchanged.
- The producer catches only `StructureMembershipInvalidError`, records the
  retirement, and returns a superseded checkpoint so the next scheduler tick
  naturally starts a new window instead of retrying the contradictory one.

Verification: targeted RED/GREEN tests, full publication suite, scheduler/CLI
regression suites, changed-file Ruff, diff check, and `make planning-status`.

Production acceptance: generation 880 must be retired with immutable evidence;
the next natural window must collect, certify, publish, and reset snapshot
failure evidence without any pointer, read-mode, Quote, or manual data action.
