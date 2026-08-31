# M1 Daily Business Intelligence — Task 2 Summary

Task 2 documented the daily business-evidence workflow and added its regression
contract.

## Changed files

- `docs/learning/106-M1日常业务情报操作指南.md` — daily operating mental model and
  business-truth boundaries.
- `docs/learning/00-INDEX.md` — indexed chapter 106.
- `docs/ops/m1-daily-business-intelligence-log.md` — append-only daily log
  template.
- `tests/m1-perception/test_m1_manual_contract.py` — verifies the guide,
  conclusions, log, and index remain connected.

## Evidence

- Manual contract test:
  `uv run pytest tests/m1-perception/test_m1_manual_contract.py -q`.
- Documentation checker: `make docs-m1-check`.
- The guide binds conclusions to three distinct read-only commands and
  explicitly separates a valid available zero from unavailable business data.

## Non-goals

This documentation does not authorize deployment, recovery, qualification
mutation, trading, execution, or any claim that a reported candidate produces
return or P&L.

## Commit

- `ee9e1da6e2d43c6cf3d26bebb379fed2411d430f` — daily business-intelligence
  guide, index, append-only log, and regression contract.

## Final operations and security-remediation closure

The guide now fixes the observation schedule to Beijing `08:30` for the daily
three-command baseline and `09:00–23:00` every `15` minutes for active-session
status/opportunity review. It explicitly routes observation faults through
`.runtime_incidents`, `.recovery_actions`, and `.runtime_watchdog`.

The accompanying reader hardening is documented here because the guide directs
operators to that target: `6640b330` removed shell interpolation/failure-pipe
ambiguity, and `05ff19a9` removed globally exported lowercase Make values in
favour of target-scoped raw `CONTROL_PLANE_OPPORTUNITIES_*` capture. The final
manual contract locks this cadence and runtime/recovery vocabulary; the related
literal Make syntax regression is in `tests/test_makefile.py`. Fresh final
verification is recorded in `.superpowers/sdd/daily-intel-closure-fix-report.md`.
