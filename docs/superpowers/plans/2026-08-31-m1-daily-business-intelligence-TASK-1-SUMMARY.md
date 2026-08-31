# M1 Daily Business Intelligence — Task 1 Summary

Task 1 added the official bounded public reader for the current certified M1
opportunity projection.

## Changed files

- `Makefile` — added the read-only `control-plane-opportunities` target, with
  bounded curl transport and JSON formatting.
- `tests/m1-perception/test_makefile_contract.py` — added the target contract
  and then corrected its source-token lint coverage.

## Evidence

- Focused Makefile contract test:
  `uv run pytest tests/m1-perception/test_makefile_contract.py -k
  control_plane_opportunities_is_current_read_only_business_entrypoint -q`.
- In the delivery verification session, `make control-plane-opportunities
  limit=5` exited 0 and returned `status=available`,
  `current_opportunity_count=0`, and an empty bounded item list.

## Non-goals

The target neither deploys nor writes production state. It does not create an
order, execute a trade, access a wallet, or turn a candidate projection into a
profit or P&L claim.

## Commits

- `307d3dd2d8bbf375dc5bd63390618b63c04cf786` — implementation.
- `f358998105a4a0d5e10e8e24fc21d7d02e466eb8` — Task 1 contract-lint completion fix.

## Final security-remediation closure

The original Make recipe interpolated command-line `limit` and
`after_group_id` values into shell/URL source. `6640b330` first moved the
values behind URL encoding and stopped formatting a failed curl response.
`05ff19a9` closed the remaining Make-expansion boundary: lowercase values are
unexported, while this target alone captures raw values with `$(value ...)` in
`CONTROL_PLANE_OPPORTUNITIES_*` variables and the shell reads only those
target-scoped values.

The final delivery regression is in
`tests/m1-perception/test_makefile_contract.py`; the independent literal-Make
syntax regression added in `tests/test_makefile.py` is also part of the closed
surface. Fresh final command evidence is recorded in
`.superpowers/sdd/daily-intel-closure-fix-report.md`.

- `6640b33069920b0010645c534f3ea9b45b74ab34` — initial query hardening.
- `05ff19a91655d65f6713643d7787c117a2aa597d` — target-scoped raw capture and
  Make-expansion hardening.
