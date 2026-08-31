# Daily Intelligence Closure Fix Report

Date: 2026-08-31

## Scope and root cause

Final review found delivery-owned tests and records still described superseded
behavior. The security root cause was command-line Make `limit` and
`after_group_id` values reaching generated shell/URL source through global
exports/expansion. `6640b330` contained direct shell interpolation and curl
failure-pipe ambiguity. `05ff19a9` closed the remaining Make boundary by
unexporting lowercase globals and capturing raw values only on the
`control-plane-opportunities` target as
`CONTROL_PLANE_OPPORTUNITIES_LIMIT` and
`CONTROL_PLANE_OPPORTUNITIES_AFTER_GROUP_ID` via `$(value ...)`.

## RED

Before this closure edit:

```text
uv run pytest tests/m1-perception/test_makefile_contract.py::test_control_plane_opportunities_is_current_read_only_business_entrypoint tests/m1-perception/test_m1_manual_contract.py::test_daily_business_intelligence_guide_keeps_business_truth_boundaries -q
FF                                                                       [100%]
```

The Makefile contract still required obsolete `export limit after_group_id` /
`$${limit}` text. The guide contract still required vague `早晨开盘前` wording.

## GREEN

The updated contracts require target-scoped raw capture plus shell references,
and exact Beijing cadence `08:30`, `09:00–23:00`, every `15` minutes, with
`.runtime_incidents`, `.recovery_actions`, and `.runtime_watchdog`.

```text
uv run pytest tests/m1-perception/test_makefile_contract.py::test_control_plane_opportunities_is_current_read_only_business_entrypoint tests/m1-perception/test_m1_manual_contract.py::test_daily_business_intelligence_guide_keeps_business_truth_boundaries -q
..                                                                       [100%]

uv run ruff check tests/m1-perception/test_makefile_contract.py tests/m1-perception/test_m1_manual_contract.py
All checks passed!
```

## Fresh delivery verification

- `uv run pytest tests/m1-perception/test_makefile_contract.py tests/m1-perception/test_m1_manual_contract.py -q` — pass.
- `make docs-m1-check` — `M1 manual contract: OK`.
- `make planning-status` — no drift; reviewed evidence hashes match current bytes.
- `git diff --check` — pass.
- `make smoke-control-plane-prod` — HTTP 200, `status=ok`,
  `control_plane=available`.
- `make control-plane-opportunities limit=5` — `status=available`,
  `current_opportunity_count=0`, empty `items`, exit 0.
- `make control-plane-status limit=20` — nonzero `QueryCanceled`; this is
  recorded as business data unavailable, not a zero-opportunity result.

`uv run pytest tests/test_makefile.py -q` has exactly one independently
pre-existing failure: `test_status_uses_the_canonical_current_state` still
expects `唯一当前状态入口`, while the maintained status text says
`唯一动态状态入口`. No delivery test or implementation was changed to conceal it.

## Records updated

- Task 1 and Task 2 summaries now include both remediation commits, root
  cause/fix, final cadence, `tests/test_makefile.py`, and this fresh evidence.
- `CURRENT.md`, M1 `STATE.md`, SDD progress, and append-only JOURNAL reflect
  the security boundary and the live status-reader failure accurately.
