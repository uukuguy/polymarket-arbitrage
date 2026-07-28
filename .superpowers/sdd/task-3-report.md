# Task 3 Implementer Report

Status: DONE

## Scope

Implemented only rollout Task 3: bounded Discovery, real group certification,
promotion, priority, rolling coverage, candidate-source composition, default-off
daemon wiring, and the local read-only status command. No Reconciliation,
incident system, public API/Dashboard, deployment, production enablement, or
trading behavior was added.

## RED → GREEN

- Gamma page RED: `GammaClient` lacked `fetch_active_event_page`.
- Priority/Discovery RED: modules and `EventPage` did not exist.
- Status RED: `polyarb.cli_discovery` did not exist.
- Scheduler integration RED: first Candidate work ignored Discovery score and
  sorted lexical IDs.
- Duplicate-group RED: one page could write the same group twice.
- Final Task 3 focused tests: 20 passed.
- Task 1/2, Gamma streaming, routing, daemon and legacy proportional regression:
  241 passed.

## Files

- `src/polyarb/clients/gamma_client.py`
- `src/polyarb/perception/priority.py`
- `src/polyarb/perception/discovery.py`
- `src/polyarb/perception/store.py`
- `src/polyarb/perception/candidate_watcher.py`
- `src/polyarb/storage/schemas.py`
- `src/polyarb/config.py`
- `src/polyarb/daemon/main.py`
- `src/polyarb/cli_discovery.py`
- `Makefile`
- `tests/clients/test_gamma_discovery_page.py`
- `tests/perception/test_priority.py`
- `tests/perception/test_discovery.py`
- `tests/perception/test_discovery_status.py`
- `docs/learning/32-bounded-discovery.md`
- `docs/learning/00-INDEX.md`
- `docs/M1-市场感知平台使用手册.md`
- `docs/superpowers/plans/2026-07-28-m1-opportunity-first-rollout-TASK-3-SUMMARY.md`

## Verification

- Task 3 focused: pass.
- Task 1/2 and legacy proportional regression: pass.
- Valid fixture `make perception-discovery-status`: exit 0 with bounded JSON.
- `make docs-m1-check`: `M1 manual contract: OK`.
- Changed-file Ruff: pass.
- `git diff --check`: pass.

## Self-review / Concerns

- The bounded event page may receive a structurally supported group whose nested
  markets omit condition/token identity. That group is persisted as
  `incomplete-source` and not promoted; no identity is fabricated.
- `EventPage` is a frozen dataclass, while its projected payload dictionaries
  remain ordinary mappings for compatibility with the existing normalizers.
- Rolling coverage denominator is the current known schedule, not an unknowable
  true-universe count. Documentation and output deliberately call it
  active-known/statistical coverage.
- Feature remains dark. Production rollout and resource qualification belong to
  later tasks.
