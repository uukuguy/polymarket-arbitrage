# Task 7 Summary — classifier-v2 performance and operator qualification

Date: 2026-08-03

## Outcome

Classifier v2 now has a deterministic, production-shaped 120,000-market / 5,000-event deployment-qualification gate, an operator-facing Make target, and updated operating and learning contracts. The gate exercises the real old-v1 and classifier-v2 pipelines end to end. It does not fabricate source authority, include fixture construction in timing, or substitute estimated work for either numerator or denominator.

This task does **not** deploy classifier v2, switch generation reads, enable Quote, or authorize a production SHA. The deployment candidate is the exact commit containing this summary; its 40-character SHA is resolved after commit and requires a separate reviewer to emit `DEPLOY_SHA_APPROVE <SHA>`.

## Implementation

- Added `test_120k_production_shaped_complete_classifier_gate`, marked `slow`.
- Seeded exactly 120,000 markets and 5,000 events, with 24 members per group, a real global relation conflict, an independent event-only anti-join candidate, an orphan raw market, and one legacy-only row required to initialize comparison.
- Warmed each path, then recorded three actual timed samples for old v1 and classifier v2.
- Measured complete projection, classification plus diagnostic/generation-mirror work, legacy scan, terminal receipt, maximum actual rolling 100-chunk slice, and production projection SELECT count.
- Added `make classifier-v2-deploy-perf` and registered it in the M1 manual-contract surface.
- Reused normalized event evidence within one classifier chunk. Evidence still derives from the pinned raw source; the cache only removes repeated normalization of the same 24-member parent.
- Removed one redundant event-member authority SELECT by passing the already authenticated progress receipt into group-truth validation.
- Updated the operator runbook, M1 usage manual, and learning guide for v1→v2 supersession, complete market/event-only union, conflict precedence, group-ineligible semantics, immutable terminal evidence, Polywatch lifecycle, same-contract no-retry, and the two-step generation-read/Quote rollout.

## TDD and review corrections

The implementation passed through these observed RED states:

1. The new test initially failed because `_run_production_shaped_classifier_benchmark` did not exist.
2. A fabricated conflict-parent fixture failed closed with `structure-event-conflict-summary-invalid`; it was replaced by a relation between two real source events.
3. Identical exact legacy and generation roots failed initialization with `structure-drift-exact-already-matches`; the fixture gained one legitimate legacy-only row.
4. Incomplete certification counts failed with `structure-drift-source-counts-invalid`; the fixture now certifies the exact 5,000 / 120,000 counts.
5. The first complete measurement was RED: old-v1 18.136 s versus v2 54.066 s (0.335x), projection SELECT count 18, and maximum slice 11.725 s.
6. Profiling found repeated per-member normalization of the same parent event. A chunk-local raw-derived normalization cache reduced generation work from about 28.014 s to 17.463 s.
7. SQL tracing found the 18th SELECT was a duplicate `source_receipt_digest` lookup after the progress row had already authenticated it. Removing only that duplicate brought the exact budget to 17.
8. Review rejected an interim estimated-numerator approach. The final gate restores the actual old-v1 warm run plus three complete timed runs.
9. The first full-repository run found that the new documented Make target was absent from `M1_MAKE_TARGETS`. The target was registered and the failing contract test passed before the full rerun.

## Performance evidence

Standalone command:

```text
make classifier-v2-deploy-perf
1 passed, 5 deselected in 830.21s (13:50)
```

Standalone aggregate:

| Measurement | Result |
|---|---:|
| old-v1 median | 152.796017 s |
| classifier-v2 median | 45.139646 s |
| old-v1 / v2 | 3.385x |
| complete projection | 7.979122 s |
| classification + diagnostics / generation mirror | 17.836242 s |
| legacy scan | 4.460744 s |
| terminal receipt | 0.012034 s |
| maximum rolling 100-chunk slice | 7.437705 s |
| maximum projection SELECTs/call | 17 / 17 |
| projection calls | 241 |

Fresh focused sample-output run:

- old-v1 warm: 154.422985 s
- old-v1 timed: 153.971981 / 155.191916 / 156.168971 s; median 155.191916 s
- classifier-v2 warm: 44.896679 s
- classifier-v2 timed: 45.414150 / 45.631430 / 45.537621 s; median 45.537621 s
- ratio: 3.4089x
- projection: 8.296444 s
- classification + diagnostics / generation mirror: 18.026360 s
- legacy: 4.645809 s
- terminal: 0.012150 s
- maximum rolling 100-chunk slice: 7.544726 s
- projection SELECTs: 17 / 17 across 241 calls

All contractual assertions are green: ratio ≥ 2.0, maximum actual child slice < 45 seconds, and projection query count ≤ the bounded 17-query budget.

## Verification

Focused and static evidence:

- Task 7 focused collection: 555 tests.
- Task 7 non-slow focused run: 554 passed.
- Standalone long gate: 1 passed, 5 deselected.
- `uv run ruff check src tests scripts/polywatch`: all checks passed.
- `make docs-m1-check`: `M1 manual contract OK`.
- `make planning-status`: 84 plans, no drift.
- Scoped `git diff --check` for Task 7 files: clean.

Complete repository evidence:

- `uv run pytest -o addopts='' --collect-only -q`: 4,712 tests collected in 2.74 s.
- First complete run, `uv run pytest -q --junitxml=/tmp/m1-classifier-v2-full.xml`: 4,712 tests, 0 failures, 0 errors, 2 skipped-or-xfailed, 1,699.342 s; exit 0.
- Post-baseline-cleanup complete run, `uv run pytest -q --junitxml=/tmp/m1-classifier-v2-full-postfix.xml`: 4,712 tests, 0 failures, 0 errors, 2 skipped-or-xfailed, 1,879.245 s; exit 0. This second run includes the slow 120k gate and covers the final candidate worktree.
- The two non-pass entries are repository-known: one explicit streaming-memory-budget `pytest.xfail` and one Supabase-mirror-disabled `pytest.skip`.
- `uv run ruff check src tests scripts`: all checks passed.
- `make docs-m1-check`: `M1 manual contract OK`.
- `make planning-status`: 84 plans, no drift.
- Task-scoped `git diff --check`: clean.

The full Ruff deployment gate initially exposed 16 findings in seven scripts that were already present at Task 7 base `d20ecae71a618b500e5c249516b0264dcc920cf8`. Deployment qualification did not accept a baseline exception. The findings were closed mechanically: import ordering and unused imports, `datetime.UTC`, the Python 3.12 `TimeoutError` alias, non-behavioral line wrapping, and removal of an empty f-string prefix. Verification included full Ruff, `py_compile` for all seven scripts, 20 relevant cleanup/patch/Make-contract tests, CLI help for four operator scripts, and identity assertions for the UTC and timeout aliases.

The global worktree contains pre-existing shared `.superpowers/sdd/` edits that are explicitly excluded from Task 7 staging. In particular, global `git diff --check` reports only the pre-existing trailing blank line in `.superpowers/sdd/task-7-brief.md`; Task 7 does not modify or stage that shared file.

## Deployment boundary

The candidate SHA is the 40-character commit that contains this summary and all Task 7 changes. It is not self-embedded because a commit cannot contain its own final SHA. No deployment may proceed until an independent reviewer checks that exact SHA and emits the exact approval token. Task 8 remains responsible for deploy, observe, read-switch, and Quote rollout.
