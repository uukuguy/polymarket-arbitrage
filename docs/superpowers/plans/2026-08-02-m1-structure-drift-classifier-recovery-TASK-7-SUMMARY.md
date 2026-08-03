# Task 7 Summary — classifier-v2 performance and operator qualification

Date: 2026-08-03

## Outcome

Classifier v2 now has a deterministic, production-shaped 120,000-market / 5,000-event deployment-qualification gate, an operator-facing Make target, and updated operating and learning contracts. The gate exercises the real old-v1 and classifier-v2 pipelines end to end. It does not fabricate source authority, include fixture construction in timing, or substitute estimated work for either numerator or denominator.

The final candidate also includes the deploy-review repair for invalid terminal/member receipt authority. These failures now become durable resident incidents without publishing untrusted comparison evidence, and recover only after a strictly later authenticated current-v2 safe seal.

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

### Deploy review rejection and recovery

Independent review rejected candidate `8147f49c1a5c0a0f734647176419ce3bf0e9943d` with `DEPLOY_SHA_REJECT`. The blocking finding was an incomplete invalid-authority lifecycle: `/health` and `/healthz` failed closed when a classifier terminal/member receipt was invalid, but their constant public shape did not retain a trusted discriminator that Polywatch could use to create, deduplicate, remind, persist across restart, and later recover the resident incident. Treating that failure as generic unhealthy state either lost the classifier incident identity or allowed recovery without proof of a later valid classifier-v2 seal.

Follow-up `a43d7d3d8402ea258c2acc810e9d3a61e7a99459` (`fix(m1): recover invalid drift authority incidents`) closes that chain:

- Health exposes only the allow-listed `authorityError` (`structure-drift-terminal-receipt-invalid` or `structure-drift-member-receipt-invalid`) while continuing to withhold untrusted comparison, checkpoint, diagnostic count, and sample fields.
- Polywatch derives a stable fingerprint from the trusted authority error and current classifier contract, records a local detection boundary, persists it, and suppresses repeated identical alerts while retaining reminders and failed-delivery retry.
- Repeated detections advance the required recovery boundary without changing incident identity. Recovery requires a strictly later nonempty, receipt-authenticated, current-v2 `drift-safe-sealed` check; disabled, exact, incomplete, wrong-contract, and non-later states cannot clear it.
- The same incident path produces an explicit Telegram authority-invalid alert and an authenticated recovery notification.

Fixer evidence was watcher 67/67, focused invalid-authority 4/4, focused `/health` + `/healthz` 4/4, full health plus Structure-drift end-to-end regression, Ruff, docs, scoped diff, and 84-plan planning status. Learning document 47 references were also corrected to the current store entries at lines 7026, 8213, 7686, and 8640.

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

- Historical pre-fixer collection: 4,712 tests in 2.74 s.
- Final post-`a43d7d3` collection: 4,720 tests in 2.88 s.
- Historical pre-review run, `uv run pytest -q --junitxml=/tmp/m1-classifier-v2-full.xml`: 4,712 tests, 0 failures, 0 errors, 2 skipped-or-xfailed, 1,699.342 s; exit 0.
- Historical pre-review post-baseline-cleanup run, `uv run pytest -q --junitxml=/tmp/m1-classifier-v2-full-postfix.xml`: 4,712 tests, 0 failures, 0 errors, 2 skipped-or-xfailed, 1,879.245 s; exit 0. This included the slow 120k gate but predates the deploy-review authority-lifecycle fix and is not used as final-candidate proof.
- First final-tree run, `uv run pytest -q --junitxml=/tmp/m1-classifier-v2-full-final.xml`: 4,720 tests, 1 failure, 0 errors, 2 skipped-or-xfailed, 1,762.224 s. The 120k gate passed; the sole RED was the unrelated strict-wall-clock `test_more_stalled_highs_than_workers_cannot_delay_reserved_lanes_to_bound`, which observed 62.4 ms against a 50 ms assertion immediately after the resource-heavy slow gate.
- The exact failed scheduler test then passed 10/10 isolated repetitions without source or test changes. This is diagnostic evidence only, not a release exception; a new complete run in this summary-bearing tree must still finish with zero failures and errors.
- Zero-failure diagnostic rerun before root-cause repair, `uv run pytest -q --junitxml=/tmp/m1-classifier-v2-full-final-rerun.xml`: 4,720 tests, 0 failures, 0 errors, 2 skipped-or-xfailed, 1,675.543 s. Passing this rerun was not accepted as root-cause closure.
- Root cause was a process-wide generation-2 cyclic-GC pause overlapping a test intended to measure the scheduler's algorithmic steady-state lane budget. In one process, 100 ordinary exact repetitions produced median 37.133 ms, p95 41.831 ms, max 42.803 ms, and only two generation-0 overlaps. cProfile measured the scheduling snapshot at about 2 ms, while five isolated `gc.collect` calls accumulated 108 ms (about 21.6 ms each). Forcing generation-2 collection 5 ms into the measured window reproduced the failure 3/3 at 56.432 / 62.028 / 62.904 ms, matching the full-suite RED at 62.398 ms.
- The test now collects inherited cyclic garbage before starting its timer, pauses automatic GC only for the measured scheduler cycle, and restores the caller's original GC-enabled state in `finally`. The 50 ms assertion is unchanged; production scheduler and production GC behavior are unchanged. This gate explicitly measures steady-state algorithmic delivery, while the existing watcher delivery/lifecycle tests retain production correctness coverage.
- Post-fix candidate-watcher file: 37/37 passed. Post-fix same-process 640 MB RSS sequence: 100/100 passed, median 38.409 ms, p95 43.296 ms, max 44.456 ms.
- Final post-GC-isolation candidate run, `uv run pytest -q --junitxml=/tmp/m1-classifier-v2-full-final-gcfix.xml`: 4,720 tests, 0 failures, 0 errors, 2 skipped-or-xfailed, 1,622.386 s; exit 0. The slow 120k gate and the repaired scheduler timing gate both passed in the same final worktree.
- The two non-pass entries are repository-known: one explicit streaming-memory-budget `pytest.xfail` and one Supabase-mirror-disabled `pytest.skip`.
- `uv run ruff check src tests scripts`: all checks passed.
- `make docs-m1-check`: `M1 manual contract OK`.
- `make planning-status`: 84 plans, no drift.
- Task-scoped `git diff --check`: clean.

The full Ruff deployment gate initially exposed 16 findings in seven scripts that were already present at Task 7 base `d20ecae71a618b500e5c249516b0264dcc920cf8`. Deployment qualification did not accept a baseline exception. The findings were closed mechanically: import ordering and unused imports, `datetime.UTC`, the Python 3.12 `TimeoutError` alias, non-behavioral line wrapping, and removal of an empty f-string prefix. Verification included full Ruff, `py_compile` for all seven scripts, 20 relevant cleanup/patch/Make-contract tests, CLI help for four operator scripts, and identity assertions for the UTC and timeout aliases.

The global worktree contains pre-existing shared `.superpowers/sdd/` edits that are explicitly excluded from Task 7 staging. In particular, global `git diff --check` reports only the pre-existing trailing blank line in `.superpowers/sdd/task-7-brief.md`; Task 7 does not modify or stage that shared file.

## Deployment boundary

The candidate SHA is the 40-character commit that contains this summary and all Task 7 changes. It is not self-embedded because a commit cannot contain its own final SHA. No deployment may proceed until an independent reviewer checks that exact SHA and emits the exact approval token. Task 8 remains responsible for deploy, observe, read-switch, and Quote rollout.
