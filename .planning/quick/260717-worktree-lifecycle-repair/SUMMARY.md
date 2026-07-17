# Quick Task 260717 — Worktree Lifecycle Repair Summary

> **Status:** ✅ COMPLETE
> **Started:** 2026-07-17
> **Completed:** 2026-07-17

## Root cause

- 21 Claude agent worktrees occupied 7.4 GB under `.claude/worktrees`.
- All lock-owner PIDs (`29192`, `51232`, `57877`, `62636`) were dead; no open-file holders remained.
- GSD cleanup enumerated every linked worktree, not only the current execution's worktrees.
- Cleanup used one `--force`; Git requires `--force --force` for locked worktrees.
- Removal errors were hidden by `2>/dev/null || true`, so failed cleanup appeared successful.

## Non-ancestor branch audit

| Branch | Plan owner | Disposition evidence |
|---|---|---|
| `worktree-agent-a5baf083147d7fbd1` | `03.1-02` | Main SUMMARY, commits, source surfaces, and tests exist |
| `worktree-agent-ad1bbb460f1783335` | `03.1-04` | Main SUMMARY, commits, source surfaces, and tests exist |
| `worktree-agent-ae50719ed3168dc18` | `03.1-06` | Main SUMMARY, commits, source surfaces, and tests exist |
| `worktree-agent-afebf7618612e84c8` | `03-01` | Main SUMMARY, commits, source surfaces, and tests exist |

These are duplicate executor lineages for completed plans, not unplanned work. Final deletion still requires the explicit audited-discard CLI argument and passing focused tests.

## Progress

- [x] Root cause and safety audit
- [x] Approved design and implementation plan
- [x] M2 ROADMAP Phase 2 metadata restored
- [x] Safe reaper RED/GREEN — 12 tests pass; dry-run default, PID/dirty/path/ancestry gates, explicit audited discard
- [x] Makefile operational surface — contract RED observed, target/help contract now GREEN (commit deferred with patcher so no dangling target)
- [x] GSD workflow patcher RED/GREEN and installed patch — 5 tests pass; actual-format regression added after shape gate caught execute-phase indentation; installed execute/quick both CURRENT
- [x] 21-worktree cleanup — 17 ancestry-proven branches + 4 explicitly audited duplicate lineages removed
- [x] Final verification and JOURNAL closure

## Delivered

- `scripts/cleanup_agent_worktrees.py` — Git-registry-driven, dry-run-first cleanup with strict path, PID, dirty-state, ancestry, and explicit-disposition gates.
- `make cleanup-worktrees [apply=1] [discard_unmerged="..."]` — unified operational entry point.
- `scripts/patch_gsd_worktree_cleanup.py` — idempotent, shape-checked installed-workflow patcher.
- `make patch-gsd-worktree-cleanup [check=1]` — apply/check entry point.
- Installed GSD `execute-phase.md` and `quick.md` now snapshot pre-execution worktrees, process only newly-created paths, use double-force locked removal, report failures, and gate branch deletion on ancestry.
- M2 ROADMAP now records completed Phase 2 consistently with STATE and SUMMARY.

## Verification evidence

- `make planning-status` — exit 0, zero shipped-plan drift.
- Reaper tests — 12 passed, including real temporary Git repositories/worktrees.
- GSD patcher tests — 5 passed, including idempotence and unknown-upstream refusal.
- Full Makefile contract suite — passed.
- M2 routing/execution/arbitrage CLI regression suite — passed.
- `make patch-gsd-worktree-cleanup check=1` — both installed workflows `CURRENT`.
- `git worktree list --porcelain` — main worktree only.
- `git for-each-ref ... refs/heads/worktree-agent-*` — no remaining temporary branches.
- `.claude/worktrees` — 7.4 GB before, 0 B after.

## Commits

- `56dabdd` — approved repair design
- `ab86d09` — implementation plan
- `37c1da5` — M2 ROADMAP restoration + quick task anchors
- `6118171` — safe stale-worktree reaper
- `c1dcbac` — GSD lifecycle patcher + Makefile surfaces

## Deviation caught by the safety gate

The first installed-workflow patch attempt was refused because the audited test fixture omitted the three-space Markdown indentation used by the real `execute-phase.md` code fence. No installed file changed. A RED regression fixture was added, the patcher learned the execute/quick formatting distinction, all tests passed, and only then were both installed workflows patched.
