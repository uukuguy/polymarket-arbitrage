# Worktree Lifecycle Repair Design

Date: 2026-07-17

## Goal

Repair the M2 roadmap metadata drift and make Claude/GSD agent worktree cleanup safe, observable, and repeatable without sacrificing parallel worktree isolation.

Success means:

- M2 `ROADMAP.md` records the already-completed Phase 2 before new work begins.
- GSD cleanup only touches worktrees created by the current execution.
- Locked worktrees can be removed after their agent has returned.
- Cleanup failures are visible rather than silently ignored.
- Cross-session cleanup can recover stale worktrees whose owner PID is dead.
- Dirty worktrees and unmerged branches never lose data.
- All repository commands have Makefile entry points.

## Evidence and Root Cause

The repository currently has 21 linked worktrees under `.claude/worktrees`, consuming 7.4 GB. Their lock files refer to PIDs `29192`, `51232`, `57877`, and `62636`; all four PIDs are dead and no open-file holder was found.

GSD's `execute-phase.md` and `quick.md` cleanup blocks currently:

1. enumerate every linked worktree other than the current directory;
2. run `git worktree remove "$WT" --force 2>/dev/null || true`; and
3. delete the associated branch regardless of whether removal succeeded.

Git requires `--force` twice to remove a locked worktree. The single-force removal therefore fails, and stderr plus the exit status are discarded. Because cleanup enumerates all linked worktrees, a later run may also merge or remove worktrees owned by an earlier task.

Safety audit results:

- 17 worktree branches are ancestors of `main` and may be removed after their directories are removed.
- Four branches contain commits not represented as ancestors of `main`, but each is a duplicate executor lineage for a completed plan rather than unplanned work:

| Stale branch | Plan owner | Main evidence |
|---|---|---|
| `worktree-agent-a5baf083147d7fbd1` | `03.1-02` | Main commits `742d662` through `be8eb73`; SUMMARY and all functional surfaces exist |
| `worktree-agent-ad1bbb460f1783335` | `03.1-04` | Main commits `a714529` through `934a1a4`; SUMMARY and all functional surfaces exist |
| `worktree-agent-ae50719ed3168dc18` | `03.1-06` | Main commits `da1ca37` through `76fedb6`; three commits are patch-equivalent and remaining surfaces exist |
| `worktree-agent-afebf7618612e84c8` | `03-01` | Main commits `cd8aa8d` through `cb89c28`; three commits are patch-equivalent and remaining surfaces exist |

- All 21 working directories are clean. The four non-ancestor branches require explicit semantic audit and disposition; they must not be retained indefinitely as ownerless islands.

## Design

### 1. Repair M2 roadmap metadata

Update `.planning/workstreams/m2-combinatorial/ROADMAP.md` to record Phase 2 (`02-arbitrage-engine`) as complete, including its goal, plan count, and completion date. Do not create Phase 3 in this repair: position persistence still needs its own discuss/plan decision after repository health is restored.

### 2. Harden GSD lifecycle instructions

Patch the installed GSD `execute-phase.md` and `quick.md` workflows:

- Capture the set of linked worktrees before spawning executor agents.
- After agents return, compute the new worktrees created by that execution and process only that set.
- Do not merge, remove, or delete branches belonging to pre-existing worktrees.
- After a Task has returned, unlock its worktree and remove it with `git worktree remove --force --force`.
- Treat failed removal as a visible cleanup failure and retain the branch.
- Delete a temporary branch only after worktree removal succeeds and the branch is confirmed merged into the current branch.

These changes address the producing workflow. They do not assume that every session reaches normal cleanup, so a cross-session reaper remains necessary.

### 3. Add a repository-owned stale-worktree reaper

Add a Python script under `scripts/` and expose it through `make cleanup-worktrees`.

The script will:

1. Read `git worktree list --porcelain` rather than walking arbitrary directories.
2. Consider only linked paths located under the repository's `.claude/worktrees/agent-*` directory.
3. Parse the lock reason for a PID when present.
4. Refuse cleanup when the owner PID is alive, PID status cannot be established safely, the worktree is dirty, or the path is outside the expected root.
5. In dry-run mode, report the exact proposed action without mutation.
6. In apply mode, unlock and remove eligible worktrees using Git commands, never filesystem recursion.
7. Delete the associated branch automatically only when it is an ancestor of `main`.
8. Refuse cleanup and return non-zero for every non-ancestor branch. The operator must map it to a plan and explicitly choose recovery or deletion after semantic audit; merely preserving it is not considered successful cleanup.
9. Run `git worktree prune` only after linked worktree removals finish.

The Makefile target defaults to dry-run. Mutation requires `apply=1`:

```text
make cleanup-worktrees
make cleanup-worktrees apply=1
```

### 4. Reclaim the current stale worktrees

Run dry-run first and compare the result with the safety audit. Then run apply mode for the 17 ancestor branches. For the four non-ancestor branches, verify the plan mapping, corresponding main commits, SUMMARY, functional surfaces, and focused tests. Because all four are duplicate executor lineages for already-completed plans, explicitly remove their linked directories and branches after that evidence is recorded.

Expected result:

- All 21 stale linked directories are removed.
- All 21 temporary branches are deleted: 17 by ancestry proof and four by explicit duplicate-lineage audit.
- `.claude/worktrees` drops from approximately 7.4 GB to an empty or near-empty directory.

No unresolved branch is carried forward without a plan owner and explicit disposition.

## Error Handling

- Git command failures are returned to the caller and summarized by worktree.
- One unsafe or failed worktree does not authorize broader deletion; it remains untouched.
- Missing paths are delegated to `git worktree prune` only after registry checks.
- Malformed lock reasons are treated as unknown ownership and refused, not assumed stale.
- The main worktree can never match the allowed `.claude/worktrees/agent-*` path predicate.

## Testing

Use test-driven development for the repository reaper:

- RED: dry-run classifies dead-PID clean worktrees as removable without mutation.
- RED: live-PID worktrees are refused.
- RED: dirty worktrees are refused.
- RED: paths outside `.claude/worktrees/agent-*` are refused.
- RED: merged branches are marked deletable only after successful worktree removal.
- RED: non-ancestor branches block automatic cleanup and return non-zero.
- RED: removal failures surface as non-zero results and do not delete branches.
- GREEN: implement the minimum behavior required by these tests.

Verification will include the focused tests, the relevant broader test suite, `make planning-status`, GSD workflow text checks, a cleanup dry-run, apply-mode output, `git worktree list`, branch-disposition checks, disk usage, and final `git status` review.

## Documentation and Operational Surface

- Add the Makefile target to `make help` with a concise safety description.
- Record the root cause, reclaimed space, four duplicate-lineage plan mappings, explicit branch dispositions, and verification evidence in `.planning/JOURNAL.md`.
- Update M2 `STATE.md` after the ROADMAP repair.
- Do not alter the user's existing `CLAUDE.md` type change or untracked `AGENTS.md` unless separately requested.

## Non-goals

- Merging stale executor branches whose completed plan already landed through another lineage.
- Disabling worktree isolation globally.
- Cleaning unrelated worktrees outside this repository.
- Introducing a generic process manager or background daemon.
