# Worktree Lifecycle Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair M2 planning metadata, prevent GSD from leaking or sweeping unrelated Claude agent worktrees, and safely reclaim the current 21 stale worktrees without leaving ownerless branches.

**Architecture:** A repository-owned Python reaper treats Git's worktree registry as the source of truth and defaults to dry-run. A second idempotent Python patcher hardens the installed GSD workflow text so each execution records and cleans only its own worktrees; non-ancestor branches are never silently retained or deleted. Existing stale branches are closed through explicit plan mapping and disposition.

**Tech Stack:** Python 3.12, standard library (`argparse`, `dataclasses`, `os`, `pathlib`, `re`, `subprocess`), pytest, Git worktree porcelain output, Make.

## Global Constraints

- Use `uv`; do not install dependencies with `pip`.
- Every executable command must have a documented Makefile target surfaced by `make help`.
- Never recursively delete `.claude/worktrees`; all removal goes through `git worktree remove`.
- Default cleanup mode is dry-run. Mutation requires `apply=1` / `--apply`.
- Only `.claude/worktrees/agent-*` linked worktrees in this repository are in scope.
- Live PID, malformed lock ownership, dirty worktree, unexpected path, or unresolved non-ancestor branch must block automatic cleanup.
- A non-ancestor branch may be deleted only when its exact branch name is supplied explicitly after plan/semantic audit.
- Preserve the user's existing `CLAUDE.md` type change and untracked `AGENTS.md`.
- Do not spawn subagents; implementation runs inline in the current session.

---

## File Map

- Create `scripts/cleanup_agent_worktrees.py`: inspect, classify, dry-run, and safely remove stale Claude agent worktrees.
- Create `scripts/patch_gsd_worktree_cleanup.py`: idempotently harden installed GSD `execute-phase.md` and `quick.md`.
- Create `tests/test_cleanup_agent_worktrees.py`: Git-backed integration tests for cleanup safety and branch disposition.
- Create `tests/test_patch_gsd_worktree_cleanup.py`: fixture-backed tests for GSD workflow rewriting and idempotence.
- Modify `tests/m1-perception/test_makefile_contract.py`: require both operational targets in `make help`.
- Modify `Makefile`: expose `cleanup-worktrees` and `patch-gsd-worktree-cleanup`.
- Modify `.planning/workstreams/m2-combinatorial/ROADMAP.md`: register completed Phase 2.
- Modify `.planning/workstreams/m2-combinatorial/STATE.md`: clear ROADMAP drift and record repair position.
- Modify `.planning/JOURNAL.md`: record root cause, branch mappings, cleanup evidence, and next command.
- Modify installed `/Users/sujiangwen/.codex/get-shit-done/workflows/execute-phase.md` and `quick.md` by running the tested patcher.

---

### Task 1: Repair M2 Planning Metadata

**Files:**
- Create: `.planning/quick/260717-worktree-lifecycle-repair/PLAN.md`
- Create: `.planning/quick/260717-worktree-lifecycle-repair/SUMMARY.md`
- Modify: `.planning/workstreams/m2-combinatorial/ROADMAP.md`
- Modify: `.planning/workstreams/m2-combinatorial/STATE.md`

**Interfaces:**
- Consumes: Phase facts in `02-1-SUMMARY.md`.
- Produces: A ROADMAP phase entry parseable by future GSD resume/discuss commands.

- [ ] **Step 1: Create quick-task tracking artifacts**

Create a concise PLAN referencing the approved design and this implementation plan. Create an in-progress SUMMARY with the root-cause evidence, the four branch-to-plan mappings, and a task checklist. Every later repair commit updates and includes this SUMMARY so no plan-scoped code commit exists without its retrieval anchor.

- [ ] **Step 2: Add the completed phase to ROADMAP**

Replace the placeholder phase body with:

```markdown
### Phase 2: Arbitrage Execution Engine ✅

**Goal:** Turn Type-2 cross-venue signals into slippage-aware routed executions with position lifecycle, environment-driven risk settings, CLI surfaces, and E2E failure-mode coverage.
**Status:** Complete — 2026-06-07
**Plans:** 1/1 complete

- [x] `02-1-PLAN.md` — T1-T8 signal, slippage, routing, execution, position tracking, settings, CLI, and E2E chaos coverage
```

- [ ] **Step 3: Update STATE continuity**

Record that ROADMAP drift is repaired and Phase 3 has not yet been created.

- [ ] **Step 4: Verify planning metadata**

Run: `make planning-status`

Expected: exit 0 and `no drift detected`; M2 `02-1` remains `SUMMARY ✓`.

- [ ] **Step 5: Commit metadata repair and tracking artifacts**

```bash
git add .planning/quick/260717-worktree-lifecycle-repair .planning/workstreams/m2-combinatorial/ROADMAP.md .planning/workstreams/m2-combinatorial/STATE.md
git commit -m "docs(m2): restore completed phase 2 roadmap entry"
```

---

### Task 2: Build the Safe Stale-Worktree Reaper with TDD

**Files:**
- Create: `scripts/cleanup_agent_worktrees.py`
- Create: `tests/test_cleanup_agent_worktrees.py`

**Interfaces:**
- Produces: `Worktree(path: Path, branch: str | None, locked_reason: str | None)`.
- Produces: `parse_porcelain(text: str) -> list[Worktree]`.
- Produces: `extract_pid(reason: str | None) -> int | None`.
- Produces: `cleanup(repo_root: Path, *, apply: bool, discard_unmerged: frozenset[str]) -> int`.
- CLI: `python scripts/cleanup_agent_worktrees.py [--apply] [--discard-unmerged BRANCH ...]`.

- [ ] **Step 1: Write failing parser and classification tests**

Tests must assert:

```python
def test_parse_porcelain_keeps_lock_reason_and_branch(): ...
def test_extract_pid_rejects_missing_or_malformed_reason(): ...
def test_dry_run_reports_dead_pid_clean_merged_worktree_without_mutation(tmp_path): ...
def test_live_pid_blocks_cleanup(tmp_path): ...
def test_dirty_worktree_blocks_cleanup(tmp_path): ...
def test_path_outside_agent_root_is_ignored(tmp_path): ...
def test_nonancestor_branch_blocks_without_explicit_disposition(tmp_path): ...
def test_explicit_discard_removes_nonancestor_worktree_and_branch(tmp_path): ...
def test_remove_failure_does_not_delete_branch(tmp_path, monkeypatch): ...
```

The Git-backed fixture creates a temporary repository, configures a local identity, commits `README.md`, and creates linked worktrees under `.claude/worktrees/agent-test-*`. Use `git worktree lock --reason "claude agent agent-test (pid 999999)" PATH` for dead-owner cases and `os.getpid()` for the live-owner case.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_cleanup_agent_worktrees.py -q`

Expected: collection/import failure because `scripts.cleanup_agent_worktrees` does not exist.

- [ ] **Step 3: Implement parsing and safety classification**

Implement standard-library-only code with these invariants:

```python
AGENT_DIR_RE = re.compile(r"^agent-[A-Za-z0-9_-]+$")
PID_RE = re.compile(r"\(pid (?P<pid>[1-9][0-9]*)\)")

def extract_pid(reason: str | None) -> int | None:
    if not reason:
        return None
    match = PID_RE.search(reason)
    return int(match.group("pid")) if match else None

def pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
```

Resolve both repository root and candidate path before checking `candidate.parent == repo_root / ".claude/worktrees"` and matching the directory name. Obtain dirtiness using `git -C PATH status --porcelain`. Determine ancestry with `git merge-base --is-ancestor BRANCH main`.

- [ ] **Step 4: Implement dry-run and apply behavior**

Dry-run prints one deterministic line per candidate with `REMOVE`, `BLOCK`, or `IGNORE`. Apply mode:

```python
run_git(repo_root, "worktree", "unlock", str(path), check=False)
removed = run_git(
    repo_root, "worktree", "remove", str(path), "--force", "--force", check=False
)
if removed.returncode != 0:
    errors += 1
    continue
if merged or branch in discard_unmerged:
    deleted = run_git(repo_root, "branch", "-D", branch, check=False)
    if deleted.returncode != 0:
        errors += 1
```

Run `git worktree prune` after candidate processing. Return non-zero if any candidate is blocked or any Git mutation fails.

- [ ] **Step 5: Run focused tests to verify GREEN**

Run: `uv run pytest tests/test_cleanup_agent_worktrees.py -q`

Expected: all tests pass.

- [ ] **Step 6: Commit the reaper**

Update the quick-task SUMMARY with Task 2 RED/GREEN evidence, then:

```bash
git add scripts/cleanup_agent_worktrees.py tests/test_cleanup_agent_worktrees.py .planning/quick/260717-worktree-lifecycle-repair/SUMMARY.md
git commit -m "fix(ops): add safe stale agent worktree reaper"
```

---

### Task 3: Add Makefile Operational Surfaces

**Files:**
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`

**Interfaces:**
- Consumes: cleanup CLI from Task 2.
- Produces: `make cleanup-worktrees [apply=1] [discard_unmerged="branch ..."]`.
- Produces: `make patch-gsd-worktree-cleanup [check=1]` after Task 4.

- [ ] **Step 1: Write failing Makefile contract test**

Add a test that runs `make help` and requires:

```python
assert "cleanup-worktrees:" in result.stdout
assert "patch-gsd-worktree-cleanup:" in result.stdout
```

Also inspect Makefile text for `--apply` and `--discard-unmerged` forwarding.

- [ ] **Step 2: Run test to verify RED**

Run: `uv run pytest tests/m1-perception/test_makefile_contract.py -q`

Expected: FAIL because the two targets are missing.

- [ ] **Step 3: Add Makefile targets**

Add under Project state:

```make
## cleanup-worktrees: Dry-run stale Claude agent worktree cleanup; use apply=1 and audited discard_unmerged="branch ..." to mutate
cleanup-worktrees:
	@uv run python scripts/cleanup_agent_worktrees.py $(if $(apply),--apply,) $(foreach branch,$(discard_unmerged),--discard-unmerged $(branch))

## patch-gsd-worktree-cleanup: Harden installed GSD worktree lifecycle; use check=1 for verification only
patch-gsd-worktree-cleanup:
	@uv run python scripts/patch_gsd_worktree_cleanup.py $(if $(check),--check,)

.PHONY: cleanup-worktrees patch-gsd-worktree-cleanup
```

- [ ] **Step 4: Run contract test to verify GREEN**

Run: `uv run pytest tests/m1-perception/test_makefile_contract.py -q`

Expected: all Makefile contract tests pass.

- [ ] **Step 5: Commit Makefile surface**

Commit together with Task 4 so the patch target never points at a missing script.

---

### Task 4: Harden Installed GSD Workflow Cleanup with TDD

**Files:**
- Create: `scripts/patch_gsd_worktree_cleanup.py`
- Create: `tests/test_patch_gsd_worktree_cleanup.py`
- Modify: `/Users/sujiangwen/.codex/get-shit-done/workflows/execute-phase.md` via tested patcher
- Modify: `/Users/sujiangwen/.codex/get-shit-done/workflows/quick.md` via tested patcher

**Interfaces:**
- Produces: `rewrite_execute_phase(text: str) -> str`.
- Produces: `rewrite_quick(text: str) -> str`.
- CLI: `python scripts/patch_gsd_worktree_cleanup.py [--root PATH] [--check]`.
- Marker: `GSD_WORKTREE_CLEANUP_V2` appears exactly once in each patched workflow.

- [ ] **Step 1: Write failing rewrite tests**

Fixture excerpts must contain the current broad enumeration and single-force removal. Tests assert rewritten text:

```python
assert "GSD_WORKTREE_CLEANUP_V2" in rewritten
assert "comm -13" in rewritten
assert 'git worktree remove "$WT" --force --force' in rewritten
assert 'git branch -D "$WT_BRANCH"' in rewritten
assert "if git worktree remove" in rewritten
assert rewrite_execute_phase(rewritten) == rewritten
```

Also assert an unknown upstream shape raises `PatchShapeError` instead of partially rewriting.

- [ ] **Step 2: Run tests to verify RED**

Run: `uv run pytest tests/test_patch_gsd_worktree_cleanup.py -q`

Expected: import failure because the patcher does not exist.

- [ ] **Step 3: Implement idempotent workflow rewriting**

For execute-phase, insert a snapshot command before executor dispatch using a deterministic file under the Git common directory keyed by phase and wave. For quick, key it by quick task ID. Cleanup computes only newly-created worktrees:

```bash
# GSD_WORKTREE_CLEANUP_V2: only worktrees created by this execution
CURRENT_WORKTREES_FILE=$(mktemp)
git worktree list --porcelain | sed -n 's/^worktree //p' | sort > "$CURRENT_WORKTREES_FILE"
WORKTREES=$(comm -13 "$PRE_WORKTREES_FILE" "$CURRENT_WORKTREES_FILE")
rm -f "$CURRENT_WORKTREES_FILE" "$PRE_WORKTREES_FILE"
```

Replace removal/deletion with a conditional block:

```bash
git worktree unlock "$WT" 2>/dev/null || true
if git worktree remove "$WT" --force --force; then
  if git merge-base --is-ancestor "$WT_BRANCH" HEAD; then
    git branch -D "$WT_BRANCH"
  else
    echo "BLOCKED: removed worktree but retained non-ancestor branch $WT_BRANCH"
  fi
else
  echo "ERROR: failed to remove executor worktree $WT" >&2
  CLEANUP_FAILED=1
fi
```

Exit/report non-zero when `CLEANUP_FAILED=1`. Refuse an unexpected upstream file shape.

- [ ] **Step 4: Run focused tests to verify GREEN**

Run: `uv run pytest tests/test_patch_gsd_worktree_cleanup.py -q`

Expected: all tests pass.

- [ ] **Step 5: Apply and check installed workflows**

Run:

```bash
make patch-gsd-worktree-cleanup
make patch-gsd-worktree-cleanup check=1
```

Expected: first command reports both workflows patched; second exits 0 and reports both current.

- [ ] **Step 6: Commit repository-owned patcher and Makefile surface**

Update the quick-task SUMMARY with Makefile and GSD patcher verification, then:

```bash
git add scripts/patch_gsd_worktree_cleanup.py tests/test_patch_gsd_worktree_cleanup.py Makefile tests/m1-perception/test_makefile_contract.py .planning/quick/260717-worktree-lifecycle-repair/SUMMARY.md
git commit -m "fix(gsd): close agent worktree cleanup lifecycle"
```

---

### Task 5: Reclaim Existing Stale Worktrees with Explicit Disposition

**Files:**
- Modify: `.planning/JOURNAL.md`

**Interfaces:**
- Consumes: cleanup target from Task 3.
- Produces: one main worktree only; no `worktree-agent-*` branches.

- [ ] **Step 1: Run default dry-run**

Run: `make cleanup-worktrees`

Expected: 17 ancestor worktrees marked removable; four non-ancestor branches marked `BLOCK`; exit non-zero is expected because disposition is missing.

- [ ] **Step 2: Re-run dry-run with audited dispositions**

Run:

```bash
make cleanup-worktrees discard_unmerged="worktree-agent-a5baf083147d7fbd1 worktree-agent-ad1bbb460f1783335 worktree-agent-ae50719ed3168dc18 worktree-agent-afebf7618612e84c8"
```

Expected: all 21 candidates marked removable; the four explicit branches are labeled `DISCARD-AUDITED`.

- [ ] **Step 3: Apply cleanup**

Run the same command with `apply=1`.

Expected: 21 worktrees removed, 21 temporary branches deleted, `git worktree prune` succeeds.

- [ ] **Step 4: Verify physical and registry cleanup**

Run:

```bash
git worktree list --porcelain
git for-each-ref --format='%(refname:short)' 'refs/heads/worktree-agent-*'
du -sh .claude/worktrees
```

Expected: only the main worktree; no worktree-agent branches; empty/near-empty directory.

- [ ] **Step 5: Record evidence in JOURNAL**

Append root cause, before/after disk usage, GSD patch status, plan mapping for the four duplicate branches, and the exact next command.

---

### Task 6: Final Verification and Planning Closure

**Files:**
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/workstreams/m2-combinatorial/STATE.md`

**Interfaces:**
- Produces: verified repository health and resumable next action.

- [ ] **Step 1: Run focused tests**

```bash
uv run pytest tests/test_cleanup_agent_worktrees.py tests/test_patch_gsd_worktree_cleanup.py tests/m1-perception/test_makefile_contract.py -q
```

Expected: all pass.

- [ ] **Step 2: Run broader regression and planning gates**

```bash
make planning-status
uv run pytest tests/routing tests/execution tests/cli/test_arbitrage_cli.py -q
```

Expected: planning zero drift and M2 regression green.

- [ ] **Step 3: Verify installed GSD patch and worktree state**

```bash
make patch-gsd-worktree-cleanup check=1
git worktree list --porcelain
git status --short --branch
```

Expected: patch current; only main worktree; only known user changes plus this repair's planning documentation before final commit.

- [ ] **Step 4: Finalize repair SUMMARY and commit planning closure**

Finalize `.planning/quick/260717-worktree-lifecycle-repair/SUMMARY.md` with all verification evidence, then commit JOURNAL, STATE, ROADMAP, PLAN, and SUMMARY without touching `CLAUDE.md` or `AGENTS.md`.

- [ ] **Step 5: Re-run final gates after commit**

Run: `make planning-status && git status --short --branch`

Expected: zero drift; branch status accurately reflects only pre-existing user changes.
