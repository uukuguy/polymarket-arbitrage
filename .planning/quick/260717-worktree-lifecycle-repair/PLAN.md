# Quick Task 260717 — Worktree Lifecycle Repair

## Goal

Repair M2 ROADMAP metadata and close the Claude/GSD agent-worktree lifecycle leak without losing dirty state or unreviewed commits.

## Source of truth

- Design: `docs/superpowers/specs/2026-07-17-worktree-lifecycle-repair-design.md`
- Implementation plan: `docs/superpowers/plans/2026-07-17-worktree-lifecycle-repair.md`

## Deliverables

- M2 ROADMAP records completed Phase 2.
- Safe dry-run-first stale-worktree reaper with Makefile entry and tests.
- Idempotent patcher for installed GSD execute-phase/quick cleanup workflows.
- Existing 21 stale worktrees and their branches receive explicit, evidence-backed disposition.
- Planning status, focused tests, M2 regressions, Git registry, and disk usage are verified.

## Safety constraints

- Never recursively delete `.claude/worktrees`.
- Refuse live-PID, malformed-owner, dirty, or unexpected-path candidates.
- Refuse non-ancestor branch deletion unless its exact name is explicitly audited and supplied.
- Preserve the user's existing `CLAUDE.md` type change and untracked `AGENTS.md`.
