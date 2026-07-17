# Quick 260717 — Climb CSV Line Endings Summary

## Status

Complete.

## Root Cause

Python `csv.DictWriter` uses the Excel dialect's `\r\n` terminator by default.
The climb cycle opened `runs.csv` with `newline=""`, which preserves that CRLF
rather than converting it. Every newly generated run therefore appeared as
trailing whitespace to `git diff --check`.

## Commits

- `247a7e6` — quick repair plan and bounded scope.
- `d384cf6` — RED raw-byte regression rejecting CRLF evidence.
- `56b7e00` — atomic LF-only full-file rewrite, existing evidence normalization,
  and state-machine test support for the valid no-pending terminal state.

## Evidence

- `uv run pytest tests/climb -q` — **16 passed**.
- Ruff on cycle and modified climb tests — all checks passed.
- `git diff --check` — clean after normalizing tracked H-001/H-002 evidence.
- `make climb-status` — cycle 2, H-001/H-002 confirmed, no in-flight run,
  next action `rank next pending hypothesis`.

## Behavior Preserved

- Run rows remain append-only by unique `run_id`; duplicate rejection is unchanged.
- Hypothesis scores, verdicts, session progression, and deterministic research
  tree generation are unchanged.
- No external leaderboard, AI, deployment, venue, or credential action was added.
