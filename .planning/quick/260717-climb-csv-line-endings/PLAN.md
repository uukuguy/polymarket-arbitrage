# Quick 260717 — Climb CSV Line Endings

## Goal

Make climb cycle synchronization write deterministic LF-only `runs.csv` so
generated evidence passes repository whitespace gates on every platform.

## Root Cause

`csv.DictWriter` defaults to the Excel dialect's `\r\n` line terminator.
Opening with `newline=""` prevents translation but does not select LF, so each
new run appears as trailing whitespace to `git diff --check`.

## Scope

- RED regression that inspects raw `runs.csv` bytes after one cycle;
- atomic full-file rewrite with `lineterminator="\n"`, preserving append-only
  row semantics and duplicate-ID rejection;
- normalize the already-generated H-001/H-002 evidence file;
- no change to scoring, verdicts, hypotheses, or external behavior.

## Verification

- `uv run pytest tests/climb -q`
- `git diff --check`
- `make climb-status`
