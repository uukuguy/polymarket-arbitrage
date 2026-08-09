# M1 Isolated SQLite Volume Compaction — Task 1 Summary

Date: 2026-08-10

## Delivered

Added `polyarb.ops.sqlite_volume_backup`, a deliberately local-only primitive
for making a consistent SQLite backup artifact. It opens the source database
read-only, writes through an exclusive sibling partial file, validates
`PRAGMA integrity_check`, measures page facts, calculates SHA-256, then
atomically publishes the requested destination.

## Safety contract

- No Fly, R2, traffic, volume, `VACUUM`, checkpoint, or source-write action is
  present in this task.
- An existing destination, stale partial file, invalid source, missing parent,
  self-copy, or invalid page count is refused rather than overwritten.
- A changing WAL source does not need to have the same digest as its backup;
  the completed immutable backup artifact is the object that is integrity- and
  digest-verified.
- A failed backup removes only its private partial file and never replaces the
  requested destination.

## Verification

- RED: the test suite initially failed because `polyarb.ops` did not exist.
- GREEN: four focused tests pass, including a WAL writer invoked during a
  one-page-at-a-time online backup, independent readability, existing
  destination refusal, and missing-source refusal.
- `uv run ruff check src/polyarb/ops tests/ops` and scoped `git diff --check`
  pass.

## Next

Task 2 can add a separately tested R2 transfer/restore receipt. It must keep
this primitive offline by default and retain the no-overwrite contract.
