# L1 Storage Recovery

## Goal

Restore fresh production snapshots after a 39-day outage and prevent the mounted
SQLite volume from filling silently again.

## Tasks

- [x] Capture authenticated Fly logs and identify the first actionable exception.
- [x] Inspect the exact mounted volume and SQLite allocation read-only.
- [x] Extend the exact L1 volume from 5GB to 15GB for transactional recovery room.
- [x] Run snapshot retention from the app process that owns the volume.
- [x] Preserve the original SQLite failure when rollback is no longer active.
- [x] Verify fresh production snapshot, focused tests, Ruff, and planning status.
