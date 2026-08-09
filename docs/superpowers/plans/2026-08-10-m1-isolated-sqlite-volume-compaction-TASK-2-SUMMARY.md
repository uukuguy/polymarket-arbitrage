# M1 Isolated SQLite Volume Compaction — Task 2 Summary

## Delivered

Added the R2 transfer and restore verifier of the isolated SQLite recovery path. A
verified local backup is uploaded under its SHA-256 content-addressed key;
the object is then HEAD-verified for exact size and digest metadata before the
JSON manifest is written. No Fly volume, routing, live SQLite, or promotion
operation is included. Restore accepts only the expected content-addressed
object key, writes an exclusive sibling partial file, and atomically publishes
only after digest, page facts, and SQLite integrity agree with the manifest.

`make sqlite-volume-backup source=<path> destination=<new-path>` and
`make sqlite-volume-restore-verify object_key=<key> manifest=<path>
destination=<new-path>` expose the two offline-only operations.

## Verification

`uv run pytest tests/ops/test_sqlite_volume_backup.py -q`

Result: `6 passed`.

Ruff and diff checks passed after formatting.

## Next

Add deterministic manifest-file emission to the backup command, then test the
CLI's complete local round-trip with a stubbed R2 client.
