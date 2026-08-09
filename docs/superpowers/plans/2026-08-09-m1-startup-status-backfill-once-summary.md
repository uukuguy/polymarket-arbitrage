# M1 Startup Status Backfill Once Summary

Production release `9e172b3…` confirmed that startup still blocked on SQLite
I/O after WAL idempotence: process 662 was in disk sleep while writing the
38.7 GB `state.db`. The remaining recurring path was the legacy
`snapshot_status` projection query, which scanned historical Structure
snapshots on every boot even when no status column migration was occurring.

The column helper now reports whether it added a column, and the historical
projection runs only when `snapshot_status` was newly added. The regression
test asserts that a current-schema reinitialization does not issue the
historical joined query; legacy migration and active-publication protection
continue to pass.
