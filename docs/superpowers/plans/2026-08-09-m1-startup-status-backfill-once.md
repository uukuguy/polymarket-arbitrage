# M1 Startup Status Backfill Once

`snapshot_status` is an additive schema field. Its historical Structure status
projection is required only when that field is first added. Ordinary daemon
boots must not execute the joined, ordered historical query against the
production `snapshots` table.

## Verification

- Existing-schema boot emits no status-backfill query.
- A legacy database that gains `snapshot_status` still derives its Layer-1
  status correctly.
- Building Structure publications remain protected from legacy backfill.
