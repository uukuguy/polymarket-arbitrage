# M1 Startup Status Backfill Repair Summary

Production boot was still blocked after the index-statistics fix because
`_backfill_structure_snapshot_statuses` scanned all historical validation
payloads and issued updates for settled snapshots. The repaired candidate query
preserves legacy correction semantics while making ordinary restarts bounded.
The migration suite proves a settled Structure snapshot receives no repeat
status update.
