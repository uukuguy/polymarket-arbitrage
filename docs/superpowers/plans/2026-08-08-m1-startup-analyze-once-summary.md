# M1 Startup Statistics Repair Summary

The production daemon was blocked in disk sleep during boot because
`init_schema()` ran `ANALYZE` against two multi-gigabyte drift indexes every
time. The repair keeps the one-time statistics build for new/rebuilt indexes,
then recognizes persisted `sqlite_stat1` rows and skips the full rescans on
ordinary restarts.

Evidence: production already has statistics for both indexes; the focused test
was RED before the change and GREEN after it. Schema-lockstep and health suites
plus Ruff and planning audit passed.
