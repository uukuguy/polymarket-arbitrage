# M1 Startup WAL Idempotence Repair

`init_schema()` no longer replays `PRAGMA journal_mode=WAL` through shared DDL
on every boot. It checks the persistent current mode and sets WAL only for a
fresh/non-WAL database. This preserves the first-boot contract while avoiding
large-volume journal coordination on ordinary restarts.
