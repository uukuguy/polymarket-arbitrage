# M1 Startup Stage Telemetry Summary

After WAL and historical-status repairs, production still performed heavy
SQLite I/O during startup. Stage logs now distinguish base DDL, Structure
migrations, additive migrations, and nested opportunity-schema initialization,
so the next restart identifies the remaining path from authoritative logs.
