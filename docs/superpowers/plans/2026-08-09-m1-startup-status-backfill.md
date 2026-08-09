# M1 Startup Status Backfill Repair

The legacy Structure status backfill was re-reading every historical Structure
snapshot and validation issue, then rewriting every status at each daemon boot.
It now considers only rows that can still need correction: default `ok` rows
that are invalid or have a Layer-1 issue. Settled `degraded`/`failed` evidence
is never rescanned or rewritten on normal startup.
