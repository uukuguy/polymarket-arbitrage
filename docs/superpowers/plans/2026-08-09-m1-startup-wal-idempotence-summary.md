# M1 Startup WAL Idempotence Repair Summary

After bounding status backfill, startup remained in disk sleep. Shared schema
DDL still replayed the persistent `journal_mode=WAL` pragma on every boot.
The repair makes WAL activation conditional on the existing journal mode;
the WAL-mode regression and migration suite pass.
