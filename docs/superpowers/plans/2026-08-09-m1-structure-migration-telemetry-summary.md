# M1 Structure Migration Telemetry Summary

v250 showed base and sync-window DDL finish immediately; the remaining startup
span is inside the Structure migration sequence. The stage log now brackets
each migration to identify the exact function on the next restart. Migration
tests and Ruff pass.
