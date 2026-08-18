# Runtime Watchdog Dashboard Ledger — Task 3 Summary

Added the independently deployable `fly-runtime-event-writer.toml.template`:
one 256MB private Fly writer with an explicit `/healthz` check. The template
documents the exact credential boundary: scoped incident-ledger DSN and ingest
token only, never Telegram/R2/Gamma/CLOB/scheduler/browser credentials.

Added `make control-plane-runtime-event-writer-serve enable=1` as the sole
local command entry point. This task intentionally does not deploy during the
active immutable acceptance window; provisioning the scoped DB role and fresh
topology is the next cloud action.

Verification: target rejects unarmed use with exit 2; template and Makefile
references are present; `git diff --check` passes.
