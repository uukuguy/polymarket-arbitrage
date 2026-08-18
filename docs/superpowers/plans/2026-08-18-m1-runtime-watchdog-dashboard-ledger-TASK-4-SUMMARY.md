# Runtime Watchdog Dashboard Ledger — Task 4 Summary

The pure-local rollout renderer now names four mutually isolated apps: API,
transactional data worker, Telegram watchdog, and runtime-event writer. The
writer is rendered into its own config and must be a distinct non-legacy app;
the rollout checklist explicitly carries that topology. The CLI and Makefile
require the writer app identity, removing a manual deployment omission.

Verification: `tests/m1-perception/test_control_plane_rollout.py` and
`tests/m1-perception/test_control_plane_cli.py` pass, including legacy-name
rejection and no-control-plane-connection rendering.

Follow-up hardening: the writer now uses the incident key returned by the
unique `dedupe_key` conflict path, so concurrent detection cannot attach an
event to a transient UUID. A first healthy watchdog observation is persisted
as an explicit no-op rather than being mistaken for a failed ledger write.
`dashboard/.env.example` documents the server-only control-plane source.
