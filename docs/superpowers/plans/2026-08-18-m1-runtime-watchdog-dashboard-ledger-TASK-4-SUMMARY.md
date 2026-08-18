# Runtime Watchdog Dashboard Ledger — Task 4 Summary

The pure-local rollout renderer now names four mutually isolated apps: API,
transactional data worker, Telegram watchdog, and runtime-event writer. The
writer is rendered into its own config and must be a distinct non-legacy app;
the rollout checklist explicitly carries that topology. The CLI and Makefile
require the writer app identity, removing a manual deployment omission.

Verification: `tests/m1-perception/test_control_plane_rollout.py` and
`tests/m1-perception/test_control_plane_cli.py` pass, including legacy-name
rejection and no-control-plane-connection rendering.
