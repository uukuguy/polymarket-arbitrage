# Runtime Watchdog Dashboard Ledger — Task 2 Summary

The existing read-only `operational_snapshot()` now projects the canonical
`runtime-watchdog` incident and bounded detected/recovered event history.
The Next dashboard has a fail-closed `/control-plane` route: active incidents
are prominent red panels and recovery/failure reason codes remain visible in a
chronological ledger. It reads only the public read API and never exposes a
write credential to the browser.

Verification: focused control-plane API/watchdog tests and Dashboard TypeScript
typecheck/build pass. Deployment and controlled cloud event proof are pending.
