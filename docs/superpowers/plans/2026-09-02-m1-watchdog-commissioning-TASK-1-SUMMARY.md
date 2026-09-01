# M1 Watchdog Commissioning — Task 1 Summary

**Delivered:** The permanent production watchdog now treats formal soak
evidence as an explicit acceptance option, rather than an implicit dependency
on a retired sampler.  Its normal liveness gate continues to fail closed on the
public control API, three primary workers, and the runtime-event writer.

**Why:** The new database carried no legacy formal run ID.  The deployment
template expanded an unset environment variable to an empty `--soak-run-id`,
which correctly failed validation but made the watchdog restart-loop instead of
observing production.

**Verification:** Focused CLI and deployment-template tests pass.  The rendered
command contains neither the retired evidence Machine nor an implicit legacy
soak identifier.  Production deployment must show consecutive watchdog JSON
heartbeats after the replacement is live.
