# M1 Watchdog Retired Target — Task 1 Summary

**Delivered:** The production watchdog template no longer treats the retired
`polyarb-control-evidence` Machine as a runtime dependency. It continues to
observe the three fenced data-worker Machines, the read-only control API, and
the private runtime-event writer.

**Why:** A stopped evidence sampler is not a failed business service. Keeping
it in the exact-machine gate generated a permanent critical incident and
obscured real failures.

**Verification:** the static deployment contract proves the rendered watchdog
command contains no evidence app or machine environment reference. Production
verification must observe a healthy watchdog transition after the new template
is deployed.
