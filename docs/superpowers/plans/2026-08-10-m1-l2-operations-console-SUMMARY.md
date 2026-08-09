# L2 Operations Console Summary

`polyarb-l2` now exposes a public, same-origin, read-only `/console` for
operators.  It renders every live warning/failure from the authoritative L2
`/health` contract as a card with its impact, observed value, automatic
recovery posture, prescribed next operator action, and the raw check evidence.
The console deliberately treats an unavailable health read as a visibility
fault rather than an all-clear and grants no restart or other control authority.

The existing L1 incident console links directly to the L2 console, so severe
order-book failures are discoverable from the M1 operator entry point.

Verification: L2 health, L3 health-chain, candidate-fetch, Fly-config, and
dashboard contract tests pass; Ruff passes.
