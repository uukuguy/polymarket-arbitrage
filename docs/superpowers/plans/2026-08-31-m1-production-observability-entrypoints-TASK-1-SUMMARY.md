# Task 1 Summary — Production Health Contract

`make smoke-control-plane-prod` is the current unauthenticated public strict
readiness probe. It performs only `GET https://polyarb-control-api.fly.dev/health`
and requires both HTTP 200 and `{status: "ok", control_plane: "available"}`.

The retired `polyarb-l1` / `polyarb-l2` health and Fly-status targets remain
discoverable but exit 2 with explicit replacements. They are not silently
redirected because old L1/L2 data-product semantics differ from control-plane
readiness.

Verification: focused Makefile contract tests passed; `make -n` confirms the
new recipe contains only GET/curl/jq logic and retired recipes contain no
network action.
