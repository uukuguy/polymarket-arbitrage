# Daily Intelligence Make Remediation 2 Report

## Root cause

`limit` and `after_group_id` were globally exported recursive command-line Make
variables. A value containing Make syntax could therefore be expanded while
Make prepared the recipe environment, before the static shell recipe ran.

## Remediation

- `limit` and `after_group_id` are explicitly unexported.
- `control-plane-opportunities` exports only target-scoped, simply-expanded
  `CONTROL_PLANE_OPPORTUNITIES_LIMIT` and
  `CONTROL_PLANE_OPPORTUNITIES_AFTER_GROUP_ID`, captured through `$(value ...)`.
- The recipe refers only to shell variables, retains `curl --get` with
  `--data-urlencode`, the exact production URL, 3-second connect/10-second
  total timeout, and stops on curl failure before JSON formatting.

## Regression evidence

The focused pytest invokes Make with a literal
`limit=$(shell touch SENTINEL)` passed directly as argv. It installs fake
`curl` and `touch` executables, then verifies all of the following:

1. neither the sentinel nor the fake unexpected-command record exists;
2. fake curl receives the exact literal `limit=$(shell touch SENTINEL)` once;
3. fake curl emits `FAKE_CURL_BODY` to redirected stdout and `json.tool`
   formats that successful body.

Fresh verification:

- `uv run pytest tests/test_makefile.py -k control_plane_opportunities -q` — pass.
- `make -n control-plane-opportunities limit=50 after_group_id=cursor-1` —
  generated shell source contains only `CONTROL_PLANE_OPPORTUNITIES_*` shell
  references.
- `uv run ruff check tests/test_makefile.py` — pass.
- `make docs-m1-check` — pass.
- `make control-plane-opportunities limit=1` — live read-only response:
  `status=available`, `current_opportunity_count=0`.
- `make planning-status` and `git diff --check` — pass.

The complete `tests/test_makefile.py` run has one pre-existing unrelated
failure: `test_status_uses_the_canonical_current_state` expects
`唯一当前状态入口`, while the current status text says `唯一动态状态入口`.
