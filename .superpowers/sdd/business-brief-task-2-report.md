# Business Brief Task 2 Report

## Delivered

- Added the read-only `business-brief` CLI command with `--format text|json` and
  bounded public opportunity input (`--limit` defaults to 50).
- The command reads the established scoped-DSN durable status projection with a
  fixed sample limit of 20, then performs an explicit 10-second `GET` to the
  public opportunities authority.
- Both text and sorted JSON render the Task 1 canonical brief. Authority,
  transport, HTTP, JSON, and shape failures return exit code 2 with only
  `业务数据不可用` on stderr.
- Added `make control-plane-business-brief [format=text|json]`, including the
  scoped-DSN guard and `make help`/`.PHONY` exposure.

## Safety boundaries

- The Make target contains no Fly deployment, secret, wallet, order, or trade
  operation.
- Existing `control-plane-status` and `control-plane-opportunities` audit
  readers were not changed.
- No schema, scheduler, database-write, or execution capability was added.

## Verification

```text
uv run pytest tests/m1-perception/test_business_brief.py tests/m1-perception/test_makefile_contract.py -k business_brief -q
9 passed

uv run ruff check src/polyarb/cli_control_plane.py tests/m1-perception/test_business_brief.py tests/m1-perception/test_makefile_contract.py
All checks passed!

make -n control-plane-business-brief format=json
... business-brief --format "json"

git diff --check
exit 0
```

## Deliberate implementation note

The current `PostgresControlPlane` exposes the status authority as
`operational_snapshot(sample_limit=20)` rather than a `status()` method. Task 2
uses that existing read-only primitive and marks a successful read as available
before passing it to the Task 1 canonical mapper.
