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

## Review follow-up — format safety and local bounds

- `control-plane-business-brief` now exports the target-specific raw Make value
  as `CONTROL_PLANE_BUSINESS_BRIEF_FORMAT`; the recipe never interpolates
  `$(format)` directly. Its shell-local `format` accepts only `text` or `json`
  and passes the quoted value to the CLI.
- Regression coverage verifies both shell-metacharacter input and an escaped
  literal Make-function payload do not execute a command or invoke the CLI.
  (GNU Make expands an unescaped `$(shell ...)` while parsing a command-line
  assignment, before any target recipe can receive it.)
- The CLI now rejects `business-brief --limit` outside the local inclusive
  `1..500` bound. The same bound remains defended by the public-opportunity
  reader.
- Extracted the business-brief dispatch branch from `main` without changing its
  authorities, failure envelope, output, or exit codes. This removes the prior
  Pyright complexity diagnostic for `main`.

### Follow-up verification

```text
uv run pytest tests/m1-perception/test_business_brief.py tests/m1-perception/test_makefile_contract.py -k business_brief -q
14 passed

uv run ruff check src/polyarb/cli_control_plane.py tests/m1-perception/test_business_brief.py tests/m1-perception/test_makefile_contract.py
All checks passed!
```

### Remaining Pyright limitation

The extraction exposes six pre-existing type-contract errors in runtime
reconciliation paths, rather than the prior `main` complexity bailout.
`PostgresControlPlane._connection_factory` returns an
`AbstractContextManager[Connection[Any]]`, while the local and
`recovery_store.ConnectionFactory` aliases require `Callable[[], Connection[Any]]`.
This makes calls at `cli_control_plane.py:2306,2316,2335,2373,2388,2408`
incompatible. Correcting those shared factory contracts is unrelated to this
review-only brief change and was intentionally left out.
