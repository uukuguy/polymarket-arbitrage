# Task 1 Report: Production Opportunity Reader

## Result

Implemented the read-only `make control-plane-opportunities` business entrypoint.
It reads `GET /perception/opportunities`, supports optional `limit` and
`after_group_id` values, pretty-prints the raw JSON response, and preserves
HTTP failures as nonzero command failures via `curl -f`.

## TDD evidence

RED (before adding the Makefile target):

```text
uv run pytest tests/m1-perception/test_makefile_contract.py -k control_plane_opportunities_is_current_read_only_business_entrypoint -q
F                                                                        [100%]
assert match is not None
```

GREEN:

```text
uv run pytest tests/m1-perception/test_makefile_contract.py -k control_plane_opportunities_is_current_read_only_business_entrypoint -q
.                                                                        [100%]
```

Full contract file:

```text
uv run pytest tests/m1-perception/test_makefile_contract.py -q
.................................................                        [100%]
```

Dry run:

```text
make -n control-plane-opportunities limit=5 after_group_id=example
curl --disable --connect-timeout 3 --max-time 10 --retry 0 -fsS "https://polyarb-control-api.fly.dev/perception/opportunities?limit=5&after_group_id=example" | python -m json.tool
```

Live read-only verification returned HTTP success and valid JSON:

```json
{
    "status": "available",
    "current_opportunity_count": 0,
    "items": [],
    "limit": 5,
    "next_after_group_id": null
}
```

## Files changed

- `Makefile`: added the `.PHONY` declaration and production reader target.
- `tests/m1-perception/test_makefile_contract.py`: added the Makefile contract test.

## Self-review

- URL, pagination defaults, curl timeouts, fail-on-HTTP-error behavior, and
  `python -m json.tool` formatting match the task contract.
- No deployment, mutation, credential, database, wallet, order, or trade logic
  is included.
- `make help` exposes the target through its `##` description.

## Concerns

None. The live endpoint currently reports zero available opportunities, which
is a valid authenticated projection rather than a synthesized fallback.

## Task 1 Review Fix Verification

Wrapped the forbidden-token tuple in `tests/m1-perception/test_makefile_contract.py:3505`
to satisfy Ruff E501 without changing the token set or assertion behavior.

Exact commands and results:

```text
uv run ruff check tests/m1-perception/test_makefile_contract.py; ruff_status=$?; uv run pytest tests/m1-perception/test_makefile_contract.py -q; pytest_status=$?; printf 'RUFF_STATUS=%s\nPYTEST_STATUS=%s\n' "$ruff_status" "$pytest_status"; exit $((ruff_status || pytest_status))
All checks passed!
........................................................................ [ 39%]
........................................................................ [ 79%]
.....................................                                    [100%]
RUFF_STATUS=0
PYTEST_STATUS=0
```
