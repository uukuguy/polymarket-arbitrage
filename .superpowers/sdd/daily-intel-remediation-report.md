# Daily Intelligence Remediation Report

Date: 2026-08-31

## Root cause

`control-plane-opportunities` expanded untrusted Make command-line values directly
into generated shell/URL source.  Shell metacharacters could therefore alter the
recipe before curl received the query.  Its `curl | python -m json.tool` pipeline
also invoked the formatter after curl failed and let the formatter determine the
pipeline result.

## Changes

- Exported the lowercase `limit` and `after_group_id` Make variables and consume
  them only as shell environment values.
- Changed the read-only production request to `curl --get` with two
  `--data-urlencode` arguments, retaining the exact production URL, bounded
  timeout, and zero retries.
- Buffered curl output in a temporary file, exited immediately on curl failure,
  and format JSON only after a successful request.
- Added executable regression coverage for metacharacters, `&`, `#`, and curl
  exit 22, plus the strengthened business-facing learning-guide contract.
- Updated the M1 guide with the correct readiness and opportunity code map,
  copyable status summary, observation cadence, and escalation boundaries.

The ops-log template already has an explicit `证据` field, so it did not need a
new evidence field.

## RED evidence

Command:

```text
uv run pytest tests/m1-perception/test_makefile_contract.py::test_control_plane_opportunities_is_current_read_only_business_entrypoint tests/m1-perception/test_makefile_contract.py::test_control_plane_opportunities_encodes_untrusted_make_values_without_shell_execution tests/m1-perception/test_makefile_contract.py::test_control_plane_opportunities_preserves_curl_failure_without_formatting tests/m1-perception/test_m1_manual_contract.py::test_daily_business_intelligence_guide_keeps_business_truth_boundaries -q
```

Output:

```text
FFFF                                                                     [100%]
FAILED test_control_plane_opportunities_is_current_read_only_business_entrypoint
  assert 'export limit after_group_id' in Makefile
FAILED test_control_plane_opportunities_encodes_untrusted_make_values_without_shell_execution
  /bin/sh: -c: line 0: unexpected EOF while looking for matching `''
  make: *** [control-plane-opportunities] Error 2
FAILED test_control_plane_opportunities_preserves_curl_failure_without_formatting
  assert 0 == 22
  stdout='{}\\n'
FAILED test_daily_business_intelligence_guide_keeps_business_truth_boundaries
  assert 'src/polyarb/control_plane/api.py:34' in guide
4 failed
```

## GREEN evidence

Focused regressions, lint, and documentation checker:

```text
....                                                                     [100%]
All checks passed!
M1 manual contract: OK
```

Commands:

```text
uv run pytest tests/m1-perception/test_makefile_contract.py::test_control_plane_opportunities_is_current_read_only_business_entrypoint tests/m1-perception/test_makefile_contract.py::test_control_plane_opportunities_encodes_untrusted_make_values_without_shell_execution tests/m1-perception/test_makefile_contract.py::test_control_plane_opportunities_preserves_curl_failure_without_formatting tests/m1-perception/test_m1_manual_contract.py::test_daily_business_intelligence_guide_keeps_business_truth_boundaries -q
uv run ruff check tests/m1-perception/test_makefile_contract.py tests/m1-perception/test_m1_manual_contract.py
make docs-m1-check
```

Relevant full contract suites:

```text
........................................................................ [ 28%]
........................................................................ [ 56%]
........................................................................ [ 85%]
.....................................                                    [100%]
```

Command:

```text
uv run pytest tests/m1-perception/test_makefile_contract.py tests/m1-perception/test_m1_manual_contract.py -q
```

Live, read-only production target:

```text
{
    "status": "available",
    "current_opportunity_count": 0,
    "items": [],
    "limit": 1,
    "next_after_group_id": null
}
```

Command: `make control-plane-opportunities limit=1`

## Concerns

- The target intentionally remains a read-only observation tool; a successful
  request or zero opportunities is not a readiness, execution, or P&L claim.
- GNU Make reports a recipe `exit 22` as its own nonzero process exit (2) while
  retaining `Error 22` in stderr.  The regression asserts both nonzero Make
  status and that the formatter was never invoked.
