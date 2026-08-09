# L2 quiet-state console clarification

## Outcome

The read-only L2 operations console now distinguishes the intentionally
non-alerting `ws:connection_state=WAITING_FOR_EVENT` observation from an
actionable transport or evidence outage. It directs the operator to intervene
only when websocket age, mirror freshness, or L3 evidence ceases to pass.

## Evidence

- Added a regression test before the implementation; it failed because the
  console had no `ws:connection_state` action text.
- `uv run pytest tests/m1-perception/test_l2_health_endpoint.py -q` — 10 passed.
- `uv run ruff check src/polyarb/http/l2_console.py tests/m1-perception/test_l2_health_endpoint.py` — passed.

## Boundaries

- No strict-health threshold, recovery loop, or alerting gate was relaxed.
- The console remains read-only and uses the same-origin `/health` contract.
