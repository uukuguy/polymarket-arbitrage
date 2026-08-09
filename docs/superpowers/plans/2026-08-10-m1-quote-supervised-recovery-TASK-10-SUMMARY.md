# Task 10 Summary — quote P1 closure proof

## Outcome

An exhausted Quote supervisor no longer leaves a permanent false P1 after a
healthy replacement is running. On supervisor startup, only prior escalated
Quote incidents are moved back to `recovering`; a later certified Quote run
then verifies them only when its durable run ID, completion time, quote time,
requested count, and successful response count agree with the database.

## Verification

- Added a regression for an escalated Quote supervisor incident: restart
  handoff plus a post-handoff complete run closes it; a manual clear is never
  used.
- `uv run pytest tests/m1-perception/test_quote_incidents.py tests/perception/test_supervisor.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/daemon/test_quote_worker.py -q` passed.
- `uv run ruff check src/polyarb/perception/supervisor.py src/polyarb/perception/incidents.py src/polyarb/daemon/quote_incidents.py tests/m1-perception/test_quote_incidents.py` passed.

## Production consequence

The direct Fly incident console and Dashboard retain the full P1 lifecycle.
After a supervised recovery, the card moves from open to verified only on a
new authentic collection result, so operators neither lose a real outage nor
remain stuck with an obsolete alarm.
