# Task 9 Summary — supervisor control-plane continuity

## Outcome

The daemon now owns a persistent runner for every enabled isolated producer.
If `ProducerSupervisor.run()` returns after bounded child retries, or raises an
unexpected exception, the daemon records the normal supervisor evidence,
logs the control-plane failure, backs off, and creates a new supervisor. A
live HTTP parent can no longer silently lose its Quote producer.

## Verification

- Added a regression proving a returned Quote supervisor is recreated while
  the daemon stop event remains clear.
- `uv run pytest tests/perception/test_supervisor.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/daemon/test_quote_worker.py -q` passed.
- `ruff check src/polyarb/daemon/main.py tests/perception/test_supervisor.py` passed.

## Production consequence

After deployment, the replacement Quote worker will run the existing bounded
attempt-admission recovery; an old collecting attempt beyond its 120-second
parent limit is terminalized before the successor is admitted. The control
plane itself continues retrying rather than leaving the parent healthy and the
producer absent.
