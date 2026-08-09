# M1 cross-process producer arbitration — summary

Date: 2026-08-10

Implemented the first production slice of the approved arbitration design.

- Added a SQLite `BEGIN IMMEDIATE` lease for the shared market-write producer slot.
  Quote and Structure are mutually exclusive across the supervised child/parent
  process boundary; only the exact lease holder can release, and expired leases
  are durably recorded and reclaimed by the next producer.
- Wired supervised Quote to acquire the lease for its bounded collection and
  certification boundary.  Wired supervised Structure to acquire at most a
  45-second window and stopped using the parent-only Quote runtime as an
  authority in that mode.
- Added `/perception/producer-arbitration` and a **Producer handoff** card on
  `/perception/console`: current owner/expiry, recent handoff evidence,
  automatic recovery, and operator action are directly inspectable.

Verification:

```text
uv run pytest tests/m1-perception/test_perception_http.py \
  tests/daemon/test_producer_arbitration.py \
  tests/daemon/test_quote_worker.py \
  tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_l1_quote_worker_wiring.py -q
uv run ruff check …
```

The next production step is deployment followed by proof that a fresh Structure
publication arrives while Quote continues at its 60-second cadence.
