# Task 1 Summary — Adaptive Structure Timing Controller

## Delivered

- Added a pure, bounded policy over the latest 30 durable Structure attempts.
- Success duration uses `elapsed_ms`, with `finished_at_ms - started_at_ms` for legacy rows.
- At 10+ successes, nearest-rank p95 drives `timeout=p95+30s` and a non-overlapping cadence.
- The latest subprocess timeout immediately raises the previous timeout by 20%; ordinary
  changes use a three-attempt cooldown and ignore changes smaller than 15 seconds.
- Effective timeout/cadence changes are append-only, source-attempt-bound, and restored
  across process restarts.
- Scheduler child execution and wait cadence use effective values.
- Strict health exposes `snapshot:schedule` and judges a running attempt against the same
  persisted effective timeout.

## Safety Boundaries

- Timeout clamp: 180–600 seconds.
- Cadence clamp: 300–900 seconds.
- The existing five-failure PAUSED gate is unchanged.
- The controller never unpauses Structure and never weakens coverage/publication checks.

## Verification

```text
uv run pytest tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_structure_schedule.py \
  tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_snapshot_attempt_status.py -q
58 passed

uv run ruff check <changed Python files>
All checks passed!

make planning-status
no drift detected
```

Production deployment and continuous-attempt evidence belong to Task 2 and are not claimed
by this summary.
