# Phase 05 — Deferred Items

## Plan 05-03 (Wave 2)

- **Pre-existing failure: `tests/m1-perception/test_ws_watchdog_liveness.py`** — 6
  async tests fail at collection because `pytest-asyncio` is not configured
  (`async def functions are not natively supported`). Confirmed pre-existing
  on base commit `db6638c` (before Plan 05-03 changes). Out of scope per the
  executor SCOPE BOUNDARY (only auto-fix issues caused by current task).
  GAP-401 watchdog liveness gate IS implemented in `src/polyarb/daemon/ws_watchdog.py`
  and `ws_consumer.py` per SESSION 33; only the pytest harness for the
  test suite needs `pytest-asyncio` or `anyio` registration. Recommend a
  separate housekeeping task: `uv add --dev pytest-asyncio` + set
  `asyncio_mode = "auto"` in pyproject.toml (the config currently uses
  `asyncio_mode` under `[tool.pytest.ini_options]` but the plugin itself
  is not installed — hence the "Unknown config option: asyncio_mode" warning).

## Plan 05-04 (Wave 3)

- **Flaky test under broad collection: `tests/m1-perception/test_chaos_r2.py::test_r2_retry_config_is_applied`**
  — Fails when run as part of the full m1-perception sweep (`uv run pytest tests/m1-perception/ -x`)
  but PASSES in isolation (`uv run pytest tests/m1-perception/test_chaos_r2.py`). Pre-existing
  cross-test state-leak (likely tenacity / boto retry config polluted by an earlier
  test in collection order). Out of scope per executor SCOPE BOUNDARY — Plan 05-04
  touches `l3_promote.py`, `l2_main.py`, `l2_health.py`, `scan_recipes/l3-promote.yaml`,
  not any R2 or tenacity code path. Recommend separate housekeeping task to isolate
  the leaking fixture (probable suspects: any test that calls `setup_retry_config`
  or monkeypatches `tenacity.retry` without a proper teardown).
