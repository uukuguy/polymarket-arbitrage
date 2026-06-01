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
