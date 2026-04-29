---
phase: 01
plan: 1
workstream: m1-perception
wave: 1
status: complete
started_at: 2026-04-29T07:36:58Z
completed_at: 2026-04-29T07:43:20Z
duration_seconds: 382
duration_human: "6m22s"
tasks_completed: 7
tasks_total: 7
tags:
  - skeleton
  - pyproject
  - config
  - settings
dependency_graph:
  requires: []
  provides:
    - "Installable polyarb package (editable mode)"
    - "Settings dataclass + YAML loader with F-3 path validator"
    - "5 empty sub-packages (clients, storage, snapshot, validator + top-level)"
    - "config/snapshot.yaml as discoverable default config"
    - "tests/m1-perception/ test directory with passing smoke suite"
  affects:
    - "Plan 02 (gamma client) — can now `from polyarb.clients import ...`"
    - "Plan 03 (clob client) — can now `from polyarb.clients import ...`"
    - "Plan 04 (snapshot CLI) — can wire `polyarb.cli:app` into Typer"
    - "Plan 05 (storage + tests) — has tests/m1-perception/__init__.py and conftest pattern"
tech_stack:
  added:
    - "hatchling 1.x (build backend)"
    - "pydantic-settings 2.14.0 (Settings)"
    - "py-clob-client 0.34.6"
    - "pyarrow 17.0.0"
    - "httpx 0.27.2 (with http2)"
    - "aiolimiter 1.2.1, tenacity 8.5.0"
    - "typer 0.12.5, loguru 0.7.3, tqdm 4.67.3"
    - "pyyaml 6.0.3"
  patterns:
    - "src layout (`src/polyarb`) — Pitfall 7 from RESEARCH.md addressed"
    - "Lazy submodule imports (top-level `__init__.py` only declares `__version__`)"
    - "F-3 path constraint via `field_validator` + `POLYARB_ALLOW_EXTERNAL_PATHS` test escape hatch"
    - "YAML defaults written explicitly to mirror in-code defaults (self-documenting)"
key_files:
  created:
    - pyproject.toml
    - src/polyarb/__init__.py
    - src/polyarb/clients/__init__.py
    - src/polyarb/storage/__init__.py
    - src/polyarb/snapshot/__init__.py
    - src/polyarb/validator/__init__.py
    - src/polyarb/config.py
    - config/snapshot.yaml
    - tests/m1-perception/__init__.py
    - tests/m1-perception/test_skeleton.py
  modified:
    - .gitignore
decisions:
  - "Adopted src layout (Pitfall 7) — wheel target packages = ['src/polyarb']"
  - "Used pydantic-settings v2 with env_prefix=POLYARB_ for env-var overrides"
  - "F-3 validator rejects out-of-project db_path/parquet_root by default; tests opt out via POLYARB_ALLOW_EXTERNAL_PATHS=1"
  - "Did NOT pin lockfile (deferred to F-7 in a later plan)"
  - "Did NOT modify Makefile (Plan 4 owns snapshot-markets target wiring)"
  - "Documented env-vs-YAML precedence: in pydantic-settings 2.14.0, init kwargs (YAML) win over env vars when both are present"
metrics:
  duration: "6m22s"
  completed_date: "2026-04-29"
  tasks: "7/7"
  smoke_tests_passing: "5/5"
---

# Phase 01 Plan 1: Skeleton Summary

Established the buildable `polyarb` package skeleton with hatchling/src-layout, pydantic-settings-based config loader (with F-3 path validator), default YAML config, and a 5-test smoke suite — all installed in editable mode and proven to import from any working directory.

## Per-Task Execution

| Task | Name | Commit | Files |
| ---- | ---- | ------ | ----- |
| T1 | pyproject.toml (hatchling + 11 runtime + 6 dev deps) | `106765f` | pyproject.toml |
| T2 | scaffold src/polyarb package tree | `d37185c` | src/polyarb/{__init__,clients/__init__,storage/__init__,snapshot/__init__,validator/__init__}.py |
| T3 | Settings + load_settings with F-3 validator | `ebfb733` | src/polyarb/config.py |
| T4 | config/snapshot.yaml defaults | `60c1147` | config/snapshot.yaml |
| T5 | .gitignore canonical block | `0a17b3d` | .gitignore |
| T6 | smoke test suite (5 tests) | `c0ad576` | tests/m1-perception/{__init__.py,test_skeleton.py} |
| T7 | install editable + verify | (no new files — verify only) | — |

## Installed Dependency Versions (resolver-picked)

Within the caret-pinned ranges declared in pyproject.toml, the resolver selected:

| Package | Version | Range declared |
| ------- | ------- | -------------- |
| httpx | 0.27.2 | >=0.27,<0.28 |
| py-clob-client | 0.34.6 | >=0.34.6,<0.35 |
| aiolimiter | 1.2.1 | >=1.2,<2 |
| tenacity | 8.5.0 | >=8.4,<9 |
| pyarrow | 17.0.0 | >=17.0,<18 |
| pydantic | 2.13.3 | >=2.7,<3 |
| pydantic-settings | 2.14.0 | >=2.4,<3 |
| pyyaml | 6.0.3 | >=6.0,<7 |
| typer | 0.12.5 | >=0.12,<0.13 |
| tqdm | 4.67.3 | >=4.66,<5 |
| loguru | 0.7.3 | >=0.7,<0.8 |
| pytest | 8.4.2 | >=8.2,<9 |
| pytest-asyncio | 0.23.8 | >=0.23,<0.24 |
| respx | 0.21.1 | >=0.21,<0.22 |
| duckdb | 1.5.2 | >=1.0,<2 |
| freezegun | 1.5.5 | >=1.5,<2 |
| ruff | 0.15.12 | >=0.5,<1 |

`pip install -e '.[dev]'` produced no warnings, no resolver conflicts. Editable install of `polyarb 0.1.0` registered cleanly via hatchling/src-layout.

## Final Verification Output

### `python -c "import polyarb; from polyarb.config import load_settings; print(polyarb.__version__, load_settings().gamma_url)"`

```
0.1.0 https://gamma-api.polymarket.com
```

### `pytest tests/m1-perception/test_skeleton.py -xvs`

```
============================= test session starts ==============================
platform darwin -- Python 3.12.11, pytest-8.4.2, pluggy-1.6.0
rootdir: /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage
configfile: pyproject.toml
plugins: asyncio-0.23.8, respx-0.21.1, anyio-4.13.0
asyncio: mode=Mode.AUTO
collected 5 items

tests/m1-perception/test_skeleton.py .....

============================== 5 passed in 0.07s ===============================
```

### Import-from-anywhere check (cwd=/tmp)

```
import-from-anywhere OK: 0.1.0
```

### Package layout check

```
src/polyarb/clients/__init__.py
src/polyarb/snapshot/__init__.py
src/polyarb/storage/__init__.py
src/polyarb/validator/__init__.py
config/snapshot.yaml: OK
```

## Configuration Precedence (probed in T6)

The lenient `test_env_var_overrides_yaml` was designed to surface the actual behavior. Running it isolated confirms:

> **In pydantic-settings 2.14.0, when YAML values are passed as `Settings(**data)` kwargs, the init kwargs WIN over `POLYARB_*` env vars.**

This is the documented pydantic-settings precedence (init kwargs > env > .env > secrets > defaults). For a future plan that wants env vars to override YAML at runtime, the loader would need to either:
- Build the kwargs dict by merging (env values onto YAML), or
- Switch to `Settings.model_validate({**yaml_data, **env_overrides})`, or
- Drop YAML kwargs and rely on env-only overrides (rejected — config files are nicer for ops).

For Plan 1's purposes the existing precedence is acceptable: YAML is the canonical config, env vars are escape hatches that require either deleting the YAML key or setting the env var BEFORE Settings is instantiated and constructing without the conflicting kwarg. **Action item logged for Plan 4 / Plan 5 to revisit if a real ops scenario hits this.**

## F-3 Security Validator

The `@field_validator("db_path", "parquet_root")` works as designed:

```python
$ POLYARB_DB_PATH=/etc/passwd python -c "from polyarb.config import Settings; Settings()"
ValueError: path /etc/passwd resolves outside project root /Users/.../polymarket-arbitrage
```

Tests that need pytest's `tmp_path` (outside project root) set `POLYARB_ALLOW_EXTERNAL_PATHS=1` in their conftest — Plan 5 owns the conftest. The current 5 smoke tests do not need this because they either:
- don't instantiate `db_path` / `parquet_root` from external locations, or
- run with cwd=tmp_path (`monkeypatch.chdir(tmp_path)`) so the validator's `Path.cwd()` matches.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Performed `pip install -e '.[dev]'` early (before T7)**

- **Found during:** T3 verify
- **Issue:** The plan's T3, T4, T6 verify blocks all run `python -c "from polyarb.config import ..."` which requires the package installed editable; without install, every verify after T2 would fail.
- **Fix:** Ran `pip install -e '.[dev]'` between T2 and T3 (before T3 verify), instead of waiting for T7.
- **Files modified:** none (install is a venv-state change, not a tracked-file change)
- **Commit:** none — install is not committable. T7 became a verify-only gate (no commit needed).

No other deviations. The plan's locked decisions (deps, defaults, F-3 validator code) were applied verbatim.

### Open Items (Deferred, Not Blocking)

- **F-7 lockfile** — not in scope for Plan 1 (plan does not list it). Deferred to a future hardening plan.
- **`polyarb.cli:app` console script** — declared in `[project.scripts]` per resolved Q7, but `cli.py` does not exist yet. Plan 4 owns creating `src/polyarb/cli.py`. The skeleton install does not break (entry point is resolved lazily on `polyarb` invocation, not at install time).
- **Makefile `snapshot-markets` target** — not added by this plan (per important_constraints in the executor prompt). Plan 4 owns wiring.
- **conftest `POLYARB_ALLOW_EXTERNAL_PATHS=1`** — Plan 5 owns this. Current smoke tests do not need it.
- **Mid-test env-vs-YAML precedence** — documented above; revisit if Plan 4 or Plan 5 needs override semantics that don't match pydantic-settings defaults.

## Authentication Gates

None. Plan 1 has no auth requirements.

## Self-Check: PASSED

All claimed commits exist in git log:
- `106765f` — pyproject.toml — FOUND
- `d37185c` — src/polyarb scaffolding — FOUND
- `ebfb733` — config.py with F-3 validator — FOUND
- `60c1147` — config/snapshot.yaml — FOUND
- `0a17b3d` — .gitignore update — FOUND
- `c0ad576` — smoke tests — FOUND

All claimed files exist on disk:
- pyproject.toml — FOUND
- src/polyarb/__init__.py — FOUND
- src/polyarb/clients/__init__.py — FOUND
- src/polyarb/storage/__init__.py — FOUND
- src/polyarb/snapshot/__init__.py — FOUND
- src/polyarb/validator/__init__.py — FOUND
- src/polyarb/config.py — FOUND
- config/snapshot.yaml — FOUND
- tests/m1-perception/__init__.py — FOUND
- tests/m1-perception/test_skeleton.py — FOUND
- .gitignore — FOUND (modified)

Smoke suite: 5/5 PASS.

## Threat Flags

None. No new attack surface added (skeleton has no network, no I/O endpoints, no auth paths). The F-3 validator hardens an existing surface (storage path injection via env/YAML) before storage code lands in Plan 5.
