# Bounded Health Read Lane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure M1 `/health` and `/healthz` return a bounded, explicit unavailable result when SQLite health evidence is saturated instead of exceeding Fly's response-header deadline.

**Architecture:** Reuse the application-owned `BoundedReadLane` already used by perception projections. A new health-specific lane prevents expensive health aggregation from sharing workers with dashboard queries. Both health routes execute the same builder through the lane; timeout, saturation, and SQLite failures are converted to an IETF-shaped P1 health payload, while `/healthz` retains its always-200 routing contract.

**Tech Stack:** Python 3.12, Starlette, SQLite, pytest.

## Global Constraints

- `/healthz` stays HTTP 200 for a completed response, but cannot be allowed to hang past the bounded read deadline.
- `/health` stays strict HTTP 503 whenever the health read is unavailable or saturated.
- Failure payloads expose no database paths, DSNs, credentials, or SQL errors.
- Do not change Structure or Quote producer behavior in this repair.

---

### Task 1: Bound health aggregation and preserve fault truth

**Files:**
- Modify: `src/polyarb/http/app.py:209-245`
- Modify: `src/polyarb/http/health.py:2189-2265`
- Test: `tests/m1-perception/test_health_endpoint.py`

**Interfaces:**
- Consumes: `BoundedReadLane.run(function, *, timeout_s)` and `ReadLaneSaturatedError` from `polyarb.http.opportunity_read_health`.
- Produces: `app.state.health_read_lane` and a shared async health body builder returning `(body, overall, unavailable_reason)`.

- [ ] **Step 1: Write the failing tests**

```python
def test_health_returns_503_when_health_read_lane_is_saturated(http_test_client):
    http_test_client.app.state.health_read_lane = _SaturatedLane()
    response = http_test_client.get("/health")
    assert response.status_code == 503
    assert response.json()["status"] == "fail"
    assert response.json()["checks"]["runtime:health_read_lane"][0]["output"] == "reason=read-model-saturated"

def test_healthz_returns_200_and_explicit_failure_when_health_read_times_out(http_test_client):
    http_test_client.app.state.health_read_lane = _TimeoutLane()
    response = http_test_client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "fail"
    assert response.json()["checks"]["runtime:health_read_lane"][0]["output"] == "reason=read-model-unavailable"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/m1-perception/test_health_endpoint.py -k 'health_read_lane' -q`

Expected: FAIL because health currently uses unbounded `asyncio.to_thread` and has no `runtime:health_read_lane` check.

- [ ] **Step 3: Add the dedicated lane and shared bounded builder**

```python
# app.py
health_read_lane = BoundedReadLane("health-read", capacity=1)
app.state.health_read_lane = health_read_lane

# health.py
async def _health_response(request: Request, *, probe: bool) -> JSONResponse:
    try:
        checks, overall = await request.app.state.health_read_lane.run(
            _build_health_checks, store, settings, time.time(), runtime, read_health,
            timeout_s=0.8,
        )
    except (ReadLaneSaturatedError, TimeoutError, sqlite3.Error):
        checks = {"runtime:health_read_lane": [_health_read_failure_check(reason)]}
        overall = "fail"
    return JSONResponse(_build_health_body(...), status_code=(200 if probe else 503), ...)
```

The failure check must identify the reason as `read-model-saturated` or `read-model-unavailable`, state the P1 impact, automatic action, and operator action in credential-free text.

- [ ] **Step 4: Run focused tests and the health suite**

Run: `uv run pytest tests/m1-perception/test_health_endpoint.py -q`

Expected: PASS, including the two new failure-contract tests and existing `/healthz` always-200 tests.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/http/app.py src/polyarb/http/health.py tests/m1-perception/test_health_endpoint.py docs/superpowers/plans/2026-08-10-bounded-health-read-lane.md
git commit -m "fix(m1): bound health evidence reads"
```

