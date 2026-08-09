# M1 Incident Read Lane — Summary

Date: 2026-08-10

## Problem

During production Quote collection, the public bounded incident read model
intermittently returned HTTP 503 `read-model-unavailable`. The durable incident
rows were present; the read request had been starved behind producer work in
the shared default executor before its absolute SQLite deadline.

## Change

- Added a dedicated, capacity-bounded `perception-read` lane to the HTTP app.
- Routed all bounded perception readers through that lane rather than
  `asyncio.to_thread`'s shared default executor.
- Propagated `contextvars` into `BoundedReadLane` workers so the existing
  absolute SQLite deadline and interrupt chain remains active.
- Kept overload explicit: a full read lane returns HTTP 503
  `read-model-saturated`; it never queues unbounded work or reports zero
  incidents.

## Verification

- RED: the new default-executor-starvation test returned HTTP 503 before the
  lane existed.
- GREEN: the same test returns HTTP 200 with the default executor blocked.
- `uv run pytest tests/m1-perception/test_perception_http.py::test_perception_read_uses_dedicated_lane_when_default_executor_is_blocked tests/m1-perception/test_arbitrage_opportunities_http.py::test_bounded_read_lane_propagates_request_context_to_its_worker -q` → `2 passed`.
- `uv run ruff check src/polyarb/http/opportunity_read_health.py src/polyarb/http/app.py src/polyarb/http/perception.py tests/m1-perception/test_perception_http.py tests/m1-perception/test_arbitrage_opportunities_http.py` → passed.

## Remaining gate

Deploy this commit and sample the production incident endpoint across several
active Quote cycles. The Vercel Dashboard 302 remains a separate operator
visibility fault; the direct Fly incident console work follows this stability
repair.
