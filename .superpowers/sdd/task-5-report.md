# Task 5 Implementer Report

Status: DONE — independent review findings repaired; pending re-review

## Scope

Task 5 only: durable incident lifecycle, component-authentic recovery proof,
resource shedding/hysteresis, isolated producer subprocesses, bounded restart
and strict-health truth chain. No Task 6 API/Dashboard/cutover, deployment,
wallet, signing, balances, orders or real-money execution.

## Truth chain

1. Incident events are append-only. Detection deduplicates an active
   scope/kind; transitions serialize under `BEGIN IMMEDIATE`, replay exact
   sequence/scope/kind/time and reject invalid lifecycle edges.
2. `verified` re-reads real producer tables. Candidate binds group, current
   certified membership, complete Quote batch and atomic candidate fact strictly
   beyond recovery row anchors; Discovery validates its
   complete cursor history and latest advancing batch; Reconciliation validates
   the current exact window/checkpoint. HTTP proof can only be written by the
   bounded loopback probe, which disables proxies and binds the actual JSON
   release ID plus recovery nonce and row anchor. All evidence must postdate
   recovery.
3. Resource samples reject nonfinite/negative values. Every decision replays the
   complete append-only sample/decision chain, exact policy version, sequence,
   source sample, inputs, transition anchor and TTL before runtime or health can
   consume it. Corrupt, stale or forged decisions fail closed.
4. Shedding order is Reconciliation off, Discovery batch/duty down, normal
   Candidate slower. High Candidate and HTTP multipliers stay 1. Empty
   Candidate expands Discovery but persists `health_claimed=false`. Discovery
   pressure alone cannot slow Candidate. Cooldown is anchored only to an actual
   mode transition, and the persisted duty multiplier drives real runner delay.
5. Enabling isolation prevents Candidate, Discovery, Reconciliation and legacy
   in-process producer tasks from executing in HTTP. The only production child
   argv values are `PRODUCER_COMMANDS`; tests inject commands through a private
   supervisor seam rather than weakening `ProducerSpec`.
6. stdout/stderr are drained continuously, tail-bounded to <=16 KiB and common
   credential forms are redacted. Timeout and cancellation always terminate,
   wait for grace, then kill if needed before a terminal receipt is committed.
7. Only heartbeat progress bound to the exact current supervisor run and child
   nonce extends a stall deadline. Current-child starts and terminal receipts
   feed strict liveness health; parent/old-child heartbeats cannot keep a wedged
   child alive, and exit code zero is still unexpected producer disappearance.
   Restart count and exponential backoff are bounded; retry exhaustion durably
   escalates.
8. `perception:open_incidents`, per-producer liveness and
   `perception:resource_mode` read and validate
   the exact same SQLite mutations. Candidate/HTTP incidents can fail overall
   strict health; background incidents remain scoped warnings. Uvicorn bind,
   early task exit or readiness timeout creates a durable escalated HTTP incident,
   cleans the server task and returns nonzero before any producer starts.

## Verification

```text
Focused lifecycle/resource/supervisor: 176 pass
Proportional perception + strict health + daemon wiring/shutdown: pass
Full repository: 2590 collected, 100% pass (1 expected xfail, 1 skip)
Ruff changed scope: pass
compileall: pass
make docs-m1-check: pass
git diff --check: pass
make planning-status: 82 plans, no drift
git hooksPath: .githooks
python:3.12-slim subprocess primitives: 3.12.13 True True True
```

`make chaos-l2-fly-image-check` could not pull the private registry digest
(`NAME_UNKNOWN`) in this local environment. The public exact base image check
proved all primitives used by this implementation; it intentionally does not
use `pkill`, `ps`, `dig`, `ping` or `which`.

## Remaining boundary

Task 6 owns public opportunity API/operator workflow, Task 7 Dashboard views
and Task 8 production qualification/cutover. All new flags remain false and
nothing was deployed.
