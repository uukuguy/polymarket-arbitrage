# Task 5 Implementer Report

Status: DONE — pending independent review

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
   certified membership and complete Quote batch; Discovery validates its
   complete cursor history and latest advancing batch; Reconciliation validates
   the current exact window/checkpoint; HTTP binds a responsive <=2s probe to
   the expected release. All evidence must postdate recovery.
3. Resource samples reject nonfinite/negative values and production decisions
   re-check actual durable Candidate count/p95/missing Quote and producer
   incident state. Every sample and decision is append-only and history-valid.
4. Shedding order is Reconciliation off, Discovery batch/duty down, normal
   Candidate slower. High Candidate and HTTP multipliers stay 1. Empty
   Candidate expands Discovery but persists `health_claimed=false`. Cooldown
   prevents an immediate protect→normal flap.
5. Enabling isolation prevents Candidate, Discovery, Reconciliation and legacy
   in-process producer tasks from executing in HTTP. The only production child
   argv values are `PRODUCER_COMMANDS`; tests inject commands through a private
   supervisor seam rather than weakening `ProducerSpec`.
6. stdout/stderr are drained continuously, tail-bounded to <=16 KiB and common
   credential forms are redacted. Timeout and cancellation always terminate,
   wait for grace, then kill if needed before a terminal receipt is committed.
7. Only producer-written durable heartbeat progress extends a stall deadline.
   A parent timer cannot make a wedged child appear alive. Restart count and
   exponential backoff are bounded; retry exhaustion durably escalates.
8. `perception:open_incidents` and `perception:resource_mode` read and validate
   the exact same SQLite mutations. Candidate/HTTP incidents can fail overall
   strict health; background incidents remain scoped warnings.

## Verification

```text
Focused lifecycle/resource/supervisor: pass
Proportional perception + strict health + daemon wiring/shutdown: pass
Full repository: 2525 collected, 100% pass (1 expected xfail, 1 skip)
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
