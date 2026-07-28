# Task 5 Implementer Report

Status: DONE — formal re-review findings repaired; ready for final re-review

## Scope

Task 5 only: durable incident lifecycle, component-authentic recovery proof,
resource shedding/hysteresis, isolated producer subprocesses, bounded restart
and strict-health truth chain. No Task 6 API/Dashboard/cutover, deployment,
wallet, signing, balances, orders or real-money execution.

## Truth chain

1. Incident events are append-only. Detection deduplicates an active
   scope/kind; transitions serialize under `BEGIN IMMEDIATE`, replay exact
   sequence/scope/kind/time and reject invalid lifecycle edges.
2. `verified` re-reads real producer tables. Candidate requires a dedicated
   append-only success receipt written in the same SQLite transaction as the
   complete Quote batch and terminal fact. The receipt binds the current
   certified group/event/membership and exact group/quote/fact row IDs with a
   canonical hash strictly beyond the recovery receipt anchor; independently
   publishing a quote and fact cannot prove recovery. Discovery validates its
   complete cursor history and latest advancing batch; Reconciliation validates
   the current exact window/checkpoint. HTTP proof can only be written by the
   bounded loopback probe, which disables proxies and binds the actual JSON
   release ID plus recovery nonce and row anchor. All evidence must postdate
   recovery.
3. Incident event clocks are monotonic within the same serialized transaction.
   Resource samples reject nonfinite/negative values. Every decision replays the
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
   supervisor seam rather than weakening `ProducerSpec`. Disabled producers do
   not read or require resource evidence and do not appear in strict liveness
   health.
6. stdout/stderr are drained continuously, tail-bounded to <=16 KiB and common
   credential forms are redacted. Timeout and cancellation always terminate,
   wait for grace, then kill if needed before a terminal receipt is committed.
7. A child generates the heartbeat authority preimage after spawn; SQLite stores
   only its domain-separated hash. Only heartbeats that prove that preimage and
   bind the exact current supervisor run/attempt extend a stall deadline.
   Attempts are reserved transactionally before spawn; restart reconciliation
   closes abandoned reservations as `spawn-error` before allocating the next
   attempt. Liveness replays the full historical start/heartbeat/receipt chain,
   so old corruption fails closed. Read errors or marker status changes never
   extend the deadline; heartbeat count, sequence and timestamp must all
   strictly advance. Receipt replay enforces `success=0`, `nonzero!=0`, and
   `timeout/cancelled/spawn-error=None`. Exit code zero is still unexpected
   producer disappearance. Restart count and exponential backoff are bounded;
   retry exhaustion durably escalates.
   Receipt output tails must remain UTF-8 encodable strings of at most 16 KiB;
   a writer-time integrity hash makes SQLite affinity conversions and later
   output mutation detectable during full-history replay.
8. `perception:open_incidents`, per-producer liveness and
   `perception:resource_mode` read and validate
   the exact same SQLite mutations. Candidate/HTTP incidents can fail overall
   strict health; background incidents remain scoped warnings. Uvicorn bind,
   early task exit or readiness timeout creates a durable escalated HTTP incident,
   cleans the server task and returns nonzero before any producer starts.

## Verification

```text
Focused lifecycle/resource/supervisor: 198 pass
Proportional perception + strict health + daemon wiring/shutdown: 316 pass
Full repository: 2625 collected, 2623 pass (1 expected xfail, 1 skip)
Ruff changed scope: pass
compileall: pass
make docs-m1-check: pass
git diff --check against review baseline 114f30e and working diff: pass
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
