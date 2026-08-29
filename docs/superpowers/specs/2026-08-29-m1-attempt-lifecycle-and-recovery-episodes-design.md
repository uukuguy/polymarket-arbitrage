# M1 Attempt Lifecycle and Recovery Episodes Design

Date: 2026-08-29
Status: approved by the user's standing autonomous repair authorization
Scope: M1 production control-plane runtime only

## Problem

M1 already centralizes lease, heartbeat, progress, attempt, provider-I/O and
terminal-drain policy, but production evidence exposed three remaining
authority leaks:

1. a failed long-lived Gamma HTTP transport is reused by the next durable job
   attempt, so a connection-pool failure can deterministically consume every
   retry even though a fresh process reads the exact page in 0.115 seconds;
2. `structure-fetch` records no stage until an I/O operation has completed, so
   a fetch timeout and an R2 upload timeout can share the same failure identity;
3. recovery budgets are keyed only by `(controller, target type, target ID)`,
   so a repaired executable and a new failure episode inherit an exhausted
   budget from the old defect.

The observed target is
`structure-source:300:5959460:fetch:events:162`. Attempts 8–10 failed at the
same pre-progress timeout boundary and legitimately reopened the circuit.
A fresh local read completed in 0.549 seconds and a fresh Fly Machine in the
same app/region/image completed in 0.115 seconds. The page, cursor, image and
Fly egress are therefore healthy; the differing variable is transport
lifetime.

Production rollout disproved the narrower form of that diagnosis. A fresh
revision-035 transport did replace the old generation, yet three coordinator
attempts still reached the 29-second worker-I/O boundary. The same release,
cursor, provider policy, app and Fly region fetched 100 records in 0.164
seconds in an isolated Machine. A read-only production probe measured eight
sequential claim-shaped database reads in 2.292 seconds, so neither the page
nor one database query is intrinsically slow.

The remaining differentiator is coordinator scheduling. Its source pool runs
eight asynchronous lanes and its materializer budget adds eight more turns.
Several `async run_once()` implementations execute synchronous `claim_job()`
on the event-loop thread before their first `await`. Those calls serialize
connection bootstrap, query and row-lock work across otherwise independent
lanes. Under live contention they can prevent both the provider socket and its
15-second timer from being serviced until the outer 29-second timer is already
due. This is an event-loop ownership defect, not a reason to enlarge a timeout.

## Chosen architecture

One durable attempt owns one ordered lifecycle:

```
claim -> stage-start -> provider I/O -> stage-complete -> terminal receipt
             |              |
             +-- failure ---+-> close transport generation -> retryable fact
service stop ------------------> interrupted fact -> immediate fenced resume
```

The absolute attempt deadline remains the business-work ceiling. Provider
timeouts bound one provider operation and terminal grace bounds only shutdown.
No scheduler or operator wrapper may invent another attempt deadline.

### Transport generations

`GammaClient` retains its rate limiter across requests but owns a replaceable
HTTP transport generation. After any failed durable source attempt, the worker
closes that generation within the existing central I/O envelope and installs a
fresh client before the next claim. A close timeout cannot block replacement;
the abandoned generation is never reused.

The worker resets after the attempt fails, not inside `GammaClient._get`.
Consequently one provider request still has exactly one inner attempt, durable
retry remains the sole retry authority, and malformed/validation failures do
not gain hidden HTTP retries.

### Stage truth

Every externally blocking Structure source stage persists `(stage, 0, 1)`
before work and `(stage, 1, 1)` after success. The failure fingerprint uses the
last durable stage together with exception type and code site. Runtime events,
incidents and the circuit therefore distinguish `fetch-page`, `upload-page`
and terminal database failures without storing exception messages or provider
bodies.

### Recovery episodes

Recovery budgets become episode-scoped:

- job episode: current `attempt_id`;
- circuit episode: current secret-free `failure_fingerprint`;
- process or Machine episode: the exact incident/fence identity already
  carried by the scheduled action.

The persisted key is
`(controller_id, target_type, target_id, episode_key)`. Existing target-only
rows migrate to an immutable `legacy` episode; no counts are deleted or
replenished. A new episode receives the existing policy budget of three. The
same fingerprint can still consume at most three actions, while a repaired
transport generation no longer inherits an unrelated exhausted episode.

Observe records include the episode key so replay and live scheduling resolve
the same budget. Action detail records the episode key; scheduling, budget
consumption and idempotency validate it in one transaction.

### Non-blocking claim boundary

Every worker claim is database I/O and therefore uses the shared cancellable
blocking bridge before an asynchronous worker starts its attempt runtime.
There is no per-worker timeout around the claim: connection, statement and
lock deadlines remain the sole database authorities. Cancellation follows the
same two-step service-stop contract as all other nonterminal blocking calls.

Synchronous workers continue to be bridged as a whole by `run_worker()`.
Asynchronous workers may execute provider I/O on the event loop only after all
synchronous database/R2 work has crossed an explicit bridge. A static coverage
test rejects direct `.claim_job()` calls inside the audited async worker
modules, and a behavioral test blocks a fake claim while proving an unrelated
loop task continues to run.

### Fan-in commit independence

A range or batch producer owns only its receipt and terminal attempt state. It
must not wait for the shared certifier row merely to commit those facts. Direct
successor wakeup is therefore a best-effort optimization: the producer first
checks that the successor exists, then acquires it with `FOR UPDATE SKIP
LOCKED`. A busy row skips the direct wake without changing producer outcome.

Every certifier turn repairs at most one ready waiting successor from committed
inputs, receipts and producer terminal states before claim. This bounded repair
is the durable convergence authority. It closes the concurrent-final-receipt
lost-wakeup case without turning the successor row or the one-second database
lock bound into a synchronized failure point for all producers.

### Cross-shape generation backpressure

One Structure generation changes durable shape as it moves through the DAG:
one `structure-materialize` job becomes many `structure-normalize` jobs plus a
waiting `structure-certify` job. Admission must count all three unfinished
forms. Counting only ranges creates a false zero before materialization and
after the last range but before certification.

Materializer claim is a second authority because source windows may already be
durably admitted before a later backpressure configuration takes effect. Only
the oldest unfinished materializer may claim, and it remains ineligible while
any range or certifier from a prior generation is unfinished. The transition
from successful materializer to ranges/certifier is already atomic, so this
predicate closes the remaining gap without another timer or mutable counter.

## Sequencing and interruption invariants

1. The eight-job DAG in `runtime_deadlines.py` remains the only stage order.
2. Scheduler cadence may start an idle lane but cannot cancel a running sibling
   merely because another lane finishes or times out.
3. SIGTERM first stops new claims, then requests cooperative cancellation.
   `finish_interrupted` releases the lease without incident/circuit mutation.
4. Checkpointed workers resume from the last fenced checkpoint; source-page
   work, which has no partial safe receipt, restarts only that single page with
   a new transport generation.
5. A recovery action claims its exact scheduled `action_id`; no generic
   operator turn may consume an older action.
6. A circuit probe changes only the job/circuit state and never publication
   pointers.
7. A slow claim may delay that worker's lease acquisition, but it cannot delay
   a sibling provider request, heartbeat, watchdog, signal handler or cadence
   clock on the shared event loop.
8. A busy certifier row may delay successor eligibility until its next repair
   turn, but it cannot roll back a producer receipt or terminal success.
9. A generation remains backpressure-visible while it is a materializer, a set
   of ranges or a certifier; changing shape cannot briefly authorize a sibling.

## Audit boundary

The implementation will produce a checked timeout/cancellation inventory for
all production M1 control-plane paths. Each boundary must name:

- authority: provider, DB, attempt, scheduler cadence, or terminal drain;
- source: the function or central policy that derives it;
- cancellation behavior;
- durable recovery point;
- proof test or fault-matrix case.

Duplicate attempt killers, relative clocks reconstructed from absolute facts,
unbounded waits and stage-less external I/O are blocking findings. Poll cadence
and transport bounds are not attempt deadlines and must be documented as such.

## Verification

The release is acceptable only when:

- tests first reproduce poisoned-transport reuse, pre-I/O stage ambiguity and
  target-only budget exhaustion;
- focused unit and real-PostgreSQL migration tests pass;
- the deterministic runtime fault matrix covers transport replacement,
  episode isolation, service interruption and exact probe execution;
- the complete M1 suite, Ruff, Pyright, climb and planning gates pass without
  an arbitrary outer timeout;
- a blocking-claim regression test proves event-loop liveness and production
  logs show the exact source page crossing fetch, validation, upload and the
  terminal receipt on the revised image;
- an exact production image is rolled sequentially with `SIGTERM/40s`;
- the exhausted legacy budget remains immutable, a new fingerprint episode
  owns its own budget, the exact source page succeeds, and downstream
  Structure/Quote/opportunity freshness resumes;
- a new 86,400-second qualification epoch starts. M1 is not complete until its
  immutable certificate is independently reverified.

## Rejected approaches

- Increasing the 29-second outer wait or retry count: masks a poisoned
  transport and repeats the single-point failure.
- Recreating every Gamma client for every successful request: removes useful
  connection reuse and makes the limiter per-request rather than service-owned.
- Resetting the existing target-only budget in place: erases history and makes
  later budget exhaustion unverifiable.
- Rewriting the scheduler: unnecessary for the proven failure and expands the
  production risk surface before the M1 certificate.
- Raising the 29-second worker-I/O bound: it would preserve the event-loop
  starvation and merely move the single-point failure later.
- Making the eight-lane pool sequential: it hides the unsafe boundary and
  discards deliberate exact-ID concurrency; the claim itself belongs off-loop.
