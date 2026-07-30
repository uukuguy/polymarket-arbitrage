# Scoped Upstream Fault Control for Production Qualification

**Status:** approved design  
**Date:** 2026-07-30  
**Scope:** M1 opportunity-first Task 8, Gamma/CLOB/Telegram upstream faults  
**Boundary:** observer-only; no wallet, signing, balance, order, or trade path

## 1. Decision

Build a permanent, production-grade, in-process fault-control boundary around
typed Gamma, CLOB, and Telegram client calls.

The system is not temporary qualification scaffolding. It remains part of the
final production platform, normally dormant and pass-through. Paper,
observer-only, qualification, canary, and eventual real-money operation use the
same client paths with different authority levels.

The project-wide standard is:

> Future design defaults to the final production platform. Lower-authority
> modes exercise the production architecture; they do not justify disposable
> parallel systems.

This design deliberately does not create a generic HTTP proxy, sidecar, or
separate Fly proxy service. A canary sidecar may later test DNS, TLS, connection
reset, or regional network failures, but it is not the initial production data
path.

## 2. Goals

1. Inject bounded, attributable Gamma, CLOB, and Telegram failures without
   changing the normal production network topology.
2. Guarantee that missing, invalid, expired, or damaged fault-control state
   cannot inject a fault.
3. Restore the data plane before attempting to persist cleanup evidence.
4. Prove detection, containment, business recovery, cleanup, and final verdict
   from durable, independently readable evidence.
5. Preserve exact release, machine, boot, fault, target, timing, authorization,
   and recovery identity for later audit.
6. Keep host, storage, process, and deployment failure primitives separate from
   upstream API faults.

## 3. Non-Goals

- Arbitrary URL matching, response scripting, shell execution, or packet
  manipulation.
- DNS, TLS, routing, regional outage, or kernel-level network simulation.
- Combining SQLite lock, disk pressure, host load, daemon restart, or deploy
  interrupt into the upstream transport controller.
- Automatically enabling production mutations after deployment.
- Treating process restart, HTTP responsiveness, or log messages as business
  recovery.
- Claiming that the current observer-only cloud runtime is real-money
  production.

## 4. Why the In-Process Typed Boundary

### 4.1 Chosen: typed in-process transport wrapper

The wrapper executes in the existing client boundary. With no valid active
intent, it performs a direct pass-through and adds no network hop. It can
deterministically model the upstream semantic failures currently required:
timeout, latency, HTTP 429, malformed payload, cursor inconsistency, missing
book leg, and Telegram delivery failure.

It provides the smallest blast radius, the clearest call-class attribution,
and the least change to eventual production topology.

### 4.2 Deferred: same-host sidecar

A sidecar better approximates connection and network failures, but introduces a
new always-on dependency, process supervisor, port, startup-order contract,
resource budget, and cleanup failure domain. It is appropriate for later
canary or staging network qualification, not the primary production path.

### 4.3 Rejected for now: separate Fly proxy

An independent service gives stronger fault-domain isolation and could serve
multiple clients, but changes DNS/routing/authentication topology and adds its
own deployment, availability, cost, and security contracts. The current
single-platform qualification scope does not justify it.

## 5. Safety Semantics

Data-plane safety and qualification truth use different failure semantics:

- **Fault control unavailable or invalid:** pass through to the real upstream.
  Control failure must not stop eventual live market perception.
- **Qualification evidence unavailable or invalid:** return a FAIL verdict.
  Missing proof must never authorize promotion.

The wrapper injects only when all of the following match:

1. fault capability is present in the deployed build;
2. the fault family is explicitly enabled;
3. a valid single-use authorization exists;
4. the intent matches the current release, machine, and boot;
5. the intent targets the exact typed call class;
6. the nonce has not been used;
7. the monotonic deadline has not expired; and
8. no other fault is active.

Any mismatch is pass-through plus an auditable rejected-control fact. It is not
an injected failure.

## 6. Components

### 6.1 Fault Controller

`FaultController` owns one immutable in-memory `ActiveFault`. Normal client
calls consult only this snapshot; they do not read SQLite or call a remote
control service.

The controller:

- validates the typed intent;
- atomically admits at most one active fault;
- derives a monotonic expiry deadline;
- exposes a read-only match decision to typed adapters;
- clears the active fault before recording cleanup; and
- starts in pass-through mode after every process start.

An old `armed` or `injected` durable intent is never rearmed on restart. Startup
records it as abandoned/cleaned when the evidence store is writable while
remaining pass-through regardless of evidence-store availability.

### 6.2 Typed Fault Policies

The controller accepts enumerated call classes rather than arbitrary URLs:

- Gamma event page;
- Gamma market page or point lookup where explicitly planned;
- CLOB Candidate book batch;
- CLOB Discovery book batch where explicitly planned; and
- Telegram opportunity-card delivery.

Each policy owns its allowed fault modes and parameters. Gamma response
construction is not reused for CLOB or Telegram.

Examples:

- Gamma: bounded delay/timeout, malformed page, cursor inconsistency;
- CLOB: bounded delay/timeout, 429, selected missing leg;
- Telegram: deterministic delivery failure for one exact outbox identity.

The selected target must be narrower than the component whenever possible. A
Gamma cursor fault must not affect Gamma point lookups. A Discovery CLOB fault
must not affect the Candidate hot path.

### 6.3 Control API

Operator commands use a dedicated fault authority over the existing HMAC,
deadline, nonce, and replay-protection patterns. Ordinary perception controls
cannot arm a fault.

The control payload is bounded and schema-validated before mutation. It carries
only whitelisted fault parameters. Secrets, headers, cookies, raw URLs, query
strings, Telegram tokens, and response bodies are never persisted.

Every executable operation is exposed through a documented
`make <verb>-<noun>` target.

### 6.4 Durable Evidence Store

Append-only facts record:

1. authorization digest;
2. admitted or rejected intent;
3. injection receipt;
4. detected Incident identity;
5. containment evidence;
6. cleanup receipt;
7. component-specific recovery writer receipt; and
8. final evaluator verdict.

The stored identity includes fault ID, call class, normalized target, expected
release/machine/boot, timestamps, expiry, nonce digest, and canonical parameter
digest. Sensitive upstream material is excluded.

Historical facts are immutable. Current state is a deterministic projection of
the append-only chain.

### 6.5 Independent Evaluator

The evaluator is read-only and cannot arm, extend, clean, or close a fault. It
binds one unique intent to one Incident lifecycle, one cleanup result, one
business recovery receipt, and one runtime identity.

The wrapper and the affected component never declare their own qualification
PASS.

## 7. Fault Lifecycle

The only successful lifecycle is:

```text
authorized
  → armed
  → injected
  → detected
  → contained
  → cleaned
  → recovered
  → verified
```

Terminal failure states include rejected, expired, abandoned, cleanup-failed,
recovery-timeout, evidence-invalid, and escalated.

Rules:

- state transitions append facts and never overwrite history;
- only one active fault exists across all upstream families;
- an expired fault cannot be extended in place;
- a new release or boot invalidates the authorization;
- cleanup always disables injection before persisting its receipt;
- only the process that owns the in-memory controller may persist the cleanup
  receipt; a remote control API may request, wait for, and idempotently confirm
  cleanup, but may not claim that it cleared another process's memory;
- a cleanup receipt failure leaves production pass-through but qualification
  failed and later injection frozen;
- cancellation and timeout run the same cleanup path;
- startup remains pass-through even when stale intent cleanup cannot be
  recorded; and
- a later manual note cannot replace a missing machine fact.

## 8. Recovery Truth

Upstream reachability is necessary but not sufficient. Each fault requires a
new component-specific writer fact after cleanup:

- Candidate CLOB fault: a complete Candidate quote batch with the current
  membership hash;
- Discovery Gamma/CLOB fault: a successful bounded Discovery batch and its
  cursor/coverage receipt;
- Reconciliation-related Gamma fault: an advancing or terminal reconciliation
  checkpoint;
- Telegram failure: the exact opportunity outbox item receives a durable
  delivered attempt;
- generic HTTP recovery where explicitly allowed: a release-bound probe
  receipt, never process liveness alone.

The recovery fact must occur after injection and belong to the same
release/machine/boot unless the fault contract explicitly requires a new boot.

## 9. Error Handling

Before injection:

- reject a non-green baseline;
- reject open Incidents, an active fault, identity mismatch, replayed nonce,
  unsupported target, or invalid TTL;
- reject cross-membership Quote batches or orphan collecting runs; and
- run the production-image primitive check when the adapter depends on image
  tools.

During injection:

- enforce a bounded timeout;
- accept exactly one matching Incident;
- fail on ambiguous Incident identity;
- clean up immediately if detection or containment misses its deadline; and
- preserve partial evidence without promoting it to PASS.

After injection:

- require cleanup before waiting for business recovery;
- freeze the matrix if cleanup cannot be proven;
- require a complete Incident history and authentic recovery writer receipt;
- reject any cross-membership, partial publication, or orphan-run evidence; and
- evaluate from immutable evidence only.

## 10. Qualification Layers

1. **Model tests:** lifecycle, TTL, nonce, runtime identity, one-active-fault.
2. **Adapter tests:** exact call-class scoping and unaffected-call
   pass-through.
3. **Lifecycle tests:** cancellation, timeout, restart, evidence-store failure,
   and cleanup ordering.
4. **Image verification:** required production-image commands and process
   assumptions.
5. **Cloud read-only baseline:** release, machine, boot, health, Incident,
   resource, and data-integrity facts.
6. **Authorized single-fault qualification:** one fault at a time through full
   recovery.
7. **Post-matrix continuous window:** a new clean baseline followed by 24 hours
   of continuous evidence.

A synthetic fixture proves only evaluator determinism. It never qualifies a
cloud release.

## 11. Promotion Gate

A release is eligible for eventual production promotion only when:

- HTTP and Candidate freshness meet the locked SLOs;
- Discovery and Reconciliation make bounded, durable progress;
- MTTD, containment, cleanup, and recovery meet their deadlines;
- cross-membership Quote batches, partial publication, and orphan collecting
  runs are zero;
- every fault has one attributable intent, Incident, cleanup receipt, and
  component recovery receipt;
- the continuous window has unambiguous release/boot/config identity;
- Dashboard, operator runbook, monitoring, and alert delivery pass acceptance;
  and
- qualification never touches wallet, signing, balance, order, or trade
  state.

In eventual real-money operation, mutation authority is disabled by default.
New releases and material dependency changes first qualify in a canary or
qualification environment. Production retains the same dormant wrapper and
audit reader, allowing tightly authorized, low-blast-radius exercises without
replacing the production data path.

## 12. Separate Primitive Families

The following retain independent implementations and cleanup contracts:

- SQLite lock;
- disk pressure;
- host CPU/load contention;
- Candidate/Discovery process exit;
- Reconciliation process stall;
- daemon restart; and
- deploy interrupt.

They may share the immutable evidence envelope and evaluator vocabulary, but
they do not share an injection implementation with the upstream controller.

## 13. Future Network Qualification

A later canary/staging phase may add a same-host sidecar for DNS, TLS,
connection-reset, and network-partition behavior. It must remain outside the
steady-state production dependency chain unless independent evidence proves
that the additional hop and failure domain improve total system reliability.

A shared external proxy is deferred until multiple independently deployed
services need coordinated network fault qualification and the project has
explicitly accepted its availability, authentication, routing, and operational
cost.

## 14. Implementation Planning Boundary

The implementation plan must:

- begin with RED contracts for controller admission, pass-through safety,
  cleanup ordering, and exact typed scoping;
- define additive append-only evidence schema and deterministic projections;
- reuse existing HMAC/deadline/nonce patterns without sharing ordinary control
  authority;
- add Makefile targets and operator documentation with every executable
  command;
- preserve disabled-by-default production configuration;
- keep user-owned planning files untouched;
- complete local and read-only gates before requesting any production
  authorization; and
- request separate authorization for each exact release and fault mutation.
