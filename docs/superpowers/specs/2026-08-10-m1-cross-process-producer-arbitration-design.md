# M1 Cross-Process Producer Arbitration Design

## Problem

L1 runs Quote in an isolated supervised child, while Structure scheduling stays
in the HTTP parent. The parent's `QuoteWorkerRuntime` is not updated by that
child. Its `pipeline_due()` therefore remains true after cold start and the
Structure scheduler records `quote-pipeline-due` forever. This starves market
structure publication even though Quote itself is healthy.

The production evidence is unambiguous: Quote runs 2280–2288 advanced every
roughly 60 seconds with a fresh certified M2 feed and durable observer facts,
while the latest Structure snapshot remained more than five hours old and
`/opportunity-watch/status` correctly rejected its 30-minute market-map SLA.

## Decision

Use a SQLite-backed producer-arbitration lease as the single source of truth
between the supervised Quote child and parent-owned Structure scheduler. This
replaces the invalid process-local priority decision; it does not weaken Quote
freshness or construct opportunities from stale Structure truth.

The lease has a bounded owner (`quote` or `structure`), an expiry timestamp,
and an immutable transition receipt. Acquisition is serialized by
`BEGIN IMMEDIATE`. A process may only act while it owns an unexpired lease;
expired ownership is reclaimed with a terminal `expired-owner` receipt before
the successor is admitted.

## Arbitration policy

1. Quote starts by acquiring or renewing a `quote` lease for its bounded child
   window. It still uses the existing 180-second child deadline, 150-second
   fetch deadline, and 300-second freshness SLA.
2. Structure may acquire a `structure` lease only when no live Quote lease
   exists. Its lease is capped at 45 seconds, which fits inside the 60-second
   Quote cadence. A scheduler tick records a durable defer when Quote owns the
   window.
3. A due Quote encountering a valid Structure lease waits only until that
   lease expires, then records a bounded `structure-window-yield` handoff and
   runs immediately. It never spins or silently degrades.
4. Every acquisition, yield, expiry recovery and release is retained in a
   bounded receipt history. Health and the incident console expose the current
   owner, expiry age, last handoff and any missed deadline. A stale or malformed
   arbitration record fails the relevant producer health check.

## Why not the alternatives

- Removing Quote priority lets a 75-second Structure child overlap a Quote
  child and reintroduces SQLite persistence stalls that previously produced
  hard timeouts.
- Lowering Quote cadence reduces the M2 feed freshness rather than repairing
  coordination, and is unnecessary under the current 300-second SLA.
- Reusing the existing in-memory `asyncio.Lock` cannot coordinate separate OS
  processes and is the cause of the present starvation.

## Verification

- Unit tests prove cross-owner exclusion, expiry takeover, bounded Structure
  window, Quote immediate handoff and no process-local cold-start starvation.
- Integration tests prove a supervised Quote child and parent scheduler use the
  same durable lease state.
- Production evidence requires a new Structure publication, a subsequent
  certified Quote run, fresh opportunity feed, no open P1/P2 incident, and
  Polywatch/console visibility of each arbitration decision.

## Safety boundaries

The system remains observer-only: no wallet, signing, orders, or production
data mutation outside its existing normal SQLite producer writes. Arbitrated
Structure work remains bounded and Quote continues fail-closed once its
certified feed exceeds the 300-second SLA.
