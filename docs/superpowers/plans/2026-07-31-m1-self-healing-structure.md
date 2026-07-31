# H-011: Self-Healing Structure Synchronization Plan

## Scope

Replace the deployed full-universe, timeout-to-`PAUSED` Structure job with a
durable page-window producer.  Preserve the existing atomic `snapshots` / `markets`
publication contract and do not touch trading, wallet, or production deployment.

## Delivery order

1. **Control-plane stopgap (this commit):** migrate the scheduler state machine
   from terminal `PAUSED` to active `RECOVERING`; preserve the five-failure alert
   threshold and only clear recovery on a certified snapshot.  Historical
   persisted `PAUSED` rows migrate on daemon start.
2. **Durable window store:** add additive SQLite tables for one active Structure
   window and idempotent staged Gamma event/market rows.  Provide transactional
   APIs: create-or-resume, commit one page plus successor cursor, inspect
   progress, and terminalize a failed window.  No table writes `markets`.
3. **Bounded producer:** add `fetch_active_market_page` alongside the existing
   `fetch_active_event_page`; the Structure runner executes precisely one stage
   page per child invocation, retaining opaque cursors.  Restart tests prove no
   page skip or duplicate effect.
4. **Atomic finalizer:** after both streams complete, apply the current source
   coverage/membership validators to staged facts and atomically publish one
   `data_product='structure'` snapshot.  A validator failure keeps current
   `markets` intact and records recovery evidence.
5. **Observability/operator path:** expose window stage, page counts, cursor
   presence, retry time, and recovery state in strict health and the existing
   read-only status target.  Alert recovery and publish confirmation separately.
6. **Qualification:** deploy only after all local gates.  Record release id and
   execute read-only proof of a fresh certified Structure revision, a Quote run
   bound to it, then a 24-hour SLA observation.  No claim of production readiness
   before that evidence.

## Verification contracts

- `FAILURE_THRESHOLD` failures still issue one incident alert but make the next
  bounded attempt; a successful certified result resets state to `RUNNING`.
- a process crash before page commit advances neither cursor nor staged facts;
  one after commit resumes at exactly the persisted successor cursor.
- partial staging is never visible through `markets`, health certification, Quote,
  or M2 opportunity output.
- `make sync-structure-local` remains the sole local mutation entry point; the
  planned read-only `make structure-sync-status` is added with its implementation.

## Chain truth

| Writer | Durable fact | Reader/gate | E2E test |
|---|---|---|---|
| page worker | window cursor + staged page transaction | next child invocation | restart continues exact cursor |
| finalizer | certified Structure snapshot + `markets` | market truth health / Quote store | staged-only data cannot be consumed |
| scheduler | `RECOVERING`, attempts, counter | health / Polywatch | threshold then success produces recovery |

