# M1 Recovery Isolation and Read-only Quote Activation

## Purpose

Make the M1 opportunity feed continuously usable after a Structure publication,
without weakening certified-truth gates.  Enable the already implemented
public-read-only Quote producer so the authenticated Structure universe can
produce durable opportunity candidates for M2.

## Evidence and root cause

On 2026-08-09, snapshot 885 was atomically published and market truth switched
to it.  The resident cleanup worker then held the shared producer lock in a
cleanup attempt without a deadline.  The scheduler could not acquire that lock
to initialize the required v4 drift comparison.  Strict health therefore
correctly failed with `structure-drift-progress-missing`; Polywatch sent the
Telegram alert.  Separately, the public opportunity endpoint returned 503
because `POLYARB_NEG_RISK_QUOTE_WORKER_ENABLED=false` means no certified quote
feed exists.

## Alternatives considered

1. **Recommended: bounded cleanup isolation.** Cleanup remains a low-priority,
   durable worker but each SQLite cleanup chunk has a hard deadline. On expiry,
   it records `backoff`, releases the producer lock, and retries later. Structure
   publication and drift certification take priority.
2. Let cleanup keep the lock until it completes. This preserves the existing
   code but permits indefinite failure of the certification chain; rejected.
3. Relax strict health while drift state is absent. This hides an unverified
   generation from M2 and defeats the purpose of the gate; rejected.

## Design

### Cleanup ownership and recovery

- Add a bounded cleanup execution budget to Settings, with a conservative
  production value below the Structure scheduler's admission budget.
- Execute cleanup in its existing worker thread behind the shared producer
  lock, but race it against that budget.
- On timeout, preserve the durable cleanup attempt as `backoff` with a stable
  `cleanup-timeout` error kind, schedule the normal retry delay, and release
  the lock immediately. The late thread result must not falsely mark success;
  a subsequent authenticated attempt owns the next state transition.
- The scheduler continues to initialize/advance the drift comparison once the
  lock becomes free. Existing strict health continues to fail until the new
  comparison has a durable authorized seal.

### Read-only Quote activation

- Set `POLYARB_NEG_RISK_QUOTE_WORKER_ENABLED=true` in the persisted Fly
  configuration and deploy through the normal `make deploy` path.
- The worker uses only public CLOB reads and the existing SQLite quote/candidate
  stores. No signer, wallet, order endpoint, secret, or execution capability is
  introduced.
- After a Structure publication, the worker wakes immediately; otherwise it
  refreshes at the existing 60-second cadence. The public endpoint remains
  fail-closed until the exact current certified universe has a complete fresh
  quote run.

## Acceptance evidence

1. Regression tests prove a timed-out cleanup cannot retain the producer lock,
   cannot overwrite a later attempt, and leaves a durable retry state.
2. A production publication auto-initializes and seals its drift comparison
   without manual database mutation; strict health returns no failure.
3. Production Quote runs are complete on the current certified universe and
   remain fresh for repeated intervals; `/arbitrage/opportunities` returns 200
   with a durable identity even when it contains zero profitable candidates.
4. Polywatch observes both a failure and recovery transition. M2 can read the
   same live, certified candidate identity without access to execution paths.

## Non-goals

- No trade execution, wallet integration, private keys, order signing, or
  changes to strategy thresholds.
- No relaxation of Structure, drift, quote freshness, or opportunity identity
  validation.
