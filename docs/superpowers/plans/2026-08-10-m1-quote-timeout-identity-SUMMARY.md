# M1 Quote Timeout Identity — Summary

Date: 2026-08-10

## Outcome

Quote timeout incidents now carry the exact durable collection `attempt_id` that
was terminalized by the isolated collector. The failure path no longer presents
the last successful Quote `run_id` as if it belonged to a new timeout.

## Operator impact

`/perception/console` displays **Failed attempt** on both open and recent
recovered incident cards, alongside severity, impact, automatic action, next
operator action and recovery evidence. If a safe `quote_run_id` projection is
not available at the hard-timeout boundary, it remains empty rather than being
guessed.

## Design boundary

The hard-timeout reaper performs no post-termination SQLite read. It propagates
the attempt identity already held in memory, preserving the bounded cleanup
latency that protects the automatic retry path.

## Verification

- Focused quote-worker identity and hard-timeout tests.
- Perception console and incident-diagnosis HTTP contracts.
- Full quote-worker and perception HTTP suites, Ruff, and M1 docs validation.
