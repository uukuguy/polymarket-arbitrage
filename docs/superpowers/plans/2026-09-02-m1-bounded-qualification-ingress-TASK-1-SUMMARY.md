# M1 bounded qualification ingress — Task 1 Summary

## Outcome

Routine high-frequency lifecycle events no longer duplicate into the
qualification ingress ledger. Qualification retains failure, recovery, and
other exceptional evidence needed to decide whether a release is safe.

## Implementation

- Migration `041` replaces the runtime-to-qualification trigger function.
- Normal `started`, `stage-changed`, and `succeeded` events for
  Structure fetch/normalize/materialize and Quote batch jobs are omitted from
  the qualification ledger.
- The current runtime-state table remains the live source for those jobs; the
  immutable runtime event stream remains unchanged in this task.
- All other runtime events, including retryable and terminal failures, still
  call the hardened qualification-ingress function.
- The control-plane schema contract now requires revision `041`.

## Verification

- Migration source contract test passes.
- A PostgreSQL integration test inserts both a normal Quote-batch stage event
  and a terminal failure. Only the failure appears in qualification ingress.

## Production follow-up

Apply migration `041` with the direct administrative DSN, verify the function
body and ingress rate in production, then implement bounded qualification
cursor/replay behavior before re-enabling the qualification worker.
