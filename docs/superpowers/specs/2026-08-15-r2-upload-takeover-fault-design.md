# Staging R2-upload-before-receipt takeover fault

## Purpose

The final M1 cloud acceptance gate needs a real, non-production process-loss
boundary after a Structure range or Quote batch artifact has been uploaded and
HEAD-verified in R2, but before its fenced Postgres receipt exists. A normal
machine restart cannot reliably hit this few-second interval.

## Decision

`serve` and `tick-once` receive an optional exact job-key fault target plus a
literal acknowledgement: `staging-r2-upload-before-receipt`. Only when both
are present does the matching Structure or Quote worker raise
`KeyboardInterrupt` at that exact boundary. The exception intentionally escapes
the normal retry handler, terminating the service without a receipt.

The staging operator removes both options from the machine command immediately
after Fly records the stop; the replacement worker then waits for the existing
lease to expire and reclaims the frozen input. No persistent fault flag, secret,
pointer mutation, or implicit environment toggle is introduced.

## Safety invariants

- Default is disabled; a missing target creates no callback.
- A target without the literal acknowledgement is rejected.
- The callback receives the claimed lease and can affect only an exact job key.
- It runs after R2 upload/HEAD verification and before receipt insertion.
- `KeyboardInterrupt` is deliberately not caught by worker `except Exception`
  blocks, so the test is genuine process loss rather than a retry simulation.
- This is staged only. Production commands never receive the target/ack pair.

## Acceptance evidence

For each of `structure-normalize` and `quote-batch`: R2 artifact exists,
original receipt is absent at crash, replacement lease has a higher epoch within
120 seconds, exactly one durable receipt exists after recovery, old certified
truth remains readable, and the circuit/incident recovery record is durable.
