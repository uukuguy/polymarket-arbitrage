# Task 8 Summary — actionable Quote supervisor incidents

## Outcome

The bounded perception API now classifies failed Quote supervisor children
(`quote` / `child-*`) as P1 incidents instead of leaving their recovery state
as opaque raw evidence. The Dashboard promotes both collector and supervisor
Quote failures to the same P1 panel.

## Operator contract

- A recovering supervisor shows `retry-supervised-producer`.
- An escalated supervisor shows `automatic-retries-exhausted` and directs the
  operator to inspect the bounded producer receipt and restart the producer.
- The P1 card renders failure reason, retry state, next retry and durable
  recovery evidence; the full bounded incident list remains below it.

## Verification

- RED: the new supervisor disposition API test returned `diagnosis: null` and
  the Dashboard source test could not find `quote` scope in the P1 filter.
- GREEN: 24 focused perception API/Dashboard contract tests passed.
- `pnpm exec tsc --noEmit` and `pnpm run build` passed in `dashboard/`.
- `make planning-status` reported no drift before commit.

## Production follow-up

Deploy Fly and Vercel from the same commit, then verify the retained real
`quote` supervisor incidents render an actionable P1 disposition rather than
an unclassified open row.
