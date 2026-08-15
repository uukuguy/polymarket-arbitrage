# Structure materializer turn budget

## Evidence

Each continuous Gamma source window currently seals about 208 event pages. The
v3 materializer processes four pages per durable batch, so one window needs 52
fenced materializer turns. Windows arrive every five minutes. With only the
base scheduler round-robin turn, eight recovered windows advanced by roughly
one batch per two minutes, so the queue grows despite the independent Structure
range budget.

## Decision

Add `structure_materializer_turns`, defaulting to zero, to the transactional
scheduler and its `tick-once`/`serve` CLI. A tick retains all base workers,
then executes the configured extra materializer turns serially before the
existing extra Structure-range turns.

## Invariants

- No local concurrency is added; every extra turn still claims a Postgres lease.
- Default zero preserves existing behavior.
- Only `structure-source-materialize` receives the new capacity; source fetch,
  certification, Quote, and alert behavior are unchanged.
- Non-negative validation applies to both optional budgets.
- Staging will start at eight extra materializer turns, matching the evidence
  backed range budget, and the result must be measured before any further change.
