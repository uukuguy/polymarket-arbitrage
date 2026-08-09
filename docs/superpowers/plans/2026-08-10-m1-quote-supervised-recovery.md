# M1 Quote Supervised Recovery Plan

## Goal

Replace unbounded in-process Quote retry after repeated hard child timeouts with
a bounded, durable supervisor-owned recovery cycle. Operators must be able to
see every failed Quote child, restart decision, retry budget and either a
certified recovery or explicit escalation.

## Constraints

- Reuse the existing `ProducerSupervisor` receipt, heartbeat, redaction and
  incident lifecycle; do not add an untracked restart loop.
- Quote's existing child CLI remains the only component that fetches CLOB and
  writes Quote runs. The supervisor owns the outer worker process only.
- A successful Quote worker must remain long-lived; its first successful cycle
  is not an unexpected worker exit.
- Each failed outer Quote worker must terminalize any collecting Quote attempt
  before a replacement can start.
- `quote-collection` P1 history stays visible through the direct incident
  console, including supervisor attempt/restart evidence.
- A bounded restart budget ends in an escalated, alerted state; it never loops
  silently or starts overlapping workers.

## Tasks

1. Extend the producer command and worker entrypoint with a `quote` component
   that invokes the existing production Quote worker under the supervisor's
   attempt/heartbeat authority. Add contract tests for disabled flags and
   clean cancellation.
2. Extend `ProducerSupervisor` to distinguish an intentionally long-lived
   producer from a one-shot producer: progress/heartbeat causes remain
   verifiable, and a Quote process exit is a fault unless requested by stop.
3. Add Quote to the production supervised topology and remove the parallel
   un-supervised Quote worker in isolated mode. Verify one owner only.
4. Add end-to-end timeout/restart tests: hard Quote timeout -> receipt ->
   contained/recovering incident -> replacement; restart budget exhaustion ->
   escalated incident and no overlapping child.
5. Deploy only after focused and relevant M1 regression gates pass. Prove a
   natural Quote cycle, direct incident history, console 200, and Polywatch
   recovery with exact release evidence.
