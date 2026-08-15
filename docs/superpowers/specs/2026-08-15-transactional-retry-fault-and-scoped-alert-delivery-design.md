# Transactional Retry Fault and Scoped Alert Delivery Design

## Goal

Produce real staging evidence for the retry-circuit half of M1 transactional
fault-soak acceptance without replaying the 1,670 historical alert intents
currently pending in the control-plane outbox.

## Constraints

- Staging only; no production pointer, L1/L2, or existing Telegram mutation.
- A fault must match one exact immutable Structure or Quote job key.
- Fault injection is disabled unless both the target and a literal acknowledgement
  are present.
- It must use the ordinary worker `Exception` path and therefore the existing
  `finish_retryable_with_incident` lease fence; it must not simulate database
  state or directly open circuits.
- Exactly three injected attempts must yield existing 15/30/60-second delays.
- Alert delivery for acceptance must consume only outbox rows bearing a newly
  generated acceptance-run identifier. Historical rows remain untouched.

## Considered approaches

1. Start the existing `alert-serve` worker unchanged. Rejected: it sends all
   pending historical Telegram messages before acceptance evidence.
2. Delete/mark historical outbox rows delivered. Rejected: destructive,
   destroys audit history, and fabricates delivery.
3. Add a scoped retry fault and scoped alert-delivery selector. **Chosen**:
   the control plane remains append-only, a new run scope is explicit in newly
   created outbox payloads, and the delivery worker claims only that scope.

## Design

### Retry fault boundary

`--fault-retry-job-key`, `--fault-retry-attempts`, and a distinct literal
acknowledgement are available only on `tick-once` and `serve`. The callback
matches the current lease's exact key and only while its epoch/attempt is within
the configured finite count. It raises a dedicated ordinary exception after a
worker claims its frozen input but before it writes the durable receipt. The
existing worker `except Exception` code calls `finish_retryable_with_incident`;
Postgres determines delay/circuit state. Removing the CLI arguments permits
the normal, fenced probe to complete and invoke `record_job_recovery`.

The existing R2-upload-before-receipt `KeyboardInterrupt` hook remains
unchanged and is a separate acceptance boundary.

### Acceptance-run alert scope

An explicit `acceptance_run_id` is supplied only to the recovery-event/outbox
write path for the faulted worker. It is persisted inside the newly created
outbox payload. `TransactionalAlertDeliveryWorker` accepts an optional scope
selector and claims only due rows whose payload has that exact identifier.
Without a selector it preserves today's whole-outbox behavior.

The isolated staging alert process is started only with the new scope. It
delivers the new dashboard and Telegram intents, producing immutable rows in
`m1_alert_deliveries`; it cannot select historical payloads that lack the ID.

### Acceptance sequence

1. Select a new Structure or Quote job with no prior receipt and allocate a
   UUID-like acceptance-run ID.
2. Arm the retry hook for exactly three attempts; observe durable 15/30/60
   retry/circuit facts, then remove the hook.
3. Start the scoped alert worker only after normal recovery writes the recovery
   event/outbox pair; record dashboard and Telegram delivery receipts.
4. Repeat independently for Structure and Quote, then assemble evidence for
   `verify_fault_soak` and start the 24-hour window.

## Failure handling

- Wrong/missing acknowledgement, attempts less than one, or a scope without an
  exact target is a CLI validation error before any worker starts.
- A stale lease cannot create a retry transition, recovery, or delivery receipt
  because all existing repository writes retain their lease fences.
- If Telegram fails, the scoped row becomes retryable; data collection remains
  unaffected and evidence remains incomplete rather than forged.
- The scoped alert process has no SQL delete/update path outside normal fenced
  delivery transitions.

## Verification

- Unit/CLI tests prove the retry callback's bounds and acknowledgement gate.
- Worker tests prove an injected retry takes the existing retry path; normal
  next probe records recovery.
- Alert worker/repository tests prove scope selection excludes historical rows.
- A real staging run proves three delays, one opened circuit, one recovery,
  two scoped delivery receipts, and no historical outbox state changes.
