# Task 6 Implementer Report

## Status and scope

Implemented locally with TDD. No deployment, feature enablement, production
database, secret, wallet, signing, balance, order, harness, evaluator, or
fault `VERIFIED` work was performed.

The seam is exactly the existing durable outbox boundary:
`OpportunityWatcher.deliver_pending_notifications()` consumes
`TELEGRAM_OPPORTUNITY_CARD/str(PendingNotification.id)` immediately before
the unchanged `_send_telegram(settings, _format_card(notification))` call.
`SendTelegram`, `send_opportunity_alert`, card formatting, and the real
transport remain owned by their existing code.

## Delivered

- Added `TelegramDeliveryFault` and the deterministic
  `QualifiedTelegramTransportError`. Injection receipt persistence completes
  before the exception is raised; the exception carries only fault ID, call
  ID, and injection time.
- Exact-target injection fails only that notification. Other pending
  notifications in the same loop call the real sender once. Disabled,
  unmatched, consumed, degraded, controller-failed, and injection-receipt
  failure paths pass through without authority reads on the ordinary hot path.
- Extended notification attempts so each append returns its exact immutable
  `NotificationAttempt` from one `INSERT ... RETURNING`. The watcher no longer
  infers ownership from `notification_attempts(...)[-1]` or shared counts.
- Preserved the existing `telegram-delivery-failed` Incident lifecycle.
  `NotificationIncidents` now returns the authoritative Incident/attempt
  pointer and a typed first-event qualified receipt. The first detected event
  contains the exact injected call ID. Pre-existing, same-millisecond, deduped,
  and other-notification Incidents cannot be linked.
- Added `TELEGRAM_DELIVERY` recovery receipts. Recovery validation and the
  `RECOVERED` append run in the existing single fault-authority transaction and
  require the same notification, current runtime/ownership, exact latest
  append-only attempt overall, `outcome='delivered'`, no error kind, and a
  writer time strictly after injection and not after authority time.
- Wrong writer family or other notification is `NOT_APPLICABLE` without
  mutation. Stale, failed, fabricated, or semantically invalid exact evidence
  is `INVALID`/`EVIDENCE_INVALID`. Authority/SQLite failure is
  `UNAVAILABLE` with cleanup/freeze/degrade, never false invalidity.
- Cleanup is persisted before retry. Existing Incident verification happens
  only from the exact delivered attempt; fault recovery does not write
  `VERIFIED`.
- Failed and delivered attempt writers plus their evidence work are
  cancellation-settled per invocation. A committed write retains exactly one
  receipt before the original outer cancellation propagates. An uncommitted
  write fabricates no attempt, Incident detection, or recovery and cannot
  strand an `INJECTED` fault.
- Fault/Incident evidence and logs contain only outbox ID, safe error type,
  call/Incident IDs, and recovery IDs. Tests directly scan all fault tables and
  the real intent/event rows for a card marker, Telegram URL, and bot token;
  none is persisted.

## Plan-versus-real-API corrections

The brief listed only the adapter, watcher, and two new test files. The real
interfaces required four minimal typed extensions:

1. `OpportunityLedger.mark_notification_failed/delivered` returns the exact
   append receipt.
2. `NotificationIncidents` returns and validates the authoritative first
   Incident event and exact delivered attempt.
3. `FaultRecoveryWriter` accepts an integer Telegram delivery attempt ID.
4. `FaultRuntime` and `FaultAuthorityStore` route and validate the notification
   recovery family.

No schema, sender signature, card payload, global Telegram client, control
surface, or Make target changed.

## RED / GREEN evidence

1. Initial RED failed collection because
   `QualifiedTelegramTransportError`, `TelegramDeliveryFault`, and
   `QualifiedNotificationIncidentReceipt` did not exist.
2. Watcher RED showed zero typed outbox calls and incorrectly delivered the
   targeted notification. GREEN proves exact ID 1 fails while ID 2 uses the
   real sender exactly once.
3. Real-authority recovery RED stopped at `CLEANED`. GREEN records
   `authorized → armed → injected → detected → contained → cleaned →
   recovered` only after the later exact delivered attempt.
4. Latest-attempt RED/contract uses `d1 delivered → f2 failed`: d1 is stale and
   invalid. A later d3 delivered attempt is accepted as the latest overall.
5. Failed-attempt commit/cancel RED stranded `INJECTED`; GREEN preserves one
   exact failed attempt, exact Incident evidence, and `CLEANED`.
6. Delivered-attempt commit/cancel RED stranded `CLEANED`; GREEN preserves one
   delivered attempt and `RECOVERED`. Both rethrow the original cancellation.
7. Attempt-store and Incident-store unavailability RED escaped or stranded
   injection. GREEN produces no fabricated receipt, cleans to `ABANDONED`,
   freezes/degrades, then passes the next real sender call through.

## Verification

- Task 6 adapter/Incident + daemon watcher + outbox ledger: 37 tests passed.
- Full `tests/perception` plus daemon opportunity watcher/main fault wiring,
  outbox ledger, and M1 control regressions: 1,023 tests passed.
- The first aggregate run had one unrelated load-sensitive Resource
  open-authority read failure. The untouched isolated test passed 5/5; a fresh
  full 1,023-test aggregate rerun passed 100%.
- Focused Ruff: passed.
- `git diff --check`: passed.
- Post-commit focused tests and Ruff: passed.
- `make planning-status`: 82 plans, no drift.

## Concerns

- Telegram `delivered` remains transport-writer evidence, not proof of handset
  display/read.
- The independent evaluator and fault `VERIFIED` transition remain Task 7.
