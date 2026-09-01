# M1 Alert Delivery Failure Containment — Task 1 Summary

## Outcome

- Classified Telegram HTTP 401/403 as terminal credential failures instead of retryable transport failures.
- Kept transient Telegram failures retryable, with the existing bounded retry delay.
- Set transactional worker rollout defaults to the always-available Dashboard channel; Telegram is re-enabled only after a credential preflight succeeds.

## Verification

- `uv run pytest tests/m1-perception/test_transactional_alert_delivery.py -q`
- `ruff check src/polyarb/control_plane/alert_delivery.py tests/m1-perception/test_transactional_alert_delivery.py`

## Production follow-up

- Deploy the alert-delivery image, then terminally close the old 401 Telegram outbox intents with an auditable receipt.
- Update active transactional workers to Dashboard-only configuration while the Telegram bot credential remains invalid.
