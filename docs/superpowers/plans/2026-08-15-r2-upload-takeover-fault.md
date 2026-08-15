# Staging R2-upload takeover fault — implementation plan

1. Add optional worker callbacks at the verified-upload/pre-receipt boundary.
2. Add explicit CLI target and acknowledgement validation, forwarding one
   exact-key callback only to the Structure and Quote worker factories.
3. Add RED/GREEN worker and CLI tests proving the callback prevents receipt
   insertion and defaults to disabled.
4. Deploy only when a fresh staging range/Quote job exists; remove the target
   from the machine command after the intentional stop and record reclaimed
   lease/receipt evidence.
