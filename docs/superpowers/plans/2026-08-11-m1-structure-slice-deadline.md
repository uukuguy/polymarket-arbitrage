# M1 Structure Slice Deadline Repair

**Goal:** Stop a late Gamma page from outliving the 45-second cooperative
Structure slice and being killed by the 75-second parent watchdog.

**Root cause:** `run_structure_sync_until_published` measured the 45-second
budget only after a whole `fetch + commit` page. A page that started late could
still receive the fixed 35-second request timeout, so it crossed the slice and
the parent killed the child before it could emit a normal checkpoint.

**Design:** Before every page, reserve the bounded SQLite commit window and
cap the request timeout to `min(remote_page_cap, remaining_slice - commit_reserve)`.
When no commit reserve remains, return the existing durable cursor as a normal
`StructureSyncCheckpoint`; do not start another network request.

**Verification:** A RED test sets the clock to second 37 of a 45-second slice
and proves the worker gets only a 3-second request budget. Existing elapsed
slice tests are updated to assert the new immediate-checkpoint contract.
