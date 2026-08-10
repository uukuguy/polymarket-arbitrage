# M1 Structure Bootstrap Query Deadline

**Goal:** Prevent a single SQLite statement in online event→market bootstrap
from consuming the Structure child watchdog after the outer cooperative loop
has already been made chunked.

**Root cause evidence:** Production progressed through small bootstrap commits,
then a child with no terminal stderr stage still ran through its 75-second
watchdog. The outer loop can check elapsed time only after
`advance_structure_event_market_backfill` returns; its SQLite read/insert work
therefore needed its own cancellation boundary.

**Design:** Each online bootstrap call now receives a five-second execution
deadline (also bounded by remaining slice time). SQLite's progress handler
interrupts a statement beyond that deadline; the child catches only the
expected interruption and returns an ordinary `bootstrap` checkpoint without
advancing the durable cursor. Stage markers now include `bootstrap`, so any
remaining failure evidence identifies this exact subpipeline.

**Verification:** A regression injects SQLite's `interrupted` error into the
bootstrap call and proves `run_structure_sync_until_published` returns a
checkpoint rather than escaping to the parent watchdog. Existing durable
resume, small-chunk, and legacy schema migration tests remain green.
