# M1 Dashboard Structure Window Progress

**Goal:** Make the active Structure recovery quantitatively inspectable from
the Fly-native incident console, without SSH or inference from logs.

**Design:** Extend the bounded incident-read projection with the existing
durable `structure_sync_windows` row.  Render it beside the latest child
checkpoint so operators see the synchronization status, submitted event and
market pages, cursor/checkpoint, publication result, and failure reason in
one Dashboard card.

**Verification:** First assert the API has no phantom window, then create a
durable window and assert the exact store projection is returned.  Run the
focused console/API tests and Ruff.  Production acceptance is a console read
showing the active window's committed counters.
