# M1 Structure Bootstrap Slice Bound

**Goal:** Make the completed-window event→market relationship bootstrap obey
the same cooperative child budget as Gamma page collection, so production
recovery advances by checkpoints instead of recurring 75-second watchdog
timeouts.

**Root cause evidence:** After the market ordinal index fixed page writes, the
production window `bf8cb511…` reached `complete`, but its relationship
bootstrap processed 902 events / 7,500 relationships before the parent killed
the child at 75 seconds. The bootstrap loop passed 500 rows to one synchronous
SQLite operation and used the store's 120-second writer wait, so one unit of
work could overrun the 45-second cooperative slice before the loop could check
the deadline.

**Design:** Online Structure bootstrap now uses independently committed units
of at most 50 events and 50 relationships. Each unit receives a writer-lock
timeout capped by both five seconds and the remaining slice budget. The durable
cursor and exact relationship staging contract are unchanged; a later child
resumes from the committed subcursor.

**Verification:** The regression constructs a complete 60-event window and
advances synthetic time immediately after its first bootstrap call. It proves
only 50 relationships commit and the online write receives a five-second lock
cap. Existing durability/resume tests continue to cover no-loss restart
semantics. The legacy worker-schema migration also proves the ordinal-index
dependency column is added before the DDL creates that index.
