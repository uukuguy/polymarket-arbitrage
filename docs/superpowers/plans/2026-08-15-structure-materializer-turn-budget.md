# Structure materializer turn budget — implementation plan

1. Add a default-zero, non-negative scheduler parameter and append only its
   serial worker turns after the base ordered worker set.
2. Add scheduler regression coverage proving extra turns target materializer
   only and leave the range budget independent.
3. Expose and validate the option on `tick-once` and `serve`; update CLI tests.
4. Run focused tests/Ruff, commit a phase summary, deploy staging-only at eight
   turns, and verify source-materializer checkpoints advance without new local
   concurrency or pointer mutation.
