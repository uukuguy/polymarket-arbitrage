# M1 Quote Input Capacity Design

## Decision

R2 is the immutable authority for every large Quote-batch input and output.
Postgres retains only fenced job identity, content-addressed R2 references,
digests, counts, timestamps, and the small fields required for scheduling.

## Evidence

The live `polyarb` database reports 359 MB in `m1_quote_batch_inputs` for
1,848 rows. Its `token_ids` and `legs` JSONB columns duplicate the immutable
batch information already derivable from the authenticated Structure bundle
and required to construct each Quote artifact. Historical L1/L2 tables are
empty and collectively negligible. The legacy L2 machine is stopped.

## Design

1. Quote admission serializes each deterministic `QuoteBatchSpec` as a
   canonical content-addressed R2 input artifact. It PUTs and HEAD-authenticates
   that artifact before the Postgres transaction creates its job and receipt.
2. `m1_quote_batch_inputs` replaces `token_ids` and `legs` JSONB with
   `input_artifact_key`, `input_artifact_digest`, and `leg_count`. Its existing
   structural digests remain so retries are still fenced to the same generation.
3. A Quote worker re-reads and digest-verifies the R2 input artifact before it
   requests books. An unreadable, altered, or mismatched artifact fails closed;
   it never falls back to an in-memory or historical Postgres payload.
4. The opportunity certifier continues to authenticate the completed Quote
   output from R2. It no longer joins the large input JSONB merely to recover
   legs; it reads the sealed R2 input artifact associated with the receipt.
5. A one-shot migration/backfill verifies each existing live input against its
   current immutable constraints, uploads it to R2, records its reference, and
   only then removes the JSONB columns. It writes an auditable count/digest
   report. It must not run while the old formal acceptance window is considered
   valid; a fresh acceptance run begins after deployment.
6. Retention is explicit: completed historic Quote input/output artifacts stay
   in R2 for the configured evidence window. Postgres contains no large raw
   payload and exposes capacity metrics plus an alert before 80% of its plan
   quota.

## Safety invariants

- R2 upload plus HEAD/digest authentication happens before an SQL reference can
  be committed.
- A retry with the same job key must prove the same key, digest and legs; a
  different payload raises `JobIdentityConflict`.
- No migration deletes a payload until its R2 object has been authenticated and
  its durable reference committed.
- M1 tables and applications are in scope; legacy L1/L2 is not a fallback or
  data source.

## Acceptance

- The Quote worker and opportunity certifier pass their existing recovery and
  crash-after-upload contracts while reading R2 input artifacts.
- A real Postgres migration test proves JSONB removal only after a verified R2
  backfill record.
- Production relation-size report keeps `m1_quote_batch_inputs` below 10 MB
  after historical compaction, with an auditable before/after report.
- A fresh cloud acceptance run starts only after the capacity and alert metrics
  are visible in the Dashboard.
