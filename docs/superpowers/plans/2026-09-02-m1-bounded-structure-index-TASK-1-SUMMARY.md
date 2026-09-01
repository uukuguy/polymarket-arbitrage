# M1 bounded Structure business index — Task 1 Summary

## Outcome

The production business view no longer requires a full PostgreSQL copy of the
complete Structure artifact. Immutable R2 range artifacts remain the complete
research record; PostgreSQL holds a bounded browse index for events and group
truth, which are the two useful starting points for market research.

## Implementation

- Structure range workers stage research-index rows only for `events` and
  `group_truth`; raw market, tag, membership, and issue components remain in
  their immutable R2 artifacts.
- A range receipt still reports and certifies its full source record count;
  index row count is intentionally allowed to be lower.
- Structure research pages expose both `source_record_count` and
  `indexed_record_count`, so the dashboard never misrepresents a thin index as
  a complete table mirror.
- The dashboard labels loaded/indexed/source counts in its research header.

## Verification

- RED: a published Structure generation with one indexed row out of two source
  rows was incorrectly reported as an unavailable incomplete index.
- GREEN: Postgres integration verifies the bounded page is available with both
  counts; worker unit coverage verifies only Events and Group Truth create
  index rows; API tests and dashboard typecheck pass.

## Production follow-up

Deploy the worker and API image before resuming the Structure lane. Existing
unpublished wide rows are a transient, bounded staging remnant; a newly
published generation will retain only the thin index.
