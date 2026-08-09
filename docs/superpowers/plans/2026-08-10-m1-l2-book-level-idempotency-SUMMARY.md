# L2 book-level mirror idempotency

## Incident

The newly restarted production L2 daemon logged `23505` duplicate-key failures
for repeated `l2_book_levels` frames sharing the schema's declared identity:
`(asset_id, ts, side, level)`. The next book frame recovered, but the duplicate
was incorrectly classified as an error and only visible in logs.

## Root cause and repair

`push_book_levels` used `insert` despite the table's uniqueness contract.
It now uses an upsert with `on_conflict="asset_id,ts,side,level"`, matching the
idempotent handling already used for trade mirror rows. A repeated upstream
book frame therefore updates the same durable identity rather than generating
a false production error. Genuine REST failures remain fail-soft, unhidden,
and continue to leave the freshness anchor untouched.

## Verification

- Test-first regression failed while the method still used `insert`.
- `uv run pytest tests/storage/test_l2_supabase_mirror.py tests/daemon/test_ws_book_evidence_chain.py tests/m1-perception/test_l2_health_endpoint.py -q` — 31 passed.
- `uv run ruff check src/polyarb/storage/l2_supabase_mirror.py tests/storage/test_l2_supabase_mirror.py` — passed.
