# M1 Durable Event-Member Staging Design

**Status:** Approved in principle on 2026-08-03; written-spec review pending

**Parent design:** `2026-08-02-m1-structure-drift-classifier-recovery-design.md`

## 1. Problem

Classifier-v2 must enumerate the complete frozen event-member universe with at
most 500 source-member operations per cooperative reader call. The current
source stores each event's `markets` array only inside
`structure_sync_event_staging.payload_json`.

SQLite `json_each()` cannot keyset-resume inside that array. A predicate on the
JSON array index still reparses and walks the array from its beginning. A
1,200-member event therefore performs 1,200 database-side member operations on
every comparison chunk even when Python returns only 500 rows. This violates
the existing 500-row production contract and can become quadratic across
resume chunks.

The existing `structure_sync_event_market_staging` relation table cannot carry
the missing truth safely. Its primary key `(window_id,event_id,market_id)`
intentionally collapses repeated market identities, while classifier-v2 must
preserve duplicate ordinals as fail-closed evidence. Rebuilding that frozen
table would also rewrite historical source evidence.

## 2. Considered Approaches

### A. Append-only normalized member sidecar — selected

Create one durable row per raw event-member ordinal. Preserve the raw member
JSON and strictly extracted nullable fields. Populate and seal it in bounded
chunks, then make fresh projection read only the indexed sidecar.

Benefits: true keyset resume, duplicate preservation, historical relation rows
remain immutable, and invalid raw identities remain diagnosable.

Cost: new event metadata/source-receipt authority, one member source table,
bounded derivation progress, an immutable seal receipt, migration logic, and
publication-chain validation.

### B. Add columns to `structure_sync_event_market_staging` — rejected

This appears smaller but cannot represent two occurrences of the same market
inside one event because its primary key collapses them. It would also require
rebuilding already frozen relation evidence.

### C. Keep JSON expansion and relax the bound — rejected

This preserves schema but makes the 500-row contract cosmetic. Large events
would keep rescanning from ordinal zero and could prevent the 45-second child
slice from recovering naturally.

## 3. Canonical Sidecar Schema

Add `structure_sync_event_member_staging`:

```sql
CREATE TABLE structure_sync_event_member_staging (
    window_id       TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    event_ordinal   INTEGER NOT NULL CHECK(event_ordinal >= 0),
    member_ordinal  INTEGER NOT NULL CHECK(member_ordinal >= 0),
    market_id       TEXT,
    market_sort_key TEXT NOT NULL,
    group_id        TEXT,
    member_kind     TEXT,
    active          INTEGER CHECK(active IS NULL OR active IN (0,1)),
    closed          INTEGER CHECK(closed IS NULL OR closed IN (0,1)),
    payload_json    TEXT NOT NULL,
    payload_hash    TEXT NOT NULL CHECK(length(payload_hash)=64),
    PRIMARY KEY(window_id,event_id,member_ordinal)
);
```

`market_id` is populated only for an exact, nonblank, unpadded string.
`market_sort_key` equals that valid market ID, otherwise the empty string.
Invalid members remain rows; they are never silently dropped. The raw canonical
member JSON and hash preserve enough evidence to reconstruct the nullable
diagnostic envelope without reparsing the parent event array.

Indexes:

```text
(window_id, market_sort_key, event_id, event_ordinal, member_ordinal)
(window_id, event_id, member_ordinal)
(window_id, market_id, event_id, member_ordinal)
```

The first index is the projection keyset. The second is derivation/resume
authority. The third supports global duplicate/relation evidence. No unique
constraint may collapse `market_id` duplicates.

The table receives the same frozen-window INSERT/UPDATE/DELETE guards as the
other Structure staging tables. After its seal receipt exists, all mutation is
rejected with a stable error.

## 4. Bounded Derivation State Machine

### 4.1 New-window event source authority

Historical event payloads do not have a pre-publication immutable content
commitment, and their canonical JSON places the large `markets` array before
the event-level `negRiskMarketID`. They are therefore not eligible for member
sidecar backfill.

For every new window created after this contract ships, event-page ingestion
writes `structure_sync_event_metadata_staging` in the same transaction as the
existing event row. Each metadata row contains:

```text
window_id, event_id, event_ordinal, event_group_id,
payload_hash, payload_length, metadata_contract
```

`event_group_id` is extracted from the already-decoded top-level Gamma event,
not from nested market objects. `payload_hash` is computed from the exact
canonical payload string already being persisted; this adds no second parse or
member-array walk. Event retries must match the existing metadata byte-for-byte.

The event writer maintains a rolling `source-event` commitment over these
metadata rows. The transition to `events_complete` atomically inserts an
append-only, replacement-safe `structure_sync_event_source_receipts` row bound
to the window, event count/root, terminal page/cursor, metadata contract, and
receipt digest. Publication cannot start until this receipt validates.

The exact source contract is `structure-event-source-v1`. Migration creates the
empty metadata/receipt authority but never derives it for old windows. A window
without this receipt returns `structure-event-source-receipt-unavailable`; it
is neither scanned nor repeatedly retried. The existing pointer/serving window
remains unchanged while the scheduler naturally creates a new eligible window.

Sidecar derivation reads event group ID and payload hash only from the sealed
metadata row. It never searches the parent payload for group identity and never
rehashes all event payloads during advance, status, health, or receipt
validation.

Add one progress row per window with:

```text
window_id, event_cursor, member_ordinal, rows_written,
member_character_offset, member_byte_offset,
source_receipt_digest, parent_payload_hash, checkpoint_digest,
member_state, diagnostic_state, checkpoint_at_ms,
completed_at_ms, failure_reason
```

One advance call:

1. authenticates the frozen window and source-event identity;
2. uses a stdlib `json.JSONDecoder.raw_decode` array scanner and the durable
   byte offset to decode at most 500 member objects; it never calls
   `json.loads()` on the complete event object or member array;
3. reads only enough following event rows to expose at most 500 raw members;
4. parses and writes at most 500 sidecar rows in one `BEGIN IMMEDIATE` CAS;
5. advances `(event_id, member_ordinal, member_character_offset,
   member_byte_offset)` and its checkpoint digest in the same transaction;
6. never decodes an already committed member object after restart.

The parent event JSON text may be loaded as one immutable blob, but no call may
materialize the complete event object or member array. A small structural
scanner finds the top-level `markets` array without decoding its contents;
`raw_decode` then decodes one member object at a time from the persisted
character offset; the corresponding UTF-8 byte offset is stored and bound but
never recomputed by encoding the committed prefix. No call may decode, iterate,
normalize, hash, or insert more than 500 member elements. Resume validates a
checkpoint digest binding both offsets, ordinal, source receipt, parent payload
hash, row count, and row-chain states. It never rescans the committed prefix.

The scanner accepts only canonical JSON structure: top-level object, exactly
one `markets` key whose value is an array, comma-separated JSON values, and no
trailing non-whitespace data. Malformed structure blocks sealing with a stable
bounded failure; it is never skipped or guessed.

New windows derive the sidecar after the event-source receipt seals and before
publication can start. Historical/open windows without the new source receipt
are ineligible and remain immutable; no existing event, relation, market,
publication, pointer, serving, generation, or exact-receipt row is updated.

An exception leaves the last committed cursor intact. Automatic scheduler
retry resumes from that cursor. Deterministic malformed-member evidence is
stored as nullable metadata and is not a derivation failure; only unreadable
parent JSON, source identity drift, cursor inconsistency, or write/CAS failure
blocks sealing.

## 5. Immutable Seal and Authentication

Add `structure_sync_event_member_receipts`, append-only and replacement-safe.
Its fixed digest fields include:

```text
window_id
source event count/root and source identity hash
metadata contract = structure-event-member-staging-v1
member row count/root
invalid-member count/root
derivation progress terminal cursor
sealed_at_ms
receipt_digest
```

The row commitments reuse the existing `source-event` row-chain domain with an
explicit first tuple element `structure-event-member-staging-v1`; no new
row-chain domain is added. This preserves the parent design's exact registry.

Fresh databases and migrated databases must have identical columns, indexes,
and triggers. Migration creates empty event-metadata/source-receipt and
sidecar/progress/member-receipt tables only; it performs no startup backfill.
Only naturally collected post-contract windows receive the new source receipt.
Any failed DDL/rebuild step rolls back without changing business-table rows.

The member receipt validator recomputes the receipt digest and binds it to the
current immutable source-event identity. Missing, mixed, or tampered receipts
return `structure-event-member-receipt-invalid` and expose no member counts or
samples.

## 6. Fresh Projection Reader

The event-only half of `fetch_structure_drift_fresh_projection_chunk` reads the
sealed sidecar by the covering keyset index:

```text
(market_sort_key, event_id, event_ordinal, member_ordinal)
```

It anti-joins market staging and bulk-loads relation cardinality/quarantine
evidence for at most 500 candidate rows. It never calls `json_each()` and never
normalizes a sibling by reopening the parent event payload.

Global conflict and duplicate facts are computed with indexed sidecar/relation
queries over the current candidate IDs. Conflict remains higher priority than
local quarantine. Exact raw/staged event, market, group, state, condition, and
token identities must agree before an 11-field projection member is emitted.

Projection authorization additionally binds the validated member receipt
digest. A sidecar created for a different window/source identity cannot be
mixed into a classifier comparison.

## 7. Scheduler and Health Chain

Structure scheduling order becomes:

```text
event staging complete
→ event-source receipt sealed
→ bounded event-member derivation/seal
→ publication/generation work
→ classifier-v2 fresh projection
```

Quote priority, producer lock, 500 rows, 100 chunks, 45-second cooperative
slice, and 75-second parent timeout remain unchanged.

Before the member receipt seals, status is recovering with exact progress.
Deterministic receipt/source mismatch is fail-closed. Operational failure flows
through the existing attempt ledger and Structure health component so resident
Polywatch can alert, deduplicate, remind, and clear naturally; there is no
manual pointer or publication repair path.

## 8. Verification Contract

Deployment is forbidden until tests prove:

1. a 1,200-member event advances in 500/500/200 `raw_decode` operations, with
   database, decoder, and Python inspection counters never exceeding 500 per
   call and no whole-event `json.loads()`;
2. restart after every cursor boundary emits every ordinal exactly once;
3. duplicate market IDs at different ordinals remain distinct sidecar rows and
   later fail closed as `duplicate-market-identity`;
4. nullable, blank, padded, and valid-but-mismatched identities remain
   diagnosable and never become projection rows;
5. fresh and migrated schema/index/trigger signatures are identical;
6. historical windows receive no metadata/source/member rows, while a new
   naturally collected window atomically seals event-source authority before
   member derivation;
7. update, delete, duplicate insert, and `INSERT OR REPLACE` cannot overwrite a
   sealed receipt;
8. every receipt field tamper, missing receipt, and mixed identity fails closed;
9. projection uses no `json_each`, per-member SELECT, nullable-OR keyset, or
   temporary order B-tree;
10. chunk sizes 1, 17, and 500 produce identical complete projection counts,
    roots, diagnostics, and samples;
11. the production-shaped performance gate remains at least 2x the original
    classifier-v1 complete gate and every child stays below 45 seconds;
12. all prior classifier, publication, migration, scheduler, health, and Quote
    priority tests remain green.

## 9. Scope Boundary

This amendment authorizes only the durable source-member metadata needed to
make classifier-v2 genuinely bounded. It does not authorize read-mode cutover,
Quote enablement, pointer mutation, manual production scans, trading, wallet
access, or candidate-lifecycle shortcuts. Those remain behind the existing
Task 7/8 exact-SHA review and natural-production acceptance gates.
