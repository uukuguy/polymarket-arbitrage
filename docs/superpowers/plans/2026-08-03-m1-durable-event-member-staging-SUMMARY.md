# Durable Event-Member Staging — Task 3 Summary

## Outcome

Fresh Structure projection now consumes only a validated natural-window event source receipt, its sealed event-member
receipt, indexed sidecar rows, bounded market payload rows, relation evidence, and quarantine receipts. Runtime event-array
expansion and whole-sibling normalization were removed.

## TDD evidence

- RED: commitment had no member receipt identity; missing/tampered receipts still returned projection evidence.
- RED: the reader trace contained `json_each(event.payload_json,'$.markets')`.
- GREEN: receipt failures occur before candidate reads; no count/root/sample is returned and no production table mutates.
- GREEN: limits 1, 17, and 500 produce the same complete 1,200-row event-only diagnostics and terminal cursor.
- GREEN: duplicates, nullable/padded fields, quarantine/global conflict precedence, sibling conflict, and generation omission pass.

## Work bounds and query shape

- Candidate budget is 1..500 per call; the 1,200-row regression proves multi-page completeness.
- Event-only keyset is `(market_sort_key,event_id,event_ordinal,member_ordinal)` with separate first/resume SQL.
- Trace uses at most 10 bulk SELECTs/call and contains no `json_each`, parent `$.markets`, nullable-OR, or per-member SELECT.
- EXPLAIN reports no `USE TEMP B-TREE FOR ORDER BY`.
- Output remains exactly 11 fields. Count/root are generation-independent; commitment identity binds member receipt digest.

## Verification

- Projection/end-to-end focused gate: 51 passed.
- Performance projection gate: 2 passed; v2 row-chain cases retain at least 2x the v1 baseline and the production-shaped
  12,000-member indexed sidecar reader measured 2.31x the raw `json_each` expansion baseline.
- Ruff and `git diff --check`: passed before final proportional gates.

## Handoff

This is the clean re-review boundary for parent classifier-recovery Task 3. Historical windows without natural source
receipts remain unavailable and are never synthesized from mutable raw payloads.
