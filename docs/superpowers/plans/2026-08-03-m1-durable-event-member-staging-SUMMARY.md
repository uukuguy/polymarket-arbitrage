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
- Direct trace uses at most 17 SELECTs/call: 7 fixed authority/source receipt queries plus at most 10 bulk candidate/evidence
  queries. Production-shaped commitment trace is 16/call because validated authority is reused internally. Neither budget scales per member;
  traces contain no `json_each`, parent `$.markets`, nullable-OR, or per-member SELECT.
- EXPLAIN reports no `USE TEMP B-TREE FOR ORDER BY`.
- Output remains exactly 11 fields. Count/root are generation-independent; commitment identity binds member receipt digest.

## Verification

- Projection/end-to-end focused gate: 51 passed.
- Performance projection gate: 2 passed; v2 row-chain cases retain at least 2x the v1 baseline. The review-wave replacement
  invokes the real complete commitment path over 1,200 production-shaped rows: receipt validation, sidecar/metadata/market/
  relation/conflict/quarantine queries, JSON decode, diagnostics, and root accumulation. Final median was 0.048181s versus
  0.130906s for the rejected raw whole-sibling path (2.72x), with 48 SELECTs across 3 v2 calls and each child below 45s.
- Ruff and `git diff --check`: passed before final proportional gates.

## Handoff

This is the clean re-review boundary for parent classifier-recovery Task 3. Historical windows without natural source
receipts remain unavailable and are never synthesized from mutable raw payloads.

Review wave 2 added global-conflict-over-duplicate precedence, complete 1,200-row count/root/diagnostic/sample/cursor/receipt
oracles, recomputed-digest mixed-window/source rejection, and replaced the SQL surrogate benchmark with the production path.
