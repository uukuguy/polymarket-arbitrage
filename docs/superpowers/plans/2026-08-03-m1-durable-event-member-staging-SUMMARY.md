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
- Review-fix RED/GREEN: the production scheduler now derives a 1,200-member event in one isolated 500/500/200 slice;
  a 45-second post-CAS deadline checkpoints without rollback, a 50,000-row fixture stops at 100 chunks, and the next
  admission repeats Quote priority checks before spawning another child.
- Review-fix RED/GREEN: a pre-contract window is authenticated as `waiting-natural-window/pass`; source evidence with a
  missing or invalid receipt remains fail-closed and alertable.
- Review-fix RED/GREEN: 2,000 duplicate ordinals plus 200 relation parents return at most two cardinality rows per
  candidate key while preserving conflict-over-duplicate semantics.
- Final-review RED/GREEN: event-member admission is authenticated as `waiting-event-market-backfill` until the same
  window's event-market bootstrap is terminal; a real scheduler flow proves one market shared by two events seals and
  projects `conflicting-event-membership` only after that prerequisite completes.
- Final-review RED/GREEN: every persisted event-conflict summary has a receipt-bound Merkle proof. Update, delete,
  insert/replace, cross-window proof substitution, and receipt-root tampering all fail closed.

## Work bounds and query shape

- Candidate budget is 1..500 per call; the 1,200-row regression proves multi-page completeness.
- Event-only keyset is `(market_sort_key,event_id,event_ordinal,member_ordinal)` with separate first/resume SQL.
- Direct trace uses at most 17 SELECTs/call: 7 fixed authority/source receipt queries plus at most 10 bulk candidate/evidence
  queries. The current production-shaped commitment trace is 14/call because validated authority is reused internally. Neither budget scales per member;
  traces contain no `json_each`, parent `$.markets`, nullable-OR, or per-member SELECT.
- EXPLAIN reports no `USE TEMP B-TREE FOR ORDER BY`.
- Output remains exactly 11 fields. Count/root are generation-independent; commitment identity binds member receipt digest.
- Relation and sidecar probes inspect at most `2 * candidate_count + 1` rows. The sentinel triggers indexed per-key fallback,
  which returns at most two cardinality witnesses per key rather than materializing every duplicate or parent row;
  identity-cardinality, conflict, and quarantine probes are likewise scoped to current candidate/event keys.

## Verification

- Projection/end-to-end focused gate: 51 passed.
- Performance projection gate: 5 passed; v2 row-chain cases retain at least 2x the v1 baseline. The review-wave replacement
  invokes the real complete commitment path over 1,200 production-shaped rows: receipt validation, sidecar/metadata/market/
  relation/conflict-proof/quarantine queries, JSON decode, diagnostics, and root accumulation. Final median was 0.042571s versus
  0.126162s for the rejected raw whole-sibling path (2.96x), with 42 SELECTs across 3 v2 calls. This benchmark is a
  synchronous projection-reader measurement, not child timing evidence.
- The standalone 120k row-chain gate remains in the performance module. Conflict lookup VM steps are independent of
  unrelated sibling cardinality: 117,276 steps at both 100 and 50,000 siblings (1.000 ratio).
- Actual child evidence: the scheduler-path 1,200-member subprocess test completes and seals in a 1.12s pytest call on the
  verification host. Deterministic child-clock tests prove a post-CAS 45-second checkpoint and the 100 x 500 = 50,000-row
  production cap; the parent protocol timeout remains 75 seconds with TERM/KILL cleanup.
- Ruff and `git diff --check`: passed before final proportional gates.

## Handoff

This is the re-review boundary for parent classifier-recovery Task 3. Historical windows without natural source receipts
remain immutable and appear as authenticated `waiting-natural-window/pass`; they are never synthesized from mutable raw
payloads. Existing source metadata/progress with a missing or invalid receipt is a failure, not migration waiting.

Review wave 2 added global-conflict-over-duplicate precedence, complete 1,200-row count/root/diagnostic/sample/cursor/receipt
oracles, recomputed-digest mixed-window/source rejection, and replaced the SQL surrogate benchmark with the production path.

Final review additionally made event-market backfill a hard authenticated predecessor of member admission and replaced the
conflict-summary-only commitment with a resumable `members -> conflicts -> merkle -> proofs -> complete` state machine.
Every CAS writes at most its 1..500 child budget. Non-terminal odd chunks persist an authenticated pending child and pair it
after restart; only a terminal odd child self-duplicates. The 501-row, limits 1/17/500, reopen-on-every-call regression proves
identical per-level node cardinality, independently recomputes the canonical Merkle root, and verifies every proof before the
member receipt can seal. Fresh and migrated databases expose the
same conflict summary/proof/node schema and migration failure rolls back exactly.
