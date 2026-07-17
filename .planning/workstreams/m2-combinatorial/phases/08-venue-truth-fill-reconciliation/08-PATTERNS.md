# Phase 8 Pattern Map — Venue-Truth Fill Reconciliation

**Date:** 2026-07-17  
**Requirement:** H-006

## Files to modify or create

| File | Change | Closest existing analog |
|---|---|---|
| `src/polyarb/routing/position_tracker.py` | Add immutable `VenueSettlement`, attach it optionally to `Fill`, add immutable `SettlementReceipt`, and branch the close transition between modeled and venue truth. | `Fill` construction at lines 140-194 and H-005 allocation/remaining transition at lines 441-522. |
| `src/polyarb/routing/position_repository.py` | Extend `TransitionResult`, strict receipt codec, `OperationReceipt`, repository `apply`, schema, and additive migration with request fingerprints. | Tagged `Money` codec at lines 154-222; in-memory copy/apply at 225-270; SQLite `BEGIN IMMEDIATE` replay/commit at 313-363; money/unit migrations at 375-565. |
| `src/polyarb/execution/engine.py` | Preserve the complete `Fill` unchanged through the fill-provider path; surface tracker rejection without adding settlement math. | `_maybe_close_for_leg` at lines 280-330. |
| `src/polyarb/cli_arbitrage.py` | Add all-or-none venue arguments, construct settlement before mutation, force fingerprint validation on replay, and render structured receipts. | Existing `close` command and durable replay envelope at lines 339-452. |
| `Makefile` | Forward `venue_cash`, `venue_fee`, `venue_status`, and `venue_ref` through `close-arb`; update help/usage examples. | `close-arb` target around lines 1185-1208 and its `SIZE_FLAG`/identity branches. |
| `tests/routing/test_position_repository.py` | Add settlement codec, migration, fingerprint replay/conflict, malformed storage, and concurrent-writer tests. | Phase 4/5 schema fixtures at lines 20-159; exact Money receipt tests around lines 689-768. |
| `tests/routing/test_position_tracker.py` | Add exact venue cash vectors, formula-supersession, rollback, identity conflict, and modeled-path compatibility tests. | `TestDurablePartialFillAccounting` at lines 420-541. |
| `tests/execution/test_engine.py` | Add complete settlement forwarding/replay and rejection tests. | Immutable fill and restarted partial sequence tests at lines 301-426. |
| `tests/cli/test_arbitrage_cli.py` | Add option all-or-none validation and structured output assertions. | Existing Typer `CliRunner` close tests in the T5 section. |
| `tests/cli/test_arbitrage_cli_process.py` | Add true process response-loss replay, changed-fee conflict, and raw SQLite authority proof. | `test_partial_fill_recovers_lost_response_across_processes` at lines 241-327. |
| `tests/test_makefile.py` | Assert all venue fields are forwarded by `make -n close-arb`. | Partial fill identity forwarding test at lines 57-70. |
| `docs/learning/17-venue-truth-reconciliation.md` | Explain terminal venue truth, exact receipt, and fingerprinted replay. | `docs/learning/16-部分成交如何不重不漏.md`. |
| `docs/learning/00-INDEX.md` | Add chapter 17 to the reading order. | Existing numbered chapter entries. |

No new dependency, live venue client, network call, or second receipt table is needed.

## Canonical data flow

```text
CLI local harness / ExecutionEngine.fill_provider
    -> Fill(quantity, fill_id, optional VenueSettlement)
    -> PositionTracker.close_position_with_fill
         validates fill + allocates exact residual cost
         builds canonical request fingerprint
         chooses modeled Money or venue SettlementReceipt
    -> PositionRepository.apply
         BEGIN IMMEDIATE
         compare operation type + target + fingerprint
         mutate account/remaining position + encode receipt
         commit once
    -> tracker/CLI receives the committed receipt, including on replay
```

The tracker remains the accounting authority. Engine and CLI normalize inputs and
forward facts; neither may reconstruct fee cash or compute venue PnL independently.

## Repository patterns

### Result union and strict codec

Extend the existing `TransitionResult` union rather than adding a parallel API. Follow
the tagged Money shape in `_encode_result/_decode_result`:

- `SettlementReceipt` is frozen and contains `Money` fields.
- Encode a discriminator such as `kind=settlement`, fixed
  `source=venue-confirmed`, and integer `gross_micros`, `fee_micros`, `net_micros`,
  and `pnl_micros`.
- Decode only the exact key set. Use `type(value) is int` so booleans are rejected,
  construct every `Money` to enforce signed-64-bit bounds, and verify
  `net == gross - fee`.
- Keep existing `None`, bool, float, and tagged Money decoding unchanged so old
  receipts remain readable.

`OperationReceipt.result` and `_validate_transition_result` must accept the structured
result in both repositories. In-memory storage continues to use `deepcopy`, matching
the immutable receipt behavior already proved for Money.

### Additive fingerprint migration

Follow `_migrate_money_schema` / `_migrate_position_units`:

1. Add `request_fingerprint` to `_SCHEMA` and `_REQUIRED_COLUMNS`.
2. Let the pre-migration schema check accept a database without it.
3. Under the initialization `BEGIN IMMEDIATE`, `ALTER TABLE` once, then validate the
   column's dynamic storage type and complete schema.
4. Preserve legacy rows with the research-defined empty fingerprint; do not attempt to
   reconstruct historical quantity/cash/fee facts from a Money receipt.

Fingerprint input must be canonical authority, not presentation values: market ID,
filled quantity micros, gross micros, fee micros, exact status, and source reference.
Use a stable versioned serialization and compare the stored value inside the same
transaction that checks `operation_id`. Never compare after `get_receipt` and never
overwrite a stored fingerprint.

Update all three SQLite query shapes together: `get_receipt` SELECT, `apply` SELECT,
and INSERT. The in-memory `OperationReceipt` must carry the same fingerprint so unit
and SQLite semantics cannot diverge.

### Concurrency pattern

Keep `BEGIN IMMEDIATE` before reading the existing operation. The first writer commits
state, result, and fingerprint atomically; the second writer then either returns the
identical structured receipt or raises identity conflict. A test should use two
repository instances against one database and prove exactly one operation row and one
state mutation.

## Tracker transition pattern

Reuse H-005 in this order:

1. Resolve `venue-fill:{fill_id}` and validate positive/non-overfill quantity.
2. Allocate `allocated_cost = Money.allocate(...)` before changing the position.
3. For paper fills, retain `Money.pnl_for` and `balance += allocated_cost + pnl`.
4. For a settlement fill, require immutable fill ID and complete confirmed settlement,
   then compute `net = gross - fee`, `pnl = net - allocated_cost`,
   `balance += net`, and `realized_pnl += pnl`; do not call `Money.pnl_for`.
5. Reuse the existing partial/final remaining quantity and cost-basis branch.
6. Return `SettlementReceipt` for venue truth and the legacy Money result for paper.

Build the fingerprint before calling `repository.apply` and pass it into that atomic
boundary. A deliberately wrong `exit_price` must not change venue cash/PnL; it remains
observational only.

## Engine pattern

`ExecutionEngine._maybe_close_for_leg` already forwards the returned `Fill` directly
to the tracker. Keep that shape. Add tests where the provider returns a complete
settlement Fill, then repeat it through a restarted SQLite tracker. The result must be
one mutation and one structured receipt. Invalid/nonterminal settlement should follow
the existing warning-and-leave-open behavior; the Engine must not fill missing fields.

## CLI replay pattern and required correction

The current CLI checks `operation_receipt` and returns immediately at lines 383-411.
That is unsafe for H-006 because a retry with the same fill ID but changed fee would
never reach repository fingerprint comparison.

For venue settlement calls:

1. Detect whether any venue option is present; require all four plus `fill_id` before
   loading/mutating the position.
2. Construct the same `Fill(VenueSettlement(...))` on first delivery and replay.
3. A pre-read may set the display-only `replayed` flag, but always invoke
   `close_position_with_fill`; repository `apply` decides replay versus conflict.
4. Render source, gross, fee, net, and PnL from the returned/stored
   `SettlementReceipt`, not from command-line flags.
5. Preserve the current Money/float rendering path for legacy operator and paper
   receipts.

Catch construction/tracker `ValueError` and exit 2 as the current close command does.
The subprocess acceptance test should discard the first stdout, replay all identical
fields in a new process, then retry the same fill ID with a different fee and prove no
state or receipt change.

## Makefile and test patterns

Extend `close-arb` rather than creating a command. Build one venue flag bundle only
when all required make variables are present and forward quoted values. The usage line
must show the all-or-none group. Mirror `test_close_arbitrage_target_forwards_partial_fill_identity`
with `make -n` assertions for all four flags.

The strongest end-to-end analog is the Phase 7 subprocess test:

- process 1 opens BUY 100 @ 0.40;
- process 2 books 30 shares with gross 13.80 and fee 0.30, then stdout is discarded;
- process 3 replays and receives gross 13.80, fee 0.30, net 13.50, PnL 1.50;
- raw SQLite shows q=70, cost=28, balance=973.50, one close operation, exact integer
  receipt micros, and the stored fingerprint;
- a changed-fee process exits 2 while all raw values remain unchanged.

## Planning hazards

- Do not let CLI receipt prefetch bypass the fingerprint check.
- Do not calculate venue truth from `exit_price` or `fee_rate_bps`.
- Do not weaken exact JSON decoding to accept extra/missing keys or bool-as-int.
- Do not perform fingerprint comparison outside `BEGIN IMMEDIATE`.
- Do not backfill a fabricated fingerprint for legacy receipts.
- Do not change paper Fill behavior or the H-005 residual allocation branch.
- Do not add live credentials, SDK calls, polling, or a second ledger in this phase.

