# Phase 8 Research — Venue-Truth Fill Reconciliation

**Date:** 2026-07-17  
**Requirement:** H-006

## Protocol facts that constrain the plan

1. Polymarket authenticated trade objects expose immutable trade ID, exact-looking
   string `size`, price, `fee_rate_bps`, and lifecycle status. `CONFIRMED` is the only
   successful terminal state; `MATCHED`, `MINED`, and `RETRYING` can still fail/reorg.
2. The ordinary trade response does not expose actual fee cash. A fee rate plus local
   formula is an estimate and cannot populate venue truth.
3. Builder trade responses expose `size`, `sizeUsdc`, `fee`, and `feeUsdc`, providing a
   documented complete source for exact share/cash/fee normalization.
4. Therefore the domain accepts normalized complete settlement facts. Adapter/network
   selection is deferred; incomplete payloads fail rather than mixing confirmed size
   with modeled cash.

Primary references:

- <https://docs.polymarket.com/trading/orders/overview>
- <https://docs.polymarket.com/api-reference/trade/get-trades>
- <https://docs.polymarket.com/api-reference/trade/get-builder-trades>

## Existing patterns to reuse

- `Money` and `Quantity` already quantize public decimal-facing inputs once to signed
  SQLite-safe micro integers.
- `PositionTracker.close_position_with_fill` owns allocation, cash/PnL mutation, and
  remaining authority in one repository transition.
- `SQLitePositionRepository.apply` uses `BEGIN IMMEDIATE`, checks operation identity,
  applies a copied state, writes state + tagged receipt, and commits atomically.
- `_encode_result/_decode_result` already provide additive tagged receipt compatibility;
  extend this union instead of adding a second receipt store.
- SQLite migrations classify missing/all/partial authority and validate dynamic types.
- CLI process tests already prove committed-response-loss recovery by discarding stdout
  and starting another `uv run python -m ...` process.

## Recommended data model

### VenueSettlement

Immutable value with `gross_cash: Money`, `fee: Money`, `status: str`, and non-empty
`source_ref`. Construction requires non-negative gross/fee, fee <= gross, and
`status == "CONFIRMED"`. It does not contain price-derived values.

### SettlementReceipt

Immutable result with exact `gross_cash`, `fee`, `net_cash`, `realized_pnl`, and
`source="venue-confirmed"`. Repository JSON should be:

```json
{"kind":"settlement","source":"venue-confirmed","gross_micros":13800000,"fee_micros":300000,"net_micros":13500000,"pnl_micros":1500000}
```

Decode requires the exact key set, integer dynamic types (bool rejected), Money bounds,
`net=gross-fee`, and `pnl=net-allocated-cost` only at creation time. Legacy Money/float
receipts remain readable.

### Operation request fingerprint

Add `request_fingerprint TEXT NOT NULL DEFAULT ''` to `m2_applied_operations`. Existing
rows backfill to empty. Venue settlement fingerprint must deterministically include
market, fill quantity micros, gross/fee micros, status, and source reference. On replay,
`apply` compares stored and supplied fingerprints inside the same `BEGIN IMMEDIATE`.
Different non-empty fingerprints raise `operation identity conflict`; identical values
return the receipt. Empty legacy operations retain compatibility.

Do not use a transaction-external `get_receipt` precheck: two processes can both see
none and race with conflicting payloads.

## Tracker transition

For exact filled quantity `f` against current q/cost:

1. retain H-005 zero/overfill/anonymous checks;
2. allocate exact cost basis using `Money.allocate`;
3. if settlement exists, require fill ID and compute `net=gross-fee`,
   `pnl=net-allocated_cost`; never call `Money.pnl_for`;
4. update `balance += net`, `realized_pnl += pnl`, and remaining q/cost;
5. return `SettlementReceipt`; modeled path continues to return Money.

The supplied exit price remains observational when venue settlement exists. A test must
set a deliberately wrong price and still obtain venue cash PnL.

## CLI/Engine surface

- Engine already forwards Fill; it needs tests proving structured venue receipt survives
  duplicate delivery/restart and nonterminal settlement leaves the position open.
- CLI adds all-or-none `--venue-cash`, `--venue-fee`, `--venue-status`, `--venue-ref`.
  These require `--fill-id`; any subset exits 2 before mutation.
- Replay output must expose source, gross, fee, net, and PnL from the stored receipt, not
  reconstruct from current CLI flags.
- Makefile forwards these arguments through existing `close-arb`; no new command.

## Security and failure analysis

| Threat | Severity | Mitigation |
|---|---|---|
| Fee-rate-only payload mislabeled actual | High | require actual gross + fee fields; no formula fallback |
| MATCHED cash booked before finality | High | constructor accepts only CONFIRMED |
| Same fill ID, changed fee/cash | High | fingerprint comparison under BEGIN IMMEDIATE |
| Receipt JSON unit/type corruption | High | exact tagged key set and integer/identity validation |
| Concurrent duplicate process | Medium | SQLite writer serialization + fingerprinted receipt replay |
| CLI partial argument set | Medium | validate all-or-none before tracker mutation |

No secrets, signing, network writes, or external state mutation are introduced.

## Validation Architecture

### Focused feedback gates

- receipt codec/fingerprint: `uv run pytest tests/routing/test_position_repository.py -q`
- tracker reconciliation: `uv run pytest tests/routing/test_position_tracker.py -q`
- Engine: `uv run pytest tests/execution/test_engine.py -q`
- subprocess/CLI: `uv run pytest tests/cli/test_arbitrage_cli_process.py tests/test_makefile.py -q`

### Full gate

```bash
uv run pytest tests/models/test_slippage.py tests/routing tests/execution tests/cli -q
uv run pytest tests/test_makefile.py tests/climb -q
make planning-status
git diff --check
```

### Required RED vectors

1. structured settlement receipt codec/restart and corrupt JSON rejection;
2. conflicting same-fill fingerprint across two repository instances;
3. wrong paper price but exact venue gross/fee drives balance/PnL;
4. nonterminal/incomplete/fee-over-gross settlement rolls back;
5. true subprocess lost-response replay returns stored structured fields;
6. CLI same fill ID with changed fee fails without mutation.

Existing pytest/SQLite infrastructure covers all requirements; no Wave 0 dependency or
manual-only verification is needed.

## Scope guard

Do not add a live SDK dependency, fee formula, wallet configuration, polling daemon,
second fill ledger, or general accounting framework. H-006 is complete when normalized
venue facts are exact, atomic, durable, observable, and provably override modeled cash.
