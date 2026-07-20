# M1 L3 Prerequisite Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the strict five-market/ten-token L3 promotion gate reachable without changing its spread, depth, recency, or N=5 thresholds.

**Architecture:** Preserve both binary outcome token IDs in the L1 Supabase projection, seed L2 with at most 100 deterministic liquid mid-price Yes assets, and resolve the selected Yes-side TOB assets back to complete Yes/No pairs. Promotion remains fail-closed on incomplete identity, while the operator dry-run uses the same read/plan path with all mutations disabled.

**Tech Stack:** Python 3.12, pytest, Alembic/SQLAlchemy, SQLite, Supabase PostgREST client, async WebSocket consumer, Make.

## Global Constraints

- Keep the Phase 05 recipe predicates unchanged: `spread < 0.02`, `depth_yes_usd > 500`, recent TOB, limit five.
- `l3:active_count == 10` means five complete, distinct Yes/No pairs; never synthesize a missing token.
- The L2 seed is exactly `mid_price BETWEEN 0.1 AND 0.9`, `liquidity_usd >= 500`, non-null `yes_token_id`, ordered by `liquidity_usd DESC, market_id ASC`, limit 100.
- The existing global `MAX_CANDIDATES = 500`, watchlist precedence, reconciliation, and WS update behavior remain unchanged.
- Alembic 006 is add-only in `upgrade()` and adds nullable TEXT `markets_latest.no_token_id` after revision 005.
- `make l3-promote-dry-run` may read production-backed Supabase data when credentials are supplied, but performs zero remote writes, zero WS mutations, and zero module-state mutations.
- No production migration, deploy, restart, scale, config, secret, threshold, external submission, or real-money action is authorized by this plan.
- Every new executable entry is exposed through Makefile; direct Python commands below are verification commands, not new user-facing workflows.

---

## File map

- Create `alembic/versions/006_add_no_token_id.py`: add-only durable No-token projection.
- Create `tests/alembic/test_006.py`: static and local Postgres replay contract for revision 006.
- Modify `src/polyarb/storage/supabase_mirror.py`: widen the narrow market projection to 12 columns.
- Modify `tests/storage/test_supabase_mirror.py`: verify No-token passthrough and exact projection shape.
- Modify `src/polyarb/observation/l2_temp_db.py`: map `no_token_id` from `markets_latest` instead of NULL-filling it.
- Modify `tests/observation/test_l2_temp_db.py`: verify both token IDs survive the temp adapter.
- Modify `src/polyarb/observation/recipes.py`: add deterministic built-in `l3-seed`.
- Modify `tests/observation/test_l2_candidate_refresh.py`: verify seed bounds, cap, and specialized-recipe precedence.
- Modify `Makefile` and `tests/test_makefile.py`: expose `scan-l3-seed` and preserve dry-run wording.
- Modify `src/polyarb/observation/l3_promote.py`: Yes-keyed lookup, complete-pair validation, and explicit apply boundary.
- Modify `tests/m1-perception/test_l3_promoter.py`: production-shaped token lookup, 5→10 proof, and incomplete/duplicate fail-closed cases.
- Modify `scripts/l3_promote_dry_run.py`: invoke `promote_run(..., apply_mutations=False)` and print proposed rather than applied state.
- Create `tests/m1-perception/test_l3_promote_dry_run.py`: process-level dry-run contract.
- Create `docs/learning/21-L3-候选与双Token.md` and modify `docs/learning/00-INDEX.md`: explain observation coverage vs promotion and the Yes/No identity chain. Sequence 21 is required because 12–20 already exist.

## Execution registration

Register Phase `05.3: L3 prerequisite repair` with four sequential plans. The
canonical design is `docs/superpowers/specs/2026-07-20-m1-l3-prerequisite-repair-design.md`.
The four GSD plan pointers map one-to-one to Tasks 1–4 below. Run
`make planning-status`; expected result is four `NOT-STARTED` plans and no
`DRIFT`. Commit the planning artifacts before implementation.

### Task 1: Persist the No token through migration, mirror, and temp projection

**Files:**
- Create: `alembic/versions/006_add_no_token_id.py`
- Create: `tests/alembic/test_006.py`
- Modify: `src/polyarb/storage/supabase_mirror.py`
- Modify: `tests/storage/test_supabase_mirror.py`
- Modify: `src/polyarb/observation/l2_temp_db.py`
- Modify: `tests/observation/test_l2_temp_db.py`

**Interfaces:**
- Produces: nullable `markets_latest.no_token_id`, 12-column `_NARROW_MARKET_COLUMNS`, and a temp `markets.no_token_id` populated from the narrow row.
- Consumes: existing normalizer-shaped `dict` rows containing `yes_token_id` and `no_token_id`.

- [x] **Step 1: Write failing schema and projection tests**

Create `tests/alembic/test_006.py` with static assertions for `revision = "006"`,
`down_revision = "005"`, `op.add_column("markets_latest", ...)`, nullable
`sa.Text`, and no `op.drop_` inside `upgrade()`. Reuse the Docker fixture pattern
from `tests/alembic/test_004.py` to prove upgrade/downgrade/re-upgrade locally.

Extend the storage test with these exact contracts:

```python
def test_narrow_no_token_id_passthrough_and_nullable() -> None:
    from polyarb.storage.supabase_mirror import narrow_market_row
    assert narrow_market_row(_make_full_row(no_token_id="NO-X"), 42)["no_token_id"] == "NO-X"
    missing = _make_full_row()
    missing.pop("no_token_id")
    assert narrow_market_row(missing, 42)["no_token_id"] is None
    assert narrow_market_row(_make_full_row(no_token_id=None), 42)["no_token_id"] is None
```

Change the exact-column test to expect 12 keys including `no_token_id`. Add a
temp-DB assertion:

```python
def test_build_temp_db_preserves_token_pair() -> None:
    tmp = build_temp_db([_narrow_row("m1", no_token_id="NO-m1")])
    try:
        with sqlite3.connect(tmp) as con:
            assert con.execute(
                "SELECT yes_token_id, no_token_id FROM markets WHERE market_id='m1'"
            ).fetchone() == ("YES-m1", "NO-m1")
    finally:
        os.unlink(tmp)
```

- [x] **Step 2: Run RED**

Run:

```bash
uv run pytest tests/alembic/test_006.py tests/storage/test_supabase_mirror.py tests/observation/test_l2_temp_db.py -q
```

Expected: failures because revision 006 is absent, the projection has 11 keys,
and the temp DB currently NULL-fills `no_token_id`.

- [x] **Step 3: Implement the minimal durable projection**

Create the migration with:

```python
revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column(
        "markets_latest",
        sa.Column("no_token_id", sa.Text, nullable=True),
    )

def downgrade() -> None:
    op.drop_column("markets_latest", "no_token_id")
```

Append `"no_token_id"` to `_NARROW_MARKET_COLUMNS`. Add
`"no_token_id": "no_token_id"` to `_NARROW_TO_MARKETS` and remove it from
`_NULL_FILLED_COLS`.

- [x] **Step 4: Run GREEN and regression tests**

Run:

```bash
uv run pytest tests/alembic/test_006.py tests/storage/test_supabase_mirror.py tests/observation/test_l2_temp_db.py tests/m1-perception/test_supabase_mirror.py -q
```

Expected: all static/projection tests pass; Docker-dependent replay either passes
or reports its existing explicit skip when Docker is unavailable.

- [x] **Step 5: Create `05.3-01-SUMMARY.md` and commit**

Commit migration, projection, tests, and SUMMARY as:

```bash
git commit -m "feat(05.3-01): persist L3 token pairs"
```

### Task 2: Add the bounded liquid mid-market L2 seed

**Files:**
- Modify: `src/polyarb/observation/recipes.py`
- Modify: `tests/observation/test_l2_candidate_refresh.py`
- Modify: `Makefile`
- Modify: `tests/test_makefile.py`

**Interfaces:**
- Produces: `BUILTIN_RECIPES["l3-seed"]` and user entry `make scan-l3-seed`.
- Consumes: the 12-column temp `markets` projection from Task 1.

- [x] **Step 1: Write failing seed and Make-contract tests**

Add a test that creates 105 qualifying rows with descending liquidity, plus rows
at `0.099`, `0.901`, liquidity `499.99`, and missing Yes ID. Assert:

```python
rows = compute_candidates(settings_with_db, markets_rows=market_rows)
seed = [row for row in rows if row.recipe_name == "l3-seed"]
assert len(seed) == 100
assert all(row.asset_id for row in seed)
assert "YES-out-low" not in {row.asset_id for row in seed}
assert "YES-out-high" not in {row.asset_id for row in seed}
assert "YES-out-liq" not in {row.asset_id for row in seed}
```

Add an overlap case where one qualifying row also matches `near-end`; assert its
final `recipe_name == "near-end"`. Add a Makefile contract assertion that the
`scan-l3-seed` recipe invokes `cli_observation scan --name l3-seed --verbose`.

- [x] **Step 2: Run RED**

Run:

```bash
uv run pytest tests/observation/test_l2_candidate_refresh.py tests/test_makefile.py -k "l3_seed or scan_l3_seed" -q
```

Expected: failures because the recipe and Make target do not exist.

- [x] **Step 3: Implement the built-in recipe and Make target**

Insert `l3-seed` before specialized built-ins:

```python
"l3-seed": Recipe.from_builtin(
    name="l3-seed",
    description="L3 观察覆盖：流动性中间价市场（上限 100）",
    where=(
        "yes_token_id IS NOT NULL "
        "AND mid_price BETWEEN 0.1 AND 0.9 "
        "AND liquidity_usd >= 500"
    ),
    order_by="liquidity_usd DESC, market_id ASC",
    limit=100,
),
```

Add:

```make
## scan-l3-seed: Liquid mid-price markets used to give L3 representative books
scan-l3-seed:
	uv run python -m polyarb.cli_observation scan --name l3-seed --verbose
```

Add it to the relevant `.PHONY` declaration and `make help` discovery path.

- [x] **Step 4: Run GREEN and candidate regressions**

Run:

```bash
uv run pytest tests/observation/test_l2_candidate_refresh.py tests/m1-perception/test_observation_scanner.py tests/test_makefile.py -q
```

Expected: all candidate, scanner, and Make contract tests pass.

- [x] **Step 5: Create `05.3-02-SUMMARY.md` and commit**

```bash
git commit -m "feat(05.3-02): seed representative L3 books"
```

### Task 3: Resolve complete token pairs by Yes-side TOB asset

**Files:**
- Modify: `src/polyarb/observation/l3_promote.py`
- Modify: `tests/m1-perception/test_l3_promoter.py`

**Interfaces:**
- Produces: `_fetch_market_token_map(client, yes_asset_ids) -> dict[str, tuple[str, str]]` keyed by the Yes token ID.
- Consumes: selected `l2_top_of_book.asset_id` values, which are Yes token IDs, and the Task 1 production projection.

- [x] **Step 1: Write failing PostgREST and fail-closed tests**

Update fixtures so TOB `asset_id` values equal their `yes_token_id`. Assert the
query builder receives:

```python
markets_table.select.assert_called_once_with("yes_token_id, no_token_id")
markets_table.in_.assert_called_once_with("yes_token_id", selected_yes_ids)
```

Add cases for a missing No token, `yes == no`, and a token repeated across two
pairs. For each, assert the result has fewer than ten tokens, contains no
synthesized fallback, and the warning names the rejected Yes asset. Retain the
positive proof that five complete distinct pairs subscribe exactly ten tokens.

- [x] **Step 2: Run RED**

Run:

```bash
uv run pytest tests/m1-perception/test_l3_promoter.py -k "token_map or yes_no or incomplete or duplicate" -q
```

Expected: failures show the old query selects nonexistent `asset_id`, filters on
the wrong column, and falls back to the TOB asset.

- [x] **Step 3: Implement Yes-keyed lookup and complete-pair validation**

Replace the helper query with:

```python
resp = (
    client.table("markets_latest")
    .select("yes_token_id, no_token_id")
    .in_("yes_token_id", yes_asset_ids)
    .execute()
)
```

Build the map only from non-empty Yes keys. During expansion, accept a pair only
when Yes and No are non-empty, distinct, and neither token was already claimed by
another selected pair. Log and skip invalid pairs; delete the old asset fallback.
Keep the existing outage freeze behavior and use Yes asset IDs for
`l2_candidates.l3_promoted_at_ts` updates.

- [x] **Step 4: Run GREEN and health regressions**

Run:

```bash
uv run pytest tests/m1-perception/test_l3_promoter.py tests/m1-perception/test_l2_health_l3_subchecks.py tests/m1-perception/test_candidate_refresh_l3_protection.py -q
```

Expected: all existing promoter, health, and WS-set protection tests pass.

- [x] **Step 5: Create `05.3-03-SUMMARY.md` and commit**

```bash
git commit -m "fix(05.3-03): resolve L3 pairs by Yes token"
```

### Task 4: Make dry-run non-mutating, document the model, and close local H-010 gates

**Files:**
- Modify: `src/polyarb/observation/l3_promote.py`
- Modify: `scripts/l3_promote_dry_run.py`
- Create: `tests/m1-perception/test_l3_promote_dry_run.py`
- Modify: `tests/m1-perception/test_l3_promoter.py`
- Create: `docs/learning/21-L3-候选与双Token.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/CURRENT.md`
- Modify: `docs/status/climb/*` through the existing deterministic cycle path.

**Interfaces:**
- Produces: `promote_run(..., apply_mutations: bool = True) -> dict` and a truthful `make l3-promote-dry-run`.
- Consumes: Task 3's proposed `added`, `removed`, and complete-pair map.

- [x] **Step 1: Write failing zero-mutation tests**

Seed every module-level field with a sentinel, invoke
`promote_run(..., apply_mutations=False)`, and assert:

```python
consumer.add_subscriptions.assert_not_awaited()
consumer.remove_subscriptions.assert_not_awaited()
assert capture_updates == []
assert l3_promote._l3_active_set == before_active
assert l3_promote._last_known_tob_rows is before_tob
assert l3_promote._last_known_market_token_map is before_map
assert l3_promote._last_promote_at_s == before_promote
assert result["dry_run"] is True
assert len(result["proposed_active"]) == 10
```

Add a script test that patches `promote_run` and asserts `_main()` passes
`apply_mutations=False` and does not report an “active set after dry-run”.

- [x] **Step 2: Run RED**

Run:

```bash
uv run pytest tests/m1-perception/test_l3_promoter.py tests/m1-perception/test_l3_promote_dry_run.py -k "dry_run or apply_mutations" -q
```

Expected: failure because `promote_run` has no apply boundary and currently
mutates the mirror/global anchors.

- [x] **Step 3: Implement the explicit apply boundary**

Add the keyword-only parameter with default `True`. Compute reads, recipe output,
pair validation, and proposed diffs in both modes. Guard all assignments to
last-known-good globals, WS calls, `_mirror_l3_promoted_at_ts`, `_l3_active_set`,
and `_last_promote_at_s` behind `if apply_mutations:`. Return:

```python
{
    "added": added,
    "removed": removed,
    "active": sorted(_l3_active_set) if apply_mutations else sorted(new_token_set),
    "proposed_active": sorted(new_token_set),
    "dry_run": not apply_mutations,
}
```

Update the script to pass `apply_mutations=False` and label the output
`proposed_active`; retain the Make target name and clarify that both WS and
Supabase mutations are disabled.

- [x] **Step 4: Run focused GREEN, lint, and full repository gates**

Run in order:

```bash
uv run pytest tests/alembic/test_006.py tests/storage/test_supabase_mirror.py tests/observation/test_l2_temp_db.py tests/observation/test_l2_candidate_refresh.py tests/m1-perception/test_l3_promoter.py tests/m1-perception/test_l3_promote_dry_run.py tests/test_makefile.py -q
uv run ruff check alembic/versions/006_add_no_token_id.py src/polyarb/storage/supabase_mirror.py src/polyarb/observation/l2_temp_db.py src/polyarb/observation/recipes.py src/polyarb/observation/l3_promote.py scripts/l3_promote_dry_run.py tests/alembic/test_006.py tests/storage/test_supabase_mirror.py tests/observation/test_l2_temp_db.py tests/observation/test_l2_candidate_refresh.py tests/m1-perception/test_l3_promoter.py tests/m1-perception/test_l3_promote_dry_run.py tests/test_makefile.py
uv run pytest -q
make planning-status
make climb-check
git diff --check
```

Expected: zero failures/errors, with only environment-declared skips/xfails.

- [x] **Step 5: Write the teaching document and close local planning artifacts**

The teaching document must contain: a 30-second model, the full L1→L2→L3 token
flow with current file/line references, why observation seed is not promotion,
why `asset_id` means Yes token in this chain, design trade-offs, three adversarial
self-check questions, and an FAQ increment section. Update the learning index.

Create `05.3-04-SUMMARY.md`, `05.3-LEARNINGS.md`, and a local validation artifact.
Mark H-010 confirmed only after the fresh full gates above produce a score of 100;
do not attach production evidence or claim the Phase 05 soak passed.

- [x] **Step 6: Commit local closure**

```bash
git commit -m "fix(05.3-04): make L3 dry-run mutation-free"
```

Stop after the local closure commit. The next command is a separately authorized
production migration/deploy action, not part of this implementation plan.
