---
quick_id: 260602-diag-depth
type: quick
ws: m1-perception
status: complete
completed: 2026-06-02
parent_phase: 05-ws-book-prices
follow_up_of: 260601-included-at-ts
files_modified:
  - src/polyarb/observation/l3_promote.py (env override in _load_recipe)
  - tests/m1-perception/test_l3_promoter.py (+3 RED→GREEN tests)
---

# Quick Task SUMMARY: `POLYARB_L3_DEPTH_MIN_USD` env override for L3 depth threshold

## Context (Phase 05 Wave 5 carry bug 2 — observed SESSION 35 after v24 deploy)

After `260601-included-at-ts` (bug 1 fix) shipped to prod v24, `l2_candidates`
upserts succeeded (`+61 -3` candidates inserted) and 61 assets got subscribed
on WS. But `l3:active_count` stayed at 0/10 across the next two 5-min promoter
cycles. Carry-bugs memory's "bug 2 likely auto-heals" hypothesis did not hold.

Root cause (chain-walked):

1. `compute_candidates` yielded 61 candidates from BUILTIN_RECIPES union
   (`near-end`, `thick-but-slippery`, `ghost-suspicious`, etc.)
2. These recipes order by `liquidity_usd DESC` — picking high-volume markets
   that happen to be extreme-priced (one side near $1, the other near $0)
3. Extreme-priced markets have ample NO-side depth but no YES bids
4. `l3-promote.yaml` recipe filters `depth_yes_usd > 500` → ALL 61 rejected
5. Promoter outputs `+0 -0 markets=0 tokens=0` consistently

We need real data on the depth distribution before deciding the fix
(threshold retune? candidate-recipe rework? both?). Tuning blindly violates
CLAUDE.md "experiment values never touch baseline defaults" if we mutate
the yaml.

## Resolution

`_load_recipe` now reads `POLYARB_L3_DEPTH_MIN_USD` at load time:

- **Unset (default)** → yaml baseline (`> 500`) used verbatim
- **Set to a valid float N** → WHERE rewritten in-memory to `depth_yes_usd > N`,
  INFO log line emitted for audit
- **Invalid value** → fallback to yaml baseline + WARNING log

The yaml file on disk is never touched. Override is intended to be set via
`fly secrets set POLYARB_L3_DEPTH_MIN_USD=<N>` for diagnostic windows, then
unset (or set back to 500) once the right baseline is known.

## Tests

3 new tests in `tests/m1-perception/test_l3_promoter.py`:

- `test_load_recipe_default_threshold_is_yaml_baseline` — env unset, WHERE
  unchanged
- `test_load_recipe_env_override_substitutes_threshold` — env=0, WHERE shows
  `depth_yes_usd > 0`, baseline 500 removed
- `test_load_recipe_env_override_invalid_falls_back_to_yaml` — env value
  `"not-a-number"`, WHERE falls back to baseline 500

RED commit: 1/3 (override + invalid) failed. GREEN commit: 3/3 pass.

## Regression

```
tests/m1-perception/test_l3_promoter.py            (15 tests) ✓
tests/observation/test_l2_candidate_refresh.py     (22 tests) ✓
make planning-status                                (zero drift) ✓
```

## Deployment & observation plan

This is a **diagnostic** ship — its purpose is to observe prod data, not lock
in a new threshold:

1. `git push` (covers 260602 atomic commits — RED + GREEN + this SUMMARY)
2. `env -u FLY_API_TOKEN flyctl secrets set POLYARB_L3_DEPTH_MIN_USD=0 \\
    --app polyarb-l2 --config fly-l2.toml` (triggers redeploy)
3. Wait one full promote cycle (5 min interval). Observe:
   - `l3:active_count` should be > 0 (probably saturate at 10 because every
     candidate now matches the loose filter)
   - `flyctl logs` will show actual `depth_yes_usd` values via the promoter's
     SQL output (`order_by depth_yes_usd DESC` puts the highest values first)
4. From the observed distribution, decide:
   - **(a) keep baseline at 500**: data shows real candidates exist with
     depth ≥ 500 but they got starved by candidate recipe selection →
     open follow-up quick task `260602-candidate-diversity`
   - **(b) retune baseline**: real distribution caps at e.g. ~250 USD →
     update `l3-promote.yaml` to a calibrated threshold (separate commit,
     yaml baseline change with audit trail)
   - **(c) both**: candidate-diversity + threshold retune in tandem
5. Whichever path: `flyctl secrets unset POLYARB_L3_DEPTH_MIN_USD` once the
   diagnosis is done, so baseline stays clean.

## Why not just lower yaml baseline?

`l3-promote.yaml` carries an audit trail comment ("D-13 阈值 baseline — DO NOT
promote thresholds to env var; tune by editing this file + commit"). That
discipline is correct for *production* tuning, but this is a *diagnostic*
window — we need an ephemeral knob to discover what the prod data actually
looks like before we know what value to commit. Once the right threshold is
known (post-observation), it gets a dedicated yaml commit with rationale.

## Why not change the candidate recipe?

That's option (a)/(c) from the deployment plan — possibly the right answer.
But we don't know yet whether candidates exist with depth ≥ 500 that we're
just not selecting, versus the whole prod universe being shallow on the YES
side. The env override gives us the data to choose intelligently.

## Pre-existing bugs found / impact

This quick task does not fix a bug — it adds a diagnostic surface. It does
expose a methodological gap: **D-13 thresholds (500, 0.02, etc.) were locked
in Phase 05 plan-phase without prod data**. The 24h soak (D-12) is the first
opportunity to validate them, but the soak cannot start until we know the
thresholds are at-all-reachable. This quick task bridges that chicken-egg
gap. Lesson for future phases: ship a "diagnostic mode" env knob alongside
any data-driven threshold so we can observe before locking.
