# M1 Qualification Publication Liveness Design

**Date:** 2026-08-28

**Status:** Approved by the user's autonomous-repair authorization

**Scope:** Remove the production Structure certifier retry loop and align
qualification freshness with transactional publication truth.

## Production evidence

The current `structure-certify` job has 1,117 immutable range receipts. Each
attempt advances `verify-parity` at roughly one range per second, remains
heartbeat- and progress-healthy, then is cancelled at the generic 300-second
attempt deadline. Attempts 75 through 85 ended between range 286 and 297; the
same job reached lease epoch 86 without ever reaching its terminal manifest
commit.

The transactional Structure path deliberately does not publish the legacy
`structure:current` pointer. Its contract produces a certified Structure
manifest, admits Quote work, and eventually publishes `quote:current`. The
qualification worker nevertheless queries only `structure:current`, so it
emits a missing Structure freshness fact even when the transactional chain has
durable certified truth.

## Goals

1. Give `structure-certify` enough bounded wall-clock time to verify the
   largest admitted production generation without weakening heartbeat or
   progress-stall detection.
2. Derive Structure freshness from the certified Structure generation consumed
   by `quote:current`.
3. Preserve fail-closed behavior when any pointer, admission, or manifest link
   is missing.
4. Preserve every existing lease fence, terminal transaction, database role,
   recovery allowlist, and trading boundary.

## Design

### Job-specific absolute deadline

Runtime attempt creation will continue to derive the common profile from the
lease duration. Only `structure-certify` replaces the generic `lease * 10`
absolute attempt deadline with `lease * 120`, which is 3,600 seconds for the
production 30-second lease. The 10-second heartbeat deadline and 30-second
progress deadline stay unchanged.

This is intentionally an absolute ceiling rather than an unbounded
progress-renewed deadline. A certifier that advances forever still terminates
within one hour, while a stalled or lease-losing certifier is detected within
the existing short deadlines. At the observed production rate, 1,117 ranges
finish parity verification in about 19 minutes.

### Transactional Structure freshness

The Structure freshness query will start at `quote:current`, map its canonical
`quote:<structure bundle digest>` identity to `structure:<digest>`, and join
that exact generation to `m1_generation_manifests`. The query therefore
measures the certified Structure snapshot actually consumed by the currently
published Quote generation.

The query uses only relations already granted to the production qualification
capability. It accepts only `quote:` followed by exactly 64 lowercase hex
characters before deriving the Structure identity. A malformed pointer or
missing certified manifest returns no row, which keeps the existing
`evidence.gap` behavior. No compatibility pointer, new table grant, or
migration is needed.

## Verification

- Unit RED/GREEN proof for the job-specific 3,600-second attempt deadline while
  other job types remain at 300 seconds.
- Qualification SQL contract proof that the Structure query maps
  `quote:current` directly to the certified Structure manifest, touches no
  ungranted admission relation, and contains no legacy `structure:current`
  predicate.
- Real-PostgreSQL proof that matching malformed Quote and Structure manifests
  still produce fail-closed gaps for both freshness products.
- Focused runtime/qualification tests, the broader M1 control-plane suite,
  Ruff, formatting, `make planning-status`, and `make climb-check`.
- Production rollout only after local verification, followed by proof that one
  certifier attempt passes range 300 and reaches terminal success, all three
  freshness products return within the 900-second SLO, and a new accumulating
  qualification epoch advances without a breaking fact.

## Non-goals

- No recovery execution, fault injection, schema migration, role change, or
  pointer backfill.
- No wallet, signing, order, balance, or trade operation.
- No redesign of Structure range sizing or parity semantics.
