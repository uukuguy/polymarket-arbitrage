# L3 Soak Manifest Runtime Lock Design

## Problem

Phase 05.4 binds one immutable five-market mapping before T0 and the verifier
rejects any second mapping hash inside `[T0,T24)`. Production proves that the
normal promoter can legitimately rotate the depth-ranked Top-5 every five
minutes. A manifest that only records the mapping cannot therefore enforce the
identity it promises.

The fix must make the existing phrase “manifest fixes mapping” true at runtime
without weakening the verifier or changing normal, unbound promoter behavior.

## Considered approaches

1. Wait for a naturally stable Top-5. Rejected: production already alternated
   between two mappings, and a 24-hour success would depend on luck.
2. Permit mapping drift in the verifier. Rejected: this removes a locked
   identity invariant and makes the manifest non-authoritative.
3. Treat a database-bound manifest as a time-bounded runtime mapping lock.
   Selected: it preserves the strict verifier and scopes the behavior change to
   the exact formal window.

## Binding contract

The one `soak_manifest_bound` event remains the durable source of truth. Its
canonical `detail` object contains exactly:

```json
{
  "manifest_sha256": "<64 lowercase hex>",
  "mapping_hash": "<64 lowercase hex>",
  "t0": "<canonical UTC RFC3339>",
  "t24": "<canonical UTC RFC3339>"
}
```

The existing event ID, event sequence, soak hash reason code, boot ID, and
server `recorded_at < T0` checks remain unchanged. Binding continues to use a
transaction advisory lock so the append-only runtime role needs no UPDATE
privilege.

## Runtime read model

`L3EvidenceStore.fetch_active_soak_mapping_lock(boot_id, observed_at)` returns a
validated immutable `SoakMappingLock` or `None`.

- Only binding events for the exact boot are considered.
- A lock is active only when `t0 <= observed_at < t24`.
- Multiple overlapping bindings are allowed only when every active binding has
  the same mapping hash. This supports a rejected T0 followed by a later
  attempt on the same locked mapping.
- Malformed timestamps, hashes, late bindings, or conflicting active hashes
  raise a typed evidence-read failure. They never fall back to dynamic
  selection.

## Promoter behavior

At each promoter tick, the production evidence store is queried once for an
active lock.

- No active lock: preserve the current dynamic Top-5 recipe exactly.
- Active lock: reconstruct the five mapping rows from the current desired or
  committed tokens plus the bounded last-known token-identity cache. The
  canonical mapping hash must equal the lock.
- A complete match reuses those 5 markets/10 tokens as the proposal while the
  existing control, generation, evidence, mirror, and terminal-ledger paths
  continue unchanged.
- Missing cache identity, wrong cardinality, or hash mismatch produces one
  terminal non-success row and no substitute mapping.
- At and after T24, the lock is inactive and normal dynamic selection resumes.

The sampler remains independent and recomputes its hash from
`markets_latest`. Equality between promoter, sampler, manifest, and verifier is
therefore still measured rather than asserted.

## Failure and security properties

- Runtime credentials retain SELECT/INSERT only; no owner or retention
  capability is added.
- The lock cannot be supplied through an environment variable or local file.
- Database unavailability terminalizes the promoter tick rather than silently
  bypassing the lock.
- Existing immutable rejected manifests and bindings remain readable; nothing
  is updated or deleted.
- No trading, order placement, or H-009 behavior is introduced.

## Verification

Tests must prove:

1. binding detail is exact and tamper-sensitive;
2. active-window, endpoint, overlapping-same-hash, conflicting-hash, and
   malformed-row store behavior;
3. unbound promoter behavior remains dynamic;
4. a bound mapping survives a changing recipe result and records the manifest
   hash with truthful 5/10/10 cardinality;
5. an unavailable or mismatched bound mapping terminalizes without mutation;
6. production runtime role can execute the advisory lock and read the binding
   while still lacking UPDATE;
7. focused suites, full pytest, changed-file Ruff, compile, manual docs, and
   planning-status all pass before exact-SHA deployment.

