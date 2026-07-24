# L3 Sampler Startup Gate Design

## Production finding

Exact-SHA boot `ba6630c2-5ca9-49b2-a0c0-947bff9d1f03` started the promoter and
sampler as sibling tasks. The sampler won the scheduling race and attempted
boot-grid sample seq 0 before the promoter had published the five-market,
ten-token desired mapping.

`collect_sample()` correctly rejects fewer than five complete pairs, but
`run_sampler()` currently classifies every collection exception as
`evidence_writer_failed / sample_collection_failed`. That event is durable and
disallowed by the immutable soak contract even though no evidence write was
attempted. The next slot recovered, but the boot cannot pass readiness.

## Considered approaches

1. **Gate sampling until desired membership is exactly ten tokens
   (selected).** Skip startup grid slots while the promoter has not established
   the locked input cardinality. Preserve the boot grid and all existing
   fail-closed behavior after the gate opens.
2. **Make the sampler await a promoter-owned readiness event.** This gives an
   explicit synchronization primitive but couples two otherwise independent
   tasks, adds shutdown/cancellation states, and duplicates the runtime
   snapshot that already carries desired membership truth.
3. **Suppress or reclassify the first collection exception.** This is based on
   sequence position rather than cause and could hide a real seq-0 database or
   integrity failure.

The first approach is the narrowest cause-based boundary.

## Decision

At each boot-grid boundary, `run_sampler()` reads one runtime snapshot before
calling `sample_once()`:

- if `len(desired) != 10`, it advances to the next boundary without appending a
  health sample or runtime event;
- if `len(desired) == 10`, it runs the existing collection/write path
  unchanged.

The gate uses desired membership only. It does not wait for committed or
evidenced convergence: once the promoter has selected the exact five pairs,
incomplete WS convergence remains visible as ordinary failed health/market
samples. Any aggregate-read, mapping-integrity, slot, or append problem after
the gate opens still produces the existing fail-closed writer event.

Skipped startup slots are not evidence and cannot be used as T0. Readiness
still requires twelve later contiguous passing samples over at least 330
seconds, and manifest T0 remains an exact future boot-grid boundary.

## Verification

- A scheduler test starts with empty desired membership, proves seq 0 is
  skipped without calling `sample_once()`, publishes ten desired tokens, and
  proves seq 1 runs on its exact grid boundary.
- Existing tests continue to prove elapsed boundaries are skipped, early timer
  wakeups are rechecked, collection exceptions after the gate emit durable
  failure truth, and T0 requires an exact complete scheduled sample.
- Production requires another exact-SHA deployment and a new boot. The rejected
  boot remains immutable diagnostic evidence and can never host A5.

## Boundaries

- No AcceptanceConfig threshold changes.
- No event-kind exception is added.
- No manifest, report, schema, credential, trading, H-009, retention, or chaos
  behavior changes.
