# M1 Perception Fault Qualification Runbook

## Safety boundary

`make qualify-perception-local` is an observer-only conformance check. It runs
the evaluator tests and a committed synthetic PASS fixture. Its PASS proves
only that the verdict contract and CLI work; it does not qualify a cloud
release, authorize deployment, enable a feature flag, inject a fault, or permit
wallet/order/trade activity.

Production qualification remains fail-closed until exact release, machine,
boot, evidence window, live samples, incident IDs, actions, cleanup results,
and component-specific recovery writer receipts are captured and bound into an
immutable evidence artifact.

## Local gate

```bash
make qualify-perception-local
```

The target:

1. runs `tests/m1-perception/test_perception_fault_acceptance.py`;
2. evaluates `tests/fixtures/perception-fault-acceptance-pass.json`;
3. writes a verdict to a fresh temporary directory using exclusive create;
4. prints canonical JSON containing `schema_version`, `status`, stable reason
   codes, and the canonical evidence SHA-256; and
5. removes the temporary directory.

The evaluator exits `0` for PASS, `1` for a valid FAIL verdict, and `2` for
invalid input/output. It refuses to overwrite an existing verdict.

## Current deterministic thresholds

| Evidence | Gate |
|---|---:|
| HTTP p95 | at most 2 s |
| high-priority Candidate Quote p95 | at most 30 s |
| high-priority stale declaration | at most 90 s |
| normal stale declaration | at most 120 s |
| liquidity-weighted active-known coverage | at least 90% within 15 min |
| oldest known-group visit | at most 6 h |
| promotion to watch | at most 60 s |
| Reconciliation | closes within 24 h or checkpoint visibly advances |
| MTTD / containment | at most 30 s / 60 s |
| cross-membership Quote / orphan collecting run | exactly zero |
| verified Incident | component-specific positive writer receipt required |

Missing, malformed, negative, non-finite, or wrong-type evidence fails closed.

## Production sequence

Do not skip or reorder:

1. pass local evaluator and deployment-contract tests;
2. pass the production read-only collector against the exact intended release;
3. run `make chaos-l2-fly-image-check` before any image-dependent primitive;
4. obtain authorization for the exact deployment or fault mutation;
5. establish baseline, inject one fault, clean up, verify writer-side recovery,
   and close its Incident before the next fault; and
6. after the matrix, collect and evaluate the required 24-hour continuous
   window.

A cleanup failure blocks every later injection. A locally green fixture can
never substitute for any production step.
