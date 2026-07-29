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

## Production read-only baseline

```bash
make qualify-perception-prod-readonly expected_release=<40-char-sha>
```

The command performs only HTTPS GET requests to `/healthz`,
`/perception/discovery`, `/perception/reconciliation`,
`/perception/resources?limit=1`, and `/perception/incidents?limit=100`.
It also reads `/perception/qualification`, which explicitly counts
cross-membership Quote batches and expired collecting leases after validating
the perception authority.
It requires at least five samples with one unchanged `releaseId`, `machineId`,
and UUID `bootId`, then preserves `evidence.json` and `verdict.json` under a
new timestamped directory in `output/perception-qualification/`.

This baseline intentionally omits any metric the public read models cannot
prove. In particular, MTTD and containment require later fault-specific
evidence. Their absence produces a FAIL verdict rather than a default zero.
Cross-membership and orphan counters are explicit current observations; the
fault adapter must compare the same runtime before and after injection. An open
Incident also fails the final stability gate.

## Fault plan matrix

Every fault has a Make target with the `chaos-perception-` prefix. With no
arguments the target is read-only and prints its canonical JSON plan:

```bash
make chaos-perception-gamma-timeout
```

All 16 plans require only `python` in the deployed image and name the image
gate, component, expected incident behavior, authentic recovery writer, and
cleanup proof. The source of truth is `scripts/perception_chaos.py`.

| Family | Fault IDs | Recovery authority |
|---|---|---|
| Gamma | `gamma-timeout`, `gamma-partial`, `gamma-malformed` | Discovery batch |
| Gamma reconciliation | `gamma-cursor` | Reconciliation window progress |
| CLOB | `clob-missing-leg`, `clob-429`, `clob-latency` | Candidate success receipt |
| Producer | `candidate-exit`, `discovery-exit`, `reconciliation-stall` | component-specific writer above |
| Host/store | `sqlite-busy`, `disk-pressure`, `contention` | Candidate receipt or Resource decision |
| External/runtime | `telegram-failure`, `daemon-restart`, `deploy-interrupt` | notification state or release-bound HTTP probe |

`expected_incident_kind` values beginning with `not-wired:` are deliberate
blockers, not acceptable evidence. They mean the current batch/error path does
not yet open a durable Incident. Its execution adapter must first close that
chain-truth gap. A log line or resource decision alone cannot be relabeled as
an Incident.

Actual execution is an independent, fault-specific capability. It requires
all of the following:

```bash
make chaos-perception-gamma-timeout \
  mode=execute \
  expected_release=<40-char-sha> \
  authorization=fault:gamma-timeout:<40-char-sha> \
  evidence_dir=<new-path>
```

All targets except `candidate-exit` currently reject such requests with
`adapter-not-implemented` before creating the evidence directory or making a
network request. The Candidate adapter first passes the image gate and a
five-sample clean baseline, then binds one machine/boot/PID, writes immutable
intent, sends the exact SIGTERM, discovers the new Incident from the bounded
recent ledger, verifies its exact terminal history/writer receipt, and takes a
second five-sample clean window. A target becomes executable only after the
same adapter, cleanup, chain-truth health surface, and end-to-end review.

## Recovery verdict

Once a fault adapter has produced immutable `production-fault` evidence:

```bash
make verify-perception-recovery \
  evidence=<evidence.json> \
  output=<new-verdict.json> \
  expected_release=<40-char-sha>
```

This validates the same complete SLA set as the final gate, including exact
release/machine/boot/window provenance, zero open incidents, and a
component-matching positive recovery writer receipt. It exclusively creates
the verdict file and refuses overwrite. It does not inject, clean up, close an
Incident, or mutate production.

The fault runner must observe the Incident ID while it is open, then read
`GET /perception/incidents/{incident_id}/history`. This bounded endpoint
validates the incident checkpoint and retained suffix before returning at most
100 lifecycle events. `history_complete=false`, a missing terminal `verified`,
or a null `recovery_writer_receipt` fails qualification. This avoids SSH
database reads and keeps the same terminal proof visible to the operator and
Dashboard/API consumers.

To avoid racing a fast recovery, Candidate qualification discovers the ID via
`GET /perception/incidents/recent?scope=candidate&after_ms=<injection>&limit=10`.
That endpoint returns the latest retained state per matching Incident even
after it left the open list. More than one new `child-nonzero` ID is ambiguous
and fails the experiment.

For `candidate-exit`, the image-safe primitive is
`python -m polyarb.perception.chaos_primitive`. Its read-only `locate` command
requires exactly one Candidate worker whose command line is exact and whose
parent is the PID-1 M1 daemon. Its `terminate` command additionally binds the
runtime `POLYARB_RELEASE_ID`, the previously observed PID, and
`fault:candidate-exit:<release>:<pid>` before sending SIGTERM. It cannot target
PID 1, an arbitrary process, or a second component.
