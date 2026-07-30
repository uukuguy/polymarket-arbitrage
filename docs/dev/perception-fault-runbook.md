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
blockers, not acceptable evidence. Gamma timeout, malformed response, and
cursor-integrity failures now open durable `gamma-*` Incidents in the bounded
Discovery/Reconciliation runners; their next exact batch/window writer closes
recovery. `gamma-partial` is intentionally different: a shape-valid but
incomplete page is persisted as rejected/partial coverage, not relabelled as a
process failure. Candidate CLOB missing-leg/429/latency and SQLite BUSY/LOCKED
open exact `candidate:<group_id>` Incidents; the SQLite fault is not called
`child-failed` because the fail-soft scheduler does not exit. A log line or
resource decision alone cannot be relabeled as an Incident.

Disk pressure and host contention are now derived from authenticated Resource
samples rather than the injection command: database-filesystem free bytes below
`POLYARB_RESOURCE_MIN_DISK_FREE_MB` opens `resource-disk-pressure`, while
one-minute load divided by CPU count at or above
`POLYARB_RESOURCE_MAX_LOAD_PER_CPU` opens `resource-contention`. Both contain by
entering `protect-hot-path`; only a later replay-valid normal/healthy Resource
decision closes recovery. Their production mutation adapters remain disabled.

Opportunity Telegram delivery reuses the durable outbox. A failed attempt opens
`telegram-delivery-failed` at `notification:<outbox-id>` and retains the card
for retry. Only a later `delivered` attempt for that exact outbox can verify
recovery; a different successful notification cannot close it. This proves the
Telegram API delivery writer, not handset display or user acknowledgement. Its
production failure adapter remains disabled.

Actual execution is an independent, fault-specific capability. It requires
all of the following:

```bash
make chaos-perception-gamma-timeout \
  mode=execute \
  expected_release=<40-char-sha> \
  machine_id=<exact-machine> \
  boot_id=<exact-boot-uuid> \
  call_class=gamma-discovery-event-page \
  target_key=discovery \
  parameters_json='{"delay_ms":10}' \
  evidence_dir=<new-path>
```

The eight typed upstream targets (four Gamma, three CLOB, and Telegram) use the
HTTP control orchestrator. Their Make invocation additionally requires exact
`machine_id`, `boot_id`, `call_class`, `target_key`, and `parameters_json`.
The ordinary and fault-control HMAC keys are loaded only from
`POLYARB_SCAN_SHARED_SECRET` and
`POLYARB_UPSTREAM_FAULT_CONTROL_SECRET`; no CLI authorization placeholder is
accepted. Missing input fails before transport construction or network access.
The orchestrator takes a five-sample green baseline, resolves exact producer
runtime, arms through double HMAC,
observes one injection and one Incident/coverage fact, and requests cleanup for
every `BaseException`. Cleanup failure freezes the remaining matrix. Evidence
is exported only after the component-specific business writer produces a
newer recovery receipt, and stops at `RECOVERED`. The source/export service
alone holds `POLYARB_UPSTREAM_FAULT_SOURCE_PRIVATE_KEY` and signs the complete
canonical source-derived envelope with a dedicated Ed25519 keypair. The
orchestrator preserves the authenticated HTTP response bytes exactly.

`candidate-exit`, `discovery-exit`, `reconciliation-stall`, and the remaining
host/store/restart/deploy targets are plan-only and reject execution fail-closed.

## Independent verdict and finalization

Keep the three phases in different processes and do not combine their secrets:

```bash
make evaluate-upstream-fault-candidate \
  evidence=<recovered.json> output=<new-candidate.json> \
  expected_release=<40-char-sha>

make finalize-upstream-fault \
  fault_id=<exact-id> artifact=<candidate.json> \
  expected_release=<40-char-sha>

make evaluate-upstream-fault-final \
  evidence=<re-exported-verified.json> candidate=<candidate.json> \
  output=<new-final.json> expected_release=<40-char-sha>
```

The candidate evaluator has only
`POLYARB_UPSTREAM_FAULT_SOURCE_PUBLIC_KEY` plus the separate
`POLYARB_UPSTREAM_FAULT_EVALUATOR_PRIVATE_KEY`. It verifies the producer
attestation, recomputes every intent/event/tail hash, binds the exact evidence
file SHA-256 and immutable source-facts digest, then signs a PASS artifact.
Source and verdict keypairs must never be reused. The
disabled-by-default finalizer has the ordinary and fault-control HMAC keys plus
the pinned `POLYARB_UPSTREAM_FAULT_EVALUATOR_PUBLIC_KEY`, but never the private
key or source signing key. In the same SQLite transaction it rebuilds the full
immutable source facts, rejects source changes, and enforces the persisted
source-valid-until deadline as `verdict-source-stale` before appending
`VERIFIED(verdict_id, verdict_digest)`. The final evaluator is read-only and
holds only the source and verdict public keys. Re-export and final evaluation
are mandatory: a finalizer response alone is not qualification evidence.

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

For `candidate-exit` and `discovery-exit`, the image-safe primitive is
`python -m polyarb.perception.chaos_primitive`. Its read-only `locate` command
requires exactly one requested worker whose command line is exact and whose
parent is the PID-1 M1 daemon. Its `terminate` command additionally binds the
runtime `POLYARB_RELEASE_ID`, the previously observed PID, and
the component-specific `fault:<component>-exit:<release>:<pid>` before sending SIGTERM. It cannot target
PID 1, an arbitrary process, or a second component.

`reconciliation-stall` uses a separately authorized SIGSTOP/SIGCONT pair. The
child emits authenticated `yielded` liveness every 12.5 seconds during its
normal 60-second inter-page wait. Missing liveness opens durable
`child-stalled` at the configured 25-second detection point; the process is not
killed until the independently configured 180-second hard timeout. Once the
Incident is visible, the adapter resumes the same release/PID and waits for a
new Reconciliation page checkpoint to supply the recovery writer receipt.
SIGCONT is also attempted from `finally` if observation fails after SIGSTOP.
This preserves MTTD headroom without relabelling normal idle time as failure or
lowering the statistically tuned restart threshold.
