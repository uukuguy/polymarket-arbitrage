# Task 7 Fresh Production Review

Baseline: `fe89de0`
Reviewed HEAD: `c87250d`

## Spec Compliance ❌

Task 7 is not production-safe despite the green focused and full-suite reports.
The typed adapters, append-only authority, HTTP boundary, and local E2E tests
are substantial, but the committed production chain still has fail-open audit
claims and an unsafe post-arm cleanup gap. In particular, the actual production
exporter fabricates several qualification facts instead of reading them, the
finalizer possesses the evaluator's symmetric signing secret, and the
independent evaluator accepts cross-fault and wrong-mode evidence.

No cloud/production call was made during this review. The focused authority,
control, orchestrator, and evaluator suites run locally and pass. Two direct
source-level reproductions additionally proved:

- an arm response timeout produces calls `baseline, runtime, arm` with no
  cleanup attempt; and
- the candidate evaluator returns `PASS` after changing the envelope mode to
  `final`, and also after rebinding the complete hash-valid history to a
  different fault ID than the intent.

## Strengths

- The eight upstream fault kinds have explicit typed call-class/target/parameter
  contracts, and the upstream branch does not dispatch into the legacy process,
  disk, load, restart, or deploy implementation.
- The control HTTP surface is disabled by default, uses ordinary plus
  fault-domain HMAC checks, constant-time signature comparisons, bounded bodies,
  deadline-aware SQLite operations, and durable nonce replay records.
- `FaultAuthorityStore.finalize_verdict()` performs its history re-read and
  `RECOVERED -> VERIFIED` append in one `BEGIN IMMEDIATE` transaction; exact
  fresh-nonce retry is idempotent and conflicting verdicts fail closed.
- `FaultAuthorityStore(read_only=True)` really opens SQLite with `mode=ro`, and
  the legacy auth-table migration preserves explicit IDs and passes the existing
  foreign-key/history tests.
- The Task 2 stale source-introspection test now checks
  `main -> _build_daemon_perception_workers -> feature flag -> exact runtime`,
  and the supervisor test uses separate condition-based bounded phase
  deadlines while retaining `finally` cleanup.
- The reviewed diff adds no wallet, order, signing, balance, or trade mutation.

## Issues

### CRITICAL

1. `scripts/perception_chaos.py:586`

   The cleanup protection starts only after `transport("arm", ...)` has
   returned a valid accepted fault ID. If the server commits the arm and the
   response is lost, times out, is malformed JSON, is oversized, or is a
   non-mapping response, execution exits before lines 593-667 and never requests
   cleanup. `UpstreamHttpTransport` generates the fault ID internally at lines
   374-377, so the caller cannot even recover the exact ID after an ambiguous
   arm result. This leaves a real admitted production fault active until TTL or
   process behavior happens to clear it and violates the mandatory
   `arm -> finally cleanup` contract and response-lost retry case.

   Fix: generate and durably persist the fault ID before network access; enter
   the `BaseException` cleanup envelope before POSTing arm; on an ambiguous arm
   response, query the exact ID and issue idempotent cleanup before re-raising
   the original exception.

2. `scripts/perception_chaos.py:474`

   The production CLI does not use `export_fault_envelope()` or another
   authoritative post-recovery SQLite/API export. It assembles candidate
   evidence from a cached public status response and hard-codes
   `open_injection_fault_count=0`,
   `pending_verification_fault_count=1`,
   `source_projection_active=True`,
   `partial_publication_count=0`, `freshness_gate=True`, and
   `reconciliation_gate=True` at lines 505-510. `open_incident_count` and other
   baseline fields are copied from the pre-injection window at line 491, not
   re-read after recovery. Detection at lines 390-416 merely relabels a
   `detected` fault event; it never reads or verifies the authoritative Incident
   row/kind/history.

   The nominal SQLite exporter is not wired to any production command or
   endpoint and itself accepts caller-supplied `integrity` truth at
   `scripts/perception_fault_readonly.py:38-129`. Its tests pass all-zero/true
   integrity values directly. Consequently a production candidate can claim
   zero open incidents and green integrity while the source DB says otherwise,
   and the evaluator will sign it.

   Fix: create one bounded read-only server/API export backed by a single SQLite
   read transaction that derives every projection, Incident, recovery writer,
   freshness, reconciliation, partial-publication, cross-membership, and orphan
   fact from source tables. Remove all caller-supplied/hard-coded qualification
   facts.

3. `src/polyarb/http/perception_faults.py:506`

   The finalizer verifies an HMAC using
   `settings.upstream_fault_evaluator_secret`, so the finalizer process
   necessarily possesses the evaluator's signing secret. The setting is loaded
   into the production app at `src/polyarb/config.py:183,400-411`. A compromised
   or buggy control/finalizer plane can therefore mint its own candidate PASS
   and append `VERIFIED`; the claimed third authority is not independent.
   The CLI-only test at
   `tests/m1-perception/test_perception_fault_controls.py:178-218` proves merely
   that the local submission CLI does not read the evaluator secret, not that
   the production finalizer lacks it.

   Fix: use an asymmetric evaluator signature. The evaluator alone holds the
   private signing key; the finalizer and final evaluator receive only the
   pinned public verification key.

### HIGH

1. `scripts/perception_fault_acceptance.py:62`

   The independent evaluator does not enforce the canonical candidate/final
   state machine. It ignores `evidence["mode"]`, never validates each event's
   `fault_id` against the intent, does not bind injected `call_id` evidence to
   kind/call-class/target/runtime, does not reject unexpected terminal/failure
   states in candidate mode, and does not validate runtime field formats. The
   production-fault CLI requires `--expected-release` at lines 652-653 but never
   passes or compares it in fault mode at lines 655-680.

   Direct reproduction produced `QualificationVerdict(status='PASS')` for both
   a `mode=final` candidate envelope and a fully rehashed history whose event
   fault IDs differ from the intent. These are missing named FAIL cases, not
   cosmetic validation omissions.

   Fix: define an exact canonical schema with no extra/missing fields; validate
   production identity formats and expected release; bind every event to the
   intent/runtime/call/target; require candidate tail exactly `RECOVERED` with
   pending `1`, and final tail exactly the one matching `VERIFIED`.

2. `scripts/perception_chaos.py:258`

   Missing real control secrets do not fail before network. The production
   secrets are first read inside `_signed_post_json`, but execution has already
   collected five HTTP baseline rounds, fetched runtime, created the evidence
   directory, and written intent evidence at lines 559-586. The CLI's
   `ordinary_authorization` and `fault_authorization` arguments are unrelated
   caller strings used only for non-empty/distinct checks; the transport ignores
   them and signs from environment secrets. This violates the required
   fail-before-network posture and exposes misleading “authorization” CLI
   arguments despite the no-secret-argument contract.

   In addition, the fault HMAC canonical form signs the raw body at
   `scripts/perception_chaos.py:268-276` and
   `src/polyarb/http/perception_faults.py:109-117`, while the locked design
   specifies `sha256(body)`.

   Fix: preflight the two environment-held control authorities before any GET
   or filesystem mutation, remove the dummy authorization arguments, and use
   one versioned canonical HMAC definition matching the approved contract.

3. `scripts/perception_chaos.py:642`

   When observe raises an original `BaseException` and cleanup also fails,
   lines 664-665 raise a new cleanup error and discard the original exception;
   line 666 can re-raise the original only when cleanup succeeded. This fails
   the explicit “cleanup without swallowing CancelledError/KeyboardInterrupt/
   SystemExit/original failure” requirement.

   Fix: retain both failures in a structured exception/exception group, keep the
   original exception as the primary cause, and still mark the matrix frozen
   from the cleanup failure.

4. `scripts/perception_chaos.py:191`

   `execute_supported=True` is set for three legacy producer primitives
   (`candidate-exit`, `discovery-exit`, `reconciliation-stall`) in addition to
   the eight upstream kinds enabled at lines 222-226. Thus the public contract
   exposes eleven executable kinds, not “only eight”. The upstream dispatcher
   itself is separated correctly, but the required capability flag invariant is
   false.

   Fix: separate legacy primitive executability from the Task 7
   `execute_supported` capability or rename/split the flags so exactly the eight
   typed upstream kinds advertise this contract.

### MEDIUM

1. `scripts/perception_fault_acceptance.py:620`

   Evidence and candidate inputs use `Path.read_text()`, and all exclusive
   writers use `Path.open("x")` without `O_NOFOLLOW`, regular-file checks,
   parent-directory ownership checks, or inode stability checks. A symlink or
   replacement race can redirect/read a different artifact between phases.
   Partial writes fail closed, but symlink/replacement safety required by the
   Task 7 race matrix is not implemented.

   Fix: open inputs/outputs with descriptor-based no-follow semantics, verify
   regular files and stable `(dev, ino, size)`, fsync the parent directory after
   atomic publication, and bind the exact candidate bytes transferred to the
   finalizer.

2. `scripts/perception_chaos.py:417`

   Cleanup proof reports the CLEANED event timestamp as both
   `memory_cleared_at_ms` and `receipt_persisted_at_ms`. Durable evidence
   contains only `cleanup_id`; the public response has no explicit
   memory-clear timestamp/boolean. Process ownership makes the event meaningful,
   but the transport is manufacturing two exact timing fields that were never
   observed.

   Fix: expose the process-owned cleanup result or explicit clear-before-write
   receipt fields from the authority, and validate those actual fields rather
   than aliasing the event occurrence time.

### LOW

None.

## Assessment

**Needs fixes.**

The 2750-test green report and the focused passing suites are not sufficient
for this production mutation/audit boundary. The ambiguous-arm cleanup hole,
fabricated post-state evidence, symmetric-secret authority collapse, and
evaluator false-PASS cases are release blockers. Do not run the upstream fault
matrix or finalize a production candidate until these issues are remediated and
covered by real response-loss, source-tamper, cross-fault, exact-release,
post-recovery SQLite, and three-process authority tests.
