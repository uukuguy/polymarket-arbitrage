# M1 Dashboard Read-Model Remediation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `subagent-driven-development` task by task and `test-driven-development`
> within every task. Do not start Task 8 deployment until the final UI gate
> passes.

**Goal:** Close all three Important findings from the Task 7 UI audit so the
Dashboard is a truthful, bounded, production-usable M1 opportunity watcher.

**Architecture:** Extend the existing authenticated SQLite authorities instead
of reading unverified latest rows. Current opportunity and state summaries are
derived from the candidate-current authority; Discovery and Reconciliation
reuse already validated store projections. Group history becomes one bounded
four-class timeline with an explicit compaction floor. Resource and incident
history gain checkpoint-plus-bounded-suffix validation so their public read
cost does not grow without bound.

**Tech Stack:** Python 3.12, SQLite/WAL, Starlette, pytest, Next.js 15,
React 19, TypeScript, Make.

## Non-negotiable constraints

- Observer-only: no wallet, signing, balance, order, or real-money mutation.
- No production deployment in this plan; deployment remains rollout Task 8.
- Every public reader retains the existing 0.8 s SQL/Python deadline, 1 s HTTP
  timeout, 250 ms busy timeout, pagination bounds, 1 MiB response cap,
  secret redaction, and fail-closed evidence validation.
- `0 opportunities` is valid only after authenticated authority validation;
  read failure remains `503 unavailable`.
- Compacted history is never presented as complete. Responses expose an
  explicit floor and `history_complete`.
- Preserve the user-owned `findings.md`, `progress.md`, and `task_plan.md`.
- Each task ends with focused tests, proportional regression, one atomic code
  commit, an updated task ledger, and an independent review.

---

## Task 1: Expose Existing Discovery and Reconciliation Truth

**Files**

- Modify: `src/polyarb/http/perception.py`
- Modify: `dashboard/lib/perception.ts`
- Modify: `dashboard/lib/types.ts`
- Modify: `dashboard/app/perception/page.tsx`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Modify: `tests/m1-perception/test_perception_http.py`
- Modify: `tests/m1-perception/test_dashboard_perception_contract.py`
- Create: `tests/m1-perception/perception_contract_cases.mjs`

**Public contract**

- `GET /perception/discovery` additionally returns:
  `coverage.by_minutes.{15,30,60}.{visited_groups,raw_fraction,liquidity_weighted_fraction}`,
  `coverage.known_groups`, `coverage.total_liquidity_weight`, `load_state`,
  `admission_proof`, `candidate_attempt_start_count`, and
  `candidate_start_deadline_breach_count`.
- `GET /perception/reconciliation` additionally returns duration and all
  authenticated diff counts:
  `duration_ms`, `observations_count`, `baseline_count`, `added_count`,
  `changed_count`, `closed_count`, `unchanged_count`, and
  `applied_rejected_count`.
- Contract revision: the endpoint shows the current fully validated window
  duration only. Historical duration distribution is explicitly labelled
  **not tracked** because validating old full-market windows is not bounded by
  group cardinality. No distribution claim is made in Task 7.

- [x] Add HTTP tests that expect the fields from validated store objects and
      reject a tampered projection/window.
- [x] Run the focused tests and capture RED because the mapping omits fields:

  ```bash
  uv run pytest tests/m1-perception/test_perception_http.py \
    -k 'discovery or reconciliation' -q
  ```

- [x] Map every existing validated field in `perception.py`, including
      `next_cursor`, queue depths, and oldest visit age. Compute
      `duration_ms` as `max(0, (finished_at_ms or checkpoint_at_ms) -
      started_at_ms)` only after the window has passed store validation.
- [x] Extend strict TypeScript validators; do not accept missing, negative,
      non-finite, or out-of-range fractions.
- [x] Replace the corresponding `not exposed` placeholders with actual 15/30/60
      coverage, queue/load/admission, duration, and diff values.
- [x] Run:

  ```bash
  uv run pytest tests/m1-perception/test_perception_http.py \
    tests/m1-perception/test_dashboard_perception_contract.py -q
  make dashboard-typecheck
  ```

- [x] Commit:

  ```bash
  git commit -m "feat(m1): expose perception progress evidence"
  ```

---

## Task 2: Authenticated Current Opportunities and Candidate State Counts

**Files**

- Modify: `src/polyarb/perception/store.py`
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/http/perception.py`
- Modify: `src/polyarb/http/app.py`
- Modify: `dashboard/lib/perception.ts`
- Modify: `dashboard/lib/types.ts`
- Modify: `dashboard/app/perception/page.tsx`
- Modify: `tests/perception/test_store.py`
- Modify: `tests/m1-perception/test_perception_http.py`
- Modify: `tests/m1-perception/test_dashboard_perception_contract.py`

**Store interface**

```python
@dataclass(frozen=True)
class CurrentOpportunity:
    group_id: str
    event_id: str
    group_revision: int
    membership_hash: str
    quote_batch_id: str
    fact_id: int
    bundle_cost: Decimal
    gross_edge_bps: Decimal
    max_bundle_size: Decimal
    structure_observed_at_ms: int
    quote_started_at_ms: int
    quote_quoted_at_ms: int

@dataclass(frozen=True)
class CandidateCurrentSummary:
    current_group_count: int
    opportunity_count: int
    state_counts: dict[str, int]
    authority_hash: str

def current_opportunities(
    self, *, after_group_id: str, limit: int,
    _connection: sqlite3.Connection | None = None,
) -> tuple[tuple[CurrentOpportunity, ...], str | None]: ...

def candidate_current_summary(
    self, *, _connection: sqlite3.Connection | None = None,
) -> CandidateCurrentSummary: ...
```

Both methods validate the bounded owner journal/root and aggregate digest;
the full Candidate checkpoint replay remains an explicit audit path rather
than part of the real-time O(1) summary. Opportunity rows must bind the exact authority `fact_id`,
`quote_batch_id`, and current group revision; identity mismatch fails closed.
State counts cover `watching`, `no-edge`, and `unavailable`; add these counters
to `neg_risk_candidate_current_aggregate` and update them atomically inside
`_sync_candidate_current_authority`, under the existing owner-write journal and
aggregate digest. Structure `stale`/`invalidated` counts remain explicitly
labelled as **returned bounded-page counts**, not global totals. This task does
not scan the growing revision universe to claim global structure counts.
This additive column change performs owner-authority **v2 → v3** migration:
upgrade columns and aggregate seed first, then atomically install the exact v3
manifest. Copied-v2, interrupted-upgrade/restart, idempotency, and forged
partial-manifest cases are tested.

**HTTP contract**

- `GET /perception/opportunities?after_group_id=&limit=100`
- Response: `{status, server_time_ms, candidate_authority_hash,
  current_opportunity_count, items, limit, next_after_group_id}`.
- Extend `/perception/status` with `server_time_ms`, candidate state counts,
  current candidate group count, and the same candidate authority hash. Do not
  add global structure counts.

- [x] Write store tests for exact fact/quote/revision binding, deterministic
      keyset pagination, aggregate/count agreement, tampering, valid zero,
      additive migration of an existing DB, and exact owner-manifest bootstrap
      after the new aggregate columns are installed.
- [x] Write HTTP tests for bounds, cursor validation, timeout/503, response cap,
      and server-side ages derivable from `server_time_ms`.
- [x] Run focused tests and capture RED.
- [x] Implement fixed-query-count and fixed-page-size authenticated store
      readers. Aggregate validation is O(1), and the opportunity query examines
      at most `limit + 1` authority rows. Never use the legacy Polywatch
      JSON/outbox as opportunity authority.
- [x] Add the route and strict Dashboard envelope validators.
- [x] Render the current opportunity table/cards with edge bps, bundle cost,
      max executable bundle size, Structure age, Quote age, and link to the
      group timeline. Render real global watching/no-edge/unavailable counts
      and retain honest bounded-page labels for structure states.
- [x] Run:

  ```bash
  uv run pytest tests/perception/test_store.py \
    tests/m1-perception/test_perception_http.py \
    tests/m1-perception/test_dashboard_perception_contract.py -q
  make dashboard-typecheck
  ```

- [x] Commit:

  ```bash
  git commit -m "feat(m1): publish authenticated current opportunities"
  ```

---

## Task 3: Bounded Incident Lifecycle and Operator Actions

**Files**

- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/perception/store.py`
- Modify: `src/polyarb/perception/incidents.py`
- Modify: `src/polyarb/perception/supervisor.py`
- Modify: `src/polyarb/http/perception.py`
- Modify: `src/polyarb/http/app.py`
- Modify: `dashboard/lib/perception.ts`
- Modify: `dashboard/lib/types.ts`
- Modify: `dashboard/app/perception/page.tsx`
- Modify: `src/polyarb/http/health.py`
- Modify: `tests/perception/test_incidents.py`
- Modify: `tests/perception/test_store.py`
- Modify: `tests/perception/test_supervisor.py`
- Modify: `tests/m1-perception/test_perception_http.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_dashboard_perception_contract.py`

**Authority and compaction contract**

- Task 3 performs owner-authority **v3 → v5** migration for both
  incident and resource checkpoint families. It creates the incident tables,
  the Task 4 resource checkpoint table, `neg_risk_incident_scope_floors`, and a
  shared `neg_risk_evidence_failures` breadcrumb authority, then installs the
  exact v5 trigger/column manifest. The previously committed v4 manifest is
  frozen and accepted only by an explicit atomic v4 → v5 migration; partial or
  forged v4 manifests fail closed. Task 4 uses the already-migrated v5 schema and
  does not bump the manifest again. Tests cover copied-v2 upgrade, crash between
  DDL/bootstrap phases, restart/idempotency, and rejection of a forged or
  partially upgraded manifest. The migration fixture runs the real Task 2
  v2 → v3 upgrade followed by the production v3 → v5 path; there is no
  test-only direct migration.
- Add `neg_risk_incident_authority_checkpoint` for the event prefix and
  `neg_risk_incident_open_authority` plus a one-row aggregate for current open
  incidents. The checkpoint contains no ever-growing latest-state JSON map.
  Current open incident anchors and recovery anchors live in normalized,
  keyset-pageable authority rows; a verified transition deletes its open row
  and updates the aggregate atomically.
- Add authenticated `neg_risk_incident_scope_floors` rows:
  `{scope,through_event_id,compacted_event_count,floor_hash,row_hash}`. Add a
  mandatory one-row suffix authority that chains every retained event from the
  compacted prefix. Compaction
  updates only scopes represented in the at-most-256-row deleted chunk. Exact
  indexed scope lookup plus the checkpoint's authenticated floor-set count
  proves per-group incident completeness. Scope floors are hard-capped at
  8,192 rows; exceeding the cap rolls back and records an unresolved breadcrumb.
- `IncidentManager.detect()` and `.transition()` own compaction. After the new
  event/open-authority mutation in the same `BEGIN IMMEDIATE`, they validate the
  prior checkpoint plus suffix. At suffix high-water 512 they publish a new
  checkpoint and retain at most 256 event rows total. Open/latest/recovery
  anchors live only in normalized open authority and never force event-suffix
  retention. A suffix above hard limit 512 rolls back the attempted write.
- Add `neg_risk_incident_replay_anchors`, one authenticated predecessor per
  incident whose earlier event was compacted but which still has an event in
  the retained suffix. The anchor contains incident ID, sequence, scope, kind,
  state, timestamp, and recovery predecessor/proof state. Compaction rebuilds
  this table from the retained at-most-256 event rows, so it has at most 256
  rows. A verified transition deletes the open-authority row but keeps its
  replay anchor until no event for that incident remains in the suffix; thus a
  terminal suffix remains fully replayable across the floor.
- After a hard-limit rollback, the caller records an unresolved
  `neg_risk_evidence_failures(component='incident', failed_at_ms, reason)` in a
  separate `BEGIN IMMEDIATE`; `/health` reads this exact breadcrumb. The next
  successful writer-side checkpoint/validation marks it recovered in a
  separate commit.
  If even that breadcrumb cannot commit, the existing producer failure receipt
  and supervisor incident remain the outer evidence.
- `neg_risk_evidence_failures` is keyed current authority, not history:
  `component PRIMARY KEY CHECK(component IN ('incident','resource'))`,
  `failed_at_ms`, `reason`, nullable `recovered_at_ms`, and `row_hash`.
  Failure/recovery uses owner-journaled UPSERT, so the table holds at most two
  rows and validation/read cost is O(2). The public incident/resource endpoint
  and matching health subcheck both fail closed on an unresolved component row.
  `reason` is a canonical enum of at most 64 characters (never raw exception
  text), and validation requires `recovered_at_ms >= failed_at_ms` when set.
- Open authority is hard-capped at 4,096 rows. Current reads reconcile the
  authenticated aggregate count against the exact bounded leaf count before
  examining at most `limit + 1` returned row hashes. Event-history reads
  reconcile at most 8,192 floor rows and validate at most 512 suffix rows.
  These are bounded O(cap) scans, not unbounded O(N) or claimed O(1) work.
- Enrich lifecycle evidence with canonical `action`, `retry_count`,
  `next_retry_at_ms`, and `recovery_start_evidence`; missing legacy values are
  explicit `null`. The latter is the authenticated `recovering` transition,
  not terminal verification proof.
- `GET /perception/incidents?before=<opaque>&limit=100` exposes current/open
  state, lifecycle age, canonical actions, recovery-start evidence, and history
  floor. Verified terminal incidents are omitted from this open-only endpoint.
- Add a store-level group-scoped history reader used later by Task 5. It binds
  group identity only through exact `scope == "candidate:<group_id>"`, uses
  `(scope,id)` indexing and keyset pagination, and never filters a global page.

- [x] Write RED tests for checkpoint/replay/tamper/crash/repeated compaction,
      transition and recovery invariants, 2,000 appended terminal incidents,
      2,000 distinct scopes, many simultaneously open incidents,
      open-authority aggregate integrity, per-scope floors, pagination, group
      scope, redaction, concurrent high-water writers, rollback/retry, and
      Supervisor action/retry fields. Cover recovering → compaction floor →
      verified → open-row deletion while replay remains valid. Assert replay
      anchors stay at or below 256, the event suffix never exceeds 512 after a
      successful write, and each public validation examines no more than 512
      suffix rows. Repeat 2,000 failure/recovery cycles per component and assert
      the breadcrumb table remains exactly two rows with O(2) health reads.
- [x] Add schema, owner-authority manifest/trigger coverage, deterministic
      hashes, writer-triggered high/low-water compaction, and bounded readers.
- [x] Add the endpoint, strict Dashboard types, incident badges, lifecycle age,
      automated action, retry/next retry, and recovery-start evidence.
- [x] Add `/health` `incident_evidence` chain truth: it reads the same
      checkpoint/suffix validator; there is no new config gate; successful
      writes advance event/open authority atomically, while failed validation
      rolls back. It also fails on the separately committed unresolved
      breadcrumb. HTTP tests cover committed suffix corruption and a hard-limit
      rollback/breadcrumb, proving health/public reads fail closed.
- [x] Record an explicit contract revision in the manual/UI: notification
      delivery is **not tracked** and the Dashboard makes no delivery claim.
      Durable notification outbox work is a separate production slice.
- [x] Run:

  ```bash
  uv run pytest tests/perception/test_incidents.py \
    tests/perception/test_store.py \
    tests/perception/test_supervisor.py \
    tests/m1-perception/test_perception_http.py \
    tests/m1-perception/test_health_endpoint.py \
    tests/m1-perception/test_dashboard_perception_contract.py -q
  make dashboard-typecheck
  ```

- [x] Commit:

  ```bash
  git commit -m "feat(m1): bound incident recovery evidence"
  ```

---

## Task 4: Bounded Resource Decision History

**Files**

- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/perception/resource_controller.py`
- Modify: `src/polyarb/http/perception.py`
- Modify: `src/polyarb/http/app.py`
- Modify: `dashboard/lib/perception.ts`
- Modify: `dashboard/lib/types.ts`
- Modify: `dashboard/app/perception/page.tsx`
- Modify: `src/polyarb/http/health.py`
- Modify: `tests/perception/test_resource_controller.py`
- Modify: `tests/m1-perception/test_perception_http.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_dashboard_perception_contract.py`

**Authority design**

- Add a single resource checkpoint containing through sequence/row IDs,
  compacted counts, prefix digest, last decision JSON/digest, and checkpoint
  hash.
- Compact only after the full pre-compaction replay validates. Retain the
  checkpoint anchor plus a bounded suffix; current validation starts from the
  authenticated anchor and replays the suffix with the existing deterministic
  policy.
- `ResourceController.decide()` is the compaction owner. In the same
  `BEGIN IMMEDIATE` as the sample/decision pair, it validates the prior
  checkpoint and suffix. At suffix high-water 512 it publishes the checkpoint
  and retains 256 pairs. A suffix above hard limit 1,024 rolls back; the caller
  then commits `neg_risk_evidence_failures(component='resource', ...)` in a
  separate transaction. The next successful checkpoint marks it recovered.
- `GET /perception/resources?before_sequence=&limit=100` returns current
  decision plus bounded recent decision/sample pairs and history floor.

- [ ] Write RED tests for checkpoint creation, replay equivalence, tampered
      anchor/hash/suffix, crash before/after checkpoint publication, repeated
      compaction, 2,000 appended decisions, concurrent high-water writers,
      rollback/retry, v4 restart, and bounded validation cost (at most 1,024
      suffix pairs per public read).
- [ ] Add schema/hash helpers, owner-authority manifest/trigger coverage, and
      writer-triggered compact/replay without changing policy behavior.
- [ ] Add the bounded HTTP endpoint and strict Dashboard reader.
- [ ] Render current mode, reason, policy age/TTL, hot-path freshness inputs,
      and recent mode transitions.
- [ ] Add `/health` `resource_evidence` chain truth: it calls the same bounded
      validator, has no new config gate, success advances checkpoint/suffix
      atomically, and validation failure rolls back. It also reads the separate
      unresolved breadcrumb. Tests cover both suffix corruption and
      hard-limit rollback/breadcrumb.
- [ ] Make `/perception/resources` read the same keyed current breadcrumb and
      return unavailable while the resource row is unresolved.
- [ ] Run:

  ```bash
  uv run pytest tests/perception/test_resource_controller.py \
    tests/m1-perception/test_perception_http.py \
    tests/m1-perception/test_health_endpoint.py \
    tests/m1-perception/test_dashboard_perception_contract.py -q
  make dashboard-typecheck
  ```

- [ ] Commit:

  ```bash
  git commit -m "feat(m1): bound resource decision evidence"
  ```

---

## Task 5: Four-Class Bounded Group Timeline

**Files**

- Modify: `src/polyarb/perception/store.py`
- Modify: `src/polyarb/http/perception.py`
- Modify: `src/polyarb/http/app.py`
- Modify: `dashboard/lib/perception.ts`
- Modify: `dashboard/lib/types.ts`
- Modify: `dashboard/app/perception/[group_id]/page.tsx`
- Modify: `tests/perception/test_store.py`
- Modify: `tests/m1-perception/test_perception_http.py`
- Modify: `tests/m1-perception/test_dashboard_perception_contract.py`

**HTTP contract**

- `GET /perception/groups/{group_id}/timeline?before=<opaque>&limit=100`
- Items are `membership_revision`, `quote_batch`,
  `opportunity_transition`, or `incident_event`, ordered by
  `(occurred_at_ms DESC, class_order, stable_id DESC)`.
- Each source reads at most `limit + 1`. Candidate history validates the
  checkpoint `through_group_revision_id`, `through_quote_rowid`,
  `through_fact_id`,
  compacted counts, and per-group seed anchor. Incident events come from Task
  3's exact group-scoped reader.
- `opportunity_transition` means a candidate fact whose normalized state
  `(last_result, opportunity)` differs from its authenticated predecessor. The
  first retained suffix fact compares against the candidate checkpoint seed,
  so a transition crossing the compaction floor is represented correctly.
- `history_floor` reports membership/quote/fact/incident floors separately.
  Candidate membership/quote/opportunity floors are deliberately
  **global/conservative**, because the existing candidate checkpoint has only
  global compacted counts. If a class has any compacted global prefix, that
  class is `history_complete=false` for every group; the API never invents a
  per-group deleted count. Incident floor remains exact-scope.
  `history_complete` does not mean the current page is exhausted;
  `next_before` alone expresses pagination continuation.

```json
{
  "history_floor": {
    "membership": {"scope": "global", "through_id": 0, "compacted_count": 0},
    "quote": {"scope": "global", "through_id": 0, "compacted_count": 0},
    "opportunity": {
      "scope": "global",
      "through_id": 0,
      "source_rows_compacted": 0
    },
    "incident": {
      "scope": "candidate:<group_id>",
      "through_id": 0,
      "compacted_count": 0
    }
  },
  "history_complete": {
    "membership": true,
    "quote": true,
    "opportunity": true,
    "incident": true
  }
}
```

All IDs and counts are non-negative integers. Candidate counts are the
authenticated global checkpoint counts. `opportunity.source_rows_compacted`
means compacted candidate fact source rows, not a count of rendered
transitions. Incident floor values come from the exact authenticated
scope-floor row; its absence means zero compacted rows for that exact scope,
not a global inference.

- [ ] Add RED tests for four interleaved classes, equal-timestamp cursor ties,
      each source `limit + 1`, wrong identity, transition across the candidate
      floor, retained older rows, no-compaction complete history, another group
      causing conservative candidate incompleteness, oversized merged response,
      SQL deadline, and exact group-scoped incident completeness.
- [ ] Implement canonical versioned base64url cursors, bounded per-source reads,
      exact identity validation, merge, response cap, and explicit floors.
- [ ] Extend strict TypeScript types and render all four classes, recovery
      evidence, floor copy, 14 px metadata, semantic colors, and safe wrapping.
- [ ] Run:

  ```bash
  uv run pytest tests/perception/test_store.py \
    tests/m1-perception/test_perception_http.py \
    tests/m1-perception/test_dashboard_perception_contract.py -q
  make dashboard-typecheck
  ```

- [ ] Commit:

  ```bash
  git commit -m "feat(m1): add bounded group operations timeline"
  ```

---

## Task 6: Dashboard Acceptance, Documentation, and Task 7 Closure

**Files**

- Modify: `Makefile`
- Modify: `scripts/check_m1_manual.py`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Create: `docs/learning/35-opportunity-operations-read-models.md`
- Modify: `docs/learning/00-INDEX.md`
- Create: `docs/superpowers/plans/2026-07-28-m1-opportunity-first-rollout-TASK-7-SUMMARY.md`
- Modify: `.superpowers/sdd/progress.md`
- Modify: `.planning/JOURNAL.md`

- [ ] Add/extend source-contract tests for zero vs unavailable, all required
      fields, four timeline classes, empty groups, long IDs, and semantic style
      tokens. Include canonical cursor rejection, cross-class timestamp ties,
      four-source response cap, SQL deadline, and `limit + 1` scan bounds.
- [ ] Run complete verification:

  ```bash
  uv run pytest tests/perception tests/m1-perception -q
  uv run pytest -q
  make dashboard-typecheck
  make dashboard-build
  make smoke-perception-dashboard
  make docs-m1-check
  make planning-status
  git diff --check
  ```

- [ ] Run the Dashboard against a deterministic fixture and capture desktop
      plus 375 px mobile screenshots for overview and group pages, including an
      unavailable state and long group ID. Visually inspect every capture.
- [ ] Repeat the formal six-pillar `gsd-ui-review`. The Task 7 gate requires:
      no Critical/Important findings, all approved operational fields backed by
      real authenticated data, all four timeline classes, current validated
      reconciliation duration plus explicit “historical duration distribution
      not tracked” and “notification delivery not tracked” copy, and
      mobile-safe layout.
- [ ] Produce the learning document with the 30-second mental model, concrete
      `file:line` pointers, design trade-offs, self-check questions, and FAQ
      increment.
- [ ] Write the Task 7 summary, update the SDD ledger and JOURNAL, run
      `make planning-status`, then commit:

  ```bash
  git commit -m "docs(m1): close opportunity dashboard task"
  ```

- [ ] Only after this gate passes may rollout Task 8 production qualification
      start.
