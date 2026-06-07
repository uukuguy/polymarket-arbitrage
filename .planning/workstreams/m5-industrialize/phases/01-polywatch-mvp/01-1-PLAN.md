---
phase: 01
plan: 01
type: execute
wave: 1
workstream: m5-industrialize
depends_on: []
files_modified:
  - scripts/polywatch/healthz_watcher.py
  - scripts/polywatch/chaos_replay.py
  - scripts/polywatch/memory_sanity.sh
  - scripts/polywatch/autoresearch_tune.py
  - .planning/polywatch/trials/healthz-watcher.jsonl
  - .planning/polywatch/trials/chaos-inj-replay.jsonl
  - .planning/polywatch/trials/memory-sanity-check.jsonl
  - .planning/polywatch/trials/autoresearch-validation-tuning.jsonl
  - .github/workflows/polywatch-chaos-replay.yml
  - Makefile
  - ~/.claude/skills/polywatch/SKILL.md
  - ~/.claude/skills/polywatch/trial_runner.py
  - ~/.claude/skills/polywatch/escalation.py
  - ~/.claude/skills/polywatch/templates/template_cron.py
  - ~/.claude/skills/polywatch/templates/template_ralph.py
  - ~/.claude/skills/polywatch/templates/template_autoresearch.py
autonomous: false
requirements: []
makefile_targets:
  - polywatch-chaos-replay
  - polywatch-chaos-replay-dry
  - polywatch-memory-sanity
  - polywatch-autoresearch-tune
  - polywatch-autoresearch-tune-dry
  - polywatch-status
user_setup:
  - "Phase 03.1 Plan 07 must complete (GAP-103 fail-reason in snapshots.notes enables Trial 1 richer push)"
  - "Fly machine cron setup for chaos-inj-replay (one-time: `flyctl machines update` on polyarb-l1)"
  - "GH_TOKEN with repo scope for L3 auto-issue creation (Trial 1-4 escalation)"
  - "Sentry auth token with project:read scope for breadcrumb auto-fetch (Trial 1 enhancement)"

must_haves:
  truths:
    - "Trial 1 healthz-watcher: jsonl ledger writes every iteration, verdict=pass when both L1/L2 healthy, verdict=fail with reason when unhealthy"
    - "Trial 1 healthz-watcher: /healthz JSON `snapshot:last_status.observedValue` is consumed by decide_l1/l2 (already working), Phase 03.1 notes field consumed if present"
    - "Trial 2 chaos-inj-replay: nightly UTC 18:00 runs L2-1/L2-2/L2-3a (existing PASS Inj), verdict=pass when all pass, verdict=fail when any fail"
    - "Trial 2 chaos-inj-replay: --dry-run flag prevents Supabase/Sentry writes during replay (verifiable: check Sentry has no new issues during run)"
    - "Trial 3 memory-sanity-check: ralph-loop iterates ≤10 times until all VERIFIED file:line references confirmed or generates propose patch doc"
    - "Trial 3 memory-sanity-check: output file is .planning/polywatch/memory-sanity-{date}.md with listed failures, no auto-commit"
    - "Trial 4 autoresearch-validation-tuning: grid search over 10 tolerance values completes, signal:noise ratio computed per value, results appended to jsonl"
    - "Polywatch global skill exists at ~/.claude/skills/polywatch/ with SKILL.md + trial_runner.py + escalation.py + 3 templates"
    - "Escalation L0: all 4 trials write to .planning/polywatch/trials/{trial}.jsonl (verdict + metrics)"
    - "Escalation L1: streak=3 consecutive fail → Sentry breadcrumb (verify via playwright-cli Sentry UI query post-PR)"
    - "Escalation L2: red-line triggers (max iter exhausted, side-effect boundary breached) → Telegram push (verify via existing Telegram channel)"
    - "Escalation L3: infra broken (cron missed ≥2 cycles, harness startup crash) → `gh issue create` auto-filed"
  artifacts:
    - path: scripts/polywatch/healthz_watcher.py
      provides: enhanced healthz-watcher with notes field consumption + Sentry breadcrumb fetch + jsonl ledger writes
    - path: scripts/polywatch/chaos_replay.py
      provides: nightly chaos-inj-replay runner with dry-run flag
    - path: scripts/polywatch/memory_sanity.sh
      provides: ralph-loop memory-sanity checker
    - path: scripts/polywatch/autoresearch_tune.py
      provides: L4 tolerance grid-search over historical snapshots
    - path: .planning/polywatch/trials/
      provides: append-only jsonl ledger directory for all 4 trials
    - path: ~/.claude/skills/polywatch/
      provides: global polywatch skill (thin-shell — SKILL.md + runner + escalation + templates)
  key_links:
    - "Phase 03.1 LEARNINGS.md → GAP-103 notes field schema for Trial 1 integration"
    - "tests/chaos/test_l2_chaos_plan.py → L2_CHAOS_PLAN dataclass for Trial 2 Inj catalog"
    - "memory: polywatch-decision-framework → 4 conditions + 8 application points + 3 red lines"
---

<objective>
**Deliver Polywatch MVP: 4 trials running + global skill extracted**

This is a single plan covering the entire phase scope because the 4 trials are
independent (no data-flow dependency) and small enough to handle in one wave.
The global skill extraction (D-3) runs after all trials are in place to
ensure templates are grounded in real code, not speculation.

Each trial is scoped narrowly — MVP means "run and produce a jsonl ledger
entry", not "comprehensive error handling" or "beautiful dashboard".

Four work items, executed sequentially (each is small — 30-150 lines):
1. Trial 1: Formalize healthz-watcher (enhance existing code)
2. Trial 2: chaos-inj-replay script
3. Trial 3: memory-sanity-check ralph-loop
4. Trial 4: autoresearch-validation-tuning grid search
5. Phase close: global skill extraction + escalation wiring + verification
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/workstreams/m5-industrialize/phases/01-polywatch-mvp/01-CONTEXT.md
@.planning/threads/polywatch-architecture.md
@scripts/polywatch/healthz_watcher.py
@.github/workflows/polywatch-healthz.yml
@tests/chaos/test_l2_chaos_plan.py
@src/polyarb/http/health.py
@src/polyarb/http/l2_health.py
@src/polyarb/snapshot/orchestrator.py  (GAP-103 notes field, lines 130-153)
@src/polyarb/config.py  (l2_mirror_enabled field, lines 114-119, 236-240)
</context>

<tasks>

<task type="checkpoint:decision" gate="blocking">
  <name>Task 0: Pre-flight — verify Phase 03.1 readiness + dev env</name>
  <decision>Phase 03.1 Plan 07 chaos runs must be complete before Trial 2 can replay them. If not done, this plan blocks on 03.1.</decision>
  <context>
    Phase 03.1 Plan 07 is the final wave of chaos injections (L2-2 re-run, L2-3b, L2-4).
    Trial 2 chaos-inj-replay re-runs these on schedule. If 03.1 isn't complete:
    - chaos primitives might not be in place
    - dry-run safety not validated yet

    Check: does `flyctl machines list -a polyarb-l1` show a running machine with `POLYARB_EVENT_BUS_ENABLED` configurable? (needed for L2-3b)
  </context>
  <checklist>
    <item>Phase 03.1 LEARNINGS.md exists with L2-1/L2-2/L2-3a all PASS</item>
    <item>snapshots.notes column populated for recent snapshots (verify: `uv run python -c "from polyarb.storage.sqlite_store import SqliteStore; s = SqliteStore(); r = s.get_latest_snapshot(); print(r.get('notes', 'NO NOTES'))"`)</item>
    <item>l2_mirror_enabled config field exists in config.py (verified: line 119)</item>
    <item>GH_TOKEN available in local env for `gh issue create` testing</item>
    <item>Sentry auth token available (SENTRY_AUTH_TOKEN env or equivalent)</item>
  </checklist>
  <output>Decision: proceed or block on Phase 03.1 completion.</output>
</task>

<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<!-- Trial 1: healthz-watcher formalization                                     -->
<!-- ═══════════════════════════════════════════════════════════════════════════ -->

<task type="implement" gate="blocking">
  <name>Task 1: Trial 1 — Enhance healthz-watcher (formalize into phase governance)</name>
  <context>
    Current healthz_watcher.py (253 lines, stdlib-only, GHA cron */15):
    - Polls L1/L2 /healthz, decides escalate action
    - Sends Telegram on L1 snapshot stale / L2 fail / L2 WS silent
    - Already has HMAC-signed POST /control/unpause
    - Already has DRY_RUN env toggle

    Enhancements (MVP-appropriate, not over-engineering):
    a) Write jsonl ledger entry to .planning/polywatch/trials/healthz-watcher.jsonl (D-1)
       Fields: {trial_name, iteration, timestamp, verdict (pass|fail), metrics: {l1_action, l1_reason, l2_action, l2_reason}, notes, ref_commit}
    b) Consume snapshots.notes field if present in /healthz checks (03.1 GAP-103 integration)
       Currently /healthz exposes snapshot:last_status.observedValue (OK/DEGRADED/FAILED)
       but NOT the raw notes. The health.py _build_health_checks only reads notes to
       determine status, doesn't add a notes field to the check output.
       → Minimal fix: add notes (truncated to 160 chars) to the snapshot:last_status
         check body as an optional `notes` field. This is a 2-line change in health.py.
    c) Sentry breadcrumb auto-fetch on L1 fail (MVP deferred, now ship):
       When L1 snapshot sub-check is fail, fetch recent breadcrumbs from Sentry API
       for the current issue (121111789) and include in Telegram push.
       Use playwright-cli edge profile pattern (headless fetch, not dashboard click).
       → FALLBACK: if Sentry API token not available, skip breadcrumb fetch (degrade
         gracefully — don't break the watcher). Log "[polywatch] Sentry breadcrumb
         fetch skipped — no SENTRY_AUTH_TOKEN".

    Non-changes (keep MVP simple):
    - Don't change GHA workflow (already working, D-2 locked)
    - Don't change threshold constants (POLYWATCH_L1_SNAPSHOT_FAIL_AGE_S etc — already env-configurable)
    - Don't add retry loops (cron re-fires in 15 min)
  </context>
  <subtasks>
    <subtask name="1a">Add jsonl ledger write to healthz_watcher.py</subtask>
    <subtask name="1b">Add notes field to snapshot:last_status in health.py (optional, truncated to 160 chars)</subtask>
    <subtask name="1c">Consume notes field in healthz_watcher.py decide_l1 (append to Telegram message if present)</subtask>
    <subtask name="1d">Sentry breadcrumb auto-fetch on L1 fail (best-effort, SENTRY_AUTH_TOKEN gate)</subtask>
  </subtasks>
  <tests>
    <test>make polywatch-healthz-dry → prints jsonl ledger path + contents (no Telegram, no unpause)</test>
    <test>jsonl ledger file exists after run, valid JSON lines, all required fields present</test>
    <test>health.py unit test: snapshot:last_status check includes notes field when notes present in DB</test>
    <test>Sentry fetch graceful degradation: without SENTRY_AUTH_TOKEN, prints skip message (no crash, exit 0)</test>
  </tests>
</task>

<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<!-- Trial 2: chaos-inj-replay                                                  -->
<!-- ═══════════════════════════════════════════════════════════════════════════ -->

<task type="implement" gate="blocking">
  <name>Task 2: Trial 2 — chaos-inj-replay script + Fly cron + GHA notification</name>
  <context>
    Replay existing Phase 03 chaos injections nightly. NOT creating new chaos — just
    running what's already built.

    Starting Inj set: L2-1 (WS kill), L2-2 (Supabase key revoke), L2-3a (DNS poison).
    These 3 are in L2_CHAOS_PLAN dataclass (tests/chaos/test_l2_chaos_plan.py:45-60+).
    Phase 03.1 adds L2-3b/L2-4 — include them once available (check in execute step).

    New script: scripts/polywatch/chaos_replay.py
    - Reads L2_CHAOS_PLAN from tests/chaos/test_l2_chaos_plan.py
    - For each Inj in subset: execute action_cmds via subprocess, wait for effect,
      execute programmatic_cmds as verification, execute cleanup_cmds
    - Each Inj gets its own verdict (pass if all programmatic_cmds return expected)
    - Overall verdict: pass if all Inj pass
    - Write jsonl ledger entry

    Dry-run flag: --dry-run
    - Prints commands that WOULD run, does NOT execute them
    - Essential safety gate: first run always --dry-run to verify command set
    - This IS the dry-run implementation required by CONTEXT (no existing --dry-run in chaos toolkit)

    Cron: Fly machine cron on polyarb-l1 (D-2 decision)
    - polyarb-l1 runs 24/7, is closest to L2 endpoints
    - UTC 18:00 nightly (Asia 02:00, low traffic)
    - Fly cron via `flyctl machines update` with schedule config
    - Fallback: if Fly cron setup fails, use GHA nightly (add to polywatch-healthz.yml or separate workflow)

    Safety (red lines):
    - Dry-run by default during development
    - Chaos replayed against production polyarb-l1/l2 but with programmatic verification only
    - No action_cmds that write to Supabase or modify prod state (verified by review BEFORE first live run)
    - L2-2 (revoke Supabase key) is the riskiest — must verify dry-run + confirm restore path
  </context>
  <subtasks>
    <subtask name="2a">Create scripts/polywatch/chaos_replay.py (≤200 lines, stdlib-only like healthz_watcher)</subtask>
    <subtask name="2b">Implement --dry-run flag (print commands, no execute, exit 0)</subtask>
    <subtask name="2c">Wire jsonl ledger (same format as Trial 1, verdict per-Inj + overall)</subtask>
    <subtask name="2d">Add Makefile targets: polywatch-chaos-replay, polywatch-chaos-replay-dry</subtask>
    <subtask name="2e">Fly machine cron setup on polyarb-l1 (or GHA workflow as fallback)</subtask>
  </subtasks>
  <tests>
    <test>make polywatch-chaos-replay-dry → prints all commands, exits 0, no network calls</test>
    <test>chaos_replay.py --dry-run → jsonl ledger entry with verdict=dry_run, metrics lists Inj IDs</test>
    <test>Manual: run one Inj (L2-1) live → programmatic_cmds verify, jsonl records verdict</test>
  </tests>
</task>

<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<!-- Trial 3: memory-sanity-check                                               -->
<!-- ═══════════════════════════════════════════════════════════════════════════ -->

<task type="implement" gate="blocking">
  <name>Task 3: Trial 3 — memory-sanity-check ralph-loop</name>
  <context>
    Verify all MEMORY.md VERIFIED entries have valid file:line references.
    This is a ralph-loop pattern: single prompt iteration until completion or max iter.

    Implementation approach: shell script (stdlib: grep, awk, file, wc) —
    NOT Python, NOT Claude-dependent. This is a file-system verification that
    runs without AI. The "ralph" pattern applies when Claude interprets the
    results and proposes patches — the verification itself is mechanical.

    Script: scripts/polywatch/memory_sanity.sh
    - grep MEMORY.md for lines matching "VERIFIED.*file:line" pattern
    - For each match: extract file path and line number
    - Verify: file exists, line number ≤ file line count, content at line matches
    - Output: list of {pass|fail} per reference
    - Write jsonl ledger entry (verdict=pass only if ALL verified)

    The ralph-loop part (Claude-driven):
    - If failures found: Claude reads output, proposes patches in
      .planning/polywatch/memory-sanity-{date}.md
    - Claude does NOT commit (red line #4)
    - Claude does NOT fix MEMORY.md entries (that's the user's decision after review)
    - Max iter: 10 (10 rounds of Claude proposing patches until all pass or exhausted)

    Makefile target: polywatch-memory-sanity
    - Runs the shell script, prints results
    - File to temp markdown for Claude review
  </context>
  <subtasks>
    <subtask name="3a">Create scripts/polywatch/memory_sanity.sh (≤100 lines, POSIX sh, no deps)</subtask>
    <subtask name="3b">Write jsonl ledger entry (verdict + failed_count + details)</subtask>
    <subtask name="3c">Add Makefile target: polywatch-memory-sanity</subtask>
    <subtask name="3d">Create ralph-loop prompt template (inline in CLI or separate .planning file)</subtask>
  </subtasks>
  <tests>
    <test>make polywatch-memory-sanity → runs script, prints pass/fail counts, writes jsonl</test>
    <test>Intentional failure: delete a known file ref'd in MEMORY → script catches it (fail count ≥ 1)</test>
    <test>jsonl ledger: verdict=fail when failures found, verdict=pass when all clean</test>
  </tests>
</task>

<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<!-- Trial 4: autoresearch-validation-tuning                                    -->
<!-- ═══════════════════════════════════════════════════════════════════════════ -->

<task type="implement" gate="blocking">
  <name>Task 4: Trial 4 — autoresearch-validation-tuning grid search</name>
  <context>
    Grid search L4 tolerance values over historical snapshot data to find the
    tolerance that maximizes signal:noise ratio.

    Script: scripts/polywatch/autoresearch_tune.py (depends on polyarb package
    for SQLite access — unlike other trials which are stdlib-only)

    Algorithm:
    1. Load historical snapshots from local SQLite (data/snapshots/*.db)
    2. For each tolerance ∈ {0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5}:
       a. Re-run L4 validation with this tolerance (using existing L4 code paths)
       b. Count: total alerts generated
       c. Count: true problems (alerts where L1/L2 status went to fail within 30 min)
       d. Compute signal:noise = true_problems / total_alerts
    3. Write results to jsonl (one entry per tolerance: {trial_name, iteration (tolerance_idx),
       timestamp, verdict: {tolerance, total_alerts, true_problems, sn_ratio}, notes})

    MVP scope (speed over completeness):
    - Start with 1 day of snapshots (~24 ticks) — fast to iterate
    - Use existing L4 validation code (src/polyarb/snapshot/layer4_cross_source.py)
    - Don't build a new validation pipeline — reuse what's there
    - If 1 day produces zero alerts (tolerance too strict), note it and move on
    - max trials: 10 (grid size) — no infinite search

    Dry-run: --dry-run flag
    - Print grid values + snapshot count + expected run time
    - Don't execute validation
  </context>
  <subtasks>
    <subtask name="4a">Create scripts/polywatch/autoresearch_tune.py (depends on polyarb, uv run)</subtask>
    <subtask name="4b">Implement grid search over 10 tolerance values</subtask>
    <subtask name="4c">Implement signal:noise computation (true positives / total alerts)</subtask>
    <subtask name="4d">Write jsonl results (one entry per tolerance)</subtask>
    <subtask name="4e">Add Makefile targets: polywatch-autoresearch-tune, polywatch-autoresearch-tune-dry</subtask>
  </subtasks>
  <tests>
    <test>make polywatch-autoresearch-tune-dry → prints grid values + snapshot count, exits 0</test>
    <test>make polywatch-autoresearch-tune → runs grid, writes 10 jsonl entries, all fields populated</test>
    <test>At least one tolerance produces sn_ratio > 0 (validates the pipeline works)</test>
    <test>All tolerances produce valid jsonl entries (no crashes on missing data)</test>
  </tests>
</task>

<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<!-- Global skill extraction + escalation wiring + phase close                  -->
<!-- ═══════════════════════════════════════════════════════════════════════════ -->

<task type="implement" gate="blocking">
  <name>Task 5: Global polywatch skill + escalation wiring + phase close</name>
  <context>
    D-3: Extract ~/.claude/skills/polywatch/ global skill (thin-shell).
    After all 4 trials have concrete implementations, extract the common patterns.

    Skill files (minimal — thin shell per D-3 risk mitigation):
    1. SKILL.md — describes 4 trial types, decision tree, red lines, usage
    2. trial_runner.py — generic jsonl ledger writer (imported by all trials)
    3. escalation.py — 4-level escalation (L0 silent / L1 Sentry / L2 Telegram / L3 GH issue)
    4. templates/template_cron.py — template for cron-type trials (healthz, chaos)
    5. templates/template_ralph.py — template for ralph-loop trials (memory-sanity)
    6. templates/template_autoresearch.py — template for autoresearch trials (tuning)

    Escalation wiring:
    - L0 (silent): already done — all 4 trials write jsonl
    - L1 (streak=3 → Sentry breadcrumb): add streak tracking to trial_runner.py
      → When verdict=fail and streak≥3, call sentry_sdk.add_breadcrumb()
    - L2 (red-line → Telegram): wire to existing Telegram API (same as healthz_watcher)
      → Triggered when: max iter exhausted (Trial 3), chaos dry-run boundary breached (Trial 2)
    - L3 (infra broken → GH issue): add to trial_runner.py
      → Check: did cron fire? (compare timestamps). Did script crash? (check exit code).
      → `gh issue create --title "[polywatch L3] ..." --label "polywatch,infra-broken,auto-filed"`

    Phase close:
    - make polywatch-status → print all 4 trial last results from jsonl
    - Add polywatch-status to Makefile
    - All 4 jsonl ledger files have at least 1 entry
    - All Makefile targets documented with `## comment` lines
  </context>
  <subtasks>
    <subtask name="5a">Create ~/.claude/skills/polywatch/SKILL.md</subtask>
    <subtask name="5b">Create trial_runner.py (jsonl ledger writer + streak tracking)</subtask>
    <subtask name="5c">Create escalation.py (L0-L3 implementation)</subtask>
    <subtask name="5d">Create 3 template files (cron, ralph, autoresearch)</subtask>
    <subtask name="5e">Back-annotate Trial 1-4 scripts to use trial_runner.py (optional — keep stdlib independence if templates diverge)</subtask>
    <subtask name="5f">Wire L1 Sentry breadcrumb (streak tracking in trial_runner)</subtask>
    <subtask name="5g">Wire L3 GH issue (infra check in trial_runner)</subtask>
    <subtask name="5h">Add make polywatch-status target</subtask>
  </subtasks>
  <tests>
    <test>~/.claude/skills/polywatch/SKILL.md is valid markdown, loads without error</test>
    <test>trial_runner.py importable: python -c "import trial_runner" → no error</test>
    <test>escalation.py: L0 write works (jsonl file created), L1 Sentry call doesn't crash (with mock token), L3 gh issue command built correctly (--dry-run prints, doesn't create)</test>
    <test>make polywatch-status → prints all 4 trial last results</test>
  </tests>
</task>

</tasks>

<gates>
## Quality Gates

### Pre-execute
- [ ] Phase 03.1 Plan 07 complete (GAP-103 notes field in snapshots)
- [ ] GH_TOKEN + SENTRY_AUTH_TOKEN available
- [ ] Fly machine polyarb-l1 accessible (for chaos cron setup)

### Post-execute
- [ ] All 4 jsonl ledger files exist with ≥1 entry each
- [ ] make polywatch-status works
- [ ] Trial 1 GHA cron still fires (verify in GHA web UI)
- [ ] Trial 2 dry-run exits cleanly (make polywatch-chaos-replay-dry)
- [ ] Trial 3 catches a known-bad file:line reference (manual test)
- [ ] Trial 4 produces 10 jsonl entries with sn_ratio values
- [ ] Global skill SKILL.md is coherent (manual review)
- [ ] No red-line violations (no auto-commit, no prod state mutation, no external channel push)
</gates>

<notes>
## Implementation Notes

### Stdlib-only preference
Trials 1, 2, 3 follow healthz_watcher's stdlib-only pattern. Trial 4 is the exception
(needs polyarb SQLite access). This keeps deployment simple — GHA runner needs only
Python 3.12, no `uv sync`.

### Phase 03.1 dependency
Trial 1's notes field consumption and Trial 2's Inj set both depend on Phase 03.1 Plan 07.
If 03.1 is not complete, Trial 1 ships without notes field (no crash) and Trial 2 starts
with only L2-1/L2-2/L2-3a (the 3 Phase 03-inherited Inj).

### Fly cron setup
One-time manual step: `flyctl machines update` to add schedule config on polyarb-l1.
Document exact command in execute step. Fallback: GHA nightly schedule.

### Skill thin-shell
Skill templates are NOT required to be perfect abstractions. They exist to show the
pattern shape. m5 phase 04-polywatch-globalize will refine them after real-world use.

### Summary note
After this plan's code commits, create 01-1-SUMMARY.md. This is a single-plan phase
so SUMMARY = phase close document.
</notes>
