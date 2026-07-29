# M1 Dashboard Read-Model Remediation — UI Review

**Audited:** 2026-07-29
**Re-audited:** 2026-07-29 after remediation
**Baseline:** Approved remediation plan plus abstract six-pillar standards
**Screenshots:** Six final desktop/375 px captures inspected
**Gate result:** **PASS** — 0 Critical, 0 Important, 0 Minor findings

---

## Pillar Scores

| Pillar | Score | Key Finding |
|--------|-------|-------------|
| 1. Copywriting | 4/4 | Zero, unavailable, bounded history, Reconciliation limits, and notification limits are explicit in every applicable state. |
| 2. Visuals | 4/4 | The hierarchy is strong, four timeline classes are distinct, and operator-relevant incident evidence is readable without exposing raw JSON by default. |
| 3. Color | 4/4 | Semantic blue/purple/green/orange timeline colors and amber unavailable states remain legible on the dark surface. |
| 4. Typography | 4/4 | Headings, values, compact tables, and required 14 px timeline metadata form a consistent hierarchy. |
| 5. Spacing | 4/4 | The 12/16/24 px rhythm is consistent, cards collapse cleanly, and long identities remain contained at 375 px. |
| 6. Experience Design | 4/4 | Authenticated state handling, direct opportunity navigation, available/unavailable states, and bounded-history disclosure form a complete operator path. |

**Overall: 24/24**

---

## Required Task 7 Gate

| Requirement | Result | Evidence |
|-------------|--------|----------|
| No Critical/Important findings | **PASS** | Final audit has 0 Critical, 0 Important, and 0 Minor findings. |
| Approved operational fields use authenticated read models | **PASS** | Strict validators reject malformed identity, authority, floor, ordering, and cross-envelope relations in `dashboard/lib/perception.ts:180-230`, `dashboard/lib/perception.ts:283-439`, and `dashboard/lib/perception.ts:754-831`; Task 1–5 summaries record the corresponding authenticated authorities. |
| Four timeline classes | **PASS** | The discriminated union is declared at `dashboard/lib/types.ts:164-222`, mapped at `dashboard/app/perception/[group_id]/page.tsx:72-126`, and all four classes are visible in both final available captures. |
| Current validated Reconciliation duration and explicit historical-duration-not-tracked copy | **PASS** | The real-store fixture publishes and applies a 6,250 ms Reconciliation window at `scripts/perception_dashboard_fixture.py:59-70`; the final overview captures show `duration_ms: 6250`, all authenticated diff counts, and the limitation. The limitation is unconditional at `dashboard/app/perception/page.tsx:380-383`. |
| Explicit notification-delivery-not-tracked copy | **PASS** | The copy is unconditional at `dashboard/app/perception/page.tsx:472-479` and visible in both final overview captures. |
| Mobile safe | **PASS** | Long group IDs wrap, the four-class timeline remains inside 375 px, and the 900 px opportunity table is contained by an explicit horizontal scroller at `dashboard/app/perception/page.tsx:191-201`. |

---

## Remediation Verification

1. **Current opportunity → group timeline:** resolved. Each primary opportunity
   group ID is now an encoded timeline link at
   `dashboard/app/perception/page.tsx:214-223`. The link is visible in both
   final overview captures.
2. **Reconciliation acceptance evidence:** resolved. The deterministic fixture
   uses the real store API to begin, publish, and apply a Reconciliation window
   (`scripts/perception_dashboard_fixture.py:59-70`). Desktop and mobile show
   state `applied`, duration 6,250 ms, all five diff counts as zero, and the
   explicit historical-distribution limitation.
3. **Resource time reference:** resolved. Policy age and TTL use the validated
   API `status.server_time_ms` at
   `dashboard/app/perception/page.tsx:397-411`; `Date.now()` is no longer used.
4. **Incident evidence scanability:** resolved. Canonical action, retry count,
   next retry, success receipt, and verification fields are promoted at
   `dashboard/app/perception/[group_id]/page.tsx:50-69`; raw evidence is
   secondary and collapsed by default at `:238-249`.

---

## Detailed Findings

### Pillar 1: Copywriting (4/4)

The UI consistently states the evidence boundary:

- “Unavailable is not zero opportunities” and “Unavailable is not an empty
  group history” prevent false market conclusions
  (`dashboard/app/perception/page.tsx:64-75`,
  `dashboard/app/perception/[group_id]/page.tsx:138-147`).
- Global Candidate totals are separated from bounded Structure-page counts
  (`dashboard/app/perception/page.tsx:120-160`).
- Pagination and compaction are described separately on the group page
  (`dashboard/app/perception/[group_id]/page.tsx:182-195`).
- Historical Reconciliation duration distribution is explicitly not tracked
  even before a row exists (`dashboard/app/perception/page.tsx:380-383`).
- Notification delivery is explicitly not tracked
  (`dashboard/app/perception/page.tsx:472-479`).

The final captures show the required copy in both desktop and mobile states.

### Pillar 2: Visuals (4/4)

The overview has a clear flow from global Candidate state to current
opportunities, coverage, producer state, resources, incidents, and observed
groups. The group page provides a strong focal title, four summary cards, and a
descending evidence stream.

All four timeline classes are visible and immediately distinguishable in both
available captures. Incident events now surface their useful operational
fields inline, while “Raw evidence” is a collapsed disclosure. This preserves
audit access without allowing JSON to dominate the timeline.

### Pillar 3: Color (4/4)

The group page uses four restrained semantic accents:

- Membership `#9ec5fe`
- Quote `#c9a7ff`
- Opportunity `#74d99f`
- Incident `#ffb36b`

These are declared once at
`dashboard/app/perception/[group_id]/page.tsx:31-36`. Amber `#ffd47a` plus the
dark amber surface identifies unavailable and continuation states without
reusing success green. The final screenshots show adequate contrast and no
accent overuse.

### Pillar 4: Typography (4/4)

The implementation uses a compact set of sizes: 13 px for overview
metadata/tables, 14 px for group timeline metadata, 26 px for the overview
title, and 30 px for the opportunity count. Required 14 px timeline metadata
appears at `dashboard/app/perception/[group_id]/page.tsx:30`, `:233`, and
`:251`. Bold labels and values provide clear hierarchy without excessive
weight variants.

### Pillar 5: Spacing (4/4)

The UI consistently uses 12 px grid gaps, 16 px panel padding, and 24 px page
padding. Auto-fit grids collapse to one column cleanly on 375 px
(`dashboard/app/perception/page.tsx:121-150`,
`dashboard/app/perception/[group_id]/page.tsx:198-222`). Long identities use
safe wrapping, and the wide opportunity table is isolated inside an
`overflowX: "auto"` container.

The final 375 px overview, available timeline, and unavailable timeline
captures show no page-level horizontal overflow or clipped card content.

### Pillar 6: Experience Design (4/4)

The data and interaction boundaries are complete:

- Every fetch is `no-store`, shares a three-second absolute signal, and maps
  transport/HTTP/JSON/contract failure to unavailable
  (`dashboard/lib/perception.ts:722-752`, `dashboard/lib/perception.ts:770-868`).
- Candidate status and opportunity pages must share the same authenticated
  authority hash and counts (`dashboard/lib/perception.ts:754-767`,
  `dashboard/lib/perception.ts:821-823`).
- The group envelope validates exact identity, four-class order, exact
  Incident scope, history-floor relationships, and bounded continuation
  semantics (`dashboard/lib/perception.ts:283-439`).
- Every current opportunity links directly to its encoded group evidence route
  (`dashboard/app/perception/page.tsx:214-223`).
- Available, empty, continuation, compressed-history, and unavailable
  semantics are explicit; unavailable never becomes zero or empty history.
- Policy age and TTL share the authenticated API clock
  (`dashboard/app/perception/page.tsx:397-411`).

Focused verification passed:

```text
18 passed — tests/m1-perception/test_dashboard_perception_contract.py
dashboard-typecheck — passed
git diff --check (reviewed UI/fixture/contracts) — passed
```

---

## Priority Fixes

None. The final review has no Critical, Important, or Minor findings.

---

## Files Audited

- `docs/superpowers/plans/2026-07-29-m1-dashboard-read-model-remediation.md`
- `docs/superpowers/plans/2026-07-29-m1-dashboard-read-model-remediation-TASK-1-SUMMARY.md`
- `docs/superpowers/plans/2026-07-29-m1-dashboard-read-model-remediation-TASK-2-SUMMARY.md`
- `docs/superpowers/plans/2026-07-29-m1-dashboard-read-model-remediation-TASK-3-SUMMARY.md`
- `docs/superpowers/plans/2026-07-29-m1-dashboard-read-model-remediation-TASK-4-SUMMARY.md`
- `docs/superpowers/plans/2026-07-29-m1-dashboard-read-model-remediation-TASK-5-SUMMARY.md`
- `dashboard/app/perception/page.tsx`
- `dashboard/app/perception/[group_id]/page.tsx`
- `dashboard/lib/perception.ts`
- `dashboard/lib/types.ts`
- `scripts/perception_dashboard_fixture.py`
- `tests/m1-perception/test_dashboard_perception_contract.py`
- `docs/M1-市场感知平台使用手册.md`
- `output/playwright/task6/overview-available-desktop.png`
- `output/playwright/task6/overview-available-mobile-375.png`
- `output/playwright/task6/group-long-available-desktop.png`
- `output/playwright/task6/group-long-available-mobile-375.png`
- `output/playwright/task6/group-long-unavailable-desktop.png`
- `output/playwright/task6/group-long-unavailable-mobile-375.png`

No `components.json` is present, so the third-party registry audit does not
apply.
