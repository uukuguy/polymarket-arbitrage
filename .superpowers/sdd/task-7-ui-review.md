# Task 7 — Dashboard Perception and Incident Views UI Review

**Audited:** 2026-07-29
**Baseline:** Approved opportunity-first design §10 and Task 7 brief/rollout
**Screenshots:** Three existing local captures inspected (1280×900 overview, 375×812 overview, 1280×720 group); no fresh capture because no dev server responded on 3000, 5173, or 8080
**Gate:** **FAIL — Important functionality gaps remain**

---

## Pillar Scores

| Pillar | Score | Key Finding |
|---|---:|---|
| 1. Copywriting | 3/4 | Zero, unavailable, bounded-page, and not-exposed states are explicit, but implementation terminology is repeated where operator-oriented recovery copy is needed. |
| 2. Visuals | 3/4 | Desktop and 375 px overview have a clear hierarchy and responsive cards, but amber placeholders dominate the operational surface and there is no visual prioritization of actionable incidents. |
| 3. Color | 3/4 | State colors are restrained and mostly high contrast; the 12 px `#777` timeline timestamp is only 4.22:1 against `#111`. |
| 4. Typography | 3/4 | Heading, metric, body, and metadata hierarchy is coherent, though 12–13 px metadata is small and lacks an explicit line-height. |
| 5. Spacing | 3/4 | The overview grid collapses cleanly to one column at 375 px and uses a consistent 8/10/12/16/24 scale; group mobile behavior lacks captured evidence and long identifiers have no wrap rule. |
| 6. Experience Design | 2/4 | Fail-soft truth is strong, but most of the approved opportunity-first operator data and two required timeline classes are placeholders rather than working views. |

**Overall: 17/24**

---

## Gate Findings

### Important 1 — The overview does not deliver the approved opportunity-first operating picture

The approved Task 7 contract requires actual current opportunity edge/capacity,
Structure and Quote age, 15/30/60-minute raw and weighted coverage, resource
mode, and group-state distinctions. The implementation renders a valid
opportunity count, but all four opportunity measures, all six coverage values,
resource mode, `watching`, and per-group `unavailable` are literal
`not exposed` placeholders
(`dashboard/app/perception/page.tsx:83-98`,
`:118-155`, `:198-201`). The result is honest, but it is not yet the
operator-facing M1 opportunity watcher specified in the approved design.

This cannot be fixed by visual polish alone. Task 6 must expose bounded,
authoritative fields (or an approved contract revision must explicitly reduce
Task 7 scope), after which the typed reader and overview must render them.

### Important 2 — The required unified group timeline omits Quote and opportunity history

The approved timeline must merge membership revisions, Quote batches,
opportunity transitions, and incident events. The implementation's
`TimelineItem` union only permits membership and incident events
(`dashboard/app/perception/[group_id]/page.tsx:7-13`), and the actual merge only
contains those two sources (`:67-87`). Quote batch and opportunity transition
cards are placeholders (`:118-129`) and contribute no timeline rows.

Expose bounded Quote-batch and opportunity-transition history through the
public read contract, validate it in `dashboard/lib/perception.ts`, and merge
all four classes by authoritative event timestamp.

### Important 3 — Incident and progress views omit the operational details promised by the design

The overview shows incident kind, latest state, timestamp, and scope only
(`dashboard/app/perception/page.tsx:203-228`). It does not show automated
action, retries, age, recovery evidence, or notification delivery. Discovery
does not show priority/cursor/queue classes/oldest age, while Reconciliation
does not show duration distribution or differences (`:166-195`). These fields
are central to the project's definition of production stability: detecting,
following, and recovering from abnormal operation.

Extend the bounded read model and present these as scannable operator fields,
with severity/state badges and explicit recovery evidence. Until then, this
surface cannot support the approved incident-response workflow.

---

## Top 3 Priority Fixes

1. **Close the overview data-contract gap** — expose and render real opportunity, age, coverage, group-state, and resource-allocation values; retain `not exposed` only for a deliberate, approved contract exception.
2. **Complete the four-class group timeline** — add bounded Quote-batch and opportunity-transition history, validate group identity, and merge all four classes by timestamp.
3. **Make abnormal operation traceable** — show incident action/retry/age/recovery and richer Discovery/Reconciliation progress; then add a mobile group capture and wrap long group IDs.

---

## Detailed Findings

### Pillar 1: Copywriting (3/4)

- Strong truth-preserving copy separates a valid zero
  (`dashboard/app/perception/page.tsx:103-117`) from a full read failure
  (`:39-55`). The group page likewise says unavailable is not an empty history
  (`dashboard/app/perception/[group_id]/page.tsx:53-63`).
- Bounded-page qualifications are explicit (`dashboard/app/perception/page.tsx:88`,
  `:96`, `:207-215`, `:232-237`; group page `:98-103`).
- Repeating “Task 6 public read model” throughout the primary UI is
  implementation-centric. Operators need concise labels such as “Telemetry not
  yet available” plus the producing component or recovery action.
- The observed-groups panel has no explicit empty state when `groups.items` is
  empty (`dashboard/app/perception/page.tsx:231-255`).

### Pillar 2: Visuals (3/4)

- The existing desktop overview has a clear path from health summary to current
  opportunity, coverage, producer progress, incidents, and groups.
- The existing group screenshot makes the descending timeline easy to scan,
  with class label, event title, detail, and timestamp.
- The 375 px overview screenshot confirms summary cards collapse to one column
  without clipping in the captured portion.
- Large volumes of identical amber `not exposed` text compete with actual
  state. Actionable incidents have no severity/state badge or stronger focal
  treatment (`dashboard/app/perception/page.tsx:217-227`).

### Pillar 3: Color (3/4)

- The palette is coherent: neutral panels (`#111`/`#292929`), blue navigation
  and timeline classes (`#9ec5fe`), amber unknown/warning states
  (`#d2a85a`/`#ffd47a`), and green valid-zero evidence (`#9bc79b`).
- Calculated contrast is strong for amber, green, blue, and normal muted text:
  `#d2a85a` on `#111` is 8.53:1; `#888` on `#111` is 5.33:1.
- The group timestamp uses 12 px `#777` on `#111`
  (`dashboard/app/perception/[group_id]/page.tsx:148-150`), measuring 4.22:1
  and missing WCAG AA's 4.5:1 requirement for normal text.
- Eleven hard-coded colors are repeated across the two pages. Shared semantic
  tokens would prevent state-color drift.

### Pillar 4: Typography (3/4)

- The 26 px page title, 30 px primary count, default section headings, body
  text, and 12–13 px metadata form a clear hierarchy
  (`dashboard/app/perception/page.tsx:69`, `:105-107`;
  group page `:92-94`, `:145-150`).
- Only a small number of sizes and weights are used, so the pages remain
  visually consistent with the existing L1 dashboard.
- The 12–13 px operational metadata is dense for prolonged monitoring and no
  explicit line-height is set. Use at least 14 px for essential timestamps and
  state details, or raise contrast and line-height if compact density is
  required.

### Pillar 5: Spacing (3/4)

- Panels consistently use 16 px padding, 8 px radii, and 10–12 px grid gaps;
  page gutters are 24 px (`dashboard/app/perception/page.tsx:7-12`, `:68-81`;
  group page `:15-20`, `:89-112`).
- `auto-fit/minmax` produces a clean one-column summary on the captured 375 px
  overview and avoids fixed desktop-only columns.
- No fresh runtime server was available, and there is no existing mobile group
  screenshot. Source inspection indicates the cards collapse, but the group ID
  and event details have no `overflowWrap`/`wordBreak`
  (`dashboard/app/perception/[group_id]/page.tsx:93`, `:147`), so long
  production identifiers can force horizontal overflow.

### Pillar 6: Experience Design (2/4)

- The reader correctly uses one shared three-second signal per assembled read,
  `cache: "no-store"`, HTTP checks, JSON parsing, and nested validation
  (`dashboard/lib/perception.ts:173-195`, `:205-245`, `:247-273`).
- A failure in any required request produces the typed unavailable page rather
  than a false zero. A valid `available/count=0` has distinct copy
  (`dashboard/app/perception/page.tsx:39-65`, `:103-117`).
- Empty states exist for Discovery, Reconciliation, incidents, and the group
  timeline, but not the observed-groups list.
- The core operator experience remains incomplete because the overview lacks
  the approved opportunity evidence and the group timeline lacks two of four
  required event classes. Incident response details are also absent. These are
  functionality gaps, not cosmetic recommendations.

---

## Mobile Verification

- **Overview:** Existing 375×812 capture inspected. Navigation fits at that
  width, summary cards collapse to one column, and the visible opportunity
  panel wraps text without clipping.
- **Group:** Desktop capture inspected. No mobile group capture exists and no
  dev server was available for a fresh one. Responsive card layout is present
  in source, but long IDs/details need explicit wrapping before mobile
  acceptance.

## Registry Safety

No `components.json` exists, so the third-party shadcn registry audit is not
applicable.

## Files Audited

- `dashboard/app/layout.tsx`
- `dashboard/app/perception/page.tsx`
- `dashboard/app/perception/[group_id]/page.tsx`
- `dashboard/lib/perception.ts`
- `dashboard/lib/types.ts`
- `tests/m1-perception/test_dashboard_perception_contract.py`
- `src/polyarb/http/perception.py` (public incident/read semantics)
- `output/playwright/task7-perception-overview.png`
- `output/playwright/task7-perception-overview-mobile.png`
- `output/playwright/task7-perception-group.png`

## Completion Recommendation

**Task 7 may not be marked complete and Task 8 cutover should not begin.**
Resolve the three Important functionality findings, rerun the contract/build
gates, and repeat the UI audit with desktop and mobile captures for both
available and unavailable states.
