# Sentry Alert Audit Report — Phase 03.1 GAP-102

**Generated:** 2026-05-27 01:09 UTC
**Audit method:** playwright-cli driving Edge persistent profile with live Sentry login (per `memory/reference_playwright-cli-edge-profile.md`)
**Purpose:** Determine why 6-day, 3-occurrence `SCHEDULER_PAUSED` alert (Sentry issue [121111789](https://speechlessai.sentry.io/issues/121111789/?project=4511406009024592)) produced **no user response** for 3.5 days.

**Audit dashboard entry points (real URLs hit during this audit — W-4 gate evidence):**
- Alerts list: <https://speechlessai.sentry.io/alerts/rules/>
- Rule 597424 details: <https://speechlessai.sentry.io/issues/alerts/rules/python/597424/details/>
- Rule 10000568957 details: <https://speechlessai.sentry.io/issues/alerts/rules/python/10000568957/details/>
- Issue 121111789: <https://speechlessai.sentry.io/issues/121111789/?project=4511406009024592>
- Integrations: <https://speechlessai.sentry.io/settings/integrations/>

## Alert Rules Enumerated

| Rule ID | Name | Environment Filter | Action Targets | External Integration | Verdict |
|---|---|---|---|---|---|
| [597424](https://speechlessai.sentry.io/issues/alerts/rules/python/597424/details/) | "Send a notification for high priority issues" | **All Environments** (no filter) | Member: Jiangwen Su | **None** (no Telegram / no Slack / no webhook) | Would fire on every env — but only delivers to Sentry inbox + Sentry-account email (easy to miss for low-frequency events) |
| [10000568957](https://speechlessai.sentry.io/issues/alerts/rules/python/10000568957/details/) | "Notify Suggested Assignees" | **-** (none / All) | IssueOwners → fallback ActiveMembers | **None** | Same delivery channel as 597424 — Sentry-internal only |

**Action-target details (from Edit form snapshots):**
- Rule 597424 edit form shows exactly **one** "Then perform these actions" row: `Notify` → target type `Member` → user `Jiangwen Su`. The `Select an action` dropdown next to it is empty (no Telegram/Slack/Webhook action added).
- Rule 10000568957 detail page shows `Then perform these actions: Send a notification to IssueOwners and if none can be found then send a notification to ActiveMembers`. Same in-Sentry delivery profile.

**Integrations check** (<https://speechlessai.sentry.io/settings/integrations/>): the integrations grid is lazy-loaded React; the static snapshot did not enumerate installed integrations. However the alert-rule action-target dropdowns above are exhaustive proof that **no Telegram integration is wired into any active alert rule** regardless of whether the integration is "installed" at the org level.

## Issue 121111789 Linked Alerts / Tags

| Field | Value | Source |
|---|---|---|
| Latest event message | `2026-05-22 01:45:59.445 \| ERROR \| polyarb.daemon.scheduler:_on_paused:112 - SCHEDULER_PAUSED: consecutive failure threshold reached (counter=3). Manual restart required.` | Issue page, line 135 |
| Tag `environment` distribution | **100% `dev`** | <https://speechlessai.sentry.io/issues/121111789/distributions/environment/?project=4511406009024592> |
| Tag `release` distribution | **100% `dev`** | <https://speechlessai.sentry.io/issues/121111789/distributions/release/?project=4511406009024592> |
| `module` tag | `polyarb.daemon.scheduler` | Sidebar tags region |
| Linked Alerts tab | Not surfaced as a separate panel on this issue (Issue 4xx visualization lacks "Linked Alerts" widget by default) | — |

**Occurrence trace (per `memory/project_fly-dns-chronic-failure-2026-05.md`):**

| # | UTC time | Source | Rule that *would* match | Actually delivered? |
|---|---|---|---|---|
| 1 | 2026-05-19 21:06 | Sentry issue 121111789 history | 597424 + 10000568957 (both env=All, both WHEN=new issue) | In-Sentry only (Telegram not wired) |
| 2 | 2026-05-22 00:16 | Sentry issue 121111789 history | same | In-Sentry only |
| 3 | 2026-05-22 01:45 | Visible in current event line 135 | same | In-Sentry only |

(Sentry-internal email to `uukuguy@gmail.com` is the only out-of-app channel — Sentry's default behavior on a member-target rule. Whether it actually arrived depends on Sentry's account email preferences, which are NOT exposed to playwright without further authenticated nav.)

## Telegram Bot History

**Status:** GAP — could not be verified from the playwright session.

**Why:** Sentry → Telegram delivery requires a Sentry Telegram integration. Audit found **zero** Telegram action targets on either alert rule, which is the upstream proof: even if the Telegram bot were configured at the org level, it would not be invoked because no rule action wires it. There is therefore **no Telegram message to search for** corresponding to the 3 SCHEDULER_PAUSED occurrences.

**Independent fallback:** `src/polyarb/daemon/alerts.py` does have a `telegram_bot_token` direct-fallback path (per `config.py:169-173`), but that path is only invoked when Better Stack returns 5xx. SCHEDULER_PAUSED goes through `sentry_sdk.capture_message`, not the Better Stack path — confirming Telegram was structurally never going to fire for this issue.

## Hypothesis Validation

**SESSION 27 hypothesis:** "environment=dev tag silences/downgrades alert routing."

**Evidence:**
- ✅ Issue 121111789 IS tagged `environment=dev` and `release=dev` — 100% of events
- ❌ BUT both active alert rules have **no environment filter** (Environment: All / `-`), so an env=dev tag does **not** prevent rule firing
- 🆕 The real silencing mechanism is **the rule action targets**: 0 of 2 rules wire any external integration. Both deliver only to Sentry's in-app inbox + the automatic email Sentry sends to the targeted member. These are low-salience channels for a sole-developer setup that doesn't habit-check the Sentry web UI.
- 🆕 Compounding factor: the env=dev tag *visually* makes new issues look like noise when the user does open the Sentry inbox — even if rules fired, the dev/prod ambiguity reduces the perceived urgency.

**Verdict: REFINED → PARTIALLY CONFIRMED.**

The original hypothesis (env=dev tag blocks routing) is **REFUTED at the rule-level filter** — neither rule filters by environment. But the *spirit* of the hypothesis (the env=dev tag contributes to silence) is **CONFIRMED at the perception level** (env=dev visually downgrades the issue's apparent importance).

The **dominant root cause** is the missing Telegram/Slack/webhook action targets, not the env tag.

## Recommended Alert Rule Changes

After flipping production deploys to `environment=production`, the following changes restore real alert routing:

| # | Action | Why | Risk if skipped |
|---|---|---|---|
| 1 | **Add Telegram action target to rule 597424** — `Then Send a notification to <Telegram bot via integration>` | Forces high-priority issues out of Sentry inbox into a channel the user actively monitors | Future SCHEDULER_PAUSED still goes only to dormant email + Sentry inbox |
| 2 | **(After change 1)** Restrict rule 597424's environment filter to `environment:production` | Once prod issues route to Telegram, env-filtering keeps dev noise out of Telegram | None — without change 1 this would actively make things worse |
| 3 | Leave rule 10000568957 unchanged (IssueOwners + ActiveMembers fallback is appropriate for the broader catch-all) — OR delete it once rule 597424 handles high-priority routing | Avoid double-firing on every issue | Cosmetic / inbox clutter only |
| 4 | (Out of scope per D-03) PagerDuty / SMS escalation | Bypasses email entirely for true-critical incidents | Pushed to m5-industrialize Polywatch backlog |

**Pre-condition for change 2 (env filter):** Plan 03.1-05 Task 2 must land + `POLYARB_SENTRY_ENV=production` must be set on Fly secrets BEFORE adding the env filter, otherwise legitimate prod issues will be tagged `environment=dev` and silenced.

## Out of Scope

Per D-03 step 3: PagerDuty / SMS escalation → m5-industrialize Polywatch backlog.

## Re-running this audit

```bash
make sentry-alert-audit
```

The script (`scripts/sentry_alert_audit.py`) emits a JSON-lines baseline that can be diffed against future runs to detect rule drift. Re-extract live rule configuration by re-running the playwright-cli command sequence printed by `make sentry-alert-audit` (under `"type": "steps"`).
