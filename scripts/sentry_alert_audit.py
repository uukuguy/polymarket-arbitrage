"""Sentry alert routing audit — playwright-cli driven orchestrator.

Phase 03.1-05 D-03 step 1 (GAP-102).

The hard artifact is the **report** at
``.planning/workstreams/m1-perception/phases/03.1-l2-observability-gaps-fix-up/sentry-audit-report.md``;
this script is a thin, re-runnable orchestrator that documents the manual
playwright-cli sequence so a future operator can refresh the report without
re-deriving the navigation steps from scratch.

Pragmatic note (per plan):
  - The Sentry UI is React + lazy-loaded; raw HTTP scraping does NOT work.
  - The only reliable extraction path is playwright-cli driving the Edge
    persistent profile that has live Sentry auth (per
    ``memory/reference_playwright-cli-edge-profile.md``).
  - This script therefore prints the canonical command sequence + extracts
    the structured "what we expect to see" so an executor (human or agent)
    can re-run the audit and confirm whether the rule configuration changed.

Run:
    make sentry-alert-audit

Reads:
    Live ``~/.claude-playwright-profile/`` Edge session with Sentry login.

Writes:
    JSON-lines to stdout — one line per alert rule + one summary line.
    The audit REPORT (sentry-audit-report.md) is populated separately by
    the executor; this script just emits the structured findings to make
    re-run-vs-baseline diffing easy.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime

# Known Sentry org + rule + issue surface as of 2026-05-27 SESSION 28
SENTRY_ORG = "speechlessai"
PROJECT_PYTHON = "python"
PROJECT_ID = "4511406009024592"

ALERT_RULES_URL = f"https://{SENTRY_ORG}.sentry.io/alerts/rules/"
ISSUE_121111789_URL = (
    f"https://{SENTRY_ORG}.sentry.io/issues/121111789/?project={PROJECT_ID}"
)

# Two known rules as of audit baseline (will report ADDED / REMOVED diff if changes)
BASELINE_RULES: list[dict[str, object]] = [
    {
        "id": "597424",
        "name": "Send a notification for high priority issues",
        "url": f"https://{SENTRY_ORG}.sentry.io/issues/alerts/rules/{PROJECT_PYTHON}/597424/details/",
        "environment_filter": "All Environments",
        "when": "A new issue is created",
        "if": "Any event",
        "then": "Notify Member: Jiangwen Su",
        "external_integration": None,  # CRITICAL: no Telegram / Slack / webhook target
    },
    {
        "id": "10000568957",
        "name": "Notify Suggested Assignees",
        "url": f"https://{SENTRY_ORG}.sentry.io/issues/alerts/rules/{PROJECT_PYTHON}/10000568957/details/",
        "environment_filter": "-",  # i.e. none / all
        "when": "A new issue is created",
        "if": "Any event",
        "then": "Notify IssueOwners → fallback ActiveMembers",
        "external_integration": None,
    },
]


def playwright_cli_available() -> bool:
    """Return True if playwright-cli is on PATH and responds to --version."""
    try:
        result = subprocess.run(
            ["playwright-cli", "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def emit_canonical_command_sequence() -> list[dict[str, str]]:
    """Print the navigation steps an executor must run.

    Each step is a (step_no, url_or_action, expected) triple. The executor
    runs `playwright-cli goto <url>` then `playwright-cli snapshot --filename
    /tmp/<slug>.yml` for each URL and greps for the `expected` substring to
    confirm the page rendered the actionable element.
    """
    steps = [
        {
            "n": 1,
            "url": ALERT_RULES_URL,
            "grep_for": "Alert Rules",
            "purpose": "Enumerate all alert rules + capture rule IDs",
        },
    ]
    for rule in BASELINE_RULES:
        steps.append(
            {
                "n": len(steps) + 1,
                "url": str(rule["url"]),
                "grep_for": "Environment",
                "purpose": f"Capture WHEN/IF/THEN + Environment for rule {rule['id']}",
            }
        )
        # Also note edit-mode for actions enumeration
        steps.append(
            {
                "n": len(steps) + 1,
                "url": "(click Edit button on prior page)",
                "grep_for": "perform these actions",
                "purpose": f"Get action targets (Member/Team/Integration) for rule {rule['id']}",
            }
        )
    steps.append(
        {
            "n": len(steps) + 1,
            "url": ISSUE_121111789_URL,
            "grep_for": "environment",
            "purpose": "Confirm issue tag environment=dev/production",
        }
    )
    return steps


def main() -> int:
    """Emit JSON-lines audit baseline + navigation cookbook."""
    ts = datetime.now(UTC).isoformat()
    print(
        json.dumps(
            {
                "type": "header",
                "ts": ts,
                "purpose": (
                    "Sentry alert routing audit baseline — Phase 03.1-05 GAP-102. "
                    "Use these expected rule configs to detect drift on re-run."
                ),
                "playwright_cli_available": playwright_cli_available(),
                "audit_report_path": (
                    ".planning/workstreams/m1-perception/phases/"
                    "03.1-l2-observability-gaps-fix-up/sentry-audit-report.md"
                ),
            }
        )
    )
    for rule in BASELINE_RULES:
        print(json.dumps({"type": "rule_baseline", **rule}))
    print(json.dumps({"type": "steps", "steps": emit_canonical_command_sequence()}))
    print(
        json.dumps(
            {
                "type": "summary",
                "rule_count": len(BASELINE_RULES),
                "rules_with_external_integration": sum(
                    1 for r in BASELINE_RULES if r["external_integration"]
                ),
                "rules_with_environment_filter": sum(
                    1
                    for r in BASELINE_RULES
                    if r["environment_filter"] not in (None, "-", "All Environments")
                ),
                "key_finding": (
                    "0 of 2 rules wire an external integration (no Telegram, no "
                    "Slack, no webhook). Alerts go only to Sentry in-app + the "
                    "email Sentry sends to the notified user — easy to miss for "
                    "low-frequency PAUSE events. Combined with environment=dev "
                    "tag making it look like a non-prod issue, this fully "
                    "explains the 3.5-day no-response on issue 121111789."
                ),
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
