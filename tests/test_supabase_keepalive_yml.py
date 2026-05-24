"""Structural tests for the Supabase keepalive GitHub Actions workflow.

This is a Wave 0 RED test for Plan 03-01 (D-01: Supabase Free + GHA cron
keepalive, reversing RESEARCH §2.6 Pro recommendation on cost grounds).

The workflow must:
1. Exist at .github/workflows/supabase-keepalive.yml
2. Run daily (cron < 4-day Supabase pause threshold)
3. Reference POLYARB_SUPABASE_* secrets (never hardcoded URLs)
4. POST to a Better Stack heartbeat (second alert path — Phase 02 L8 precedent
   showed single-path GHA email alone is insufficient: 4-day silent fail)
5. Avoid the @v1.5 flyctl-actions anti-pin (Phase 02 L8 — non-existent tag)

These are PURE structural assertions on the YAML source text + parsed schema.
They DO NOT execute the workflow; they only enforce that the file shape
matches the contract documented in 03-01-PLAN.md must_haves.

Note on PyYAML `on:` pitfall: GHA workflows use `on:` as the trigger key,
but PyYAML 3.x/5.x/6.x parses `on:` as boolean True (YAML 1.1 legacy).
Defensive: read raw text for substring checks; only use yaml.safe_load
when accessing nested structures, with a True-key fallback.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

WORKFLOW = (
    Path(__file__).resolve().parents[1]
    / ".github"
    / "workflows"
    / "supabase-keepalive.yml"
)


def _raw_text() -> str:
    assert WORKFLOW.exists(), f"keepalive workflow missing: {WORKFLOW}"
    return WORKFLOW.read_text()


def test_workflow_file_exists():
    """Workflow file must exist with non-trivial content."""
    assert WORKFLOW.exists(), f"missing: {WORKFLOW}"
    lines = WORKFLOW.read_text().splitlines()
    assert len(lines) >= 30, f"workflow too short ({len(lines)} lines); expected ≥30"


def test_workflow_has_daily_cron_schedule():
    """Cron must fire daily — anything ≥4 days would let Supabase Free pause.

    Daily cron format: `minute hour * * *` (5 fields, days/months/dow all *).
    """
    text = _raw_text()
    assert "schedule:" in text, "no `schedule:` key in workflow"

    # Find the cron line (handles both quoted and unquoted YAML forms)
    cron_match = re.search(
        r'-\s+cron:\s*["\']?(\S+\s+\S+\s+\S+\s+\S+\s+\S+)["\']?',
        text,
    )
    assert cron_match, "no `- cron:` entry under schedule"

    cron = cron_match.group(1).strip("\"'")
    # Daily pattern: minute hour * * *
    assert re.match(r"^[0-9*]+\s+[0-9]+\s+\*\s+\*\s+\*$", cron), (
        f"cron {cron!r} is not daily — Supabase Free pauses after 4 days idle, "
        "so anything coarser than daily violates D-01"
    )


def test_workflow_references_supabase_secrets():
    """Workflow must reference POLYARB_SUPABASE_* secrets (re-using Phase 02
    Fly secrets — no fresh secret minting per PLAN <interfaces>)."""
    text = _raw_text()
    assert "secrets.POLYARB_SUPABASE_URL" in text, (
        "POLYARB_SUPABASE_URL not referenced — no Supabase endpoint to ping"
    )
    assert (
        "secrets.POLYARB_SUPABASE_ANON_KEY" in text
        or "secrets.POLYARB_SUPABASE_DB_DSN" in text
    ), "neither anon key nor DSN referenced — no auth path for the ping"


def test_workflow_no_hardcoded_supabase_url():
    """No literal `https://<projectid>.supabase.co` allowed — only `${{ secrets.* }}`
    templating. Prevents secret leakage via the (public) workflow file."""
    text = _raw_text()
    # Literal URL pattern: https://<lowercase-alnum-10+>.supabase.co
    # The 10+ length requirement skips synthetic short matches.
    literal_matches = re.findall(r"https://[a-z0-9]{10,}\.supabase\.co", text)
    assert not literal_matches, (
        f"hardcoded Supabase URLs found (should use ${{{{ secrets.* }}}}): "
        f"{literal_matches}"
    )


def test_workflow_includes_better_stack_heartbeat():
    """Second alert path: Better Stack heartbeat URL POST.

    Phase 02 L8 precedent: GHA email alone is insufficient (4-day silent
    fail observed). Heartbeat-miss → Telegram via existing alert chain.
    """
    text = _raw_text()
    assert "BETTER_STACK_KEEPALIVE_HEARTBEAT_URL" in text, (
        "no Better Stack heartbeat — single-path keepalive violates "
        "Phase 02 L8 LEARNINGS (GHA email alone = 4d silent fail)"
    )


def test_workflow_pin_discipline_no_v1_5():
    """No `@v1.5` anti-pin anywhere.

    Phase 02 L8: flyctl-actions/setup-flyctl@v1.5 is a non-existent tag and
    silently fails for 4 days. Even though THIS workflow likely doesn't use
    flyctl, the check is a defensive guard against future drift.
    """
    text = _raw_text()
    assert "@v1.5" not in text, (
        "`@v1.5` is the non-existent flyctl-actions tag (Phase 02 L8 — "
        "4-day silent fail precedent). Use `@1.6` if flyctl needed."
    )


def test_workflow_yaml_is_parseable():
    """The file must be syntactically valid YAML (defensive — catches stray tabs
    or unclosed quotes that break GHA but pass other regex checks)."""
    text = _raw_text()
    parsed = yaml.safe_load(text)
    assert parsed is not None, "workflow YAML parsed to None"
    # `on:` parses as True (boolean) due to YAML 1.1 legacy — accept either key
    trigger_key = "on" if "on" in parsed else True
    assert trigger_key in parsed, (
        f"no trigger key in parsed YAML (keys: {list(parsed.keys())})"
    )
    triggers = parsed[trigger_key]
    assert "schedule" in triggers, f"no `schedule` in triggers ({triggers!r})"
