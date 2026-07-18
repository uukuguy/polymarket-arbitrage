"""Declarative chaos plan for Phase 03 L2 (Wave 6 / Plan 03-07).

The plan itself is data — a list of `ChaosInjection` dataclasses, one per Inj
L2-1..L2-5. Two invariant tests enforce the Phase 02 L6/L7 + Phase 02.1
D-09 + L8 discipline:

1. `test_every_injection_has_programmatic_verification`: every Inj must
   expose `programmatic_cmds` (shell commands that produce machine-readable
   evidence — curl, psql, flyctl logs, Sentry API). No "open Sentry dashboard
   and look for X" entries. Phase 02.1 D-09 verification-ownership rule.

2. `test_every_injection_has_container_fallback`: every Inj must expose
   `container_localhost_fallback` (a `flyctl ssh console -C "curl
   localhost:8080/..."` form). Phase 02.1 L8 lesson: when Fly proxy is the
   thing that broke mid-Inj, public endpoints lie; container-localhost is
   the only ground truth.

Both invariants are pytest assertions — they fail loudly if a future
Inj is added without the discipline. They do NOT execute the chaos.
The actual injections run via `make chaos-l2-injN` (manual, observed by
Claude with results recorded in 03-SOAK-LOG.md).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class ChaosInjection:
    """One row of L2_CHAOS_PLAN. Declarative — does not execute."""

    inj_id: str  # e.g. "L2-1"
    title: str
    code_path: str  # which Plan-04/05/06 invariant this tests
    action_cmds: list[str] = field(default_factory=list)
    programmatic_cmds: list[str] = field(default_factory=list)
    container_localhost_fallback: list[str] = field(default_factory=list)
    expected_truth: str = ""
    cleanup_cmds: list[str] = field(default_factory=list)


L2_CHAOS_PLAN: list[ChaosInjection] = [
    ChaosInjection(
        inj_id="L2-1",
        title="Kill WS connection mid-stream → watchdog reconnect",
        code_path="src/polyarb/daemon/ws_watchdog.py (Plan 04, D-03)",
        action_cmds=[
            # Pick the most expensive approach that does NOT need code changes:
            # SSH into container and pkill the python process. uvicorn auto-restarts
            # within Fly's machine-level restart_policy. We measure the L2 mirror
            # gap from the timestamp delta in l2_top_of_book.
            # Alternative (more surgical, requires WS test flag in code): set
            # POLYARB_WS_TEST_KILL=1 as Fly secret then restart. We use the first.
            'flyctl ssh console -a polyarb-l2 -C "pkill -SIGTERM -f polyarb.daemon.l2_main"',
        ],
        programmatic_cmds=[
            # Watchdog state visible via /health right after kill — expect RECONNECTING.
            'curl -fsS https://polyarb-l2.fly.dev/health | jq ".checks[\\"ws:connection_state\\"][0].observedValue"',
            # Then within 45s expect CONNECTED again and l2_top_of_book latest row younger than 45s.
            'sleep 50 && curl -fsS https://polyarb-l2.fly.dev/health | jq ".checks[\\"ws:last_event_age_seconds\\"][0].observedValue"',
            # Ground truth: a new l2_top_of_book row after the kill timestamp.
            'psql "$POLYARB_SUPABASE_DB_DSN" -tAc "SELECT count(*) FROM l2_top_of_book WHERE ts > now() - interval \'45 seconds\'"',
        ],
        container_localhost_fallback=[
            'flyctl ssh console -a polyarb-l2 -C "curl -fsS localhost:8080/health"',
        ],
        expected_truth=(
            "watchdog state transitions to RECONNECTING within 30s of WS silence; "
            "CONNECTED restored within 45s; at least one new l2_top_of_book row "
            "written in the 45s window after kill. Natural recovery via Fly "
            "machine auto-restart — no manual cleanup needed."
        ),
        cleanup_cmds=[],
    ),
    ChaosInjection(
        inj_id="L2-2",
        title="Revoke POLYARB_SUPABASE_SERVICE_KEY → l2-mirror fail-soft",
        code_path="src/polyarb/storage/l2_supabase_mirror.py (Plan 06, D-12 envelope)",
        action_cmds=[
            # Snapshot current secret first so cleanup restores it byte-for-byte.
            'echo "BACKUP=$(grep ^POLYARB_SUPABASE_SERVICE_KEY .env | cut -d= -f2-)"',
            # Unset the service key on polyarb-l2 ONLY (L1 keeps its key).
            'flyctl secrets unset POLYARB_SUPABASE_SERVICE_KEY -a polyarb-l2',
        ],
        programmatic_cmds=[
            # After ~60s, mirror writes should be failing fail-soft; daemon stays up.
            'curl -fsS https://polyarb-l2.fly.dev/healthz | jq ".status"  # always 200',
            'curl -sS -o /dev/null -w "%{http_code}\\n" https://polyarb-l2.fly.dev/health  # likely 503 due to mirror skipped',
            # Sentry breadcrumb category=l2-mirror present in last hour:
            'curl -fsS "https://de.sentry.io/api/0/projects/$SENTRY_ORG/$SENTRY_PROJECT/events/?statsPeriod=1h&query=category%3Al2-mirror" -H "Authorization: Bearer $SENTRY_AUTH_TOKEN" | jq "length"',
            # Crucial: daemon does NOT crash — Fly machine still in started state.
            'flyctl status -a polyarb-l2 | grep "started"',
            # No new l2_top_of_book rows in last 60s (mirror write was skipped):
            'psql "$POLYARB_SUPABASE_DB_DSN" -tAc "SELECT count(*) FROM l2_top_of_book WHERE ts > now() - interval \'60 seconds\'"',
        ],
        container_localhost_fallback=[
            'flyctl ssh console -a polyarb-l2 -C "curl -fsS localhost:8080/health"',
            'flyctl logs -a polyarb-l2 --no-tail | grep -c "l2-mirror" | tail',
        ],
        expected_truth=(
            "daemon stays in 'started' machine state; /healthz returns 200; "
            "/health returns 503 (mirror sub-check fails); Sentry breadcrumb "
            "with category='l2-mirror' is recorded; ZERO new l2_top_of_book "
            "rows in the post-revoke window"
        ),
        cleanup_cmds=[
            # Restore from .env (which still has the key — we only unset on Fly).
            'set -a; . ./.env; set +a; '
            'flyctl secrets set POLYARB_SUPABASE_SERVICE_KEY="$POLYARB_SUPABASE_SERVICE_KEY" -a polyarb-l2',
        ],
    ),
    ChaosInjection(
        inj_id="L2-3",
        title="L1 NOTIFY emission gate probe (default OFF + opt-in path)",
        code_path="src/polyarb/snapshot/orchestrator.py step 7.7 + src/polyarb/events/listener.py (Plan 05, B1 spawn constraint)",
        action_cmds=[
            # Part A — confirm default-state is OFF on L1:
            'flyctl secrets list -a polyarb-l1 | grep -i event_bus || echo "EVENT_BUS_ENABLED unset = OFF (B1 default)"',
            # Confirm L1 does NOT emit NOTIFY when running a snapshot:
            'flyctl logs -a polyarb-l1 --no-tail | grep -c "publish_snapshot_complete\\|event-bus" || echo "0 publishes (expected)"',
            # Part B — temporarily opt-in:
            'flyctl secrets set POLYARB_EVENT_BUS_ENABLED=1 -a polyarb-l1',
            # Force an L1 snapshot to fire NOTIFY:
            'curl -fsS -X POST -H "X-Signature: $(echo -n \'{}\' | openssl dgst -sha256 -hmac \\"$POLYARB_SCAN_SHARED_SECRET\\" | cut -d\\" \\" -f2)" -d \'{}\' https://polyarb-l1.fly.dev/scan',
        ],
        programmatic_cmds=[
            # Part A: L2 still functioning despite no L1 NOTIFY (catchup_from_cursor + bootstrap_assets keep it alive).
            'curl -fsS https://polyarb-l2.fly.dev/health | jq ".checks[\\"event_bus:listener_state\\"][0].observedValue"  # = listening',
            # Part B: after opt-in + L1 scan, L2 logs should show NOTIFY received + candidate_refresh dispatch:
            'sleep 90 && flyctl logs -a polyarb-l2 --no-tail | grep -oE "candidate refresh.*snapshot_id=[0-9]+" | tail -3',
            # Cursor advanced post-NOTIFY:
            'psql "$POLYARB_SUPABASE_DB_DSN" -tAc "SELECT last_snapshot_id FROM l2_event_cursor WHERE consumer=\'l2-candidate-refresh\'"',
        ],
        container_localhost_fallback=[
            'flyctl ssh console -a polyarb-l2 -C "curl -fsS localhost:8080/health | jq .checks"',
        ],
        expected_truth=(
            "Part A (default OFF): L1 publishes 0 NOTIFYs; L2 event_listener.state=listening; "
            "L2 still gets candidates via catchup_from_cursor and bootstrap_asset_ids — daemon healthy. "
            "Part B (opt-in): after enabling + scan, L2 receives NOTIFY, candidate_refresh runs, "
            "l2_event_cursor.last_snapshot_id advances by ≥1."
        ),
        cleanup_cmds=[
            # CRITICAL: revert to default OFF (B1 invariant — opt-in only after this Inj PASS).
            'flyctl secrets unset POLYARB_EVENT_BUS_ENABLED -a polyarb-l1',
        ],
    ),
    ChaosInjection(
        inj_id="L2-4",
        title="Cross-bug: WS reconnect storm + Supabase pause simultaneously",
        code_path="ws_watchdog (Plan 04) + supabase-keepalive GHA (Plan 01) + L2 mirror fail-soft (Plan 06)",
        action_cmds=[
            # Trigger reconnect storm: 11 successive WS process kills in <1 hour
            # forces watchdog into MAX_RECONNECTS_PER_HOUR cap → degrade to REST.
            'for i in $(seq 1 11); do flyctl ssh console -a polyarb-l2 -C "pkill -SIGTERM -f polyarb.daemon.l2_main"; sleep 60; done &',
            # Simultaneously: simulate Supabase pause by unsetting the DB DSN on L2.
            # (Real Supabase Free pause takes 4 days idle — too long; this is the
            # closest impl-substitute that hits the same code path — listener fail.)
            'flyctl secrets unset POLYARB_SUPABASE_DB_DSN -a polyarb-l2',
        ],
        programmatic_cmds=[
            # Daemon stays in started state through the storm:
            'flyctl status -a polyarb-l2 | grep "started"',
            # Watchdog hits storm cap → state = DEGRADED_REST visible:
            'curl -fsS https://polyarb-l2.fly.dev/health | jq ".checks[\\"ws:connection_state\\"][0].observedValue"',
            # event_bus:listener_state observability = either listening (cached state)
            # or degraded after Supabase DSN unset.
            'curl -fsS https://polyarb-l2.fly.dev/healthz | jq ".checks[\\"event_bus:listener_state\\"][0].observedValue"',
            # Alert dedup proof — Sentry should NOT have 11 separate ws-reconnect events:
            'curl -fsS "https://de.sentry.io/api/0/projects/$SENTRY_ORG/$SENTRY_PROJECT/events/?statsPeriod=1h&query=ws_watchdog" -H "Authorization: Bearer $SENTRY_AUTH_TOKEN" | jq "[.[] | select(.message | contains(\\"reconnect\\"))] | length"',
        ],
        container_localhost_fallback=[
            'flyctl ssh console -a polyarb-l2 -C "curl -fsS localhost:8080/healthz"',
            'flyctl ssh console -a polyarb-l2 -C "ps aux | grep -c polyarb.daemon"',
        ],
        expected_truth=(
            "Despite 11 SIGTERM + DSN unset, polyarb-l2 machine state stays 'started'. "
            "Watchdog hits storm cap (10/hour) and degrades to REST. Listener gracefully "
            "marks itself degraded. Alert dedup keeps Sentry event count ≤3 not 11. "
            "GHA supabase-keepalive next cron run unpauses (in real scenario; here we restore DSN manually)."
        ),
        cleanup_cmds=[
            'set -a; . ./.env; set +a; '
            'flyctl secrets set POLYARB_SUPABASE_DB_DSN="$POLYARB_SUPABASE_DB_DSN" -a polyarb-l2',
            # Wait for the kill-loop to terminate (it's backgrounded above).
            'wait',
        ],
    ),
    ChaosInjection(
        inj_id="L2-5",
        title="Data API /trades 429 → retry-with-backoff + checkpoint resume",
        code_path="src/polyarb/clients/data_api_client.py (Plan 06, 429 handler)",
        action_cmds=[
            # Trigger many backfill calls in tight loop — should hit 429 from
            # Polymarket Data API (rate-limit 15 RPS via AsyncLimiter(150,10),
            # but a tight loop on `make backfill-trades` can exceed).
            'for i in $(seq 1 30); do '
            '  flyctl ssh console -a polyarb-l2 -C "cd /app && python -c \\"'
            'import asyncio; from polyarb.clients.data_api_client import PolymarketDataApiClient; '
            'async def m(): '
            '  c = PolymarketDataApiClient(); '
            '  rows = await c.backfill_trades_for_asset(\\\\\\"53465512181802150755993130711224070738002100921790051090044528012833736167995\\\\\\", days=1); '
            '  print(len(rows)); '
            'asyncio.run(m())\\""; '
            'done',
        ],
        programmatic_cmds=[
            # 429 retry log lines visible in flyctl logs:
            'flyctl logs -a polyarb-l2 --no-tail | grep -c "status_code == 429\\|429.*backoff" || true',
            # Backfill checkpoint progress: rows added to l2_trades over time:
            'psql "$POLYARB_SUPABASE_DB_DSN" -tAc "SELECT count(*) FROM l2_trades WHERE created_at > now() - interval \'10 minutes\'"',
            # No daemon crash:
            'flyctl status -a polyarb-l2 | grep "started"',
        ],
        container_localhost_fallback=[
            'flyctl ssh console -a polyarb-l2 -C "curl -fsS localhost:8080/healthz"',
        ],
        expected_truth=(
            "Data API client receives 429 at least once; tenacity retry kicks in; "
            "backfill resumes and at least 1 l2_trades row gets persisted; daemon "
            "stays 'started' throughout. Natural recovery — Polymarket rate limit "
            "clears in ~10s, no manual cleanup."
        ),
        cleanup_cmds=[],
    ),
]


# ─── Invariant tests (Phase 02.1 D-09 + L8 enforcement) ───────────────────


def test_l2_chaos_plan_has_5_entries() -> None:
    """Sanity: VALIDATION.md mandates exactly 5 chaos injections."""
    assert len(L2_CHAOS_PLAN) == 5, (
        f"Phase 03 VALIDATION.md locks 5 chaos injections; "
        f"L2_CHAOS_PLAN has {len(L2_CHAOS_PLAN)}"
    )


def test_l2_chaos_plan_ids_are_unique_and_match_spec() -> None:
    """IDs must be L2-1..L2-5 (no L2-6, no duplicates)."""
    expected = {"L2-1", "L2-2", "L2-3", "L2-4", "L2-5"}
    actual = {inj.inj_id for inj in L2_CHAOS_PLAN}
    assert actual == expected, f"Inj IDs drift: expected={expected}, got={actual}"


@pytest.mark.parametrize("inj", L2_CHAOS_PLAN, ids=lambda i: i.inj_id)
def test_every_injection_has_programmatic_verification(inj: ChaosInjection) -> None:
    """Phase 02.1 D-09: every truth is verified via shell command, not UI clicks.

    A `programmatic_cmds` entry is anything Claude can run in a Bash tool
    and parse mechanically: curl, psql, flyctl logs, Sentry API. Empty
    list or a single "go check Sentry dashboard" comment fails.
    """
    assert inj.programmatic_cmds, (
        f"Inj {inj.inj_id} has empty programmatic_cmds — "
        f"Phase 02.1 D-09 forbids UI-only verification"
    )
    forbidden_substrings = ("open dashboard", "look at sentry ui", "check ui", "navigate to")
    for cmd in inj.programmatic_cmds:
        cmd_lower = cmd.lower()
        for bad in forbidden_substrings:
            assert bad not in cmd_lower, (
                f"Inj {inj.inj_id} has UI-bound verification: {cmd!r}"
            )


@pytest.mark.parametrize("inj", L2_CHAOS_PLAN, ids=lambda i: i.inj_id)
def test_every_injection_has_container_fallback(inj: ChaosInjection) -> None:
    """Phase 02.1 L8: when Fly proxy breaks, container-localhost is the only ground truth.

    Every Inj must expose at least one `flyctl ssh console -C "curl
    localhost:..."` form so verification works even if the public endpoint
    is the thing under attack.
    """
    assert inj.container_localhost_fallback, (
        f"Inj {inj.inj_id} has empty container_localhost_fallback — "
        f"Phase 02.1 L8 mandates a flyctl ssh localhost probe"
    )
    has_ssh_localhost = any(
        "flyctl ssh" in cmd and "localhost" in cmd
        for cmd in inj.container_localhost_fallback
    )
    assert has_ssh_localhost, (
        f"Inj {inj.inj_id} container_localhost_fallback does not contain a "
        f"`flyctl ssh ... localhost:...` form (Phase 02.1 L8)"
    )


@pytest.mark.parametrize("inj", L2_CHAOS_PLAN, ids=lambda i: i.inj_id)
def test_every_injection_documents_cleanup(inj: ChaosInjection) -> None:
    """A chaos plan without explicit cleanup is a chaos plan that pollutes prod.

    Empty cleanup_cmds is acceptable IFF expected_truth explicitly says
    'natural recovery' (matching the L2-1, L2-5 cases in VALIDATION.md
    where Fly auto-restart or rate-limit clearance suffices).
    """
    if not inj.cleanup_cmds:
        truth_lower = inj.expected_truth.lower()
        assert (
            "natural" in truth_lower
            or "auto-restart" in truth_lower
            or "auto" in truth_lower
            or "rate limit" in truth_lower
        ), (
            f"Inj {inj.inj_id} has no cleanup_cmds AND no natural-recovery "
            f"language in expected_truth — explicit cleanup required"
        )


def test_listener_recovery_make_target_is_image_aware_and_has_two_modes() -> None:
    """Phase 05.1: one stable entry proves listener and poll recovery separately."""
    makefile = (ROOT / "Makefile").read_text()
    script = (ROOT / "scripts/chaos_l2_listener_recovery.py").read_text()

    assert "## chaos-l2-listener-recovery:" in makefile
    assert "chaos-l2-listener-recovery:" in makefile
    assert "--mode $(mode)" in makefile
    assert 'choices=("listener", "poll")' in script

    # The production image is python:3.12-slim. The new primitive may not rely
    # on binaries known to be absent there.
    for unavailable in ("pkill", " ps ", " dig ", " ping ", " which "):
        assert f'"{unavailable.strip()}"' not in script
        assert f"'{unavailable.strip()}'" not in script


def test_poll_recovery_contract_uses_an_exact_notification_anchor_and_restores_l1() -> None:
    """Timer proof is invalid unless NOTIFY stays exactly unchanged and is restored."""
    script = (ROOT / "scripts/chaos_l2_listener_recovery.py").read_text()

    assert "event_bus:last_notification_at" in script
    assert "notification_after_poll != notification_before" in script
    assert "POLYARB_EVENT_BUS_ENABLED=0" in script
    assert "POLYARB_EVENT_BUS_ENABLED=1" in script
    assert "MAX_RECOVERY_SECONDS = 180" in script
    assert "POLL_PROOF_SECONDS = 60" in script
    assert "no L2 restart" in script
