"""Polywatch — healthz-watcher MVP.

Polls polyarb-l1 + polyarb-l2 /healthz, decides whether to escalate
(Telegram push + optional L1 auto-unpause).

Run modes
---------
- GHA cron (every 15 min): set POLYWATCH_DRY_RUN=0 (default)
- Local smoke: POLYWATCH_DRY_RUN=1 (prints decisions, no Telegram/unpause)

Exit codes
----------
- 0: all healthy OR escalation completed cleanly (success path covers both)
- 1: escalation attempted but failed (e.g. Telegram down) — GHA email fallback

Decision rules
--------------
- L1 /healthz status == "fail" AND snapshot.last_success_age_seconds > 1800
    → attempt POST /control/unpause (HMAC signed, empty body)
    → push Telegram with Sentry link
- L1 /healthz status == "warn"
    → log only (do not push; warn is expected during snapshot in progress)
- L2 /healthz status == "fail"
    → push Telegram (no auto-action — L2 has no equivalent to unpause)
- L2 /healthz status == "warn" AND ws:last_event_age_seconds > 600
    → push Telegram (10 min WS silence is meaningful)
- Otherwise → no action

Safety
------
- All HTTP calls timeout=10s
- Telegram failures are logged but do not raise (alert chain best-effort)
- Auto-unpause is idempotent (already_running response is OK)
- No retry loops (cron will re-fire in 15 min)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

L1_HEALTHZ = os.environ.get("POLYWATCH_L1_HEALTHZ", "https://polyarb-l1.fly.dev/healthz")
L2_HEALTHZ = os.environ.get("POLYWATCH_L2_HEALTHZ", "https://polyarb-l2.fly.dev/healthz")
L1_UNPAUSE = os.environ.get("POLYWATCH_L1_UNPAUSE", "https://polyarb-l1.fly.dev/control/unpause")

# Thresholds (seconds)
L1_SNAPSHOT_FAIL_AGE_S = int(os.environ.get("POLYWATCH_L1_SNAPSHOT_FAIL_AGE_S", "1800"))   # 30 min
L2_WS_SILENCE_S = int(os.environ.get("POLYWATCH_L2_WS_SILENCE_S", "600"))                  # 10 min

DRY_RUN = os.environ.get("POLYWATCH_DRY_RUN", "0") == "1"

# Sentry issue link (well-known: SCHEDULER_PAUSED issue 121111789)
SENTRY_PAUSED_LINK = (
    "https://speechlessai.sentry.io/issues/121111789/?project=4511406009024592"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetch_json(url: str, *, timeout: float = 10.0) -> dict | None:
    """GET url, parse JSON. Return None on any failure (we don't want a fragile
    watcher; the watcher itself crashing is worse than a missed signal —
    GHA will re-fire in 15 min)."""
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[fetch] {url} failed: {e!r}", file=sys.stderr)
        return None


def _extract_check(healthz: dict, check_key: str, default=None):
    """healthz['checks'][check_key] is a list of {componentId, observedValue, status, ...}.
    Return the first entry's dict, or default."""
    checks = healthz.get("checks") or {}
    entries = checks.get(check_key) or []
    if not entries:
        return default
    return entries[0]


def _post_unpause() -> tuple[bool, str]:
    """POST /control/unpause with HMAC X-Signature. Empty body.
    Returns (ok, message_for_log)."""
    secret = os.environ.get("POLYARB_SCAN_SHARED_SECRET", "")
    if not secret:
        return False, "POLYARB_SCAN_SHARED_SECRET not set"

    if DRY_RUN:
        return True, "[DRY] would POST /control/unpause"

    body = b""
    sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    req = urllib.request.Request(
        L1_UNPAUSE,
        data=body,
        method="POST",
        headers={
            "X-Signature": f"sha256={sig}",
            "Content-Type": "application/json",
            "Content-Length": "0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return True, f"unpause response: {payload}"
    except Exception as e:
        return False, f"unpause POST failed: {e!r}"


def _send_telegram(text: str) -> bool:
    """Send a single Telegram message. Returns True on 2xx, False otherwise.
    Failures are logged but not raised."""
    token = os.environ.get("POLYARB_TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("POLYARB_TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("[telegram] token/chat_id missing — skipping push", file=sys.stderr)
        return False

    if DRY_RUN:
        print(f"[DRY telegram] {text}")
        return True

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            return 200 <= resp.status < 300
    except Exception as e:
        print(f"[telegram] send failed: {e!r}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# Decision
# ---------------------------------------------------------------------------


def decide_l1(healthz: dict | None) -> tuple[str, str]:
    """Return (action, reason). action ∈ {'noop', 'push', 'unpause+push'}.

    Look at the snapshot:last_success_age_seconds sub-check, NOT the top-level
    status — top-level is 'warn' whenever any sub-check is warn (e.g. R2 archive),
    but we only care about scheduler health here. Other sub-checks (r2, supabase
    mirror) have their own follow-up channels.
    """
    if not healthz:
        return "push", "L1 /healthz unreachable (network or daemon down)"

    top_status = healthz.get("status", "unknown")
    snap = _extract_check(healthz, "snapshot:last_success_age_seconds", {})
    snap_status = snap.get("status") if snap else None
    age = snap.get("observedValue") if snap else None

    if snap_status == "fail" and isinstance(age, (int, float)) and age > L1_SNAPSHOT_FAIL_AGE_S:
        return "unpause+push", f"L1 snapshot stale {int(age)}s > {L1_SNAPSHOT_FAIL_AGE_S}s (likely PAUSED)"

    if snap_status == "fail":
        return "push", f"L1 snapshot sub-check fail (age={age})"

    return "noop", f"L1 ok (top={top_status}, snapshot_status={snap_status}, age={age})"


def decide_l2(healthz: dict | None) -> tuple[str, str]:
    """Return (action, reason). action ∈ {'noop', 'push'}."""
    if not healthz:
        return "push", "L2 /healthz unreachable"

    status = healthz.get("status", "unknown")
    if status == "fail":
        return "push", "L2 /healthz status=fail"

    ws = _extract_check(healthz, "ws:last_event_age_seconds", {})
    ws_age = ws.get("observedValue") if ws else None
    if status == "warn" and isinstance(ws_age, (int, float)) and ws_age > L2_WS_SILENCE_S:
        return "push", f"L2 WS silent {int(ws_age)}s > {L2_WS_SILENCE_S}s"

    return "noop", f"L2 ok (status={status}, ws_age={ws_age})"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"[polywatch] tick at {now_iso} (DRY_RUN={DRY_RUN})")

    l1 = _fetch_json(L1_HEALTHZ)
    l2 = _fetch_json(L2_HEALTHZ)

    l1_action, l1_reason = decide_l1(l1)
    l2_action, l2_reason = decide_l2(l2)

    print(f"[polywatch] L1 → {l1_action}: {l1_reason}")
    print(f"[polywatch] L2 → {l2_action}: {l2_reason}")

    push_lines: list[str] = []
    escalation_ok = True

    if l1_action == "unpause+push":
        unpause_ok, unpause_msg = _post_unpause()
        print(f"[polywatch] unpause result: ok={unpause_ok} msg={unpause_msg}")
        push_lines.append(
            f"⚠️ <b>polyarb-l1 SCHEDULER_PAUSED</b>\n"
            f"reason: {l1_reason}\n"
            f"auto-unpause: {'sent ✓' if unpause_ok else 'FAILED ✗'}\n"
            f"detail: {unpause_msg}\n"
            f'<a href="{SENTRY_PAUSED_LINK}">Sentry issue</a>'
        )
        if not unpause_ok:
            escalation_ok = False
    elif l1_action == "push":
        push_lines.append(f"⚠️ <b>polyarb-l1 unhealthy</b>\n{l1_reason}")

    if l2_action == "push":
        push_lines.append(f"⚠️ <b>polyarb-l2 unhealthy</b>\n{l2_reason}")

    if push_lines:
        msg = f"🔔 polywatch — {now_iso}\n\n" + "\n\n".join(push_lines)
        tg_ok = _send_telegram(msg)
        if not tg_ok:
            escalation_ok = False
        print(f"[polywatch] telegram push: ok={tg_ok}")
    else:
        print("[polywatch] all green — no push")

    return 0 if escalation_ok else 1


if __name__ == "__main__":
    sys.exit(main())
