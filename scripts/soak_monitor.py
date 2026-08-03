#!/usr/bin/env python3
"""Soak audit tool: pulls Better Stack 7-day history, summarizes uptime/incidents.

This script does NOT run continuously — Better Stack monitor (Plan 05) is the
real 7×24 prober (云端). This is a one-shot status / export tool.

W9 fix (revision 2026-05-12): Re-positioned from "local 7-day foreground daemon"
to "Better Stack history aggregator". The cloud-native architecture
(architecture_cloud-native-deployment.md) requires 7×24 monitoring to run on
the cloud (Better Stack public probe), NOT locally on a laptop.

Usage:
    uv run python scripts/soak_monitor.py status     # print current uptime % + incident count
    uv run python scripts/soak_monitor.py export     # dump 7-day history to 02-SOAK-LOG.md

Environment variables:
    BETTERSTACK_API_TOKEN   — Better Stack API token (required)
    BETTERSTACK_MONITOR_ID  — Better Stack monitor ID (required for status/export)

Phase 02 soak completion flow (no local long-running daemon):
  1. Plan 04 deployed → Plan 05 configured Better Stack monitor (30s cloud ping)
  2. Wait 7 days (user does nothing — Better Stack runs on cloud 7×24)
  3. Day 7: `make soak-status` → see 7-day uptime + incident count
  4. Day 7: `make soak-export` → audit trail written to 02-SOAK-LOG.md
"""
from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import typer

app = typer.Typer(no_args_is_help=True, help=__doc__)

# Path to the soak log (relative to project root — script is called from root)
LOG_PATH = Path(
    ".planning/workstreams/m1-perception/phases/02-l1-production-grade/02-SOAK-LOG.md"
)

BS_API = "https://uptime.betterstack.com/api/v2"

# Phase 02 soak gate threshold (thread §1 生产级判定标准)
SOAK_GATE_UPTIME_PCT = 99.0


def _headers() -> dict:
    token = os.environ.get("BETTERSTACK_API_TOKEN")
    if not token:
        typer.echo("ERROR: BETTERSTACK_API_TOKEN env var required", err=True)
        raise typer.Exit(code=1)
    return {"Authorization": f"Bearer {token}"}


def _fetch_sla(monitor_id: str, days: int = 7) -> dict:
    """Fetch SLA summary from Better Stack for the last `days` days."""
    from_dt = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    url = f"{BS_API}/monitors/{monitor_id}/sla?from={from_dt}"
    try:
        r = httpx.get(url, headers=_headers(), timeout=30.0)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        typer.echo(f"ERROR: Better Stack API returned {e.response.status_code}: {e}", err=True)
        raise typer.Exit(code=1) from e
    except httpx.RequestError as e:
        typer.echo(f"ERROR: Could not reach Better Stack API: {e}", err=True)
        raise typer.Exit(code=1) from e


def _parse_sla(sla_data: dict) -> tuple[float, int]:
    """Extract (availability_pct, incident_count) from Better Stack SLA response."""
    attrs = sla_data.get("data", {}).get("attributes", {})
    availability = float(attrs.get("availability", 0))
    total_incidents = int(attrs.get("total_incidents", 0))
    return availability, total_incidents


@app.command()
def status(
    monitor_id: str = typer.Option(
        ...,
        envvar="BETTERSTACK_MONITOR_ID",
        help="Better Stack monitor ID (from dashboard URL or API)",
    ),
    days: int = typer.Option(7, help="Window in days to check (default: 7)"),
) -> None:
    """Print Better Stack uptime % + incident count for last N days.

    Exit code:
      0 — gate PASS (uptime >= 99%)
      1 — gate FAIL (uptime < 99%) OR API error
    """
    sla_data = _fetch_sla(monitor_id, days)
    availability, total_incidents = _parse_sla(sla_data)

    gate_pass = availability >= SOAK_GATE_UPTIME_PCT
    gate_marker = "PASS" if gate_pass else "FAIL"

    typer.echo(f"Better Stack {days}-day SLA summary:")
    typer.echo(f"  Uptime:    {availability:.3f}%")
    typer.echo(f"  Incidents: {total_incidents}")
    typer.echo("")
    typer.echo(
        f"Phase 02 soak gate (thread §1 生产级判定标准): "
        f"uptime >= {SOAK_GATE_UPTIME_PCT}% — [{gate_marker}]"
    )

    if not gate_pass:
        typer.echo(
            f"\nGATE FAIL: uptime {availability:.3f}% < {SOAK_GATE_UPTIME_PCT}%. "
            "Extend soak window or investigate incidents.",
            err=True,
        )
        raise typer.Exit(code=1)


@app.command()
def export(
    monitor_id: str = typer.Option(
        ...,
        envvar="BETTERSTACK_MONITOR_ID",
        help="Better Stack monitor ID",
    ),
    days: int = typer.Option(7, help="Window in days to export (default: 7)"),
) -> None:
    """Dump Better Stack history to 02-SOAK-LOG.md as Phase 02 audit trail.

    Appends a timestamped section to the soak log with:
    - Export timestamp
    - Window duration
    - Uptime percentage
    - Incident count
    - Raw SLA response JSON
    """
    sla_data = _fetch_sla(monitor_id, days)
    availability, total_incidents = _parse_sla(sla_data)
    gate_pass = availability >= SOAK_GATE_UPTIME_PCT
    gate_marker = "PASS" if gate_pass else "FAIL"

    export_ts = datetime.now(UTC).isoformat()

    if not LOG_PATH.exists():
        typer.echo(
            f"WARNING: {LOG_PATH} does not exist — creating it.",
            err=True,
        )
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    with LOG_PATH.open("a", encoding="utf-8") as log:
        log.write(f"\n## Soak audit export — {export_ts}\n\n")
        log.write(f"- **Window**: last {days} days\n")
        log.write(f"- **Uptime**: {availability:.3f}%\n")
        log.write(f"- **Incidents**: {total_incidents}\n")
        log.write(
            f"- **Gate ({SOAK_GATE_UPTIME_PCT}% threshold)**: [{gate_marker}]\n"
        )
        log.write(f"\n### Raw SLA response\n\n```json\n{sla_data}\n```\n\n")

    typer.echo(
        f"Wrote {days}-day audit to {LOG_PATH} "
        f"(uptime={availability:.3f}%, incidents={total_incidents}, gate=[{gate_marker}])"
    )


if __name__ == "__main__":
    app()
