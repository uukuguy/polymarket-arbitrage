#!/usr/bin/env python3
"""snapshot-status — one-glance status of the snapshot pipeline.

Shows:
  - Currently running snapshot process (PID + elapsed)
  - Recent SQLite snapshot rows (CST local time)
  - Latest Parquet file on disk

Read-only. Pure stdlib (no polyarb imports — works even if src/ is mid-edit).
"""
from __future__ import annotations

import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

DB_PATH = Path("data/state.db")
PARQUET_ROOT = Path("data/snapshots")


def _local_str(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def _humanize_seconds(s: float) -> str:
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60:02d}s"
    return f"{s // 3600}h {(s % 3600) // 60:02d}m"


def show_running() -> None:
    """Detect a live `python -m polyarb.snapshot` process via ps -axo."""
    print("CURRENT:")
    # Try `ps -axo pid,etimes,command` first. Some sandboxes / minimal envs
    # restrict `ps` — fall through silently and tell the user how to check
    # manually instead of pretending nothing is running.
    rows: list[tuple[str, int, str]] = []
    ps_failed = False
    try:
        ps_out = subprocess.run(
            ["ps", "-axo", "pid,etimes,command"],
            capture_output=True,
            text=True,
            check=True,
            timeout=3,
        ).stdout
        for line in ps_out.splitlines():
            if "polyarb.snapshot" not in line:
                continue
            if "snapshot_status" in line or "grep" in line:
                continue
            parts = line.strip().split(None, 2)
            if len(parts) < 3:
                continue
            pid_s, etime_s, cmd = parts
            try:
                elapsed = int(etime_s)
            except ValueError:
                continue
            rows.append((pid_s, elapsed, cmd))
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        ps_failed = True

    if ps_failed:
        print("  (cannot inspect processes here — try in your shell:")
        print("     ps aux | grep polyarb.snapshot | grep -v grep)")
    elif not rows:
        print("  (no snapshot process running)")
    else:
        for pid_s, elapsed, cmd in rows:
            print(
                f"  PID {pid_s} running for {_humanize_seconds(elapsed)} — {cmd[:80]}"
            )
    print()


def show_recent_snapshots(limit: int = 5) -> None:
    """List the most recent SQLite snapshot rows."""
    print(f"RECENT SNAPSHOTS (last {limit}, local time):")
    if not DB_PATH.exists():
        print(f"  (no DB at {DB_PATH})")
        print()
        return

    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    except sqlite3.OperationalError as e:
        print(f"  (DB open failed: {e})")
        print()
        return

    try:
        cur = con.execute(
            "SELECT id, taken_at_ms, finished_at_ms, mode, market_count, "
            "is_valid, parquet_path FROM snapshots ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
    except sqlite3.OperationalError as e:
        print(f"  (no `snapshots` table yet: {e})")
        con.close()
        print()
        return
    finally:
        con.close()

    if not rows:
        print("  (no snapshots in DB yet)")
        print()
        return

    for sid, taken, finished, mode, count, valid, pq in rows:
        duration = (finished - taken) / 1000
        valid_str = "valid" if valid else "INVALID"
        pq_short = Path(pq).name if pq else "—"
        print(
            f"  id={sid:<3} {_local_str(taken)}  {_humanize_seconds(duration):>7}  "
            f"{count:>6} markets  mode={mode}  {valid_str:7}  {pq_short}"
        )
    print()


def show_latest_parquet() -> None:
    """Find the most recently modified parquet under data/snapshots/."""
    print("LATEST PARQUET ON DISK:")
    if not PARQUET_ROOT.exists():
        print(f"  ({PARQUET_ROOT} does not exist)")
        print()
        return

    pqs = list(PARQUET_ROOT.rglob("*.parquet"))
    if not pqs:
        print(f"  (no .parquet files under {PARQUET_ROOT})")
        print()
        return

    latest = max(pqs, key=lambda p: p.stat().st_mtime)
    st = latest.stat()
    mtime_local = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    size_mb = st.st_size / (1024 * 1024)
    age_s = (datetime.now() - datetime.fromtimestamp(st.st_mtime)).total_seconds()
    print(
        f"  {latest}  ({size_mb:.1f} MB, written {mtime_local}, "
        f"{_humanize_seconds(age_s)} ago)"
    )
    print()


def main() -> int:
    print()
    show_running()
    show_recent_snapshots()
    show_latest_parquet()
    return 0


if __name__ == "__main__":
    sys.exit(main())
