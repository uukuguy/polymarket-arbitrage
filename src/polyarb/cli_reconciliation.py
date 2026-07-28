"""Operator entry points for one bounded reconciliation batch and status."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from polyarb.clients.gamma_client import GammaClient
from polyarb.config import load_settings
from polyarb.perception.reconciliation import ReconciliationWorker
from polyarb.perception.store import OpportunityPerceptionStore


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="reconcile-market-map")
    parser.add_argument("command", choices=("run", "status"))
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--page-limit", type=int)
    return parser


def _status(store: OpportunityPerceptionStore) -> dict:
    window = store.current_reconciliation()
    if window is None:
        return {"status": "idle"}
    payload = {
        "status": window.status,
        "window_id": window.id,
        "next_cursor": window.next_cursor,
        "started_at_ms": window.started_at_ms,
        "checkpoint_at_ms": window.checkpoint_at_ms,
        "finished_at_ms": window.finished_at_ms,
        "pages_completed": window.pages_completed,
        "events_seen": window.events_seen,
        "groups_staged": window.groups_staged,
        "rejected_count": window.rejected_count,
    }
    if window.status == "applied":
        payload["diff"] = {
            "added": window.added_count,
            "changed": window.changed_count,
            "closed": window.closed_count,
            "unchanged": window.unchanged_count,
            "rejected": window.applied_rejected_count,
            "started_at_ms": window.started_at_ms,
            "finished_at_ms": window.finished_at_ms,
        }
    return payload


async def _run(args) -> dict:
    settings = load_settings()
    db_path = settings.db_path if args.db_path is None else args.db_path
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    page_limit = settings.reconciliation_page_limit if args.page_limit is None else args.page_limit
    async with GammaClient(settings) as gamma:
        result = await ReconciliationWorker(
            gamma=gamma, store=store, page_limit=page_limit
        ).run_batch()
    payload = _status(store)
    payload["batch"] = {
        "requested_cursor": result.requested_cursor,
        "next_cursor": result.next_cursor,
        "completed": result.completed,
        "page_event_count": result.page_event_count,
        "groups_staged": result.groups_staged,
        "rejected_count": result.rejected_count,
    }
    if result.diff is not None:
        payload["diff"] = {
            "added": result.diff.added,
            "changed": result.diff.changed,
            "closed": result.diff.closed,
            "unchanged": result.diff.unchanged,
            "rejected": result.diff.rejected,
            "started_at_ms": result.diff.started_at_ms,
            "finished_at_ms": result.diff.finished_at_ms,
        }
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            if args.db_path is None:
                raise ValueError("--db-path-required-for-read-only-status")
            store = OpportunityPerceptionStore(args.db_path, read_only=True)
            payload = _status(store)
        else:
            payload = asyncio.run(_run(args))
    except Exception as error:
        print(
            f"reconciliation {args.command} failed: {type(error).__name__}",
            file=sys.stderr,
        )
        return 2
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
