"""Read-only operator status for bounded opportunity Discovery."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

from polyarb.perception.store import OpportunityPerceptionStore


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="perception-discovery-status")
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--now-ms", type=int)
    return parser


def _decimal(value: Decimal) -> str:
    return format(value, "f")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    now_ms = args.now_ms if args.now_ms is not None else int(time.time() * 1_000)
    try:
        if now_ms < 0:
            raise ValueError("invalid-now")
        store = OpportunityPerceptionStore(args.db_path, read_only=True)
        status = store.discovery_status(now_ms)
    except Exception as error:
        print(
            f"invalid discovery state: {type(error).__name__}",
            file=sys.stderr,
        )
        return 2

    payload = {
        "cursor": status.next_cursor,
        "completed": status.completed,
        "last_batch": {
            "started_at_ms": status.last_started_at_ms,
            "finished_at_ms": status.last_finished_at_ms,
            "page_event_count": status.page_event_count,
            "groups_seen": status.groups_seen,
            "promoted_count": status.promoted_count,
        },
        "queue_depth_by_class": status.queue_depth_by_class,
        "oldest_visit_age_ms": status.oldest_visit_age_ms,
        "load_control": {
            "degraded_streak": status.load_state.degraded_streak,
            "last_reason": status.load_state.last_reason,
            "last_decision": status.load_state.last_decision,
            "probe_every_cycles": status.load_state.probe_every_cycles,
            "updated_at_ms": status.load_state.updated_at_ms,
        },
        "admission_control": (
            None
            if status.admission_proof is None
            else {
                "effective_capacity": (
                    status.admission_proof.effective_capacity
                ),
                "candidate_max_wait_ms": (
                    status.admission_proof.candidate_max_wait_ms
                ),
                "selection_budget_ms": (
                    status.admission_proof.selection_budget_ms
                ),
                "effective_start_bound_ms": (
                    status.admission_proof.effective_start_bound_ms
                ),
                "poll_interval_ms": status.admission_proof.poll_interval_ms,
                "group_timeout_ms": status.admission_proof.group_timeout_ms,
                "terminal_write_budget_ms": (
                    status.admission_proof.terminal_write_budget_ms
                ),
                "high_burst_groups": (
                    status.admission_proof.high_burst_groups
                ),
                "reserved_non_high_slots": (
                    status.admission_proof.reserved_non_high_slots
                ),
                "promotion_queue_depth": status.promotion_queue_depth,
                "outstanding_admitted_count": (
                    status.outstanding_admitted_count
                ),
            }
        ),
        "candidate_start_control": {
            "attempt_start_count": status.candidate_attempt_start_count,
            "deadline_breach_count": (
                status.candidate_start_deadline_breach_count
            ),
            "ready": status.candidate_start_ready,
        },
        "known_groups": status.coverage.known_groups,
        "coverage": {
            str(minutes): {
                "visited_groups": window.visited_groups,
                "raw_fraction": _decimal(window.raw_fraction),
                "liquidity_weighted_fraction": _decimal(
                    window.liquidity_weighted_fraction
                ),
            }
            for minutes, window in status.coverage.by_minutes.items()
        },
    }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
