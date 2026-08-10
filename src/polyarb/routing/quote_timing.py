"""One source of truth for the production Quote freshness budget."""

QUOTE_AGE_SLA_SECONDS = 300.0
QUOTE_PUBLISH_RESERVE_SECONDS = 30.0
QUOTE_CHILD_SHUTDOWN_RESERVE_SECONDS = 2.0


def bounded_quote_supervisor_timeout_s(
    supervisor_timeout_s: float,
    interval_s: float,
) -> float:
    """Keep one Quote pipeline inside the freshness SLA, including its tail."""
    return min(
        float(supervisor_timeout_s),
        QUOTE_AGE_SLA_SECONDS - float(interval_s) - 1.0,
    )
