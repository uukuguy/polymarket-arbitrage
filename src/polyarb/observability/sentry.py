"""Sentry SDK initialization + before_send redact filter.

Plan 02-05 — D-15 (Sentry Developer Free) + T-02-08 (Sentry breadcrumb PII).

init_sentry(settings) must be called ONCE at daemon startup, AFTER init_logging
(so the LoguruIntegration can attach to the already-installed loguru sink) and
BEFORE any logger.info or scheduler/server task creation (so early exceptions
during settings load are captured).

Defence layers (in order applied to outgoing events):
  1. send_default_pii=False — sentry_sdk does NOT auto-attach IP / user agent /
     cookies / request body when off. This alone covers the common case where
     a sentry_sdk.capture_exception() inside a Starlette route would otherwise
     include the request body.
  2. before_send hook — for fields sentry_sdk DOES attach (extra=, contexts=,
     tags=, breadcrumbs), we run through the same pattern + key-name redact
     filter the loguru sink uses (observability.redact). This catches secrets
     embedded in messages we (the developer) wrote into logger.exception(...)
     calls or extra= kwargs.

If settings.sentry_dsn is empty (the dev default), init_sentry logs a warning
and returns — no sentry_sdk.init call is made. This keeps local dev quiet
without forcing every developer to register a personal Sentry project.

Source references:
- docs.sentry.io/platforms/python/integrations/loguru
- docs.sentry.io/platforms/python/configuration/options/#before-send
- RESEARCH.md §10 (Sentry verification) + §12 T-02-08 mitigations
"""
from __future__ import annotations

from typing import Any

import sentry_sdk
from loguru import logger
from sentry_sdk.integrations.loguru import LoguruIntegration

from polyarb.config import Settings
from polyarb.observability.redact import _redact_obj, _redact_string


# ---------------------------------------------------------------------------
# before_send hook — strip secrets from outgoing Sentry events
# ---------------------------------------------------------------------------


def _before_send(event: dict[str, Any], hint: Any = None) -> dict[str, Any]:
    """Strip secrets from event before transmission to Sentry.

    Mutates event in place AND returns it. Sentry SDK accepts either contract.

    Coverage:
      - event["request"]: stripped through _redact_obj (request body + headers)
      - event["extra"]: stripped through _redact_obj (developer-attached kwargs)
      - event["contexts"]: stripped through _redact_obj (custom context dicts)
      - event["tags"]: stripped through _redact_obj (low-cardinality labels)
      - event["breadcrumbs"]["values"][i]["message"]: _redact_string
      - event["breadcrumbs"]["values"][i]["data"]: _redact_obj

    Returning None would drop the event entirely; we always return the
    redacted version so error visibility is preserved.
    """
    for key in ("request", "extra", "contexts", "tags"):
        if key in event:
            event[key] = _redact_obj(event[key])

    breadcrumbs = event.get("breadcrumbs")
    if isinstance(breadcrumbs, dict):
        values = breadcrumbs.get("values", [])
        for crumb in values:
            if not isinstance(crumb, dict):
                continue
            if "message" in crumb:
                crumb["message"] = _redact_string(crumb["message"])
            if "data" in crumb:
                crumb["data"] = _redact_obj(crumb["data"])

    return event


# ---------------------------------------------------------------------------
# init_sentry — daemon startup hook
# ---------------------------------------------------------------------------


def init_sentry(settings: Settings) -> None:
    """Initialize Sentry SDK with redact filter + loguru integration.

    No-op (with a warning log) when settings.sentry_dsn is empty. Production
    deploys must set POLYARB_SENTRY_DSN; local dev can leave it blank.

    Environment is auto-detected from app hostname: ``fly.dev`` → ``prod``,
    anything else → ``dev``. This keeps Sentry's environment filter useful
    without an extra env var to manage.
    """
    if not settings.sentry_dsn:
        logger.warning("sentry skipped — POLYARB_SENTRY_DSN not set")
        return

    # Auto-detect environment from any hostname-ish setting; default to dev
    # so a misconfigured prod doesn't pollute Sentry "dev" issues. We do NOT
    # currently track app_hostname in Settings — release_id is the closest
    # proxy ("v0.2.0-abc123" implies a real deploy).
    environment = "prod" if settings.release_id != "dev" else "dev"

    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        release=settings.release_id,
        environment=environment,
        # T-02-08: do NOT auto-attach request body / IP / cookies / user agent.
        send_default_pii=False,
        # L1 daemon is a batch job, not a request-handler app — no traces.
        traces_sample_rate=0.0,
        integrations=[
            # event_level=ERROR: only ERROR+ logs become Sentry issues.
            # level=ERROR: only ERROR+ logs become breadcrumbs.
            # This keeps the 5k-events/month free tier breathing room.
            LoguruIntegration(level="ERROR", event_level="ERROR"),
        ],
        before_send=_before_send,
    )
    logger.info(f"sentry initialized — release={settings.release_id} env={environment}")
