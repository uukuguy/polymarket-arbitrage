"""Shared secret-redaction primitives for loguru filter + Sentry before_send.

Plan 02-05 — T-02-07 (loguru log leakage) + T-02-08 (Sentry breadcrumb leakage).

Two layers of defence:
  1. Pattern-based: regex over a string for Bearer / token= / secret= / api_key=
     / sk-* / JWT triplet → replace the secret value with [REDACTED].
  2. Key-name-based: for dict-shaped data (extras, request body, breadcrumb
     data), if the *key* matches a known sensitive Settings field name, replace
     the value wholesale with [REDACTED] regardless of pattern.

Layered approach matters: pattern-based catches secrets embedded in free-text
log messages; key-name based catches secrets that don't yet match a pattern
(e.g. opaque 8-char alphanumeric Telegram chat tokens). Either layer alone
leaks; both together cover the realistic blast radius.

Lives in its own module to avoid circular imports between
``observability.sentry`` (uses for Sentry before_send) and
``observability.logging`` (uses for loguru filter).
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Pattern layer — substring regex over free text
# ---------------------------------------------------------------------------
#
# Each pattern has exactly one capture group: the secret-value portion.
# _redact_string replaces the captured group with the placeholder, preserving
# the surrounding prefix ("Bearer ", "token=", ...) so the log line stays
# diagnostic-friendly ("Bearer [REDACTED]" tells you it WAS a bearer token).

_SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Bearer <opaque-token>  (HTTP Authorization)
    re.compile(r"(Bearer\s+)([A-Za-z0-9\-_\.]{6,})"),
    # token=... / secret=... / key=... / api_key=... / access_key=...
    # Accepts optional surrounding quotes; matches `=` or `:` separator.
    re.compile(
        r"((?:token|secret|key|api_key|access_key|service_key)\s*[=:]\s*\"?)"
        r"([A-Za-z0-9\-_\.]{6,})",
        re.IGNORECASE,
    ),
    # JWT: three base64-url segments joined by dots, each ≥ 20 chars.
    # Match the whole JWT as a single redacted unit (no prefix to preserve).
    re.compile(r"(eyJ[A-Za-z0-9_\-]{10,})\.([A-Za-z0-9_\-]{10,})\.([A-Za-z0-9_\-]{10,})"),
    # Stripe-style sk-... openai-style keys.
    re.compile(r"(sk-)([A-Za-z0-9]{16,})"),
)
_TELEGRAM_BOT_PATH = re.compile(
    r"(?P<prefix>api\.telegram\.org/bot)"
    r"(?P<secret>[0-9]{5,}:[A-Za-z0-9_-]{10,})"
    r"(?P<suffix>/)",
    re.IGNORECASE,
)

REDACTED = "[REDACTED]"


def _redact_string(value: str) -> str:
    """Replace any secret-shaped substring with '[REDACTED]'.

    The PREFIX (e.g. "Bearer ", "token=") is preserved so logs stay diagnostic.
    JWTs and `sk-*` keys are matched as full tokens — for JWT, the entire
    `eyJ....`.`...`.`...` triplet is replaced; for `sk-...`, the prefix
    `sk-` is kept and the body becomes `[REDACTED]`.
    """
    if not isinstance(value, str):
        return value

    # Pattern 0 (Bearer): keep prefix, redact value
    result = _SECRET_PATTERNS[0].sub(lambda m: m.group(1) + REDACTED, value)
    # Pattern 1 (token=/secret=/key=/...): keep prefix incl. quote, redact value
    result = _SECRET_PATTERNS[1].sub(lambda m: m.group(1) + REDACTED, result)
    # Pattern 2 (JWT): replace entire 3-part token
    result = _SECRET_PATTERNS[2].sub(REDACTED, result)
    # Pattern 3 (sk-*): keep `sk-` prefix, redact body
    result = _SECRET_PATTERNS[3].sub(lambda m: m.group(1) + REDACTED, result)
    # Telegram requires its bot token in the URL path rather than a header.
    # Redact that provider-specific shape before httpx request logs leave the
    # process.
    result = _TELEGRAM_BOT_PATH.sub(
        lambda m: m.group("prefix") + REDACTED + m.group("suffix"),
        result,
    )
    return result


# ---------------------------------------------------------------------------
# Key-name layer — wholesale value replacement when key matches sensitive name
# ---------------------------------------------------------------------------

# Lowercase. We match on lowercased keys so case variations don't slip through.
SENSITIVE_KEY_NAMES: frozenset[str] = frozenset(
    {
        "api_key",
        "token",
        "secret",
        "service_key",
        "access_key",
        "secret_key",
        "telegram_bot_token",
        "sentry_dsn",
        "scan_shared_secret",
        "supabase_service_key",
        "r2_access_key_id",
        "r2_secret_access_key",
        "authorization",
        "password",
    }
)


def _is_sensitive_key(key: Any) -> bool:
    """True if the key (case-insensitive) matches a known sensitive Settings field."""
    if not isinstance(key, str):
        return False
    return key.lower() in SENSITIVE_KEY_NAMES


def _redact_obj(obj: Any) -> Any:
    """Recursively redact a dict / list / str.

    - Dict: each key is checked against SENSITIVE_KEY_NAMES. If sensitive,
      the value becomes "[REDACTED]" wholesale. Otherwise the value is
      recursed into.
    - List/tuple: each element is recursed into; the container type is
      preserved.
    - String: passed through _redact_string for pattern matching.
    - Anything else: returned as-is.
    """
    if isinstance(obj, str):
        return _redact_string(obj)
    if isinstance(obj, dict):
        return {k: (REDACTED if _is_sensitive_key(k) else _redact_obj(v)) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return type(obj)(_redact_obj(x) for x in obj)
    return obj
