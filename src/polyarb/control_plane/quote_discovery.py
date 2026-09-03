"""Bounded, explainable discovery evidence for current Quote research."""

from __future__ import annotations

import json
from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Mapping
from math import isfinite, log1p

_MAX_CURSOR_LENGTH = 256
_MEANINGFUL_NOTIONAL_USD = 10.0
_NON_NEUTRAL_PRICE_EXTREMITY_BPS = 1_500.0


def quote_discovery(payload: Mapping[str, object]) -> dict[str, object]:
    """Return research-priority evidence without making an opportunity claim."""
    if payload.get("terminal_state") != "executable":
        return _zero("not-executable")
    ask = _unit_interval_number(payload.get("best_ask_price"))
    size = _positive_number(payload.get("best_ask_size"))
    if ask is None or size is None:
        return _zero("missing-or-invalid-quote")
    notional = round(ask * size, 8)
    extremity = round(abs(ask - 0.5) * 10_000, 8)
    score = round(log1p(notional) * extremity, 8)
    reasons = [
        "meaningful-executable-depth"
        if notional >= _MEANINGFUL_NOTIONAL_USD
        else "insufficient-executable-depth"
    ]
    if extremity >= _NON_NEUTRAL_PRICE_EXTREMITY_BPS:
        reasons.append("non-neutral-yes-price")
    return {
        "executable_notional_usd": notional,
        "price_extremity_bps": extremity,
        "score": score,
        "reasons": reasons,
    }


def encode_discovery_cursor(score: float, notional: float, token_id: str) -> str:
    """Encode the stable sort position for one discovery row."""
    if not _non_negative_finite(score) or not _non_negative_finite(notional) or not token_id:
        raise ValueError("invalid-discovery-cursor")
    payload = json.dumps(
        {"notional": notional, "score": score, "token_id": token_id},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    encoded = urlsafe_b64encode(payload).decode().rstrip("=")
    if len(encoded) > _MAX_CURSOR_LENGTH:
        raise ValueError("invalid-discovery-cursor")
    return encoded


def decode_discovery_cursor(value: str) -> tuple[float, float, str] | None:
    """Decode an untrusted discovery cursor, returning no position when invalid."""
    if not value:
        return None
    if len(value) > _MAX_CURSOR_LENGTH:
        return None
    try:
        decoded = urlsafe_b64decode(value + "=" * (-len(value) % 4))
        payload = json.loads(decoded)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or set(payload) != {"notional", "score", "token_id"}:
        return None
    score, notional, token_id = payload["score"], payload["notional"], payload["token_id"]
    if not _non_negative_finite(score) or not _non_negative_finite(notional):
        return None
    if not isinstance(token_id, str) or not token_id:
        return None
    return float(score), float(notional), token_id


def _zero(reason: str) -> dict[str, object]:
    return {
        "executable_notional_usd": 0.0,
        "price_extremity_bps": 0.0,
        "score": 0.0,
        "reasons": [reason],
    }


def _unit_interval_number(value: object) -> float | None:
    if not _non_negative_finite(value):
        return None
    number = float(value)
    return number if number <= 1.0 else None


def _positive_number(value: object) -> float | None:
    if not _non_negative_finite(value):
        return None
    number = float(value)
    return number if number > 0 else None


def _non_negative_finite(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and isfinite(float(value))
        and float(value) >= 0
    )
