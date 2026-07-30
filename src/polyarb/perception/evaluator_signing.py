"""Strict Ed25519 authority boundary for upstream-fault verdicts."""

from __future__ import annotations

import base64
import re

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

SIGNATURE_VERSION = "ed25519-v1"
_KID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,31}")
_RAW_KEY_RE = re.compile(r"[A-Za-z0-9_-]{43}")
_SIGNATURE_RE = re.compile(r"[A-Za-z0-9_-]{86}")


def _decode_raw(value: str, *, signature: bool = False) -> bytes:
    pattern = _SIGNATURE_RE if signature else _RAW_KEY_RE
    if pattern.fullmatch(value) is None:
        raise ValueError("invalid-ed25519-encoding")
    try:
        raw = base64.urlsafe_b64decode(value + ("==" if signature else "="))
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid-ed25519-encoding") from exc
    expected = 64 if signature else 32
    if len(raw) != expected or _encode_raw(raw) != value:
        raise ValueError("invalid-ed25519-encoding")
    return raw


def _encode_raw(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _parse_key_spec(value: str) -> tuple[str, bytes]:
    parts = value.split(":")
    if (
        len(parts) != 3
        or parts[0] != SIGNATURE_VERSION
        or _KID_RE.fullmatch(parts[1]) is None
    ):
        raise ValueError("invalid-ed25519-key-spec")
    return parts[1], _decode_raw(parts[2])


def load_private_key(value: str) -> tuple[str, Ed25519PrivateKey]:
    kid, raw = _parse_key_spec(value)
    return kid, Ed25519PrivateKey.from_private_bytes(raw)


def load_public_key(value: str) -> tuple[str, Ed25519PublicKey]:
    kid, raw = _parse_key_spec(value)
    return kid, Ed25519PublicKey.from_public_bytes(raw)


def sign_digest(key_spec: str, digest: str) -> tuple[str, str]:
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("invalid-artifact-digest")
    kid, key = load_private_key(key_spec)
    return kid, _encode_raw(key.sign(digest.encode("ascii")))


def verify_digest(
    key_spec: str,
    *,
    kid: object,
    version: object,
    digest: str,
    signature: object,
) -> bool:
    try:
        expected_kid, key = load_public_key(key_spec)
        if (
            version != SIGNATURE_VERSION
            or kid != expected_kid
            or not isinstance(signature, str)
        ):
            return False
        key.verify(_decode_raw(signature, signature=True), digest.encode("ascii"))
        return True
    except (InvalidSignature, TypeError, ValueError):
        return False
