"""Versioned canonical authentication message for fault-domain control."""

from __future__ import annotations

import hashlib


def fault_hmac_message(
    *,
    timestamp: str,
    nonce: str,
    method: str,
    path: str,
    body: bytes,
) -> bytes:
    return b"\n".join(
        (
            b"polyarb-fault-v2",
            timestamp.encode("ascii"),
            nonce.encode("ascii"),
            method.encode("ascii"),
            path.encode("ascii"),
            hashlib.sha256(body).hexdigest().encode("ascii"),
        )
    )
