"""Secret-free identities for durable retry and circuit correlation."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path


def retry_failure_fingerprint(error: BaseException, *, component: str) -> str:
    """Hash exception type and innermost code site without hashing its message."""
    if not component:
        raise ValueError("failure identity component must be non-empty")
    traceback = error.__traceback__
    site = "no-traceback"
    while traceback is not None:
        frame = traceback.tb_frame
        site = f"{Path(frame.f_code.co_filename).name}:{frame.f_code.co_name}:{traceback.tb_lineno}"
        traceback = traceback.tb_next
    identity = f"{component}\0{type(error).__module__}.{type(error).__qualname__}\0{site}"
    return f"sha256:{sha256(identity.encode()).hexdigest()}"


__all__ = ["retry_failure_fingerprint"]
