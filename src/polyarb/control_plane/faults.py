"""Explicit, non-production control-plane acceptance fault signals."""

from __future__ import annotations


class IntentionalStagingRetryFault(RuntimeError):
    """A bounded staging-only failure already persisted as retryable work."""
