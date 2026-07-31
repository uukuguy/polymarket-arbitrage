"""Authenticated writer for bounded loopback HTTP recovery evidence."""

from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass

from polyarb.perception.store import OpportunityPerceptionStore

_HTTP_PROBE_WRITE_AUTHORITY = object()
_MAX_RESPONSE_BYTES = 65_536


@dataclass(frozen=True)
class HttpProbeResult:
    expected_release_id: str
    observed_release_id: str | None
    probe_nonce: str
    started_at_ms: int
    finished_at_ms: int
    responsive: bool


class BoundedHttpProbeWriter:
    def __init__(
        self,
        store: OpportunityPerceptionStore,
        *,
        timeout_s: float = 2.0,
        clock_ms=None,
    ) -> None:
        if not 0 < timeout_s <= 2.0:
            raise ValueError("invalid-http-probe-timeout")
        self._store = store
        self._timeout_s = timeout_s
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1_000))

    def probe(
        self,
        url: str,
        *,
        expected_release_id: str,
        probe_nonce: str,
    ) -> HttpProbeResult:
        if (
            not url.startswith("http://127.0.0.1:")
            or not expected_release_id
            or not probe_nonce
        ):
            raise ValueError("invalid-http-probe-target")
        started_at_ms = self._clock_ms()
        observed_release_id: str | None = None
        responsive = False
        try:
            request = urllib.request.Request(
                url,
                headers={"Accept": "application/health+json, application/json"},
            )
            # This is an authenticated loopback proof. Environment proxy
            # variables must not redirect it off-host or synthesize a response.
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(request, timeout=self._timeout_s) as response:
                body = response.read(_MAX_RESPONSE_BYTES + 1)
                if response.status == 200 and len(body) <= _MAX_RESPONSE_BYTES:
                    payload = json.loads(body.decode("utf-8"))
                    if isinstance(payload, dict):
                        observed = payload.get("releaseId")
                        if isinstance(observed, str):
                            observed_release_id = observed
                            responsive = observed == expected_release_id
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
            responsive = False
        finished_at_ms = max(started_at_ms, self._clock_ms())
        result = HttpProbeResult(
            expected_release_id=expected_release_id,
            observed_release_id=observed_release_id,
            probe_nonce=probe_nonce,
            started_at_ms=started_at_ms,
            finished_at_ms=finished_at_ms,
            responsive=responsive,
        )
        self._store._record_http_probe_result(
            result,
            _authority=_HTTP_PROBE_WRITE_AUTHORITY,
        )
        return result


__all__ = ["BoundedHttpProbeWriter", "HttpProbeResult"]
