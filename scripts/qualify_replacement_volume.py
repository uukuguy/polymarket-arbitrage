"""Read-only qualification for an isolated replacement SQLite volume."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.request import urlopen


def _fetch_json(url: str) -> dict:
    with urlopen(url, timeout=10) as response:  # noqa: S310 - operator URL argument
        return json.loads(response.read())


def _fetch_status(url: str) -> int:
    with urlopen(url, timeout=10) as response:  # noqa: S310 - operator URL argument
        return int(response.status)


def qualify(
    *,
    manifest_path: Path,
    health_url: str,
    console_url: str,
    expected_release_id: str,
    output_path: Path,
) -> dict[str, str]:
    """Write an exclusive PASS only after release, integrity and console checks."""
    if output_path.exists():
        raise ValueError("qualification-output-already-exists")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("integrity_check") != "ok":
        return {"status": "rejected", "reason": "manifest-integrity-not-ok"}
    health = _fetch_json(health_url)
    if health.get("releaseId") != expected_release_id:
        return {"status": "rejected", "reason": "release-id-mismatch"}
    if _fetch_status(console_url) != 200:
        return {"status": "rejected", "reason": "console-unavailable"}
    verdict = {
        "status": "qualified",
        "release_id": expected_release_id,
        "backup_sha256": str(manifest["backup_sha256"]),
        "boot_id": str(health.get("bootId", "")),
    }
    output_path.write_text(json.dumps(verdict, sort_keys=True) + "\n")
    return verdict
