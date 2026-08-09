"""Read-only qualification for an isolated replacement SQLite volume."""

from __future__ import annotations

import argparse
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
    quote = health.get("checks", {}).get("quote_feed:last_complete_age_seconds", [])
    if (
        not isinstance(quote, list)
        or not quote
        or quote[0].get("status") != "pass"
        or not isinstance(quote[0].get("observedValue"), (int, float))
        or quote[0]["observedValue"] > 300
    ):
        return {"status": "rejected", "reason": "fresh-quote-required"}
    if _fetch_status(console_url) != 200:
        return {"status": "rejected", "reason": "console-unavailable"}
    base_url = console_url.removesuffix("/perception/console")
    incidents = _fetch_json(f"{base_url}/perception/incidents?limit=1")
    if incidents.get("open_count") != 0:
        return {"status": "rejected", "reason": "open-incidents"}
    verdict = {
        "status": "qualified",
        "release_id": expected_release_id,
        "backup_sha256": str(manifest["backup_sha256"]),
        "boot_id": str(health.get("bootId", "")),
    }
    output_path.write_text(json.dumps(verdict, sort_keys=True) + "\n")
    return verdict


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--health-url", required=True)
    parser.add_argument("--console-url", required=True)
    parser.add_argument("--expected-release", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    print(
        json.dumps(
            qualify(
                manifest_path=args.manifest,
                health_url=args.health_url,
                console_url=args.console_url,
                expected_release_id=args.expected_release,
                output_path=args.output,
            ),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
