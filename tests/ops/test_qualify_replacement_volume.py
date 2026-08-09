from __future__ import annotations

import json


def test_qualification_rejects_release_mismatch(tmp_path, monkeypatch) -> None:
    from scripts import qualify_replacement_volume as qualify

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"backup_sha256": "a" * 64, "integrity_check": "ok"}))
    output = tmp_path / "verdict.json"

    def fetch(_url: str):
        return {"releaseId": "b" * 40, "bootId": "boot", "checks": {}}

    monkeypatch.setattr(qualify, "_fetch_json", fetch)

    verdict = qualify.qualify(
        manifest_path=manifest,
        health_url="https://replacement.example/healthz",
        console_url="https://replacement.example/perception/console",
        expected_release_id="a" * 40,
        output_path=output,
    )

    assert verdict["status"] == "rejected"
    assert verdict["reason"] == "release-id-mismatch"
    assert not output.exists()
