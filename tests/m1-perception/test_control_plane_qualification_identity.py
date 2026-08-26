"""Qualification release/config identity contracts."""

from __future__ import annotations

import re

import pytest


def test_qualification_config_identity_is_canonical() -> None:
    from polyarb.control_plane.qualification_identity import (
        qualification_config_id,
        qualification_config_payload,
    )

    payload = qualification_config_payload(
        interval_seconds=30,
        batch_size=100,
        role_identity=("opportunity", "quote", "structure"),
        runtime_recovery_mode="observe-only",
        runtime_recovery_allowed_targets=(),
    )

    assert payload == {
        "batch_size": 100,
        "interval_seconds": 30,
        "max_gap_seconds": 900,
        "policy_version": "m1-rolling-qualification-v1",
        "required_seconds": 86400,
        "role_identity": ["opportunity", "quote", "structure"],
        "runtime_recovery_allowed_targets": [],
        "runtime_recovery_mode": "observe-only",
        "signature_budget": 3,
    }
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", qualification_config_id(payload))


def test_qualification_identity_from_env_accepts_exact_canonical_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane.qualification_identity import (
        qualification_config_id,
        qualification_config_payload,
        qualification_identity_from_env,
    )

    release_id = "0123456789abcdef0123456789abcdef01234567"
    payload = qualification_config_payload(
        interval_seconds=30,
        batch_size=100,
        role_identity=("opportunity", "quote", "structure"),
        runtime_recovery_mode="observe-only",
        runtime_recovery_allowed_targets=(
            "polyarb-worker-staging/fly-control-plane-quote-batch",
            "polyarb-worker-staging/fly-control-plane-coordinator",
            "polyarb-worker-staging/fly-control-plane-quote-batch",
        ),
    )
    config_id = qualification_config_id(payload)
    monkeypatch.setenv("POLYARB_QUALIFICATION_RELEASE_ID", release_id)
    monkeypatch.setenv("POLYARB_QUALIFICATION_CONFIG_ID", config_id)
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_ROLE_IDENTITY",
        "opportunity,quote,structure",
    )
    monkeypatch.setenv("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE", "observe-only")
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS",
        (
            "polyarb-worker-staging/fly-control-plane-quote-batch,"
            "polyarb-worker-staging/fly-control-plane-coordinator,"
            "polyarb-worker-staging/fly-control-plane-quote-batch"
        ),
    )

    identity = qualification_identity_from_env(interval_seconds=30.0, batch_size=100)

    assert identity.release_id == release_id
    assert identity.config_id == config_id
    assert identity.role_identity == ("opportunity", "quote", "structure")
    assert identity.config_payload["interval_seconds"] == 30
    assert identity.config_payload["runtime_recovery_allowed_targets"] == [
        "polyarb-worker-staging/fly-control-plane-coordinator",
        "polyarb-worker-staging/fly-control-plane-quote-batch",
    ]


def test_qualification_config_digest_changes_with_cadence_batch_and_allowlist() -> None:
    from polyarb.control_plane.qualification_identity import (
        qualification_config_id,
        qualification_config_payload,
    )

    def digest(
        *,
        interval_seconds: float = 30,
        batch_size: int = 100,
        targets: tuple[str, ...] = (),
    ) -> str:
        return qualification_config_id(
            qualification_config_payload(
                interval_seconds=interval_seconds,
                batch_size=batch_size,
                role_identity=("opportunity", "quote", "structure"),
                runtime_recovery_mode="observe-only",
                runtime_recovery_allowed_targets=targets,
            )
        )

    baseline = digest()

    assert digest(interval_seconds=60) != baseline
    assert digest(batch_size=50) != baseline
    assert digest(targets=("polyarb-worker-staging/fly-control-plane-coordinator",)) != baseline


@pytest.mark.parametrize(
    ("env_name", "env_value", "message"),
    [
        (
            "POLYARB_QUALIFICATION_RELEASE_ID",
            "0123456789abcdef0123456789abcdef0123456X",
            "release-invalid",
        ),
        (
            "POLYARB_QUALIFICATION_RELEASE_ID",
            "0123456789abcdef0123456789abcdef0123456",
            "release-invalid",
        ),
        (
            "POLYARB_QUALIFICATION_ROLE_IDENTITY",
            "quote,opportunity,structure",
            "roles-invalid",
        ),
        (
            "POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE",
            "active",
            "recovery-mode-invalid",
        ),
        (
            "POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS",
            "polyarb-worker/${MACHINE}",
            "targets-invalid",
        ),
    ],
)
def test_qualification_identity_from_env_rejects_unknown_or_mismatched_values(
    monkeypatch: pytest.MonkeyPatch,
    env_name: str,
    env_value: str,
    message: str,
) -> None:
    from polyarb.control_plane.qualification_identity import (
        QualificationIdentityError,
        qualification_config_id,
        qualification_config_payload,
        qualification_identity_from_env,
    )

    payload = qualification_config_payload(
        interval_seconds=30,
        batch_size=100,
        role_identity=("opportunity", "quote", "structure"),
        runtime_recovery_mode="observe-only",
        runtime_recovery_allowed_targets=(),
    )
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_RELEASE_ID",
        "0123456789abcdef0123456789abcdef01234567",
    )
    monkeypatch.setenv("POLYARB_QUALIFICATION_CONFIG_ID", qualification_config_id(payload))
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_ROLE_IDENTITY",
        "opportunity,quote,structure",
    )
    monkeypatch.setenv("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE", "observe-only")
    monkeypatch.setenv("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS", "")
    monkeypatch.setenv(env_name, env_value)

    with pytest.raises(QualificationIdentityError, match=message):
        qualification_identity_from_env(interval_seconds=30.0, batch_size=100)


def test_qualification_identity_from_env_rejects_non_integral_cadence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane.qualification_identity import (
        QualificationIdentityError,
        qualification_config_id,
        qualification_config_payload,
        qualification_identity_from_env,
    )

    payload = qualification_config_payload(
        interval_seconds=30,
        batch_size=100,
        role_identity=("opportunity", "quote", "structure"),
        runtime_recovery_mode="observe-only",
        runtime_recovery_allowed_targets=(),
    )
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_RELEASE_ID",
        "0123456789abcdef0123456789abcdef01234567",
    )
    monkeypatch.setenv("POLYARB_QUALIFICATION_CONFIG_ID", qualification_config_id(payload))
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_ROLE_IDENTITY",
        "opportunity,quote,structure",
    )
    monkeypatch.setenv("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE", "observe-only")
    monkeypatch.setenv("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS", "")

    with pytest.raises(QualificationIdentityError, match="interval-invalid"):
        qualification_identity_from_env(interval_seconds=30.5, batch_size=100)


def test_qualification_identity_from_env_rejects_config_digest_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane.qualification_identity import (
        QualificationIdentityError,
        qualification_identity_from_env,
    )

    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_RELEASE_ID",
        "0123456789abcdef0123456789abcdef01234567",
    )
    monkeypatch.setenv("POLYARB_QUALIFICATION_CONFIG_ID", "sha256:" + "0" * 64)
    monkeypatch.setenv(
        "POLYARB_QUALIFICATION_ROLE_IDENTITY",
        "opportunity,quote,structure",
    )
    monkeypatch.setenv("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE", "observe-only")
    monkeypatch.setenv("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS", "")

    with pytest.raises(QualificationIdentityError, match="config-mismatch"):
        qualification_identity_from_env(interval_seconds=30.0, batch_size=100)
