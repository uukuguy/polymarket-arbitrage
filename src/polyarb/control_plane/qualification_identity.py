"""Canonical qualification release/config identity."""

from __future__ import annotations

import hmac
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import Final

from .qualification import RollingQualificationPolicy

_RELEASE_ID = re.compile(r"[0-9a-f]{40}")
_CONFIG_ID = re.compile(r"sha256:[0-9a-f]{64}")
_RECOVERY_TARGET = re.compile(r"[a-z0-9][a-z0-9-]{1,62}/[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_ORDERED_ROLES: Final[tuple[str, ...]] = ("opportunity", "quote", "structure")
_RECOVERY_MODE: Final[str] = "observe-only"


class QualificationIdentityError(ValueError):
    """Qualification release/config identity is missing or inconsistent."""


@dataclass(frozen=True, slots=True)
class QualificationIdentity:
    release_id: str
    config_id: str
    role_identity: tuple[str, ...]
    config_payload: Mapping[str, object]


def qualification_config_payload(
    *,
    interval_seconds: float,
    batch_size: int,
    role_identity: Sequence[str],
    runtime_recovery_mode: str,
    runtime_recovery_allowed_targets: Sequence[str],
) -> dict[str, object]:
    """Build the deterministic qualification config identity payload."""

    return {
        "batch_size": _positive_int(batch_size, field="batch_size"),
        "interval_seconds": _integral_interval(interval_seconds),
        "max_gap_seconds": 900,
        "policy_version": RollingQualificationPolicy.DEFAULT_POLICY_VERSION,
        "required_seconds": 86_400,
        "role_identity": list(_validate_roles(role_identity)),
        "runtime_recovery_allowed_targets": list(
            _validate_recovery_targets(runtime_recovery_allowed_targets)
        ),
        "runtime_recovery_mode": _validate_recovery_mode(runtime_recovery_mode),
        "signature_budget": 3,
    }


def qualification_config_id(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{sha256(encoded).hexdigest()}"


def qualification_identity_from_env(
    *,
    interval_seconds: float,
    batch_size: int,
) -> QualificationIdentity:
    release_id = _required_env("POLYARB_QUALIFICATION_RELEASE_ID")
    supplied_config_id = _required_env("POLYARB_QUALIFICATION_CONFIG_ID")
    roles = tuple(
        value.strip()
        for value in _required_env("POLYARB_QUALIFICATION_ROLE_IDENTITY").split(",")
        if value.strip()
    )
    mode = _required_env("POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE")
    targets = tuple(
        value.strip()
        for value in _required_env(
            "POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS",
            allow_empty=True,
        ).split(",")
        if value.strip()
    )
    if _RELEASE_ID.fullmatch(release_id) is None:
        raise QualificationIdentityError("qualification.identity.release-invalid")
    if _CONFIG_ID.fullmatch(supplied_config_id) is None:
        raise QualificationIdentityError("qualification.identity.config-invalid")
    payload = qualification_config_payload(
        interval_seconds=interval_seconds,
        batch_size=batch_size,
        role_identity=roles,
        runtime_recovery_mode=mode,
        runtime_recovery_allowed_targets=targets,
    )
    expected_config_id = qualification_config_id(payload)
    if not hmac.compare_digest(supplied_config_id, expected_config_id):
        raise QualificationIdentityError("qualification.identity.config-mismatch")
    return QualificationIdentity(
        release_id=release_id,
        config_id=supplied_config_id,
        role_identity=roles,
        config_payload=payload,
    )


def _required_env(name: str, *, allow_empty: bool = False) -> str:
    raw = os.environ.get(name)
    if raw is None:
        raise QualificationIdentityError(f"qualification.identity.missing:{name}")
    value = raw.strip()
    if not value and not allow_empty:
        raise QualificationIdentityError(f"qualification.identity.empty:{name}")
    return value


def _integral_interval(value: float) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise QualificationIdentityError("qualification.identity.interval-invalid")
    interval = float(value)
    if not interval.is_integer() or interval <= 0:
        raise QualificationIdentityError("qualification.identity.interval-invalid")
    return int(interval)


def _positive_int(value: int, *, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise QualificationIdentityError(f"qualification.identity.{field}-invalid")
    return value


def _validate_roles(values: Sequence[str]) -> tuple[str, ...]:
    roles = tuple(values)
    if roles != _ORDERED_ROLES:
        raise QualificationIdentityError("qualification.identity.roles-invalid")
    return roles


def _validate_recovery_mode(value: str) -> str:
    if value != _RECOVERY_MODE:
        raise QualificationIdentityError("qualification.identity.recovery-mode-invalid")
    return value


def _validate_recovery_targets(values: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(sorted(set(values)))
    for target in normalized:
        if not isinstance(target, str) or _RECOVERY_TARGET.fullmatch(target) is None:
            raise QualificationIdentityError("qualification.identity.targets-invalid")
        if any(marker in target for marker in ("$", "{", "}", "?")):
            raise QualificationIdentityError("qualification.identity.targets-invalid")
    return normalized
