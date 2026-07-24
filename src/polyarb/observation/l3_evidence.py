"""Immutable in-process records for L3 continuous-soak evidence."""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field, fields
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any
from uuid import UUID, uuid4


class PromoteStatus(StrEnum):
    SUCCESS = "success"
    FROZEN = "frozen"
    UNDERFILLED = "underfilled"
    FAILED = "failed"


class HealthStatus(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


class RuntimeEventSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class RuntimeEventKind(StrEnum):
    WATCHDOG_STALE = "watchdog_stale"
    RECONNECT_RESERVED = "reconnect_reserved"
    RECONNECT_DEFERRED = "reconnect_deferred"
    RECONNECT_STARTED = "reconnect_started"
    RECONNECT_SUCCEEDED = "reconnect_succeeded"
    RECONNECT_FAILED = "reconnect_failed"
    WS_GENERATION_CHANGED = "ws_generation_changed"
    SUBSCRIPTION_CONTROL_FAILED = "subscription_control_failed"
    SUBSCRIPTION_COMPENSATED = "subscription_compensated"
    EVIDENCE_WRITER_FAILED = "evidence_writer_failed"
    EVIDENCE_WRITER_RECOVERED = "evidence_writer_recovered"
    SHUTDOWN_SIGNAL = "shutdown_signal"
    SOAK_MANIFEST_BOUND = "soak_manifest_bound"
    CHECKPOINT_REPORT_BOUND = "checkpoint_report_bound"


_RUNTIME_EVENT_DETAIL_KEYS: Mapping[RuntimeEventKind, frozenset[str]] = MappingProxyType(
    {
        RuntimeEventKind.WATCHDOG_STALE: frozenset({"stale_seconds"}),
        RuntimeEventKind.RECONNECT_RESERVED: frozenset(
            {"reconnect_attempt", "budget_count"}
        ),
        RuntimeEventKind.RECONNECT_DEFERRED: frozenset(
            {"retry_after_ms", "budget_count"}
        ),
        RuntimeEventKind.RECONNECT_STARTED: frozenset({"source"}),
        RuntimeEventKind.RECONNECT_SUCCEEDED: frozenset({"source"}),
        RuntimeEventKind.RECONNECT_FAILED: frozenset({"operation", "error_type"}),
        RuntimeEventKind.WS_GENERATION_CHANGED: frozenset(
            {"previous_generation", "new_generation"}
        ),
        RuntimeEventKind.SUBSCRIPTION_CONTROL_FAILED: frozenset(
            {"operation", "error_type"}
        ),
        RuntimeEventKind.SUBSCRIPTION_COMPENSATED: frozenset(
            {"operation", "close_succeeded"}
        ),
        RuntimeEventKind.EVIDENCE_WRITER_FAILED: frozenset({"failed_event_seq"}),
        RuntimeEventKind.EVIDENCE_WRITER_RECOVERED: frozenset(
            {"recovered_event_seq"}
        ),
        RuntimeEventKind.SHUTDOWN_SIGNAL: frozenset({"signal"}),
        RuntimeEventKind.SOAK_MANIFEST_BOUND: frozenset({"manifest_sha256"}),
        RuntimeEventKind.CHECKPOINT_REPORT_BOUND: frozenset(
            {"checkpoint", "report_sha256"}
        ),
    }
)

_RUNTIME_EVENT_SOURCES = frozenset({"watchdog", "connection_initializer"})
_RUNTIME_EVENT_OPERATIONS = frozenset(
    {
        "on_reconnect",
        "initial_subscribe",
        "subscribe",
        "unsubscribe",
        "connection_close",
        "candidate_replace",
        "book_refresh",
    }
)
_RUNTIME_EVENT_ERROR_TYPES = frozenset(
    {
        "Exception",
        "RuntimeError",
        "ConnectionError",
        "TimeoutError",
        "OSError",
        "StateMismatch",
        "ControlRejected",
        "MissingHook",
        "NoActiveConnection",
    }
)
_RUNTIME_EVENT_SIGNALS = frozenset({"SIGINT", "SIGTERM"})
_RUNTIME_EVENT_CHECKPOINTS = frozenset({"T+0", "T+6", "T+12", "T+18", "T+24"})


def _invalid_event_detail(kind: RuntimeEventKind, key: str) -> ValueError:
    return ValueError(f"invalid {kind.value} runtime event detail value for {key}")


def _bounded_detail_int(
    kind: RuntimeEventKind,
    key: str,
    value: object,
    *,
    maximum: int,
) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise _invalid_event_detail(kind, key)


def _validate_runtime_event_detail_values(
    kind: RuntimeEventKind,
    detail: Mapping[str, object],
) -> None:
    for key, value in detail.items():
        if key in {
            "stale_seconds",
            "reconnect_attempt",
            "budget_count",
            "retry_after_ms",
            "previous_generation",
            "new_generation",
            "failed_event_seq",
            "recovered_event_seq",
        }:
            maximum = {
                "stale_seconds": 3_600,
                "reconnect_attempt": 1_000_000,
                "budget_count": 128,
                "retry_after_ms": 3_600_000,
            }.get(key, 2**63 - 1)
            _bounded_detail_int(kind, key, value, maximum=maximum)
        elif key == "source" and (
            not isinstance(value, str) or value not in _RUNTIME_EVENT_SOURCES
        ):
            raise _invalid_event_detail(kind, key)
        elif key == "operation" and (
            not isinstance(value, str) or value not in _RUNTIME_EVENT_OPERATIONS
        ):
            raise _invalid_event_detail(kind, key)
        elif key == "error_type" and (
            not isinstance(value, str) or value not in _RUNTIME_EVENT_ERROR_TYPES
        ):
            raise _invalid_event_detail(kind, key)
        elif key == "close_succeeded" and type(value) is not bool:
            raise _invalid_event_detail(kind, key)
        elif key == "signal" and (
            not isinstance(value, str) or value not in _RUNTIME_EVENT_SIGNALS
        ):
            raise _invalid_event_detail(kind, key)
        elif key in {"manifest_sha256", "report_sha256"} and (
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
        ):
            raise _invalid_event_detail(kind, key)
        elif key == "checkpoint" and (
            not isinstance(value, str) or value not in _RUNTIME_EVENT_CHECKPOINTS
        ):
            raise _invalid_event_detail(kind, key)

    previous = detail.get("previous_generation")
    current = detail.get("new_generation")
    if previous is not None and current is not None and current <= previous:  # type: ignore[operator]
        raise _invalid_event_detail(kind, "new_generation")


def safe_runtime_error_type(error: BaseException) -> str:
    """Map arbitrary exception classes to a non-secret bounded taxonomy."""
    name = type(error).__name__
    return name if name in _RUNTIME_EVENT_ERROR_TYPES else "Exception"


def build_runtime_event_detail(
    kind: RuntimeEventKind,
    values: Mapping[str, object],
) -> Mapping[str, object]:
    """Build one bounded event detail from a kind-specific safe-key contract."""
    _require_enum("kind", kind, RuntimeEventKind)
    if not isinstance(values, Mapping):
        raise TypeError("runtime event detail values must be a Mapping")
    unknown = set(values) - _RUNTIME_EVENT_DETAIL_KEYS[kind]
    if unknown:
        raise ValueError(
            f"runtime event detail keys are not allowed for {kind.value}: "
            f"{sorted(unknown)!r}"
        )
    normalized = _normalize_json(dict(values))
    _validate_runtime_event_detail_values(kind, normalized)
    return _frozen_mapping(normalized)


def _normalize_json(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise TypeError("unsupported non-finite float in canonical JSON")
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        _require_utc("canonical datetime", value)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("unsupported non-string canonical JSON mapping key")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_json(item) for item in value]
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def _validate_event_detail(value: Any) -> None:
    if isinstance(value, float):
        raise ValueError("detail must not contain float values")
    if isinstance(value, str):
        if "\x00" in value:
            raise ValueError("detail strings and mapping keys must not contain NUL")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            _validate_event_detail(key)
            _validate_event_detail(item)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _validate_event_detail(item)


def _canonical_json(value: Mapping[str, object] | Sequence[object]) -> bytes:
    if not isinstance(value, (Mapping, Sequence)) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise TypeError("stable_sha256 requires a mapping or sequence")
    return json.dumps(
        _normalize_json(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def stable_sha256(value: Mapping[str, object] | Sequence[object]) -> str:
    """Hash canonical compact JSON without relying on object reprs."""
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _require_nonempty(name: str, value: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str")
    if not value:
        raise ValueError(f"{name} must not be empty")


def _require_uuid(name: str, value: object) -> None:
    if not isinstance(value, UUID):
        raise TypeError(f"{name} must be a UUID")


def _require_enum(name: str, value: object, enum_type: type[StrEnum]) -> None:
    if type(value) is not enum_type:
        raise TypeError(f"{name} must be the exact {enum_type.__name__} enum type")


def _require_bool(name: str, value: object, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    if type(value) is not bool:
        qualifier = "bool or None" if optional else "bool"
        raise TypeError(f"{name} must be a {qualifier}")


def _require_sha256(name: str, value: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str")
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")


def _require_utc(name: str, value: datetime | None) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value)):
        raise ValueError(f"{name} must be timezone-aware UTC")


def _require_nonnegative(name: str, value: int | None) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _require_reason(name: str, value: str, *, required: bool = True) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str")
    if (required and not value) or len(value) > 64:
        qualifier = "non-empty and " if required else ""
        raise ValueError(f"{name} must be {qualifier}at most 64 characters")


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    return value


def _frozen_mapping[ValueT](value: Mapping[str, ValueT]) -> Mapping[str, ValueT]:
    return _freeze(value)


def _postgres_jsonb_text(value: Mapping[str, object]) -> str:
    """Render the byte-count contract used by PostgreSQL ``jsonb::text``."""
    return json.dumps(
        _normalize_json(value),
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    )


def _empty_mapping() -> Mapping[str, Any]:
    return MappingProxyType({})


@dataclass(frozen=True)
class MarketPair:
    market_id: str
    yes_token_id: str
    no_token_id: str

    def __post_init__(self) -> None:
        for name in ("market_id", "yes_token_id", "no_token_id"):
            _require_nonempty(name, getattr(self, name))
        if self.yes_token_id == self.no_token_id:
            raise ValueError("MarketPair must contain two distinct token IDs")


@dataclass(frozen=True, slots=True)
class SoakMappingLock:
    mapping_hash: str
    t0: datetime
    t24: datetime

    def __post_init__(self) -> None:
        _require_sha256("mapping_hash", self.mapping_hash)
        _require_utc("t0", self.t0)
        _require_utc("t24", self.t24)
        if self.t24 - self.t0 < timedelta(hours=24):
            raise ValueError("soak mapping lock must cover at least 24 hours")


@dataclass(frozen=True)
class AcceptanceConfig:
    recipe_sha256: str
    sample_interval_s: int
    max_sample_gap_s: int
    promote_interval_s: int
    promote_max_start_gap_s: int
    market_book_fresh_s: int
    market_ohlc_fresh_s: int
    expected_market_count: int
    expected_token_count: int
    retention_days: int
    schema_revision: str
    code_version: str

    def __post_init__(self) -> None:
        _require_sha256("recipe_sha256", self.recipe_sha256)
        for config_field in fields(self):
            if config_field.name not in {"recipe_sha256", "schema_revision", "code_version"}:
                _require_nonnegative(config_field.name, getattr(self, config_field.name))
        _require_nonempty("schema_revision", self.schema_revision)
        _require_nonempty("code_version", self.code_version)

    @classmethod
    def from_settings(
        cls,
        settings: Any,
        recipe_path: Path,
        code_version: str,
    ) -> AcceptanceConfig:
        return cls(
            recipe_sha256=hashlib.sha256(Path(recipe_path).read_bytes()).hexdigest(),
            sample_interval_s=settings.l3_evidence_sample_interval_s,
            max_sample_gap_s=settings.l3_evidence_max_sample_gap_s,
            promote_interval_s=settings.l3_promote_interval_s,
            promote_max_start_gap_s=settings.l3_promote_max_start_gap_s,
            market_book_fresh_s=settings.l3_market_book_fresh_s,
            market_ohlc_fresh_s=settings.l3_market_ohlc_fresh_s,
            expected_market_count=5,
            expected_token_count=10,
            retention_days=settings.l3_evidence_retention_days,
            schema_revision="007",
            code_version=code_version,
        )

    def digest(self) -> str:
        return stable_sha256({field.name: getattr(self, field.name) for field in fields(self)})


@dataclass(frozen=True)
class RuntimeIdentity:
    machine_id: str
    machine_version: str
    image_ref: str
    release_id: str
    code_version: str
    recipe_sha256: str
    acceptance_config_hash: str

    def __post_init__(self) -> None:
        for name in ("machine_id", "machine_version", "image_ref", "release_id", "code_version"):
            _require_nonempty(name, getattr(self, name))
        _require_sha256("recipe_sha256", self.recipe_sha256)
        _require_sha256("acceptance_config_hash", self.acceptance_config_hash)

    @classmethod
    def from_environment(cls, settings: Any) -> RuntimeIdentity:
        import polyarb

        recipe_path = Path(__file__).resolve().parents[1] / "scan_recipes" / "l3-promote.yaml"
        acceptance = AcceptanceConfig.from_settings(
            settings,
            recipe_path,
            code_version=polyarb.__version__,
        )
        return cls(
            machine_id=os.environ.get("FLY_MACHINE_ID", "local"),
            machine_version=os.environ.get("FLY_MACHINE_VERSION", "local"),
            image_ref=os.environ.get("FLY_IMAGE_REF", "local"),
            release_id=settings.release_id,
            code_version=polyarb.__version__,
            recipe_sha256=acceptance.recipe_sha256,
            acceptance_config_hash=acceptance.digest(),
        )


@dataclass(frozen=True)
class RuntimeBootRecord:
    boot_id: UUID
    started_at: datetime
    machine_id: str
    machine_version: str
    image_ref: str
    release_id: str
    code_version: str
    acceptance_config_hash: str

    def __post_init__(self) -> None:
        _require_uuid("boot_id", self.boot_id)
        _require_utc("started_at", self.started_at)
        for name in ("machine_id", "machine_version", "image_ref", "release_id", "code_version"):
            _require_nonempty(name, getattr(self, name))
        _require_sha256("acceptance_config_hash", self.acceptance_config_hash)


@dataclass(frozen=True)
class PromoteRunRecord:
    boot_id: UUID
    run_seq: int
    scheduled_at: datetime
    started_at: datetime
    finished_at: datetime
    status: PromoteStatus
    reason_code: str
    selected_count: int
    desired_count: int
    committed_count: int
    evidenced_count: int
    add_count: int
    remove_count: int
    mapping_hash: str
    desired_hash: str
    committed_hash: str
    acceptance_config_hash: str
    ws_generation: int
    add_succeeded: bool | None
    remove_succeeded: bool | None
    mirror_succeeded: bool
    duration_ms: int

    def __post_init__(self) -> None:
        _require_uuid("boot_id", self.boot_id)
        _require_enum("status", self.status, PromoteStatus)
        for name in ("scheduled_at", "started_at", "finished_at"):
            _require_utc(name, getattr(self, name))
        for name in (
            "run_seq",
            "selected_count",
            "desired_count",
            "committed_count",
            "evidenced_count",
            "add_count",
            "remove_count",
            "ws_generation",
            "duration_ms",
        ):
            _require_nonnegative(name, getattr(self, name))
        for name in ("mapping_hash", "desired_hash", "committed_hash", "acceptance_config_hash"):
            _require_sha256(name, getattr(self, name))
        _require_bool("add_succeeded", self.add_succeeded, optional=True)
        _require_bool("remove_succeeded", self.remove_succeeded, optional=True)
        _require_bool("mirror_succeeded", self.mirror_succeeded)
        _require_reason("reason_code", self.reason_code)


@dataclass(frozen=True)
class PromoteRunResult(Mapping[str, object]):
    """Typed terminal outcome with a complete immutable legacy mapping view."""

    status: PromoteStatus
    reason_code: str
    desired: frozenset[str]
    committed: frozenset[str]
    evidenced: frozenset[str]
    run_seq: int
    scheduled_at: datetime
    added: frozenset[str] = frozenset()
    removed: frozenset[str] = frozenset()
    added_markets: frozenset[str] = frozenset()
    removed_markets: frozenset[str] = frozenset()
    dry_run: bool = False
    persisted: bool = False

    def __post_init__(self) -> None:
        _require_enum("status", self.status, PromoteStatus)
        _require_reason("reason_code", self.reason_code)
        _require_nonnegative("run_seq", self.run_seq)
        _require_utc("scheduled_at", self.scheduled_at)
        for name in (
            "desired",
            "committed",
            "evidenced",
            "added",
            "removed",
            "added_markets",
            "removed_markets",
        ):
            values = frozenset(getattr(self, name))
            for value in values:
                _require_nonempty(f"{name} identity", value)
            object.__setattr__(self, name, values)
        _require_bool("dry_run", self.dry_run)
        _require_bool("persisted", self.persisted)

    def _mapping(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "added": sorted(self.added),
                "removed": sorted(self.removed),
                "added_markets": sorted(self.added_markets),
                "removed_markets": sorted(self.removed_markets),
                # Historical dry-run callers treated active as the proposal.
                "active": sorted(self.desired if self.dry_run else self.committed),
                "proposed_active": sorted(self.desired),
                "dry_run": self.dry_run,
                "persisted": self.persisted,
                "skipped": None if self.status is PromoteStatus.SUCCESS else self.reason_code,
                "status": self.status.value,
                "reason_code": self.reason_code,
            }
        )

    def __getitem__(self, key: str) -> object:
        return self._mapping()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._mapping())

    def __len__(self) -> int:
        return len(self._mapping())


@dataclass(frozen=True)
class HealthSampleRecord:
    boot_id: UUID
    sample_seq: int
    scheduled_at: datetime
    sampled_at: datetime
    desired_count: int
    committed_count: int
    evidenced_count: int
    promote_age_ms: int | None
    global_book_age_ms: int | None
    ws_age_ms: int | None
    mirror_age_ms: int | None
    candidate_age_ms: int | None
    reconciliation_age_ms: int | None
    listener_state: str
    cursor_lag: int
    watchdog_count: int
    reconnect_count: int
    ws_generation: int
    mapping_hash: str
    acceptance_config_hash: str
    status: HealthStatus
    reason_code: str

    def __post_init__(self) -> None:
        _require_uuid("boot_id", self.boot_id)
        _require_enum("status", self.status, HealthStatus)
        _require_utc("scheduled_at", self.scheduled_at)
        _require_utc("sampled_at", self.sampled_at)
        if self.scheduled_at > self.sampled_at:
            raise ValueError("scheduled_at must not follow sampled_at")
        if self.sampled_at >= self.scheduled_at + timedelta(seconds=30):
            raise ValueError("sampled_at must remain inside the scheduled_at 30-second slot")
        for name in (
            "sample_seq",
            "desired_count",
            "committed_count",
            "evidenced_count",
            "promote_age_ms",
            "global_book_age_ms",
            "ws_age_ms",
            "mirror_age_ms",
            "candidate_age_ms",
            "reconciliation_age_ms",
            "cursor_lag",
            "watchdog_count",
            "reconnect_count",
            "ws_generation",
        ):
            _require_nonnegative(name, getattr(self, name))
        _require_nonempty("listener_state", self.listener_state)
        _require_sha256("mapping_hash", self.mapping_hash)
        _require_sha256("acceptance_config_hash", self.acceptance_config_hash)
        _require_reason("reason_code", self.reason_code)


@dataclass(frozen=True)
class MarketSampleRecord:
    boot_id: UUID
    sample_seq: int
    sampled_at: datetime
    market_id: str
    yes_token_id: str
    no_token_id: str
    yes_desired: bool
    no_desired: bool
    yes_committed: bool
    no_committed: bool
    yes_evidenced: bool
    no_evidenced: bool
    evidence_generation: int
    yes_book_at: datetime | None
    no_book_at: datetime | None
    yes_book_age_ms: int | None
    no_book_age_ms: int | None
    worst_book_age_ms: int | None
    yes_ohlc_at: datetime | None
    yes_ohlc_age_ms: int | None
    status: HealthStatus
    reason_code: str

    def __post_init__(self) -> None:
        _require_uuid("boot_id", self.boot_id)
        _require_enum("status", self.status, HealthStatus)
        for name in ("sampled_at", "yes_book_at", "no_book_at", "yes_ohlc_at"):
            _require_utc(name, getattr(self, name))
        for name in (
            "sample_seq",
            "evidence_generation",
            "yes_book_age_ms",
            "no_book_age_ms",
            "worst_book_age_ms",
            "yes_ohlc_age_ms",
        ):
            _require_nonnegative(name, getattr(self, name))
        for name in (
            "yes_desired",
            "no_desired",
            "yes_committed",
            "no_committed",
            "yes_evidenced",
            "no_evidenced",
        ):
            _require_bool(name, getattr(self, name))
        MarketPair(self.market_id, self.yes_token_id, self.no_token_id)
        _require_reason("reason_code", self.reason_code)


def _validate_five_markets(
    markets: tuple[MarketSampleRecord, ...],
    *,
    boot_id: UUID,
    sample_seq: int,
    sampled_at: datetime,
) -> None:
    if len(markets) != 5:
        raise ValueError("sample batch requires exactly five market samples")
    if any(
        market.boot_id != boot_id
        or market.sample_seq != sample_seq
        or market.sampled_at != sampled_at
        for market in markets
    ):
        raise ValueError("market samples must share boot, sample sequence, and timestamp")
    if len({market.market_id for market in markets}) != 5:
        raise ValueError("market samples must contain five distinct markets")
    if len({market.yes_token_id for market in markets}) != 5:
        raise ValueError("market samples must contain five distinct Yes token IDs")
    if len({market.no_token_id for market in markets}) != 5:
        raise ValueError("market samples must contain five distinct No token IDs")
    tokens = {token for market in markets for token in (market.yes_token_id, market.no_token_id)}
    if len(tokens) != 10:
        raise ValueError("market samples must contain ten distinct token IDs")


@dataclass(frozen=True)
class SampleBatch:
    health: HealthSampleRecord
    markets: tuple[MarketSampleRecord, ...]

    def __post_init__(self) -> None:
        frozen_markets = tuple(self.markets)
        object.__setattr__(self, "markets", frozen_markets)
        _validate_five_markets(
            frozen_markets,
            boot_id=self.health.boot_id,
            sample_seq=self.health.sample_seq,
            sampled_at=self.health.sampled_at,
        )


@dataclass(frozen=True)
class RuntimeEventRecord:
    event_id: UUID
    boot_id: UUID
    event_seq: int
    occurred_at: datetime
    kind: RuntimeEventKind
    severity: RuntimeEventSeverity = RuntimeEventSeverity.INFO
    generation: int | None = None
    reason_code: str = ""
    detail: Mapping[str, object] = field(default_factory=_empty_mapping)

    def __post_init__(self) -> None:
        _require_uuid("event_id", self.event_id)
        _require_uuid("boot_id", self.boot_id)
        _require_enum("kind", self.kind, RuntimeEventKind)
        _require_enum("severity", self.severity, RuntimeEventSeverity)
        _require_nonnegative("event_seq", self.event_seq)
        _require_nonnegative("generation", self.generation)
        _require_utc("occurred_at", self.occurred_at)
        _require_reason("reason_code", self.reason_code, required=False)
        if not isinstance(self.detail, Mapping):
            raise TypeError("detail root must be a Mapping")
        safe_detail = build_runtime_event_detail(self.kind, self.detail)
        _validate_event_detail(safe_detail)
        try:
            normalized_detail = _normalize_json(safe_detail)
            encoded_size = len(_postgres_jsonb_text(normalized_detail).encode("utf-8"))
        except (TypeError, ValueError) as exc:
            raise ValueError("detail must contain canonical JSON values") from exc
        if encoded_size > 2048:
            raise ValueError("detail PostgreSQL jsonb::text must be at most 2048 bytes")
        object.__setattr__(self, "detail", _frozen_mapping(normalized_detail))


@dataclass(frozen=True)
class WsMembershipSnapshot:
    generation: int = 0
    desired: frozenset[str] = frozenset()
    committed: frozenset[str] = frozenset()
    evidenced: frozenset[str] = frozenset()
    evidenced_at: Mapping[str, datetime] = field(default_factory=_empty_mapping)

    def __post_init__(self) -> None:
        _require_nonnegative("generation", self.generation)
        desired = frozenset(self.desired)
        committed = frozenset(self.committed)
        evidenced = frozenset(self.evidenced)
        evidence_times = dict(self.evidenced_at)
        if not evidenced <= committed:
            raise ValueError("evidenced membership must be a subset of committed membership")
        if set(evidence_times) != set(evidenced):
            raise ValueError("evidenced_at keys must exactly match evidenced membership")
        for token_id in desired | committed | evidenced:
            _require_nonempty("membership token ID", token_id)
        for token_id, at in evidence_times.items():
            _require_utc(f"evidenced_at[{token_id!r}]", at)
        object.__setattr__(self, "desired", desired)
        object.__setattr__(self, "committed", committed)
        object.__setattr__(self, "evidenced", evidenced)
        object.__setattr__(self, "evidenced_at", _frozen_mapping(evidence_times))


@dataclass(frozen=True)
class FrameDispatchResult:
    """Durable write outcomes for one production WebSocket frame."""

    tob_written: bool
    book_levels_written: bool
    observed_at: datetime | None

    def __post_init__(self) -> None:
        _require_bool("tob_written", self.tob_written)
        _require_bool("book_levels_written", self.book_levels_written)
        if self.observed_at is not None and type(self.observed_at) is not datetime:
            raise TypeError("observed_at must be a datetime or None")
        _require_utc("observed_at", self.observed_at)


@dataclass(frozen=True)
class EvidenceStatus:
    identity: RuntimeIdentity
    boot_id: UUID
    started_at: datetime
    acceptance_config_hash: str
    ws_generation: int
    desired: frozenset[str]
    committed: frozenset[str]
    evidenced: frozenset[str]
    evidenced_at: Mapping[str, datetime]
    last_promote_persisted_at: datetime | None
    last_sample_persisted_at: datetime | None
    last_market_samples: tuple[MarketSampleRecord, ...]
    writer_ok: bool | None
    last_writer_result_at: datetime | None
    writer_reason_code: str
    pending_event_count: int
    event_queue_overflowed: bool
    event_integrity_failed: bool
    event_integrity_reason_code: str
    status: HealthStatus
    reason_code: str

    def __post_init__(self) -> None:
        for name in (
            "started_at",
            "last_promote_persisted_at",
            "last_sample_persisted_at",
            "last_writer_result_at",
        ):
            _require_utc(name, getattr(self, name))
        _require_sha256("acceptance_config_hash", self.acceptance_config_hash)
        _require_nonnegative("ws_generation", self.ws_generation)
        _require_nonnegative("pending_event_count", self.pending_event_count)
        _require_reason("writer_reason_code", self.writer_reason_code, required=False)
        _require_bool("event_integrity_failed", self.event_integrity_failed)
        _require_reason(
            "event_integrity_reason_code",
            self.event_integrity_reason_code,
            required=self.event_integrity_failed,
        )
        if not self.event_integrity_failed and self.event_integrity_reason_code:
            raise ValueError(
                "event_integrity_reason_code requires event_integrity_failed"
            )
        _require_reason("reason_code", self.reason_code)
        membership = WsMembershipSnapshot(
            self.ws_generation,
            self.desired,
            self.committed,
            self.evidenced,
            self.evidenced_at,
        )
        object.__setattr__(self, "desired", membership.desired)
        object.__setattr__(self, "committed", membership.committed)
        object.__setattr__(self, "evidenced", membership.evidenced)
        object.__setattr__(self, "evidenced_at", membership.evidenced_at)
        object.__setattr__(self, "last_market_samples", tuple(self.last_market_samples))


@dataclass(frozen=True)
class EvidenceWindow:
    start: datetime
    end: datetime
    boots: tuple[RuntimeBootRecord, ...] = ()
    promote_runs: tuple[PromoteRunRecord, ...] = ()
    health_samples: tuple[HealthSampleRecord, ...] = ()
    market_samples: tuple[MarketSampleRecord, ...] = ()
    runtime_events: tuple[RuntimeEventRecord, ...] = ()
    book_coverage_counts: Mapping[str, int] = field(default_factory=_empty_mapping)
    yes_ohlc_coverage_counts: Mapping[str, int] = field(default_factory=_empty_mapping)
    raw_rows_by_table: Mapping[str, tuple[Mapping[str, object], ...]] = field(
        default_factory=_empty_mapping
    )

    def __post_init__(self) -> None:
        _require_utc("start", self.start)
        _require_utc("end", self.end)
        if self.start >= self.end:
            raise ValueError("evidence window start must precede end")
        for name in ("boots", "promote_runs", "health_samples", "market_samples", "runtime_events"):
            object.__setattr__(self, name, tuple(getattr(self, name)))
        for name in ("book_coverage_counts", "yes_ohlc_coverage_counts"):
            mapping = dict(getattr(self, name))
            for key, count in mapping.items():
                _require_nonempty(f"{name} key", key)
                _require_nonnegative(f"{name}[{key!r}]", count)
            object.__setattr__(self, name, _frozen_mapping(mapping))
        object.__setattr__(self, "raw_rows_by_table", _frozen_mapping(self.raw_rows_by_table))


EVIDENCE_TABLES = frozenset(
    {
        "l3_runtime_boots",
        "l3_promote_runs",
        "l3_health_samples",
        "l3_market_samples",
        "l3_runtime_events",
    }
)


@dataclass(frozen=True)
class RetentionBounds:
    oldest_recorded_at_by_table: Mapping[str, datetime | None]
    newest_recorded_at_by_table: Mapping[str, datetime | None]
    row_count_by_table: Mapping[str, int]

    def __post_init__(self) -> None:
        for name in (
            "oldest_recorded_at_by_table",
            "newest_recorded_at_by_table",
            "row_count_by_table",
        ):
            mapping = dict(getattr(self, name))
            if set(mapping) != EVIDENCE_TABLES:
                raise ValueError(f"{name} must contain exactly the five evidence table keys")
            if name == "row_count_by_table":
                for table, count in mapping.items():
                    _require_nonnegative(f"{name}[{table!r}]", count)
            else:
                for table, at in mapping.items():
                    _require_utc(f"{name}[{table!r}]", at)
            object.__setattr__(self, name, _frozen_mapping(mapping))


_EVENT_QUEUE_CAPACITY = 128
_WRITER_CHANNELS = frozenset({"boot", "promoter", "sample", "event"})


class L3EvidenceRuntime:
    """Process-local truth and persisted-success anchors for one L3 boot."""

    def __init__(
        self,
        identity: RuntimeIdentity,
        boot_id: UUID | None = None,
        started_at: datetime | None = None,
    ) -> None:
        if not isinstance(identity, RuntimeIdentity):
            raise TypeError("identity must be a RuntimeIdentity")
        effective_started_at = datetime.now(UTC) if started_at is None else started_at
        _require_utc("started_at", effective_started_at)
        self._identity = identity
        self._boot_id = uuid4() if boot_id is None else boot_id
        self._started_at = effective_started_at
        self._run_seq = 0
        self._sample_seq = 0
        self._event_seq = 0
        self._pending_events: deque[RuntimeEventRecord] = deque()
        self._event_queue_overflowed = False
        self._event_integrity_failed = False
        self._event_integrity_reason_code = ""
        self._ws_generation = 0
        self._desired: frozenset[str] = frozenset()
        self._committed: frozenset[str] = frozenset()
        self._evidenced: frozenset[str] = frozenset()
        self._evidenced_at: Mapping[str, datetime] = _empty_mapping()
        self._last_promote_persisted_at: datetime | None = None
        self._last_sample_persisted_at: datetime | None = None
        self._last_market_samples: tuple[MarketSampleRecord, ...] = ()
        self._writer_ok: bool | None = None
        self._last_writer_result_at: datetime | None = None
        self._writer_reason_code = ""
        self._writer_ok_by_channel: dict[str, bool] = {}
        self._writer_result_at_by_channel: dict[str, datetime] = {}
        self._writer_reason_by_channel: dict[str, str] = {}

    def next_run_seq(self) -> int:
        sequence = self._run_seq
        self._run_seq += 1
        return sequence

    def next_sample_seq(self) -> int:
        sequence = self._sample_seq
        self._sample_seq += 1
        return sequence

    def record_event(
        self,
        kind: RuntimeEventKind,
        *,
        occurred_at: datetime,
        severity: RuntimeEventSeverity = RuntimeEventSeverity.INFO,
        generation: int | None = None,
        reason_code: str = "",
        detail: Mapping[str, object] | None = None,
    ) -> RuntimeEventRecord:
        sequence = self._event_seq
        self._event_seq += 1
        safe_detail = build_runtime_event_detail(
            kind,
            {} if detail is None else detail,
        )
        event = RuntimeEventRecord(
            event_id=uuid4(),
            boot_id=self._boot_id,
            event_seq=sequence,
            occurred_at=occurred_at,
            kind=kind,
            severity=severity,
            generation=generation,
            reason_code=reason_code,
            detail=safe_detail,
        )
        if len(self._pending_events) >= _EVENT_QUEUE_CAPACITY:
            self._event_queue_overflowed = True
            raise OverflowError("pending runtime event queue capacity 128 exceeded")
        self._pending_events.append(event)
        return event

    def drain_pending_events(self) -> tuple[RuntimeEventRecord, ...]:
        events = tuple(self._pending_events)
        self._pending_events.clear()
        return events

    def peek_pending_event(self) -> RuntimeEventRecord | None:
        """Return the queue head without transferring ownership to the writer."""
        return self._pending_events[0] if self._pending_events else None

    def acknowledge_pending_event(self, event: RuntimeEventRecord) -> None:
        """Remove exactly the durably appended queue head, preserving order."""
        if not isinstance(event, RuntimeEventRecord):
            raise TypeError("acknowledged event must be a RuntimeEventRecord")
        if not self._pending_events or self._pending_events[0].event_id != event.event_id:
            raise ValueError("only the current pending event may be acknowledged")
        self._pending_events.popleft()

    def quarantine_conflicting_event(
        self,
        event: RuntimeEventRecord,
        *,
        at: datetime,
        reason_code: str,
    ) -> None:
        """Isolate one poison head while preserving sticky integrity-fail truth."""
        _require_utc("event integrity failure at", at)
        _require_reason("event integrity reason_code", reason_code)
        if self._last_writer_result_at is not None and at < self._last_writer_result_at:
            raise ValueError("writer result timestamp cannot move backward")
        if not isinstance(event, RuntimeEventRecord):
            raise TypeError("quarantined event must be a RuntimeEventRecord")
        if not self._pending_events or self._pending_events[0].event_id != event.event_id:
            raise ValueError("only the current pending event may be quarantined")
        self._pending_events.popleft()
        if not self._event_integrity_failed:
            self._event_integrity_failed = True
            self._event_integrity_reason_code = reason_code
        self.note_writer_result(False, at, reason_code, channel="event")

    def snapshot(self) -> EvidenceStatus:
        if self._event_integrity_failed:
            status = HealthStatus.FAIL
            reason_code = "event_integrity_failed"
        elif self._event_queue_overflowed:
            status = HealthStatus.FAIL
            reason_code = "event_queue_overflow"
        elif self._writer_ok is False:
            status = HealthStatus.FAIL
            reason_code = "evidence_writer_failed"
        elif self._last_promote_persisted_at is None or self._last_sample_persisted_at is None:
            status = HealthStatus.WARN
            reason_code = "cold_start"
        else:
            status = HealthStatus.PASS
            reason_code = "ok"
        return EvidenceStatus(
            identity=self._identity,
            boot_id=self._boot_id,
            started_at=self._started_at,
            acceptance_config_hash=self._identity.acceptance_config_hash,
            ws_generation=self._ws_generation,
            desired=self._desired,
            committed=self._committed,
            evidenced=self._evidenced,
            evidenced_at=self._evidenced_at,
            last_promote_persisted_at=self._last_promote_persisted_at,
            last_sample_persisted_at=self._last_sample_persisted_at,
            last_market_samples=self._last_market_samples,
            writer_ok=self._writer_ok,
            last_writer_result_at=self._last_writer_result_at,
            writer_reason_code=self._writer_reason_code,
            pending_event_count=len(self._pending_events),
            event_queue_overflowed=self._event_queue_overflowed,
            event_integrity_failed=self._event_integrity_failed,
            event_integrity_reason_code=self._event_integrity_reason_code,
            status=status,
            reason_code=reason_code,
        )

    def update_membership(self, snapshot: WsMembershipSnapshot) -> None:
        if snapshot.generation < self._ws_generation:
            raise ValueError("websocket membership generation rollback is not allowed")
        copied = WsMembershipSnapshot(
            snapshot.generation,
            snapshot.desired,
            snapshot.committed,
            snapshot.evidenced,
            snapshot.evidenced_at,
        )
        self._ws_generation = copied.generation
        self._desired = copied.desired
        self._committed = copied.committed
        self._evidenced = copied.evidenced
        self._evidenced_at = copied.evidenced_at

    def mark_promote_persisted(self, at: datetime) -> None:
        _require_utc("promote persisted at", at)
        if self._last_promote_persisted_at is not None and at < self._last_promote_persisted_at:
            raise ValueError("promote persisted anchor cannot move backward")
        self._last_promote_persisted_at = at

    def mark_sample_persisted(
        self,
        at: datetime,
        markets: tuple[MarketSampleRecord, ...],
    ) -> None:
        _require_utc("sample persisted at", at)
        if self._last_sample_persisted_at is not None and at < self._last_sample_persisted_at:
            raise ValueError("sample persisted anchor cannot move backward")
        copied = tuple(markets)
        sample_seq = copied[0].sample_seq if copied else 0
        _validate_five_markets(
            copied,
            boot_id=self._boot_id,
            sample_seq=sample_seq,
            sampled_at=at,
        )
        self._last_sample_persisted_at = at
        self._last_market_samples = copied

    def note_writer_result(
        self,
        ok: bool,
        at: datetime,
        reason_code: str,
        *,
        channel: str = "event",
    ) -> None:
        _require_utc("writer result at", at)
        _require_reason("reason_code", reason_code)
        if channel not in _WRITER_CHANNELS:
            raise ValueError(f"unknown writer channel: {channel}")
        previous_channel_at = self._writer_result_at_by_channel.get(channel)
        if previous_channel_at is not None and at < previous_channel_at:
            raise ValueError(f"{channel} writer result timestamp cannot move backward")

        prior = self._writer_ok
        next_channel_results = dict(self._writer_ok_by_channel)
        next_channel_results[channel] = ok
        aggregate_ok = all(next_channel_results.values())
        transition_at = (
            at
            if self._last_writer_result_at is None
            else max(at, self._last_writer_result_at)
        )
        if not aggregate_ok and prior is not False:
            self.record_event(
                RuntimeEventKind.EVIDENCE_WRITER_FAILED,
                occurred_at=transition_at,
                severity=RuntimeEventSeverity.WARNING,
                generation=self._ws_generation,
                reason_code=reason_code,
            )
        elif aggregate_ok and prior is False:
            self.record_event(
                RuntimeEventKind.EVIDENCE_WRITER_RECOVERED,
                occurred_at=transition_at,
                generation=self._ws_generation,
                reason_code=reason_code,
            )

        self._writer_ok_by_channel = next_channel_results
        self._writer_result_at_by_channel[channel] = at
        self._writer_reason_by_channel[channel] = reason_code
        self._writer_ok = aggregate_ok
        self._last_writer_result_at = transition_at
        if aggregate_ok:
            self._writer_reason_code = reason_code
        else:
            failed_channels = (
                failed_channel
                for failed_channel, channel_ok in next_channel_results.items()
                if not channel_ok
            )
            latest_failed_channel = max(
                failed_channels,
                key=lambda failed_channel: (
                    self._writer_result_at_by_channel.get(failed_channel, at),
                    failed_channel,
                ),
            )
            self._writer_reason_code = self._writer_reason_by_channel.get(
                latest_failed_channel,
                reason_code,
            )
