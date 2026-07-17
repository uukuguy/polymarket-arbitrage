"""Persistence boundary for M2 paper-account position state."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, TypeAlias


@dataclass
class PositionState:
    balance: float
    snapshot_balance: float
    realized_pnl: float = 0.0
    open_positions: dict[str, Any] = field(default_factory=dict)


TransitionResult: TypeAlias = bool | float | None
Transition: TypeAlias = Callable[[PositionState], TransitionResult]


class PositionRepository(Protocol):
    def load(self) -> PositionState: ...

    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
    ) -> TransitionResult: ...


class RepositoryStateError(RuntimeError):
    """Durable state violates repository invariants."""


@dataclass(frozen=True)
class AppliedOperation:
    operation_type: str
    target_id: str
    result: TransitionResult


class InMemoryPositionRepository:
    def __init__(self, initial_balance: float) -> None:
        self._state = PositionState(
            balance=initial_balance,
            snapshot_balance=initial_balance,
        )
        self._operations: dict[str, AppliedOperation] = {}

    def load(self) -> PositionState:
        return deepcopy(self._state)

    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
    ) -> TransitionResult:
        applied = self._operations.get(operation_id)
        if applied is not None:
            if (
                applied.operation_type != operation_type
                or applied.target_id != target_id
            ):
                raise ValueError(
                    "operation identity conflict: "
                    f"{operation_id!r} was already used for "
                    f"{applied.operation_type!r}/{applied.target_id!r}"
                )
            return deepcopy(applied.result)

        candidate = deepcopy(self._state)
        result = transition(candidate)
        if result is not None and not isinstance(result, (bool, float)):
            raise TypeError("transition result must be bool, float, or None")

        self._state = candidate
        self._operations[operation_id] = AppliedOperation(
            operation_type=operation_type,
            target_id=target_id,
            result=deepcopy(result),
        )
        return result
