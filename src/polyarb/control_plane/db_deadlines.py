"""Single-source database connection, statement, and lock deadlines."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DatabaseDeadlinePolicy:
    """One named, internally ordered database I/O boundary."""

    connect_timeout_seconds: int
    statement_timeout_ms: int
    lock_timeout_ms: int

    def __post_init__(self) -> None:
        if (
            min(
                self.connect_timeout_seconds,
                self.statement_timeout_ms,
                self.lock_timeout_ms,
            )
            <= 0
        ):
            raise ValueError("database deadlines must be positive")
        if self.lock_timeout_ms > self.statement_timeout_ms:
            raise ValueError("database lock timeout cannot exceed statement timeout")

    @property
    def statement_setting(self) -> str:
        return f"{self.statement_timeout_ms}ms"

    @property
    def lock_setting(self) -> str:
        return f"{self.lock_timeout_ms}ms"

    @property
    def connection_options(self) -> str:
        """libpq session options carrying both ordered server-side bounds."""
        return f"-cstatement_timeout={self.statement_setting} -clock_timeout={self.lock_setting}"

    @property
    def stop_grace_seconds(self) -> float:
        """Bound shutdown above connect plus one statement, with one tick margin."""
        return float(self.connect_timeout_seconds + (self.statement_timeout_ms + 999) // 1_000 + 1)

    @property
    def request_timeout_seconds(self) -> float:
        """Envelope one connect plus one statement and bounded response transfer."""
        return self.connect_timeout_seconds + self.statement_timeout_ms / 1_000 + 0.5


CONTROL_PLANE_DB_POLICY = DatabaseDeadlinePolicy(
    connect_timeout_seconds=5,
    statement_timeout_ms=5_000,
    lock_timeout_ms=1_000,
)

# Recovery actions operate below short controller/action leases, so they use a
# tighter statement cap while retaining the same lock-acquisition boundary.
RECOVERY_DB_POLICY = DatabaseDeadlinePolicy(
    connect_timeout_seconds=5,
    statement_timeout_ms=2_000,
    lock_timeout_ms=1_000,
)

# Migrations may scan or validate more data than one runtime transaction, but
# connection and lock acquisition remain short, explicit, and fail-closed.
MIGRATION_DB_POLICY = DatabaseDeadlinePolicy(
    connect_timeout_seconds=10,
    statement_timeout_ms=30_000,
    lock_timeout_ms=1_000,
)


__all__ = [
    "CONTROL_PLANE_DB_POLICY",
    "DatabaseDeadlinePolicy",
    "MIGRATION_DB_POLICY",
    "RECOVERY_DB_POLICY",
]
