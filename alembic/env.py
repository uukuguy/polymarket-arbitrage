"""Alembic env.py — Phase 02 Plan 03 Supabase Postgres migration runner.

W6 fix: reads POLYARB_SUPABASE_DB_DSN (Postgres DSN) not POLYARB_SUPABASE_URL
(REST URL). These are two distinct env vars:
  - POLYARB_SUPABASE_URL  = https://<ref>.supabase.co  (supabase-py SDK)
  - POLYARB_SUPABASE_DB_DSN = postgresql://postgres:[PW]@db.<ref>.supabase.co:5432/postgres

target_metadata = None because migrations use imperative op.create_table()
(not autogenerate from SQLAlchemy models). Phase 02 Plan 03 only manages
the narrow dashboard mirror schema; SQLite source-of-truth uses schemas.py DDL.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from sqlalchemy import create_engine, pool

from alembic import context
from polyarb.control_plane.db_deadlines import MIGRATION_DB_POLICY

# The alembic Config object from alembic.ini (for logging setup)
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# target_metadata = None — we use imperative op.create_table() in migration files,
# not autogenerate from SQLAlchemy ORM models.
target_metadata = None


def _get_url() -> str:
    """Load the Supabase Postgres DSN from env var POLYARB_SUPABASE_DB_DSN.

    W6: This is the Postgres DSN (postgresql://...) used ONLY by alembic.
    It is DISTINCT from POLYARB_SUPABASE_URL (REST URL for supabase-py SDK).

    Raises EnvironmentError if the env var is not set (fail-fast; never run
    migrations without explicit credentials).
    """
    dsn = os.environ.get("POLYARB_SUPABASE_DB_DSN", "")
    if not dsn:
        raise OSError(
            "POLYARB_SUPABASE_DB_DSN is not set. "
            "W6 fix: this is the Postgres DSN used ONLY by alembic "
            "(NOT POLYARB_SUPABASE_URL which is the REST URL for supabase-py). "
            "Set: postgresql://postgres:[PASSWORD]@db.<ref>.supabase.co:5432/postgres"
        )
    # Force psycopg v3 driver: project uses psycopg[binary], not legacy psycopg2.
    # Supabase dashboard gives bare `postgresql://...` URLs; SQLAlchemy defaults
    # that scheme to psycopg2. Rewrite to `postgresql+psycopg://` so SQLAlchemy
    # picks v3. Idempotent if scheme already specifies a driver.
    if dsn.startswith("postgresql://"):
        dsn = "postgresql+psycopg://" + dsn[len("postgresql://") :]
    elif dsn.startswith("postgres://"):
        dsn = "postgresql+psycopg://" + dsn[len("postgres://") :]
    return dsn


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL and not an Engine, though an
    Engine is acceptable here as well. By skipping the Engine creation we
    don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the script output.
    """
    url = _get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine and associate a connection
    with the context.

    Supabase pgbouncer pitfall: use prepared_statement_cache_size=0 with
    NullPool to avoid prepared statement conflicts (RESEARCH §4 pitfall).
    """
    url = _get_url()
    connectable = create_engine(
        url,
        poolclass=pool.NullPool,
        connect_args={
            "connect_timeout": MIGRATION_DB_POLICY.connect_timeout_seconds,
            "options": MIGRATION_DB_POLICY.connection_options,
        },
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
