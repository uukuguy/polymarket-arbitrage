#!/usr/bin/env bash
set -euo pipefail
# Triple check: make snapshot-markets exit 0 ↔ SQLite row +1 ↔ Parquet file exists ↔ row counts match
# Closes LEARNINGS L11/S5 silent failure root cause.
#
# This script verifies the full contract of make snapshot-markets:
#   1. Command exits 0
#   2. SQLite snapshots table row count increments by exactly 1
#   3. Parquet file count increments by exactly 1
#   4. Newest parquet row count == SELECT market_count FROM snapshots ORDER BY id DESC LIMIT 1
#
# Exit codes:
#   0  = all checks passed (triple-check contract satisfied)
#   1  = a check failed (contract violated — see stderr for which gate)
#   77 = skip (fixture dirs unavailable for shell-level test execution)
#       Per autotools convention, 77 = skip, not failure.
#       This is a CONTRACT test, NOT optional — Plan 04 will harden the
#       fixture path for prod-like environments (shell-runnable fixtures).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ─────────────────────────────────────────────────────────────────────────────
# Determine data directories
# ─────────────────────────────────────────────────────────────────────────────

# Allow override via environment variables (for CI / test environments)
DB_PATH="${POLYARB_DB_PATH:-${PROJECT_ROOT}/data/state.db}"
PARQUET_ROOT="${POLYARB_PARQUET_ROOT:-${PROJECT_ROOT}/data/snapshots}"

# Check if fixture override dirs are specified (for isolated testing)
GAMMA_FIXTURE_DIR="${POLYARB_GAMMA_FIXTURE_DIR:-}"
CLOB_FIXTURE_DIR="${POLYARB_CLOB_FIXTURE_DIR:-}"

# ─────────────────────────────────────────────────────────────────────────────
# Safety check: if no live DB exists and no fixture dirs, skip gracefully
# ─────────────────────────────────────────────────────────────────────────────

if [ ! -f "${DB_PATH}" ]; then
    # Check if fixture dirs exist for isolated shell execution
    if [ -z "${GAMMA_FIXTURE_DIR}" ] || [ -z "${CLOB_FIXTURE_DIR}" ]; then
        echo "SKIP: DB not found at ${DB_PATH} and fixture dirs not set." >&2
        echo "SKIP: Set POLYARB_GAMMA_FIXTURE_DIR and POLYARB_CLOB_FIXTURE_DIR to enable shell-level triple-check." >&2
        echo "SKIP: Plan 04 will harden fixture path for prod-like environments." >&2
        echo "SKIP: This is a CONTRACT test — do not remove. Exit 77 = skip per autotools convention." >&2
        exit 77
    fi
fi

if [ ! -f "${DB_PATH}" ]; then
    echo "SKIP: DB not found at ${DB_PATH} even with fixture dirs set — cannot run triple-check." >&2
    echo "SKIP: Run 'make snapshot-markets' once to initialize the database first." >&2
    exit 77
fi

# ─────────────────────────────────────────────────────────────────────────────
# BEFORE: record baseline counts
# ─────────────────────────────────────────────────────────────────────────────

BEFORE_SNAPSHOT_COUNT=$(sqlite3 "${DB_PATH}" "SELECT COUNT(*) FROM snapshots")
BEFORE_PARQUET_COUNT=$(find "${PARQUET_ROOT}" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')

echo ">> triple-check BEFORE: snapshots=${BEFORE_SNAPSHOT_COUNT} parquets=${BEFORE_PARQUET_COUNT}"

# ─────────────────────────────────────────────────────────────────────────────
# RUN make snapshot-markets (with optional fixture overrides)
# ─────────────────────────────────────────────────────────────────────────────

echo ">> running make snapshot-markets ..."

MAKE_ARGS=""
if [ -n "${GAMMA_FIXTURE_DIR}" ]; then
    MAKE_ARGS="${MAKE_ARGS} POLYARB_GAMMA_FIXTURE_DIR=${GAMMA_FIXTURE_DIR}"
fi
if [ -n "${CLOB_FIXTURE_DIR}" ]; then
    MAKE_ARGS="${MAKE_ARGS} POLYARB_CLOB_FIXTURE_DIR=${CLOB_FIXTURE_DIR}"
fi

# Run and capture exit code (don't let set -e abort here — we check manually)
set +e
make -C "${PROJECT_ROOT}" snapshot-markets ${MAKE_ARGS}
MAKE_EXIT_CODE=$?
set -e

# ─────────────────────────────────────────────────────────────────────────────
# GATE 1: make snapshot-markets exit 0
# ─────────────────────────────────────────────────────────────────────────────

if [ "${MAKE_EXIT_CODE}" -ne 0 ]; then
    echo "FAIL: GATE 1 — make snapshot-markets exited ${MAKE_EXIT_CODE} (expected 0)" >&2
    exit 1
fi
echo ">> GATE 1 passed: make snapshot-markets exit 0"

# ─────────────────────────────────────────────────────────────────────────────
# GATE 2: SQLite snapshots row count incremented by exactly 1
# ─────────────────────────────────────────────────────────────────────────────

AFTER_SNAPSHOT_COUNT=$(sqlite3 "${DB_PATH}" "SELECT COUNT(*) FROM snapshots")
DELTA_SNAPSHOTS=$(( AFTER_SNAPSHOT_COUNT - BEFORE_SNAPSHOT_COUNT ))

if [ "${DELTA_SNAPSHOTS}" -ne 1 ]; then
    echo "FAIL: GATE 2 — SQLite snapshots count went from ${BEFORE_SNAPSHOT_COUNT} to ${AFTER_SNAPSHOT_COUNT} (delta=${DELTA_SNAPSHOTS}, expected 1)" >&2
    exit 1
fi
echo ">> GATE 2 passed: SQLite snapshots count incremented by 1 (now ${AFTER_SNAPSHOT_COUNT})"

# ─────────────────────────────────────────────────────────────────────────────
# GATE 3: Parquet file count incremented by exactly 1
# ─────────────────────────────────────────────────────────────────────────────

AFTER_PARQUET_COUNT=$(find "${PARQUET_ROOT}" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
DELTA_PARQUETS=$(( AFTER_PARQUET_COUNT - BEFORE_PARQUET_COUNT ))

if [ "${DELTA_PARQUETS}" -ne 1 ]; then
    echo "FAIL: GATE 3 — Parquet file count went from ${BEFORE_PARQUET_COUNT} to ${AFTER_PARQUET_COUNT} (delta=${DELTA_PARQUETS}, expected 1)" >&2
    exit 1
fi
echo ">> GATE 3 passed: Parquet file count incremented by 1 (now ${AFTER_PARQUET_COUNT})"

# ─────────────────────────────────────────────────────────────────────────────
# GATE 4: Newest parquet row count == SQLite snapshots.market_count
# ─────────────────────────────────────────────────────────────────────────────

# Get market_count from the latest snapshot row
SQLITE_MARKET_COUNT=$(sqlite3 "${DB_PATH}" "SELECT market_count FROM snapshots ORDER BY id DESC LIMIT 1")

# Find the newest parquet file
NEWEST_PARQUET=$(find "${PARQUET_ROOT}" -name '*.parquet' -newer "${DB_PATH}" 2>/dev/null | head -1)

if [ -z "${NEWEST_PARQUET}" ]; then
    # Fallback: find newest by modification time
    NEWEST_PARQUET=$(find "${PARQUET_ROOT}" -name '*.parquet' 2>/dev/null | sort -t/ -k1 | tail -1)
fi

if [ -z "${NEWEST_PARQUET}" ]; then
    echo "FAIL: GATE 4 — Could not find newest parquet file to verify row count" >&2
    exit 1
fi

# Use Python/uv to count parquet rows (pyarrow available in project venv)
PARQUET_ROW_COUNT=$(cd "${PROJECT_ROOT}" && uv run python -c "
import pyarrow.parquet as pq
import sys
t = pq.read_table('${NEWEST_PARQUET}')
print(t.num_rows)
")

if [ "${PARQUET_ROW_COUNT}" -ne "${SQLITE_MARKET_COUNT}" ]; then
    echo "FAIL: GATE 4 — Parquet row count ${PARQUET_ROW_COUNT} != SQLite market_count ${SQLITE_MARKET_COUNT}" >&2
    echo "FAIL: newest parquet: ${NEWEST_PARQUET}" >&2
    exit 1
fi
echo ">> GATE 4 passed: parquet rows (${PARQUET_ROW_COUNT}) == SQLite market_count (${SQLITE_MARKET_COUNT})"

# ─────────────────────────────────────────────────────────────────────────────
# All gates passed
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo ">> triple-check PASSED: all 4 gates satisfied"
echo ">> make snapshot-markets does exactly what it claims (L11/S5 silent failure closed)"
exit 0
