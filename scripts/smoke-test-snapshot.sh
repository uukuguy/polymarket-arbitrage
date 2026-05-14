#!/usr/bin/env bash
# Smoke test: query latest snapshot row from local SQLite.
# Use after `make snapshot-markets-v` to verify Plan 03 mirror+R2 fields landed.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB="$PROJECT_ROOT/data/state.db"

if [ ! -f "$DB" ]; then
  echo "ERROR: $DB not found — run 'make snapshot-markets-v' first" >&2
  exit 1
fi

sqlite3 "$DB" "SELECT id, mode, is_valid, market_count, supabase_mirror_at_ms, parquet_r2_url FROM snapshots ORDER BY id DESC LIMIT 1;"
