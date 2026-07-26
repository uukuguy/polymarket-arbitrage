"""Storage package — SQLite + Parquet + Supabase mirror + Cloudflare R2 archive.

Phase 02 Plan 03 additions:
  - SupabaseMirror: post-write dashboard mirror (fail-soft, D-02/D-19)
  - R2UploadError: project-typed exception for R2 upload failures
  - compute_r2_key: deterministic R2 key from UTC timestamp (T-02-12)
  - upload_parquet_to_r2: fail-soft R2 upload function (D-03)
"""

from polyarb.storage.r2_sync import R2UploadError, compute_r2_key, upload_parquet_to_r2
from polyarb.storage.supabase_mirror import SupabaseMirror, narrow_market_row

__all__ = [
    "SupabaseMirror",
    "narrow_market_row",
    "R2UploadError",
    "compute_r2_key",
    "upload_parquet_to_r2",
]
