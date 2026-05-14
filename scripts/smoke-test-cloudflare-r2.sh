#!/usr/bin/env bash
# Cloudflare R2 烟测 — 验证 API token + bucket 可达
# 凭证从项目根 .env 读取（.env 已 gitignored）
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ ! -f "$PROJECT_ROOT/.env" ]; then
  echo "ERROR: $PROJECT_ROOT/.env not found" >&2
  echo "Set up .env from .env.example first; see docs/setup/03-wave3-saas-prep.md Step B" >&2
  exit 1
fi

# 从 .env 加载 R2 三件套（兼容值带不带单/双引号、有无空格）
set -a
# shellcheck disable=SC1090,SC1091
source "$PROJECT_ROOT/.env"
set +a

for var in POLYARB_R2_ACCESS_KEY_ID POLYARB_R2_SECRET_ACCESS_KEY POLYARB_R2_ENDPOINT; do
  if [ -z "${!var:-}" ]; then
    echo "ERROR: $var is empty in .env" >&2
    exit 1
  fi
done

BUCKET="${POLYARB_R2_BUCKET:-polyarb-snapshots}"

# boto3 用 AWS_* 标准变量名
export AWS_ACCESS_KEY_ID="$POLYARB_R2_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$POLYARB_R2_SECRET_ACCESS_KEY"
export AWS_ENDPOINT_URL="$POLYARB_R2_ENDPOINT"

echo ">> smoke-test-cloudflare-r2 — listing bucket=$BUCKET"
uv run python - <<PY
import boto3, os, sys
client = boto3.client('s3', endpoint_url=os.environ['AWS_ENDPOINT_URL'])
try:
    resp = client.list_objects_v2(Bucket="$BUCKET")
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}", file=sys.stderr)
    sys.exit(1)
contents = resp.get('Contents', [])
print(f"OK — bucket reachable, {len(contents)} object(s)")
for obj in contents[:5]:
    print(f"  {obj['Key']}  ({obj['Size']} bytes)")
PY

unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_URL
