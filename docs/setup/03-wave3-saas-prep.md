# Wave 3 SaaS 注册照方抓药指南

> **目的**：分两段把云栈准备好 —
> **Phase 1 (调试阶段，零成本)**：建好 3 个 SaaS 账号 + 本地 daemon 写 mirror 到 Supabase Free + R2，验证整条数据链路。
> **Phase 2 (投运阶段，~$8-35/月)**：当调试通过后，把 daemon 搬到 Fly.io 7×24 跑 + Supabase 升 Pro。
>
> **预计时间**：Phase 1 ~20 分钟；Phase 2 ~15 分钟（且可以任意时候推迟）。
>
> **前置**：本指南假定 Phase 02 Wave 1 + Wave 2 已落地（`make planning-status` 显示 02-01/02/03 全 OK）。
>
> **Plan 04 的 Task 4 (Step A-F)** 是本指南对应的"contract 版"；本指南是它的"扩展手册"（多了 cost / pitfalls / 在哪里点 / 失败排查）。

## 🚦 两阶段路线图

```
┌─ Phase 1：调试期 (今天做，$0) ─────────────────────┐
│                                                    │
│  Step A.1-A.5  Fly 账号 + app + volume + token     │
│  Step A.7      生成 HMAC 密钥 (openssl rand)       │
│  Step B        Cloudflare R2 + bucket + API token  │
│  Step C        Supabase Free + Alembic migrate     │
│                                                    │
│  → 本地跑 make daemon-run-local                    │
│  → 验证 mirror 写进 Supabase Free + R2             │
│  → 月开销 $0.75（仅 Fly volume）                   │
└────────────────────────────────────────────────────┘
                       ↓
              （调试通过、想 7×24 投运）
                       ↓
┌─ Phase 2：投运期 (任意时间，$8-35/月) ──────────────┐
│                                                    │
│  Step D        flyctl secrets set (8 个一次性)     │
│  Step E        GitHub FLY_API_TOKEN                │
│  Step F        make deploy (在 Wave 3 dispatch 时) │
│  [可选]        Supabase Free → Pro                 │
└────────────────────────────────────────────────────┘
```

**Phase 1 的价值**：所有 SaaS 账号 + secrets 都齐了，本地 daemon 已经验证能写 mirror。这时 Wave 3 真要 dispatch，只剩 Plan 04 的 Dockerfile + fly.toml + 5 分钟首次 deploy。**风险与开销都最小化**。

**Phase 2 推迟**：你可以测一周 / 一个月再回来开 Phase 2，已经建好的账号 + secrets 不会失效。

---

## 📋 快速清单（你最后要拿到的 6 个值）

完成本指南后，你手上应该有这 6 个 secret 值（最终通过 `flyctl secrets set` 注入到 Fly app）：

| Secret | 来源 | 形如 |
|---|---|---|
| `POLYARB_SUPABASE_URL` | Step C | `https://abc123.supabase.co` |
| `POLYARB_SUPABASE_DB_DSN` | Step C | `postgresql://postgres:xxx@db.abc123.supabase.co:5432/postgres` |
| `POLYARB_SUPABASE_SERVICE_KEY` | Step C | `eyJhbGciOi...` (JWT) |
| `POLYARB_R2_ENDPOINT` | Step B | `https://<account_id>.r2.cloudflarestorage.com` |
| `POLYARB_R2_ACCESS_KEY_ID` | Step B | `a1b2c3d4...` (32 hex) |
| `POLYARB_R2_SECRET_ACCESS_KEY` | Step B | `e5f6g7h8...` (64 hex) |
| `POLYARB_SCAN_SHARED_SECRET` | Step A.7 | `openssl rand -hex 32` (64 hex) |

> ⚠️ **真实凭证绝对不要写到本文件**。本指南是 git 仓库的一部分（会推到 GitHub）。所有真实 secret 值只能写到：
> - `.env`（已在 `.gitignore` 里，不会提交 — 跑 `git check-ignore .env` 验证）
> - 密码管理器（1Password / Bitwarden / Keychain）
> - Fly.io secrets (`flyctl secrets set`，加密存在 Fly 后端)
> - GitHub Actions repo secrets（加密存在 GH 后端）
>
> **如果不慎写到本文件**：(1) 立刻把对应服务的 token revoke 重发；(2) 从文件里删掉；(3) 检查是否已 `git add` / `git commit` 过 — 已 commit 的话还要用 `git filter-repo` 重写历史。

> `POLYARB_R2_BUCKET=polyarb-snapshots` 已经写死在 `.env.example`，不算 secret。

另外还有 1 个 secret 要单独写到 **GitHub Actions** 的 repo secrets（不在 Fly）：

| Secret | 来源 | 形如 | 放哪 |
|---|---|---|---|
| `FLY_API_TOKEN` | Step A.6 | `fm2_xxx...` | GitHub repo → Settings → Secrets and variables → Actions |

---

## 💰 成本预期

| 服务 | 计费 | Phase 1 调试期 | Phase 2 投运初期 | Phase 2 长测/正式 |
|---|---|---|---|---|
| Fly.io machine | shared-cpu-1x 1GB ≈ $0.0000027/s ≈ $7/月 持续运行 | **$0**（machine 不启动）| ~$7/月 | ~$7/月 |
| Fly.io volume | $0.15/GB/月 × 5G | **$0.75/月** | $0.75/月 | $0.75/月 |
| Fly.io trial credit | 新账号 $5 一次性 | 部分抵 volume | 部分抵 machine | 已用完 |
| Cloudflare R2 | 10GB 免费 + 1M A ops / 10M B ops 免费 | **$0** | $0 | $0 |
| Supabase | Free / Pro $25 | **$0 (Free)** | **$0 (Free)** | $25 (Pro) — 升级判定见下 |
| GitHub Actions | 公开 repo 免费 | $0 | $0 | $0 |

**Phase 1 调试期合计 ≈ $0**（$5 trial credit 头一两个月足够覆盖 volume）
**Phase 2 投运初期 ≈ $8/月**（machine + volume，仍用 Supabase Free）
**Phase 2 长测/正式 ≈ $33/月**（+ Supabase Pro）

Wave 4 加 Sentry/Axiom/Better Stack 再加 ~$10/月（多数 free tier 足够 L1 用量）。

### Fly.io 计费要点

- **不部署 → 不收 machine 费**。`apps create` / `volumes create` 只是元数据。
- **Volume 一旦创建就计费**（5G × $0.15/月 = $0.75/月），即使 machine 没启动。
- **新账号 $5 trial credit** 自动抵扣前 ~半个月开销，注册时无需付费。
- **加信用卡是反滥用要求**，第一次预扣 $5 hold 几天后退回，不是真扣费。
- ⚠️ **注册后立即去 Dashboard → Billing → Spend Management 设月度 hard cap**（如 $20），防止 bug 死循环刷流量产生意外账单。
- `flyctl machine stop` 可以随时暂停 machine（停机不收 machine 费，volume 仍 $0.75/月），用于"不调试时省钱"。

### 🎯 Supabase Free → Pro 升级判定标准

**调试期用 Free 即可，以下任一条件满足时升 Pro**：

- [ ] 数据量 > 400MB（接近 Free 500MB 上限）
- [ ] 准备跑 Wave 5 末 **7-day soak gate**（Free 7 天无活动 auto-pause 会让 soak 中断）
- [ ] daemon 真投运（持续 24×7 跑超过 1 周）
- [ ] dashboard 开始有真实用户访问（Free cold start ~3-5s 会让用户体验拉胯）

**Free 升 Pro 是 in-place**：不丢数据、不丢 schema、不改 connection string。**所以现在花的 $0 完全无沉没成本**。

### Free tier 的 3 个真坑（要心里有数，不是 blocker）

1. **7 天无活动自动 pause**：你 daemon 出问题导致 7 天没 mirror 写入 → Supabase 实例暂停 → 下次 mirror 报 `connection refused`。处理：dashboard 点 "Restore" 复活；或加个 keepalive cron。
2. **500MB 数据上限**：Phase 02 subset 2/day × 4MB ≈ 240MB/月。1.5-2 个月后撞墙 → 升 Pro。
3. **2 个 project 上限**：调试期肯定够。

---

## 🛠 准备工作（5 分钟）

```bash
# 1. 装 flyctl（macOS）
brew install flyctl
flyctl version   # 验证装上了

# 2. 装/确认 openssl（macOS 自带 LibreSSL 够用）
openssl version

# 3. 进项目根
cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage

# 4. 确认 .env 已经从 .env.example 拷贝过（之前 Phase 01.1 翻译用过）
ls -la .env
```

> 没装过 flyctl 的话：[https://fly.io/docs/flyctl/install/](https://fly.io/docs/flyctl/install/)。Linux 用 `curl -L https://fly.io/install.sh | sh`。

---

# 📦 PHASE 1 — 调试期账号建设（零成本，今天做）

完成 Step A.1-A.5 + Step A.7 + Step B + Step C 后：
- 3 个 SaaS 账号建好
- 6 个 secret 值都在你密码管理器里
- Supabase Free 数据库 schema 已 apply
- 月度开销 **$0.75**（仅 Fly volume，被 $5 trial credit 抵掉）

然后用 `make daemon-run-local` 在本地跑 daemon，验证 mirror 真的写进 Supabase + R2。

---

## Step A — Fly.io 账号 + app + Volume + Deploy Token（~10 min）

### A.1 注册 + 加信用卡

打开 [https://fly.io/app/sign-up](https://fly.io/app/sign-up)。
- 用 GitHub OAuth 注册（最快，省去验证邮箱）。
- 注册后 **Billing → Add payment method**：必须加信用卡，否则 `apps create` 会成功但 `deploy` 会被 hold。
- 第一次会预扣 $5 hold（不是扣费，是验证），通常几天内退回。

> **CN 用户提示**：Fly.io 接受国内 Visa/Mastercard。如果信用卡被拒，换 Wise 或 Stripe 友好卡。

### A.2 安装 + 登录 flyctl

```bash
brew install flyctl   # 已装过跳过
flyctl auth login     # 浏览器打开 fly.io 登录页 → 授权 → 终端自动接到 token
flyctl auth whoami    # 应该显示你的 email
```

### A.3 创建 app（无 region 默认）

```bash
flyctl apps create polyarb-l1 --org personal
```

**Expected output**:
```
New app created: polyarb-l1
```

**Pitfall**：如果报 `app name 'polyarb-l1' is already taken` → app name 是全球唯一。换名（如 `polyarb-l1-<yourhandle>`），但要记住改名后 **fly.toml 里的 `app = "polyarb-l1"` 也要同步改**（Plan 04 dispatch 前告诉我新名字）。

### A.4 创建 Volume（5G，AMS）

```bash
flyctl volumes create polyarb_data --size 5 --region ams -a polyarb-l1
```

**为什么 AMS**：Polymarket API 主要走 Cloudflare 北美 / 欧洲边缘节点；EU Supabase（London 或 Dublin，看 Supabase 当前提供哪个）+ Amsterdam (`Fly`) 同 EU 主干网，跨服务延迟 ~5-15ms。换 US / SG / SA region 会让 Supabase mirror 慢很多（跨大西洋 80ms+）。

**为什么 5G**：subset snapshot 一个 ~5MB，full 一个 ~20MB；按 2/day subset + 1/week full + 30 天保留 = ~400MB SQLite + ~600MB parquet。预留 5G 留 4-5 倍冗余。

**Expected output**：
```
ID                  : vol_xxx
Name                : polyarb_data
App                 : polyarb-l1
Region              : ams
Zone                : xxx
Size GB             : 5
Encrypted           : true
Created at          : ...
```

### A.5 验证 app 状态

```bash
flyctl status -a polyarb-l1
```

应该看到 app 存在但 "no machines"（还没 deploy）。这是正常的。

### A.6 创建 Deploy Token（给 GHA 用）

```bash
flyctl tokens create deploy -a polyarb-l1
```

**Expected output**：一长串 `fm2_xxx...` token（**只显示一次！立即记到密码管理器**）。

把这个值临时记到剪贴板 / 安全笔记 — 后面 Step E 要粘到 GitHub。

### A.7 生成 HMAC 共享密钥

```bash
openssl rand -hex 32
```

**Expected output**：64 个十六进制字符（32 字节熵），形如 `7b3a...`。

**这是你的 `POLYARB_SCAN_SHARED_SECRET`** — 记到密码管理器。Plan 06 Vercel dashboard 也会用同一个值（但那边 env-var 名字叫 `SCAN_SHARED_SECRET`，没 `POLYARB_` 前缀，这是因为 Vercel 那边不用 pydantic-settings）。

> **为什么是 32 字节**：HMAC-SHA256 推荐密钥 ≥ block size (64 字节理论)，但 32 字节熵在实践中已远超暴力枚举可行性，且写入 env-var 长度可控。

---

## Step B — Cloudflare R2（~5 min）

### B.1 启用 R2

[https://dash.cloudflare.com/](https://dash.cloudflare.com/) → 左侧 **R2 Object Storage** → "Get Started"。

**好消息**：R2 不要信用卡（D-03 锁定的原因之一）。Free tier 10GB storage + 1M Class A ops + 10M Class B ops — 远超 Phase 02 用量。

### B.2 创建 bucket

R2 主页面 → "Create bucket"：
- **Name**: `polyarb-snapshots`（**必须这个名字** — `.env.example` 和 `POLYARB_R2_BUCKET` 默认值已经锁定。改名要同步改 `.env.example`，所以别改）
- **Location**: Automatic（让 Cloudflare 就近放）
- **Default Storage Class**: Standard（不要选 Infrequent Access — Phase 02 频繁读 parquet）
- **Public access**: ⛔ NO（保持 private，dashboard 通过 backend 拿 signed URL）

**Expected**：bucket 出现在 R2 列表里，状态 "Active"。

### B.3 创建 API Token

R2 主页面 → 右上角 **"Manage R2 API Tokens"** → "Create API Token"：
- **Token name**: `polyarb-l1-rw`
- **Permissions**: ✅ **Object Read & Write**
- **Specify buckets**: 选 `polyarb-snapshots`（不要选 "Apply to all buckets" — 最小权限原则）
- **TTL**: Forever（或 1 年 — 看你偏好；Wave 5 之后可以做 rotation）
- **Client IP Address Filtering**: 留空（Fly.io machine IP 是动态的）
- 点 **"Create API Token"**

**只显示一次的三个值要立即记下**：
- **Access Key ID** → 你的 `POLYARB_R2_ACCESS_KEY_ID`
- **Secret Access Key** → 你的 `POLYARB_R2_SECRET_ACCESS_KEY`
- **Endpoint for S3 clients** → 形如 `https://<account_id>.r2.cloudflarestorage.com` — 这是你的 `POLYARB_R2_ENDPOINT`

> **Pitfall**：页面会显示三个 endpoint：S3 / jurisdictional EU / jurisdictional FedRAMP。**用第一个 (S3 通用)**。

### B.4 本地烟测（可选但强烈推荐）

```bash
# 临时设到 shell（不写 .env，避免误提交）
export AWS_ACCESS_KEY_ID='[B.3 Access Key ID]'
export AWS_SECRET_ACCESS_KEY='[B.3 Secret]'
export AWS_ENDPOINT_URL='[B.3 Endpoint]'

# 列 bucket（应该返回空 — 还没东西上传过）
uv run python -c "
import boto3, os
client = boto3.client('s3', endpoint_url=os.environ['AWS_ENDPOINT_URL'])
resp = client.list_objects_v2(Bucket='polyarb-snapshots')
print('OK — bucket reachable, contents:', resp.get('Contents', '(empty)'))
"

# 清掉临时 env
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_URL
```

**Expected**：`OK — bucket reachable, contents: (empty)`。
**Failure mode**：`InvalidAccessKeyId` → 你 token 复制错了；`NoSuchBucket` → bucket 名拼错（确认是 `polyarb-snapshots` 不是 `polyarb-snapshot`）。

---

## Step C — Supabase EU（London / Dublin）（~5 min）

### C.1 注册 + 创建 project

[https://supabase.com](https://supabase.com) → Sign up（GitHub OAuth）→ **New project**：

- **Project name**: `polyarb`
- **Organization**: 你的 personal org
- **Database Password**: ⚠️ 用密码管理器生成 16+ 字符随机 — **现在记下！这是 DSN 的一部分，丢了只能重置整个 project**
- **Region**: **🇬🇧 West EU (London)** 或 **🇮🇪 West EU (Dublin)** — 选 Supabase 当前提供的任一 EU 选项即可。**必须是 EU**（与 Fly AMS 同主干网）。2026-05 Supabase 把 Dublin 重命名/迁移到 London，两者效果一样。**禁选**：任何 US / SG / SA 选项（跨洋会让 mirror 延迟从 ~10ms 飙到 80-200ms）
- **Pricing Plan**: **Free**（调试期 — 详见上方 "Supabase Free → Pro 升级判定标准"；Wave 5 末 soak gate 前再升）

> **D-02 历史决定 Pro 是基于"持续投运"假设**；调试期不存在持续投运语义，Free tier 完全够用。Free → Pro 是 in-place 升级（不停服 / 不丢数据 / DSN 不变），所以现在选 Free 没有沉没成本。

> **CN 用户支付提示**：未来升 Pro 时 Supabase 走 Stripe，国内 Visa 一般 OK。如果 Stripe 拒卡 → Wise 卡 / 实体 USD 卡 / Revolut。Free tier 不需要绑卡。

点 **"Create new project"** → 等 30-60 秒 provisioning。

### C.2 收集三个值

Project dashboard 左下 **⚙️ Project Settings** → **API**：

1. **Project URL** → 形如 `https://abc123.supabase.co` → 这是 `POLYARB_SUPABASE_URL`
2. **`service_role` key**（不是 anon key！）→ 形如 `eyJhbGciOi...`（JWT）→ 这是 `POLYARB_SUPABASE_SERVICE_KEY`
   - ⚠️ service_role 绕过 RLS，**严禁前端 / Vercel 使用**；Plan 06 dashboard 用 anon key + RLS 策略
   - 当前 Phase 02 daemon 通过 service_role 直接 upsert mirror，是 server-side 流程

3. **Database DSN**：Project Settings → **Database** → **Connection String** → 切到 "URI"：
   - 形如 `postgresql://postgres:[YOUR-PASSWORD]@db.abc123.supabase.co:5432/postgres`
   - 把 `[YOUR-PASSWORD]` 替换成 C.1 的密码
   - 这是 `POLYARB_SUPABASE_DB_DSN`

   > **重要**：这里 Supabase UI 会显示两种 DSN — "Direct connection" 5432 vs "Connection pooling" 6543。Phase 02 Alembic migration **用 Direct connection (5432)**，理由：Alembic 用 `CREATE TABLE` DDL 不兼容 pgbouncer transaction pooling。Wave 4 dashboard 跑应用查询时再考虑 pool。

### C.3 本地 apply Alembic 001 migration

```bash
# 把 DSN 写到 .env（永久；Plan 03 supabase_seed.py 也会读）
echo "POLYARB_SUPABASE_DB_DSN='postgresql://postgres:YOUR-PASSWORD@db.abc123.supabase.co:5432/postgres'" >> .env
echo "POLYARB_SUPABASE_URL='https://abc123.supabase.co'" >> .env
echo "POLYARB_SUPABASE_SERVICE_KEY='eyJhbGc...'" >> .env

# 运行 migration
make supabase-migrate
```

**Expected output**：
```
>> alembic upgrade head — applying initial dashboard schema to Supabase
...
INFO  [alembic.runtime.migration] Running upgrade  -> 001, initial dashboard schema
```

### C.4 验证 schema 落地

Supabase dashboard → **Table Editor** → 应该看到：
- `snapshots`（id / market_count / status / created_at / supabase_mirror_age_ms / r2_uploaded_at_ms）
- `markets_latest`（snapshot_id / market_id / ... / page_fetched_at_ms）
- View: `top_movers_view`

或者命令行验证：
```bash
make supabase-reconcile   # 跑 init_check，验证 schema 完整
```

**Expected**：`init_check PASSED — 2 tables + 1 view created, RLS anon-SELECT policy active`。

> **Pitfall**：如果看到 `relation "alembic_version" already exists` → 你之前手动建过表了。`make supabase-reconcile drop-all` 清场再重跑 `supabase-migrate`（**注意会删数据**，但当前是空库，OK）。

---

## ✅ Phase 1 完工 — 本地验证整条数据链路

到这里 Phase 1 已经做完。现在去验证你 mirror + R2 上传逻辑真的能工作（本地跑 daemon 直接打 Supabase + R2）：

### 1. 把 6 个 secret 写到本地 `.env`

```bash
# Supabase 三件套（来自 Step C.2）
echo "POLYARB_SUPABASE_URL='https://abc123.supabase.co'" >> .env
echo "POLYARB_SUPABASE_DB_DSN='postgresql://postgres:PASSWORD@db.abc123.supabase.co:5432/postgres'" >> .env
echo "POLYARB_SUPABASE_SERVICE_KEY='eyJhbGc...'" >> .env

# R2 三件套（来自 Step B.3）
echo "POLYARB_R2_ENDPOINT='https://ACCOUNT_ID.r2.cloudflarestorage.com'" >> .env
echo "POLYARB_R2_ACCESS_KEY_ID='YOUR-ACCESS-KEY'" >> .env
echo "POLYARB_R2_SECRET_ACCESS_KEY='YOUR-SECRET'" >> .env

# HMAC 密钥（来自 Step A.7）
echo "POLYARB_SCAN_SHARED_SECRET='YOUR-OPENSSL-HEX-OUTPUT'" >> .env
```

### 2. 启动本地 daemon

```bash
make daemon-run-local
```

**Expected**：uvicorn 起来，scheduler 加载 cron，loguru JSON 行刷在终端。访问 `http://localhost:8000/health` 应该返回 IETF 三态 JSON（snapshot age 是 `fail` 因为还没跑过；mirror / R2 status 是 `pass` 因为凭证在场）。

### 3. 触发一次完整 snapshot

新开一个终端：

```bash
# 加载 .env 里的 HMAC 密钥到当前 shell
export POLYARB_SCAN_SHARED_SECRET=$(grep POLYARB_SCAN_SHARED_SECRET .env | cut -d"'" -f2)

# 计算空 body 的 HMAC 签名
BODY=''
SIG=$(printf "%s" "$BODY" | openssl dgst -sha256 -hmac "$POLYARB_SCAN_SHARED_SECRET" | awk '{print $NF}')

# POST /scan
curl -i -X POST http://localhost:8000/scan \
  -H "X-Signature: sha256=$SIG" \
  -H "Content-Type: application/json" \
  -d "$BODY"
```

**Expected**：HTTP 200 + JSON 报告 snapshot id + market_count + supabase_mirror_status: ok + r2_upload_status: ok。

### 4. 三处眼见为实

| 目标 | 怎么验证 |
|---|---|
| **本地 SQLite 落地** | `sqlite3 data/state.db "SELECT id, market_count, status FROM snapshots ORDER BY id DESC LIMIT 1;"` |
| **Supabase mirror 落地** | Supabase dashboard → Table Editor → `markets_latest` → 应该看到 N 条记录；`snapshots` 表有一行 status=ok |
| **R2 parquet 落地** | Cloudflare dashboard → R2 → `polyarb-snapshots` bucket → 应该看到 `parquet/YYYY/MM/DD/HH-MM-SS.parquet` |

**如果三处都有数据**，说明整条 daemon → SQLite/Parquet → Supabase mirror → R2 upload 链路打通。Phase 1 ✅ 全部通过。

### 5. 故意制造失败，验证 fail-soft

```bash
# 临时把 Supabase URL 改成错的，触发 mirror 失败
POLYARB_SUPABASE_URL='https://bogus.supabase.co' make daemon-run-local
# 再触发一次 /scan
# Expected: snapshot 仍成功 (SQLite + Parquet 落地)，但 /health → status: warn (mirror status: degraded)
# 这就是 D-12 amendment + LEARNINGS P5 fail-soft 的现场验证
```

恢复正确 URL，再触发一次确认重新变 `pass`。

---

# 🚀 PHASE 2 — 投运期部署（任意时间，$8/月起）

⚠️ **以下章节是 Wave 3 dispatch 时才跑的内容**。你可以现在跳过 Phase 2，等想"7×24 跑"再回来。

Phase 2 触发条件：
- ✅ Phase 1 三处眼见为实都通过了（数据真在 SQLite + Supabase + R2）
- ✅ fail-soft 故障演练通过了（mirror 失败 → DEGRADED 不阻断 snapshot）
- ✅ 你愿意每月承担 ~$8（machine + volume）让 daemon 7×24 跑

---

## Step D — Fly app secrets（~3 min）

把前 7 个 secret 一次性写入 Fly app（用 HEREDOC 防 shell 拆词）：

```bash
flyctl secrets set \
  POLYARB_SUPABASE_URL='https://abc123.supabase.co' \
  POLYARB_SUPABASE_DB_DSN='postgresql://postgres:YOUR-PASSWORD@db.abc123.supabase.co:5432/postgres' \
  POLYARB_SUPABASE_SERVICE_KEY='eyJhbGc...' \
  POLYARB_R2_ENDPOINT='https://YOUR-ACCOUNT-ID.r2.cloudflarestorage.com' \
  POLYARB_R2_ACCESS_KEY_ID='YOUR-R2-ACCESS-KEY' \
  POLYARB_R2_SECRET_ACCESS_KEY='YOUR-R2-SECRET' \
  POLYARB_R2_BUCKET='polyarb-snapshots' \
  POLYARB_SCAN_SHARED_SECRET='YOUR-OPENSSL-HEX-OUTPUT' \
  -a polyarb-l1
```

**Expected output**：
```
Secrets are staged for the first deployment
```

> **没有 app instance 时只是 "staged"** — 等 Step F first deploy 后才真正注入到 machine env。

### D.1 验证 secret 名字（不验证值）

```bash
flyctl secrets list -a polyarb-l1
```

**Expected**：8 行，每行一个 secret 名字 + digest（不显示值，正常）。
```
NAME                                DIGEST              CREATED AT
POLYARB_R2_ACCESS_KEY_ID            xxx                 just now
POLYARB_R2_BUCKET                   xxx                 just now
POLYARB_R2_ENDPOINT                 xxx                 just now
POLYARB_R2_SECRET_ACCESS_KEY        xxx                 just now
POLYARB_SCAN_SHARED_SECRET          xxx                 just now
POLYARB_SUPABASE_DB_DSN             xxx                 just now
POLYARB_SUPABASE_SERVICE_KEY        xxx                 just now
POLYARB_SUPABASE_URL                xxx                 just now
```

**少了**：检查 D 命令的拼写（特别是 `POLYARB_` 前缀容易漏）。重跑 `flyctl secrets set` 是幂等的，可以补缺。

---

## Step E — GitHub Actions FLY_API_TOKEN（~2 min）

[https://github.com/](https://github.com/) → 你的 `polymarket-arbitrage` repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**：

- **Name**: `FLY_API_TOKEN`（**字面如此**，大小写敏感；Plan 04 的 GHA workflow 引用这个名字）
- **Value**: Step A.6 那个 `fm2_xxx...` 长 token

点 **"Add secret"**。

**验证**：列表里出现 `FLY_API_TOKEN`，值已加密不可见（正常）。

---

## Step F — First deploy（~5 min，在 Wave 3 dispatch 期间由 agent 跑）

⚠️ **不要现在跑** — 这一步是 **Plan 04 Task 5 (Wave 3 dispatch 内)** 的内容。Dockerfile + fly.toml 还没建。

完成 A-E 后回来跟我说"deployed prep done"，我就 `/gsd-execute-phase 02 --wave 3 --ws m1-perception`：
- Plan 04 会建 Dockerfile + fly.toml + crontab + GHA workflows（Task 1-3）
- 到 Task 4 自动暂停 → 我让你 `make deploy`（这一刻才用得上 Step F）
- `make deploy` 期望输出：
  - flyctl 上传 Docker context
  - 远程 build ~3-5 分钟
  - flyctl 等机器 healthy
  - `bash scripts/deploy_smoke.sh` 报 `/health = fail`（**这是正常** — 还没首次 snapshot，健康检查找不到数据）
- 访问 `https://polyarb-l1.fly.dev/health` → 看到 `{"status":"fail",...}` 是 success criterion

---

## ✅ 完工 checklist

### Phase 1 (调试期，零成本)

- [ ] Fly app `polyarb-l1` 存在，volume `polyarb_data` 5G 在 AMS region（machine 尚未启动 → 不收 machine 费）
- [ ] Fly Dashboard → Billing → Spend Management 设了月度 hard cap（建议 $20）
- [ ] R2 bucket `polyarb-snapshots` 存在，private 访问，API token 三件套已记录
- [ ] Supabase **Free** project (EU — London 或 Dublin) 存在，Alembic 001 migration 已 apply（`snapshots` + `markets_latest` 表 + `top_movers_view` view + RLS policy 看得见）
- [ ] HMAC 共享密钥（Step A.7 的 64 hex）已经记到密码管理器 — Plan 06 也要用
- [ ] 本地 `.env` 写了 7 个 secret（6 个 SaaS + HMAC 密钥）
- [ ] `make daemon-run-local` + 一次 HMAC-签名 `/scan` 调用成功
- [ ] **三处眼见为实**：SQLite / Supabase / R2 都有新数据
- [ ] fail-soft 故障演练通过：Supabase URL 写错 → snapshot 仍成功 + `/health` 报 warn

Phase 1 完成后告诉我："phase1 done" + 三处眼见为实的截图/输出，我就准备 Phase 2 路径。

### Phase 2 (投运期，~$8/月起，**可任意推迟**)

- [ ] `flyctl secrets list -a polyarb-l1` 列出 8 个 secrets
- [ ] GitHub repo Settings → Secrets 看到 `FLY_API_TOKEN`
- [ ] `make deploy` 成功，`https://polyarb-l1.fly.dev/health` 返回 IETF JSON
- [ ] Fly machine 正常 running（`flyctl status` 看到 1 个 healthy machine）

Phase 2 完成后告诉我："deployed prep done"，我 dispatch Wave 3：

```bash
flyctl status -a polyarb-l1
flyctl secrets list -a polyarb-l1
curl -fsS https://polyarb-l1.fly.dev/health
```

---

## 🚨 排错速查

| 症状 | 可能原因 | 处理 |
|---|---|---|
| `flyctl apps create` 报 name taken | app name 全球唯一 | 换名（如 `polyarb-l1-<handle>`），同步改 fly.toml 前告诉我 |
| `flyctl volumes create` 报 "no payment method" | A.1 没加信用卡 | 回去 Billing 加卡 |
| Supabase `make supabase-migrate` 报 `password authentication failed` | DSN 里 password 没 URL-encode | 密码含 `@/:%` 等特殊字符需 percent-encode；或 C.1 重生成无特殊字符密码 |
| Supabase 报 `connection to db.xxx timeout` | 用了 6543 pooler 端口 | 改回 5432 (Direct connection) |
| R2 boto3 烟测报 `InvalidAccessKeyId` | Access Key ID 复制时多了空格 / 换行 | 重新从 dashboard 拷贝；或重建 token |
| `flyctl secrets set` 报 `Error: missing app name` | 漏了 `-a polyarb-l1` | 加上 |
| GitHub Settings 找不到 "Secrets and variables" | 没在 repo 的 Settings 而在用户 Settings | URL 应该是 `github.com/<user>/polymarket-arbitrage/settings/secrets/actions` |

---

## 📚 参考

- Plan 04 Task 4 (Step A-F) — `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-04-PLAN.md`
- Deployment thread §0.1 user 锚点决策 — `.planning/threads/deployment-architecture.md`
- D-22 amendment（HMAC 不是 Flycast）— `02-CONTEXT.md` + Plan 02 SUMMARY
- D-02 / D-03 / D-09 / D-10 / D-11 — `02-CONTEXT.md`

> **指南维护原则**：发现指南漏了什么 / 有什么坑 → 直接在这个文件加章节；不另开文件。
