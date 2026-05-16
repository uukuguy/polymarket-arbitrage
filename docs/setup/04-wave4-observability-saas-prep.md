# Wave 4 可观测性 SaaS 注册照方抓药指南

> **目的**：注册 4 个 SaaS 账号（Sentry / Axiom / Better Stack / Telegram），拿到 5 个凭据，把它们配进 Fly secret store。这 4 个一起构成 Plan 05 的告警栈 + Plan 06 dashboard 的下游数据源。
>
> **预计时间**：30-40 分钟（Sentry 5min + Telegram 5min + Axiom 10min + Better Stack 10min + Fly 配置 5min）
>
> **月开销**：**$0**（全部 Free tier 起步，量级足够 L1 daemon）
>
> **前置**：Phase 02 Wave 3 已落地（`polyarb-l1.fly.dev` 在线，`make planning-status` 显示 02-01..04+08+09 全 OK）。
>
> **后置**：本指南完成后，跑 `/gsd-execute-phase 02 --wave 4` 触发 Plan 05+06 落地。

---

## 🗺️ 你最后需要拿到的 5 个凭据

把下面这张表打开放手边，一边注册一边填。**所有值都不要进 git**，直接进 Fly secret store。

| # | 凭据名 | 来自 | 形如 | Fly secret 名 |
|---|---|---|---|---|
| 1 | Sentry DSN | Sentry 项目 Settings → Client Keys | `https://abc123@o567.ingest.sentry.io/890` | `POLYARB_SENTRY_DSN` |
| 2 | Axiom API token | Axiom Settings → API Tokens | `xaat-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` | `POLYARB_AXIOM_TOKEN` |
| 3 | Axiom dataset name | Axiom 你创建的 dataset 名称 | `polyarb-l1` (自取，全小写) | `POLYARB_AXIOM_DATASET` |
| 4 | Better Stack heartbeat URL | Better Stack Monitors → Heartbeats → 新建 | `https://uptime.betterstack.com/api/v1/heartbeat/<hash>` | `POLYARB_HEARTBEAT_URL` |
| 5a | Telegram bot token | @BotFather 给的 | `123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11` | `POLYARB_TELEGRAM_BOT_TOKEN` |
| 5b | Telegram chat ID | getUpdates API 拿到 | 整数，如 `987654321` 或负数 group ID | `POLYARB_TELEGRAM_CHAT_ID` |

---

## ⏱️ 推荐顺序

```
1. Telegram bot          (5 min, 无依赖)
2. Sentry                (5 min, 注册时把 Telegram 集成顺手配上)
3. Axiom                 (10 min)
4. Better Stack          (10 min, 注册时把 Telegram 集成顺手配上)
5. Fly secrets set       (5 min, 把上面 6 个值一次性贴进去)
```

为什么 Telegram 优先：Sentry 和 Better Stack 注册流程里都问"要不要把告警发到 Telegram"，先有 bot token 流程不卡。

---

## 1. Telegram bot（5 分钟）

### 1.1 创建 bot

1. 用 Telegram app 搜 **@BotFather**（认证账号有蓝勾），打开私聊
2. 发 `/newbot`
3. BotFather 问 bot 名（display name）→ 自取，例如 `PolyArb Alert Bot`
4. 问 username（必须以 `_bot` 结尾，全网唯一）→ 例如 `polyarb_alert_2026_bot`
5. ✅ BotFather 回复消息里有一段：
   ```
   Use this token to access the HTTP API:
   123456789:ABCdef-ghIJKlmnOpQRstUVwxYZ1234567ab
   ```
   **整段冒号包含后面的字符串就是 bot token** → 填进第 5a 行

### 1.2 拿 chat ID（最常踩坑的一步）

bot 必须**先和你互动一次**才能向你发消息。两种方式：

**方式 A — 私聊接收（推荐）**：
1. 在 Telegram 里搜你刚建的 bot 的 username（不带 @ 也行），点开
2. 按 **START** 按钮（或发 `/start`）
3. 在浏览器打开：`https://api.telegram.org/bot<你的TOKEN>/getUpdates`
   - 注意 URL 里 `bot` 是字面文字，紧跟 token，没有冒号
4. 找返回 JSON 里 `"chat":{"id":987654321,...}`，**这个数字就是 chat ID**（私聊是正数）→ 填第 5b 行

**方式 B — 推到 group**（多人接收）：
1. 创建一个 Telegram group，把 bot 加进去（必须给 bot 群权限，BotFather 默认禁了 `/setjoingroups` 是开放的）
2. 在 group 里发任意消息（必须艾特 bot 一下，例如 `@polyarb_alert_2026_bot hi`）
3. 打开 `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. 找 `"chat":{"id":-1001234567890,...}` — group id 是**负数**

### 1.3 测试

终端跑：
```bash
TOKEN="你的token"
CHAT_ID="你的chat_id"
curl -sX POST "https://api.telegram.org/bot$TOKEN/sendMessage" \
  -d "chat_id=$CHAT_ID" \
  -d "text=hello from polyarb"
```
Telegram app 收到 "hello from polyarb" = 凭据 OK。

**踩坑提示**：
- 找不到 `getUpdates` 数据 → 检查你确实给 bot 发过 START / 互动过；bot 的 `getUpdates` 只保留最近的对话
- `403 Forbidden: bot was blocked by the user` → 你之前 block 了 bot，去解锁
- group 里 id 没拿到 → bot 必须**被艾特或被回复**才能收到 message，群里普通消息它看不到

---

## 2. Sentry（5 分钟）

### 2.1 注册

1. 访问 https://sentry.io/signup/
2. 用 GitHub OAuth 或邮箱注册（Developer plan = Free，5k errors/月）
3. 创建 org：随便取，例如 `polyarb-personal`
4. **平台选 Python**
5. Sentry 会跳到一个 "Configure SDK" 页面，第一行就是 DSN：
   ```python
   sentry_sdk.init(
       dsn="https://abc123@o4567.ingest.sentry.io/890",
       ...
   )
   ```
   **复制 dsn 引号里的整段 URL** → 填进第 1 行

### 2.2 关键设置（在 Project Settings 里点）

- **Settings → General → Project Name**：建议改成 `polyarb-l1`，dashboard 好认
- **Settings → Alerts → New Alert Rule**：
  - Type: Issue Alert
  - Condition: `A new issue is created` + `The event's level is equal to error`
  - Action: **"Send a Telegram notification"** → 这里 Sentry 会让你绑定 Telegram bot
    - 直接用你刚建的 bot 的 token + chat id 即可
  - Action Filter: 默认全部
  - Name: `prod alert → telegram`
  - Save Rule

### 2.3 测试

留到 Wave 4 落地后 Plan 05 提供 smoke test。**现阶段拿到 DSN 即可**。

**踩坑提示**：
- DSN 里包含 secret，**绝对不可进 git** — 只进 .env 或 Fly secrets
- Free tier 5k errors/月，L1 daemon 量级足够（一次 OOM 也就一两个 exception）

---

## 3. Axiom（10 分钟）

Axiom 是日志聚合，比 Better Stack Logs 慷慨 30 倍 retention，是项目 D-14 锁定的选择。

### 3.1 注册

1. 访问 https://axiom.co/
2. **Sign up free**（Free tier = 500GB/月 ingest + 30 天 retention，远超 L1 daemon 量级）
3. 用 GitHub OAuth 或邮箱
4. 第一步问你 workspace 名：例如 `polyarb`

### 3.2 创建 dataset

1. 左侧导航 **Datasets** → **New dataset**
2. Name: `polyarb-l1`（全小写，简单一点）→ 这就是第 3 行
3. Description（可选）：`Polymarket L1 observation daemon stdout JSON logs`
4. Create

### 3.3 创建 API token

1. 左下角点头像 → **Settings** → **API Tokens**（或直接 https://app.axiom.co/<org>/settings/api-tokens）
2. **New API token**
3. Name: `polyarb-l1-ingest`
4. **Permissions**: 勾选你刚建的 `polyarb-l1` dataset 的 **Ingest** 权限（Query 也可以勾上，方便后面 dashboard 用同个 token 查）
5. Expiration: **Never**（或自取，但记得 Plan 07 soak 时不要让它在 7 天内过期）
6. Save → **立刻复制 token**（形如 `xaat-...`）→ 填第 2 行
   ⚠️ **Axiom 创建后只显示一次**，离开页面就找不回，丢了只能重建

### 3.4 测试 ingest

终端跑（替换你的值）：
```bash
TOKEN="xaat-你的token"
DATASET="polyarb-l1"
curl -sS https://api.axiom.co/v1/datasets/$DATASET/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{"_time":"2026-05-16T13:00:00Z","level":"INFO","msg":"axiom smoke test"}]'
```
预期返回类似 `{"ingested":1,"failed":0,...}`。
然后回 Axiom 网页 Datasets → polyarb-l1 应该看见这条 record。

**踩坑提示**：
- ingest 端点必须用 dataset 的 token，**不能用 org-wide token**（org-wide 不能写）
- 如果返回 `403 forbidden` → 检查 token 权限是否包含目标 dataset 的 Ingest

---

## 4. Better Stack（10 分钟）

Better Stack（曾用名 Logtail / Uptime.com）做主动 health check，**Free tier 10 monitors × 30 秒**。

### 4.1 注册

1. 访问 https://betterstack.com/
2. **Sign up free** → 进入 dashboard 首页

### 4.2 创建一个 Heartbeat monitor

这是项目要用的关键 monitor 类型 — 不是 Better Stack 主动 ping 你（那叫 Uptime monitor），而是**你主动 ping Better Stack**（daemon 每次 snapshot 成功后发一次 heartbeat）。Heartbeat 适合 cron 任务监控。

1. 左侧 **Heartbeats** → **Create heartbeat**
2. Name: `polyarb-l1 snapshot tick`
3. **Period**: 期望多久收到一次心跳。Daemon scheduler 是每小时一次 tick → 选 **1 hour**
4. **Grace**: 容忍多久不来。建议 **15 minutes** → 1h + 15min = 75min 没心跳就报警
5. Save → 页面给一个 URL：
   ```
   https://uptime.betterstack.com/api/v1/heartbeat/abcdef1234567890
   ```
   **复制整段 URL** → 填第 4 行

### 4.3 配 Telegram 通知

1. 左下角 **Integrations** → **Telegram**
2. 用你 Wave 4 step 1.1 的 bot token + chat ID（同一个）
3. **Save**
4. 回 Heartbeats，点你刚建的 monitor → **On-call** tab → 把刚建的 Telegram channel 加进 escalation policy

### 4.4 测试

```bash
HEARTBEAT_URL="你的URL"
curl -fsS -X POST "$HEARTBEAT_URL"
```
预期返回空 / 200 OK。Better Stack dashboard 该 monitor 状态变成 **up**。

**踩坑提示**：
- Better Stack 的 **Uptime monitor**（主动 ping）和 **Heartbeat monitor**（被动收）是两个东西，选错了 Plan 05 集成会失败。**必须是 Heartbeat**。
- Free tier 限 10 monitors，到上限新建会被禁，但 L1 阶段 1 个 heartbeat 就够。
- Grace 太短（< 5min）会因网络抖动误报；太长（> 2× period）会漏掉真实事故。15min 是合理起点。

---

## 5. 把凭据塞进 Fly secret store（5 分钟）

**绝对不要把这些值写进 `.env`、`fly.toml`、git 提交**。Fly secrets 加密存储，部署时自动注入容器。

终端跑（一次性贴 6 个 secret）：

```bash
flyctl secrets set \
  POLYARB_SENTRY_DSN="https://abc123@o567.ingest.sentry.io/890" \
  POLYARB_AXIOM_TOKEN="xaat-你的token" \
  POLYARB_AXIOM_DATASET="polyarb-l1" \
  POLYARB_HEARTBEAT_URL="https://uptime.betterstack.com/api/v1/heartbeat/abcdef..." \
  POLYARB_TELEGRAM_BOT_TOKEN="123456:ABCdef..." \
  POLYARB_TELEGRAM_CHAT_ID="987654321" \
  -a polyarb-l1
```

输出应包含：
```
Secrets are staged for the first deployment
```

**触发 redeploy 让 secrets 生效**（不会改 image，只会重启 machine）：

```bash
flyctl deploy --remote-only --wait-timeout 600
```

部署完成后验证：

```bash
flyctl secrets list -a polyarb-l1
```

应该看到 6 个 secret 名（值不会显示，只看名字 + digest）。

---

## 6. 验收清单（贴回我这边）

在 Wave 4 dispatch 之前，请确认：

- [ ] **5 个凭据值都在手**（不需要贴给我，自己保存好）
- [ ] **本地分别测试通过**：
  - [ ] Telegram `curl sendMessage` 收到消息
  - [ ] Sentry DSN 在 https://sentry.io/<你的org>/projects/ 可见
  - [ ] Axiom `curl ingest` 返回 `ingested:1`，dataset 页有记录
  - [ ] Better Stack `curl -X POST $HEARTBEAT_URL` 让 monitor 变 **up**
- [ ] **`flyctl secrets list -a polyarb-l1`** 列出 6 个 secret
- [ ] Sentry 和 Better Stack 都配了 Telegram 通知（这两个会用 step 1 的 bot 直接推消息）

清单全通过后告诉我，我就可以 dispatch `/gsd-execute-phase 02 --wave 4`，executor agent 会把 daemon 代码接进这 4 个 SaaS、加 health check、配 alert rule。

---

## 7. 如果某个 SaaS 你不想用 / 注册失败

| 卡点 | 替代方案 |
|---|---|
| Sentry 不想用 | 暂跳过，Wave 4 dispatch 时显式 `--skip-sentry`（**plan 05 需要修**，工作量小）。但裸 daemon 没异常聚合，**不推荐** |
| Axiom 注册不上 | Better Stack Logs 是次选（更短 retention，但 OK）— 修 plan 05 切换 |
| Better Stack 不想用 | Sentry 自己也能 ping `/health`，但 Free tier 只有 7 天 retention，不如 Better Stack 30 天 |
| Telegram 在中国注册卡 | 可以用 Discord webhook 替代（Sentry / Better Stack 都支持）— 修 plan 05 + 06 |

**不推荐改 plan**。这 4 个的组合是 discuss 阶段权衡过的（D-14~D-17），改栈意味着重新跑 plan check。建议优先解决注册问题。

---

## 8. 成本对照

| 服务 | Free tier 容量 | L1 daemon 实际用量 | 撑得过 7 天 soak? |
|---|---|---|---|
| Sentry | 5k errors/月 | ~10/月（一次 OOM 计 1-2 个） | ✅ 远远 |
| Axiom | 500GB ingest/月 + 30 天 retention | ~50MB/月（loguru JSON log）| ✅ 远远 |
| Better Stack | 10 monitors × 30s | 1 monitor × hourly | ✅ 远远 |
| Telegram | 无限 | ~10 msg/月 | ✅ 无成本 |

**总月开销：$0**。如果未来量级超 free（例如 Sentry 5k errors 撞顶 = 系统真的烂），那是好信号——意味着 L1 该升 Pro 时再说。

---

## 9. 安全注意事项

- **Sentry DSN 包含 secret**，不是公开 URL。`grep -r "sentry.io/" .` 不应出现在 commit 历史
- **Axiom token**：失效后只能重建（旧 token 不可恢复显示）
- **Telegram bot token**：被泄漏 = 任何人能用你的 bot 发消息。如果不慎进 git，**立刻向 @BotFather 发 `/revoke`** + 重建 bot
- **Better Stack heartbeat URL** 是 secret — 任何人知道这个 URL 都能 ping 让 monitor 显示 healthy（假数据）
- 所有 6 个值都进 Fly secrets（加密存储 + 不能 list 值，只能 list 名）

---

## 10. 排查（常见失败）

| 症状 | 排查 |
|---|---|
| `flyctl secrets set` 报 "no auth" | `flyctl auth login` 重登 |
| Telegram `curl` 返回 401 unauthorized | token 抄错了，或没有 `bot` 前缀 |
| Telegram `curl` 返回 400 chat_id wrong | 拿的是 user id 不是 chat id；或忘记给 bot 发过 START |
| Axiom `curl` 返回 403 | token 不带 ingest 权限，或 dataset name 写错 |
| Sentry DSN 测试时报 SSL error | DSN 是 https，不是 http；URL 完整复制 |
| Better Stack heartbeat 一直显示 down | URL 写错了，或 grace 设置太短在你测试间隔大于 grace |

---

写完上面所有步骤后，确认验收清单全勾，告诉我 "Wave 4 SaaS prep done"，我就 dispatch Wave 4。
