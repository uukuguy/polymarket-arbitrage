---
slug: deployment-architecture
title: Cloud-Native Deployment Stack for Polymarket Arbitrage (M1-M3 horizon)
status: drafting
created: 2026-05-11
updated: 2026-05-11
researcher: subagent (parallel-window-B)
---

# Thread: Cloud-Native Deployment Stack

> 选型调研。不锁决策。给用户看完对比表自己定。
>
> 适用 horizon：M1 市场感知（当下 Phase 02+）→ M2 combinatorial → M3 cross-platform/trading
> （6-12 月内含交易执行）。M4/M5 不在本文范围。

---

## 0. 前置硬约束（不可妥协）

来自用户 2026-05-10 / 2026-05-11 会话原话：

> "本地研发 → 一键部署 → 云上 7×24 自主跑"
> "选稳定主流的，价格合适"
> "支付能力：CN + 美区 + PayPal 都可以；启动阶段先用免费额度"
> "未来云上交易执行，必须支持 KMS/Vault/TEE + 私网出站 + 固定 IP 白名单"
> "地区不预判，按'延迟（数据源）/ CN 操控 dashboard 延迟 / 合规'三向量给对比表"

由此推出**5 条硬过滤条件**，任何不满足的栈直接出局：

| # | 硬约束 | 含义 |
|---|---|---|
| F1 | 支持长跑容器 / cron / 后台 worker | L1 每日 8 分钟全量快照、L2 分钟级、未来 L3 WebSocket 持久连接 |
| F2 | Postgres-compatible 关系型 + 易演进到时序 | 现状 SQLite + Parquet，云上换成 Postgres；后续 K 线接 TimescaleDB 或同库内 hypertable |
| F3 | 一键部署（git push / CLI deploy）+ GitOps-friendly | 拒绝 Terraform-only 或控制台戳的方案 |
| F4 | 支持 outbound static IP / 私网出站 / KMS 集成（≥ 演进路径） | 交易执行落地不能重选栈 |
| F5 | 主流稳定（团队 ≥ 50 人 + 公司持续运营 ≥ 3 年 + 文档完整 + 中文社区有人讨论） | 框架启动期不踩冷门坑 |

---

## 0.1 关键事实先固定（一次性写清，后面不重复）

### 0.1.1 Polymarket 服务器实际位置（直接影响地区选型）

**这是本调研最大的方向纠偏点**。早期假设"数据源在美东"是错的。

来源：[NYCServers Polymarket Server Location Guide, 2026-04-07](https://newyorkcityservers.com/blog/polymarket-server-location-latency-guide)

> "Polymarket runs its core trading infrastructure on Amazon Web Services in
> the **eu-west-2 region**, which maps to **London, United Kingdom**. This is
> where the Central Limit Order Book (CLOB) lives, where orders get matched,
> and where your latency clock starts ticking."
>
> "The CLOB API, Gamma API, WebSocket streams, and the Real-Time Data Stream
> (RTDS) all route through the London-based backend."

**实测延迟矩阵**（同上来源）：

| VPS 位置 | 到 Polymarket（London） | 地区封锁 |
|---|---|---|
| Dublin, Ireland | ~1 ms | 不封 |
| London, UK | ~0.5-1 ms | 封（IP 黑名单含 GB） |
| Frankfurt, Germany | ~8-12 ms | 封 |
| Amsterdam, NL | ~10-15 ms（外推） | 不封 |
| New York, US | ~70-80 ms | 封 |
| Singapore | ~200-250 ms | 不封 |
| Tokyo | ~230-260 ms（外推） | 不封 |
| Hong Kong | ~250-290 ms（外推） | 封（多数 HK IP 被识别为受限区） |

**Polymarket 33 国地区封锁**（IP-based，非 KYC）：US / UK / 法国 / 比利时 / 新加坡 / 中国大陆 / 等。"International exchange" 用上面这套封锁；"Polymarket US"（CFTC 监管，2025-12 起）是单独的 invite-only 域名，本调研不涵盖。

**对部署架构的直接含义**：
- L1/L2/L3 数据抓取层 = 不能部署在被封地区（US/UK/CN）→ Dublin/Amsterdam/Frankfurt（注：FRA 被封；改 AMS）/Tokyo/Singapore 都可选
- Dashboard 监控网站 = 可以放任何地方（不调用 Polymarket API）；考虑 CN 本人访问延迟即可
- 交易执行（M3）= 强约束在 **Dublin / Amsterdam** 两地（同时满足"低延迟到 London"+"非封锁区"）

### 0.1.2 Polymarket Auth = EOA 私钥（无法换 KMS-only 模式）

来源：[Polymarket Authentication Docs](https://docs.polymarket.com/api-reference/authentication)

下单签名是 **EIP-712 L1 + HMAC-SHA256 L2** 两层。L1 必须有 secp256k1 私钥能签消息；可以 BYOK（自带私钥）也可以放 AWS KMS / Vault / HSM 用其签名 API 间接签（kaleido-io/vault-plugin-secrets-ethsign、Nethereum Cloud KMS Signing 都是成熟方案）。

**含义**：trading-readiness 维度不是"找一个有 KMS 的栈"，是"找一个允许应用以最小权限调用外部 KMS"的栈 — 几乎所有候选都行（AWS KMS API 走 HTTPS）。**真正卡的是私网出站固定 IP 白名单**，因为 Polymarket Cloudflare 限流是按出口 IP 算的。

### 0.1.3 Polymarket Cloudflare 速率限制

[NYCServers, 2026-04](https://newyorkcityservers.com/blog/polymarket-server-location-latency-guide)：

> "All rate limits are enforced via Cloudflare throttling... 3,500 req/10s
> for order placement, 3,000 req/10s for cancellations, 9,000 req/10s general
> CLOB operations."

→ 出口 IP **必须固定且能被白名单**（向 Polymarket 申请提速时），所有"出口 IP 随机轮换"的 serverless 方案直接死。

---

## 1. 评估维度（6 个）

### 1.1 compute（长跑容器 / serverless / 调度）

**为什么重要**：
- L1 daily snapshot：每天 1 次、~8 分钟、~20000 条市场数据 → 定时 worker，不需要 always-on，但需要确定能跑满 8 分钟（不被 serverless 函数超时切断）
- L2 定向跟踪：分钟级，always-on worker
- L3 K 线 WebSocket：always-on persistent connection（如果断开会丢数据）
- Dashboard backend：always-on HTTP，低 QPS

**判别要点**：
- 支持 always-on 长跑容器？（否则 L2/L3 死）
- 支持 cron / scheduled jobs？（L1 命脉）
- 支持 background worker（无入站 HTTP 的进程）？（L2 干净跑法）
- 单次执行时长上限（serverless 函数往往 5-15 min 截断）

### 1.2 db（关系型 + 时序）

**为什么重要**：
- 当下 schema 是 SQLite snapshot 行 + Parquet 冷存档 → 云上换成 Postgres
- 未来 K 线 = 高 ingest 时序数据 → 用 TimescaleDB hypertable 或单独时序库（InfluxDB / QuestDB / TigerData）

**关键分歧**：Postgres + TimescaleDB extension 跑同一库（运维省事）vs 拆两库（业务清晰但同步成本）。**本调研推荐合并**（Phase 1 验证）→ 选项收敛到"有 TimescaleDB extension 的 Postgres 托管服务"。

**判别要点**：
- Postgres 14+ 支持？
- TimescaleDB extension 是否启用（自托管要装；托管平台要看是否官方支持）
- 是否分支（Neon 优势）/ PITR / 自动备份 / 读副本
- 数据库连接来源 IP 是否可控（影响 trading-readiness 维度）

### 1.3 observability（log + metrics + alerting + uptime）

**为什么重要**：
- L1 每日跑要立刻知道"今天是不是漏了 100 条市场"
- L2/L3 worker 挂了要 P95 < 5 min 收到告警
- 监控 dashboard 自己也要被监控（uptime ping）

**判别要点**：
- log 聚合 + 全文搜索（CN 友好优先）
- metrics + Grafana-style 可视化
- alerting 渠道（email/PagerDuty/Slack/微信？）
- uptime monitoring + status page
- 价格随 ingest 量增长可控（不是"日志多 10x 价格 10x"）

### 1.4 deployment（一键部署 / GitOps）

**为什么重要**：
- 用户明确要求"本地研发 → 一键部署 → 云上 7×24"
- 拒绝 Terraform/Ansible/手工 SSH 流派
- 必须有 `git push` 自动构建 + 部署 + rollback

**判别要点**：
- git push to deploy（GitHub/GitLab 接管）？
- preview / staging environment？
- rollback 一键？
- Docker / buildpack / Dockerfile 自由度
- secrets 管理（不能让用户在仓库 commit .env）

### 1.5 dashboard（监控网站托管）

**为什么重要**：
- 用户用 dashboard 看快照状态、流动性变化、价格漂移
- **dashboard 操控位置 = 中国大陆**（用户人在 CN）

**判别要点**：
- 静态 + SSR 都支持？
- CN 访问延迟 / 不被墙
- HTTPS / 自定义域名免费
- 与 compute / db 同栈集成（避免跨栈 CORS 麻烦）

### 1.6 trading-readiness（私钥管理 / 私网出站 / 固定 IP）

**为什么重要**：
- 6-12 月内会上线交易执行，**栈选错就要迁移**
- 私钥不能落明文 → 选 AWS KMS / GCP KMS / HashiCorp Vault Cloud / Fly tokens secrets 等
- 出口 IP 必须固定（Polymarket Cloudflare rate-limit 白名单）
- 私网出站（compute → KMS → Polymarket）必须能配，不允许 random 出口

**判别要点**：
- 是否有 dedicated egress IP（绑应用、稳定不变）
- 是否能装 secrets / KMS 客户端（一般都行，看 SDK 友好度）
- VPC peering / Wireguard / 私网通信
- 与主流 KMS（AWS KMS / GCP KMS）跨云访问的网络可行性

---

## 2. 候选栈对比矩阵

### 2.1 Compute / PaaS 类（5 个候选）

#### 2.1.1 Fly.io

| 项 | 内容 |
|---|---|
| 定价（2026-05） | Pay-as-you-go，**已无免费层**（2024-10 移除）。小型 shared-cpu-1x（1 vCPU + 256MB RAM）约 $1.94/月 24×7；shared-cpu-1x@1GB ≈ $5.70/月；shared-cpu-2x@2GB ≈ $15.55/月；performance-2x@4GB ≈ $62/月 |
| 入门门槛 | $5 trial credit 一次性；个人无固定订阅，按用量计费 |
| Volumes | $0.15/GB/月（一直计费即使机器停） |
| 出口流量 | $0.02/GB（北美/欧洲），$0.04/GB（亚太） |
| Dedicated IPv4 | $2/月一个（公网入站用） |
| Static egress IPv4 | $3.60/月一个（**这是交易执行白名单关键**） |
| 全球区域 | 35+ 地区，含 LHR / AMS / FRA / CDG / SIN / NRT / HKG |
| 区域筛 | **AMS（Amsterdam）和 DUB Fly 没有 → 用 LHR 不行（被封）→ 用 AMS 是最佳折中** |
| 私网 | 每 org 自动 6PN 私网 + 全 region WireGuard 接入 |
| Cron | 内置 scheduled machines + `[mounts]` 持久卷 |
| WebSocket | 原生支持长连接 |
| 长跑任务超时 | 无（Machine 是真容器，进程多长跑多长） |
| KMS-friendly | secrets 内置；AWS KMS / GCP KMS / Vault Cloud 都能从应用调用 |
| Trading-readiness | ⭐⭐⭐⭐⭐ — **本类最强**：static egress IP + 私网 + 多地区 + 已经被很多 web3 项目用 |
| CN 友好（支付） | 信用卡（含国内双币）/ Stripe / PayPal 走 Stripe；CN 卡偶有拒付但可通过 |
| CN 友好（控制台访问） | ✅ 控制台和 docs 不被墙 |
| 成熟度 | 公司成立 2017，pay-as-you-go 2024 改版后稳定 |
| 主要槽点 | 2024 free tier 砍掉后口碑下滑；新用户没有 trial 太多；偶发个别 region 退役 |
| 文档来源 | [fly.io/docs/about/pricing](https://fly.io/docs/about/pricing)（2026-05 访问） [fly.io/docs/networking/egress-ips](https://fly.io/docs/networking/egress-ips) |

#### 2.1.2 Render.com

| 项 | 内容 |
|---|---|
| 定价（2026-04-23 新版） | Hobby workspace 免费；服务实例分档：Free $0、Starter $7、Standard $25、Pro $85、Pro Plus $175、Pro Max $225、Pro Ultra $450 |
| 免费层细节 | Free Web Services：512MB RAM / 0.1 CPU，**90 天无访问会休眠**（启动期可用），不能跑 background worker（worker 没有免费层） |
| Postgres | Free $0（30 天到期重建）/ Basic-256mb $6 / Basic-1gb $19 / Basic-4gb $75 / Pro-4gb $55 / Pro-8gb $100 |
| Cron Jobs | Pay-per-minute（Starter $0.00016/min ≈ $7/月 24×7） |
| Background Workers | $7+/月（无免费层） |
| 区域 | 5 个：Oregon / Ohio / Virginia / Frankfurt / Singapore |
| 区域筛 | **Frankfurt 被封；Singapore 高延迟（~200ms）；US 三区被封** → 没有可用区！⚠️ |
| Static outbound IP | 内置（Workspace 级别共享 NAT，固定 IP 范围，2025-11 完成切换） |
| Cron 超时 | 无明确硬上限（按执行时长扣费） |
| KMS-friendly | 支持 secrets / env vars；外部 KMS API 可调 |
| Trading-readiness | ⭐⭐ — **没合规区域是致命伤**（除非 Render 加 Dublin/Amsterdam，否则只能用作 dashboard 后端） |
| CN 友好（支付） | Stripe（普通信用卡 OK） |
| CN 友好（控制台访问） | ✅ |
| 成熟度 | 公司成立 2019，2026-04-23 大改了 workspace plan（取消 seat fee） |
| 主要槽点 | 区域少且全部不适合 Polymarket 数据层；近几次涨价不友好；Free Postgres 30 天到期重建（启动期已无法接受） |
| 文档来源 | [render.com/pricing](https://render.com/pricing)（2026-05 访问） [render.com/docs/regions](https://render.com/docs/regions) [render.com/blog/better-pricing-for-fast-growing-teams](https://render.com/blog/better-pricing-for-fast-growing-teams) |

#### 2.1.3 Railway.app

| 项 | 内容 |
|---|---|
| 定价 | Hobby $5/月（含 $5 usage credit）/ Pro $20/月/座位（含 $20 usage credit）/ 资源用量按秒计 |
| 单价 | $20/vCPU/月 + $10/GB-RAM/月（接近 Fly.io、比 Render 贵） |
| Free | 无免费层；新账号 $5 一次性 trial |
| 区域 | 4 个：US-West (Oregon) / US-East (Virginia) / EU-West (Amsterdam) / Asia-Southeast (Singapore) |
| 区域筛 | **Amsterdam（EU-West）可用**（~10-15ms 到 London 且不被封）✅ |
| Postgres | $5 起，按用量；无 TimescaleDB extension 官方支持（社区镜像可自管） |
| Cron / Worker | 原生 cron 模板 + always-on service |
| Static outbound IP | Pro plan 内置（2025 年加入），但**不保证固定到具体 IP**（NAT egress pool） |
| 长跑任务 | 无超时限制 |
| KMS-friendly | secrets 内置；外部 KMS API 走标准网络 |
| Trading-readiness | ⭐⭐⭐ — Amsterdam 区域是亮点；但 egress IP 不是单 IP 固定（NAT pool），白名单可能麻烦；BYOC 不支持 |
| CN 友好（支付） | Stripe；2024-2025 一些用户反馈 CN 卡有拒付，但 PayPal 走通了 |
| CN 友好（控制台访问） | ✅ |
| 成熟度 | 公司成立 2020，2023 砍 free tier 后引起反弹；目前稳定 |
| 主要槽点 | 比 Fly 贵；egress IP NAT pool 不是单 IP；近年涨价快 |
| 文档来源 | [railway.com/pricing](https://railway.com/pricing)（2026-05 访问） [docs.railway.com/pricing/plans](https://docs.railway.com/pricing/plans) |

#### 2.1.4 DigitalOcean App Platform

| 项 | 内容 |
|---|---|
| 定价 | App Platform：Basic $5/月（512MB / 1 vCPU 共享）/ Pro $12+/月（dedicated）/ Function $0+/月按调用 |
| Droplet（VPS 自管路线） | Basic $4-12（共享 vCPU）/ General Purpose $63+ / 2026-01 起改秒级计费 |
| Managed Postgres | $15/月（1 vCPU / 1GB / 10GB SSD）起 / $60 起 2vCPU/4GB；不支持 TimescaleDB extension 官方 |
| 区域 | 14+：含 NYC / SFO / LON / FRA / AMS / TOR / SGP / SYD / BLR |
| 区域筛 | **AMS（Amsterdam）+ LON（London 但被封）+ TOR（Toronto，非封锁区，~70ms 到 London）**；AMS 是首选 ✅ |
| Static outbound IP | Reserved IP 免费（绑 Droplet），App Platform Pro 也支持 |
| Cron | App Platform 原生 worker + scheduled jobs |
| KMS-friendly | 自家 Spaces + Secrets manager；外部 AWS KMS 走 HTTPS |
| Trading-readiness | ⭐⭐⭐⭐ — Amsterdam + Toronto 都可用；Reserved IP 成熟；但需要 Droplet 路线（App Platform 抽象稍重） |
| CN 友好（支付） | Stripe + **PayPal 直接支持**；CN 卡兼容好 |
| CN 友好（控制台访问） | ⚠️ 偶有访问慢（无封禁但 RTT 高） |
| 成熟度 | 公司成立 2011 上市公司；最稳健 |
| 主要槽点 | 平台不如 Fly/Railway 现代；App Platform 文档不如 Droplet 详尽；2026 大改 AI 重点偏移 |
| 文档来源 | [digitalocean.com/pricing/app-platform](https://www.digitalocean.com/pricing/app-platform)（2026-05 访问） [docs.digitalocean.com/platform/regional-availability](https://docs.digitalocean.com/platform/regional-availability) |

#### 2.1.5 Hetzner Cloud（自管 VPS 路线）

| 项 | 内容 |
|---|---|
| 定价（2026-04-01 涨价后） | CPX11 (2vCPU/2GB) ≈ €5.99 / CPX21 (3vCPU/4GB) ≈ €9.99 / CPX31 (4vCPU/8GB) ≈ €17.99 / CCX13 EU (2vCPU/8GB dedicated) ≈ €16.49 / CCX13 US (2vCPU/8GB) ≈ €16.99 |
| ARM 路线 | CAX 系列（仅 EU）：CAX11 (2vCPU/4GB) ≈ €3.99 — 单核性能不如 x86，但 4 vCPU + 8GB 仅 ≈ €5.49 性价比惊人 |
| 流量 | 20TB 包含 |
| 区域 | 5 个：Falkenstein / Nuremberg（DE）/ Helsinki（FI）/ Hillsboro Oregon（US）/ Ashburn Virginia（US）/ Singapore |
| 区域筛 | **Helsinki（FI）= 非封锁 EU，到 London ~25-30ms** 是次优选；DE 双区被封；US/SGP 不合适 ⚠️ |
| Managed Postgres | **不提供** — 必须自己装（Docker Postgres / 一键 app） |
| Cron | 系统 cron / systemd timers / Docker Compose |
| Static outbound IP | 每 VPS 自动有固定公网 IPv4，0 成本 |
| KMS-friendly | 装 Vault server 自己跑（约 +$3/月 SDN），或外部 AWS KMS API |
| Trading-readiness | ⭐⭐⭐ — IP 固定且免费；但所有 PaaS 抽象自己搭（监控/部署/HA） |
| CN 友好（支付） | SEPA / 信用卡 / PayPal；**CN 卡偶尔被拒**，CN 用户反馈 PayPal 是最稳路径 |
| CN 友好（控制台访问） | ✅ 控制台快；docs 不被墙 |
| 成熟度 | 公司成立 1997（德国电信级老牌） |
| 主要槽点 | **2026-04-01 涨价高达 50%**（业内话题）；DIY 程度高，不是 PaaS；客服评价两极 |
| 文档来源 | [hetzner.com/cloud](https://www.hetzner.com/cloud) [hetzner.com/cloud/regular-performance](https://www.hetzner.com/cloud/regular-performance) [theregister.com/.../hetzner-50-price-hike-no-fooling-from-april-1st](https://www.theregister.com/on-prem/2026/02/24/hetzner-50-price-hike-no-fooling-from-april-1st/4547119) |

#### 2.1.6 Northflank

| 项 | 内容 |
|---|---|
| 定价 | Sandbox 免费（2 services + 2 jobs + 1 addon）；Production: $0.0446/vCPU-hour + $0.0149/GB-RAM-hour ≈ Fly-tier 价格 |
| 单价测算 | 1 vCPU + 2GB RAM 24×7 ≈ $54/月（比 Fly 同规格贵 ~30%） |
| BYOC | **本类最强**：自助 BYOC to AWS / GCP / Azure / Oracle / 自托管，600+ region 可用 |
| 区域（managed） | 6+，含 EU-West / US-East / Asia |
| 区域筛 | BYOC 路线 → 可以接 **AWS eu-west-1 (Dublin)**，**这是除 VPS 外唯一能直接命中 Dublin 的 PaaS 抽象** ✅ |
| Postgres | 内置 addon（Postgres / Redis / MongoDB / Memcache），$3.50+/月起 |
| Cron / Worker | 原生 jobs（cron + one-off） + services（always-on） |
| Static outbound IP | BYOC 模式下用 AWS 自己的 EIP；managed 模式下需 Production plan |
| KMS-friendly | secrets 内置；BYOC 直接用底层 cloud KMS（AWS KMS / GCP KMS） |
| Trading-readiness | ⭐⭐⭐⭐⭐ — BYOC = AWS EIP + AWS KMS + VPC peering 全套，演进上限最高 |
| CN 友好（支付） | Stripe |
| CN 友好（控制台访问） | ⚠️ 偶有抖动，控制台不被墙 |
| 成熟度 | 公司成立 2019；BYOC 是 2024 起的核心卖点；team < 100，比 Fly/Render 小 |
| 主要槽点 | 小众（用户群比 Fly/Railway 小一个数量级）；学习曲线略陡；非 BYOC 模式 region 有限 |
| 文档来源 | [northflank.com/pricing](https://northflank.com/pricing) [northflank.com/product/bring-your-own-cloud](https://northflank.com/product/bring-your-own-cloud) |

### 2.1.7 Compute 类总览汇总

| 候选 | 起步价 | 主要 region 合规 | Egress IP | 长跑 cron | Trading-readiness | CN 友好综合 | 命中度 |
|---|---|---|---|---|---|---|---|
| Fly.io | $5+ | AMS ✅ | $3.60/月 dedicated ✅ | ✅ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | **首选** |
| Render | $7+ | ❌ 全部封 / 远 | NAT pool ✅ | 仅 Cron Jobs | ⭐⭐ | ⭐⭐⭐⭐ | dashboard only |
| Railway | $5+ | AMS ✅ | NAT pool ⚠️ | ✅ | ⭐⭐⭐ | ⭐⭐⭐ | 备选 |
| DO App Platform | $5+ | AMS ✅ TOR ✅ | Reserved IP ✅ | ✅ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | **强首选** |
| Hetzner | €4+ | HEL ✅（弱） | 免费固定 ✅ | DIY | ⭐⭐⭐ | ⭐⭐⭐ | 省钱路线 |
| Northflank | $0/$54+ | BYOC = AWS Dublin ✅✅ | BYOC EIP ✅ | ✅ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | 长期上限路线 |

---

### 2.2 Database 类（6 个候选）

#### 2.2.1 Supabase

| 项 | 内容 |
|---|---|
| 定价（2026-05） | Free $0 / Pro $25/月（含 $10 compute credit）/ Team $599/月 |
| Free 限制 | 500MB DB / 1GB storage / 5GB egress / 50k MAU；**1 周无访问会暂停项目** ⚠️ |
| Pro 包含 | 8GB DB / 100GB storage / 250GB egress / 7-day backup |
| Compute add-on | Nano 共享（免费）/ Micro $10/月 / Small $25/月 / Medium $60/月 / Large $110/月 / XL $210/月... 直到 64-core $3700/月 |
| 区域 | 12+，含 eu-west-1 (Dublin) / eu-west-2 (London) / eu-central-1 (Frankfurt) / ap-northeast-1 (Tokyo) / ap-southeast-1 (Singapore) / us-east-1 / us-west-1 |
| 区域筛 | **eu-west-1 Dublin** 完美命中 ✅✅ |
| TimescaleDB | **不支持**（Supabase 用 Postgres + 自家扩展集合，不含 timescaledb extension） |
| 附加价值 | Auth / Storage / Realtime / Edge Functions 都打包好（**dashboard 可用 supabase-js 直接连，省一层后端**） |
| 一键部署 | CLI（`supabase` CLI）+ migrations |
| KMS-friendly | env vars 加密；自家 Vault；外部 KMS 可调 |
| Trading-readiness | ⭐⭐⭐⭐ — 区域完美；2026-03 Supabase 自身遇过区域 ISP block 事件（[supabase.com/blog/navigating-regional-network-blocks](https://supabase.com/blog/navigating-regional-network-blocks)），但 Dublin 不在其中 |
| CN 友好（支付） | Stripe；CN 卡多数过 |
| CN 友好（控制台访问） | ⚠️ 2026-03 中国大陆部分 ISP 出现间歇性 block；移动用户更明显，电信稍好；**dashboard 必须挂代理或走 CN 镜像** |
| 成熟度 | 公司成立 2020；GA 已 3 年；YC 公认 PostgreSQL BaaS 一线 |
| 主要槽点 | Free 项目 1 周不用就暂停（启动期开发节奏要注意）；TimescaleDB 不支持是真伤 |
| 文档来源 | [supabase.com/pricing](https://supabase.com/pricing) [supabase.com/docs/guides/platform/regions](https://supabase.com/docs/guides/platform/regions) |

#### 2.2.2 Neon (Databricks)

| 项 | 内容 |
|---|---|
| 定价（2025-11 后） | Free $0 / Launch $19/月 / Scale $69/月 |
| Free 限制 | 0.5GB storage / 100 CU-hours/月 compute / 自动 scale-to-zero / 10 branches |
| Launch | 5GB included + $0.106/CU-hr + $0.35/GB-month |
| Scale | 100GB included + $0.222/CU-hr |
| Compute Unit | 1 CU ≈ 1 vCPU + 4GB RAM |
| 区域 | AWS：us-east-1 / us-east-2 / us-west-2 / eu-central-1 (Frankfurt) / eu-west-2 (London) / ap-southeast-1 (Singapore) / ap-southeast-2 (Sydney) ；Azure：east-us-2 / east-us / west-us-3 / west-europe |
| 区域筛 | **没有 Dublin / Amsterdam / Tokyo**；最佳折中是 **eu-west-2 (London)** — 注意：DB 部署在被封地区不影响应用层访问（DB 自己不调 Polymarket），但 compute → DB 跨区延迟要考虑 |
| TimescaleDB | **不支持** |
| 杀手特性 | 数据库分支（git-like）+ scale-to-zero（开发期省钱）+ serverless HTTP driver |
| 一键部署 | CLI + Vercel/Fly 集成模板 |
| KMS-friendly | env vars；外部 KMS 可调 |
| Trading-readiness | ⭐⭐⭐ — 适合 OLTP，但**长跑 8 分钟全量写入**可能频繁触发 cold-start（CU-hour 计费可能爆） |
| CN 友好（支付） | Stripe |
| CN 友好（控制台访问） | ✅ |
| 成熟度 | 2021 创立 → 2024-2025 被 Databricks 收购 → 2025-11 改价（降价 15-25%） |
| 主要槽点 | scale-to-zero 第一次连接有 cold start（~1-3s）；**长跑写入场景计费可能不友好**（CU-hr 按用量） |
| 文档来源 | [neon.com/pricing](https://neon.com/pricing) [neon.com/docs/introduction/regions](https://neon.com/docs/introduction/regions) |

#### 2.2.3 Render Postgres

| 项 | 内容 |
|---|---|
| 定价 | Free $0 (30-day) / Basic-256mb $6 / Basic-1gb $19 / Pro-4gb $55 / Pro-8gb $100 / Pro-16gb $200 |
| 区域 | 同 Render compute 5 区 |
| 区域筛 | 同 Render — **没有合适区** |
| TimescaleDB | 不支持 |
| Trading-readiness | ⭐⭐ |
| 推荐度 | 仅当 compute 也用 Render 时；本调研不主推 |

#### 2.2.4 TimescaleDB Cloud (Tiger Cloud)

| 项 | 内容 |
|---|---|
| 定价 | 三档：Performance $30/月起 / Time-series $36/月起 / 自定义；按 vCPU + memory + storage 拆 |
| 实例参考 | 1 vCPU/2GB ≈ $30/月 + storage $0.215/GB-mo / 2 vCPU/8GB ≈ $122/月 / 4 vCPU/16GB ≈ $244/月 |
| 免费 | 30 天 trial（$300 credit） |
| 区域 | AWS：us-east-1 / us-east-2 / us-west-2 / **eu-west-2 (London)** / eu-central-1 / eu-west-1 (Dublin?) / sa-east-1 / **ap-northeast-1 (Tokyo)** / ap-southeast-2 (Sydney) / ca-central-1 |
| 区域筛 | **Dublin / Tokyo 都可用** ✅ |
| TimescaleDB | **是它的本职** — hypertable / 95% 压缩 / continuous aggregates / 时序数据原生最优 |
| Compute 路线 | 不提供（纯 DB） |
| 一键部署 | psql 标准客户端；schema 用 migrations 工具（Atlas / Sqitch / Flyway） |
| KMS-friendly | 标准 Postgres SSL；env-based 连接串 |
| Trading-readiness | ⭐⭐⭐⭐ — 时序场景一流；K 线落地时绝佳 |
| CN 友好（支付） | Stripe |
| CN 友好（控制台访问） | ✅ |
| 成熟度 | 公司 Timescale 2017 创立 → 2025 改名 Tiger Data；时序圈一线 |
| 主要槽点 | 起步价 $30 比 Supabase Pro ($25) 高，且没有 Auth/Storage 等附加值；纯 DB 不解决 dashboard 后端问题 |
| 文档来源 | [tigerdata.com/pricing](https://www.tigerdata.com/pricing) [tigerdata.com/docs/learn/tiger-cloud/regions](https://www.tigerdata.com/docs/learn/tiger-cloud/regions) |

#### 2.2.5 Fly Postgres (managed)

注：Fly.io 早期有 managed Postgres，**已弃用为社区维护的 Fly Apps 模板**。现在推荐的是：
- 在 Fly 上自托管 Postgres + TimescaleDB（运维成本高，HA 自己管）
- 用 Fly + 外部 Postgres（Neon / Supabase / Tiger）

不再独立列。

#### 2.2.6 Hetzner 自托管 Postgres + TimescaleDB

| 项 | 内容 |
|---|---|
| 定价 | Postgres + Timescale 在 CPX21 (3vCPU/4GB) ≈ €9.99；CCX13 EU (2vCPU/8GB dedicated) ≈ €16.49 |
| 区域 | Helsinki / Nuremberg / Falkenstein / Oregon / Ashburn / Singapore |
| 区域筛 | Helsinki ✅（次优选；~25ms 到 London 且非封） |
| TimescaleDB | 自己装 `apt install timescaledb-2-postgresql-16` |
| 备份 / HA | 自己搞（pgBackRest + Streaming replication） |
| Trading-readiness | ⭐⭐⭐ — IP 固定；但 HA / 备份 / PITR 全自动化要 1-2 人周 |
| 推荐度 | 仅当用户对 Postgres 运维有信心时；启动期不推荐 |

### 2.2.7 Database 类总览汇总

| 候选 | 起步价 | 命中区域 | TimescaleDB | 附加价值 | 启动期适配 |
|---|---|---|---|---|---|
| Supabase | $0 / $25 | Dublin ✅✅ | ❌ | Auth + Storage + Realtime | **L1/L2 首选**（Pro 起步） |
| Neon | $0 / $19 | London（次优） | ❌ | DB 分支 + scale-to-zero | dev/staging 神器；prod 需测 |
| Render PG | $6+ | ❌ | ❌ | 集成 Render | 仅当 compute 也用 Render |
| Tiger Cloud | $30+ | Dublin ✅ Tokyo ✅ | ✅✅ | 时序专精 | **L3 K 线接入时切换** |
| Fly Postgres | (自托) | 35+ | DIY | 同 Fly 栈 | 不推荐启动期 |
| Hetzner 自托 | €10+ | HEL ✅（弱） | DIY | 最省钱 | 不推荐启动期 |

**关键决策（DB 维度）**：
- L1/L2 阶段（snapshot + 候选池）→ **Supabase Pro (Dublin) $25/月** 是甜蜜点
- L3 阶段（K 线时序）→ **Tiger Cloud Performance (Dublin/Tokyo) $30-60/月** 单独跑或迁移
- 早期免费验证 → **Neon Free + Supabase Free** 双栈并跑（注意各自暂停规则）

---

### 2.3 Observability 类（5 个候选）

#### 2.3.1 Better Stack

| 项 | 内容 |
|---|---|
| 定价 | Free / Logs+Uptime 起步 $29/月（年付 $24/月）/ metrics: $0.50/GB/月 (annual) 或 $0.75/GB/月 (monthly) |
| Free 包含 | 3GB logs (3 day retention) + 30GB metrics + 3GB web events + 50 uptime monitors（10 个的 30s interval） |
| 杀手特性 | log + uptime + status page + on-call 一站式（替代 Datadog + PagerDuty + Statuspage 三个工具） |
| CN 友好（推送） | 邮件 / Slack / Discord / Telegram / SMS / Webhook；**没有微信/钉钉直接集成**（要走 webhook 中转） |
| 控制台 | ⚠️ 在 CN 偶尔慢但能访问 |
| 成熟度 | 公司成立 2021（前身 Better Uptime）；社区口碑好；2025-2026 大量从 Datadog 迁过来的用户 |
| 推荐度 | **启动期 + 长期** 都推荐 |
| 文档来源 | [betterstack.com/pricing](https://betterstack.com/pricing) |

#### 2.3.2 Grafana Cloud

| 项 | 内容 |
|---|---|
| 定价 | Free 永久（无信用卡）/ Pro $19/月/座 + usage |
| Free 包含 | 10k metrics series / 50GB logs / 50GB traces / 14 day retention / unlimited dashboards |
| 杀手特性 | Grafana UI 业内标准；OpenTelemetry 原生；与所有云栈互通 |
| CN 友好（推送） | 类似 Better Stack；无微信直集成 |
| 控制台 | ✅ |
| 成熟度 | 公司成立 2014 上市公司 |
| 推荐度 | **如果走自托管 + Hetzner**，自己起 Grafana + Loki + Prometheus 是省钱方案；如果走云栈，Grafana Cloud Free 是最慷慨的免费层 |
| 文档来源 | [grafana.com/pricing](https://grafana.com/pricing) |

#### 2.3.3 Axiom

| 项 | 内容 |
|---|---|
| 定价 | Free 永久 / Pro $25/月 / Team |
| Free 包含 | **500GB/月 ingest + 30 天 retention + 无限用户**（业内最慷慨） |
| 杀手特性 | 纯 logs/traces 专精；APL 查询语言；与 Vercel/Next.js 一等公民集成 |
| CN 友好（推送） | webhook / 集成 OpsGenie / PagerDuty 等 |
| 控制台 | ✅ |
| 成熟度 | 公司成立 2020；YC 出品；2024-2026 快速崛起 |
| 推荐度 | **启动期日志单点最强**；不做 uptime，要配 Better Stack 或 UptimeRobot |
| 文档来源 | [axiom.co/pricing](https://axiom.co/pricing) |

#### 2.3.4 Sentry

| 项 | 内容 |
|---|---|
| 定价 | Developer $0（5k errors + 10k perf units + 1 user）/ Team $26/月（50k errors）/ Business $80/月 |
| 杀手特性 | error tracking 业内标准；source map / stack trace / 自动分组 |
| CN 友好（推送） | webhook / Slack / 邮件 |
| 控制台 | ✅ |
| 成熟度 | 公司成立 2012 上市；几乎所有 SaaS 都用 |
| 推荐度 | **必装** — 不管哪个栈，Sentry 都进 |
| 文档来源 | [sentry.io/pricing](https://sentry.io/pricing) |

#### 2.3.5 自托管 Grafana + Prometheus + Loki

仅当走 Hetzner 路线时考虑。单 CPX21 (€10/月) 跑 + 本地 SQLite/SSD 持久化 = 0 额外 SaaS 成本。运维成本：约 4-8 小时初始 + 每月 1-2 小时维护。**不推荐启动期**。

### 2.3.6 Observability 总览汇总

| 候选 | Free 慷慨度 | 一站式覆盖 | 起步付费 | 长期可扩 | 推荐组合 |
|---|---|---|---|---|---|
| Better Stack | 中（3GB logs / 50 uptime） | logs + uptime + status + on-call | $24/月 (annual) | 中 | 启动期 + $30 档主选 |
| Grafana Cloud | 高（10k metrics + 50GB logs/traces） | metrics + logs + traces | $19/月 | 高 | $100+ 档主选；BYO 路线必选 |
| Axiom | **极高（500GB logs ingest）** | logs only | $25/月 | 高 | 启动期日志专精 |
| Sentry | 中（5k errors） | errors only | $26/月 | 高 | **必装** |
| 自托管 LGTM | 无限 | 全栈 | 仅 VPS 钱 | 高 | 仅 Hetzner 路线 |

**启动期堆叠建议**：Axiom Free（logs）+ Sentry Free（errors）+ Better Stack Free（uptime）= $0/月覆盖三件套。

---

### 2.4 Dashboard 托管类（4 个候选）

#### 2.4.1 Vercel

| 项 | 内容 |
|---|---|
| 定价 | Hobby $0（**仅非商业用途** — 套利系统属商业）/ Pro $20/月/座位 |
| Pro 包含 | 1TB bandwidth / 1000 GB-Hours fast compute / unlimited projects |
| 区域 | 全球 anycast（Polymarket 自己 dashboard 也用 Vercel） |
| 杀手特性 | Next.js 一等公民；零配置；preview deployment 每个 PR 一个；Edge Functions |
| CN 友好（访问） | ⚠️ **CN 直连有时变慢/丢包**；过去几年偶发被部分省 ISP 间歇 block；建议自定义域名 + Cloudflare 代理 |
| KMS-friendly | env vars / 集成 AWS / GCP / Vault Cloud |
| 推荐度 | **dashboard 首选**（如果接受 CN 访问需挂代理） |
| 文档来源 | [vercel.com/pricing](https://vercel.com/pricing) |

#### 2.4.2 Netlify

类似 Vercel；Free $0 / Pro $19/月；JAMstack 派系老牌；同样 CN 访问问题。**Vercel 替代品**，不重点列。

#### 2.4.3 Cloudflare Pages / Workers

| 项 | 内容 |
|---|---|
| 定价 | Free 永久 / Workers Paid $5/月起 |
| Free 包含 | 100k requests/day / 10ms CPU/request / 5GB storage / unlimited bandwidth |
| Pages Functions | 跟随 Workers free / paid |
| 杀手特性 | **CN 友好** — Cloudflare 是 CN 用户访问相对最稳的境外 CDN（部分省份仍有抖动但比 Vercel 好） |
| 区域 | 全球 anycast；CN 节点（部分）在 JD Cloud / China Telecom 合作下；非商业付费可用 |
| 长跑 | 不行（Workers 单请求 30s CPU / 15min real time）→ **只做 dashboard，不做 L1/L2/L3** |
| 推荐度 | **dashboard 强备选** — CN 访问体验上首选 |
| 文档来源 | [developers.cloudflare.com/workers/platform/pricing](https://developers.cloudflare.com/workers/platform/pricing) [cloudflare.com/plans/developer-platform](https://www.cloudflare.com/plans/developer-platform) |

#### 2.4.4 同栈内 web 服务（Fly / Render / DO App Platform）

可以直接用 compute 栈跑 dashboard 后端 + 静态文件托管。**优势**：不跨栈，CORS 简单，secrets 复用。**劣势**：CDN 不如 Vercel/Cloudflare 快。

**适用场景**：dashboard 只给自己看（CN 一个用户），不需要全球 CDN。

### 2.4.5 Dashboard 总览汇总

| 候选 | 免费 | CN 访问 | 适用 |
|---|---|---|---|
| Vercel | $0 (非商业) | ⚠️ 抖动 | Next.js dashboard，配 CF 代理 |
| Netlify | $0 | ⚠️ 抖动 | 静态 dashboard |
| Cloudflare Pages | $0 (慷慨) | ✅ **CN 最优** | dashboard 首选（CN 用户） |
| 同栈 web | 已在 compute 预算内 | 取决于 compute 区 | 不需要 CDN 时 |

---

## 3. 推荐组合（4 档预算）

### 3.1 免费堆叠（$0/月，启动期 1-3 个月）

> 验证可行性 / 跑通 L1 / 学清楚部署流程的最低成本组合。

| 维度 | 服务 | 月成本 | 备注 |
|---|---|---|---|
| Compute | **Fly.io $5 trial credit** 一次性，shared-cpu-1x@256MB | $0（前 $5）→ $2-3 | L1 daily snapshot；启动期单实例足够 |
| Database | **Supabase Free** (Dublin region) | $0 | 500MB DB；注意 1 周不用会暂停 — 加 cron ping 避免 |
| Logs | **Axiom Free** | $0 | 500GB/月 ingest |
| Errors | **Sentry Developer** | $0 | 5k errors/月 |
| Uptime | **Better Stack Free** | $0 | 10 monitor / 30s interval |
| Dashboard | **Cloudflare Pages Free** | $0 | CN 访问最稳；静态 + Workers Functions |
| 对象存储 (Parquet 冷存) | **Cloudflare R2 Free** | $0 | 10GB storage / 1M Class A / 10M Class B 月内免费；**零 egress** |
| **总计** | | **$0-3/月** | |

**痛点**：
- Supabase Free 1 周无访问会暂停 → 必须配 daily cron 写入触发；或升级
- Fly $5 trial 用完后开始正常计费
- 没有 dedicated egress IP → Polymarket Cloudflare 限流可能触发但 startup 期请求量小不卡

**升档触发**：
- ⚡ Supabase 500MB DB 写满 → 升 Pro
- ⚡ L2 接入（always-on worker，超 $5 trial）→ Fly 按量付费转正
- ⚡ 触发 Polymarket Cloudflare 限流 → 必须固定 egress IP → Fly $3.60/月

---

### 3.2 $30/月档（早期长跑，3-6 月）

> L1 daily + L2 候选池跟踪 stable run；可证明系统稳定的最低成本"生产"组合。

| 维度 | 服务 | 月成本 | 备注 |
|---|---|---|---|
| Compute | **Fly.io** shared-cpu-1x@1GB AMS region + dedicated egress IPv4 | $5.70 + $3.60 = $9.30 | L1 cron + L2 always-on 同一机器；AMS 区命中 |
| Database | **Supabase Pro** (Dublin) | $25 | 8GB DB / 100GB storage / 不会暂停 |
| Logs | **Axiom Free** | $0 | 仍在 free tier 内 |
| Errors | **Sentry Developer** | $0 | 5k errors 量级足够 |
| Uptime | **Better Stack Free** | $0 | 10 monitor |
| Dashboard | **Cloudflare Pages Free** | $0 | |
| 对象存储 | **Cloudflare R2** | $0-1 | 月内 10GB 免费 |
| **总计** | | **~$35-40/月** | |

**痛点**：
- Fly + Supabase 跨区延迟（AMS → Dublin = ~15ms）— 大批量写入要注意
- Better Stack Free 10 monitor 不够监控多端点 → 升 $24/月

**升档触发**：
- ⚡ L3 K 线接入（高 ingest 时序）→ 引入 Tiger Cloud
- ⚡ 多服务/多 worker 拆 → 加 Better Stack Pro 监控
- ⚡ DB IOPS 撞顶 → Supabase compute add-on Small ($25 → $50)

---

### 3.3 $100/月档（接入 L2/L3 + 监控完善，6-12 月）

> L1 + L2 稳定 + L3 K 线时序入库 + 完整 observability + 准备交易执行（私网/KMS 就绪）。

| 维度 | 服务 | 月成本 | 备注 |
|---|---|---|---|
| Compute | **Fly.io** 2x shared-cpu-1x@2GB AMS + 1x performance-2x@4GB (L3 WS) + 2x dedicated egress IPv4 | ~$35 | L1 + L2 + L3 拆三机；两个 egress IP（主/备） |
| Database (OLTP) | **Supabase Pro** + Small compute add-on (Dublin) | $25 + $25 = $50 | 接入 Auth/Storage 减后端开发量 |
| Database (时序) | **Tiger Cloud Performance** 1vCPU/2GB (Dublin) | $30 | K 线 / orderbook tick 入 hypertable |
| Logs | **Axiom Free** | $0 | 仍在 free tier |
| Errors | **Sentry Team** | $26 | 50k errors / 多项目 |
| Metrics + Uptime | **Better Stack** Telemetry plan | $24 (annual) | 30 monitor + metrics + status page |
| Dashboard | **Cloudflare Pages Free** | $0 | |
| Secrets / KMS | **HashiCorp Vault Cloud Starter** 或 **AWS KMS** ($1-3/月签名 API 调用 + 1 个 KMS key) | $2-5 | trading 测试用，未实启用 |
| **总计** | | **~$170/月** | ⚠️ 实际超 $100 档 |

**$100 严格档简化**（去掉 Tiger Cloud，K 线先继续放 Supabase）：

| 维度 | 服务 | 月成本 |
|---|---|---|
| Compute | Fly.io 2x shared@2GB + 1 dedicated egress | $25 |
| Database | Supabase Pro + Small compute | $50 |
| Errors | Sentry Team | $26 |
| Uptime | Better Stack Free | $0 |
| Logs | Axiom Free | $0 |
| Dashboard | Cloudflare Pages | $0 |
| **总计** | | **~$101/月** ✅ |

**痛点**：
- 严格 $100 档 K 线放 Supabase Pro 不一定撑得住（hypertable 缺失 → 全表扫）→ 上 $170 档买 Tiger Cloud
- Sentry Team 50k errors 在系统稳定后用不完，可降回 Developer

---

### 3.4 $300/月档（含交易执行 + 高可用）

> M3 阶段：交易执行落地 + 多 region 容灾 + 7×24 报警接驳 + 数据归档冷存。

| 维度 | 服务 | 月成本 | 备注 |
|---|---|---|---|
| Compute（主） | **Fly.io** 多机：L1 cron + L2 always-on + L3 WS + Trading executor (Dublin)  + Dashboard backend (AMS)；4x shared@2GB + 2x performance-2x@4GB + 3 egress IPv4 | ~$90 | Trading executor 单独跑在 Dublin（~1ms 到 Polymarket） |
| Database (OLTP) | **Supabase Pro** + Medium compute (Dublin) | $25 + $60 = $85 | 4GB RAM 给读写并发 |
| Database (时序) | **Tiger Cloud Performance** 2vCPU/8GB (Dublin) | $122 | K 线 + orderbook history |
| Logs | **Axiom Free** | $0 | 仍未撞 500GB |
| Errors | **Sentry Team** | $26 | |
| Metrics + Uptime + On-Call | **Better Stack Telemetry** ~3 seats | $72 (annual $24×3) | 含 phone/SMS 报警 |
| Dashboard | **Cloudflare Pages Free** | $0 | |
| Secrets / KMS | **AWS KMS** 1 EOA key + signing API | $5-10 | EIP-712 签名走 KMS（不落明文私钥） |
| 对象存储 | **Cloudflare R2** ~100GB | $1.50 | Parquet 长期归档 |
| **总计** | | **~$310/月** ✅ |

**痛点**：
- 跨栈延迟链：Fly (AMS) ↔ Supabase (Dublin) ↔ Tiger (Dublin) ↔ Polymarket (London via Dublin) ≈ 加起来 ~10-20ms — 实盘下单时全链路要 < 50ms 才安全 → 考虑把 trading executor 单独跑在 **AWS eu-west-1 Dublin** + Northflank BYOC 路线（见 §3.5 进阶）
- KMS 私钥签名 API 调用每次 +5-30ms（取决于 KMS region 与 executor region）— 必须同 region

**升档触发**：
- ⚡ 多策略并行 / 多账户 → 不再够，进 BYOC AWS 路线（不在本调研范围）

### 3.5 进阶路线（>$300，trading 上量后）

不在本次调研推荐范围，但**升级方向预定**：

| 阶段 | 路线 | 理由 |
|---|---|---|
| Trading 上量 + 多账户 | **Northflank BYOC → AWS eu-west-1 Dublin** | 同 region 内 KMS / compute / 出口 IP / VPC peering 全打通；Fly 抽象掉了底层细节，交易场景需要细粒度网络控制 |
| 数据规模 > 1TB | TimescaleDB 自托管 (Hetzner CCX23) + Postgres 主 (Supabase 仍用) | 长期数据归档省钱（Tiger Cloud 100GB 月 ~$50） |
| 多策略 / 多用户 | 引入 Temporal / RabbitMQ / Redpanda | 任务编排 + 事件流 |

---

## 4. 地区选择对比表

> 与 §0.1.1 数据呼应，针对**每条服务**的地区选择给出明确推荐。

### 4.1 三向量定义

- **延迟到数据源（Polymarket eu-west-2 London）** — 影响 L1/L2/L3 抓取效率、未来交易执行成败
- **延迟到 CN 操控（用户人在 CN）** — 影响 dashboard 访问体验、CLI/SSH 操作流畅度
- **合规出境 / IP 封锁** — Polymarket 33 国 IP 黑名单 + CN 出境带宽政策

### 4.2 区域综合对比

| 地区 | 到 Polymarket (London) | 到 CN (北京/上海) | Polymarket 封 | CN 监管 | 推荐场景 |
|---|---|---|---|---|---|
| **Dublin (IE)** | ~1ms ✅✅ | ~250-300ms | 不封 ✅ | 出境正常 | **数据抓取 + 交易执行首选** |
| Amsterdam (NL) | ~10-15ms ✅ | ~220-280ms | 不封 ✅ | 出境正常 | **Fly.io 落地次选（无 Dublin 时）** |
| London (UK) | <1ms ✅✅ | ~200-250ms | **封** ❌ | — | 不适合 |
| Frankfurt (DE) | ~8-12ms | ~190-240ms | **封** ❌ | — | 不适合 |
| Helsinki (FI) | ~25-30ms | ~180-220ms | 不封 ✅ | 出境正常 | **Hetzner 路线唯一可用 EU 区** |
| Tokyo (JP) | ~230-260ms ❌ | ~50-80ms ✅ | 不封 ✅ | 出境正常 | dashboard for CN 用户；不抓数据 |
| Singapore (SG) | ~200-250ms ❌ | ~70-100ms ✅ | **封** ❌ | — | 不适合 |
| Hong Kong | ~250-290ms ❌ | ~30-60ms ✅ | **封** ❌（多数 IP） | — | 不适合 |
| Seoul (KR) | ~250-290ms ❌ | ~50-100ms ✅ | 不封 ✅ | — | dashboard for CN；不抓数据 |
| US-East Virginia | ~75-90ms ❌ | ~180-250ms | **封** ❌ | — | 不适合 |
| US-West Oregon | ~140-160ms ❌ | ~150-200ms | **封** ❌ | — | 不适合 |
| Toronto | ~70-90ms | ~180-230ms | 不封 ✅ | 出境正常 | DO Toronto 区可作 backup 区 |

### 4.3 按服务的地区推荐

| 服务类型 | 首选 | 次选 | 备选 |
|---|---|---|---|
| L1/L2 数据抓取 | Dublin (AWS via Northflank BYOC, 或 Supabase) | Amsterdam (Fly / Railway / DO) | Helsinki (Hetzner) |
| L3 WebSocket | Dublin | Amsterdam | — |
| OLTP DB (Postgres) | Dublin (Supabase / Tiger) | London (Neon，DB 不调 Polymarket 不影响) | — |
| 时序 DB | Dublin (Tiger) | Tokyo (Tiger，仅当 dashboard 强 CN-访问场景) | 自托管 Helsinki |
| 交易执行 (M3+) | Dublin | Amsterdam | — |
| Dashboard 前端 | Cloudflare 全球 anycast | Vercel 全球 anycast（注意 CN 抖动） | Tokyo/Seoul SSR（CN 访问优化） |
| 监控 SaaS | 服务自选（基本都 anycast） | — | — |

### 4.4 关键架构含义

**所有 Polymarket 数据流转必须从 Dublin/Amsterdam 出口**。这意味着：

```
[CN 用户浏览器] ─── HTTPS ──→ [Cloudflare Pages dashboard, anycast]
                                          │
                                          │ API 调用
                                          ▼
                          [Fly app, AMS region, dedicated egress IPv4]
                                          │
                  ┌───────────────────────┼───────────────────────┐
                  ▼                       ▼                       ▼
        [Supabase, Dublin]       [Polymarket Gamma API,       [AWS KMS, eu-west-1]
                                  London via Dublin tunnel]    (M3+ trading only)
```

**绝不能**把数据抓取放在 US/UK/SG/HK/CN — IP 立即被封。

---

## 5. 关键决策树

### 5.1 阶段映射

```
┌─────────────────────────────────────────────────────────────────┐
│ 启动期 (0-3 月) — Phase 02 当下                                  │
│   目标：跑通 L1，验证整套部署链路                                │
│   推荐组合：免费堆叠 $0/月                                       │
│   ✅ Fly.io trial + Supabase Free Dublin + Axiom/Sentry/        │
│      Better Stack Free + Cloudflare Pages + R2                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ 触发：L1 稳定 + 准备接 L2
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 验证期 (3-6 月)                                                  │
│   目标：L1 daily 稳跑 + L2 候选池跟踪                            │
│   推荐组合：$30-40/月                                            │
│   ✅ Fly.io + dedicated egress + Supabase Pro Dublin            │
│   ⚠️ 仍用 Free observability 三件套                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ 触发：L3 K 线接入 + 监控告警刚需
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 完善期 (6-9 月)                                                  │
│   目标：L1+L2+L3 全跑 + 完整 observability                       │
│   推荐组合：$100-170/月                                          │
│   ✅ Fly.io 多机 + Supabase Pro+compute + Tiger Cloud           │
│   ✅ Sentry Team + Better Stack Telemetry                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ 触发：交易执行落地（M3）
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 实盘期 (9-12 月)                                                 │
│   目标：交易执行 + 7×24 高可用                                   │
│   推荐组合：$300/月                                              │
│   ✅ Fly multi-region + Trading executor on Dublin              │
│   ✅ AWS KMS for EIP-712 signing                                │
│   ⚡ 若上量需要：Northflank BYOC → AWS eu-west-1                 │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 关键 if-then 决策点

| 现状 | 决策 |
|---|---|
| 还没跑 L1 | **免费堆叠开始**，不要在选型上瘫痪 |
| L1 跑稳但被 Polymarket Cloudflare 限流 | 立刻加 Fly dedicated egress IPv4 ($3.60/月) |
| Supabase Free 撞 500MB 上限 / 1 周不写入暂停 | 升 Supabase Pro Dublin ($25) |
| 想接 K 线 | 先尝试 Supabase + 自建分表；撞性能瓶颈再上 Tiger Cloud |
| 要做交易执行 | **回到本文 §5.1 实盘期组合 + 提前 1 个月在 staging 演练 KMS 签名链路** |
| Fly.io 稳定性出问题 | 候补：DigitalOcean App Platform AMS（同样 PaaS 抽象，公司更老） |
| 预算撞 $300+ 还要扩 | 进 Northflank BYOC → AWS eu-west-1 |

---

## 6. 排除项 + 理由

| 栈 | 排除理由 |
|---|---|
| **Vercel / Netlify 当主 compute** | 无长跑 worker；函数最长 5-15 分钟会切 L1 的 8 分钟全量；serverless 出口 IP 不固定 → Polymarket 限流不稳。**仅可做 dashboard 前端** |
| **Cloudflare Workers 当主 compute** | 30s CPU/15min real-time 硬限制；持久 WebSocket 连接受限（Durable Objects 路线复杂）；不适合 L1/L2/L3 |
| **AWS / GCP / Azure 当主栈（启动期）** | 复杂度爆炸；CN 卡支付难；定价不透明；启动期不需要这种自由度。**只在 M3+ via Northflank BYOC 时引入** |
| **Heroku** | 价格贵 + 没合规区域 + 2022 砍 free tier 后口碑崩盘 |
| **Vercel Hobby for commercial** | Hobby plan 明确禁止商业用途（套利系统是商业） |
| **Aiven / RDS for Postgres** | 启动期相对 Supabase/Neon 没有优势；Aiven 起步 $50+，RDS 复杂 |
| **PlanetScale**（已 dropped Postgres 路线 2024-2025） | MySQL only，不适合 |
| **Firebase / Firestore** | 文档库不适合 OLAP-ish 套利分析查询 |
| **Replit / Glitch** | 玩具级，不能长跑生产 |
| **裸 KVM (Linode / Vultr 等小厂)** | 启动期不需要这种 DIY；Hetzner 已是性价比代表，没必要再换 |
| **Hetzner DE/FI 区做主 compute** | 虽便宜，但 DIY 程度高、HA/备份要自己管、启动期心智负担大。**仅推荐**于"我对运维 Postgres + 监控有信心 + 预算 < $20/月" |

---

## 7. 待用户决策的 4 个开放问题

> 这些不预判，留给用户在看完上面对比表后定。

1. **PaaS vs DIY 偏好**：愿意每月多花 $30-50 买 PaaS 抽象，还是省钱选 Hetzner DIY 路线（运维成本会很真实）？
2. **CN 访问优先级**：dashboard 在 CN 直连慢能容忍吗（Vercel/Supabase 偶有抖动），还是必须 Cloudflare CN 友好路线？
3. **DB 合并 vs 拆**：Phase 1 阶段是否容忍 OLTP（Supabase）+ 时序（Tiger Cloud）双库 ~$80/月，还是先合并到 Supabase Pro $25/月跑到撞墙再拆？
4. **AWS KMS 提前接还是 M3 再接**：M3 才接的话栈不需要现在动；但提前接可以在 staging 中演练签名链路 — 多 $5/月 +1 周学习成本。

---

## 8. 引用与访问日期

所有定价数据访问于 **2026-05-11**。注意 Hetzner 2026-04-01 涨价 50%，Fly.io 2024-10 移除 free tier，Supabase 2026-02 调价、Render 2026-04-23 取消 seat fee — 半年内变动频繁，落实前再核一次。

主要来源：
- Fly.io：[fly.io/docs/about/pricing](https://fly.io/docs/about/pricing) / [fly.io/pricing](https://fly.io/pricing) / [fly.io/docs/networking/egress-ips](https://fly.io/docs/networking/egress-ips)
- Render：[render.com/pricing](https://render.com/pricing) / [render.com/docs/regions](https://render.com/docs/regions)
- Railway：[railway.com/pricing](https://railway.com/pricing) / [docs.railway.com/pricing/plans](https://docs.railway.com/pricing/plans)
- DigitalOcean：[digitalocean.com/pricing/app-platform](https://www.digitalocean.com/pricing/app-platform) / [docs.digitalocean.com/platform/regional-availability](https://docs.digitalocean.com/platform/regional-availability)
- Hetzner：[hetzner.com/cloud](https://www.hetzner.com/cloud) / [hetzner.com/cloud/regular-performance](https://www.hetzner.com/cloud/regular-performance) / [docs.hetzner.com/general/infrastructure-and-availability/price-adjustment](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment)
- Northflank：[northflank.com/pricing](https://northflank.com/pricing) / [northflank.com/product/bring-your-own-cloud](https://northflank.com/product/bring-your-own-cloud)
- Supabase：[supabase.com/pricing](https://supabase.com/pricing) / [supabase.com/docs/guides/platform/regions](https://supabase.com/docs/guides/platform/regions)
- Neon：[neon.com/pricing](https://neon.com/pricing) / [neon.com/docs/introduction/regions](https://neon.com/docs/introduction/regions)
- Tiger Cloud：[tigerdata.com/pricing](https://www.tigerdata.com/pricing) / [tigerdata.com/docs/learn/tiger-cloud/regions](https://www.tigerdata.com/docs/learn/tiger-cloud/regions)
- Better Stack：[betterstack.com/pricing](https://betterstack.com/pricing) / [betterstack.com/docs/logs/billing-for-metrics](https://betterstack.com/docs/logs/billing-for-metrics)
- Axiom：[axiom.co/pricing](https://axiom.co/pricing)
- Sentry：[sentry.io/pricing](https://sentry.io/pricing) / [docs.sentry.io/pricing](https://docs.sentry.io/pricing)
- Grafana Cloud：[grafana.com/pricing](https://grafana.com/pricing) / [grafana.com/products/cloud/free-tier](https://grafana.com/products/cloud/free-tier)
- Vercel：[vercel.com/pricing](https://vercel.com/pricing) / [vercel.com/docs/limits](https://vercel.com/docs/limits)
- Cloudflare：[developers.cloudflare.com/workers/platform/pricing](https://developers.cloudflare.com/workers/platform/pricing) / [developers.cloudflare.com/r2/pricing](https://developers.cloudflare.com/r2/pricing)
- Polymarket 服务器位置：[newyorkcityservers.com/blog/polymarket-server-location-latency-guide](https://newyorkcityservers.com/blog/polymarket-server-location-latency-guide)（2026-04-07）
- Polymarket API & Auth：[docs.polymarket.com/api-reference/introduction](https://docs.polymarket.com/api-reference/introduction) / [docs.polymarket.com/api-reference/authentication](https://docs.polymarket.com/api-reference/authentication) / [docs.polymarket.com/api-reference/geoblock](https://docs.polymarket.com/api-reference/geoblock)
- Hetzner 涨价：[theregister.com/on-prem/2026/02/24/hetzner-50-price-hike-no-fooling-from-april-1st](https://www.theregister.com/on-prem/2026/02/24/hetzner-50-price-hike-no-fooling-from-april-1st/4547119) / [hetzner.com/pressroom/statement-price-adjustment](https://www.hetzner.com/pressroom/statement-price-adjustment)
- AWS KMS for Ethereum：[aws.amazon.com/blogs/web3/import-ethereum-private-keys-to-aws-kms](https://aws.amazon.com/blogs/web3/import-ethereum-private-keys-to-aws-kms) / [github.com/kaleido-io/vault-plugin-secrets-ethsign](https://github.com/kaleido-io/vault-plugin-secrets-ethsign) / [docs.nethereum.com/docs/signing-and-key-management/guide-cloud-kms](https://docs.nethereum.com/docs/signing-and-key-management/guide-cloud-kms)

---

## 8.5 业内参考：35+ Polymarket OSS 项目的部署形态（2026-05-11 窗口 C 调研）

数据来源：`docs/research/polymarket-oss-landscape-2026-04.md` + 本地 `3th-party/` 实仓扫描。

### 8.5.1 高 star OSS 项目的部署形态分桶

| 项目 | star | 部署形态 | DB | 健康检查 |
|---|---|---|---|---|
| polymarket-kalshi-weather-bot（本地参考） | — | **Vercel 前端 + Railway/Nixpacks 后端（FastAPI）** | SQLite | ✅ `/api/health` |
| clawfirm（本地参考） | — | **Docker multi-stage + systemd + 嵌入前端** | SQLite WAL | ✅ HEALTHCHECK |
| polymarket-hft-engine | 45 | **AWS eu-west-1（独立 VPS）** | — | — |
| warproxxx/poly-maker | 1.1k | 纯本地脚本（无部署配置） | — | — |
| Polymarket/agents | 3.3k | 教学库（无部署） | — | — |
| pmxt-dev/pmxt | 1.6k | SDK 库（无部署） | — | — |
| ImMike/polymarket-arbitrage | 108 | 纯本地脚本 | — | — |
| taetaehoho/poly-kalshi-arb | 428 | 本地/自托管 | — | — |

### 8.5.2 业内主流模式

**模式 A — 分离制（前 PaaS + 后 PaaS）**：
- 前端：Vercel / Netlify 静态
- 后端：Railway（Nixpacks） / Render
- DB：SQLite（单实例够用）或 managed Postgres
- **占比最高**（已工程化的 3 个项目里 2 个走这条）

**模式 B — Docker + systemd**：
- 单二进制 / 单容器 + 嵌入前端资源
- 适合高控制需求（clawfirm）
- 数据库 SQLite + WAL

**模式 C — AWS / 独立 VPS（HFT only）**：
- 仅高频套利项目（polymarket-hft-engine）选
- 启动期不走

**模式 D — 纯本地脚本（无部署）**：
- 多数高 star 但**非工程化**的项目
- 用户自己维护运维 → 知识碎片化

### 8.5.3 业内反模式（OSS 项目集体踩过的坑）

1. **缺健康检查 / 自动重启** — T1-T4 多数项目无 `/health` 路由 → 生产化缺陷
2. **SQLite + FastAPI async 并发** — weather-bot 的做法，但多 worker 下容易锁表 → 实际生产应改 WAL（clawfirm 做对了）或 Postgres
3. **APScheduler 后台任务** — weather-bot 用，但无显式超时 / deadletter → 任务静默挂死无人知
4. **部署文档碎片化** — 即便 1k+ star 项目也很少写清"如何生产部署" → 知识隔离

### 8.5.4 对本项目的启示

- ✅ **模式 A（Railway/Nixpacks + Vercel）的工程难度最低，业内已被验证**
  - 但本调研报告推荐 **Fly.io AMS** 而非 Railway —— 原因：Railway 区域 us-east-only，**到 Polymarket London 高延迟 + 无固定 IP（trading-readiness 阻断）**
  - 即：**学业内模式 A 的"分离制 + PaaS"思路，换 PaaS 厂商**
- ✅ **clawfirm 的 Dockerfile + systemd + WAL** 是工程纪律范本
  - 本项目即便选 PaaS，也建议产出标准 Dockerfile（便于未来切走 / 本地 docker compose 重现）
- ❌ **不要复制 weather-bot 的 SQLite 直接上多 worker** — async + SQLite 写锁是确定的雷
- ❌ **不要走"纯本地脚本"路线** — 占比高但都是"非生产"项目；本项目目标是生产级

---

## 9. 单点结论 TL;DR（给用户的浓缩版）

1. **Polymarket 实际在 AWS eu-west-2 London**（不是 us-east），所有数据抓取必须在 Dublin / Amsterdam / Helsinki 三个非封锁低延迟区
2. **启动期免费堆叠**：Fly.io trial + Supabase Free Dublin + Axiom/Sentry/Better Stack Free + Cloudflare Pages + R2 = **$0/月**
3. **$30 档主推**：Fly.io AMS + dedicated egress IP + Supabase Pro Dublin = ~$35/月
4. **$100 档主推**：Fly.io 多机 + Supabase Pro + compute add-on + Sentry Team = ~$101/月
5. **$300 档实盘**：Fly.io 多 region + Supabase Pro Medium + Tiger Cloud Dublin + AWS KMS + Better Stack Telemetry = ~$310/月
6. **演进上限**：M3 上量后切 Northflank BYOC → AWS eu-west-1 Dublin（同 region 内 KMS+VPC+EIP 全打通）
7. **必装**（任何档）：**Cloudflare Pages 当 dashboard**（CN 访问最稳）、**Sentry**（error tracking 业内标准）、**Cloudflare R2**（零 egress 对象存储归档 Parquet）
8. **绝不选**（启动期）：AWS/GCP/Azure 直接、Heroku、Vercel 当主 compute、纯 Cloudflare Workers 当主 compute

---

*End of draft. 待 Phase 02 决策会议合并入主架构。*
