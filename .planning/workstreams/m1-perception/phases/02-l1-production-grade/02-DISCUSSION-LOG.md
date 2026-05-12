# Phase 02: L1 production-grade long-running - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-12
**Phase:** 02-l1-production-grade
**Workstream:** m1-perception
**Areas discussed:** PaaS+CI/CD / L1 cadence+失败恢复 / 健康监控+log+告警 / Dashboard 雏形

---

## Pre-discussion: Phase 02 范围重定义

| Option | Description | Selected |
|---|---|---|
| 重定义 Phase 2 = L1 production-grade, WS 推到 Phase 3+ | thread + LEARNINGS 一致指向 Phase 02 应是 L1 生产级，不是 WS | ✓ |
| 插入新 Phase 1.5 = L1 prod, 保留 Phase 2 = WS | 历史保留但与历史 Phase 1.5 (REVERTED) 命名冲突 | |
| 手工创建 CONTEXT 但不走 gsd-discuss-phase | 绕过工具失败但失去 Phase 01.1 加上的纪律基础设施 | |

**Action:** ROADMAP.md 改 Phase 2 名称为 "L1 production-grade long-running"，原 WS 内容移到 Phase 3。

---

## Gray Area Selection

| Option | Description | Selected |
|---|---|---|
| PaaS 供应商 + CI/CD 链路 | Fly / Render / Railway 选哪家 + GHA workflow 怎么写 | ✓ |
| L1 cadence + 失败恢复 | subset/full 多久跑 + 失败处理逻辑 | ✓ |
| 健康监控 + log 栈形式 | Better Stack / Axiom / Sentry / Grafana 哪个 | ✓ |
| Dashboard 雏形范围 | 最小 read-only vs 交互式 | ✓ |

**User's choice:** 全选 (4/4)

---

## Area 1: PaaS + CI/CD

### Q1a: PaaS 供应商

| Option | Description | Selected |
|---|---|---|
| Fly.io AMS | thread 首选；AMS 离 Polymarket London 近；trading-ready | ✓ (by the agent) |
| DigitalOcean App Platform | thread 强首选；CN 友好；Reserved IP 免费 | |
| Railway | thread 备选；Trading-readiness ⭐⭐⭐ 不如 Fly | |
| 其他 | | |

**User's choice:** "你来选最佳方案"
**the agent's selection rationale:** Fly.io 因 AMS region 命中 + Trading-readiness ★★★★★（D11 锁了 KMS 延 M3 但 Fly 提前 ready） + dedicated egress IPv4 $3.60/月（thread §5.2 if-then 路径）。DO 优势在 CN 友好但用户 D11.b 锁了"不约束 CN"，DO 优势失效。

### Q1b: Environment 切分

| Option | Description | Selected |
|---|---|---|
| 单 environment 起步 | 直推 prod，read-only daemon 风险低 | ✓ (by the agent) |
| Staging + prod 两环境 | 多一份 compute + DB 拆分 | |
| PR 预览环境 | 多环境调 API 拼位率会被限流 | |

**User's choice:** "你来选最佳方案"
**the agent's selection rationale:** 单 environment — L1 是只读 daemon 无数据破坏风险；trial 额度不够双机；MVP 纪律。staging 后期补一个 fly-staging app 即可（D-05）。

### Q1c: CI/CD chain

| Option | Description | Selected |
|---|---|---|
| GHA build + PaaS CLI deploy | 业内主流；GHA 跑 test gate 后 flyctl deploy | ✓ (by the agent) |
| PaaS 原生接 Git | 最简但无 test gate | |
| GHA + GHCR + PaaS 拉取 | 解耦但配置多一步 | |

**User's choice:** "你来选最佳方案"
**the agent's selection rationale:** GHA + flyctl 直接 — clawfirm / polymarket-kalshi-weather-bot 都用此模式；test gate 重要；同时建立 CI 习惯为 M2/M3 准备（D-06）。

### Q1d: Secrets 管理

| Option | Description | Selected |
|---|---|---|
| PaaS 原生 + GHA deploy token | flyctl secrets set + FLY_API_TOKEN | ✓ (by the agent) |
| 1Password / Doppler 中央 manager | 过设计 for 启动期 | |
| 混合 GHA secrets + PaaS prod secrets | CI test 需要也走 GHA | |

**User's choice:** "你来选最佳方案"
**the agent's selection rationale:** Secrets ≤ 5 个，中央 manager 是过设计；完全隔离 CI 和 prod secrets（D-07）。

---

## Area 2: L1 cadence + 失败恢复

### Q2a: L1 subset cadence

| Option | Description | Selected |
|---|---|---|
| 每天 2 次（12h 间隔） | 同覆盖亚洲/美洲；day-over-day 有意义；trial 额度够 | ✓ |
| 每天 1 次（UTC 凌晨） | 最低成本；与 99% 市场静止匹配 | |
| 每 4-6 小时 | 接近 L2 cadence，违反层级纪律 | |

**User's choice:** Option 1（D-09）

### Q2b: Full cadence

| Option | Description | Selected |
|---|---|---|
| 每周 1 次（周日凌晨） | thread §2.7 锁定 full = 周/月审计 | ✓ |
| 不跑，手动触发 | LEARNINGS D7 明明说 "full 保留" | |
| 每月 1 次 | 太少 | |

**User's choice:** Option 1（D-10）

### Q2c: 单次失败处理

| Option | Description | Selected |
|---|---|---|
| 三档 OK/DEGRADED/FAILED | amendment 24f52ba 已落地 | ✓ |
| 二态 success/fail | 回退已有进展 | |
| Retry once before fail | cycle 翻倍 worst case 30min | |

**User's choice:** Option 1（D-12）

### Q2d: 连续失败处理

| Option | Description | Selected |
|---|---|---|
| 连续 3 次失败 → 暂停 daemon + 告警 | 避免狂重试贬值 API quota | ✓ |
| 连续 5 次 → exponential backoff | 间隔可能 24h 没人看 | |
| 不区分，每次都独立告警 | Alert fatigue | |

**User's choice:** Option 1（D-13）

---

## Area 3: 健康监控 + log + 告警

### Q3a: Log 栈

| Option | Description | Selected |
|---|---|---|
| Axiom Free (500GB / 30d) | log 单点最强 Free tier | ✓ |
| Fly 原生 log | 14d retention，跨 service 不能聚合 | |
| Better Stack Logs (3GB / 3d) | 量级不够 | |

**User's choice:** Option 1（D-14）

### Q3b: Error tracking

| Option | Description | Selected |
|---|---|---|
| Sentry Developer Free (5k errors) | 业内必装；Python SDK 一行 | ✓ |
| 仅靠 log filter | 失去 source map / 自动分组 | |
| Sentry 手动上报顶层 | TransientError 可能错过 | |

**User's choice:** Option 1（D-15）

### Q3c: Uptime / health monitor

| Option | Description | Selected |
|---|---|---|
| Better Stack Free + daemon /health endpoint | 业内标准；能区分 alive vs stale | ✓ |
| Pushgateway heartbeat | 适合 cron 任务，但已有 always-on daemon | |
| Fly health check + webhook | 只能 alive vs dead 不细致 | |

**User's choice:** Option 1（D-16）

### Q3d: 告警推送

| Option | Description | Selected |
|---|---|---|
| Telegram bot + email 双遡 | CN/港主流；及时 + 兜底 | ✓ |
| 仅 email | 终端用户 mail client 未必开 | |
| Slack / Discord | 静默错过 | |
| PagerDuty | 过茂用 for read-only | |

**User's choice:** Option 1（D-17）

---

## Area 4: Dashboard 雏形

### Q4a: Dashboard 范围

| Option | Description | Selected |
|---|---|---|
| 最小 read-only（状态面板） | 不含 watchlist 编辑 / scan trigger | |
| 极简 1 页 | 余地低 | |
| 交互式：read-only + scan trigger | 跨纪律边界？需确认 | ✓ |
| 不要 dashboard，Phase 02 不做 | 与 LEARNINGS carry-over 矛盾 | |

**User's choice:** Option 3

### Q4a-follow-up: 纪律确认

| Option | Description | Selected |
|---|---|---|
| 接受 — scan trigger 是观察不是策略 | dashboard scan ≡ 本地 make scan | ✓ |
| 退一步 — 仅 read-only，scan trigger 推到 Phase 02.1 | 严格遵循 thread §1 纪律 | |
| 拆两层 — Phase 02 read-only + Phase 02.1 交互 | 动作拆得更干净但 phase 不 production-grade complete | |

**User's choice:** Option 1
**User note:** "纪律的本质是 '不偏离目标'，scan trigger 是观察不是策略" — 重要 feedback，建议记入 memory

### Q4b: 前端栈

| Option | Description | Selected |
|---|---|---|
| Vercel Next.js + Supabase pg_rest | 最主流；扩展余地大 | ✓ |
| Vercel 静态 HTML + Supabase fetch | 超简但交互一上去会显得草率 | |
| Cloudflare Pages + Workers | CN 友好但 D11.b 不约束 CN | |

**User's choice:** Option 1（D-19）

### Q4c: Dashboard 认证

| Option | Description | Selected |
|---|---|---|
| Supabase Auth magic link + email whitelist | 单用户最妥当 | ✓ |
| Basic auth via env vars | 简单但够 | |
| 公开读 + scan 问答验证 | scan 消耗 SQL 不能公开 | |

**User's choice:** Option 1（D-20）

### Q4d: scan trigger 实现

| Option | Description | Selected |
|---|---|---|
| Vercel Edge Function POST 到 Fly daemon /scan endpoint | 复用 4 层防御 + Trust-split | ✓ (by the agent) |
| Vercel 直连 Supabase 跑 scan SQL | 4 层防御要重做 | |
| Pending scan 表 + daemon 轮询 | 过重 queue 模式 | |

**User's choice:** "你按最佳实践方案"
**the agent's selection rationale:** 复用 Phase 01.1 已建立的 P1 trust-split pattern；daemon 已有 scanner.py 完整查询路径；同步返回 JSON 适合 scan（≤ 50 行）；Fly internal network 不外暴 daemon（D-21/D-22）。

---

## the agent's Discretion (carried to plan phase)

User 在 discuss 阶段未锁定，明确交给 researcher / planner：

1. DB schema 双端同步路径（SQLite + R2 archive vs 直接 Supabase Postgres）
2. Snapshot 保留策略 cron 落地（默认 30 天 SQLite + R2 永久 archive 候选）
3. 框架抽象 A 落地范围（Phase 02 同步动 vs 推 Phase 02.1）
4. Dockerfile multi-stage 细节（uv 缓存 / non-root / healthcheck）
5. GHA workflow YAML 细节（concurrency / cache / matrix）
6. /health endpoint schema
7. FastAPI vs Starlette vs Flask 选择

---

## Deferred Ideas

- watchlist 编辑能力 → Phase 02.1 或 M2
- WebSocket 增量数据流（旧 Phase 2） → Phase 3
- L2 定向跟踪 daemon → Phase 3+
- PR 预览环境 → M3 实盘前
- AWS KMS 签名链路 → M3 实盘前
- Tiger Cloud 双库 → Supabase 撞墙后
- 私网出站 + 固定 IP → thread §5.2 if-then 路径触发后
- 多 region failover → M5 工业化
- 完整 metrics 体系（Prometheus / Grafana Pro） → 启动期不需

---

*Phase: 02-l1-production-grade*
*Workstream: m1-perception*
*Discussion log: 2026-05-12*
