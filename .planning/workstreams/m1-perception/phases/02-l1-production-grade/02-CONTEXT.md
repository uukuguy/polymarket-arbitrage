# Phase 02: L1 production-grade long-running - Context

**Gathered:** 2026-05-12
**Status:** Ready for research / planning
**Workstream:** m1-perception
**Discuss method:** 手工 discuss（Claude + 用户对话直接产出，gsd-discuss-phase workflow 引导）

---

<domain>
## Phase Boundary

把 Phase 01.1 的 L1 观察工具（snapshot / scan / show-market / watchlist）从「研发期单次跑通」升级到「云上 7×24 自主跑 + 健康监控 + 一键部署」。

L1 必须达到 thread §1 的「生产级判定标准」：7 天连跑无人值守，失败自动告警，2 次成功 snapshot 可对比，磁盘不爆。达到后才有资格开 L2 工作。

**关键纪律来源**：thread `market-observation-architecture.md` §1 — "当前层不达到生产级判定标准，禁止开下一层的工作"。

**包含**：
- Makefile CLI 入口 smoke test（防 L11/S5 silent failure 复发）
- Snapshot 三态健康判定（OK/DEGRADED/FAILED，已 amendment 落地，补 parquet+SQLite 双校验）
- 框架抽象 A（统一市场状态模型）部分启动 — 至少把 fetched_at_ms vs page-level 时间显式分离（D5/L2）
- 一键部署链路（Fly + GHA + Supabase + Axiom + Sentry + Better Stack + Vercel）
- L1 云上 7×24 长跑 + 健康监控
- 交互式 dashboard 雏形（read-only 状态面板 + scan trigger 按钮）

**不包含（明确不做）**：
- KMS / 私钥栈 — D11 锁定延到 M3 实盘前
- Tiger Cloud 双库 — D11 单库先撞墙
- WebSocket 增量数据流 — 推到 Phase 3（thread §1 纪律）
- L2 定向跟踪 daemon — 推到 Phase 3+（L1 未生产级前不开）
- watchlist 在 dashboard 上的编辑能力 — 暂留 yaml 手编路径
- Translation pipeline 改造 — D10 已解耦，保持现状

</domain>

<decisions>
## Implementation Decisions

### Deployment Stack（已锁，由 thread `deployment-architecture.md` §0.1 推导）

- **D-01:** Compute = **Fly.io** AMS region（thread §2.1.7 评"首选"；离 Polymarket London ~10ms；long-running 与 cron 同机器；Trading-readiness ★★★★★，未来 M3 不用换栈）
- **D-02:** Database = **Supabase Pro** Dublin region（thread §0.1 / D11；启动期可先免费 tier 起步，撞 500MB 或 1 周不写入风险时升 Pro $25/月）
- **D-03:** Object storage = **Cloudflare R2** Free tier（10GB/月免费；用于 parquet 长期归档）
- **D-04:** Region all-eu（D11/S4 — Polymarket 在 AWS eu-west-2 London；后端 Fly AMS + DB Supabase Dublin + 对象存储 R2）

### Environment / CI/CD

- **D-05:** **单 environment 起步**（先只跑 prod；L1 是只读 daemon，没有数据破坏风险；staging 在 schema 变更或 P1+ 需要时后期补一个 fly-staging app 即可）
- **D-06:** CI/CD = **GHA build + flyctl deploy**（直接最主流，业内同行 clawfirm / polymarket-kalshi-weather-bot 都用此模式；GHA 跑 lint+test+pyright 全绿后 `flyctl deploy`；不接受 PaaS 原生 Git 直推 — 没 test gate 等于让坏代码上 prod）
- **D-07:** Secrets = **PaaS 平台原生（flyctl secrets set）+ GHA secrets 只放 deploy token**（中央 secrets manager 是过设计；完全隔离 CI 和 prod secrets）
- **D-08:** Container = Docker multi-stage（uv install + python app；参考 clawfirm 项目 Dockerfile 范本）

### L1 Cadence

- **D-09:** **subset 每天 2 次**（UTC 凌晨 + 中午 12h 间隔，同覆盖亚洲 / 美洲主市场时间；与 thread §2.1.a #2 实测的 ~8-10min/snapshot 匹配；Axiom Free 500MB ingest 完全够）
- **D-10:** **full 每周 1 次**（周日 UTC 凌晨；thread §2.7 锁定 full 作为周/月审计；~15-20min/次 × 4/月 = ~80min/月 compute）
- **D-11:** 调度方式 = **Fly cron**（fly.toml `[mounts]` + scheduled tasks；与 daemon 同 app）

### Failure Handling

- **D-12:** 单次 snapshot 失败 = **三档处理**（已在 amendment 24f52ba 落地）：
  - OK = Layer 1 count jitter < 1% 且所有 stages 通过 → 标 OK，写入
  - DEGRADED = jitter ≥ 1% 但 < 5% / 某些 stage warning → 标 DEGRADED，写入但 latest_snapshot_pair 不挑
  - FAILED = jitter ≥ 5% 或 stage exception → 不写入，告警
- **D-13:** **连续 3 次失败 → 暂停 daemon（需手动重启）+ 告警**（避免 cron 狂重试贬值 API quota；daemon 进入 paused 状态后需 SSH/dashboard manual unblock；事后能从 log 重建现场）

### Observability

- **D-14:** Log stack = **Axiom Free**（500GB/月 ingest + 30 天 retention 业内最慷慨；Fly stdout → vector / axiom-mcp 转发；APL 查询语言；不上 Better Stack Logs 是因 3GB/3d retention 不足以覆盖 30 天调试）
- **D-15:** Error tracking = **Sentry Developer Free**（5k errors/月足够 L1 量级；Python sentry_sdk 一行接入；source map / 自动分组 / Slack hook）
- **D-16:** Uptime / health = **Better Stack Free**（10 monitor × 30s）+ **daemon 暴露 HTTPS `/health` endpoint** 返回最后一次 snapshot 状态 + timestamp JSON；Better Stack 30s ping，200 + timestamp 不太老 = healthy
- **D-17:** Alert push = **Telegram bot（Better Stack 原生集成）+ email 双遡**（用户创建 @BotFather token；email 作为打底不丢历史记录；CN/港开发者主流推送通道）

### Dashboard

- **D-18:** Dashboard 范围 = **交互式 read-only + scan trigger**（用户明确解读"scan trigger 是观察不是策略，不违反 thread §1 层级纪律"；包含 4-5 页：L1 运行状态时间线 / Top movers / Sentry alerts 推背 / scan trigger 按钮 / Better Stack 状态嵌入）
- **D-19:** 前端栈 = **Vercel Next.js App Router + Supabase JS SDK pg_rest**（最主流，资源多；CSR/SSR 都可；未来 dashboard 扩展余地大；不走 Cloudflare Pages 因为 CN 友好非约束 D11.b）
- **D-20:** Dashboard 认证 = **Supabase Auth magic link + email whitelist 单用户**（dashboard 创建 DB 读负载，不可公开；个人项目 magic link 最妥当）
- **D-21:** scan trigger 实现 = **Vercel Edge Function POST 到 Fly daemon `/scan` endpoint，同步返回 JSON**（daemon 加 FastAPI endpoint 接收 `{recipe_name, params}` 调 `run_recipe()`；复用 Phase 01.1 的 4 层 SQL 防御 + Trust-split，不在 Supabase 端重新实现 — 违反 P1 trust-split pattern 复用纪律会得不偿失）
- **D-22:** Fly daemon 暴露端口 = **Fly internal network only（`<app>.internal`）+ HTTPS via Fly Anycast**（不对外暴露 IP，仅 Vercel 调用走 Fly internal DNS）

### Amendment 01 — Memory Discipline (2026-05-15, post-Plan-02-04 prod incident)

- **D-23:** **流式分页是 L1 生产稳定的硬约束** — Gamma `_paginate` 必须改成 `AsyncIterator[dict]`（逐市场 yield，不内部累积 list），orchestrator 必须改成 streaming consumer（每页 normalize → 立刻写 SQLite/parquet → 丢弃 raw）。**触发证据**：2026-05-15 Plan 02-04 首次 prod deploy（256MB Fly VM）观测到 daemon OOM-killed，root cause = paginator 累积 20k stripped dicts ≈ 160MB 常驻 + normalize 中间结构 ≈ 接近上限边际，遇 Gamma 大页即爆。**约束含义**：（1）任何后续 L1/L2 数据源接入都必须 streaming-by-default，不准累积全量 list；（2）L1 在 256MB Fly 上达到 7 天 soak 是 phase 02 完成定义的一部分，不准用"升内存"绕过；（3）此约束适用于所有 m1-perception future phases，不仅本 phase。**纪律来源**：feedback memory `fix-code-not-config-2026-05` + `profile-with-real-data-2026-05`。

### the agent's Discretion

以下交给 researcher / planner 在 plan 阶段决定（用户未在 discuss 阶段锁定）：

- **DB schema 双端同步路径** — L1 daemon 是写 SQLite + 同步到 R2 Parquet，还是直接写 Supabase Postgres？涉及 Phase 01.1 现有 `sqlite_store.py` 改造范围。Phase 02 主要工作量级 hinges on this. researcher 调研 Supabase JS/Python SDK + 双端 schema 工具（pg_dump / Alembic 等）后给 plan 推荐。
- **Snapshot 保留策略 cron 落地** — Phase 01.1 加了 `make snapshots-purge`，云上需要默认调度策略（保留 N 个 / 按天清 / R2 archive 后清 SQLite？）。planner 在 Phase 02 plan 推荐保留窗口（建议默认 30 天 SQLite + R2 永久 archive）。
- **框架抽象 A 落地范围** — thread §1.5 的"统一市场状态模型"（含 stamp 时间 vs 抓取时间显式分离）是 Phase 02 同步动还是 Phase 02.1 后续？建议 Phase 02 至少落地 fetched_at_ms 分离（L2 修），完整 Market State dataclass 可推迟。planner 决定。
- **Dockerfile multi-stage 细节** — uv 缓存层 / non-root user / healthcheck command — researcher 调研 Fly + uv + Python 的当前 best practice 后给具体范本。
- **GHA workflow YAML 细节** — concurrency control / cache 策略 / test matrix — planner 决定。
- **/health endpoint schema** — 返回什么 JSON 字段才能让 Better Stack 区分 "alive vs degraded vs stale"？planner 设计。
- **FastAPI vs Starlette vs Flask 选 framework** — daemon 加 HTTP API server，researcher 调研 Fly 上 Python 长跑 + 偶尔 RPC 调用的轻量框架推荐。

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### 上游决策与实证
- `.planning/threads/market-observation-architecture.md` §1 (L1 生产级判定标准) — Phase 02 的"完成定义"在此
- `.planning/threads/market-observation-architecture.md` §1.5 (平台框架抽象层 A/B/C) — 框架抽象的范围说明
- `.planning/threads/market-observation-architecture.md` §2.1.a — fetched_at_ms 实证 + 8 分钟 elapsed + 漂移分布
- `.planning/threads/market-observation-architecture.md` §2.5.a — 5 维度生产级缺口（CLI 入口断裂 silent failure 等）
- `.planning/threads/market-observation-architecture.md` §2.7 — subset/full 决策实证
- `.planning/threads/deployment-architecture.md` §0.1 (用户 4 锚点决策已锁) — PaaS 混合 / CN 不约束 / DB 合并 / KMS 延 M3
- `.planning/threads/deployment-architecture.md` §2.1.7 / §2.2 / §2.3.6 / §2.4 — 4 类候选栈对比汇总
- `.planning/threads/deployment-architecture.md` §3.1 / §3.2 — 免费堆叠 / $30 档推荐组合（Phase 02 起步）
- `.planning/threads/deployment-architecture.md` §5.1 — 阶段映射（Phase 02 = 启动期）

### Phase 01.1 资产与经验
- `.planning/workstreams/m1-perception/phases/01.1-observation-toolkit/01.1-LEARNINGS.md` — 14D / 12L / 10P / 8S 全部学习；尤其 D5/D7/D11/L2/L11/L12/P1/P3/P8 与 Phase 02 直接相关
- `.planning/workstreams/m1-perception/phases/01.1-observation-toolkit/01.1-CONTEXT.md` — Phase 01.1 锁定决策（部分仍生效，如 cli single-file / yaml.safe_load）
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-SECURITY-REVIEW.md` — Phase 1 安全审查（F-1..F-5），Phase 02 暴露 HTTP endpoint 需要补 F-7+

### 工程纪律基础设施
- `.githooks/pre-commit` + `scripts/planning_status.py` — plan-末 SUMMARY 三件套（D13）
- `CLAUDE.md` "每个 Plan 末" 段 / "命令入口约定（Makefile 强制）" 段

### 项目章程
- `.planning/PROJECT.md` — 项目原则（生产级 / 知识完整性 / 云原生）
- `CLAUDE.md` — Claude 角色契约 + 工作模式

### 业内参考（thread §8.5 调研）
- `3th-party/clawfirm/` — Docker multi-stage + systemd + 嵌入前端的部署范本
- `3th-party/polymarket-kalshi-weather-bot/` — Vercel 前端 + Railway/Nixpacks 后端的部署范本

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/polyarb/snapshot/orchestrator.py` — 已有 8-step pipeline + 三态健康判定 amendment 落地；Phase 02 加 entrypoint wrapping（cron-friendly） + alert hooks
- `src/polyarb/observation/scanner.py` — 4 层 SQL 防御 + 6 内置配方 + run_recipe 接口；Phase 02 dashboard scan trigger 直接调
- `src/polyarb/observation/recipes.py` — Recipe trust-split pattern；Phase 02 复用，不在 Supabase 端重做
- `src/polyarb/cli_observation.py` — typer single-file CLI；Phase 02 可能加 HTTP server entrypoint，evaluate FastAPI vs Starlette
- `Makefile` — 全部命令入口；Phase 02 加 `make deploy` / `make smoke-test` / `make tail-logs`
- `data/state.db` + `data/snapshots/YYYY/MM/DD/` — 本地 SQLite + Parquet 路径；云上需迁移决策（见 the agent's Discretion）

### Established Patterns
- **uv 包管理 + uv.lock** — Dockerfile 必须 `uv sync --frozen` 跑（不是 `pip install`）
- **SQLite WAL mode + BEGIN IMMEDIATE 事务** — 单 writer / 多 reader OK；云上 daemon + dashboard 双进程访问同 DB 可能踩 lock，需评估
- **Atomic Parquet writes** — tmp + os.replace 已存在；R2 同步可走 boto3 / aws-cli aws s3 sync
- **Loguru log** — 替代 stdlib logging；Axiom 接 stdout 即可，不需要额外 handler
- **Pre-commit hook + planning-status drift** — 已落地，云上 deploy 触发前 GHA 也要跑

### Integration Points
- **Fly Dockerfile / fly.toml** — 新建（无现有）；workdir / 端口 / cron schedule / volume mount
- **GHA workflows (.github/workflows/)** — 整个 .github/ 目录都没有，需建
- **Supabase 项目 + schema** — 不存在，研发期都跑本地 SQLite；plan 阶段决定迁移粒度
- **R2 bucket** — 不存在，需创建 + AccessKey
- **Vercel 项目 + Next.js scaffold** — 不存在，需建
- **Telegram bot + Better Stack monitor / Axiom workspace / Sentry project** — 全部启动期新建

</code_context>

<specifics>
## Specific User Intent

- **"生产级 = 可长跑 7×24，单次跑通不算数"**（thread §0.3 #6） — 验收必须看到至少 7 天连跑无人值守 + 一次自然失败后系统自愈或正确报警
- **"一键部署是工程纪律 — 部署成本低 = 迭代成本低"**（thread §0.3 #9） — Phase 02 末期 `make deploy` 应该是真正的 1 命令 + 全自动
- **scan trigger 是观察不是策略**（用户 2026-05-12 discuss） — dashboard 可以加交互按钮，因为它只是把本地 `make scan-near-end` 搬到 web，不构成 L2 高频跟踪。这条解读建议存入 feedback memory（用户对纪律的实务判断）
- **PaaS 混合路线**（D11） — staging 用 PaaS + prod 后期可 DIY VPS，但 Phase 02 阶段 prod 也走 PaaS，不预设迁移
- **不在 plan 阶段提早问 KMS / Tiger Cloud** — 这两个明确推迟，researcher 不去调研

</specifics>

<deferred>
## Deferred Ideas

### 推到 Phase 02.1 / Phase 03+
- **watchlist 编辑能力** — Phase 02 dashboard 仅显示 watchlist 状态，不可编辑；用户继续手编 yaml + git diff sync。M2 阶段策略化时再加 web 编辑层
- **WebSocket 增量数据流（旧 Phase 2）** — thread §1 纪律推到 Phase 3，作为 L3 单市场 K 线的数据源；需先调研 Polymarket WS 真实能力边界（thread §2.2）
- **L2 定向跟踪 daemon** — Phase 02 完成验证 L1 生产级后开（thread §1 层级纪律）
- **PR 预览环境（Fly preview apps）** — 启动期单 environment 足够；M3 实盘前如果 PR 量大可考虑

### 推到 M3 实盘前
- **AWS KMS 签名链路**（D11） — Phase 02 daemon 全只读，无私钥需求
- **Tiger Cloud 双库**（D11） — Supabase 单库撞墙才拆
- **私网出站 + 固定 IP 白名单** — Fly dedicated egress IPv4 ($3.60/月) 在 thread §5.2 if-then 中说明 "L1 跑稳但被 Polymarket Cloudflare 限流" 时再加；Phase 02 启动期不预设

### 推到 M5 工业化
- **多 region failover** — Fly multi-region 在 thread §5.1 实盘期才上
- **完整 metrics 体系（Prometheus / Grafana Cloud Pro）** — 启动期 Axiom + Sentry + Better Stack 三件套足够

</deferred>

---

*Phase: 02-l1-production-grade*
*Workstream: m1-perception*
*Context gathered: 2026-05-12*
*Next step: `/gsd-plan-phase 02 --ws m1-perception`（researcher 先调研 the agent's Discretion 中的 7 项）*
