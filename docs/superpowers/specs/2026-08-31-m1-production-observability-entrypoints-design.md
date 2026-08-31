# M1 生产观测入口修复设计

**日期：** 2026-08-31  
**范围：** M1 现役控制面运行验证入口

## 问题

历史 Makefile 入口 `smoke-health-prod`、`smoke-market-truth-prod` 和
`smoke-l2-health-*-prod` 指向已删除的 `polyarb-l1` / `polyarb-l2` Fly app。
这两个 hostname 现在为 DNS NXDOMAIN，因而这些命令会稳定失败，却容易被误读为
现役 M1 服务故障。

现役生产验证边界是 `polyarb-control-api`：Fly 记录其 Machine 为 `started`，平台
`/healthz` 检查通过，且运行精确 release `3a70cd9f…`。业务真相仍由
`make control-plane-status` 的只读 durable-control-plane 查询提供。

## 决策

新增语义明确的公开探针，并让旧入口 fail-loud；不做隐式兼容重定向。

### 新入口

`make smoke-control-plane-prod`

- 只读 `GET https://polyarb-control-api.fly.dev/health`。
- 输出 HTTP 状态与 JSON body；仅 HTTP 200 且 JSON `status=ok` 时成功。
- 验证公开控制 API、其数据库 readiness 与外部路由；不使用 Fly API、数据库 DSN、
  deploy、restart、secret 或任何业务 mutation。

### 业务真相入口

`make control-plane-status limit=<n>` 保持不变。

- 它是带已配置只读数据库凭据的业务状态读取：工作进度、lease/circuit、资格、
  incidents、watchdog 与最新发布 lineage。
- 它不能替代公开 HTTP 探针；公开 HTTP 探针也不能替代业务资格结论。

### 旧入口

以下命令不再尝试访问已退役 app；应以非零退出并输出迁移指引：

- `smoke-health-prod`
- `smoke-market-truth-prod`
- `smoke-healthz`
- `smoke-l2-health-prod`
- `smoke-l2-health-strict-prod`
- `fly-l2-status`

迁移指引必须区分两种语义：

- 控制 API 外部可达性 → `make smoke-control-plane-prod`
- M1 业务/资格状态 → `make control-plane-status`

旧 L1 的 market-truth 与旧 L2 orderbook 业务已无现役 producer，因此不能把任一旧
命令透明重定向为 control-plane 健康检查。

## 文档与可发现性

- 更新 Makefile 的帮助注释及 `.PHONY`。
- 更新 `docs/M1-市场感知平台使用手册.md` 的日常巡检顺序、安全分级和旧 L1/L2
  指令说明。
- 在 `.planning/JOURNAL.md` 追加发现与替代入口；不改写历史记录。
- 更新 `.planning/CURRENT.md` 的“当前下一步”，使其以新的公开探针加
  `control-plane-status` 作为只读生产证据组合。

## 验收与回归

1. Makefile 测试断言新目标使用现役 hostname、HTTP 200 与 JSON `status=ok` 双门。
2. 测试每一个已退役入口 fail-loud，并包含正确的两个替代命令。
3. 测试新目标没有 Fly mutation、数据库写入或认证 secret 依赖。
4. 对现役 Fly API 做只读状态检查；平台必须报告 control API Machine `started` 且
   `/healthz` passing。
5. 在可正常解析 Fly hostname 的外部路径上运行新公开探针；当前本机代理路径可作为
   单独的环境故障记录，不得误判为服务失败。

## 非目标

- 不恢复或重新创建 `polyarb-l1` / `polyarb-l2`。
- 不修改生产机器、镜像、数据库、secret、资格或 recovery 状态。
- 不把旧市场快照/L2 语义伪装成控制面健康。
