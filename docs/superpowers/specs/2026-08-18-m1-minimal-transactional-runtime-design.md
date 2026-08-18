# M1 最小事务型运行时设计

## 目标

在唯一新的 Supabase 权威库上恢复 M1 的持续采集，而不是复活任何旧 SQLite 或旧云资源；在启动采集前，所有数据库不可用、机器停止和推进停滞都必须由独立监控链显式报告。

## 正式最小拓扑

1. `polyarb-control-api`：256MB、只读 Postgres HTTP 读模型；它是数据库可用性和队列状态的独立探针。
2. `polyarb-control-alert`：256MB、无 Postgres/R2/Gamma/CLOB 权限；每 30 秒读取 API 与精确工作机集合，仅在异常或恢复转换时发送 Telegram。
3. `polyarb-control-worker`：初始只运行三个 fenced 角色各一台：coordinator、structure-range、quote-batch。它们共享同一个新 Postgres 与受限 R2 凭据；不启动历史的第二副本或 soak sampler。

该布局把持续采集的最低生产闭环限制为 5 个小型职责单元；吞吐不足只能通过可观测的 backlog 证据扩容，不能先按旧六台 2GB 调试规模预留。

## 部署与身份契约

- rollout renderer 必须从部署后取得的精确 worker machine ID 生成 watchdog/soak 参数；模板不得保留任何已删除机器 ID。
- API、worker、alert 使用三个不同 Fly app；不得复用 `polyarb-l1`，也不得读取或迁移旧 SQLite。
- worker runtime DSN、API read DSN、R2 key、Telegram/Fly-read token 分别最小授予；所有值仅从 Keychain/Fly secrets 读取，不能写入仓库或日志。
- 新 Supabase 的 Data API service key 仅用于 L2 mirror；L3 evidence 使用已验证的 `l3_evidence_runtime` 独立登录角色。

## 验收顺序

1. 迁移与最小权限 preflight 通过；API 的 `/healthz` 与 `/perception/control-plane` 分别可达且后者返回 `available`。
2. 启动 watchdog，强制检查 API 与每个 worker machine；先验证其 heartbeat，再启动业务 worker。
3. 启动三个 worker，证明 durable succeeded-job count 单调前进、无失效 lease/circuit 异常、API 仍可读。
4. 只有前三项稳定后，才创建新的云端 24 小时 soak baseline；旧证据和旧资源均不可作为回退或验收。

## 非目标

- 不升级 Supabase 付费套餐，不创建第二个 Supabase 项目。
- 不重启或替换现有 `polyarb-l2`；它保持单独的 L2/L3 数据面。
- 不在本次重新跑旧的本地/云端 24 小时证据，也不执行真实交易。
