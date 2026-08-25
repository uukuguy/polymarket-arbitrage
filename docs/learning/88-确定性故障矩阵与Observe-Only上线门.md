# 88. 确定性故障矩阵与 Observe-Only 上线门

## 30 秒心智模型

长时间采集系统不能把“再跑一次 24 小时”当成修复机制。先把故障变成可重复的短实验：在一次性真实 PostgreSQL 数据库里跑完整 migration，再注入 task exception、R2 timeout、heartbeat loss、progress stall、process exit、重复投递和 stale action 等 12 类故障，逐项验证检测、incident、action、Dashboard 投影、恢复和 qualification 影响。

通过矩阵只证明逻辑链完整，不自动授权生产 mutation。上线先进入 `observe-only`：controller 对每个候选写不可变 decision，没候选也写 idle；它必须在 `schedule_action` 和 `RecoveryExecutor` 之前返回。只有连续 decision 窗口与实时 runtime facts 一致、没有 gap、没有身份漂移且 recovery action 数为零，才具备请求下一道生产授权的证据。

## 代码地图

- `src/polyarb/control_plane/runtime_fault_matrix.py`：创建一次性本地数据库，执行 Alembic 到 head，跑 12 类确定性故障并清理数据库与 migration 临时角色。
- `src/polyarb/control_plane/runtime_observe.py`：定义 canonical decision/idle 事实、幂等写入和只读窗口验证。
- `src/polyarb/cli_control_plane.py`：把运行模式接进 controller；默认 `observe-only`，显式 `execute` 才进入 action 调度与 executor。
- `src/polyarb/control_plane/fly_recovery.py`：只允许精确 `(app, machine_id)` 的 capability-limited restart；controller/action lease、预算、竞争动作和独立健康确认都在 POST 前重查。
- `deploy/control-plane/fly-runtime-controller.toml.template` 与 `fly-qualification-worker.toml.template`：controller 和 qualification 各自独立应用、角色与凭据。
- `Makefile`：`make runtime-fault-matrix`、`make runtime-observe-verify` 和六应用 `make control-plane-render-rollout` 是统一入口。

## 关键代码片段

`src/polyarb/cli_control_plane.py:1243` 到 `src/polyarb/cli_control_plane.py:1369` 把观察和执行分成两条互斥路径。observe-only 会遍历有界候选并逐条写 decision；空集合写 idle；`RecoveryExecutor` 只存在于 `else` 执行分支。

```python
if recovery_mode == "observe-only":
    if evaluated:
        for candidate, decision in evaluated:
            insert_runtime_observe_decision(
                connection_factory,
                build_runtime_observe_decision_record(...),
            )
    else:
        insert_runtime_observe_decision(
            connection_factory,
            build_runtime_observe_idle_record(...),
        )
else:
    scheduled = schedule_action(...)
    result = RecoveryExecutor(...).run_once(now=now)
```

`src/polyarb/control_plane/runtime_fault_matrix.py:98` 到 `src/polyarb/control_plane/runtime_fault_matrix.py:135` 的关键不是 mock 一个 policy 函数，而是创建空数据库、升级真实 schema、验证 trigger/FK/unique authority、运行矩阵，再在 `finally` 删除精确数据库。输入 DSN 还必须是 loopback，并拒绝 Supabase、Fly、production 标记和 query 注入。

`src/polyarb/control_plane/fly_recovery.py:137` 到 `src/polyarb/control_plane/fly_recovery.py:224` 展示 mutation 前的最小能力链：action class 显式开启、controller 身份/epoch/lease 一致、action worker lease 有效、target 精确 allowlist、数据库 preflight 确认、独立健康仍失败，然后才读取 token 和 POST。provider body 与原始异常永不进入结果。

## 三种“通过”不要混淆

1. **fault matrix pass**：本地真实 schema 下，故障链可重复并满足预期；没有接触生产。
2. **observe-only pass**：生产 controller 连续记录的 decisions 与同一快照 runtime facts 一致，窗口内 recovery action 为零；仍没有开启 mutation。
3. **recovery gate pass**：在指定 release、指定 target、指定 fault 的单独授权下，真实执行 fenced action 并由 Dashboard、Telegram、incident/action ledger 与 qualification 同时证明恢复。

前两者不能推导第三者。尤其 `--enable` 只允许 controller 服务运行，不等于允许 recovery mutation；真正的 mutation 开关是闭集配置 `POLYARB_RUNTIME_RECOVERY_MODE=execute`，process/Machine action 还需要独立 action-class flag 和精确 allowlist。

## 设计取舍

- 故障矩阵用真实 PostgreSQL，不用内存 fake：我们要验证 trigger、FK、唯一索引、事务回滚和 qualification ingress，而不只是纯 policy 输出。
- 临时数据库，不复用开发 schema：失败时能按已验证名称精确清理，也不会污染人工数据；cluster-global migration role 仍需 advisory lock 和 fail-closed 检查。
- observe-only 也写 durable 事实：只打印日志无法证明 30 分钟没有漏 tick、没有身份换代混入、没有 action mutation。
- 所有候选都记录：三个采样点同时失败时，不能因为 controller 本轮只能执行一个 action，就只留下一个故障的观察证据。
- process/Machine adapter 不 shell `flyctl`：固定 API origin、固定 timeout、闭集结果和不回显 response body，缩小凭据和命令注入边界。

## 自检题

1. `runtime-fault-matrix` 连续两次 100% 通过，为什么仍不能直接开启 production execute？
2. observe-only 没有任何异常候选时，为什么必须写 idle，而不是安静跳过？
3. 三个任务同时超时，但执行模式每轮只处理一个 action；为什么 observe-only 必须记录三个 decision？
4. Fly restart 前已经有 controller lease，为什么还要 action worker lease、数据库 preflight 和独立健康确认？
5. `--enable` 与 `POLYARB_RUNTIME_RECOVERY_MODE=execute` 为什么必须是两个不同的门？

## FAQ 增量

**Q: 定时器是不是又成了唯一检测器？**

A: 不是。任务自己的 terminal/retry/incident 事实仍是第一检测器；deadline controller 处理 heartbeat/progress/lease 等缺失事实。observe-only 只是把 controller 每次判断也变成可验证事实，避免“controller 在跑”只能靠日志猜。

**Q: 为什么不继续用 24 小时 soak 找 bug？**

A: 24 小时窗口适合最终连续性证书，不适合调试反馈。确定性矩阵把 12 类故障压缩成分钟级可重复实验；修复后先跑矩阵，再用 rolling qualification 自动累计长窗口，失败时由事实立即 invalidated，不再人工从零启动一次又一次。
