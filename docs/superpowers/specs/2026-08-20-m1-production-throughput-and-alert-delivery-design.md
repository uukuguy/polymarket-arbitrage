# M1 生产吞吐与告警投递设计

## 背景与事实

2026-08-20 的线上核查表明，M1 不是一个已可用的机会发现服务：Structure
generation 有 2,293 个未完成范围，机会投影只能公开上一次完整 Quote 的零结果，
而 `m1_alert_outbox` 存在待投递意图。三个现象的共同根因是生产部署没有把
事务模型的消费者边界完整落地。

具体而言，现有 Fly alert Machine 只运行独立 runtime watchdog；
`TransactionalAlertDeliveryWorker` 虽已实现，却没有任何云端消费者。Structure
range 进程使用固定 worker id，并且每个 tick 只消费一个范围后休眠，因而既不能
安全横向扩容，也无法在一个大 generation 中将完成速度维持在可接受范围。最后，
Structure certifier 每五秒把“尚有范围未完成”作为 retryable job，这保留了
fail-closed 语义，但将正常等待错误地放进重试/告警模型。

## 决策

采用三个相互独立、可单独回滚的边界：

1. 新建私有 `polyarb-control-alert-delivery` Fly app。它只运行
   `alert-serve`，使用一个只可读写 alert outbox、delivery receipt 和所需
   incident 记录的数据库登录角色，以及 Telegram 凭据。Dashboard channel
   由该消费者写 delivery receipt，Telegram channel 则得到 provider receipt。
   Watchdog 保持数据库独立，仍只负责 runtime 级的即时检测。
2. Structure range worker id 由固定文本改为包含 Fly Machine identity。部署模板
   采用可扩的同一 process role；至少运行两台 range Machines，并把每台的
   `pool_turns=2`、`interval_seconds=2` 作为基线。Postgres lease epoch 仍是
   唯一所有权；相同 job 不会被并发处理。扩容只提高独立 job 的并行度。
3. 将 `IncompleteStructureGenerationError` 从 retryable failure 改为
   `waiting` 状态（或同等的无错误下一次检查语义）：认证器只在最后一个 range
   receipt 写入时立即重新唤醒。若内容校验、R2 或数据库操作失败，才保留
   retryable incident 与 outbox 告警。任何未完整 generation 仍绝不发布。

## 可选方案与取舍

- 只把当前单机 `pool_turns` 调大：成本最低，但只减少空闲时间，不能安全并行，
  对大 generation 的尾部延迟没有足够保障。
- 在既有 worker app 加一个 alert process：实现快，但会把 Telegram 与完整
  Structure/Quote/R2 权限放进同一故障域。
- 本设计（推荐）：独立投递 app + 可扩 range pool + 事件驱动认证等待。多一个
  256MB 服务，但保持最小权限、隔离故障，也让积压恢复有确定的容量路径。

## 约束与验收

- 不发布任何部分 Structure、Quote 或 opportunity generation。
- 每个 pending/retryable outbox 事件最终有 Dashboard 和 Telegram 的 delivery
  receipt，且历史积压以受控回放方式处理，避免无界 Telegram 重放。
- 在两台 range Machines 下，连续采集时 backlog 必须下降；当无新的 source
  generation 时，认证应在最后一条 range receipt 后推进到 Quote 与机会投影。
- 正常的不完整等待不得产生 job-retry incident 或 Telegram 告警；真实失败必须
  同时出现在 Telegram、Dashboard ledger 和 delivery receipt。
- 因部署会改变正式拓扑，重新开始唯一命名的 24 小时云端证据窗口；旧窗口只作历史。
