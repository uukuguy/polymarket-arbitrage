# 生产链 Commissioning 与健康有效秒

## 30 秒心智模型

24 小时资格窗不是集成测试，也不是“期间完全不能出错”。上线前先对
Structure→Quote→Opportunity 的八个节点逐一 commissioning：跑通正常生产回合，
再定向注入该节点已知故障，证明系统能够探测、启动预案、恢复、清理，并重新满足
业务后置条件。所有证据绑定同一个 release/config；少一项，资格窗就不能开始。

资格窗开始后只累计健康有效秒。普通故障发生时暂停计时，恢复确认后在同一 epoch
继续；证据缺口、超 SLO 或人工介入会阻塞但保留既有健康秒。只有数据完整性、围栏、
发布身份或配置身份被破坏，才说明这段历史不可信并作废整个 epoch。

```text
正常回合 + 定向攻击闭环 + E2E lineage
                  |
                  v
           commissioning ready
                  |
                  v
     eligible --故障--> paused/blocked --恢复确认--> eligible
                  |
        truth/identity 被破坏
                  v
             invalidated
```

## 代码地图

- `src/polyarb/control_plane/production_commissioning.py:41`：攻击与生产节点的闭合契约。
- `src/polyarb/control_plane/production_commissioning.py:334`：八节点严格复用运行时 DAG，
  不维护第二套顺序。
- `src/polyarb/control_plane/production_commissioning.py:369`：生成只读 commissioning plan。
- `src/polyarb/control_plane/production_commissioning.py:480`：fail-closed 验证 release、config、
  正常回合、全部攻击、清理和最终 lineage。
- `src/polyarb/control_plane/production_commissioning_runner.py:64`：一次攻击的 exact identity
  与每个阶段的 durable receipt。
- `src/polyarb/control_plane/production_commissioning_runner.py:195`：disposable attack 串行执行、
  失败后强制 cleanup 与 append-only 阶段文件。
- `src/polyarb/control_plane/production_commissioning_runner.py:331`：从 66 个 proof、八个正常
  回合和最终 lineage 汇总一份可验证 envelope。
- `src/polyarb/control_plane/production_commissioning_disposable.py:27`：在 disposable
  PostgreSQL 上完成八类真实 domain transaction，并只返回数据库中存在的
  terminal attempt、runtime success event 和因果绑定的业务后置条件 ID。
- `src/polyarb/control_plane/production_commissioning_harness.py:48`：先验证精确
  release/config 和显式 loopback test DSN，再按节点运行隔离攻击；CLI 不提供
  跳过节点的选项。
- `src/polyarb/control_plane/runtime_fault_matrix.py:139`：fault matrix 与 commissioning
  共用唯一的临时库创建、Alembic 迁移、authority 校验、删库和角色回收生命周期。
- `src/polyarb/control_plane/qualification.py:30`：真正作废 epoch 的严重原因闭集。
- `src/polyarb/control_plane/qualification.py:42`：暂停或阻塞健康秒的原因闭集。
- `src/polyarb/control_plane/qualification.py:656`：单条事实如何改变资格状态。
- `src/polyarb/control_plane/qualification.py:794`：只在上一段时间可计入时增加健康有效秒。
- `src/polyarb/control_plane/qualification_service.py:1093`：incident 生命周期如何映射为
  `recovery.started`、`incident.p1-slo` 或 `recovery.confirmed`。
- `Makefile:772`：统一的 plan 与 evidence verify 操作入口。

## 一份攻击证据为什么不是“测试通过”四个字

每份证明必须有严格递增的生命周期：

```text
injected_at < detected_at < recovery_started_at < recovered_at < verified_at
```

并且同时保存 detector fact、recovery action、recovery fact、业务 postcondition fact 与
`cleanup_verified=true`。这能区分三种很容易混淆的结果：

- 进程重启了，但业务数据仍旧没有恢复；
- 数据恢复了，但注入器或临时权限没有清理；
- 日志看起来恢复了，但证据来自另一个 release/config。

共享故障也必须在每个目标节点分别证明。例如 heartbeat outage 对八个节点都有意义，
不能只在 Structure 测一次就推断 Quote 一定正确。当前契约有 18 类攻击，对八节点展开后
形成 66 份节点级证明。

## 为什么普通故障不应重置 24 小时

原模型把“系统发现并修复了一个预期故障”与“证据历史已经失真”混为一谈。结果是
自愈越积极，资格窗反而越容易被重置。新模型区分：

- `pause`：故障或恢复正在进行，本段时间不计数；
- `block`：恢复超 SLO、证据缺口或人工介入，需要处理，既有健康秒保留；
- `invalidate`：stale owner 成功写入、完整性冲突、进度事实倒退或身份漂移，历史不再
  可相信，必须开启新 epoch。

`coverage_seconds` 因此不再等于 `now - started_at`。证书只能使用累计健康有效秒，不能
用墙钟时间把暂停区间偷偷算进去。

## 可中断不等于可以留下未清理注入

一次攻击会依次生成 `00-intent`、preflight、injected、detected、recovery-started、
cleanup、recovered、verified 与最终 proof 文件。文件使用 exclusive create，已有内容
不会被重跑覆盖。这样进程中断后可以明确看到停在哪个阶段，残缺目录也永远不能被汇总
成 ready 证据。

runner 自身不再套一个“整个实验 120 秒”的外层 timeout；Gamma、PostgreSQL、R2、lease
和 progress 各自使用它们正在验证的权威预算。只要 injection 已经成功，后续 detection
或 recovery 抛错也会进入 cleanup；如果原故障和 cleanup 同时失败，两条异常会一起
保留，不能用 cleanup 错误覆盖根因。

## 设计取舍

1. **先列契约，再写注入器。** 注入器如果先行，很容易只覆盖容易制造的故障；闭合矩阵
   先规定每个节点必须交出的恢复与业务证据。
2. **大部分攻击放在 disposable exact-image。** 这能验证围栏、租约、重试预算和持久
   状态机，而不污染生产主链。只有 Gamma/CLOB 等真实供应商边界使用精确 production
   canary，且必须有清理证据。
3. **Commissioning 不代替持续监控。** 它证明预案曾经可用；运行期 watchdog、incident
   ledger 与 qualification 继续证明当前仍可用。
4. **不用跨源全局计数判定 progress regression。** Structure、Quote、Opportunity 的
   数量不可直接比较；只有生产者作用域明确声明的倒退事实才可作废 epoch。

## 自检题

1. 一个 Gamma timeout 被自动重试并在 70 秒后恢复，为什么不应自动抹掉之前 18 小时
   的健康证据？什么条件会让它进入 block？
2. `worker-exit` 攻击后新 worker 成功了，但旧 owner 的 terminal write 没有被显式拒绝，
   该节点能否 ready？为什么？
3. 为什么 `cleanup_verified` 与业务 postcondition 必须同时存在？
4. Quote 的 `successful_count=3` 小于 Structure 的 `progress_count=10`，为何不能据此判定
   `progress.regressed`？
5. 哪四类真实外部边界值得 production canary，哪些故障应留在 disposable exact-image？
6. 一个攻击目录只有 `20-injected.json` 和 `50-cleanup.json`，为什么它既有审计价值，
   又绝不能被 resume 成一份成功 proof？

## FAQ 增量

### Commissioning 通过后，是不是以后再也不用攻击？

不是。release/config 改变后证据身份不匹配，必须重新完成受影响合同；长期运行也应按
风险和变更触发重演。资格窗负责持续健康事实，commissioning 负责证明已知恢复预案能
被真实触发并闭环，两者互相不能替代。

### 正常回合为什么需要三类事实？

`attempt=succeeded` 只说生命周期已终止；`job.succeeded` 事件说明 runtime
观测链看到了终态；业务后置条件则证明该节点真的产出了下游可消费事实。
只有三者同时存在，才能排除“状态写成成功但业务事实缺失”和“业务成功但
监控链静默”。`postcondition_fact_id` 还必须按当前 lease/job identity 精确查询，
不能用“表里随便有一行”充当该回合的证据。

### stale owner 为什么要重用同一个 terminal transaction？

如果攻击只直接调用一个“检查 epoch 不相等”的小函数，它不能证明真实业务提交
的 SQL 顺序里没有绕过围栏。`PreparedNormalTurn` 会把八种节点停在它们各自的
terminal API 之前；攻击通过正式 `claim_job` 在租约到期后获取新 epoch，然后让旧
和新 owner 分别调同一个 commit 闭包。旧 owner 必须得到 `StaleLeaseError`，而且
旧 epoch 的 succeeded attempt/event 均为 0；新 owner 才能产生终态与业务后置条件。

这个测试不直接 `UPDATE lease_expires_at`，因为那会绕过我们正在验证的 claim/
attempt 接管链。虚拟 `now` 是 control-plane API 既有的确定性输入，不是新的 timeout。

### 为什么围栏拒绝不写一条“拒绝成功”业务事件？

围栏的不变式正是 stale owner **不能写**。如果为了好看的 detector fact 反而让旧
owner 在同一事务里追加一条事件，就自己破坏了被验证的边界。因此 runner 证据使用
真实原 attempt、replacement attempt、新 epoch 的 `job.succeeded` event 和业务
postcondition；“旧 epoch 无 success attempt/event”由 detector 和 cleanup 两次查询证明。
这是负向事实与正向事实的组合，不是伪造一条业务事件。

### 为什么每个节点要用独立临时数据库？

一个节点的真实 terminal transaction 可能排队下游 job、移动 pointer 或留下
runtime event。如果八个攻击复用同一个库，后一个攻击就可能误拾前一个攻击的
successor 或业务事实。现在 harness 每个节点创建一个名称受闭合正则约束的
`m1_commissioning_<uuid>` 库，用真实 Alembic head 迁移后运行攻击，最后无条件
终止连接并删库。

迁移会创建 cluster-global role，所以生命周期还需要 advisory lock 和“清理所有权”：
只有在启动前确认这些 role 不存在，才允许本次实验回收它们。如果发现预存
role，harness 立即拒绝运行，但绝不会删除别人的集群状态。攻击失败与连接清理
失败同时发生时，两个异常会一起保留，避免“只看到 close 失败、丢了攻击根因”。

### progress stall 为什么必须保持 lease 仍然有效？

如果停止 progress 的同时也停止 heartbeat，最先触发的可能是 heartbeat missing 或
lease expiry。那只能证明接管过期 owner，不能证明 progress deadline 和 cancel 预案可用。
真正的 progress-stall 攻击先停在实际节点 terminal transaction 前的 durable checkpoint，
再按持久化 `RuntimeDeadlineProfile` 续同一 attempt 的 lease；只有 progress deadline 到期。
`RuntimeReconciler` 此时必须给出非 breaking 的 `cancel-job`，不能误判成 reclaim。

cancel action 也不能只把 action ledger 写成 succeeded。正式 `RecoveryExecutor` 必须让旧
attempt 进入 `RecoveryProgressStalled/retryable`、清除 lease，并使旧 terminal write 得到
`StaleLeaseError`。等数据库记录的 backoff 到期，新 epoch 重跑同一个节点事务，产出
`job.succeeded` 和业务 postcondition，最后用 `record_job_recovery` 关闭原 warning incident。
这条链证明的是“故障期间不计健康秒，恢复后继续积累”，不是用一次普通停滞重置整窗。

### 同一个缺陷耗尽重试后，为什么不能让普通 worker 继续抢任务？

重试次数本身不是恢复策略。若同一 failure fingerprint 连续出现，继续无条件领取只会形成
高频失败、告警风暴和下游资源挤占。正式链路让前两次失败按共享 `RuntimeRetryPolicy`
退避；第三次达到预算后把 circuit 设为 `open`，并通过 `job-retry:<job_key>` 只维护一条
incident 生命周期，而不是每次失败新建一个事故。

关键负证明发生在 `next_probe_at`：普通 `claim_job` 即使已经到达 job 的重试时间，也必须
仍被 open circuit 拒绝。只有持有新鲜 controller lease 的 reconciler 能把
`circuit.probe-due` 排成一个 fenced `probe-circuit` action；executor 成功后才释放一个
retryable successor。successor 的真实业务提交成功会把 circuit 重置成 `0/closed`、清除
failure fingerprint/next probe，并关闭事故。

所以“停止”只停止同一错误的盲目重放，不停止生产控制面：控制器、incident 和恢复预案
仍持续运行。只有 probe 成功且业务后置条件成立，节点才重新进入可用状态；这正是
`public.digest` 一类重复缺陷不应耗尽整个系统重试后永久停摆的边界。
