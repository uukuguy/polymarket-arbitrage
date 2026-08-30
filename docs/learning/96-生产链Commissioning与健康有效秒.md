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

### heartbeat 短暂失败，为什么不应立刻回收 job 或重启 Machine？

heartbeat 到期和 lease 到期是两个不同边界。前者说明 worker 的续租通道需要帮助，后者
才说明旧 owner 已失去排他能力。只要 lease 仍有效，controller 可以用带 attempt ID、
lease epoch 和 worker owner 的 fenced `heartbeat-job` 替它续租；这不会创建新 attempt，
也不会改变业务 checkpoint。此时直接 reclaim 会破坏仍有效的 owner，直接重启 Machine
则把一个数据库通道抖动升级成平台级故障。

commissioning 因此刻意不调用 worker heartbeat，而只把虚拟观测时间推进到持久化
`profile_heartbeat_seconds`。reconciler 必须给出 warning、non-breaking 的
`job.lease-at-risk`，executor 再按 `profile_lease_seconds` 延长同一 lease。最终只有 epoch 1
的一条 attempt，且由 renewed lease 完成真实业务提交，才能证明预案成立。

还要区分两种时间：action `started_at` 与 heartbeat mutation 使用同一个策略输入；action
`finished_at` 是 PostgreSQL 真正提交 ledger 时的 `clock_timestamp()`。把两者强行判等会
混淆确定性故障时间和真实事务完成时间。证据应分别验证“何时按策略续租”和“动作确实
完成”，而不是制造一条假的全局时钟。

### worker 退出后，为什么必须等 lease 到期，又为什么不重置 24 小时？

worker 进程消失不等于它过去持有的写权限立即失效。lease 到期前，旧 owner 仍是数据库
承认的 current owner；controller 此时只能记录 `job.heartbeat-missing-fence`，不能让另一个
worker 抢写。到期后，`reclaim-job` 在同一事务里把旧 attempt 标成
`RecoveryLeaseExpired/retryable`、清除 owner，并写入 retry/recovery 事实。随后旧 owner
再走真实 terminal API 必须收到 `StaleLeaseError`，新 worker 才能取得 epoch 2。

这类故障很严重，但不会让过去的健康数据变假。runtime 的历史字段
`qualification_breaking=true` 会使 incident 映射为 `incident.p1-slo`；在资格状态机里它属于
`BLOCKING_REASONS`，暂停累计并保留已有健康秒。不要把这个字段名误解为 epoch invalidate。
真正作废历史的 `BREAKING_REASONS` 仍只有 stale fence 成功写入、integrity conflict、
progress regression 和 release/config/policy/role identity 漂移。

因此 worker-exit 的完整闭环是：到期前 fence → 到期后 reclaim → 旧写拒绝 → 新 epoch
业务成功 → incident recovered → 同一资格 epoch 恢复累计。若缺任一步，只能保持 block，
不能用“进程已经重启”冒充生产恢复。

### 缺一条 Structure source receipt，为什么既不报警也不启动恢复动作？

因为它还不是故障，而是 fan-in barrier 的合法非终态。Structure window 先登记全部
`m1_structure_source_page_inputs`，每个 producer 通过带 lease epoch 的正式 API 提交
immutable receipt。只完成两条、还差一条时，`structure_source_window_digest()` 必须返回
typed incomplete condition；此时不能创建 materializer，更不能发布部分 bundle。

commissioning 的 `source-receipt-gap` 攻击刻意持有最后一个真实 producer attempt，而不是
直接改 receipt 表或把时间戳“老化”。它同时验证精确缺失 job identity、`3 inputs / 2 receipts`、
零 materializer、零 bundle、零 downstream range、零 incident 和零 recovery action。最后再通过
同一个 fenced producer API 提交缺失 receipt；正常事务边界只释放一个 materializer turn，
并产出一个完整 bundle 和一个 normalizer range。

这条预案的关键不是“等多久后重启”，而是“durable input 是否齐全”。如果 producer 后续真的
超过自己的持久化 progress/lease/retry policy，已有 progress-stall、worker-exit 或 retry-budget
路径会单独接管；barrier 本身不发明第二套 timeout。这样慢任务不会被任意外层时钟误杀，真正
失效的 producer 又不会永久静默。

### Quote batch 缺失时，incident 应该由 certifier 还是 producer 持有？

由缺失 receipt 的 producer 持有。Quote certifier 的 barrier 同时检查每个
`m1_quote_batch_receipts` 和对应 batch job 的 `succeeded`；条件不齐时 certifier 保持
`waiting`，根本不应被 claim。如果为了“让 certifier 报警”而强行唤醒它，就绕过了正在验证的
fan-in fence，还会产生两个相互竞争的重试时钟。

`quote-batch-incomplete` commissioning 因此先让一个真实 batch 成功，再让另一个带 lease epoch
的 batch 通过 `finish_retryable_with_incident()` 进入 retryable。这个单事务同时写 retry due-at、
circuit fingerprint、incident event 和 Dashboard outbox；certifier 仍等待，manifest、pointer 和
opportunity successor 都必须为零。

到共享 `RuntimeRetryPolicy` 的 due-at 后，新 epoch 才能 claim 同一个 batch。它提交 immutable
receipt 并用 `record_job_recovery()` 关闭原 incident；最后一条 terminal receipt 在正式事务中释放
certifier。certifier 再一次性校验全部 receipt，原子提交 manifest、`quote:current` 和下游 job。
所以故障可见性属于出错节点，完整性门属于消费节点，两者各守一个职责，不需要 Machine restart
或 barrier 私有 timeout。

### “Structure shard 读不到”为什么必须报告 artifact key，而不能只报 TimeoutError？

因为恢复动作需要知道哪一个 immutable input 缺失。只有 `TimeoutError + fingerprint`，操作员最多
知道 quote-admit 失败，却不知道该检查 manifest 的哪一片；这会把可自动定位的对象故障升级成
人工翻日志。反过来，直接保存 provider response/body 又可能泄露签名、请求头或账户信息。

现在 v3 manifest 先授权每个 `artifact_key + artifact_digest`。读取某片失败时，worker 抛出
`QuoteAdmissionShardUnavailable`，incident detail 只加入 manifest 中已经公开的
`missing_artifact_key`；底层 provider exception 只保留为 Python cause，不进入运维 payload。
恢复者因此能精确 HEAD/恢复这一 key，同时没有扩大秘密面。

commissioning 还验证了一个容易误解的存储边界：Quote admission 成功后，PostgreSQL 的
`m1_quote_batch_inputs` 不会再次存两份 legs/token IDs。真实 authority 是已 HEAD 验证的 Quote batch
artifact，数据库只存 key、SHA-256 和 `leg_count`。恢复证据必须核对这三者，而不是为了测试方便
要求冗余 JSON 字段存在。

### 为什么 schema corruption 不能按普通 timeout 重试？

timeout 表示“同一输入稍后可能成功”，所以进入共享 backoff/circuit；而 digest 已认证但 schema
非法的 immutable shard 表示“同一输入永远不会变好”。继续重试只会耗尽预算并掩盖根因。

因此 normalizer 使用另一条终态：在一个 lease-fenced PostgreSQL 事务内写入
`job.terminal-failed`、quarantined job/attempt、critical incident 和 Dashboard outbox。
qualification 被阻断，操作员能看到精确且安全的 artifact key，但旧 certified Structure shadow
pointer 不动，未损坏的生产读链仍有权威来源。

### 为什么“还差 receipt”和“receipt 已齐但计数冲突”不能共用 waiting？

前者是 fan-in barrier 的合法中间状态：上游仍可能通过已有 lease/retry policy 补齐，
certifier 应保持 `waiting`。后者已经拥有全部 receipt，却无法与冻结的 source identity
对上；这不是“再等一会”能改变的事实。

现在 `StructureParityMismatchError` 在
`src/polyarb/control_plane/postgres.py:84` 单独表达这个完整性冲突；真实 certifier 在
`src/polyarb/control_plane/structure_worker.py:776` 把候选 generation 隔离，写入
`integrity.conflict + qualification_impact=invalidated` 的 terminal fact。这会作废当前资格
epoch，因为历史上的结构真值已经被证明不自洽；但上一个已认证 pointer 仍继续
服务，不因候选版本损坏而整链停摆。

commissioning 在 `src/polyarb/control_plane/production_commissioning_disposable.py:3247` 先发布一个良好
shadow generation，再对一次性数据库中的候选 generation 注入冻结计数冲突。证据
要求同时成立：候选 manifest 没有进入权威表、没有 downstream admission、没有 retry
circuit、critical alert 可见，且 prior pointer 没有移动。

### 为什么 `SELECT ... FOR UPDATE` 仍然防不住旧 generation 回拨指针？

行锁只能保证两个发布事务不同时写，不能证明后一个事务的 generation 比前一个
新。旧 certifier 如果晚到，会先等新事务释放锁，然后读到新 current，最后“合法地”把它
更新回旧 generation。这是 serialization，不是 lineage compare-and-swap。

Quote 候选现在在准入事务中把当时 current 冻结到 certifier `input_identity`，核心编解码在
`src/polyarb/control_plane/postgres.py:106`。发布只允许 `current == frozen predecessor`；重放同一
generation 时又会保留原冻结 identity，避免幂等重放被当成新候选。Structure 的显式
shadow 发布同样必须传入读取到的预期前任（`postgres.py:5247`）；Opportunity lease 必须仍绑定
current Quote。

冲突并不进入 backoff/circuit：旧工作无论重试多少次都不应再成为 current。Quote 和
Opportunity worker 在 `quote_worker.py:655` 与 `opportunity_worker.py:153` 把它终止为
superseded，同时写 warning incident 和 `publication.superseded` terminal fact；当前 generation 继续服务。
commissioning 对三个指针节点逐一注入旧发布竞争（`production_commissioning_disposable.py:3551`），
要求最终只有一个 current，且它精确匹配新 generation 的 success fact。

### R2 读超时为什么不能用“给整个 commissioning 加 120 秒 timeout”来验证？

整窗 timeout 只能证明 runner 在某个时刻被杀，无法回答是哪个 artifact、哪个 runtime
stage、哪个 lease epoch 失败，也无法证明失败前没有发布半成品。更糟的是，它会把数据库迁移、
容器调度抖动和对象存储故障混成同一种“超时”，再次形成单点停摆。

`R2ReadTimeoutCommissioningAdapter`（`production_commissioning_disposable.py:1760`）因此不
sleep：它对绑定当前 `input_identity` 的一个 immutable key 注入一次 botocore
`ReadTimeoutError`。第一次 GET 必须通过中央 `finish_retryable_with_incident()` 留下
stage-exact failure fact，且业务 postcondition 仍不存在；retry due-at 完全来自共享
`RuntimeRetryPolicy`，adapter 不复制秒数公式。

normal-turn fixture 也支持停在精确读取 stage（`prepare_normal_turn()` 与
`_record_progress()`，同文件约 4174/4239 行）。否则 fixture 若已提前推进到 commit stage，
incident 写 read、runtime event 写 commit，就会形成逻辑矛盾的假证据。恢复 epoch 才继续后续
stage，第二次 GET 校验原 SHA-256，然后提交唯一业务事实并关闭 incident/circuit。6 个实际读
节点由 `run_r2_read_timeout_commissioning()` 在 6 个独立迁移数据库中逐一执行；runner 本身仍
没有任意外层超时。

### PUT 报 timeout 时，为什么不能断言“对象一定没写进去”？

因为超时可能发生在 R2 已提交对象、但客户端尚未收到响应之后。若 worker 此时直接把数据库
receipt 写成成功，就可能为错误内容背书；若无条件重新 PUT，又会把 committed-but-unacknowledged
边界隐藏掉，并在非确定 key 设计中制造重复对象。

`R2WriteTimeoutCommissioningAdapter` 选择更严格的攻击：第一次 PUT 先按 immutable key 和
SHA-256 落对象，再抛 provider-shaped `ReadTimeoutError`。失败 epoch 只能写 retryable fact 和
incident，业务 receipt 必须保持不存在。到共享 policy due-at 后，replacement 不再次 PUT，先用
HEAD 核对 metadata SHA-256；完全一致才继续原节点的 fenced database commit。

验证同时要求 `PUT count == 1`、`HEAD count == 2`（清理检查和恢复检查）、失败/成功 attempt 的
job input identity 相同、最终只有一个业务成功事实。7 个写节点分别在独立迁移数据库运行；因此
“对象已写但响应丢失”不会升级成整链重启，也不会被伪装成对象一定缺失。

### Quote 指针已认证，为什么 Opportunity 仍必须再做新鲜度门控？

认证只证明 Quote generation 完整、不可篡改，并不证明它此刻仍有交易意义。若
`opportunity-certify` 只校验 `quote:current` 的 identity，301 秒前的完整 Quote 仍会被投影成一个
全新的 Opportunity，后续读端看见的将是“刚发布但输入已经过期”的假新鲜数据。

现在输入门直接复用 `quote_timing.py` 的唯一 300 秒业务 SLA，并检查 generation 中**最老**的
batch，而不是用最新 batch 掩盖早期批次。拒绝发生在任何 R2 GET 和 Opportunity 业务写入之前；
运行时事实写 `freshness.quote + blocked`，告警/资格事实写同一原因的 `breaking`，避免一个故障在
两个真相面出现互相矛盾的影响等级。

恢复也不能把 incident 绑死在旧 job key：Quote generation 由 Structure identity 决定，真正的
刷新会产生新的 Structure/Quote lineage。`freshness:quote` 因此是跨 generation 的稳定 dedupe
identity；新鲜 lineage 成功投影后，由其成功 lease 关闭同一个 incident。commissioning 会攻击
完整链条：陈旧指针 → 无 R2/无 Opportunity 写入 → blocking incident → 新 Structure/Quote 指针 →
新 Opportunity 唯一发布 → incident recovered。

### CLOB 少返一条 book，为什么不能当成 `missing-book` 正常发布？

`missing-book` 可以表达一个有身份的 book 没有可执行 ask，但 provider 根本没有返回
某个请求 token 是另一件事：它可能是分页、代理或上游部分响应。如果把两者合并，
Quote batch 会把“未观测到”伪装成“已观测且不可执行”，然后 certifier 继续发布不完整
generation。

现在 `TransactionalQuoteBatchWorker` 在任何 R2 PUT 前比较去重后的 requested token 数与
provider 实际响应数。少一条就抛出只含计数、不含 token/body 的
`IncompleteQuoteBatchCoverageError`，通过共享 durable retry/circuit/incident 链处理；失败
epoch 不得留下 artifact 或 Quote receipt。

commissioning 也修正了边界归属：旧 `perception_chaos.py` 的 `candidate` 是 SQLite 旧链，
不是 PostgreSQL 新 DAG 的 `quote-batch`，且没有 commissioning `config_id` 绑定。所以不能把
旧 canary 证据转个字段就冒充新链证明。`clob-missing-leg` 改为 exact-image disposable
攻击：真实 worker + 真实迁移 PostgreSQL 先拒绝 0/1 覆盖，再按中央 policy due-at 用
同一 immutable batch 完成 1/1 恢复，最终只有一个 receipt。

### CLOB 429 为什么要转成新错误类，不直接保存 SDK exception？

`PolyApiException` 同时表示 429、其它 HTTP 错误和部分 transport wrapper；仅保存这个类名，
runtime 无法判断应该等待 provider 还是调查请求缺陷。反过来保存 SDK `error_msg` 也不安全，
因为它可能包含 provider body、URL 或 token identity。

`ClobReaderClient` 现在只在 `status_code == 429` 时转换成 `ClobRateLimitError`。该错误只暴露
常量 429 和固定文本，原 SDK exception 只作为 in-memory cause，不进入 durable detail。
`quote-batch` 仍通过同一 `finish_retryable_with_incident()` 链写失败，backoff 完全来自
`RuntimeRetryPolicy`，没有新的 429 秒数开关。

commissioning 在真实 worker 边界注入一次该 typed error：失败 epoch 无 R2 对象、无 Quote
receipt；清理后由 policy-due replacement 重读同一 immutable batch，完成 1/1 覆盖并只写一个
receipt。这比把旧 SQLite `candidate` 429 证据别名成新 `quote-batch` 证据更严格。
