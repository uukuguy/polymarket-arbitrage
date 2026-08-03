# M1 Structure drift-safe 运行与验收

本流程只生成和验证授权证据，不部署、不切 `current_structure_generation`、不修改 read mode。

## 启用前只读检查

```bash
make structure-generation-status
make structure-generation-compare
make structure-generation-drift-compare
```

旧 exact compare 对陈旧 legacy 失败是预期输入；drift compare 在未启动时必须 nonzero，不能解释为零风险。

## scheduler 配置

默认 `structure_generation_drift_compare_enabled=false`。受控启用时保持：

- `structure_generation_drift_max_rows=500`
- `structure_generation_drift_max_chunks_per_tick=100`
- `structure_generation_drift_slice_s=45`

`max_rows=500` 是全局上限；`source-events` phase 先取最多 100 events，再选择累计 member workload ≤500、
payload ≤512 KiB 的稳定非空前缀。正常单 event 至少推进 1 条；单 event 自身超过任一绝对上限则立即以
`source-event-workload-oversized` fail closed、0 progress，并由 attempt health 报警，不允许硬跑到 timeout。
其它 phase 仍可使用 500 行，slice 总边界仍是 100 chunks / 45 秒。

scheduler 在 `_tick_lock` 内先检查 Quote，再取得共享 producer lock 并复查 Quote，之后才 spawn child。不要直接运行
隐藏的 `structure-generation-drift-advance`；它是 parent-owned subprocess protocol，不是 operator surface。
event-member 派生同样运行在隐藏的隔离 child 中：每次 CAS 最多 500 members，一次 admission 最多 100 chunks
或 45 秒，parent 75 秒超时。未 seal 的正常 checkpoint 会令 resident loop 在约 100ms 后重新 admission；不会等
普通采集 cadence。重新 admission 会完整重复 Quote 检查，所以 Quote 可以在两个 slice 之间抢占。

## 运行中判读

当前比较算法是 `row-chain-sha256-v2`。只读状态里的
`hash_algorithm=row-chain-sha256-v2` 才能形成新的 drift-safe 授权。升级时若存在仍在推进的
`serializable-sha256-v1` progress，初始化会在一个事务内把它标成
`stale / drift-hash-algorithm-superseded`，再从 v2 cursor zero 新建比较；这是证据算法换代，
不是 legacy 数据面降级，也不是让 operator 手工续跑旧 cursor 的信号。历史 sealed v1 receipt 仍可查询，
但不能授权 v2 gate。`authorization_mode=exact` 走独立的旧 exact receipt 验证，不依赖 drift hash 版本。

分类合同独立版本化为 `structure-drift-classifier-v2`。当前 frozen identity 若只有 v1 active progress，
初始化会在同一事务把旧行终结为 `drift-classifier-contract-superseded`，再从 v2 cursor zero 开始；
历史 sealed/stale v1 只保留审计价值。同一个 identity + v2 合同一旦得到 stale terminal receipt，
不会反复重跑；必须等待 source/pointer identity 变化或未来合同升级。这是 same-contract no-retry，
不是让 operator 删除 receipt 后重试。

v2 complete projection 是 market staging 与 event-member sidecar anti-join 的有序 union。global relation
conflict 优先于 duplicate、quarantine 和本地 group-ineligible；只有无全局冲突且 source group truth 为
`complete-unsupported / standard-neg-risk-has-non-tradable-members` 时，仍活跃的 legacy sibling 才能进入
`fresh-group-ineligible`。未唯一归类的行写入 diagnostic count/root 与每 code 最多三条 sample。stale
finalization 在一个事务里同时写 progress 和 immutable terminal receipt；status 重验 receipt digest、member
receipt identity、diagnostic root/sample digest 后才暴露诊断。

v2 generation 扫描同时写两类承诺：generation-domain audit root 进入 receipt；同一 canonical row 还写入
projection/source-domain comparison mirror，只用于严格等值。状态验证会交叉核对 audit root、mirror
root/count、progress 和 receipt digest，任一缺失或替换都 fail closed。启动时会幂等补齐 120k member
scan 覆盖索引和 comparison receipt 列；启动失败必须先修复并重新运行初始化，不能绕过 schema gate。

strict health 的 `snapshot:structure_generation_drift`：

- `disabled/pass`：功能关闭，legacy 数据面不受影响。
- `none/warn`：有界比较尚在推进；检查 phase、checkpoint age 和 class counts。
- `drift-safe-sealed/pass`：当前 full identity 的 append-only receipt 已认证。
- `stale/fail` 或 receipt/identity invalid：停止验收；保存证据，等待新的 Structure generation，不得绕过。

writer busy、child timeout/cancel/SIGKILL 会释放 producer lock。已完成 chunk 不回滚；下一次 scheduler admission
从持久 cursor 恢复。Quote due 在下一次 admission 优先，不在 slice 中间强杀已持锁 child。

每次真正 spawn 前，parent 先写 `structure_drift_attempts` running ownership；写不下账本就不启动 child。parent
对 checkpoint/success/defer/timeout/SIGKILL/cancel/invalid JSON 都必须终结该行，保存 full identity、progress ID、
chunks/rows/elapsed、stderr bytes/digest 和最后一条白名单 marker。启动时把遗留 running 标为
`parent-restarted-orphan`；只保留最近 100 条 terminal evidence，且绝不写入 `snapshot_attempts` 或 adaptive schedule。
strict health 引用最新 drift attempt：failed 或 running 超 90 秒为 fail，deferred 保持可诊断 warn。
timeout/SIGKILL 时 parent 只采信 child 最后一条 post-CAS 白名单 marker；例如 marker 的 `chunks=1 rows=500`
表示这些行已经提交，attempt ledger 必须保留该值，即使 child 没有返回最终 stdout JSON。

## 实际验收门

```bash
make structure-generation-drift-compare
make health-local
```

只有 `authorized=true` 且 `authorization_mode=exact|drift-safe-sealed` 才通过 preflight。随后仍需单独的部署授权、
exact release 验证、read-mode 切换计划和回滚证据；本 gate 自身不完成这些 mutation。

上线严格分两步，不能把比较通过与 Quote 恢复合并：

1. 在 exact approved SHA 上部署 schema/worker，保持 legacy serving 与 Quote disabled；等待 natural window、
   member receipt 和 classifier-v2 terminal evidence。只有 sealed PASS 后，才单独把
   `structure_generation_read_mode` 切到 generation，并验证 pointer/read health 与 natural publication。
2. generation read 稳定且 legacy rollback 演练证据完整后，才单独启用 Quote；再复验 Quote freshness、
   candidate lifecycle 与 Polywatch。任一步失败都保持或恢复 Quote disabled。

长性能资格使用 `make classifier-v2-deploy-perf`。它在计时外生成 120,000 markets、5,000 events、
24 members/group、global conflict 和 event-only candidate；old v1 与 v2 都 warm 后完整运行三次取 median。
2026-08-03 证据：old v1 `152.796017s`，classifier-v2 `45.139646s`，`3.385x`；最坏 100-chunk
slice `7.437705s`，projection `17 SELECT/call`。该命令约 14 分钟，是 deploy gate，不是健康探针。

## 故障处理

### Fresh projection 的 sealed sidecar 前置门

fresh projection 不再展开 `structure_sync_event_staging.payload_json.markets`。自然 event page 先生成
`structure-event-source-v1` receipt；后台以每块最多 500 个 member 派生
`structure_sync_event_member_staging`，完成后生成 digest-sealed member receipt。projection 只有在 source receipt、
member progress、member receipt 和 publication 全部属于同一 window 且验证通过时才读 candidate；缺失、混窗、
混源或篡改统一 fail closed，不返回 count/root/sample，也不修改 generation 表。

member child 只有在同窗 event-market backfill 已认证为 terminal 后才会 admission；此前状态是
`waiting-event-market-backfill`，scheduler 会在 Quote 优先检查后自动重试，不会把缺关系的 member 提前 seal。
member seal 还会按 `event_id` 认证持久化的 global-conflict summary：每次最多推进 500 行，并经过
`members -> conflicts -> merkle -> proofs -> complete` 有界阶段。count、chain root、Merkle root 都进入 member
receipt digest；每个摘要行都有到 receipt root 的持久 proof。projection 只按候选 `(window_id,event_id)` 主键
读取摘要和 proof 并逐行验签，不扫描全部 relation siblings；摘要/proof 的增删改、跨窗替换或 receipt root 篡改
都 fail closed。
Merkle 层的非终端奇数块不会把孤立 child 提前 self-duplicate；它把 child index/hash 写入同一个 authenticated
checkpoint，重启后与下一 child 配对。只有确认到达该层末尾时，最后一个 odd child 才 self-duplicate。因而
limit=1/17/500 生成相同 root/proofs，且每次新消费行数不超过调用 limit。
event-member scheduler child 没有专用 attempt ledger；运维跟踪依赖 defer receipt、共享 scheduler failure counter
和 `RECOVERING` health。专用 `structure_drift_attempts` 只属于后续 classifier-drift child，不要把两条执行链混为一谈。

event-only anti-join 使用覆盖索引和复合 keyset
`(market_sort_key,event_id,event_ordinal,member_ordinal)`；首块和续块是两条独立 SQL，没有 nullable-OR、
`json_each`、逐 member SELECT 或临时排序。历史 window 如果完全没有 source metadata/progress/receipt，会显示
`waiting-natural-window/pass`；这是经过“无 source evidence”检查后的迁移态，不能从旧 raw payload 合成 receipt。
只要 metadata/progress 已存在而 receipt 缺失或无效，就仍是 fail。receipt invalid 时保留 legacy serving plane，
停止验收并等待新 window。

query budget 必须按两层读：receipt/source/group-truth authority 与 bulk candidate/evidence。完整 120k
production-shaped gate 的最坏 event-only terminal page 实测 17 条 SELECT/call；普通 market page 更少。
这个常数不会随页内 1/17/500 个 member 增长。不要把旧的
“≤10”理解成含 receipt gate 的总数；
它只描述 candidate/evidence 层。任何逐 member 增长都属于回归。

relation/sidecar bulk probe 最多读取 `2 * candidate_count + 1` 条；出现 sentinel 或任一 key 超过两条时，切换
到 indexed per-key `LIMIT 2` witness 查询。最终证据每 key 最多两条，既能判断唯一/重复/多 parent，也不会把
高重复数据完整搬进 Python。

- `structure-drift-writer-busy`：确认 Quote/其他 writer 所有权；不要并发手动推进。
- `structure-drift-child-failed`：看 child signal/timeout 分类；SIGKILL 先按 possible cgroup OOM 调查。
- `unclassified>0` / `overlap-conflict>0`：数据证据失败，不是性能重试问题。
- `fresh-group-ineligible>0`：只有 receipt 认证且 reconstruction roots 闭合时才是可授权 removal；
  它不是吞掉任意 group failure 的兜底类。
- `drift-classifier-contract-superseded`：确认新 v2 comparison ID 从 cursor zero 推进；同合同 stale
  terminal 不重试，等待新 natural identity，不删除旧 terminal receipt。
- checkpoint 超 SLA：确认 scheduler feature flag、defer receipt 和 Quote 持续 due/active 事实。
- `drift-hash-algorithm-superseded`：确认已有新的 v2 progress 从 cursor zero 推进；不要把旧 v1 stale
  改回 active，也不要因此切换或暂停 legacy serving plane。
- `structure-event-member-receipt-invalid`：核对同窗 source receipt、member checkpoint 和 sealed digest；不要
  从历史 event JSON 补 receipt，也不要绕过 sidecar gate。
