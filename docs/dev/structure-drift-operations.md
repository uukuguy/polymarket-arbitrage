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

## 故障处理

### Fresh projection 的 sealed sidecar 前置门

fresh projection 不再展开 `structure_sync_event_staging.payload_json.markets`。自然 event page 先生成
`structure-event-source-v1` receipt；后台以每块最多 500 个 member 派生
`structure_sync_event_member_staging`，完成后生成 digest-sealed member receipt。projection 只有在 source receipt、
member progress、member receipt 和 publication 全部属于同一 window 且验证通过时才读 candidate；缺失、混窗、
混源或篡改统一 fail closed，不返回 count/root/sample，也不修改 generation 表。

event-only anti-join 使用覆盖索引和复合 keyset
`(market_sort_key,event_id,event_ordinal,member_ordinal)`；首块和续块是两条独立 SQL，没有 nullable-OR、
`json_each`、逐 member SELECT 或临时排序。历史 window 如果完全没有 source metadata/progress/receipt，会显示
`waiting-natural-window/pass`；这是经过“无 source evidence”检查后的迁移态，不能从旧 raw payload 合成 receipt。
只要 metadata/progress 已存在而 receipt 缺失或无效，就仍是 fail。receipt invalid 时保留 legacy serving plane，
停止验收并等待新 window。

query budget 必须按两层读：direct reader 每次固定最多 7 条 publication/source/member authority SELECT，加最多
10 条 bulk candidate/evidence SELECT，总计最多 17；commitment 已验证 receipt 后内部复用该 authority，当前
production-shaped gate 实测 12 条 SELECT/call。这个常数不会随页内 1/17/500 个 member 增长。不要把旧的
“≤10”理解成含 receipt gate 的总数；
它只描述 candidate/evidence 层。任何逐 member 增长都属于回归。

relation/sidecar bulk probe 最多读取 `2 * candidate_count + 1` 条；出现 sentinel 或任一 key 超过两条时，切换
到 indexed per-key `LIMIT 2` witness 查询。最终证据每 key 最多两条，既能判断唯一/重复/多 parent，也不会把
高重复数据完整搬进 Python。

- `structure-drift-writer-busy`：确认 Quote/其他 writer 所有权；不要并发手动推进。
- `structure-drift-child-failed`：看 child signal/timeout 分类；SIGKILL 先按 possible cgroup OOM 调查。
- `unclassified>0` / `overlap-conflict>0`：数据证据失败，不是性能重试问题。
- checkpoint 超 SLA：确认 scheduler feature flag、defer receipt 和 Quote 持续 due/active 事实。
- `drift-hash-algorithm-superseded`：确认已有新的 v2 progress 从 cursor zero 推进；不要把旧 v1 stale
  改回 active，也不要因此切换或暂停 legacy serving plane。
- `structure-event-member-receipt-invalid`：核对同窗 source receipt、member checkpoint 和 sealed digest；不要
  从历史 event JSON 补 receipt，也不要绕过 sidecar gate。
