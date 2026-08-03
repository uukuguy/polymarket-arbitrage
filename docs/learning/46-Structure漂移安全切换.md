# Structure 漂移安全切换

## 30 秒心智模型

旧版 exact comparator 只能回答“两个不同时刻的快照是否逐字相同”。drift-safe gate 回答的是另一个问题：
“generation 是否逐字等于它自己的冻结 raw window 投影，以及相对旧 legacy 的每一项变化是否都有唯一原因”。
两条证明都通过，才生成 append-only receipt；时间差和净数量差从来不是容差。

生产推进不是 HTTP 进程中的长循环。scheduler 先让 Quote 优先，再持有共享 producer lock，启动隔离 child；
child 每行块最多 500、每 slice 最多 100 块或 45 秒，每块独立 CAS 提交。child 卡死时 parent 在 75 秒
hard timeout 后 TERM，15 秒仍不退出则 KILL。
parent 在 spawn 前先取得专用 attempt ownership；任何 child 都必须有 terminal evidence。重启会回收 orphan，
而该账本独立于普通 Structure snapshot attempts，不能影响 adaptive cadence/timeout。
全局 chunk 上限仍是 500，但 `source-events` 先看最多 100 events，再按累计 embedded/catalog member workload
≤500 和 payload ≤512 KiB 取稳定前缀：一个 event 会展开 embedded markets 和 group-truth hashing，普通“行数”
会严重低估工作量。正常 event 至少取 1 条；单 event 自身超限时必须 fail closed 并报警，不能用“防饿死”
为由执行一个可能超过 75 秒的 chunk。prefix 只改变 checkpoint 频率，不改变 cursor、串流顺序或最终 digest。

当前持久算法是 `row-chain-sha256-v2`：每行先做 canonical JSON，再用 C-backed SHA-256 leaf/chain
推进固定 32-byte state。旧 active v1 不续跑，而是留下
`drift-hash-algorithm-superseded` terminal evidence，并从 v2 cursor zero 重启。这个动作只换证明算法，
不会暂停或切换 legacy serving plane；exact authorization 仍是独立路径。

fresh projection 的 event member 来源现在是一张独立 sealed sidecar，而不是运行时展开 parent event JSON。
采集链是：natural event page → source receipt → 最多 500 行/块的 member 派生 → member receipt → indexed
anti-join。projection commitment 除了 11-field member count/root，还绑定 member receipt digest；即使两个 window
碰巧得到相同 count/root，receipt/source 身份不同也不能互认。
member 派生也由独立 child 执行：500 rows/CAS、100 chunks/45 秒、parent 75 秒。1,200 members 因而在一次
admission 内自然形成 500/500/200，而不是被三次普通 cadence 隔开。若 slice 尚未 seal，resident loop 约 100ms
后重新 admission，并重新执行 Quote priority 检查。

## 代码地图

- `src/polyarb/perception/structure_drift.py`：raw projector、成员分类和 tagged reconstruction roots。
- `src/polyarb/storage/sqlite_store.py:4760`：只读 current receipt verifier；同文件中的有界 phase state machine 写进度。
- `src/polyarb/snapshot/cli.py:346`：scheduler-only cooperative child slice。
- `src/polyarb/daemon/scheduler.py:210`：严格 child JSON parser 与 TERM/KILL 边界。
- `src/polyarb/daemon/scheduler.py:966`：Quote 双检、共享锁和 drift-first admission。
- `src/polyarb/http/health.py:616`：disabled/incomplete/sealed/stale 的三态健康投影。
- `structure_drift_attempts`：最近 100 次 child 的身份、进度、结果和安全 stderr 摘要。
- `structure_sync_event_member_staging`：逐 ordinal 的 immutable member envelope；projection 不读取 parent
  `markets` 数组。

## 审计 root 和比较 mirror 不能混为一谈

1. `projection_member_root`：只从冻结 raw event/market 投影出的 eligible member 串流。
2. `generation_member_root`：只从 generation 表读取的 actual eligible member 串流，使用独立 generation domain。
3. `generation_projection_member_comparison_root`：actual rows 的同内容镜像，但使用 projection domain，
   只和第 1 项做严格相等比较。
4. group truth 也有同样的 source audit / generation audit / source-domain comparison mirror 三件套。
5. class roots：用 `shared/add/remove-reason` tagged commitments 重构 temporal 差异。

不同 domain 的 root 即使 rows 完全相同也必然不同，所以不能直接比较 projection audit 和 generation audit。
严格相等由同 domain 的 audit/mirror 证明；generation audit 仍独立进入 receipt，防止 mirror 冒充 actual
审计。mirror state/count 每个 chunk 都持久化，receipt digest 和 status verifier 再与 progress 交叉核对。
class roots 则证明“相对旧数据的完整对称差可解释”。

## 设计取舍

- 不放宽 exact comparator：它仍用于同窗、同身份的逐字比较。
- 不用 age/count tolerance：6,048 的净差可能隐藏同量 addition/removal，必须逐成员分类。
- 不在 event loop 跑 slice：SQLite/CPU 异常不能阻塞 Quote scheduler。
- 不让 operator CLI 推进：`make structure-generation-drift-compare` 永远只读；写入只由 scheduler child 所有。
- writer busy 不增加 Structure failure counter：已提交 chunk 保留，下次 admission 从 cursor 恢复。
- timeout ledger 以最后一条 post-CAS marker 为准：marker 已写出就代表该 chunk committed，不能记录成 0 行。
- 不续跑 v1 cursor：哈希状态不能跨算法解释；保留 stale evidence，再从 v2 cursor zero 重算才可审计。
- 不扩展旧 relation table：relation 无法表达 null/padded identity、重复 ordinal、member payload hash 和精确恢复
  cursor；这些正是 fail-closed 诊断与 receipt seal 所需证据。
- 不为历史 window 合成 sidecar：没有 natural source receipt 就没有可认证的派生起点。
- “完全无 source evidence”的旧 window 是 `waiting-natural-window/pass` 迁移态；“已有 evidence 却缺/坏
  receipt”是 fail。二者不能用同一个 unavailable 告警语义混淆。

## 自检题

1. generation member root 相等，但 `unclassified=1`，能 seal 吗？为什么？
2. child 第 37 个 chunk 后 SQLite busy，前 36 个 chunk 是否应回滚？
3. Quote 在 slice 已启动后变 due，为什么不杀 child，而是在下一次 admission 抢占？
4. `authorization_mode=drift-safe-sealed` 是否自动授权修改 production read mode？
5. 为什么 `projection_member_root != generation_member_root` 在 v2 中可能正确，而 mirror 不相等一定失败？
6. 两个 window 的 11-field count/root 相同，为什么 member receipt digest 不同仍必须拒绝复用 commitment？

## FAQ 增量

### 为什么 health 在 drift disabled 时是 pass？

因为 feature 默认关闭本身不是数据面故障。启用后 incomplete 是 warn；stale、receipt invalid、identity invalid
或 checkpoint 超 SLA 才是 fail。legacy serving plane 的健康不会因未启用验收工具而被伪造为故障。
