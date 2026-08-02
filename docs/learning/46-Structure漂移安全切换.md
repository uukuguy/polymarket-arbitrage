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

## 代码地图

- `src/polyarb/perception/structure_drift.py`：raw projector、成员分类和 tagged reconstruction roots。
- `src/polyarb/storage/sqlite_store.py:4760`：只读 current receipt verifier；同文件中的有界 phase state machine 写进度。
- `src/polyarb/snapshot/cli.py:346`：scheduler-only cooperative child slice。
- `src/polyarb/daemon/scheduler.py:210`：严格 child JSON parser 与 TERM/KILL 边界。
- `src/polyarb/daemon/scheduler.py:966`：Quote 双检、共享锁和 drift-first admission。
- `src/polyarb/http/health.py:616`：disabled/incomplete/sealed/stale 的三态健康投影。
- `structure_drift_attempts`：最近 100 次 child 的身份、进度、结果和安全 stderr 摘要。

## 三套不能混为一谈的哈希

1. `projection_member_root`：只从冻结 raw event/market 投影出的 eligible member 串流。
2. `generation_member_root`：只从 generation 表读取的 actual eligible member 串流。
3. class roots：用 `shared/add/remove-reason` tagged commitments 重构 temporal 差异。

前两者相等证明“新数据忠于同窗 raw”；第三者证明“相对旧数据的完整对称差可解释”。拿同一个 root
同时填两个字段会变成自证，因此代码保留独立 SHA state/count。

## 设计取舍

- 不放宽 exact comparator：它仍用于同窗、同身份的逐字比较。
- 不用 age/count tolerance：6,048 的净差可能隐藏同量 addition/removal，必须逐成员分类。
- 不在 event loop 跑 slice：SQLite/CPU 异常不能阻塞 Quote scheduler。
- 不让 operator CLI 推进：`make structure-generation-drift-compare` 永远只读；写入只由 scheduler child 所有。
- writer busy 不增加 Structure failure counter：已提交 chunk 保留，下次 admission 从 cursor 恢复。

## 自检题

1. generation member root 相等，但 `unclassified=1`，能 seal 吗？为什么？
2. child 第 37 个 chunk 后 SQLite busy，前 36 个 chunk 是否应回滚？
3. Quote 在 slice 已启动后变 due，为什么不杀 child，而是在下一次 admission 抢占？
4. `authorization_mode=drift-safe-sealed` 是否自动授权修改 production read mode？

## FAQ 增量

### 为什么 health 在 drift disabled 时是 pass？

因为 feature 默认关闭本身不是数据面故障。启用后 incomplete 是 warn；stale、receipt invalid、identity invalid
或 checkpoint 超 SLA 才是 fail。legacy serving plane 的健康不会因未启用验收工具而被伪造为故障。
