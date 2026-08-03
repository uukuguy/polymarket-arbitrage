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

哈希版本与分类合同是两根不同的保险丝。当前分类合同是 `structure-drift-classifier-v2`：v1 active
progress 会留下 `drift-classifier-contract-superseded`，v2 从 cursor zero 重建；相同 frozen identity 的
v2 stale terminal 不重试。这样既能从旧合同自动恢复，又不会把 deterministic data conflict 变成无限重试噪声。

complete projection 不是“从 generation 反推 source”，而是 market staging 与 event-member anti-join 的完整
union。global conflict 先于 duplicate、quarantine 和 group-ineligible 判定；否则一个跨 event market 可能被
局部 inactive sibling 错误解释。`fresh-group-ineligible` 只允许一个很窄的证明：legacy member 本身仍 active、
exact source identity 相同，但它所属 standard group 因另一个 non-tradable member 成为
`complete-unsupported`。任何不满足这些谓词的 removal 都进入 diagnostic，不会被兜底类吞掉。

fresh projection 的 event member 来源现在是一张独立 sealed sidecar，而不是运行时展开 parent event JSON。
采集链是：natural event page → source receipt → 最多 500 行/块的 member 派生 → member receipt → indexed
anti-join。projection commitment 除了 11-field member count/root，还绑定 member receipt digest；即使两个 window
碰巧得到相同 count/root，receipt/source 身份不同也不能互认。
member 派生也由独立 child 执行：500 rows/CAS、100 chunks/45 秒、parent 75 秒。1,200 members 因而在一次
admission 内自然形成 500/500/200，而不是被三次普通 cadence 隔开。若 slice 尚未 seal，resident loop 约 100ms
后重新 admission，并重新执行 Quote priority 检查。
这里还有一条容易漏掉的因果顺序：event-market backfill 必须先 terminal，member child 才能 admission。否则
同一个 market 同时属于两个 event 的事实尚未落进 relation sidecar，提前 seal 会把真实冲突永久认证成无冲突。
因此 `waiting-event-market-backfill` 是可自动恢复的前置等待，不是长期降级。

global-conflict 也不能只靠“一张冻结 summary 表”获得真实性。seal 流程为
`members -> conflicts -> merkle -> proofs -> complete`：每个 `(window,event)` summary 形成 tagged leaf，receipt
绑定 Merkle root，每行保存认证路径。projection 只取本页候选行及其 proof，O(log N) 验到 receipt，不需要为了
认证一行而全表重算。表级 freeze guard 防普通写入，逐行 proof 防绕过 guard 后的删改、插入和跨窗替换。
Merkle 构建还有一个 chunk 边界陷阱：odd 只表示“当前块是奇数”，不表示“整层已经结束”。非终端孤儿若当场
self-duplicate，会让下一块首个 child 生成重复 parent，且 root 随 limit 改变。实现把孤儿作为 checkpoint-digest
覆盖的 pending child 持久化；下一 CAS 配对。整层终端 odd 才 self-duplicate，所以 limit=1 也能每次消费一行并
稳定推进。

## 代码地图

- `src/polyarb/perception/structure_drift.py`：raw projector、成员分类和 tagged reconstruction roots。
- `src/polyarb/storage/sqlite_store.py:7026`：complete fresh projection 的 bounded CAS。
- `src/polyarb/storage/sqlite_store.py:7244`：v1→v2 contract supersession 与 same-contract no-retry 初始化。
- `src/polyarb/storage/sqlite_store.py:8213`：classification、diagnostics 和 generation mirror 同 CAS 推进。
- `src/polyarb/storage/sqlite_store.py:8640`：只读 current/terminal receipt verifier。
- `src/polyarb/perception/structure_drift.py:637`：diagnostic first-match priority；同文件 `:926` 是互斥分类器。
- `src/polyarb/snapshot/cli.py:346`：scheduler-only cooperative child slice。
- `src/polyarb/daemon/scheduler.py:377`：隔离 child 与 TERM/KILL 边界；同文件 `:1183` 是 Quote 双检 admission。
- `src/polyarb/http/health.py:623`：disabled/incomplete/sealed/stale 的健康投影。
- `scripts/polywatch/healthz_watcher.py:503`：terminal incident identity；`:567` 验证同 identity recovery。
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
- terminal failure receipt 不是“失败日志”：它把 comparison identity、class/diagnostic roots、sample digest
  和 checkpoint 原子封存。status 只有重验全部字段才可展示 sample；Polywatch 用 comparison/contract/reason/root
  组成 incident identity，首次 alert、相同 identity 去重/提醒，sealed 同 identity 才 recovery。
- generation read 与 Quote 是两步 rollout：先 Quote disabled 切 read、验证 natural publication 和 legacy rollback；
  再单独 enable Quote。比较 PASS 不等于自动获得任何 mutation 权限。

## 自检题

1. generation member root 相等，但 `unclassified=1`，能 seal 吗？为什么？
2. child 第 37 个 chunk 后 SQLite busy，前 36 个 chunk 是否应回滚？
3. Quote 在 slice 已启动后变 due，为什么不杀 child，而是在下一次 admission 抢占？
4. `authorization_mode=drift-safe-sealed` 是否自动授权修改 production read mode？
5. 为什么 `projection_member_root != generation_member_root` 在 v2 中可能正确，而 mirror 不相等一定失败？
6. 两个 window 的 11-field count/root 相同，为什么 member receipt digest 不同仍必须拒绝复用 commitment？
7. 为什么只冻结 conflict summary 表仍不足以证明单个候选 event 的 conflict 值？
8. 一个 Merkle chunk 读到奇数个 child 时，什么证据允许最后一个 child self-duplicate？
9. v2 stale terminal 后 scheduler 为什么不应继续重试同一 comparison ID？什么变化才允许新运行？
10. 跨 event conflict 与 local group-ineligible 同时成立时，哪一个先判，错序会造成什么授权风险？
11. 为什么 generation read PASS 后仍不能在同一个动作里打开 Quote？

## FAQ 增量

### 为什么 health 在 drift disabled 时是 pass？

因为 feature 默认关闭本身不是数据面故障。启用后 incomplete 是 warn；stale、receipt invalid、identity invalid
或 checkpoint 超 SLA 才是 fail。legacy serving plane 的健康不会因未启用验收工具而被伪造为故障。

### 性能门为什么要跑十几分钟？

`make classifier-v2-deploy-perf` 在计时外 seed 120k markets / 5k events / 24 members/group，再让 old v1 与
classifier-v2 各自 warm 并完整跑三次。2026-08-03 实测 old v1 `152.796017s`、v2 `45.139646s`，
`3.385x`；最坏 100-chunk slice `7.437705s < 45s`，projection 最坏 17 SELECT/call。它是 release SHA
资格，不是每两分钟运行的生产健康探针。

### 为什么 `waiting-natural-window` 时 drift 也必须让路？

调度顺序是 member sidecar → drift → snapshot。历史 window 没有 natural source receipt 时，member sidecar
不能合法补造；如果只有 member 分支返回、drift 仍占住第二道门，classifier 扫完 source 后会在
`fresh-projection-members` 永久缺少 sealed member authority。每次重试都只产生 0-row `identity-stale`，而真正能
生成新 receipt 的 snapshot 永远排不到。

因此 `src/polyarb/daemon/scheduler.py:1212` 对经过认证的历史迁移态做精确让路：写入 defer breadcrumb，然后
继续正常 snapshot。它不把旧 window 变成可信、不删除已算的 v2 progress，也不切 pointer；等 natural
publication 真正切换 pointer 时，`publish_structure_generation` 在同一事务把旧 active comparison 终结为
`drift-current-generation-superseded`，但不伪造 authorization receipt。下一 tick 才从新 identity 建立唯一 v2。
若状态不完整、未认证或读取失败，仍然阻断而不是猜测。这是“为恢复链让路”，不是把安全门降级。
