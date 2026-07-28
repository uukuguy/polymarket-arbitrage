# Checkpointed Full Reconciliation：慢地图只校准，不挡盯盘

## 30 秒心智模型

Candidate Watcher 负责“现在值得盯的完整组”；Full Reconciliation 负责“长时间窗里，
已知市场地图与 Gamma 全量枚举有何差异”。后者每次只采一个页面：

```text
window(open)
  + exact certified baseline + versioned digest（窗口开始事务）
  -> page staging + batch receipt + opaque cursor（一个事务）
  -> ... 重启后继续 cursor
  -> terminal empty page => window(complete)
  -> revisions + per-group diff evidence + 5 counters（一个原子 diff）
  -> window(applied)
```

`open` 窗口绝不修改在线 group authority。它慢、失败或停住时，Candidate 的最后一版
真实 revision/Quote 仍按自己的年龄继续服务或 fail closed。

## 代码地图

| 文件 | 责任 |
|---|---|
| `src/polyarb/perception/reconciliation.py` | 单页 worker、持续 runner、取消收敛 |
| `src/polyarb/perception/store.py` | window/batch/staging checkpoint 与原子 diff |
| `src/polyarb/storage/schemas.py` | checkpoint、baseline、sample、diff evidence 表 |
| `src/polyarb/http/health.py` | 从真实 checkpoint/receipt 读 scoped health |
| `src/polyarb/cli_reconciliation.py` | 单页推进与只读状态 |

关键入口是 `ReconciliationWorker.run_batch()`（`reconciliation.py:53`）、
`publish_reconciliation_batch()`（`store.py:2118`）和
`apply_reconciliation_diff()`（`store.py:2248`）。

## 为什么有两个事务

页面事务把 staging、batch receipt、window 计数和 next cursor 一起提交。进程若在
terminal receipt 后、diff 前死亡，数据库诚实停在 `complete`；重启先 apply 该窗口，
不会重复抓页。diff 事务用 `BEGIN IMMEDIATE` 重读所有 current revisions，全部 group
成功才把窗口改为 `applied`；中间任一写失败，所有新增/变更/关闭一起回滚。

这比把“最后一页 + 全量 diff”塞进一个超长事务更适合 SQLite：checkpoint 已安全落盘，
而昂贵 diff 仍可单独恢复。

## 并发 Discovery 为什么不会被旧地图覆盖

窗口开始事务把当时每个 current certified group 的
`group/event/revision/membership/status` 写入 append-only baseline。apply 不再比较容易
同毫秒或时钟回拨的时间戳：只有 current 仍精确等于 baseline 时，staging 才能 change；
baseline 外后来出现的 online group，以及 baseline 后发生过任何 revision/status 变化的
group，都由 hot online authority 胜出。closure 也只遍历 baseline，且 current 必须仍
精确匹配。

window 同时保存带 domain、version、count 的 canonical ordered baseline SHA-256。
validator 会从 baseline rows 重算；即使攻击者保持行数不变、只把一行换成 post-begin
group，也不能获得 closure authority。旧窗口没有这个证明时只会 fail closed 并新建
窗口，绝不从当前状态猜测回填历史 baseline。

不完整或 unsupported 的 group 也会以 rejected staging 留下证据。只有其
`group_id + event_id` 能绑定 baseline 时才抑制 closure；伪造同 group 的 attacker event
不能替任意旧 event 续命。

跨页再次看见同 group 不会卡住 cursor。每页把所有 observation 写入 append-only
batch samples：相同事实记 `duplicate` 且不改 materialized staging，变化记 `updated`
并采用 latest-wins，首次出现记 `unique`。receipt 分别记录
observed/unique/update/duplicate/rejected，状态读取会重放样本证明最终 staging。
若 opaque cursor 指回当前或任一历史 cursor，窗口原子变为 `failed/cursor-loop`，
绝不 apply；下一轮创建新窗口从头恢复。

apply 还会在同一个写事务为每个最终 staged group 写唯一的
added/changed/unchanged/rejected action；exact baseline CAS closure 另写 closed action。
每条 evidence 同时绑定 baseline、最终 staging 与实际 result revision。状态读取会从
evidence 重算五个计数，因此手改 `closed_count`、删除 evidence 或伪造 revision 都会
fail closed。这里的 applied rejected 是“最终 rejected group 数”，不是跨页历史
rejected observation 数。

## 运维入口

```bash
# 写本地 SQLite；每次最多推进一个 Gamma 页面
make reconcile-market-map

# 只读 durable window/cursor/counts
make reconciliation-status db_path=/path/to/state.db
```

`POLYARB_OPPORTUNITY_RECONCILIATION_ENABLED=false` 和
`POLYARB_LEGACY_STRUCTURE_RECONCILIATION_ENABLED=false` 都是默认值。前者只在显式启用后
启动 bounded runner；后者保留旧 Structure 的统计/历史代码，但不再默认运行。

`/health` 的 `perception:reconciliation_progress` 与
`perception:reconciliation_checkpoint_age_seconds` 直接读取 window 和最后一条 batch
receipt，并复用 store 对完整 receipt/sample/staging/baseline 链的同一验证。损坏状态
显示 unavailable，而不是抛出或伪装 idle。它们是 scoped evidence，不参与
Candidate/整体 availability；Task 5 才负责把 stalled checkpoint 变成独立 incident。

## 设计取舍

- terminal 必须同时证明 event/candidate/sample 和 receipt 的所有计数为零，并返回空
  cursor；没有 completion proof 就没有 closure authority。
- duplicate group、cursor race、receipt/计数链损坏全部 fail closed。
- window 记录 `started_at_ms..finished_at_ms`，不伪装成单一 `as_of` 时刻。
- Slice D 仍是 observer-only、default-off、未部署；不包含钱包、签名、余额或订单。

## 自检题

1. 为什么一个 `open` 窗口即使已扫 99%，也不能关闭缺失 group？
2. 进程死在 terminal receipt 后，重启为什么不应重新请求第一页？
3. reconciliation 看见旧身份、Discovery 同时发布新身份时，谁应胜出？
4. identity incomplete 的 group 为什么既不能 change，也不能 close？
5. checkpoint stale 为什么应开 scoped incident，而不是让 Candidate Quote 立即不可用？

## FAQ 增量

暂无。
