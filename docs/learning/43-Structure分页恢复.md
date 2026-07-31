# Structure 分页恢复：超时不是从第一页重来

## 30 秒心智模型

Structure 全集不是一个必须在 240 秒内跑完的请求，而是一扇可恢复的窗口：

`events page N → 原子提交 cursor → markets page N → 完整认证 → 一次发布`

子进程可在任意远端页超时。已提交页面不会丢；下一次恢复读取同一个 opaque cursor，
继续下一页。在线 `markets` 只在整扇窗口完成并通过既有 validator 后换代，因此“能恢复”
不会变成“允许半套市场成员关系上线”。

## 代码地图

- `src/polyarb/perception/structure_sync.py:35`：每次只推进一页。
- `src/polyarb/storage/sqlite_store.py:967`：创建或恢复唯一开放窗口。
- `src/polyarb/perception/structure_sync.py:143`：完整窗口复用生产 validator。
- `src/polyarb/daemon/scheduler.py:101`：隔离子进程调用恢复型 CLI。
- `src/polyarb/daemon/scheduler.py:700`：失败后五秒再试，成功才回正常 cadence。
- `src/polyarb/http/health.py:861`：暴露 stage 与已提交页数。

## 三个关键取舍

### cursor 与页面事实必须同事务

先写 cursor 再写事实会漏页；先写事实再写 cursor 会在重放时产生身份冲突。
同一事务提交两者，崩溃后只能看到“旧页全部未提交”或“新页全部已提交”。

### RECOVERING 是运行状态，不是停止状态

连续失败仍会使 strict health 变黄/红并留下 attempt、stage 和 counter，但 producer
继续有界重试。`PAUSED` 只用于读取旧版本遗留状态，加载后迁移为 `RECOVERING`。

### 完整窗口仍需要少量实时 point lookup

events 和 markets 分页不是同一瞬间，validator 可能要确认分页期间变化的成员状态。
staged source 因此只对这类有界点查委托 live Gamma；全集仍来自 durable window，
不会重新退回一次性全量抓取。

## 运维读法

`snapshot:structure_sync`：

- `open / stage=events`：正在恢复 events；
- `events_complete / stage=markets`：events 完成，继续 markets；
- `complete / stage=publish`：全集已落盘，等待认证发布；
- `published`：该窗口已绑定一个 certified snapshot；
- `failed`：失败关闭，需要看 failure reason，不能把 staged 数据当真相。

同时看 `snapshot:latest_attempt`、`snapshot:failure_counter` 和
`market_truth:last_complete_age_seconds`。旧真相可读、恢复窗口在推进、最新尝试失败，
三件事可以同时成立。

## 自检题

1. 子进程在 market page 37 超时，为什么下一次不能从 market page 1 开始？
2. `event_pages=80` 是否允许 Quote 使用这扇窗口？为什么？
3. 为什么连续失败要报警，但不能再次永久 PAUSED？
4. point lookup 为什么不破坏 staged window 的全集权威？
5. 哪三类生产证据齐全后，才能说机会 feed 已恢复？

## FAQ 增量

暂无。
