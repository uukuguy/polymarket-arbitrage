# Structure 回填查询截止：外层循环不能约束一条卡住的 SQL

## 30 秒心智模型

“每 50 行检查一次时间”只有在这 50 行的 SQLite 调用能返回时才有意义。若某条
SELECT 或 INSERT 自己卡住，外层 45 秒 slice 看不到它，最后只能由 75 秒 parent
watchdog 杀 child——既慢又缺少精确原因。

在线回填现在把剩余 slice 再切给单条 SQLite 执行：最多 5 秒。SQLite progress handler
一旦越界就中断语句、事务回滚，当前 durable cursor 保持不变，child 返回一个普通
`bootstrap` checkpoint。下一轮自动重试；不会假装该数据已经处理，也不会让一个 SQL
长期占用 Quote 与 Dashboard 所依赖的 volume。

## 代码地图

- `src/polyarb/perception/structure_sync.py:run_structure_sync_until_published`
  — 计算单元 deadline，识别 SQLite `interrupted` 并返回 checkpoint。
- `src/polyarb/storage/sqlite_store.py:advance_structure_event_market_backfill`
  — 在 connection 上安装 SQLite progress handler，超时后自动 rollback。
- `src/polyarb/daemon/scheduler.py:_SNAPSHOT_STAGE_MARKER_RE` — 接受
  `bootstrap` stage，使 timeout 收据不再只有空白 stage。

## 设计取舍

中断会让本次小单元没有进度，因此它不能被计为发布成功；但这是正确的失败边界。比起
让 parent 在 75 秒后强杀，5 秒 rollback 既保护跨进程 Quote lease，也把下一步诊断
精确缩小到 `bootstrap` SQL，而不是模糊的 `gamma-markets`。

## 自检题

1. 为什么 cooperative loop 的 elapsed check 无法打断同步 SQLite 调用？
2. 为什么 deadline 后要 rollback，而不是提交已执行到一半的数据？
3. `bootstrap` checkpoint 与新的认证 Structure snapshot 分别证明什么？
