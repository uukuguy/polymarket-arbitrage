# Structure 切片截止时间：为什么“能续跑”仍可能不停超时

## 30 秒心智模型

Structure 从 Gamma 一页一页同步市场。每个 child 的正常工作时间是 45 秒，75 秒
是父进程的最后保险丝。两者不能混用：45 秒结束时应写下 cursor 并退出；75 秒只用
来处理失控 child。若在第 40 秒仍发起一个允许跑 35 秒的网络请求，父进程只能在
75 秒把它杀掉，于是会出现“页数在涨、P1 却每轮超时”的假恢复。

修复后的每页请求预算是：

`min(单页上限, 当前切片剩余时间 - SQLite 提交预留)`

不够提交预留时，立即返回已有的 durable checkpoint，下一轮从同一 cursor 继续。

## 代码地图

- `src/polyarb/perception/structure_sync.py:run_structure_sync_until_published` —
  在启动每一页前计算剩余 slice，并决定“运行受限的一页”或“正常 checkpoint”。
- `src/polyarb/perception/structure_sync.py:StructureSyncWorker.run_batch` —
  一页的 fetch 后才允许写入 cursor；因此 fetch 预算必须为 commit 留出空间。
- `src/polyarb/daemon/scheduler.py:run_snapshot_in_subprocess` — 75 秒 watchdog
  是异常 containment，不是日常分页的调度器。

## 设计取舍

这里不把 child hard limit 增大。增大只会让 Quote 与 Structure 更久争用同一
SQLite/内存 lane，也会把“正常 checkpoint 没发生”的原因藏得更久。动态缩短末页
请求会偶尔少抓一页，却换来确定的进度收据和下一轮的可恢复性；生产盯盘需要后者。

## 自检题

1. 为什么固定 35 秒的单页 timeout 在 45 秒 slice 中仍不安全？
2. 为什么 75 秒 parent kill 不能作为正常的分页退出方式？
3. 末页未开始时返回 checkpoint，如何保证下一轮不会丢页？

## FAQ 增量

- **请求被缩短后会不会丢 cursor？** 不会。cursor 只在整页 fetch 成功并完成
  SQLite commit 后推进；被 deadline 取消的一页没有提交，下一轮重试相同 cursor。
