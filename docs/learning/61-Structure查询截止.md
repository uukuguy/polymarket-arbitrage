# Structure 查询截止：slice 时钟必须进入 SQL

`max_elapsed` 若只在 Python 循环边界检查，正在运行的一条 SQL 仍可越过 slice
预算，最终让外层 watchdog 强杀子进程。现在每个 member/group-truth chunk 将剩余
45 秒预算注册为 SQLite progress handler；查询到期会得到可审计的 `deadline`
checkpoint，并由 scheduler 有界重试。

关键代码：`snapshot/cli.py` 传递剩余预算；`sqlite_store.py` 在两个 writer
connection 上安装 progress handler；`scheduler.py` 保留 `deadline` defer 原因。
