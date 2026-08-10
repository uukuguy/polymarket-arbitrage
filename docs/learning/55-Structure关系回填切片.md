# Structure 关系回填切片：恢复不能靠 watchdog 杀进程

## 30 秒心智模型

Gamma 的 event 页里包含 market 成员关系。市场页全部下载完后，M1 还要把这些
嵌套关系转成可认证的 `event_id ↔ market_id` staging 行，才能开始新的 Structure
发布。这个转换同样是生产任务，不是“本地补数据”：它必须能被中断、提交进度并在
下一次 child 中继续。

此前每个 bootstrap 调用最多吞 500 个事件，并继承 SQLite 120 秒 writer 等待。真实
volume 下，单个调用超过 45 秒协作预算，父进程只好在 75 秒 watchdog 时杀掉它。虽然
已提交一部分进度，但系统持续保持 P1，不能把这叫正常恢复。

现在在线任务每次最多提交 50 个事件/50 条关系，并把 writer 等待收紧到“最多 5 秒且
绝不超过本 slice 剩余预算”。因此 checkpoint 是正常路径；75 秒只保留给真正失控的
child。

## 代码地图

- `src/polyarb/perception/structure_sync.py:run_structure_sync_until_published`
  — 为 online bootstrap 分配 50 行单元和剩余预算的 writer timeout。
- `src/polyarb/storage/sqlite_store.py:advance_structure_event_market_backfill`
  — 读取一个持久 cursor、写入关系 staging，并原子推进 cursor。
- `tests/m1-perception/test_structure_sync_window.py:test_online_bootstrap_caps_one_relationship_backfill_chunk_before_slice_deadline`
  — 60 条输入、45 秒 slice 的回归证明。

## 设计取舍

小 chunk 增加事务次数，却换来两个更重要的性质：Quote 不会被长期 writer 锁饥饿，且
每次重启只损失当前未提交的小单元。不能只把 75 秒调大；那会把异常从“被发现”变成
“更久不可用”，并挤压 Quote 的 300 秒 SLA。

旧 volume 的 add-only 迁移也必须先补 `source_ordinal` 列，再建立依赖它的索引；否则
恢复程序会在启动时失败，连已有 checkpoint 都无法读取。

## 自检题

1. 为什么“已有 cursor”仍不足以证明一个恢复任务适合生产持续运行？
2. 协作 slice、writer timeout 和 child hard limit 分别约束哪一层？
3. 为什么 50 行 checkpoint 比一个 500 行大事务更能保护 Quote？
