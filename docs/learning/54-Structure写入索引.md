# Structure 写入索引：一个读查询如何拖垮整个恢复循环

## 30 秒心智模型

每一页 Gamma market 在 SQLite 中需要连续的 `source_ordinal`。旧代码在写事务内
查询当前最大 ordinal。数据量小时没有问题；当恢复窗口到十万行时，主键
`(window_id, market_id)` 只能定位窗口，不能按 ordinal 直接取最大值，于是 SQLite
扫描整个窗口，而且扫描期间已持有写锁。

新索引 `(window_id, source_ordinal DESC)` 让“这个窗口最大的 ordinal 是什么？”成为
一次索引头部读取，而非一次十万行扫描。它不改变 market、cursor 或发布真值，只缩短
已被证明是瓶颈的事务内读。

## 代码地图

- `src/polyarb/storage/sqlite_store.py:commit_structure_market_page` — 在
  `BEGIN IMMEDIATE` 后读取最大 ordinal，再给本页分配连续编号。
- `src/polyarb/storage/schemas.py:STRUCTURE_SYNC_WINDOWS_DDL` — 生产启动时
  幂等创建 ordinal 索引，已有 44GB volume 不需要重建表。
- `tests/m1-perception/test_structure_sync_window.py:test_bootstrap_and_source_certification_keysets_use_indexes`
  — 用真实 SQL 的 `EXPLAIN QUERY PLAN` 锁定访问路径。

## 设计取舍

索引会增加每次 page insert 的少量维护成本，但这一页本来就要写 100 行；相比在写
事务中扫描此前所有页，成本稳定且可预测。不能只增加 timeout：那会让 Quote 更久拿
不到 writer，也不能证明 child 最终会完成。

## 自检题

1. 为什么 `MAX(source_ordinal)` 在 `BEGIN IMMEDIATE` 之后尤其危险？
2. 为什么主键 `(window_id, market_id)` 不能高效回答按 ordinal 的最大值？
3. 索引如何做到优化性能却不改变结构快照的真值契约？
