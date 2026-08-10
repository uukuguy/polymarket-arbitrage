# Quote 重启租约回收：停止的 child 不能继续占着采集权

## 30 秒心智模型

Quote 和 Structure 共用一个 SQLite 写入 slot，用 lease 防止同时写坏状态。
正常退出会释放 lease；但滚动发布会先杀旧 Quote child，再启动新 child。旧进程
没来得及清理时，原来的 210 秒 lease 还有效，新 child 会被自己留下的锁挡住。

现在新 Quote child 在真正构造 worker 前，原子回收**仅限 Quote**的遗留 lease，并写
`released` 收据。它绝不会删除 Structure lease：Structure 正在做 checkpoint 时仍保留
完整的互斥保护。

## 代码地图

- `src/polyarb/daemon/producer_arbitration.py:relinquish_orphaned_quote_lease`
  — `BEGIN IMMEDIATE` 中确认 owner 是 Quote 后删除并写收据。
- `src/polyarb/perception/worker_cli.py:run_component`
  — 受监督 Quote child 在创建 collector 之前执行回收并记录 warning。
- `tests/perception/test_supervisor.py:test_quote_worker_cli_reclaims_its_orphaned_lease_before_worker_starts`
  — 证明顺序不是仅有一个底层 helper。

## 设计取舍

不把它做成“回收任意 owner”的通用 API。生产恢复需要的是缩短 Quote 自己因滚动发布
留下的空窗；回收 Structure 会把恢复优化变成数据竞争风险。对应的 receipt 让 Dashboard
和 Polywatch 能将这次恢复与普通过期区分为一次可审计的 `released` 事件。

## 自检题

1. 为什么由新 Quote child 删除旧 Quote lease 不等同于允许双 Quote worker？
2. 为什么这段代码必须在构造 Quote worker 之前运行？
3. 为什么 Structure lease 即使看起来“旧”也不能从这个入口删除？
