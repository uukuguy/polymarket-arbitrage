# 事务型 Quote 云端控制面：把一次超大写入拆成可接管的小事务

## 30 秒心智模型

旧 Quote 路径像一次搬完一座仓库：一个进程读完整 universe，再把所有结果写进
SQLite。任何超时、OOM 或 writer 争用都会让整次采集重来。

新路径把它拆成许多独立、可认证的箱子：一个 `quote-batch` 只拥有一个冻结 token
范围和它的市场身份；它把结果写到不可变 R2 对象、确认远端对象确实存在，然后把
receipt 写入 Postgres。只有 `quote-certify` 确认所有箱子到齐，才会更新
`quote:current`。所以一个 worker 消失时，旧的已认证 feed 仍在，另一个 worker 可以
接管未完成的箱子。

## 代码地图

- [models.py](../../src/polyarb/control_plane/models.py)：`QuoteBatchSpec` 与冻结的
  `QuoteBatchLeg`；这就是接管时唯一允许读取的输入。
- [quote_artifact.py](../../src/polyarb/control_plane/quote_artifact.py)：canonical JSONL、
  内容寻址 R2 key，以及 PUT 后 HEAD 的长度/SHA-256 认证。
- [quote_worker.py](../../src/polyarb/control_plane/quote_worker.py)：一次 batch 和一次
  certifier 的有界执行。
- [postgres.py](../../src/polyarb/control_plane/postgres.py)：lease epoch 栅栏、receipt、
  pointer 和不依赖 SQLite 的 operator 投影。

## 关键取舍

为什么不只持久化 token id？book 本身只告诉我们价格属于哪个 token；机会流还要
`market_id`、`condition_id`、event、slug 和 membership 身份。若接管 worker 再到新版
Structure 或 SQLite 拼这些字段，就可能把旧 book 配到新市场。因此 admission 同时冻结
完整 leg mapping。

为什么 R2 成功后仍要 Postgres receipt？R2 是大对象存储，不是并发协调器；receipt 是
带 `(job_key, lease_owner, lease_epoch)` 栅栏的事实。它把“对象存在”变成“当前 generation
可计入的对象存在”。

## 操作边界

`make quote-control-plane-once enable=1` 是一次、显式确认的 operator 工具，不是 daemon
开关。它要求 DSN 与 R2 凭据，并至多运行一个 batch 与一个 certifier。没有 `enable=1`
时立即拒绝且不连接控制面。影子阶段查看 `make control-plane-status` 或
`/perception/control-plane` 的 `quote` 区块：batch/certifier 状态、最老 retry 年龄和当前
认证指针都来自 Postgres。

这不授权上线切换。上线前仍要完成迁移、双 shadow、digest/coverage 比对、worker kill
接管演练及可逆 pointer switch。

## 自检题

1. 为什么一个 receipt 已写、但 `finish` 未写时，替代 worker 不应再次请求 CLOB？
2. 为什么 partial generation 不能移动 `quote:current`，即使其中大多数 batch 都成功？
3. `quote` operator 读模型为什么不能回退到 SQLite？

## FAQ 增量

暂无。
