# Quote 分块 Staging：把大事务变成可恢复的认证发布

## 30 秒心智模型

Quote 抓取完成后，不必把四万多条终态报价塞进一次 SQLite 事务。它们先以
500 行一块写入 `collecting` run；这个 run 对机会 API 和 M2 完全不可见。
只有 `complete_run()` 验证数量、身份和收据摘要后，才一次性把 current pointer
切到新 run。因此“分块”改变的是写入的工作量，不是认证标准。

## 代码地图

- `src/polyarb/routing/neg_risk_quote_store.py:record_terminal_quotes_chunked`
  — 每块事务、按本块 token 验证身份。
- `src/polyarb/routing/neg_risk_quote_collector.py:collect_neg_risk_quotes`
  — 每提交一块就写入 `persist_chunks` / `persisted_quotes` 阶段收据。
- `src/polyarb/routing/neg_risk_quote_store.py:complete_run`
  — 唯一允许 run 变为 `complete` 并切 current pointer 的认证边界。

## 设计取舍

一次大事务有最强的“写入全有或全无”外观，但在 44GB 生产库上把正常尾延迟
错误地变成 P1。分块后，失败 run 可能带有部分 staging rows；这不是半个机会
feed，因为所有读取路径都只选择已认证 current generation。失败 payload 由既有
回收链释放，而其阶段、失败原因和 incident 证据保留。

## 自检题

1. 为什么 chunk 已提交时 M2 仍读不到它？
2. `complete_run()` 为什么仍必须检查 quote 数等于 requested leg 数？
3. 为什么每块验证只读取本块 token，而不是读完整 41k leg 清单？

## FAQ 增量

- **这会允许部分成交或下单吗？** 不会。这里的“partial”只指本地 public CLOB
  观测数据的 staging，不产生钱包、签名、订单或执行权限。
