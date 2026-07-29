# 有界 Incident 权威

## 30 秒心智模型

Incident 是四层互相校验的证据：

1. `neg_risk_incident_events` 保留最多 512 条最近 lifecycle event；
2. checkpoint 承诺已经压缩的历史 prefix；
3. open authority 保存每个未关闭 incident 的当前状态；
4. replay anchor 为跨过 compaction floor 的 suffix 保存直接 predecessor。
5. mandatory suffix authority 从 checkpoint prefix 开始逐条哈希 retained event；
   普通 append O(1) 推进，reader 仍完整重算最多 512 行。

所以 raw suffix 只有 343 条，不等于只有 343 个开放 incident。Dashboard 读取
open authority 的 keyset page；health 还验证 checkpoint、bounded suffix、replay
anchor 和 failure breadcrumb。不一致表示 unavailable，不是零异常。

normalized row 的 hash 只能证明“这行没被改”，不能证明“该有的行没被删”。
因此 reader 还把 open leaf 实际计数与 aggregate 对账，把 floor 实际计数与
checkpoint 对账。为避免这种精确对账随运行时间无限增长，open authority
硬上限是 4,096，scope floor 硬上限是 8,192；触顶会回滚并告警。

## 关键代码

- `src/polyarb/perception/incidents.py` 的 `detect()` / `transition()` 在同一
  `BEGIN IMMEDIATE` 中追加 event、更新 open authority、聚合与 checkpoint。
- `_compact_events()` 在高水位 512 触发，把 suffix 降到约 256，并同步 scope
  floor 与 replay anchor。
- `open_incident_page()` 只取 `limit + 1` 行决定 opaque cursor。
- `src/polyarb/http/health.py` 的 `perception:incident_evidence` 与公开接口使用
  同一 validator，没有 feature flag 可以绕过。

## 为什么需要 replay anchor

假设 sequence 1 是 `detected`，sequence 2 是 `classified`。如果 compaction
删除 sequence 1、保留 sequence 2，单看 sequence 2 无法证明转换合法。replay
anchor 让 reader 验证：

```text
anchor.sequence + 1 == suffix.first.sequence
anchor.state -> suffix.first.state 是允许的边
scope / kind 不变，时间不倒退
```

当该 incident 已没有 retained suffix 时，anchor 必须删除；孤儿 anchor 也是损坏。

## Failure breadcrumb

writer 在追加前验证既有 authority。失败时原事务回滚，再用独立事务把
`neg_risk_evidence_failures(component='incident')` 标成 unresolved，避免失败随
回滚消失。修复 authority 后，首次成功 writer validation 用独立 owner-journaled
事务写入 `recovered_at_ms`；即使随后业务写是幂等 no-op，也能解除故障状态。

这张表是当前故障权威，不是无限历史：incident/resource 最多各一行。

## 设计取舍

- open authority 和 scope floor 分别受 4,096 / 8,192 硬上限保护；public
  reconciliation 是 O(cap)，不是无界扫描，也不是虚假的 O(1)。
- notification delivery 不属于这条 authority。Dashboard 能证明 action 已持久化，
  不能证明外部消息已送达。
- Dashboard 的 `recovery_start_evidence` 只证明 `recovering` transition；terminal
  verification proof 用于关闭校验，但不由 open-only endpoint 暴露。

## 自检题

1. 为什么不能用 raw event count 作为开放 incident 数？
2. sequence 2 留在 suffix、sequence 1 被压缩时，缺 anchor 应返回什么？
3. writer 验证失败后，为什么 breadcrumb 必须用独立事务写？
4. `action=retry-producer` 能否证明 Telegram 已送达？

## FAQ 增量

### checkpoint 已有 hash，为什么还需要 owner journal？

普通 SHA-256 发现字段与 hash 不一致；若同时重算，两者仍可能自洽。owner
journal/trigger manifest 证明变更经过受控 writer，row/checkpoint hash 证明
canonical 内容没有局部漂移，两者职责不同。
