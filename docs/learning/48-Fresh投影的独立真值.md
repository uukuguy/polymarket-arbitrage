# Fresh 投影的独立真值

## 30 秒心智模型

漂移比较有两边：`fresh source projection` 与 `published generation`。如果 fresh
投影反过来读取 generation 的 group truth 决定成员是否合格，两边就不再独立：一份被
篡改的 generation 可以同时改变“被比较对象”和“裁判”。正确边界是：fresh 只读 raw
event、member sidecar 以及封住它们的 receipt；generation truth 只在最终比较阶段出现。

## 代码地图

- `src/polyarb/storage/sqlite_store.py`：member → group-truth → conflict 的有界状态机；
  fresh reader 的 source truth bulk join；sealed/stale status 的 receipt 交叉验证。
- `src/polyarb/storage/schemas.py`：durable group truth、进度游标和 receipt root。
- `tests/m1-perception/test_structure_drift_projection.py`：raw source 改变 eligibility，
  membership hash 与 source member 完全一致。
- `tests/m1-perception/test_structure_drift_end_to_end.py`：generation truth 篡改不改变
  fresh root/诊断，但最终授权必须失败。

## 为什么多一个 group-truth phase

一个 event 可以有超过 500 个成员。fresh reader 若每页现场聚合整个 group，`limit=1`
时会反复扫描同一组；它虽然是一条 SQL，却不是有界工作。现在 member sidecar 完成后，
状态机按 `(event_id, group_id, market_sort_key, member_ordinal)` 续跑，每次最多 500 行：

1. `SerializableSHA256` 保存尚未完成 group 的精确 membership hash 状态；
2. 已完成 group 写入 `structure_sync_event_group_truth_staging`；
3. 现有 `source-event` RowChain 用 canonical tag 累积所有 truth 行；
4. 最终 count/root 写进 member receipt，之后表被 trigger 冻结。

因此重启只重读当前 checkpoint 后的成员，不重扫已完成前缀。

## 三方交叉绑定

classifier 到达 `sealed` 或 `stale` 后，status 必须同时看到：

`当前 window 的 member receipt digest == drift progress digest == terminal receipt digest`

缺失、替换、混搭或篡改任意一边，status 都只返回
`structure-drift-member-receipt-invalid`，不会泄露旧 class counts 或 diagnostic samples。

## 设计取舍

- 不新增 RowChain domain：group truth 使用既有 `source-event` domain，并用 canonical tag
  做行类型分隔。
- 不为 group truth 再造独立 receipt：它是 member sidecar 的派生权威，count/root 直接
  纳入 member receipt，避免两套封印产生先后竞态。
- membership hash 保持与 `market_truth.membership_hash()` 字节级一致；质量判断来自 raw
  event flags 与 sidecar member facts，不读取 generation cache。

## 自检题

1. 为什么“只执行一条 GROUP BY SQL”仍可能违反每次最多 500 行的约束？
2. generation truth 被改写后，fresh root 为什么必须保持不变？
3. stale 结果为什么也必须绑定当前 member receipt，而不只是 sealed 授权？
4. checkpoint 保存 SHA 内部状态，相比保存已扫描成员列表解决了什么生产问题？

## FAQ 增量

暂无。
