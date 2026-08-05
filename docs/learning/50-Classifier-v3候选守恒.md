# Classifier-v3 候选守恒：扫到不等于可交易

## 30 秒心智模型

Structure 完整扫描回答的是“源里一共有多少候选”，不是“策略能交易多少市场”。
classifier-v3 要求每个候选恰好进入三个出口之一：eligible member、closed-taxonomy
expected exclusion、unresolved diagnostic。只有三者严格守恒，且 diagnostic 为零，系统才可能
封印比较收据；expected exclusion 数量很大并不代表系统降级。

生产形状证明锁定为：

`166,926 candidates = 41,768 eligible + 125,158 expected exclusions + 0 diagnostics`

## 代码地图

- `src/polyarb/perception/structure_drift.py:219`：每个最多 500 行的 chunk 同时推进
  member、exclusion、diagnostic 三条 commitment，并检查候选守恒。
- `src/polyarb/perception/structure_drift.py:329`：`FreshProjectionExclusion` 只接受七个
  closed-taxonomy reason；`:342` 定义 reason-domain canonical tuple。
- `src/polyarb/storage/sqlite_store.py:6960`：event-only 候选的有序 v3 partition；
  `:7394` 是 market 候选的有序 partition。未知、畸形、冲突和伪 quarantine 仍走 diagnostic。
- `src/polyarb/storage/sqlite_store.py:7935`：`BEGIN IMMEDIATE` + checkpoint CAS；`:7945`
  起把候选数、三类状态和 reason roots 在同一事务写入。
- `src/polyarb/storage/sqlite_store.py:8636`：receipt finalizer 复核三方守恒、独立 source
  candidate count 和每个 reason 的 count/root。
- `src/polyarb/storage/sqlite_store.py:9684`：status validator 重新认证 v3 evidence；`:9744`
  之后才暴露排序后的非零 exclusion counts/roots。
- `tests/m1-perception/test_structure_drift_performance.py`：166,926 行确定性、bounded-memory
  证明；独立 one-shot oracle 与 `limit=1/17/500` 增量结果必须同 root。

## 守恒式在代码里的形状

`src/polyarb/perception/structure_drift.py:285` 的核心约束是：

```python
advanced_candidates_processed = (
    commitment.candidates_processed + chunk.candidates_processed
)
advanced_member_count = commitment.member_count + len(chunk.members)
advanced_exclusion_count = commitment.exclusion_count + len(chunk.exclusions)
advanced_diagnostic_count = commitment.diagnostic_count + len(chunk.diagnostics)
```

紧接着要求 `candidate == member + exclusion + diagnostic`。游标只改变 chunk 边界，不能改变
canonical 行顺序，所以 1、17、500 三种 limit 的最终 count/root 必须完全相同。

## Expected exclusion 为什么不是 warning / diagnostic

expected exclusion 是已经通过精确证据识别、且在 closed taxonomy 中有名字的“策略域外候选”。
例如普通 non-neg-risk market 或 augmented group 确实属于完整源扫描，却不属于当前 standard
neg-risk serving universe。它们应该被计数、按 reason 单独哈希和展示，但不应把健康状态变成失败。

diagnostic 的含义相反：分类器没有足够、可信且无冲突的证据决定出口。缺 boolean、重复 identity、
global conflict、quarantine hash 不匹配或未知 group reason 都不能塞进 catch-all exclusion；它们保持
unresolved，并阻止授权。warning 只是展示严重度，不是经过收据认证的候选出口，因此也不能替代
这两个数据契约。

## 精确生产形状证明

七类 expected exclusion 的固定计数为：

| Reason | Count |
|---|---:|
| `non-neg-risk-market` | 82,346 |
| `market-side-quarantine` | 193 |
| `non-neg-risk-event-member` | 13,655 |
| `current-nontradable-event-member` | 17,515 |
| `augmented-group` | 11,069 |
| `fresh-group-ineligible` | 312 |
| `event-only-quarantine` | 68 |
| **合计** | **125,158** |

测试不保存生产 payload，也不把 166,926 个对象同时放进内存。确定性生成器每次最多物化
`limit <= 500` 个 outcome；driver 验证每次游标精确前进、页数等于 `ceil(166926 / limit)`，因此
reader 若忽略 cursor 或 limit 会立即失败。golden root 由独立实现的 one-shot canonical-tuple
RowChain oracle 产生，再与三个增量 checkpoint 路径交叉核对。

## 设计取舍

- 七个 reason 分域 commitment，而不是只存总数：能定位哪类证据被改写，并防止 reason 之间换桶。
- taxonomy 封闭且没有 `other`：牺牲“自动吞掉新形状”的便利，换取未知源状态 fail-closed。
- checkpoint 保存 SHA 状态而不是候选列表：内存与重启成本有界，但 canonical tuple/顺序成为协议。
- source candidate count 使用独立 indexed anti-join 重算：多一次终局查询，换取 reader 自报总数不可
  单独伪造。
- expected exclusion 对 health 可观察但不降级：真正的失败条件仍是 diagnostic、收据无效或守恒破坏。

## 自检题

1. 如果 193 个 market quarantine 的 payload hash 有一个不匹配，为什么不能仍归入同一 exclusion？
2. `eligible + exclusion` 等于 candidate，但 diagnostic count 被篡改成 1，finalizer 应如何处理？
3. 为什么 `limit=1` 与 `limit=500` 只比较 count 不够，还必须比较每个 reason 的 root？
4. 某个新 Gamma group reason 看起来“显然不支持”，为什么 classifier-v3 仍不能放进 catch-all？
5. `/health` 展示 125,158 exclusions 时，什么证据允许它保持 pass，什么情况必须 fail？

## FAQ 增量

暂无。
