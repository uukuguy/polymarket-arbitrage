# Neg-risk 买齐套利：从“概率和异常”到可执行报价

## 30 秒心智模型

同一个 neg-risk 组的多个 YES 结果互斥且完备。每个结果各买 1 份，最终
恰好有一份支付 $1。只有当所有腿都能按 ask 成交且 `sum(asks) < 1`，才有
买齐型毛套利；mid price 偏离只能当研究信号，不能当可成交收益。

```text
M1 完整兄弟组 → 每腿真实 best ask/size → sum asks < 1 → M2 opportunity
```

关键代码：

- `src/polyarb/snapshot/orchestrator.py`：subset 也保留全部 neg-risk 兄弟，避免
  低流动性腿被过滤后制造假套利。
- `src/polyarb/routing/opportunity_scanner.py`：按组验证每腿状态与 ask，数量取
  `min(ask_size)`，输出 gross edge 与 gross profit。
- `src/polyarb/http/arbitrage.py`：生产只读入口，snapshot 超过 15 分钟直接 503。

## 设计取舍

- 只做 buy-all，不做 sell-all。后者需要库存、split/merge/convert 与签名权限。
- 明确写 `gross-before-fees`，不把手续费、价格移动和失败腿风险藏进“净收益”。
- 任一腿缺 ask、缺 size、inactive、closed 或 incomplete，整组丢弃。

## 自检题

1. 三腿 mid 之和 0.94，但 ask 之和 1.01，是套利吗？为什么？
2. ask 之和 0.97，其中一腿 ask size 为 0，能报告 300 bps 吗？
3. 为什么 subset 不能只保留流动性大于 $1,000 的 neg-risk 腿？

## FAQ 增量

### 这个结果可以直接实盘吗？

不可以。它是可成交盘口层的 gross 候选，仍需费用模型、原子多腿执行、失败补偿
和 live adapter 授权。当前可直接用于监控与 paper 决策。
