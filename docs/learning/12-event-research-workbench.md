# 12 — Event Research Workbench

## 30 秒心智模型

Event detail 是一个 event 的研究决策路径，不是交易面板：

```text
Structure（市场与组是否存在）
  → Quote Coverage（这些腿是否被观察到、是否可执行）
  → Analysis（同代次的组级理论证据，research-only）
```

页面只读取**当前 Quote generation 及其 parent Structure generation**。因此它不会把旧报价、别的 event 的 group 或原始 order-book payload 拼进“当前”结论；也不会把正毛利说成 Certified opportunity 或下单授权。

## 先读什么

1. **Structure**：确认 event 仍 open、组属于该 event，及预期成员数。
2. **Quote Coverage**：比较 expected、observed、executable、non-executable、missing。覆盖不完整或有不可执行腿时，不能把该组当作可整体买入。
3. **Analysis**：仅当每条腿是 `executable`、价格和数量有效时，才计算理论 all-legs bundle 的毛利；仍须把费用、滑点和成交风险当作未知。

## observed、executable、missing 不是同一件事

| 字段 | 它回答的问题 | 不能推出什么 |
|---|---|---|
| observed | Quote 投影里是否看到了该 token 的一条腿 | 它能成交 |
| executable | `terminal_state="executable"`，且 ask/size 是有效正数 | 多腿能同时成交 |
| non-executable | 看到了腿，但它终态不是 executable | 这条腿等于零成本或可以忽略 |
| missing | `expected - observed`；若 expected 本身未知则显示 Unknown | 缺腿为零，或该组没有机会 |

数据库按 token 去重计数 observed / executable / non-executable，并用当前 Structure 的 expected 成员数计算 missing；实现见 `src/polyarb/control_plane/postgres.py:10201-10225`、`:10255-10275`。单组腿的下钻也受 event 所属关系和 keyset page 上限保护，且只返回紧凑的 token、市场、ask、size、终态字段，不返回原始订单簿：`src/polyarb/control_plane/postgres.py:10319-10374`。

## 理论经济性：资本、毛利、ROI

令一个完整组各腿的可执行 ask 之和为 `B`（`bundle_cost`），最小可用数量为 `q`（`max_bundle_size`）。若 `0 < B < 1`：

```text
capital_required_usd = B × q
gross_profit_usd     = (1 - B) × q
gross_ROI            = gross_profit_usd / capital_required_usd
gross_ROI_bps        = gross_ROI × 10,000
```

例如 `B=0.90`、`q=15`：资本 `$13.50`，理论毛利 `$1.50`，毛 ROI `11.11%`（`1111.11 bps`）。公式在 `src/polyarb/control_plane/analysis_candidates.py:12-25`；组必须先通过完整、可执行腿的判定才会得到正边际状态，见 `:36-63` 和 `:109-117`。

“gross”很重要：它**没有**扣费用、滑点、同步多腿成交失败、oracle resolution 或结算延迟。它是研究优先级证据，绝不是 P&L 预测。

## current-generation 与 lineage 真相

读取 event detail 时，服务先取 `quote:current`，再从该 Quote 的输入取得 Structure generation；没有 current Quote、候选明细未发布、event 已关闭/过期都会返回 unavailable/not-published，而不是空结果。见 `src/polyarb/control_plane/postgres.py:10167-10199`。

响应 anchor 会给出 `quote_generation_key`、`structure_generation_key` 和 `changed_since_entry`。从 Structure、Quote Coverage 或 Analysis 链接过来时，后者表示你进入页面后 Quote 是否已换代；它提示“入口观察已过时”，但页面展示的仍是当前 fenced lineage。响应构造见 `src/polyarb/control_plane/postgres.py:10300-10317`，Dashboard 对 anchor 与 groups 做严格解码，见 `dashboard/lib/business-research.ts:155-220`。

## `not assessed` 绝不等于零

风险区明确写的是：执行质量、费用、滑点、oracle resolution 与结算延迟 **not assessed**，缺少源字段显示 `Unknown`，不是 `0`。这是避免“没有数据”被误读为“没有风险”。页面同时显式标注 research-only / 非 Certified opportunity：`dashboard/app/business/events/[event_id]/page.tsx:46-58`。

## 设计取舍

1. **事件级聚合，而非原始簿展开**：足以决定先修覆盖还是先研究正边际，同时控制 payload、浏览器和误读风险。
2. **current-lineage fence，而非历史任意查询**：牺牲回看便利，换取当前结论不混入旧 Quote 或错配 Structure。
3. **理论 gross，而非净收益猜测**：宁可保留未评估项，也不虚构费用、冲击或多腿成交保证。

## 自检题

1. observed=4、executable=3、expected=4 的组，为什么不能直接按四腿 bundle 下单？
2. `bundle_cost=0.95`、`max_bundle_size=20` 时，资本、毛利和 gross ROI 各是多少？
3. `changed_since_entry=true` 表示页面正在使用旧 Quote，还是入口观察已过时？
4. 为什么 `not assessed` 的 fees/slippage 不能在计算里当 `$0`？
5. 为什么 group leg endpoint 还要验证 group 属于 event？

## FAQ 增量

_暂无_

---

← [原子业务研究视图](107-原子业务研究视图.md) | 下一节 → [套利引擎](12-套利引擎.md)
