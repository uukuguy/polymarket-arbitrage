# Structure 与 Archive：把市场成员关系从历史归档中救出来

> 对应：M1 continuity repair（2026-07-27）
> 实际操作先看：[M1 市场感知平台使用手册](../M1-市场感知平台使用手册.md)

## 30 秒心智模型

M2 要先知道一个 neg-risk event 里**有哪些必须一起判断的 outcome**，这叫市场
**Structure**；它是 Gamma 的市场、事件和 member 关系，不是盘口价格。

“这几个 outcome 此刻能否按什么价格成交”是独立的 **Quote** 问题；它由独立 CLOB quote
run 提供。Parquet/R2 的 **Archive** 则是让研究、回测和审计能回看过去，不是交易关键路径。

所以三者的正确顺序是：

```text
Gamma Structure ──> Quote run ──> M2 机会判断
       │
       └──────────> Archive（可慢、可失败、不能反向覆盖前两者）
```

以前把它们绑成一次全量 snapshot：Archive 的 CLOB 大任务 OOM 时，顺便让 Structure 和 Quote
一起变旧。现在 `structure` 只做 Gamma 与最终 member reconciliation；`archive` 明确是另一种
产品，永远不替换在线 `markets`。

## 关键代码

线上 scheduler 只会启动 Structure，不会意外启动历史归档：

```python
# src/polyarb/daemon/scheduler.py:76
sys.executable, "-m", "polyarb.snapshot", "snapshot",
"--product", "structure", "--json", "--low-priority",
```

Structure 走到 CLOB 阶段时真正跳过客户端创建，而非“请求空 token 列表”这种伪隔离：

```python
# src/polyarb/snapshot/orchestrator.py:980
if product == "structure":
    logger.info("Structure product: CLOB fetch intentionally skipped")
else:
    clob = ClobReaderClient(settings)
```

Archive 即使拿到完整 Gamma/CLOB 数据，也被强制禁止发布 `markets`：

```python
# src/polyarb/snapshot/orchestrator.py:1303
if product == "archive":
    publish_markets = False
```

`/health` 的在线快照检查也只查询 Structure。Archive 被单独展示，但不会参与总状态的
fail/warn 聚合：

```python
# src/polyarb/http/health.py
last_snapshot = store.get_latest_snapshot(data_product="structure")
# archive:last_attempt / archive:last_success_age_seconds are informational
```

## 怎样实际使用

```bash
# 本地验证完整市场成员关系；轻量、Gamma-only
make sync-structure-local

# 需要回测/审计文件时才主动运行；慢任务，永不作为线上调度器
make archive-markets-local

# 看本机最近的 Structure 调度事实
make snapshot-attempt-status
```

生产上先用 `make smoke-health-prod` 和 `make smoke-market-truth-prod` 看 Structure；看到
`archive:*` warn 时，先判断是否真的需要新的历史样本。它不是“没有机会”或“报价不可用”的
同义词。

## 设计取舍

| 选择 | 为什么 |
|---|---|
| Structure 全量 Gamma，不用 liquidity subset | M2 在过滤机会前必须知道完整 event membership；漏一个 sibling 会制造假套利。 |
| Structure 不碰 CLOB/Parquet/R2 | 这些慢/大/外部依赖不该占用保障市场成员关系和 Quote cadence 的内存预算。 |
| Archive 写自己的 snapshot 行但 `market_view_published=0` | 留下成功/失败证据，仍让当前在线市场视图保持原子不变。 |
| Archive health 只 warn、不参与 overall | 历史研究重要，但它的失败不能伪造成交易数据面故障。 |
| 保留 `legacy_combined` | 老历史仍然有研究价值；但健康和 Quote 明确拒绝把它当新 Structure。 |

## 自检题

1. 为什么 M2 不能直接用 Archive Parquet 中的旧 ask 价格做当前买齐判断？
2. 一个 Archive CLOB 失败后，`markets` 应该指向什么 snapshot？为什么？
3. 为什么 Structure 仍需保留 Gamma 的最终 member reconciliation，而不能只读一次 `/markets`？
4. `archive:last_attempt=failed` 是否应让 `/health` 返回 503？什么情况下才应升级处理？

## FAQ 增量

### Archive 既然会占用磁盘，为什么现在还保留它？

因为它的价值是“可复现过去”：复盘某日机会、研究行情演变、审计数据质量。关键不是让它和
线上事实抢资源，而是把它变成显式、按需、有保留策略的产品。生产定时 Archive 仍未启用；要
启用时必须先给它独立的容量、成本预算和结果通道，而不是给 L1 盲目加内存。
