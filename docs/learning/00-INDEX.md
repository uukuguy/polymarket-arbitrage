# Phase 1 教学文档索引

> **目的**：Phase 1 的代码是 agent 并行执行造的，我（用户）需要补回知识曲线。
> 这套文档由 Claude 写成，我读 → 提问 → Claude 修订大纲 → 我再读，循环到我能独立打开任何 `src/polyarb/` 文件不慌。
>
> **不是 API 文档**。文档目标是建立心智模型，而不是穷举字段。

## 阅读顺序

| # | 文档 | 你读完之后能答 |
|---|---|---|
| 01 | [Polymarket 数据双源](01-polymarket-data-sources.md) | Gamma vs CLOB 各自给什么、为什么需要两个、什么时候打哪个 |
| 02 | [一次 snapshot 的完整旅程](02-snapshot-pipeline.md) | 一行 `make snapshot-markets` 内部走了哪 7 步、每步产出什么数据形状 |
| 03 | [MarketSnapshot 数据形状](03-market-snapshot-shape.md) | SQLite `markets` 表 / Parquet 行 / 内存里 dict 三处的字段对应、为什么要严格对齐 |
| 04 | [Validator 三层防御](04-validator-layers.md) | Layer 1/2/4 各防什么、为什么没有 Layer 3、ghost_book 在第几层、为什么 is_valid 只看 Layer 1 |
| 05 | [Issue #180 ghost_book 实战](05-ghost-book-issue-180.md) | 这个问题的根源、为什么影响 72%、下游策略写代码时的硬约束 |
| 06 | [代码安全约束（F-1 ~ F-8）](06-security-invariants.md) | 为什么每个 `float()` 都包 try、为什么 `MAX_PAGES=1000`、F 编号代表什么 |
| 07 | [观察市场（Observation Toolkit + Translation）](07-观察市场.md) | 6 个配方分别看什么、workflow 怎么走、翻译/AST/diff 三个设计取舍、5 道自检题 |

## 每篇文档的体例

- **核心心智模型**（30 秒能讲清楚的版本）
- **代码地图**（src/polyarb/ 哪几个文件实现）
- **关键代码片段**（贴最核心的 5-15 行，配 file:line 引用）
- **会让你卡住的细节**（基于 Phase 1 实际遇到的坑）
- **自检题**（答得上 = 这一节过；答不上 = 提问）

## 如何提问

1. 读完一节，把"看不懂的句子"或"看了但不知道为什么这样设计"贴出来
2. 我会把答疑内容**追加进对应文档的"FAQ 增量"区**，而不是重写正文
3. 当某个 FAQ 出现 3 次以上 → 我会把它提升成正文的一节
4. 我们持续迭代到你能不依赖我地阅读 `src/polyarb/` 任何文件

## 实物先于理论的可选路径

如果某节读着抽象，跳到 `tests/m1-perception/` 去找对应的测试 —— 测试是带具体输入输出的代码示例。不必从头读测试，只读你正在学的那一节对应的几个 test。

## 不在本系列里的内容

- 项目章程 / 角色契约 → `CLAUDE.md`
- 决策时间线 → `.planning/JOURNAL.md`
- Phase 1 决策记录 → `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-CONTEXT.md`
- live run 实战发现 → `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-LIVE-RUN-001.md`
- 跨 phase 经验沉淀 → `.planning/threads/market-microstructure.md`

教学文档只负责**让你看懂代码**。"为什么我们当时选 A 不选 B" 看 CONTEXT.md / JOURNAL.md。
