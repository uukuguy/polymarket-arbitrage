# 端到端验收指令指南

> 一条命令一条命令打下去，验证全链路无断裂。遇到失败停下来排查，不要跳过。

## 0. 前置条件

```bash
# 确认 Python 版本
python --version  # 应为 3.12+

# 安装依赖（首次或 lock 变更后）
uv sync --extra dev

# 确认 .env 已配置翻译 API（至少这三个必填）
grep -E "^TRANSLATION_" .env
# 期望输出：
#   TRANSLATION_API_BASE=https://api.deepseek.com/v1
#   TRANSLATION_API_KEY=sk-...
#   TRANSLATION_MODEL=deepseek-chat
```

## 1. 全量测试（必过，无例外）

```bash
make test
```

**验收标准**：421 passed，0 failed，0 error。有任何红色 → 停，查 `uv run pytest -v tests/ -x` 定位第一条失败。

时间：约 30-60 秒。

## 2. 翻译管线（离线验证，不依赖 Polymarket API）

```bash
# 2a. 试译 50 条（验证 .env 配置 + LLM 连通性）
make translate-pending-sample
```

**验收标准**：50 条全部译完，无 rate limit / auth 报错。输出末尾打印 `translated=50`。

```bash
# 2b. 翻译统计
make translation-stats
```

**验收标准**：`total_translated > 0`，`translator_model = deepseek-chat`（与你 .env 中配置一致）。

> 如果 2a 通过但 2b 显示 0，说明 SQLite 路径不一致 — 检查是否在项目根目录执行。

```bash
# 2c.（可选）全量翻译 — 仅当 2a 通过后执行
make translate-pending FORCE=1
```

## 3. 市场快照管线（在线验证，依赖 Polymarket API）

```bash
# 3a. 抓取子集快照（~15-30 分钟，流动性 > $1k）
make snapshot-markets-v
```

**验收标准**：
- 7 个 Phase banner 依次打印（Phase 1/7 → 7/7），无 traceback
- 末尾 `Validator Summary`：4 层校验全部 `status=OK`（Layer 3 `noop` 可接受）
- `Total markets written` > 0

```bash
# 3b. 检查快照状态
make snapshot-status
```

**验收标准**：显示最近一次 snapshot 的时间、SQLite 行数、最新 Parquet 路径。

```bash
# 3c.（可选）全量快照（~1-2 小时，所有市场）
make snapshot-markets-full-v
```

## 4. 观察工具箱（依赖至少一次快照）

> 以下命令依赖步骤 3a 已产生至少一份快照数据。

```bash
# 4a. 列出所有扫描配方
make list-recipes
```

**验收标准**：至少列出 6 个 builtin 配方（`thick-but-slippery`、`near-end`、`ghost-suspicious`、`coin-flip`、`neg-risk-incomplete`、`by-tag`）。

```bash
# 4b. 跑一个扫描配方（near-end — 72h 内到期市场）
make scan-near-end
```

**验收标准**：输出扫描结果表，列名包含 `slug / title / end_date / liq / spread / mid`。结果数取决于当前市场。

```bash
# 4c. 跑幽灵订单扫描（CLOB/Gamma 交叉校验）
make scan-ghost-suspicious
```

```bash
# 4d. 跑 neg-risk 扫描（M2 套利信号）
make scan-neg-risk-incomplete
```

```bash
# 4e. 深看一个市场——从 4b 输出中挑一个 slug
make show-market slug=<从4b输出复制>
```

**验收标准**：
- 上半部分：中文标题 + 中文描述（翻译管线已生效）
- 下半部分：5-snapshot 历史表（如果只有 1 次快照则显示 1 行）
- 如果是 neg-risk 市场，显示 sibling 列表

```bash
# 4f. 时序追踪同一个市场
make track-market slug=<同上的slug>
```

**验收标准**：显示该市场在所有快照中的 mid / spread / liq 变化表。

```bash
# 4g. 快照对比（需至少 2 次快照）
make compare-snapshots
```

**验收标准**：显示 N-1 → N 的差异汇总（new / removed / price_changed / ghost_book_changed）。

```bash
# 4h. 自选列表
make watchlist
```

**验收标准**：列出 `watchlist.yaml` 中所有市场及其当前状态（即使文件为空也应正常输出 "No markets in watchlist" 或类似提示）。

```bash
# 4i. 自选告警
make watchlist-alerts
```

**验收标准**：正常执行，无报错。触发/未触发均有明确输出。

## 5. 端到端快速回归（日常用）

如果只是验证"上次的改动没把管线搞断"，跑这条就够：

```bash
make test && make translate-pending-sample && make snapshot-markets-v
```

时间：~15-30 分钟（大部分时间在等 Polymarket API）。

## 故障排查速查

| 症状 | 先看 |
|---|---|
| `make test` 失败 | `uv run pytest -v tests/ -x --tb=short` |
| 翻译 auth 报错 | `.env` 中 `TRANSLATION_API_KEY` 是否正确；API base 是否带 `/v1` |
| 快照 0 markets | Polymarket Gamma API 是否可达：`curl -s "https://gamma-api.polymarket.com/events?limit=1"` |
| 快照 Layer 4 ghost_book 大量 FAIL | 正常现象（CLOB 和 Gamma 之间本身有延迟窗口），FAIL 数 < 总数 30% 即可 |
| `make show-market` 中文不显示 | 先跑 `make translate-pending-sample` 确认翻译管线正常，再查该 market 是否在 SQLite translation 表中有记录 |
| `make compare-snapshots` 报 "need at least 2 snapshots" | 正常 — 跑两次 `make snapshot-markets-v` 后再试 |
