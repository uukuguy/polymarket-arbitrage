# 03 — MarketSnapshot 数据形状

## 核心心智模型

我们的 "market" 这个数据，**在三个地方**有形状：

```
┌──────────────────────────┐    ┌──────────────────────────┐    ┌──────────────────────────┐
│  内存 dict               │    │  SQLite markets 表       │    │  Parquet 文件            │
│  (流水线传递时)          │    │  (热查询)                │    │  (冷归档 / 时序回放)     │
│                          │    │                          │    │                          │
│  21 个 key               │    │  21 列 + 1 PK            │    │  22 列                   │
│  + snapshot_id 由        │    │  全部 21 列              │    │  + snapshot_taken_at_ms  │
│  store 注入              │    │                          │    │  + snapshot_id           │
└──────────────────────────┘    └──────────────────────────┘    └──────────────────────────┘
```

这三个**必须严格对齐**，因为：
- 流水线把 dict 喂给 SQLite store，store 按列序 unpack 成 tuple
- 流水线把 dict 喂给 pyarrow，pyarrow 按 schema 校验

任何字段加减 → **三处必须同步改**：DDL / `MARKETS_COLUMN_ORDER` / `MARKETS_INSERT_SQL` / `SNAPSHOT_SCHEMA`。
代码注释里就有这个警告（`storage/schemas.py:3`）。

## 完整字段表

| # | 字段名 | SQLite 类型 | Parquet 类型 | 含义 | 谁写它 |
|---|---|---|---|---|---|
| 1 | `market_id` | TEXT PRIMARY KEY | string | Gamma 的 `id`，字符串形式 | normalizer |
| 2 | `condition_id` | TEXT NOT NULL | string | 链上 conditionId（hex） | normalizer |
| 3 | `slug` | TEXT | string nullable | URL slug | normalizer |
| 4 | `question` | TEXT | string nullable | 题目 | normalizer |
| 5 | `yes_token_id` | TEXT | string nullable | YES 那个 token 的 ID | normalizer |
| 6 | `no_token_id` | TEXT | string nullable | NO 那个 token 的 ID | normalizer |
| 7 | `mid_price` | REAL | float64 nullable | YES outcome price (Gamma 给的，Phase 1 不一定准) | normalizer |
| 8 | `liquidity_usd` | REAL | float64 nullable | Polymarket 流动性分（USD） | normalizer |
| 9 | `volume_usd` | REAL | float64 nullable | 成交量（USD） | normalizer |
| 10 | `best_bid_price` | REAL | float64 nullable | YES top bid（CLOB book） | orchestrator step 5 |
| 11 | `best_bid_size` | REAL | float64 nullable | YES top bid size | orchestrator step 5 |
| 12 | `best_ask_price` | REAL | float64 nullable | YES top ask | orchestrator step 5 |
| 13 | `best_ask_size` | REAL | float64 nullable | YES top ask size | orchestrator step 5 |
| 14 | `end_time_ms` | INTEGER | int64 nullable | 市场结束时间（epoch ms, UTC） | normalizer |
| 15 | `active` | INTEGER | bool | 是否活跃 | normalizer |
| 16 | `closed` | INTEGER | bool | 是否结束 | normalizer |
| 17 | `neg_risk` | INTEGER | bool | 是否 neg-risk multi-outcome | normalizer |
| 18 | `neg_risk_market_id` | TEXT | string nullable | 父市场 ID（neg-risk 时） | normalizer |
| 19 | `fetched_at_ms` | INTEGER NOT NULL | int64 | CLOB 拉完那一刻 | orchestrator step 5 |
| 20 | `snapshot_id` | INTEGER NOT NULL FK | int64 | snapshots 表的 PK | sqlite_store 注入 |
| 21 | `incomplete` | INTEGER | bool | Layer 2 标记的"字段不全" | layer2_fields 副作用 |
| - | `snapshot_taken_at_ms` | -（仅 Parquet）| int64 | snapshot 开始时间 | orchestrator step 7 |

注意点：
- bool 字段在 SQLite 里是 INTEGER 0/1，在 Parquet 里是 bool。`sqlite_store.py:31` 的 `_BOOL_COLUMNS` 负责入库前转 int。
- `snapshot_id` 在 dict 里**没有**（normalizer 不写），在 SQLite store 入库时由 `_row_to_tuple(row, snapshot_id)` 注入。
- `snapshot_taken_at_ms` 只存在于 Parquet（SQLite 用 `snapshots` 表的 `taken_at_ms` 替代）。

## 三张 SQLite 表

代码：`src/polyarb/storage/schemas.py:20-75`

### Table: `snapshots`（每次快照一行，append-only）

```sql
CREATE TABLE snapshots (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  taken_at_ms     INTEGER NOT NULL,    -- 开始
  finished_at_ms  INTEGER NOT NULL,    -- 结束
  mode            TEXT NOT NULL CHECK(mode IN ('subset','full')),
  market_count    INTEGER NOT NULL,
  is_valid        INTEGER NOT NULL,    -- 0/1
  parquet_path    TEXT NOT NULL,       -- 关联归档
  notes           TEXT
);
```

这张表是**审计日志**。每次 snapshot 加一行，永远不删。可以用它做时间序列查询："过去 7 天 ghost_book 比例如何变化"。

### Table: `markets`（当前快照镜像，每次 DELETE + INSERT）

字段就是上面 21 个字段。每次 snapshot 整表重写，永远只有"最新一次快照"的数据。

⚠️ D-C1 决策：**markets 表不是历史**，是当前镜像。要历史去查 Parquet。

### Table: `validation_issues`（审计日志，关联到 snapshots）

```sql
CREATE TABLE validation_issues (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id  INTEGER NOT NULL REFERENCES snapshots(id),
  layer        INTEGER NOT NULL,        -- 1, 2, 4
  category     TEXT NOT NULL,           -- ghost_book, clob_missing, ...
  market_id    TEXT,                    -- nullable: Layer 1 issue 没有具体 market
  detail       TEXT,                    -- 简短描述
  raw_payload  TEXT                     -- JSON 截断版（≤1024 字节）
);
```

每次 snapshot 也是 append-only。可以查"过去 N 次 snapshot，哪个 market 反复出现 ghost_book"。

## Parquet 文件布局

代码：`src/polyarb/storage/parquet_writer.py:25` `compute_snapshot_path()`

```
data/snapshots/
└── 2026/
    └── 04/
        └── 29/
            ├── 14-23-05.parquet         ← snapshot taken at 2026-04-29 14:23:05 UTC
            ├── 16-44-12.parquet         ← 同一天后面又跑了一次
            └── ...
```

- 路径格式：`{root}/YYYY/MM/DD/HH-MM-SS.parquet`，**全部 UTC**
- 一次 snapshot = 一个 .parquet 文件 = 一张二维表
- 压缩：snappy（DuckDB 友好，写得快）

**为什么这么分目录？**
方便后续按时间窗口 batch 读：`SELECT * FROM read_parquet('data/snapshots/2026/04/*/*.parquet')` 一次拉一个月。

## 数据从原始字段到落库的对应

举一个具体的例子（normalize 那一行）：

```python
# Gamma raw（原始字段）
{
    "id": "516542",
    "conditionId": "0xabc...",
    "slug": "trump-2026",
    "question": "Will Trump...",
    "clobTokenIds": '["7132...", "8924..."]',     # JSON 字符串
    "outcomePrices": '["0.46", "0.54"]',          # JSON 字符串
    "liquidityNum": 1234.56,
    "volumeNum": 99999.0,
    "endDate": "2026-11-05T00:00:00Z",
    "active": True,
    "closed": False,
    "negRisk": False,
    "negRiskMarketID": None,
}

# normalize_market() 之后（我们的 row dict）
{
    "market_id": "516542",
    "condition_id": "0xabc...",
    "slug": "trump-2026",
    "question": "Will Trump...",
    "yes_token_id": "7132...",     # ← 解了 JSON 字符串后取 [0]
    "no_token_id": "8924...",      # ← 解了 JSON 字符串后取 [1]
    "mid_price": 0.46,             # ← 解了 outcomePrices 后 [0]，转 float
    "liquidity_usd": 1234.56,
    "volume_usd": 99999.0,
    "best_bid_price": None,        # ← 占位，等 step 5 灌
    "best_bid_size": None,
    "best_ask_price": None,
    "best_ask_size": None,
    "end_time_ms": 1762300800000,  # ← ISO 转 epoch ms
    "active": True,
    "closed": False,
    "neg_risk": False,
    "neg_risk_market_id": None,
    "fetched_at_ms": None,         # ← step 5 灌
    "incomplete": False,           # ← layer2 可能改成 True
}
# step 5 灌完
# {..., "best_bid_price": 0.45, "best_bid_size": 100.0, "best_ask_price": 0.47, "best_ask_size": 200.0, "fetched_at_ms": 1730278800123, ...}

# sqlite_store._row_to_tuple(row, snapshot_id=42) 之后（INSERT 用的 tuple）
(
    "516542", "0xabc...", "trump-2026", "Will Trump...", "7132...", "8924...",
    0.46, 1234.56, 99999.0, 0.45, 100.0, 0.47, 200.0, 1762300800000,
    1, 0, 0, None, 1730278800123, 42, 0
)
# bool 已经被转成 0/1，snapshot_id 注入到第 20 个位置，column 顺序严格匹配 MARKETS_COLUMN_ORDER
```

## 三个对齐契约的实战意义

代码：`storage/schemas.py:79`

```python
# 这个常量就是真理
MARKETS_COLUMN_ORDER: tuple[str, ...] = (
    "market_id", "condition_id", "slug", "question",
    "yes_token_id", "no_token_id", "mid_price",
    "liquidity_usd", "volume_usd",
    "best_bid_price", "best_bid_size", "best_ask_price", "best_ask_size",
    "end_time_ms", "active", "closed", "neg_risk", "neg_risk_market_id",
    "fetched_at_ms", "snapshot_id", "incomplete",
)
```

加一列怎么办（Phase 2 大概率会加）：

1. 改 `DDL` 的 CREATE TABLE markets（加列、加默认值）
2. 改 `MARKETS_COLUMN_ORDER` 加字段名（位置必须和 DDL 一致）
3. 改 `MARKETS_INSERT_SQL` 加列名 + 多一个 `?` 占位
4. 改 `SNAPSHOT_SCHEMA` 加 `pa.field(...)`（Parquet 端）
5. 改 `normalizer.py` 让 normalize 出的 dict 包含新字段
6. 跑测试 → 4 个文件改齐才能过

⚠️ 漏改任何一处 → 静默 bug：可能字段错位、可能 schema 校验失败、可能行写不进去。
**正因为这套契约严格，所以不会半截错** —— 漏改了 ColumnOrder 但没改 INSERT SQL，pyarrow 或 SQLite 立刻报错，不会让坏数据落库。

## 代码地图

| 文件 | 关键内容 |
|---|---|
| `src/polyarb/storage/schemas.py` | DDL + MARKETS_COLUMN_ORDER + MARKETS_INSERT_SQL + SNAPSHOT_SCHEMA |
| `src/polyarb/snapshot/normalizer.py` | normalize_market() —— 唯一造 row dict 的地方 |
| `src/polyarb/storage/sqlite_store.py:34` | `_row_to_tuple(row, snapshot_id)` —— 把 dict 投影成 tuple |

## 自检题

1. 内存里的 row dict 有 20 个 key。SQLite markets 表有 21 列。多出来的那一列是哪个，谁来填？
2. Parquet 比 SQLite 多了哪一列？为什么 SQLite 不需要它？
3. 如果我想给 market 加一个 `category: str` 字段（"politics" / "crypto" / ...），需要改哪几个文件？
4. `incomplete` 这一列什么时候会被设成 1？
5. 为什么 Parquet 用 `pa.string()` 存 token_id 而不是 `pa.int64()`？

## FAQ 增量

_暂无_

---

← [02-snapshot-pipeline.md](02-snapshot-pipeline.md) | 下一节 → [04-validator-layers.md](04-validator-layers.md)
