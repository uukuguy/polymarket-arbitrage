# 02 — 一次 snapshot 的完整旅程

## 核心心智模型

`make snapshot-markets` 是一个 7 步流水线。每一步**只做一件事**，把上一步的输出作为输入。
失败不会让流水线整个崩 —— 失败被分类成 `Issue` 对象，流水线继续往下，最后写进数据库当作"这次快照里出了什么问题"的记录。

```
┌──────────────────────────────────────────────────────────────┐
│  1. Gamma 拉全量            list[dict]  ~50,000 raw markets   │
│           ↓                                                    │
│  2. Normalize + dedupe      list[dict]  ~48,000 unique rows   │
│           ↓                                                    │
│  3. Mode filter             list[dict]  ~17,000 (subset >$1k) │
│           ↓                                                    │
│  4. CLOB 批量拉             dict        books + prices         │
│           ↓                                                    │
│  5. Stamp + attach          mutate      把 best_bid/ask 灌回   │
│           ↓                                                    │
│  6. Validate (L1/L2/L4)     list[Issue] ghost_book / missing  │
│           ↓                                                    │
│  7. Persist                 Parquet 先 → SQLite 后             │
└──────────────────────────────────────────────────────────────┘
                ↓
            SnapshotResult
                ↓
            stdout: "OK | 17259 markets | mode=subset | 28229 issues | -> data/snapshots/..."
```

代码总入口：`src/polyarb/snapshot/orchestrator.py:97` `run_snapshot()`

## 入口长什么样

```
make snapshot-markets       # 子集模式（默认，liquidity > $1k 的市场）
make snapshot-markets-full  # 全量模式（所有市场，慢得多）
```

Makefile target 直接跑 `python -m polyarb.snapshot`，最终调到：

```python
# src/polyarb/snapshot/cli.py:58
result = asyncio.run(run_snapshot(settings, mode=mode))
```

## 7 步详解

### Step 1：Gamma 全量拉取

```python
# orchestrator.py:127
async with GammaClient(settings) as gamma:
    raw_markets = await gamma.fetch_all_active_markets()
```

- 翻页拉所有 `active=true&closed=false&archived=false` 的市场
- 返回**原始 dict 列表**（连 `clobTokenIds` 还没解 JSON 字符串）
- 失败：`Issue(layer=1, category=API_UNREACHABLE)`，`raw_markets = []`，继续往下
- live 实测一次拉 ~48,985 行，分 ~490 页（每页 100）

### Step 2：Normalize + Dedupe

```python
# orchestrator.py:146
markets = [m for m in (normalize_market(r) for r in raw_markets) if m is not None]

# 去重（Gamma 翻页有 ~4% market_id 重复）
seen_ids = set()
deduped = []
for m in markets:
    mid = m.get("market_id")
    if mid is None or mid in seen_ids:
        continue
    seen_ids.add(mid)
    deduped.append(m)
markets = deduped
```

- `normalize_market` 把原始 Gamma dict 变成我们的标准 row dict（见 [03-market-snapshot-shape.md](03-market-snapshot-shape.md)）
- 如果连 `id` 都没有 → 返回 `None` 被过滤掉（这种行无法落库，PK 都没法填）
- **去重**：live run #001 才发现的事 —— Gamma 翻页边界会重复 ~4%，不去重 SQLite UNIQUE 约束会回滚整个 snapshot。保留**第一次出现**那条。

### Step 3：Mode filter

```python
# orchestrator.py:168
if mode == "subset":
    target_markets = [
        m for m in markets
        if (m.get("liquidity_usd") or 0) > settings.liquidity_threshold_usd
    ]
else:
    target_markets = markets
```

- `subset`（默认）：只保留 `liquidity_usd > $1000` 的市场
- `full`：所有市场都进
- 然后把 target_markets 里每个市场的 `yes_token_id` 和 `no_token_id` 都收集起来，下一步要批量去问 CLOB

⚠️ 注意：filtered out（流动性低）的市场**留在 `markets` 列表里**。它们不会被持久化，但 Layer 1 count 校验还要用它（"Gamma 报了 N 个，我 normalize 后剩 N 个吗"）。

### Step 4：CLOB 批量拉

```python
# orchestrator.py:188
clob = ClobReaderClient(settings)
try:
    books = await clob.get_books(token_ids)                # 订单簿
    prices = await clob.get_prices_buy_sell(token_ids)     # buy/sell 现价
    prices_buy = prices.get("buy", {})
    prices_sell = prices.get("sell", {})
    books_by_token = _index_books_by_token(books)          # list → dict[tid, book]
except Exception as e:
    issues.append(Issue(layer=4, category=API_UNREACHABLE, ...))
```

- `get_books` 一次最多 500 个 token，内部分块循环
- `get_prices_buy_sell` 同样分块，BUY 和 SELL 是**两次独立的 SDK 调用**
- `_index_books_by_token` 把 list of `OrderBookSummary` 转成 `{token_id: book_dict}` 方便后面查
  - **关键**：`OrderBookSummary.asset_id` 是 token_id（不是 `market` —— `market` 是 conditionId）。这是 live run 经验事实。

### Step 5：Stamp + Attach top-of-book

```python
# orchestrator.py:219
clob_done_ms = int(time.time() * 1000)
for m in target_markets:
    m["fetched_at_ms"] = clob_done_ms        # 时间戳

    tid = m.get("yes_token_id")
    if not tid or tid not in books_by_token:
        continue
    book = books_by_token[tid]
    asks = book.get("asks") or []
    bids = book.get("bids") or []

    if asks:
        try:
            m["best_ask_price"] = float(asks[0]["price"])
            m["best_ask_size"] = float(asks[0]["size"])
        except (KeyError, TypeError, ValueError, IndexError) as e:
            issues.append(Issue(layer=4, category=UNKNOWN, ...))
    if bids:
        try:
            m["best_bid_price"] = float(bids[0]["price"])
            m["best_bid_size"] = float(bids[0]["size"])
        except ...:
            issues.append(...)
```

- 给每个市场打 `fetched_at_ms` 时间戳（这是我们 snapshot 一致性的近似时间）
- 把 best bid / best ask 从 book 里抽出来灌回市场 dict
- ⚠️ Phase 1 简化：**只灌 YES 这一边**。NO 在 Polymarket 是对称的（NO_price ≈ 1 - YES_price），暂时不存。Phase 2/3 策略需要 NO 时再加
- ⚠️ 每个 `float()` 都包 try/except —— 这是 F-1 安全约束（攻击者控制的 CLOB 字段不能让流水线崩），见 [06-security-invariants.md](06-security-invariants.md)

### Step 6：Validate

```python
# orchestrator.py:265
issues.extend(layer1_count(gamma_count_reported, len(markets)))
issues.extend(layer2_fields(target_markets, now_ms=taken_at_ms))
issues.extend(layer4_cross_source(target_markets, books_by_token, prices_combined))

is_valid = is_valid_overall(issues)
```

- 三层校验，每层产出 `Issue` 列表，合并起来
- `is_valid` **只看 Layer 1**（count 不一致）—— Layer 2/4 的 issue 仅记录、不影响 valid 标志
- 详细见 [04-validator-layers.md](04-validator-layers.md)

⚠️ Layer 4 期望的 `prices_combined` 形状是 `{tid: {"buy": "0.46", "sell": "0.47"}}`，但 CLOB SDK 给的是 `{tid: {"BUY": "0.46"}}` 嵌套两层 —— `orchestrator.py:281` 的 `_unwrap_side` 就是干这事的。Wave 3 集成时这个 bug 卡过我们一次。

### Step 7：Persist（先 Parquet 后 SQLite）

```python
# orchestrator.py:303
parquet_path = compute_snapshot_path(settings.parquet_root, taken_at_ms)
# data/snapshots/2026/04/29/14-23-05.parquet (UTC)

parquet_rows = [...]  # 给每行加 snapshot_taken_at_ms / snapshot_id 占位
write_parquet_atomic(parquet_rows, parquet_path)

store = SQLiteStore(settings.db_path)
store.init_schema()
snapshot_id = store.write_snapshot(...)
```

**为什么先 Parquet 后 SQLite？**

如果先 SQLite 后 Parquet，SQLite 提交后 Parquet 失败，我们就有了"行已落库但归档丢失"的状态 —— 数据库说"这次快照存档在 X 路径"，但 X 文件不存在。
反过来：先写 Parquet 再写 SQLite，如果 Parquet 写完 SQLite 失败 → 我们有一个孤儿 .parquet 文件，但数据库不认它，下次 snapshot 时这个文件被覆盖（因为路径用 timestamp 不会冲突）或者直接被忽略。**孤儿文件比孤儿数据库引用安全得多**。

**Parquet 原子写**：先写 `.parquet.tmp`，成功后 `os.replace(tmp, final)`。POSIX/Windows 都保证 replace 原子性。
代码：`storage/parquet_writer.py:41` `write_parquet_atomic()`

**SQLite 原子写**：单事务里 `BEGIN IMMEDIATE → DELETE FROM markets → INSERT 新 snapshot 元数据 → executemany INSERT 所有 rows → executemany INSERT 所有 issues → COMMIT`。任何异常 ROLLBACK 整个事务。
代码：`storage/sqlite_store.py:77` `write_snapshot()`

⚠️ `DELETE FROM markets` —— 这是 D-C1 决策：`markets` 表是**当前快照的镜像**，不是历史。历史用 Parquet。每次 snapshot 整表重写。

## 最终输出形状

`run_snapshot` 返回 `SnapshotResult`：

```python
@dataclass
class SnapshotResult:
    snapshot_id: int                       # SQLite 自增主键
    market_count: int                      # 落库的 market 数（target_markets 大小）
    is_valid: bool                         # 仅看 Layer 1
    mode: str                              # "subset" | "full"
    issue_count: int                       # 总 issue 数
    issue_categories: dict[str, int]       # {"ghost_book": 24949, ...}
    parquet_path: Path
    taken_at_ms: int
    finished_at_ms: int
```

CLI 把它打成一行 stdout：
```
OK | 17259 markets | mode=subset | 28229 issues | -> data/snapshots/2026/04/29/14-23-05.parquet
```

## 失败语义（D-D3 / D-E2）

**所有 transport 失败都不抛**：被分类成 `Issue` 后流水线继续。

| 场景 | 结果 |
|---|---|
| Gamma 整个不通 | `raw_markets = []`，markets 也是 []，target_markets 也是 []，CLOB 不调（空 token list），最后落空 snapshot 但 is_valid=False（Layer 1 count 0 vs 0 实际是 valid，但 Gamma 报错本身已经 push 了 API_UNREACHABLE issue） |
| CLOB 不通 | books / prices 都是空，所有市场都拿不到 best_bid/ask，Layer 4 全报 CLOB_MISSING |
| 个别 book 字段格式怪 | F-1：包了 try，不崩，记成 Issue(UNKNOWN) |
| Layer 1 count 不一致 | is_valid=False，process exit 1，但**仍然落库**（D-D3：失败也要可查） |

CLI 看 `result.is_valid`：False 时打 stderr 失败摘要 + `exit(1)`。
但**数据已经写到 SQLite + Parquet 了**。`make` 会因为 exit 1 报错，但这是"categorized success"，不是数据丢失。

## 代码地图

| 文件 | 行数 | 干什么 |
|---|---|---|
| `src/polyarb/snapshot/orchestrator.py` | 347 | 7 步流水线主体 |
| `src/polyarb/snapshot/cli.py` | 80 | typer CLI 包装 + stdout/stderr 格式 |
| `src/polyarb/snapshot/normalizer.py` | 139 | Step 2 normalize |
| `src/polyarb/storage/sqlite_store.py` | 160 | Step 7 SQLite 原子写 |
| `src/polyarb/storage/parquet_writer.py` | 64 | Step 7 Parquet 原子写 |
| `src/polyarb/validator/layers.py` | 280 | Step 6 三层校验 |

## 设计取舍（你需要知道的）

1. **Gamma 失败不阻断 CLOB 调用** —— 但因为 markets=[]，CLOB 也没东西可问，自然降级。
2. **Layer 2 会原地修改 market dict 加 `incomplete=True`** —— "标记，不删除"。下游策略可以选择跳过 incomplete，但数据保留在 SQLite。
3. **`fetched_at_ms` 是 best-effort，不是 transactional** —— Step 5 给所有 target_markets 打同一个时间戳（CLOB 完成那一刻），不是每个市场各自的精确时间。Phase 1 这样够用；Phase 2 WebSocket 增量需要更精确。

## 自检题

1. 如果第 4 步（CLOB）失败，第 7 步会写什么进 SQLite？
2. 我有一个 market 流动性 $500（subset 阈值之下），它会不会进 SQLite？会不会进 Parquet？Layer 1/2/4 校验它吗？
3. `is_valid=False` 的时候，数据落库了吗？怎么查这次的 issues？
4. 为什么 Step 7 一定先 Parquet 后 SQLite，反过来不行？

## FAQ 增量

### Q: 为什么默认没有进度条？我跑 `make snapshot-markets` 看不到任何中间状态。

**A**: Phase 1 设计时只考虑了"cron 友好"场景（D-F1：单行 stdout summary 给 grep 用），漏掉了"用户手动等"场景。

**短期补救**（已落实）：

```bash
make snapshot-markets-v       # 加 --verbose，每个阶段开始/结束都打 INFO 日志
make snapshot-status          # 另开终端，一条命令告诉你：
                              #   - 当前是否有 snapshot 进程在跑（PID + elapsed）
                              #   - 最近 5 次 SQLite 记录（本地时间）
                              #   - 最新 Parquet 文件路径 + 写入时间
```

`snapshot-markets-v` 是新加的 target；旧 `snapshot-markets` 保留（cron 仍然安静）。

**待补**（CLOB chunk 缓存 + 阶段 timing + ETA）：见下面"为什么不能续传"。

### Q: 为什么不能续传？跑 26 分钟中途断了就全白跑。

**A**: 这是 Phase 1 的真实爆作点，根因在 D-C1：

> markets 表是当前快照镜像，**必须原子覆盖** — 在内存里组装好整份再 DELETE+INSERT。

CONTEXT.md 写决策时估计市场数 ~1k（30 秒跑完，重跑无所谓）；live run 才发现真规模 17k（subset），跑一次 ~26 分钟。**规模假设错 → 原子性策略的代价从"30 秒"放大到"26 分钟"**。

**修复方案（已落地）**：CLOB chunk 增量缓存（`src/polyarb/snapshot/cache.py`）

```
data/.cache/snapshot-{taken_at_ms}/
├── meta.json                          # 指纹（settings / tokens / mode / created_at）
├── books/chunk-NNN.json               # 每个 CLOB chunk 拉完立刻落盘（list[dict]）
└── prices/{buy,sell}/chunk-NNN.json   # {token_id: {"BUY"|"SELL": "<price>"}}
```

启动时四步检查（任一不满足 → 删旧 cache 重跑）：
1. cache 目录存在？
2. `settings_fingerprint` 匹配（CLOB url / batch size / liquidity threshold）？
3. `token_ids_fingerprint` 匹配（target token list 排序后的 sha256）？
4. `created_at_ms` 在 30 分钟内？

匹配 → `cache.has_books_chunk(i)` / `cache.has_prices_chunk(side, i)` 跳过已完成 chunk。
step 7 SQLite commit 成功后，`cache.cleanup()` 删除该 cache 目录。
step 7 失败（罕见）→ cache 保留供下次复用。

**新 Makefile target**：

```bash
make snapshot-markets         # 默认走 cache（cron 友好，安静）
make snapshot-markets-v       # 详细模式（人盯着等，每 chunk 打 INFO）
make snapshot-fresh           # 强制重跑：清所有 cache + verbose
make snapshot-cache-purge     # 只清 cache，不跑 snapshot
make snapshot-status          # 一条命令看：进程 / 最近 SQLite / 最新 Parquet（本地时间）
```

**`--no-cache` flag**（CLI 直调）：

```bash
python -m polyarb.snapshot --no-cache --verbose
```

启动时清 `cache_root` 下所有 `snapshot-*` 目录，本次跑也不写 cache。

**chunk progress 现在默认可见**：从 `logger.debug` 升到 `logger.info`，无需 `-v` 也能看到 `CLOB books chunk 24/70: fetched (500 tokens)` 或 `CLOB books chunk 24/70: cached (498 books)`。

**Gamma 翻页进度**（LIVE-RUN-003 暴露：原版 Gamma 翻页 3-5 分钟期间零输出，无法判断"慢 vs hang"）：

```
INFO    | snapshot starting — mode=subset, cache=on, taken_at_ms=...
INFO    | Phase 1/7: Gamma fetch (active markets)
INFO    | Gamma: starting paginated fetch (page_limit=100)
INFO    | Gamma: page 1 fetched (100 markets so far)        ← 启动 30 秒内必现
INFO    | Gamma: page 50 fetched (5000 markets so far)      ← 每 50 页一行（~30 秒间隔）
INFO    | Gamma: page 100 fetched (10000 markets so far)
...
INFO    | Gamma fetched 48985 active markets in 490 pages (final)
INFO    | Phase 2/7: Normalize + dedupe
...
```

每个 step 也都打 `Phase N/7:` banner（对应教学文档的 7 步划分）。失败/卡住时**最后一行**直接告诉你"卡在第几页 / 第几个 chunk"，不再黑屏猜进度。

**测试覆盖**：`tests/m1-perception/test_snapshot_cache.py` 20 个 case
- 指纹稳定性 + drift 检测（settings / tokens / mode / age / corrupted meta）
- chunk save/load roundtrip（dict + dataclass-like SDK 对象）
- cleanup / purge_all 行为

### Q: 这次踩坑的 meta lesson 是什么？

**A**: 三条：

1. **CONTEXT.md 决策时的"假设规模"必须显式记录**。Phase 1 D-C1 没记"假设市场数 ~1k"，所以 live run 看到 17k 时，没人意识到原子性策略的代价已经爆。Phase 2 起每个有性能/原子性 trade-off 的决策必须写"假设上限"。

2. **可观测性是工程必需品，不是奢侈品**。"用户手动跑 + 没进度条 + 没 status 命令" 让 Claude 自己也搞错了 3 次状态判断（把别次的 SQLite 记录当成本次的）。可观测性差不只是 UX，是状态混乱的根源。

3. **make 是壳子不是流水线**。Phase 1 把 `make snapshot-markets` 当成"一条命令搞定"，但实际是长任务。需要配套 `make snapshot-status` / `make snapshot-resume`，才算"工具"。下一次给任何 ≥30 秒的 make target，必须**同时**给配套的 status / cancel / resume target（如果适用）。


---

← [01-polymarket-data-sources.md](01-polymarket-data-sources.md) | 下一节 → [03-market-snapshot-shape.md](03-market-snapshot-shape.md)
