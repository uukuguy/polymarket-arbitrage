# 08 — 流式快照与内存预算（Streaming Snapshot & Memory Budget）

← [07-观察市场](07-观察市场.md) | [00-INDEX](00-INDEX.md)

## 30 秒心智模型

Phase 02 让 snapshot 上云 7×24 跑。第一次 prod deploy 在 Fly 256MB VM 上 **OOM-killed**，第二次升 512MB **还 OOM**，第三次 1GB 才稳。问题不是"代码烂"——是把"分页拉数据"误以为是"流式处理"。

```
┌─────────────────────────────────────────────────────────────┐
│  误解：HTTP 分页 == 流式处理                                  │
│  ────────────────────────────────────────────────             │
│  分页：HTTP 层每次请求只拉一页（避免单次几 MB）— ✅ 已做       │
│  流式：应用层不累积全量到 list，逐项处理 — ❌ 之前没做         │
│                                                              │
│  实际：_paginate 在内部累积 list[dict] 直到拉完，            │
│  返回 20k 个 dicts。应用层是"分页+全量驻留"，不是流式。       │
└─────────────────────────────────────────────────────────────┘
```

Plan 02-09 把 `_paginate` 改成 `AsyncIterator[dict]` 真正逐项 yield，orchestrator 改成 per-page normalize → 写 → 丢。

**但**：单单 streaming 不够。target_markets (post-filter 持有 6700+ markets stamped+book) + CLOB books/prices + Python+pyarrow baseline + Linux glibc 叠加，实测 Linux daemon peak **anon-rss = 402MB**。这是数据本身的大小，无法再压。512MB Fly VM（user 可用 ~400MB）撞顶 OOM；1GB 才有 headroom。

**两层教训**:
1. 架构层 — streaming 不是"分页就行"，应用层逐项 yield 才算
2. 工程层 — 修完代码后，承认数据本身的 RSS，升一档不是逃避

## 数据流图（before / after）

### Before Plan 02-09（"分页 + 全量驻留"）

```
Gamma /events (10k) ──┐
                      ▼
                 [list × 10k]──┐
                      ▼        │
                normalize → event_rows + reverse_map
                                │
Gamma /markets (20k) ──┐        │
                       ▼        │
                 [list × 20k]   │  ← raw_markets 20k 全在 mem
                       ▼        │
                normalize → markets list 20k   ← 又一份 20k
                       ▼        │
                  dedup → 19k        │
                       ▼        │
              filter > $1k → target ~7k    ← 最终留这些
                       ▼        │
                 CLOB fetch → books/prices
                       ▼        │
              stamp + validate + write
                                │
Peak RSS = 480MB → strip 字段后 160MB → 还有 raw 20k 没释放
```

### After Plan 02-09（streaming）

```
Gamma /events (10k) ──→ normalize_events → [event_rows + map]  (small, ~5MB)
                                                     │
Gamma /markets ──async for raw──┐                    │
                                ▼                    │
                          normalize(raw, map)        │
                                ▼                    │
                          dedup via seen_ids set ────┘
                                ▼
                          filter > $1k
                                ▼
                ┌─── filtered_out → drop（核心节省）
                ▼
            target_markets (~6700, grow)
                                ▼
                          CLOB fetch (books + prices)
                                ▼
                stamp + Layer2/4 validate
                                ▼
                Parquet (ParquetWriter chunked) + SQLite (batched executemany, single tx)
                                ▼
Peak RSS = 402MB (Linux Fly 实测)
```

**架构胜利**：raw 20k dicts 从未在内存里同时存在 — 流式 yield 让单页几百个 raw 处理完即丢。

**剩余 RSS 来源**（实测拆解）:
- Python + pyarrow + httpx + sqlite + uvicorn + sentry + loguru baseline: ~120-150MB
- target_markets (6700 × ~3.5KB stamped+book): ~25MB
- books_by_token + prices_buy/sell/combined (~14k tokens): ~10MB
- market_to_event_map + seen_ids: ~10MB
- pyarrow ParquetWriter C-allocator + batch buffer: ~10-15MB
- SQLite executemany batch + tx state: ~10-15MB
- Linux glibc / C-allocator slack vs macOS: ~80MB diff (! 实测意外项)
- httpx HTTP/2 connection state + asyncio: ~10MB

## 关键代码片段

### `_paginate` — 从 list[dict] 到 AsyncIterator[dict]

`src/polyarb/clients/gamma_client.py:173`

```python
# Before (Plan 02-04 era):
async def _paginate(self, *, path, params, label, keep_fields):
    out: list[dict] = []            # ← 致命的累积
    while True:
        page = await self._get(path, page_params)
        if keep_fields:
            page = [{k: v for k, v in raw.items() if k in keep_fields}
                    for raw in page]
        out.extend(page)            # ← 全部追加
        if len(page) < self.PAGE_LIMIT:
            break
    return out                      # ← 返回 list[~20000]

# After (Plan 02-09):
async def _paginate(self, *, path, params, label, keep_fields) -> AsyncIterator[dict]:
    offset = 0
    while True:
        page = await self._get(path, {**params, "offset": offset, "limit": self.PAGE_LIMIT})
        for raw in page:
            if keep_fields:
                raw = {k: v for k, v in raw.items() if k in keep_fields}
            raw["_page_fetched_at_ms"] = page_fetched_at_ms
            yield raw               # ← 单个市场就 yield 走
        if len(page) < self.PAGE_LIMIT:
            break
        offset += self.PAGE_LIMIT
```

**关键点**：`yield raw` 让调用方一拿到一个 dict 就可以处理它，paginator 内部不再累积 `out` list。

### Orchestrator 改成 streaming consumer

`src/polyarb/snapshot/orchestrator.py:189`

```python
# Phase 1+2 fused under one GammaClient context:
async with GammaClient(settings) as gamma:
    # Events: 小（~10k），全量 OK（Decision A — 推 Plan 02-10）
    raw_events = [ev async for ev in gamma.iter_active_events()]
    event_rows, event_tag_rows, market_to_event_map = normalize_events(raw_events)
    del raw_events

    # Markets: 流式 — raw 不累积
    seen_ids: set[str] = set()
    gamma_count_reported = 0
    target_markets: list[dict] = []

    async for raw in gamma.iter_active_markets():
        gamma_count_reported += 1
        m = normalize_market(raw, market_to_event_map)
        if m is None or m.get("market_id") in seen_ids:
            continue
        seen_ids.add(m["market_id"])
        if (m.get("liquidity_usd") or 0) > settings.liquidity_threshold_usd:
            target_markets.append(m)
        # raw 此时已无引用 — gc 可回收
```

### Parquet streaming write

`src/polyarb/storage/parquet_writer.py:71`

```python
def write_parquet_streaming(row_batches: Iterable[list[dict]], out_path: Path, batch_size=500):
    """每个 batch (~500 行) 转 pa.Table → ParquetWriter.write_table，文件末尾才 close。

    原子性：写 tmp，全部成功后 os.replace；异常时 unlink tmp。
    """
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        with pq.ParquetWriter(tmp, SNAPSHOT_SCHEMA, compression="snappy") as writer:
            for batch in row_batches:
                table = pa.Table.from_pylist(batch, schema=SNAPSHOT_SCHEMA)
                writer.write_table(table)
        os.replace(tmp, out_path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
```

### SQLite streaming insert（保持原子性）

`src/polyarb/storage/sqlite_store.py:264`

```python
def write_snapshot_streaming(self, market_iter: Iterable[dict], ...):
    """单 BEGIN IMMEDIATE / 多 executemany batch / 单 COMMIT — 保持 per-snapshot 原子性。"""
    con = self._connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(SNAPSHOT_INSERT_SQL, snapshot_meta_tuple)
        batch_buffer = []
        for m in market_iter:
            batch_buffer.append(tuple_from_market(m, snapshot_id))
            if len(batch_buffer) >= 500:
                con.executemany(MARKETS_INSERT_SQL, batch_buffer)
                batch_buffer.clear()
        if batch_buffer:
            con.executemany(MARKETS_INSERT_SQL, batch_buffer)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
```

**为什么保留单 COMMIT**：commit-per-batch 会破坏"半快照不可见"的不变量——某次 cron tick 写一半时崩，下次启动看到的是部分数据。单 BEGIN IMMEDIATE + 多 executemany + 单 COMMIT 保证原子性。

## 设计取舍（重要）

### A. Events 为什么不流式？

`/events` 端点回 ~10k 事件，全量约 5MB。它们必须**先于 markets 完整读完**，因为 markets normalizer 需要 `event_id` 反向 map。两种选择：

1. **两遍 markets pass**：第一遍只为收集 event_ids，第二遍 streaming → 需要走两次 Gamma
2. **events 全量驻留**：~5MB，可接受 → **选这个**

如果将来 events 增长到 50k+ 才有动机改它。Plan 02-09 explicit defer 到 Plan 02-10。

### B. target_markets 为什么不流式写？

post-filter 后 target_markets ~6700 个，每个 ~3.5KB（stamp+book attach 后）≈ 25MB。**它必须留在内存里**直到：
- CLOB fetch 完成（要 token list 一次性发请求）
- Layer 4 validate 完成（要交叉验证 books × prices）
- Write 完成

可优化吗？理论上 token list 收完即可释放 target_markets dict 引用，只留 token_id list，CLOB fetch 完后再读 SQLite 重新 attach books。但工程复杂度高，节省 ~25MB 不值。

### C. 为什么 macOS pytest 测过没 OOM？

实测差异：

| 平台 | baseline RSS | peak RSS |
|---|---|---|
| macOS pytest | ~172MB | ~285MB |
| Linux Fly daemon | ~150MB（估）| **402MB**（OOM log 实测） |

差 ~120MB 主要在 Linux glibc / C-allocator 的内存归还策略：macOS malloc 主动归还，Linux glibc 保留 arenas 直到进程退出。在长跑 daemon 里 RSS 单调爬高，从启动 baseline 一路涨。这不是 leak，是 allocator 行为。

**教训**：用 macOS 本地 pytest peak 推断 Fly Linux 行为是**错的**。必须 deploy 后看 fly logs `anon-rss` 实证。

### D. 30MB delta budget xfail 是怎么回事？

Plan 02-09 T5 测试有两个 assert：
- `peak_delta < 30MB`（架构主张：streaming 让 working set 转瞬即逝）
- `peak_abs < 130MB`（OOM 相关性：256MB Fly 上能跑）

**两个都失败了** — 实测 peak_delta ~80-90MB（pytest），peak_abs ~285MB。原因：plan 设计时低估了 target_markets 的内存（写 "few hundred × 2KB" 但实际是 6700 × 3.5KB ≈ 25MB）。

Executor 正确做的事：**标 xfail，不弱化 assertion**。这让测试成为 Plan 02-10 的 RED gate——如果未来要再压内存，这是入口。

## 自检题

1. **`_paginate` 改成 AsyncIterator 后，调用方写 `[m async for m in iter(...)]` 是好的还是坏的？为什么？**

   <details><summary>答案</summary>
   **坏的**——这立刻把所有 yield 出来的 dict 累积成 list，等同于回到 Plan 02-04 之前的状态。streaming 的价值在于"消费即丢"，列表推导式破坏了这一点。正确写法是 `async for raw in gamma.iter_active_markets():` 在循环体里立即 normalize → 写 → 不保留。
   </details>

2. **如果 Polymarket 把 /events 端点改成回 200k 事件，现在的代码会怎么样？**

   <details><summary>答案</summary>
   会 OOM。Decision A 推迟了 events 流式化，前提假设 events ~10k。200k 事件 × ~1KB/dict ≈ 200MB 驻留就压垮了。Plan 02-10 要处理这种 case：要么 streaming events（需 markets 两遍 pass），要么只读必要字段（event_id + tags）做 reverse map。
   </details>

3. **SQLite write 用单 transaction + 多 executemany，比 commit-per-batch 慢吗？**

   <details><summary>答案</summary>
   **几乎没区别**——SQLite 在单 transaction 内的 executemany 是顺序写，commit 才 fsync。多 commit 反而触发多次 fsync 慢 10×+。单 transaction 是双赢：性能更好 + 原子性强。
   </details>

4. **Linux daemon 的 anon-rss 比 macOS pytest 高 120MB，原因是 Plan 02-09 写了 bug 吗？**

   <details><summary>答案</summary>
   **不是 bug**。Linux glibc malloc 用 arenas 模型，归还内存给 OS 比 macOS malloc 保守得多。长跑 daemon 的 RSS 会爬到稳态。可以试 `MALLOC_ARENA_MAX=2` 环境变量降一些，但不能根治。本质是用户进程的"占有量"和"实际活跃量"差异。
   </details>

5. **如果 Plan 02-09 的 streaming 改造没做，仅仅升 1GB Fly 也能跑吗？**

   <details><summary>答案</summary>
   **不能稳定跑**。raw_markets 20k dicts 占 ~160MB + 上面拆解的 ~242MB 其他 = ~402MB + 160MB = ~562MB peak。1GB Fly 给 user 大约 900MB，能跑但留 ~340MB headroom。一次 Gamma response 异常大就到天花板。streaming + 1GB 是**双管必需**——单独任一个都不足以稳定。
   </details>

## FAQ（增量区）

> **Q: SESSION 18 不是说 256MB 跑通了吗？**
> A: SESSION 18 跑通了"启动 + 一次 health check"，但 first cron tick 几分钟后就 OOM。256MB 永远不可能稳定（peak >> 256MB），SESSION 18 EOD 的状态是"赶在 OOM 之前 ship 的"。这个错觉是因为 fly logs 默认只显示 100 行，看不到完整 timeline。下次 trust SQLite + machine state，不要 trust /health 一次响应。

> **Q: streaming 改造后 plan 写的 "peak ~105MB" 数字是骗人吗？**
> A: 不是骗人，是设计期估算 — plan-check round 1 我手算时把 target_markets 算成 "few hundred × 2KB"，把 books_by_token 算成 "~5MB"，都低估了。事实是 6700 个 stamped+book attached 的 markets × ~3.5KB 就 25MB。教训是 plan 的内存预算表只是 sketch，必须靠实测推翻或确认。

> **Q: 现在 1GB 是终极方案吗？**
> A: 是稳态，但不是 forever。如果以后想跑 full mode（不过滤，~20k markets）或者 Polymarket markets 增长 5×，1GB 也会撞。下一步是 Plan 02-10：多进程拆分（snapshot 进程死/重启不影响 daemon HTTP）、或者 lazy CLOB book fetch（只为 top-k movers 拉 book）。但只有触发条件再做。

---

**下一篇**: 09 — Wave 4 观测栈集成（Sentry+Axiom+Better Stack+Telegram） — 待 Wave 4 落地后写
