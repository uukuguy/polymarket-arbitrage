# 06 — 代码安全约束（F-1 ~ F-8）

## 核心心智模型

代码里到处出现 `# F-1 SECURITY:` `# F-2 SECURITY:` 这样的注释。这是 Phase 1 SECURITY-REVIEW.md 评出的**8 条约束**，每条都是"如果不这么做，会发生什么具体的安全/可靠性问题"的对应。

不是 paranoia，是写完一遍 → 安全审计员（gsd-security-auditor agent）找出来的真问题 → 我们补上的。

存档在：`.planning/workstreams/m1-perception/phases/01-market-snapshot/01-SECURITY-REVIEW.md`（含每个 F 的根因 + 选定方案 + 替代方案）

## 8 条约束概览

| 编号 | 约束名 | 严重性 | 主要在哪 |
|---|---|---|---|
| F-1 | 攻击者控制的 float 解析必须容错 | HIGH | orchestrator step 5 + validator layer 4 |
| F-2 | follow_redirects=False + MAX_PAGES 上限 | HIGH | gamma_client |
| F-3 | 路径校验（防止 .. 越权） | MED | config 加载 |
| F-4 | fixture 数据卫生（脱敏） | MED | tests/fixtures |
| F-5 | issue detail/payload 截断（DB 防胀） | MED | validator + orchestrator |
| F-6 | 4xx 不重试 + JSONDecodeError 不重试 | LOW | gamma_client |
| F-7 | lockfile（防多进程并发写库） | DEFER | Phase 5+ |
| F-8 | 时区显式 UTC（不当本地时间） | LOW | normalizer endDate 解析 |

## F-1 — float 解析必须容错（HIGH）

**问题**：CLOB 返回的 book 是攻击者可控字段。一个恶意 / 损坏的 response 可能让 `bids[0]["price"]` 是 `"NaN"` 或缺失字段或 `null`，让 `float(...)` 抛异常崩掉整个 snapshot 流程。

**约束**：每一个 `float(可疑字段)` 都包 try/except，失败记成 `Issue(layer=4, UNKNOWN)` 而不是抛。

**实战代码**：

```python
# orchestrator.py:236
if asks:
    try:
        m["best_ask_price"] = float(asks[0]["price"])
        m["best_ask_size"] = float(asks[0]["size"])
    except (KeyError, TypeError, ValueError, IndexError) as e:
        issues.append(Issue(
            layer=4,
            category=Category.UNKNOWN,
            market_id=m.get("market_id"),
            detail=f"unparseable ask for {tid}: {str(e)[:200]}",
            raw_payload=json.dumps(book, default=str)[:500],
        ))
```

```python
# validator/layers.py:58
def _safe_float(v: Any) -> float | None:
    try:
        return float(v)
    except (KeyError, TypeError, ValueError):
        return None
```

⚠️ 注意 catch 的异常类型 —— 不是裸 `except Exception:`（会吞 KeyboardInterrupt 之类的），是显式列出可能发生的几种。

## F-2 — follow_redirects=False + MAX_PAGES（HIGH）

**问题 1**：httpx 的 `follow_redirects` 当前默认是 False，但这是 **httpx 的当前默认**，未来版本可能翻转。一个被劫持的中间人或 SSRF 攻击可以让 Gamma 重定向到内部地址。

**约束**：显式指定 `follow_redirects=False`，不依赖默认值。

```python
# clients/gamma_client.py:79
self._http = httpx.AsyncClient(
    timeout=settings.http_timeout_s,
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    headers={"User-Agent": "polyarb/0.1"},
    http2=True,
    follow_redirects=False,                     # ← 显式
)
```

**问题 2**：`fetch_all_active_markets()` 是 while True 翻页循环。如果 Gamma（被攻击或 bug）一直返回满页（每页都是 `len(page) == PAGE_LIMIT`），循环不会退出，OOM。

**约束**：`MAX_PAGES = 1000` 上限。Polymarket 实际有 ~5 万 market = 500 页，1000 远高于现实，但守住了"无限循环"的失败模式。

```python
# clients/gamma_client.py:65
PAGE_LIMIT = 100
MAX_PAGES = 1000

# fetch_all_active_markets:168
if pages_fetched >= self.MAX_PAGES:
    raise RuntimeError(f"Gamma pagination exceeded {self.MAX_PAGES} pages — possible runaway response")
```

## F-3 — 路径校验（MED）

**问题**：YAML 配置里有路径字段（`db_path`, `parquet_root`）。如果用户给 `db_path: "../../../etc/passwd"` 我们会写到那里。

**约束**：config 加载时校验路径是否在项目预期目录之下。

代码在 `config.py`（83 行，自己读，pattern 是 `pathlib.Path` resolve 之后的前缀检查）。
```bash
make snapshot-markets    # 用项目内默认路径，安全
```
配置文件里改路径前 → 走校验。

## F-4 — Fixture 卫生（MED）

**问题**：tests/m1-perception/fixtures/ 下的 JSON fixture 是从真实 API 录制的（`record_fixtures.py`）。如果不脱敏，可能包含真实地址 / wallet 信息 / 任何 PII。

**约束**：录制工具有脱敏 hook；commit 前 review。
现状：`tests/m1-perception/fixtures/{gamma,clob}_sample.json` 是手工 review 过的。
未来录制新 fixture → 仍要走脱敏流程。

## F-5 — Issue 内容截断（MED）

**问题**：`Issue.detail` 和 `Issue.raw_payload` 入库进 SQLite TEXT 字段。如果 Polymarket 返回一个 10MB 的 question 字段或一个超长的 book 列表，`validation_issues` 表会被一条 issue 撑爆。

**约束**：硬截断。

```python
# validator/layers.py:53
_DETAIL_MAX_CHARS: int = 200
_RAW_PAYLOAD_MAX_BYTES: int = 1024
_BOOK_PAYLOAD_MAX_BYTES: int = 500

# 用法
detail = f"missing: {missing}"[:_DETAIL_MAX_CHARS]
raw_payload = json.dumps({...}, default=str)[:_RAW_PAYLOAD_MAX_BYTES]
```

## F-6 — 选择性不重试（LOW）

**问题**：tenacity 默认会无脑重试所有异常，但有些异常重试只浪费时间：
- 4xx (非 429)：客户端错误，重试解决不了
- `json.JSONDecodeError`：CDN 返回错误内容，重试通常一样错

**约束**：

```python
# clients/gamma_client.py:107
retry=retry_if_exception_type(
    (httpx.RequestError, httpx.HTTPStatusError, httpx.TimeoutException)
),
```

只重试特定类型。`_NonRetryableHTTPError` 包装 4xx 让 tenacity 见到不重试的异常类型。
`json.JSONDecodeError` 不在白名单里，自然不被重试。

## F-7 — Lockfile（DEFER 到 Phase 5+）

**问题**：如果同一时间两个 `make snapshot-markets` 进程并发跑，两者都试着 `DELETE FROM markets + INSERT`，SQLite 会处理事务冲突，但 Parquet 写可能撞文件名（同一秒）→ 一个被覆盖。

**约束（暂缓）**：Phase 1 只是单进程工具，user 自己控制不并发。引入 lockfile（fcntl.flock）的成本不值得 Phase 1 现在做。

**触发时机**：M5（工业化）做调度器时，或任何引入 wallet/auth 后台进程的 phase 进来时再补。SECURITY-REVIEW.md 里登记了。

## F-8 — 时区 UTC 显式（LOW）

**问题**：`normalize_market` 解析 `endDate: "2026-11-05T00:00:00Z"`。`Z` 后缀是 UTC 标记，但 `datetime.fromisoformat` 在 Python 3.11 之前不接受 `Z`，要替换成 `+00:00`。如果替换之后还得到 naive datetime（极端边缘），按 host local time 解读 → end_time_ms 会偏几个小时。

**约束**：替换 `Z`，naive datetime 强制当 UTC 处理。

```python
# snapshot/normalizer.py:56
def _parse_end_time_ms(raw: Any) -> int | None:
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)        # ← naive 默认 UTC
    try:
        return int(dt.timestamp() * 1000)
    except (OverflowError, OSError, ValueError):
        return None
```

## 这套约束告诉我们什么

1. **每个外部输入都是不可信的**。Gamma 返回的字段、CLOB 返回的 book、用户写的 YAML config —— 都按"可能被攻击 / 可能损坏"对待。
2. **失败模式优先于成功路径**。代码注释里"如果发生 X 怎么办"出现得比"实现 X 怎么做"还多。
3. **defer 是合理选择**，不是逃避。F-7 lockfile 推到 M5 不是漏洞，是基于 Phase 1 单进程现实的合理裁剪 —— 但**显式登记**了，未来不会忘。

## 代码地图

| 文件 | F 编号出现 |
|---|---|
| `clients/gamma_client.py` | F-2, F-6 |
| `clients/clob_client.py` | (无显式 F 标，但 try/raise 节奏遵循 F-1 思路) |
| `snapshot/orchestrator.py` | F-1, F-5 |
| `snapshot/normalizer.py` | F-1 (`_safe_float`), F-8 |
| `validator/layers.py` | F-1, F-5 |
| `config.py` | F-3 |
| `.planning/.../01-SECURITY-REVIEW.md` | 全部 8 条的根因 + 选定方案 + 替代方案 |

## 自检题

1. 我看到代码里写 `try: float(x) except (KeyError, TypeError, ValueError):` —— 为什么是这三种异常类型，不是裸 `except Exception`？
2. 如果 Gamma 真的有 50,000 个 market，PAGE_LIMIT=100，会跑多少页？MAX_PAGES=1000 这个上限是不是太宽了？
3. `tenacity` 重试遇到 4xx 应该停。我们怎么把"4xx 是不可恢复"这件事告诉 tenacity 的？
4. F-7 lockfile 现在不做。如果一个用户不知道，开了 cron 每 5 分钟跑一次 snapshot，又顺手手动 `make snapshot-markets`，会发生什么具体后果？
5. F-8 时区问题，如果不修，对一个 `endDate: "2026-11-05T00:00:00Z"` 的市场，end_time_ms 会偏多少？（提示：取决于 host 时区）

## FAQ 增量

_暂无_

---

← [05-ghost-book-issue-180.md](05-ghost-book-issue-180.md) | 回到 → [00-INDEX.md](00-INDEX.md)
