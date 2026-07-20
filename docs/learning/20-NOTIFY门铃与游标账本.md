# NOTIFY 门铃与游标账本：让 L2 数据链自己追上来

## 30 秒心智模型

把 L1 的 `snapshot_complete` 想成门铃，而不是快递本身：门铃可能漏响、重复响，
但 SQLite/Supabase 里的最新 snapshot 和 `l2_event_cursor` 才是账本。L2 被唤醒或定时轮询
后，都由同一个 reconciliation pump 串行执行：读取账本 → 重算 candidate → 收敛 WS
订阅和 Supabase projection → 全链成功后才推进 cursor。

WS 也分两种真相：socket 活着只是 transport truth；收到可投影的 `book`，且 mirror
写入成功，才是 business truth。quiet refresh 因此必须做真实
`unsubscribe → subscribe(initial_dump=true)`，而且发送成功不能刷新任何 freshness。

```text
L1 snapshot
    │
    ├── NOTIFY（门铃，可丢/可重）──┐
    │                              ▼
    └── durable latest snapshot → reconciliation pump（唯一串行 owner）
                                      │
                                      ├─ candidate desired state
                                      ├─ WS subscription projection
                                      ├─ Supabase candidate projection
                                      └─ success-only cursor commit
                                                   │
WS quiet → unsubscribe→subscribe → received book → TOB mirror 201 → evidence
```

## 关键代码地图

### 1. NOTIFY 只负责唤醒

`src/polyarb/events/listener.py:89-116` 的监听循环把连接状态写进共享 state；连接结束或异常
后才进入重连。它不直接拥有 candidate refresh，也不提交 cursor。

`src/polyarb/events/reconciliation.py:89-127` 的 `ReconciliationPump.reconcile_once()` 才是
唯一协调者。它把 NOTIFY 与定时 wake 合并，读取 durable latest/cursor，并且只有
`refresh(...)` 返回成功后才提交 cursor。这样即使门铃丢失，下一次 poll 仍会对账；即使
门铃重复，也不会并发执行两次 refresh。

```python
# src/polyarb/events/reconciliation.py:114
async def reconcile_once(self) -> bool:
    cursor, latest = await self.store.read_position()
    if latest <= cursor:
        return await self.refresh({"snapshot_id": latest, "_maintenance": True})
    succeeded = await self.refresh({"snapshot_id": latest, "_reconciliation": True})
    if not succeeded:
        return False
    await self.store.commit(latest)
    return True
```

重点不是 `NOTIFY` 有没有到，而是“账本当前到了哪里、消费者承诺处理到了哪里”。

### 2. candidate 收敛是 cursor 事务的一部分

`src/polyarb/observation/l2_candidate_refresh.py:340-362` 明确规定返回值：任一必需的 WS
或 mirror 收敛失败就返回 `False`，pump 保留 cursor 等下次重试。

真正的顺序在 `src/polyarb/observation/l2_candidate_refresh.py:415-519`：

1. 从 live `markets_latest` 重算 desired candidate；
2. 通过事务化 replacement 收敛 WS membership；
3. 以 `(asset_id, recipe_name)` 收敛 Supabase candidate history；
4. 最后才提交 process-local candidate projection；
5. 所有步骤成功，返回 `True`。

这个顺序允许失败后安全重试。数据库 candidate identity 是复合键，WS identity 则是唯一
`asset_id`；两者不能偷懒当成同一个集合。

### 3. quiet refresh 为什么不是重复 subscribe

生产证明，对已订阅 asset 再发一次 `subscribe(initial_dump=true)`，发送可以成功，却不保证
服务端重新给 book。`src/polyarb/daemon/ws_consumer.py:353-405` 因此实现一个受
`_subscription_control_lock` 保护的真实 membership edge：

```python
# src/polyarb/daemon/ws_consumer.py:376-395
logger.info(f"ws quiet refresh: sending assets={len(active_assets)}")
await self._send_control(ws, {"operation": "unsubscribe", "assets_ids": active_assets})
await self._send_control(ws, {
    "operation": "subscribe",
    "assets_ids": active_assets,
    "initial_dump": True,
})
await asyncio.wait_for(asyncio.shield(waiter), timeout=_BOOK_EVIDENCE_TIMEOUT_S)
logger.info(f"ws quiet refresh: evidenced assets={len(active_assets)}")
```

waiter 不是由 send 完成，而是由 `src/polyarb/daemon/ws_consumer.py:693-704` 的正常接收路径
唤醒；它还要求同一 connection generation、事件类型为 `book`、production mirror 返回
`True`。部分发送、超时、取消或 generation 改变都会把 wire state 视为不确定，并通过共享
reconnect budget 重新收敛。

### 4. health 读的是谁真正写过的数据

`src/polyarb/http/l2_health.py:130-157` 的 WS age 来自接收循环更新的
`WsConsumer._last_event_at_s`。它代表“最近收到某个业务 frame”，不等于 TOB 已落库。

`src/polyarb/http/l2_health.py:361-402` 的 mirror age 来自 SQLite singleton
`l2_mirror_state`；只有 `src/polyarb/storage/l2_supabase_mirror.py:181-214` 的真实 Supabase
TOB 写成功后才刷新它。两条 age 可以分叉，这不是监控矛盾，而是精准指出断点位于
frame dispatch / projection / mirror 之间。

## 设计取舍

### 门铃 + 账本，而不是可靠门铃幻想

- 选择：NOTIFY 只作为低延迟 wake hint，poll + cursor 负责 completeness。
- 代价：多一次 durable read，也需要维护 cursor 表。
- 收益：LISTEN 断线、通知丢失、进程重启都不会永久漏 snapshot。

### success-only cursor，而不是“做过就算”

- 选择：candidate、WS、mirror 全部收敛后才 commit。
- 代价：下游短暂失败会重复执行 refresh。
- 收益：idempotent retry 修复半完成状态，不会让 cursor 越过坏数据。

### 真实 membership edge，而不是 optimistic refresh

- 选择：quiet 时 unsubscribe→subscribe，并等待同 generation 的 book→mirror evidence。
- 代价：控制路径更复杂，必须序列化并共享 reconnect budget。
- 收益：health freshness 不再能由一次成功 `send()` 伪造。

## 生产证据（2026-07-20）

同一 Fly machine `85e647c4eed598`、instance
`01KXSMS80B5AX2FGT5EPRC6V82`、image digest
`sha256:ec90d98e20c6ffe7ee48c899c939dab7a67addf45c28adda6695d13ed6269c4d`
自然进入 quiet 分支：

1. `05:32:41.823Z` — `ws quiet refresh: sending assets=3`；
2. `05:32:42.567Z` — Supabase `l2_top_of_book` POST 返回 HTTP 201；
3. `05:32:42.684Z` — `ws quiet refresh: evidenced assets=3`。

随后 `05:34:29Z–05:37:47Z` 的 198 秒只读窗口共 10 个 strict health 样本：HTTP
200 为 10/10，WS age 最大 50.1s（fail 阈值 120s），mirror age 最大 162.2s（fail 阈值
600s），cursor lag 恒为 0，listener 恒为 `listening`，machine/instance/image 全程未变。
L3 仍为 `0/10`，未通过放宽阈值伪装完成。这个窗口后来被一次新的上游故障覆盖：
`markets_latest` 在 snapshot 573 仍有 1939 个市场时返回空表，L2 把空投影视为成功并把
candidate 从 3 收敛到 0。它不否定 quiet-refresh 机制证据，但否定了 Phase closure。

因此又增加一条不变量：**HTTP 200 + 空列表不是“市场宇宙为零”**。L1 空 rows 必须在
任何远程写之前失败；L2 空投影必须保留 last-known-good、fetch freshness、WS membership
和 durable cursor，等待下一次 reconciliation 重试。

根因不在 Fly：Pydantic Settings 会自动读取项目 `.env`，而 mocked orchestrator test
fixture 没有显式清空 Supabase/R2 等配置。于是“本地单元测试”实际上启用了生产 mirror；
它发生在 Fly snapshot 573 成功写入之后，因此 Fly 日志找不到最后的 DELETE。修复后，
repository-wide autouse fixture 默认关闭全部外部写适配器；1413 个 collected tests 全过，
且生产 Postgres write counters 前后完全相同。

下一次自然 L1 tick 随后写出 snapshot 574/1942 markets，L2 在同一 instance 上恢复 3 个
candidate。新的 258 秒窗口 10/10 strict HTTP 200，cursor lag 恒为 0；窗口末 mirror age
接近 600 秒边界后，真实 TOB 201 把它重置到 61.7 秒。因此 Phase 05.1 已关闭，但本地
fail-closed runtime guard 尚未部署，不能称为“生产已验证该 guard”。

## 对手测试

1. LISTEN 断了 20 分钟，但 poll 每 60 秒仍工作；你凭什么判断 snapshot 没漏？应查看
   durable latest 与 committed cursor，而不是 notification age。
2. quiet refresh 日志只有 `sending assets=3`，没有 201 和 `evidenced`；能否刷新 health
   timestamp？不能，wire membership 仍可能不确定。
3. WS age 0.2s、mirror age 6306s；你会重启 socket 还是先定位 frame→projection→mirror？
   应先按两条独立 clock 找第一断点，不能把 coarse WS age 当 TOB 证据。
4. candidate mirror 已插入新 rows，但 WS replacement 失败；cursor 能否推进？不能，下一次
   reconciliation 必须重试完整 desired state。
5. 为让 strict health 变绿，把 WS fail 从 120s 改 900s 是否合理？不合理，这改变 SLA，
   掩盖数据链问题；正确做法是恢复真实数据或明确降级。

## FAQ 增量

### Q：为什么 socket ping/pong 不算 business freshness？

ping/pong 只证明 transport 还能交换控制帧。套利消费者需要的是可定价的 orderbook；没有
book/price frame 和成功持久化，socket 再健康也不能支持机会判断。

### Q：为什么 mirror age 用本地 SQLite，而不是每次 `/health` 查询 Supabase？

`/health` 必须快速、稳定且不把远程数据库故障放大成探针雪崩。写成功路径把事实缓存到
本地 singleton，health 只读该 anchor；远程写失败则不会推进它，因此仍保留 chain truth。

### Q：为什么 L3 `0/10` 没阻止 Phase 05.1 关闭？

Phase 05.1 的完成定义不包含 L3 N=5/10-token soak；后者属于 Phase 05 Plan 06。
Phase 05.1 已在空投影根因修复、自然恢复和当前链重验后关闭，但 L3 仍为 `0/10`，所以
M1 整体还没有通过 Phase 05 的最终生产门。

### Q：为什么空 `markets_latest` 不能代表真实市场真的归零？

同一时刻 durable snapshot metadata 明确记录 `market_count=1939`，所以空表与来源事实
矛盾。此时正确语义是“投影不可用/不一致”，不是“desired set 为空”；否则一个短暂或
失败的覆盖会主动退订全部资产，并让系统失去自行恢复 book freshness 的能力。

### Q：mock 了 Gamma/CLOB，为什么测试还会写生产？

mock 只替换了行情客户端，不会自动替换配置和 storage adapter。`Settings(env_file=".env")`
仍会打开 Supabase、R2、event bus 等尾部副作用。测试安全必须在配置入口 fail-closed：默认
清空所有外部凭据，只有明确的 localhost/dummy fixture 才允许启用对应 adapter。
