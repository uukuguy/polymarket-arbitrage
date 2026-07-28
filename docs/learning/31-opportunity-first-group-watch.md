# Opportunity-first：按组盯盘，而不是等全市场

## 30 秒心智模型

一次全市场采集是一个跨度可达十分钟的观察窗口，不是同一时刻的市场。
Candidate Watcher 因此只处理一个已经认证的 neg-risk 组：

```text
读组版本 before
  -> 只请求该组全部 Yes token 的 top book
  -> 再读组版本 after
  -> membership_hash 相同且每腿可执行
  -> 原子发布 quote_batch
  -> 记录本次结果和下一次到期时间
```

任何一腿缺失或采集期间成员变化，都只让这个组变成 `unavailable`。它不会
混用旧腿、新腿，也不会等待无关市场的完整采集。

## 代码地图

| 文件 | 责任 |
|---|---|
| `src/polyarb/perception/group_structure.py:15` | 异步读取一个当前 `certified` 组；同步 SQLite 读取放进线程 |
| `src/polyarb/perception/candidate_watcher.py:243` | 执行 before → books → after 的按组认证 |
| `src/polyarb/routing/focused_quote_collector.py:195` | 把一组 top books 规范成同一 `quote_batch_id` 的完整批次 |
| `src/polyarb/perception/store.py:113` | 同事务发布成功 Quote batch 与 positive fact |
| `src/polyarb/perception/store.py:188` | 追加 unavailable 终态事实和调度决定 |
| `src/polyarb/daemon/main.py:94` | 以默认关闭的 feature flag 启动兄弟任务 |

## 关键代码

真正的原子边界不是 HTTP 请求成功，而是成员身份在请求前后不变：

```python
before = await self._structure_reader.read_group(group_id)
books = await self._books_reader.get_books(
    [leg.yes_token_id for leg in before.legs],
    projection="top",
)
after = await self._structure_reader.read_group(group_id)
if after.membership_hash != before.membership_hash:
    return await self._record_unavailable(...)
```

随后 `publish_candidate_success()` 会在同一个 `BEGIN IMMEDIATE` 事务里重读当前组、
插入完整 Quote batch、再插入 positive terminal fact 后一起提交。因此在 `after`
读取和写入之间再次换版时，batch 和 positive fact 都不会出现；不会留下“报价已失效，
但盯盘结果仍为 watching”的裂缝。

调度决定不是隐藏的 sleep 常量。`priority_class`、`effective_interval_s`、
`schedule_reason` 和 `next_due_at_ms` 与每次终态一起持久化，见
`src/polyarb/perception/store.py:113` 与 `src/polyarb/perception/store.py:188`。
重启后 scheduler 继续按数据库中的
到期时间排序；相同到期条件下 high 先于 normal，再先于 explore。

`asyncio.to_thread()` 被取消时，线程里的 SQLite 写不会自动停止。Candidate Watcher
因此 shield 已开始的写任务；即使父 task 在 COMMIT 期间被连续取消多次，也始终只
shield 同一个 writer task，直到它返回 durable fact。成功时 runtime 只收敛一次，再
重新抛出第一次 `CancelledError`；writer 失败时，第一次取消仍是外层语义，写错误保留为
cause。这避免关机或 per-group timeout 撞上 COMMIT 时出现“数据库已写、runtime 永远
没看见”，也不会因第二次取消打断收敛。

Scheduler 每轮只处理配置的最大组数，并为 normal/explore 保留槽位；high 仍先执行，
但每个组有独立 timeout。由于 py-clob-client 是同步 SDK，取消 asyncio task 无法杀死
已经运行的 SDK 线程；生产 builder 因而把 high 和 normal/explore 放入两个独立的有界
线程池。卡住的 high 最多占用配置的 high worker 数，不会耗尽默认 executor，也不会
阻止 lower lane 继续采集。shutdown 会取消尚未开始的 future，但运行中的 SDK 调用只能
依靠 HTTP timeout 返回；容量边界由 worker 数保证。

枚举候选或读取 durable due state 的异常属于 source/cycle boundary，由 loop supervisor
记录、退避并在下一次 cycle 成功时恢复。单组 timeout/异常则独立记录 degraded group；
只有同一个 group 后续成功才能清除它。另一个组成功不会制造虚假的 recovery。

## 设计取舍

- **一个组一次请求**：请求集合严格等于该组有序的全部 Yes token；hot path 不调用
  全市场 `collect_quotes_in_subprocess()`。
- **失败不降级优先级**：high 候选一次失败后仍是 high，只进行有上限的退避；
  否则最值得盯的组反而会因瞬时错误掉入五分钟慢车道。
- **优先不等于垄断**：每轮保留 normal/explore 槽位，并用 per-group timeout 限制
  卡住的 high。这样 freshness priority 与 age-based anti-starvation 同时成立。
- **退避先封顶再指数**：先由 `cap/base` 算出最大有效翻倍次数，再做 `2**n`；
  即使 durable failure count 是 100000，也只得到配置 cap，不会先发生数值溢出。
- **配置先拒绝不可能的控制器**：所有秒数必须为正有限数，high cadence 不得超过
  hard-stale；保留槽必须满足 `1 <= reserved < cycle_max_groups`。normal/explore cadence
  可以高于 high 的 hard-stale，因为它们是低优先级基准周期，不是 high freshness SLA。
- **隔离资源而非假装可取消**：high/lower CLOB worker 分池；SQLite、Structure 读取仍走
  默认 executor。线程池 shutdown 不承诺中止已经进入同步 SDK 的调用。
- **旧链仍保留**：本 slice 没有替换原机会 API。新 worker 默认关闭，待后续
  Discovery、API、Dashboard 与故障资格门完成后才允许生产切换。
- **观察者边界**：这里只读 Gamma/CLOB 事实并写本地证据，不含钱包、签名、余额或下单。

## 自检题

1. 为什么只在 CLOB 返回后检查 `membership_hash`，仍不足以安全发布？
2. 两腿里有一腿没有 ask 时，为什么不能把它当作价格 0？
3. high 候选连续失败三次后，为什么需要有上限退避，而不能直接改成 explore？
4. 全市场 Structure 仍在采集时，一个已认证组能否继续被盯盘？依据是什么？
5. feature flag 打开是否等于生产切换已经完成？还缺哪些后续质量门？
6. 为什么 `asyncio.to_thread()` 外层 task 被取消，不代表 SQLite 线程里的 COMMIT 被取消？
7. 为什么另一个组成功不能清除当前组的 degraded 状态？
8. 为什么 per-group timeout 仍需独立 CLOB executor，而不是只包一层 `wait_for()`？

## FAQ 增量

### 这是否承诺任何新套利组都不会漏？

不承诺。Candidate Watcher 优化的是已发现候选的新鲜度。新组如何进入候选集合由滚动
Discovery 和周期 Reconciliation 提供统计覆盖，不能把有限资源系统描述成零遗漏。

### 现在打开 feature flag，为什么仍可能看到 `group-not-certified`？

Task 2 暂时用 legacy active masters 作为 seed，但新 group-revision authority 不会凭空
拥有这些组。Task 3 必须把滚动 Discovery 的 promotion source 与 Candidate Watcher
组合起来或替换临时 seed。Task 3 完成前打开 flag 只适合受控验证，不是生产切换。
