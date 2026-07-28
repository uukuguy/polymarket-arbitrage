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
| `src/polyarb/perception/candidate_watcher.py:199` | 执行 before → books → after 的按组认证 |
| `src/polyarb/routing/focused_quote_collector.py:195` | 把一组 top books 规范成同一 `quote_batch_id` 的完整批次 |
| `src/polyarb/perception/store.py:194` | 追加一次终态事实和调度决定 |
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

见 `src/polyarb/perception/candidate_watcher.py:204-217`。随后
`publish_quote_batch()` 还会在 SQLite 写事务里重读当前组，所以在 `after` 读取和
写入之间再次换版，也会 fail closed。

调度决定不是隐藏的 sleep 常量。`priority_class`、`effective_interval_s`、
`schedule_reason` 和 `next_due_at_ms` 与每次终态一起持久化，见
`src/polyarb/perception/store.py:194-257`。重启后 scheduler 继续按数据库中的
到期时间排序；相同到期条件下 high 先于 normal，再先于 explore。

## 设计取舍

- **一个组一次请求**：请求集合严格等于该组有序的全部 Yes token；hot path 不调用
  全市场 `collect_quotes_in_subprocess()`。
- **失败不降级优先级**：high 候选一次失败后仍是 high，只进行有上限的退避；
  否则最值得盯的组反而会因瞬时错误掉入五分钟慢车道。
- **旧链仍保留**：本 slice 没有替换原机会 API。新 worker 默认关闭，待后续
  Discovery、API、Dashboard 与故障资格门完成后才允许生产切换。
- **观察者边界**：这里只读 Gamma/CLOB 事实并写本地证据，不含钱包、签名、余额或下单。

## 自检题

1. 为什么只在 CLOB 返回后检查 `membership_hash`，仍不足以安全发布？
2. 两腿里有一腿没有 ask 时，为什么不能把它当作价格 0？
3. high 候选连续失败三次后，为什么需要有上限退避，而不能直接改成 explore？
4. 全市场 Structure 仍在采集时，一个已认证组能否继续被盯盘？依据是什么？
5. feature flag 打开是否等于生产切换已经完成？还缺哪些后续质量门？

## FAQ 增量

### 这是否承诺任何新套利组都不会漏？

不承诺。Candidate Watcher 优化的是已发现候选的新鲜度。新组如何进入候选集合由滚动
Discovery 和周期 Reconciliation 提供统计覆盖，不能把有限资源系统描述成零遗漏。
