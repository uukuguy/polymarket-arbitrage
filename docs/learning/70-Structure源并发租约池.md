# Structure 源并发租约池

## 30 秒心智模型

八个 lane 不是把同一页 Gamma 数据抓八次，而是八个独立 worker 同时向
Postgres 领取**不同的、已冻结身份的 job**。数据库 lease 是唯一的分工者：没
有可做的 job 的 lane 空转，有 job 的 lane 才读 Gamma、写 R2、提交 receipt。

因此 event stream 仍然只有一个 lane 在工作（下一页依赖前一页的 opaque
cursor）；当 event terminal receipt 产生精确的 `markets:<ordinal>` 批次后，八个
lane 可以同时处理不同的市场 ID 批。吞吐增加，但“每个 durable effect 只确认一
次”的边界没有改变。

## 代码地图

- [`structure_source.py`](../../src/polyarb/control_plane/structure_source.py:360)：单个
  lane 的 lease → Gamma → R2 → receipt 主流程。
- [`structure_source.py`](../../src/polyarb/control_plane/structure_source.py:553)：
  `TransactionalStructureSourcePool` 并发运行和聚合结果。
- [`cli_control_plane.py`](../../src/polyarb/cli_control_plane.py:234)：生产进程创建
  八个带不同 worker ID 的 Gamma lane。
- [`test_control_plane_postgres.py`](../../tests/m1-perception/test_control_plane_postgres.py)：
  三个 market batch 必须都 terminal，materializer 才可领取的真实 Postgres 合约。

## 两层并发边界

第一层是 source graph 的依赖关系：

```text
events:0 → events:1 → ... → events:terminal
                                  ↓
                    markets:0  markets:1  ... markets:N
                        ↓          ↓              ↓
                  terminal receipts 全部齐全
                                  ↓
                         structure-materialize
```

第二层才是 worker pool。它不预先给 lane 分配 ordinal；每个 lane 都向同一个
durable queue 请求 `structure-fetch` lease。这避免“lane 3 宕机后它负责的市场永远
没人做”的静态分片问题。

```python
results = await asyncio.gather(
    *(lane.run_once() for lane in self._lanes), return_exceptions=True
)
```

这段 [`structure_source.py`](../../src/polyarb/control_plane/structure_source.py:563)
只并发**执行**已经由 Postgres 安全分配的工作；它不替代数据库的 fencing。

## 为什么 event 阶段看到 `succeeded:1/8`

pool 聚合的是本轮有实际 job 的 lane，而分母保留总 lane 数：

```python
completed = [result for result in results if result.job_key is not None]
outcome = f"succeeded:{succeeded}/{len(self._lanes)}"
```

所以 `succeeded:1/8` 的含义是“八个可用 lane 中只有一个可领取 job，并且它
成功了”，不是七个失败。event cursor 必须串行，这正是我们想要的。market batch
阶段才应出现接近 `succeeded:8/8` 的日志。

## 设计取舍

- **八个固定 lane，而不是无限并发**：保护 Gamma、R2 和 Postgres，并使失控重试
  有明确上界。
- **每 lane 一个 GammaClient，R2 client 共享**：HTTP 生命周期彼此独立；对象存储
  凭证与连接池不因 lane 数重复膨胀。
- **事件串行、市场并行**：不透明 cursor 不能安全并猜；最终 event 成员集一旦冻结，
  精确 market ID 批天然可并发。
- **所有 lane 都结束后才汇报**：`gather` 等待 sibling，调度器拿到的是一轮完整事实，
  而不是某个先返回的 lane 掩盖其他失败。
- **terminal 证据读取有 90 秒上界**：最后一页需要读取此前所有 event artifact；若
  R2 读取卡住，worker 把本轮记为 retryable，而不是让 scheduler 永久停在一个已经
  过期的 lease 上。迟到的读取线程没有 receipt 写权限，下一 epoch 才是唯一能提交
  的 owner。
- **两种上界不能混用**：`max_pages` 防的是 opaque cursor 无限翻页，所以只约束
  `events`；精确 market batch 没有 cursor，改由 `max_market_batches` 约束。把前者
  套到后者会在 ordinal 1000 错误 quarantine 一个仍合法的、例如 6,427 批的市场全集。

## 自检题

1. 一个 market lane 在 R2 上传后、receipt 前死亡，另一个 lane 为什么能安全接管？
2. 为什么不能按 `hash(market_id) % 8` 永久指定 lane？
3. `succeeded:1/8` 在 event 阶段为什么不是降容故障？什么时候它才值得告警？
4. 为什么 materializer 仍要等待所有 market receipt，而不是看到第一批市场就产出
   半个 Structure bundle？

## FAQ 增量

### 为什么一个已到期的 retryable 任务还可能要等很久？

“可以重试”不等于“下一轮一定会先执行”。如果 claim 查询只按初始入队时间排序，
一个较晚失败、但已经到期的任务会排在数千个更早入队的首次任务后面。整个窗口虽
然最终仍能完成，却会把 terminal receipt 和下游 materializer 不必要地拖住。

现在 [`postgres.py`](../../src/polyarb/control_plane/postgres.py:2514) 明确将**到期的
retryable** 排在首次 runnable 前面；同一优先级内仍按既有调度时间、更新时间和 job
key 稳定排序。它不绕过退避，不把失败伪装成成功，也不改变八 lane 上限，只保证已经
满足重试条件的失败不会被无限长的初始队列饿死。

若你想确认线上某一条 `succeeded:x/8` 是正常依赖关系还是实际 lane 失败，把对应
window/job 日志贴出来；会把判读规则继续补在这里。
