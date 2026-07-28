# Bounded Discovery：覆盖是一段时间的证据

## 30 秒心智模型

Discovery 不再尝试一次抓完“全市场时刻”。每轮只读取一个有上限的 Gamma keyset
页面，并把以下事实放进同一个 SQLite 事务：

```text
opaque cursor
  + complete-supported group revision
  + priority inputs/output
  + promotion
  + per-group coverage sample
  -> COMMIT
```

事务失败时 cursor 不动；进程重启后重读 cursor。页面结束只代表一轮扫描到尾，下一轮
从头开始。15/30/60 分钟覆盖度描述滚动时间窗，不承诺任何新组都不漏。

## 代码地图

| 文件 | 责任 |
|---|---|
| `src/polyarb/clients/gamma_client.py` | 读取一个有界事件页，保留 opaque next cursor |
| `src/polyarb/perception/discovery.py` | 复用事件/市场 normalizer，认证完整组并执行一个 batch |
| `src/polyarb/perception/priority.py` | Decimal 权重、原始输入和无上限 age anti-starvation |
| `src/polyarb/perception/store.py` | 同事务写 revision、schedule、promotion、coverage、cursor |
| `src/polyarb/perception/candidate_watcher.py` | 首次盯盘前消费 Discovery 的持久化 score/class |
| `src/polyarb/cli_discovery.py` | 只读输出 cursor、队列和 15/30/60 分钟覆盖 |

## 关键数据契约

事件页的 `next_cursor` 是远端 opaque token。代码只保存、回传和检查“没有原样重复”，
不解析、不排序，也不猜测下一页。`completed=true` 时持久化空 cursor；下个 batch
重新从第一页开始一轮新的滚动采样。

Gamma event membership 可以证明 market ID 和组边界，但 Candidate Watcher 还需要
condition ID、Yes token 和标题。Discovery 因此对每个 `complete-supported` 成员复用
`normalize_market()`，只有所有 active named legs 都有完整身份时才构造
`GroupRevision.certified()`。缺任何身份都会留下 `incomplete-source` schedule 和有界
原因，但不会 promotion，更不会用空字符串伪造可运行候选。

优先级公式是显式证据：

```python
score = (
    edge_bps * Decimal("0.35")
    + activity_rank * Decimal("0.20")
    + liquidity_rank * Decimal("0.15")
    + change_rank * Decimal("0.15")
    + age_rank * Decimal("0.15")
)
```

五个输入、输出和 reason 都以 Decimal 文本持久化。`age_rank` 是距上次 visit 的分钟数，
不封顶；因此任何持续被推迟、但仍有效的组最终会超过有限的新近分数。第一次 Candidate
采集前，scheduler 直接消费这个持久化 score；第一次终态后再由 Candidate 自己的
durable due time 接管。

## 原子性和取消

一个 batch 内先认证 revision，再更新 schedule/promotion、写 coverage sample，最后
更新 cursor。任一步抛错都会 `ROLLBACK`，所以数据库不存在“cursor 已到下一页，但本页
候选或覆盖事实消失”的状态。

同步 SQLite commit 放在线程中。外层 task 在 commit 开始后被取消时，会持续 shield
同一个 writer，等它明确 COMMIT 或 ROLLBACK 后才重新抛出第一次取消。结果可能是
“整页已提交”或“整页未提交”，不会是 cursor 单独前进。

## 覆盖度怎么读

`make perception-discovery-status db_path=/path/to/state.db` 只读同一组 Discovery 表：

- cursor 与最近 batch；
- high/normal/explore promotion 数；
- 最老 visit 年龄；
- 15/30/60 分钟 distinct group raw coverage；
- 同一窗口的 liquidity-weighted coverage。

分母是当前已知 schedule，不是不可知的真实全市场；所以它是
`active-known coverage`。覆盖低仍返回 0，因为这是业务事实，不是命令故障；数据库
不存在、不可读或 schema 无效才返回非零。

## 设计取舍

- Candidate freshness 接近 hard-stale 时，Discovery 在发 Gamma 请求前主动 yield；
  没有候选时允许探索扩张。
- promotion source 是 legacy seed 与 Discovery promotion 的稳定去重并集，不会因接入
  新 producer 丢掉当前 hot candidates。
- feature flag 默认关闭；本 slice 只完成生产代码路径，不等于已经完成部署和切换。
- 这里仍是 observer-only，不含钱包、签名、余额、下单或资金执行。

## 自检题

1. 为什么 coverage=100% 仍不能说“全市场零遗漏”？
2. cursor 为什么必须和 schedule/promotion/coverage 在一个事务？
3. event membership 已 complete-supported，为什么仍可能拒绝 promotion？
4. age rank 为什么不能被固定上限永久压住？
5. Candidate Quote 已接近 stale 时，Discovery 应该继续追覆盖还是 yield？

## FAQ 增量

### 一页里某个组身份不完整，为什么仍可提交 cursor？

“身份不完整”是该组的有界 source rejection，不等于整页传输/normalization 失败。它会
作为 `incomplete-source` 事实参与覆盖，但不会 promotion。只有页面形状、cursor、
结构 normalizer 或事务本身失败时，整页才回滚且 cursor 不动。
