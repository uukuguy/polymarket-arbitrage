# Structure 动态时钟：让统计值控制 timeout 与 cadence

## 30 秒心智模型

一次 Structure attempt 是一个原子采集：父进程创建 `running` 记录，子进程要么发布完整
Structure 并记为 `succeeded`，要么被父进程终止并记为 `failed`。动态控制器不拆散这次
采集，也不靠猜测调参；它读取最近 30 条终态 attempt，用成功时长 p95 估计正常上界，并把
下一轮真正使用的 timeout/cadence 作为 append-only adjustment 持久化。

三个关键边界：

- 成功样本少于 10 条时不拿偶然值冒充分布；
- 最新一次 timeout 会立即把现有 timeout 提高 20%，普通变化要经过 3 次 attempt 冷却；
- timeout 固定限制在 180–600 秒，cadence 限制在 300–900 秒，并始终晚于 timeout，
  所以不会制造频繁重叠采集。

## 代码地图

- `src/polyarb/daemon/structure_schedule.py`：纯函数策略；计算 p95、退避、冷却和边界。
- `src/polyarb/daemon/scheduler.py`：启动时恢复 adjustment，每次终态 attempt 后重新评估，
  把 effective timeout 传给子进程，并按 effective cadence 等待。
- `src/polyarb/storage/sqlite_store.py`：读取最近 attempt，追加/恢复 adjustment。
- `src/polyarb/http/health.py`：展示 `snapshot:schedule`，并用同一个 effective timeout
  判断 running attempt 是否卡死。
- `tests/m1-perception/test_structure_schedule.py`：生产时长形状、重启幂等和运行时接线契约。

## 关键代码片段

`derive_structure_schedule()` 的 timeout 分支先尊重已有成功分布，再保证至少提升 20%：

```python
timeout_s = _clamp(
    max(
        math.ceil(previous_timeout_s * 1.2),
        (success_p95_s + 30) if success_p95_s is not None else 0,
    ),
    MIN_TIMEOUT_S,
    MAX_TIMEOUT_S,
)
```

这不是“失败就无限放宽”。`_clamp` 给自动控制明确护栏；连续失败达到 5 次后，原有 PAUSED
保护仍然生效，动态控制器不会自动 unpause。

调度器只把 adjustment 写一次。它用 `source_attempt_id` 绑定产生这次决定的 attempt；
表上的 UNIQUE 约束与启动恢复一起保证进程重启不会把同一次 timeout 再乘一次 1.2。

## 为什么 health 必须读同一份值

如果调度器已经把 timeout 调到 288 秒，而 `/health` 仍在 240 秒判 fail，就会出现
“writer 认为仍在正常采集，observer 却宣布超时”的假告警。现在两边都以
`structure_schedule_adjustments` 的最新行作为事实：

```text
snapshot:schedule:
configured_timeout_s=240 effective_timeout_s=288
configured_cadence_s=300 effective_cadence_s=348
success_samples=11 success_p95_s=236 reason=timeout-backoff
```

configured 是安全回退，effective 才是当前运行值。没有 adjustment 时两者一致。

## 设计取舍

- 选择 p95 而不是平均值：平均值会隐藏慢尾，采集 deadline 更关心尾部成功样本。
- 使用 nearest-rank：样本少且需可审计，避免插值产生看似精确的小数。
- adjustment append-only：不仅要知道“现在是多少”，还要知道哪次 attempt 导致了变化。
- 不自动解除 PAUSED：参数自调与故障恢复授权是两件事，避免控制器悄悄绕过安全门。

## 自检题

1. 最近一次成功耗时 120 秒，为什么不能单凭它把 timeout 立即降到 150 秒？
2. 同一次 timeout 后服务重启两次，为什么 timeout 不能连续乘两次 1.2？
3. `/health` 里的 configured timeout 与 effective timeout 哪个决定 running attempt 的状态？
4. 动态 cadence 为什么必须至少比 timeout 多 60 秒？
5. 连续五次失败后，动态控制器为什么不能自动 unpause？

## FAQ 增量

### 数据覆盖度能否靠延长 timeout 提升？

timeout 只决定“给一次原子采集多少完成时间”，不降低完整性门槛。覆盖度仍由 Gamma
分页完成、event/member reconciliation、发布资格和 `market_truth:coverage` 控制。
延长 timeout 可以减少慢但完整的采集被误杀，不能把不完整结果变成成功。
