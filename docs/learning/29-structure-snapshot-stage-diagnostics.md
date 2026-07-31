# Structure Snapshot 阶段诊断：超时先定位，再决定实验

> 对应：M1 Structure snapshot stage diagnostics（2026-07-28）
> 实际巡检先看：[M1 市场感知平台使用手册](../M1-市场感知平台使用手册.md)。

## 30 秒心智模型

一轮 Structure 采集不是一个黑盒的“成功 / 失败”。父 scheduler 启动隔离的
Gamma-only 子进程，并只记录**最后一个已确认的阶段**和**父进程实测的总耗时**。
它们回答的是：“这次失败最接近卡在哪，以及从启动到被父进程收尾花了多久？”

例如，`failed + snapshot-subprocess-timeout + stage=gamma-markets +
elapsed_ms=240xxx` 是一条可检验的 Gamma markets 阶段假设；它不是“把 timeout、
cadence 或 VM 调大”的授权。成功记录同样有价值：它给后续实验提供基线时长和最后完成阶段。

```text
child stderr allowlisted stage markers
             │
             ▼
parent reaps child → elapsed from parent monotonic clock
             │
             ▼
append-only snapshot_attempts terminal fact
             │                         │
             ▼                         ▼
make snapshot-attempt-status      strict /health latest-attempt check
```

因此，诊断是观察证据，不是自动调参器；仍由 Structure 的完整发布事实、Quote
绑定和 M2 fail-closed gate 决定系统能否使用数据。

## 代码地图

| 位置 | 责任 |
|---|---|
| `src/polyarb/snapshot/orchestrator.py:155-170` | 子进程在每个有界步骤写 `snapshot-stage` start/complete marker。 |
| `src/polyarb/daemon/scheduler.py:69-84` | 只解析固定词表的最后一个 marker：`gamma-events`、`gamma-markets`、`membership-recheck`、`validate`、`persist`。 |
| `src/polyarb/daemon/scheduler.py:97-176` | 父进程启动 Structure child、以 monotonic 时钟计时；超时后先 TERM/KILL 并 reap，才读取 stderr 诊断。 |
| `src/polyarb/daemon/scheduler.py:397-503` | 每个 tick 先创建 attempt，随后把成功、失败或取消的诊断关闭为终态，并保持五次失败暂停保护。 |
| `src/polyarb/storage/schemas.py:463-485` | `snapshot_attempts` 是 append-only 的父进程观察表；内核杀掉 child 时不会依赖 child 自己写终态。 |
| `src/polyarb/storage/sqlite_store.py:949-1025` | `running` 记录只可关闭一次；读取接口以只读 SQLite URI 取最新一行。 |
| `src/polyarb/http/health.py:363-412` | strict health 把 failure kind、stage 和 elapsed 作为 latest-attempt 输出；旧的 published truth 新鲜时失败是 warn，过期时才升级 fail。 |
| `scripts/snapshot_attempt_status.py:1-31` | 本机只读 JSON 入口：不建 schema、不联系 Fly、不启动 daemon。 |

父进程只信任结构严格的 marker，而不把任意 stderr 文本当成阶段名：

```python
# src/polyarb/daemon/scheduler.py:71-84
_SNAPSHOT_STAGE_MARKER_RE = re.compile(
    rb"^snapshot-stage stage="
    rb"(gamma-events|gamma-markets|membership-recheck|validate|persist) "
    rb"state=(?:start|complete) elapsed_ms=(?:0|[1-9][0-9]*)$",
    re.MULTILINE,
)
```

终态写入同样不可重写，避免后来的结果覆盖已观测到的事故：

```python
# src/polyarb/storage/sqlite_store.py:979-995
UPDATE snapshot_attempts
SET finished_at_ms=?, outcome=?, snapshot_id=?, failure_kind=?,
    last_stage=?, elapsed_ms=?
WHERE id=? AND outcome='running'
```

## Chain truth：从失败到操作者所见的证据链

1. **写入侧**：scheduler 在 spawn child 前追加 `running`（`scheduler.py:397-400`）；
   成功、失败和 timeout 都经 `_finish_attempt` 写同一 attempt（`355-381`、`416-423`、`481-488`）。
2. **采样侧**：child 的 stage marker 出现在 stderr；父进程在 timeout 时先完成 bounded
   reap（`scheduler.py:128-173`），所以不会用未回收 child 的日志虚构“终态”。
3. **持久化侧**：`last_stage` 与 `elapsed_ms` 与 outcome 在同一行落盘
   （`sqlite_store.py:963-995`）；它们可为 `NULL`，旧行不会被伪造阶段。
4. **健康侧**：`/health` 从这张表读最新行（`health.py:363-412`），因此 health 显示的
   `stage=... elapsed_ms=...` 的数据源正是 scheduler 真在 mutate 的表。
5. **端到端触发**：timeout reap 回归测试在
   `tests/m1-perception/test_scheduler.py:394-425` 模拟 child 最后停在
   `gamma-markets`；health 回归在 `test_health_endpoint.py:289-310` 断言同一诊断可见。

这条链仍是 fail-soft 的诊断链，不是放宽安全门：无法得到完整 Structure 时，Quote/M2
不应把旧成员关系当成新的可交易事实。

## 如何解读一次样本

先取本机 daemon 所在 volume 的事实；该命令只读本地 SQLite，不能替代生产 health：

```bash
make snapshot-attempt-status
make smoke-health-prod
```

| 终态样例 | 正确解释 | 下一步假设（不是立即修改） |
|---|---|---|
| `succeeded`, `stage=persist`, `elapsed_ms=108000` | 一轮 Structure 从 child 启动到持久化完成约 108 秒；可作为后续 cadence 实验的基线。 | 先积累可比较样本，确认长尾来自哪个阶段。 |
| `failed`, `snapshot-subprocess-timeout`, `stage=gamma-markets`, `elapsed_ms=240xxx` | 父进程在 deadline 后已收尾；最后可信 marker 指向 Gamma markets，而非“整个系统慢”。 | 检查 Gamma markets 请求、分页或上游延迟的证据。 |
| `failed`, `...`, `stage=membership-recheck` | Gamma 初始阶段已越过，成员关系补核验是当前假设。 | 关联孤立 market recheck 批次和外部响应，不改变并发或 timeout。 |
| `running` 且年龄超过 240 秒 | strict health 报 `snapshot-subprocess-timeout-exceeded`；可能是 parent 未留下终态的异常窗口。 | 核对 mounted SQLite、子进程 reaping 日志和 deploy/restart 时间线。 |
| `succeeded`, `last_stage=null` | 兼容旧的或无 marker 记录；不是“已完成 persist”的证明。 | 仅作为成功 outcome 读取，避免编造阶段。 |

`elapsed_ms` 是父进程的 wall-clock 观察，包含 child 生命周期和终止/reap 的少量开销；它不是
单个 Gamma HTTP 请求、CPU 时间或 memory peak。阶段只代表**最后一个已确认 marker**：在
`gamma-markets` 后超时，不能断言该阶段是唯一根因。

## 明确不做的决定

- 不改变 240 秒 Structure child deadline。
- 不改变 Structure/Quote cadence、retry、并发、资源限制或 Fly VM 尺寸。
- 不自动 unpause、不建立 unpause loop，也不因一次成功关闭长期 soak 验收。
- 不接触订单、钱包、签名、余额或执行路径；本功能是 observer-only M1 诊断。

如果样本重复支持一个假设，下一项工作应是一个单独、可验证的实验计划；它必须先说明
健康检查读什么、写入侧如何更新、以及失败如何端到端可见，不能把诊断数字直接变为配置调优。

## 自检题

1. `stage=gamma-markets` 为什么不是“Gamma markets 已经完成”的证明？
2. 为什么 timeout path 必须先 reap child，再读取最后一个 marker？
3. `elapsed_ms=240xxx` 能否单独证明 VM 内存不足？还缺什么证据？
4. published Structure 尚新鲜而 latest attempt failed 时，health 为什么可为 warn 而不是假装完全正常？
5. 一次 `succeeded + persist` 样本允许你直接延长 scheduler cadence 吗？为什么？

## FAQ 增量
