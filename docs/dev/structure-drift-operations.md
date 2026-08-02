# M1 Structure drift-safe 运行与验收

本流程只生成和验证授权证据，不部署、不切 `current_structure_generation`、不修改 read mode。

## 启用前只读检查

```bash
make structure-generation-status
make structure-generation-compare
make structure-generation-drift-compare
```

旧 exact compare 对陈旧 legacy 失败是预期输入；drift compare 在未启动时必须 nonzero，不能解释为零风险。

## scheduler 配置

默认 `structure_generation_drift_compare_enabled=false`。受控启用时保持：

- `structure_generation_drift_max_rows=500`
- `structure_generation_drift_max_chunks_per_tick=100`
- `structure_generation_drift_slice_s=45`

`max_rows=500` 是全局上限；`source-events` phase 先取最多 100 events，再选择累计 member workload ≤500、
payload ≤512 KiB 的稳定非空前缀。正常单 event 至少推进 1 条；单 event 自身超过任一绝对上限则立即以
`source-event-workload-oversized` fail closed、0 progress，并由 attempt health 报警，不允许硬跑到 timeout。
其它 phase 仍可使用 500 行，slice 总边界仍是 100 chunks / 45 秒。

scheduler 在 `_tick_lock` 内先检查 Quote，再取得共享 producer lock 并复查 Quote，之后才 spawn child。不要直接运行
隐藏的 `structure-generation-drift-advance`；它是 parent-owned subprocess protocol，不是 operator surface。

## 运行中判读

strict health 的 `snapshot:structure_generation_drift`：

- `disabled/pass`：功能关闭，legacy 数据面不受影响。
- `none/warn`：有界比较尚在推进；检查 phase、checkpoint age 和 class counts。
- `drift-safe-sealed/pass`：当前 full identity 的 append-only receipt 已认证。
- `stale/fail` 或 receipt/identity invalid：停止验收；保存证据，等待新的 Structure generation，不得绕过。

writer busy、child timeout/cancel/SIGKILL 会释放 producer lock。已完成 chunk 不回滚；下一次 scheduler admission
从持久 cursor 恢复。Quote due 在下一次 admission 优先，不在 slice 中间强杀已持锁 child。

每次真正 spawn 前，parent 先写 `structure_drift_attempts` running ownership；写不下账本就不启动 child。parent
对 checkpoint/success/defer/timeout/SIGKILL/cancel/invalid JSON 都必须终结该行，保存 full identity、progress ID、
chunks/rows/elapsed、stderr bytes/digest 和最后一条白名单 marker。启动时把遗留 running 标为
`parent-restarted-orphan`；只保留最近 100 条 terminal evidence，且绝不写入 `snapshot_attempts` 或 adaptive schedule。
strict health 引用最新 drift attempt：failed 或 running 超 90 秒为 fail，deferred 保持可诊断 warn。
timeout/SIGKILL 时 parent 只采信 child 最后一条 post-CAS 白名单 marker；例如 marker 的 `chunks=1 rows=500`
表示这些行已经提交，attempt ledger 必须保留该值，即使 child 没有返回最终 stdout JSON。

## 实际验收门

```bash
make structure-generation-drift-compare
make health-local
```

只有 `authorized=true` 且 `authorization_mode=exact|drift-safe-sealed` 才通过 preflight。随后仍需单独的部署授权、
exact release 验证、read-mode 切换计划和回滚证据；本 gate 自身不完成这些 mutation。

## 故障处理

- `structure-drift-writer-busy`：确认 Quote/其他 writer 所有权；不要并发手动推进。
- `structure-drift-child-failed`：看 child signal/timeout 分类；SIGKILL 先按 possible cgroup OOM 调查。
- `unclassified>0` / `overlap-conflict>0`：数据证据失败，不是性能重试问题。
- checkpoint 超 SLA：确认 scheduler feature flag、defer receipt 和 Quote 持续 due/active 事实。
