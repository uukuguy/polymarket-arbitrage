# 生产资格证据：PASS 必须回答“谁、在哪次启动、哪段窗口”

## 30 秒心智模型

本地 fixture 的 PASS 只证明判定器会工作。生产 PASS 还必须证明：

```text
exact release + exact machine + exact boot + bounded time window
                              ↓
                  authenticated GET evidence
                              ↓
                   deterministic FAIL/PASS
```

任一 identity 在窗口中改变，整段证据作废。某个指标没有公开读模型或 fault
receipt 能证明时，它必须保持缺失并触发 FAIL，不能填 0。

## 代码地图

- `src/polyarb/http/health.py:1003`：同一 health envelope 暴露 release、machine、boot 和 qualification policy。
- `scripts/perception_fault_readonly.py:58`：验证同窗 identity 并只推导已观察指标。
- `scripts/perception_fault_readonly.py:257`：固定五个 GET 面的有界采样。
- `scripts/perception_fault_acceptance.py:108`：production provenance fail-closed 门。
- `scripts/perception_fault_acceptance.py:163`：稳定 SLA reason codes 与最终 verdict。
- `Makefile:801`：synthetic local conformance。
- `Makefile:816`：production GET-only evidence。

## 为什么只绑定 release 不够

rolling deploy 期间，同一个 URL 可能先后命中两个进程。即使 release 一样，进程重启
也会清空内存状态、重建 worker、改变采集连续性。因此资格窗口同时绑定：

- `releaseId`：运行哪版代码；
- `machineId`：哪台 Fly machine；
- `bootId`：该 machine 的哪次进程启动；
- `window_started_at_ms/window_ended_at_ms`：哪段时间；
- `sample_count`：窗口里实际取得多少轮。

`bootId` 在 `create_app` 时生成，同一进程的 `/health` 与 `/healthz` 始终一致。

## local PASS 为什么不能冒充 production

evidence 明确带 `scope`：

```text
local-conformance
production-readonly
```

CLI 必须用 `--require-scope` 指定期望。production 还要求 40 位 SHA、非 local
machine、UUID v4 boot、合法窗口和至少五轮样本。把 local JSON 交给 production
evaluator 会得到 `scope-mismatch`，即使其中所有 SLA 数字都很好看。

## “没看到错误”为什么不等于零错误

read-only baseline 能证明 HTTP、coverage、Quote age、policy、Reconciliation 和
当前 open Incident。但它不能凭一次 GET 证明：

- fault 注入到 detect 的 MTTD；
- detect 到 contained 的时长；
- 是否曾产生 cross-membership Quote；
- 是否曾留下 orphan collecting run。

这些字段必须由后续 fault-specific durable evidence 提供。缺失会生成
`missing-*` reason，生产 verdict 保持 FAIL。这是证据缺口，不是系统故障的虚构。

## 原子与不可覆盖

collector 和 evaluator 都用 exclusive create 写新文件；已有路径会退出 2，不会覆盖。
verdict 绑定 canonical evidence SHA-256，同一 evidence 得到同一结果，不掺当前时间。
production Make target保存 evidence 与 verdict，不像 local target那样清理临时文件。

## 自检题

1. 为什么同一个 release 的两次 boot 不能拼成一段连续证据？
2. local fixture 全绿时，production evaluator 会在哪一层拒绝它？
3. 为什么 open Incident count 可以从 baseline 得到，但 MTTD 不能？
4. collector 不知道 cross-membership count 时，为什么不能填 0？
5. exclusive-create 与 evidence digest 分别防止哪类证据漂移？

## FAQ 增量

暂无。
