# Chaos Toolkit — Primitive Availability Matrix

> Phase 03 Inj L2-1 教训:`python:3.12-slim` base image 不含 `pkill`、`ps` 等
> procps 工具。假设 Unix-standard 工具存在的 chaos plan 会 silently fail
> 出"executable not found"那种容易误读为业务错误的现象。本文档把工具可用
> 性矩阵 + 替代 pattern 固化下来，避免每次 chaos plan 都靠记忆赌。
>
> **Owner**: m1-perception workstream，phase 03.1 Plan 03 (PROCESS-2) 落地。
> **关联 CLAUDE.md 章节**: "chaos 工具 image-aware 设计（强制）"。
> **关联 thread**: `.planning/threads/market-observation-architecture.md` §1.6
> chain-truth discipline（chaos 触发的 fail-soft 链验证纪律）。

## 1. 自动验证入口

任何 chaos plan 在 `<verify>` block 落地 image-availability evidence 时：

```bash
make chaos-l2-fly-image-check
```

该 target 自动从 `flyctl status -a polyarb-l2 --json` 解析当前部署 image 的
完整 ref（`registry/repo:tag`），然后对 8 个 chaos 常用 primitive 跑
`docker run --rm IMAGE /bin/sh -c "command -v TOOL"`，逐项打印 `OK` / `MISS`。

退出码语义:
- `0`: 全部 primitive 可用
- `1`: 至少一个 MISS（target 会引导你看本文档的替代 pattern）
- `2`: 本地 docker daemon 不可用（developer-local-only 限制）

## 2. 当前 production image 工具矩阵

基线: `python:3.12-slim`（polyarb-l2 Phase 03 deploy 沿用，Dockerfile 在
`docker/Dockerfile.l2`）。

| Primitive | 可用 | 安装方式 / 替代 |
|---|---|---|
| `pkill` | ✗ | `apt-get install procps`（约 +3MB image bloat）；或用 `flyctl machine restart` 替代 |
| `ps` | ✗ | 同上；只读进程信息可 `cat /proc/1/status` |
| `kill` | ✓ | shell builtin，PID 用 PID 1（daemon entrypoint） |
| `which` | ✗ | 用 POSIX `command -v TOOL` 替代（更可移植，slim image 也有） |
| `dig` | ✗ | `apt-get install dnsutils`；或用 `python -c "import socket; print(socket.gethostbyname(...))"` |
| `ping` | ✗ | `apt-get install iputils-ping`；或 `python -c "import socket; socket.create_connection(('host', 443), 2)"` |
| `curl` | ✓ | 已在我们的 Dockerfile 里（healthcheck 用） |
| `python` | ✓ | runtime |

> **不要**轻易往 image 里 `apt-get install` —— image bloat + 攻击面扩大。
> 缺工具优先用 substitute pattern（见下表）；实在不可替代且高频使用，
> 改 Dockerfile 进新 plan，不在 chaos phase 临时加。

## 3. Substitute pattern（按场景）

| 想做的事 | ❌ 假设可用 | ✅ slim image 实际方式 |
|---|---|---|
| 杀 WS 连接 | `flyctl ssh ... pkill -f ws_client` | `flyctl machine restart <id> -a <app>` — 触发 PID 1 SIGTERM，等效但更干净（也顺带验 cold-start init order） |
| WS close 从代码内触发（不重启进程） | OS-level kill | `POLYARB_WS_TEST_KILL=1` env-var-gated branch in `ws_market_client.py`（Plan 03.1-06 落地） |
| 验证 DNS 解析 | `dig gamma-api.polymarket.com` | `python -c "import socket; print(socket.gethostbyname('gamma-api.polymarket.com'))"` |
| 验证 TCP 可达 | `ping host` | `python -c "import socket; s=socket.create_connection(('host',443),2); print('ok')"` |
| 列出进程 | `ps -ef` | `cat /proc/1/status`（只看 daemon PID 1） |
| 查工具是否在 PATH | `which TOOL` | `command -v TOOL`（POSIX，shell builtin） |

## 4. Validation protocol（写新 chaos plan 必走）

每个新 chaos primitive 进 PLAN.md 前：

1. 在 plan 的 `<verify>` 或 `<read_first>` 写出 image-check 证据。要么 paste
   `make chaos-l2-fly-image-check` 输出，要么手工跑
   `docker run --rm <image> /bin/sh -c "command -v <tool>"` 把结果贴在 plan。
2. 工具缺失 → 优先选 §3 的 substitute pattern；substitute 不可行 → 不要硬塞，
   把"需要新工具"作为 deferred item，单开一个 Dockerfile-change plan。
3. plan-checker 在 review 时**必须**核查这条证据存在。

## 5. 历史代价（为什么有这份文档）

Phase 03 Inj L2-1 原设计 `pkill -f ws_market_client` from inside container。
跑到一半发现 python-slim 没 procps → 全 chaos 测试改用 `flyctl machine
restart` substitute → 用了半天才回到正轨。完整 narrative 见 Phase 03
LEARNINGS L2 + 03-SOAK-LOG Inj L2-1 段。代价：一次 chaos cycle 的时间 +
chaos truth 的延迟暴露。本文档让下次能在 plan-time 就被挡下来，而不是 chaos-time。
