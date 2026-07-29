# 故障资格矩阵：注入不是重点，证据闭环才是

## 30 秒心智模型

一次生产故障实验不是“让某个请求失败，然后看它恢复”。完整闭环是：

```text
exact runtime baseline
  → 单一、可清理的 fault
  → durable incident detected/classified/contained/recovering
  → 故障源清理
  → 组件自己的成功 writer 产生新证据
  → incident verified
  → release-bound verdict
```

日志只能帮助定位，不能替代 durable incident；进程重新启动也不能替代业务 writer
恢复。当前 16 类计划由 `scripts/perception_chaos.py` 定义，默认 Make 入口只打印
计划。标有 `not-wired:` 的项是在告诉你链路还缺什么，而不是一种可以接受的结果。

## 关键代码

- `scripts/perception_chaos.py`：故障 ID、组件、expected incident、恢复 writer、
  cleanup 和 image requirements 的单一来源。
- `src/polyarb/perception/supervisor.py`：producer 失败生成
  `child-timeout` / `child-failed` / `child-abandoned`，并进入恢复状态。
- `src/polyarb/perception/incidents.py`：verified transition 会反查 recovery 之后的
  Candidate receipt、Discovery batch、Reconciliation window、HTTP probe 或 Resource
  decision，不能靠调用者自报成功。
- `src/polyarb/perception/chaos_primitive.py`：不用 `ps/pkill`，从 `/proc` 解析唯一的
  daemon 直属 Candidate worker，并在 release/PID/fault 授权全部一致后才发 SIGTERM。
- `src/polyarb/http/perception.py`：exact Incident ID 的 bounded lifecycle 读面；让
  verified terminal 与 writer receipt 在 Incident 从 open 列表消失后仍可查询。
- `scripts/perception_fault_acceptance.py`：`production-fault` verdict 再绑定
  release/machine/boot/window，并同时检查全局 SLA 与 open incident。

## 设计取舍

1. **默认 plan-only**：列出目标不等于授权执行；同一 Make target 只有显式
   `mode=execute` 才进入 mutation 分支。
2. **fault-specific authorization**：授权串同时绑定 fault ID 和 40 字符 release
   SHA，不能拿一次批准复用到另一种故障或另一版代码。
3. **先承认 not-wired**：Gamma 批内异常、部分资源/通知路径目前只有日志或状态，
   没有 durable incident。先暴露缺口，再补 chain-truth，比造一个看似绿色的矩阵可靠。
4. **cleanup 串行门**：上一故障清理失败时，后续结果会混入多个变量，证据不再可归因，
   所以整个矩阵必须停止。

## 自检题

1. `candidate-exit` 后进程重新出现，为什么还不能把 Incident 标为 verified？
2. Gamma malformed 只有 warning log 时，MTTD 能否进入资格证据？缺的 durable 写入
   是什么？
3. 为什么 `authorization=fault:clob-429:<sha>` 不能执行 `clob-latency`？
4. cleanup 成功但 `bootId` 在窗口中改变，为什么整段 evidence 仍必须失败？

## FAQ 增量

### 为什么现在所有 execute 都拒绝？

除 `candidate-exit`、`discovery-exit` 外的 execute 仍拒绝。矩阵契约和生产注入能力是两个独立交付物。
先冻结 fault、cleanup、writer 和证据结构，再逐个实现 adapter，能避免临时 SSH 命令
绕过原子化与可恢复性要求。两个 producer-exit 使用同一完整模板：clean baseline →
exact machine/boot/PID intent → SIGTERM → scoped recent Incident discovery → exact
terminal history → component-specific writer receipt → clean post-window。接口可执行仍不等于生产 mutation 已获授权。

### 为什么 Reconciliation SIGSTOP primitive 已有，execute 仍拒绝？

“能制造故障”和“能满足生产检测 SLA”是两件事。当前 restart timeout 是 180 秒，
可避免正常慢批次被频繁误杀；但等到 timeout 才创建 Incident 会违反 MTTD ≤30 秒。
正确结构是 30 秒内持久化 `child-stalled` 并 contained，继续观察到统计驱动 timeout
才重启。不能把 restart timeout 降到 30 秒来换一个表面 PASS。
