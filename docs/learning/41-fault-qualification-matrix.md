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
- `src/polyarb/perception/gamma_incidents.py`：只把可证明的 Gamma timeout、
  malformed、cursor integrity 异常写成 durable Incident；未知错误不冒充 Gamma。
- `src/polyarb/perception/clob_incidents.py:24`：保守识别 Candidate 的缺腿、SDK
  429、有界超时和 SQLite `BUSY/LOCKED`，并以 `candidate:<group_id>` 隔离故障；
  `no such table` 等其他 OperationalError 不冒充资源争锁。
- `src/polyarb/perception/candidate_watcher.py:397`：Candidate 先提交原子终态，再排队
  exact-group recovery/failure transition；`:659` 在本轮候选服务后统一 flush，避免
  incident 写路径吃掉 reserved lane 的时间预算。
- `src/polyarb/perception/resource_controller.py`：在原有 authenticated Resource
  history 中 additive 记录 disk free 与 load-per-CPU；旧 v1 JSON 缺字段时解释为
  unknown，不改写旧 hash。
- `src/polyarb/perception/resource_incidents.py`：把真实 `disk-pressure` /
  `host-contention` decision 转为 resource-scoped lifecycle；普通 Quote 慢不冒充
  host contention。
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
3. **异常与数据质量分开**：Gamma timeout/malformed/cursor 已有 durable incident；
   shape 合法的 partial page 进入 coverage/rejected 事实，不伪造 producer failure。
4. **Candidate 以组为故障边界**：一个 group 缺腿、429 或 latency 不能把全部
   Candidate 标成不可用；恢复也只能由同一 group 的 current-membership Quote receipt
   证明。终态事实同步提交，Incident 转换批后 flush；normal/explore reserved slots
   有界并发启动，且 timeout 用 per-group attempt 计数，不能被 sibling 成功污染。
5. **cleanup 串行门**：上一故障清理失败时，后续结果会混入多个变量，证据不再可归因，
   所以整个矩阵必须停止。

## 自检题

1. `candidate-exit` 后进程重新出现，为什么还不能把 Incident 标为 verified？
2. Gamma partial page 为什么不能直接复用 `gamma-malformed` Incident？
3. 为什么 `authorization=fault:clob-429:<sha>` 不能执行 `clob-latency`？
4. cleanup 成功但 `bootId` 在窗口中改变，为什么整段 evidence 仍必须失败？

## FAQ 增量

### 为什么现在所有 execute 都拒绝？

除 `candidate-exit`、`discovery-exit`、`reconciliation-stall` 外的 execute 仍拒绝。
矩阵契约和生产注入能力是两个独立交付物。
先冻结 fault、cleanup、writer 和证据结构，再逐个实现 adapter，能避免临时 SSH 命令
绕过原子化与可恢复性要求。两个 producer-exit 使用同一完整模板：clean baseline →
exact machine/boot/PID intent → SIGTERM → scoped recent Incident discovery → exact
terminal history → component-specific writer receipt → clean post-window。接口可执行仍不等于生产 mutation 已获授权。

### 为什么 Reconciliation stall 不直接把 restart timeout 改成 30 秒？

一次全窗口可能持续十分钟，但 Reconciliation 每批只提交一页。正常 60 秒
inter-page wait 期间，child 每 12.5 秒写认证 `yielded`；25 秒收不到任何 child
liveness 才持久化 `child-stalled`。这只负责发现和 containment，不杀进程。
180 秒 hard timeout 仍独立负责重启。SIGCONT 后必须出现新的页 checkpoint 才能
verified，所以 liveness 不会伪造业务恢复。

### Gamma 的 warning log 现在还缺什么？

timeout、malformed JSON/invalid member、cursor integrity 已不再是 log-only：
Runner 会立即写 `gamma-timeout`、`gamma-malformed` 或 `gamma-cursor`，并进入
recovering。下一次成功页必须返回本次原子 publish 的 exact Discovery batch ID
或 Reconciliation window ID，才能 verified。尚未完成的是生产 fault 注入 adapter，
因此这些 fault 的 `execute_supported` 仍为 false；“运行时可观测”不等于“已获准注入”。

### CLOB 组失败为什么不在每次请求返回前同步写 Incident？

Candidate 的第一职责是让每个组得到一个原子 terminal fact，并守住 high 与
normal/explore 的连续性预算。若超时处理立即另起 SQLite incident 写线程，它会和下一组
的 Structure 读取争锁，把本地排队误报成新的 CLOB latency。现在 missing-leg、429、
timeout 和成功恢复都先排入内存中的当前 cycle 操作队列，reserved lanes 服务完后统一
落入 durable Incident。Dashboard/API 看到的是持久化后的
`candidate:<group_id>` lifecycle；日志不是 authority。正常 scheduler cycle 返回前会
完成 flush，所以这不是无管理的后台任务。

### SQLite busy 为什么不是 `child-failed`？

Candidate scheduler 对单组失败是 fail-soft 的，SQLite terminal writer 被锁并不会自然让
producer 子进程退出；把它写成 `child-failed` 会要求一个实际不会出现的 supervisor
事件。现在只接受 SQLite 的 `BUSY/LOCKED` code 或精确标准消息，先由 Candidate 自身
排队；若 terminal unavailable writer 也被锁而异常上抛，scheduler fallback 仍保留同一
group/error，cycle 末锁释放后写成 `sqlite-busy`。恢复仍需同组 current-membership
Quote success receipt，不能用“数据库后来能连接”代替业务恢复。

### 为什么磁盘和 contention 不能只看 `/proc` 或一条日志？

瞬时 OS probe 没有历史身份，日志又不能参与 recovery writer 校验。Resource controller
把 sample、阈值、decision、sequence 和 hash chain 一起持久化；这让 Dashboard 能回答
“哪个阈值在何时触发了哪次 shedding”。Incident 再引用该 decision 开始 lifecycle。
恢复时 `IncidentManager` 会反查更新且通过完整 history replay 的 Resource decision，
并额外要求 `mode=normal`、`health_claimed=true`。另一个 pressure decision 即使 ID 更新，
也不能恶意关闭 Incident。
