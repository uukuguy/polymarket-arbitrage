# PostgreSQL 连接池与压力可观测

## 30 秒心智模型

一次 Structure wave 有 12 条 lane。旧实现的每个 claim、heartbeat、progress 和 receipt
都会建立新的 PostgreSQL 连接并重复 session bootstrap；高并发时，生产失败发生在
`connect()`，不是业务 SQL。把 connect timeout 调大只会让更多 lane 同时卡住，扩大故障。

现在每个 worker 进程拥有一个 lazy bounded pool；业务 lane 上限仍是 32，但生产 pool
默认只有 2 条物理 session。API 为隔离 readiness 使用 1+1 两个 owner，controller 为 1；
完整部署中所有 DB owner 的声明预算合计 12，为运维恢复保留 3 个 Supavisor session；
其中 runtime-event-writer 也只有一条复用连接，不再按 HTTP 请求无界建连。启动时连接数为
0，有真实并发才增长，事务结束后连接归还而不是关闭。
取连接、建连和重连都服从原有数据库 deadline；池满时 lane 在最多 32 个 waiter 内有界排队，
失败，错误被标为 `database.unavailable`。API 即使读模型返回 503，也能输出不含 DSN 的
池压力计数，区分数据库不可用、池等待和 API 进程死亡。

```text
12 lanes -> one process-local pool -> reused PostgreSQL sessions
                 |                         |
       max session 2 / waiters 32     with block returns
                 |
       PoolTimeout / OperationalError
                 |
       database.unavailable + 503 pool counters
```

## 代码地图

- `src/polyarb/control_plane/db_deadlines.py:7`：32 是 lane/waiter 上限，2 是默认物理 session
  上限；两者不能再用同一个常量表达。
- `src/polyarb/control_plane/db_role_contract.py:146`：`ScopedConnectionFactory` 同时承担
  callable、pool owner、close 和 stats 四个职责。
- `src/polyarb/control_plane/db_role_contract.py:171`：lazy pool 的连接、等待、重连都复用
  同一个 `DatabaseDeadlinePolicy`。
- `src/polyarb/control_plane/db_role_contract.py:200`：每条新物理连接仍执行并验证 search
  path、statement timeout 和 lock timeout，池化不削弱数据库角色契约。
- `src/polyarb/control_plane/postgres.py:159`：`OperationalError`、`PoolTimeout`、
  `TooManyRequests` 与 `PoolClosed` 归类为 `database.unavailable`，不再冒充输入校验失败。
- `src/polyarb/control_plane/postgres.py:1250`：只投影五个安全池计数，绝不输出 DSN、用户名
  或连接参数。
- `src/polyarb/control_plane/postgres.py:1279`：operational/readiness 双池按 owner 去重关闭。
- `src/polyarb/http/control_plane.py:17`：读模型失败时仍可从进程内读取池压力，无需再访问
  PostgreSQL。

## 为什么 `with connection_factory()` 不需要大改

旧调用点已经使用：

```python
with connection_factory() as connection:
    ...
```

直接连接的 context manager 在退出时提交或回滚并关闭；pool 的 context manager 在退出时
同样提交或回滚，但把健康连接归还池。调用点的事务边界不变，所以不用修改数百个 repository
方法。真正改变的是物理连接生命周期：从“每个事务一条新 TCP/TLS/libpq session”变成
“每个进程一组有界、可复用 session”。

池的 `configure` callback 只在创建新物理连接时运行。它仍会执行 session bootstrap 并
读回验证；配置不安全的连接不会进入池。这样既减少重复建连，又保留 namespace 与 deadline
的 fail-closed 保证。

## 为什么 lane 是 32、pool 却只有 2

`clob_batch_max_concurrency` 和 `structure_range_max_concurrency` 的合法上限都是 32，但这只是
进程内业务调度上限。生产 DSN 走 Supavisor session mode，同一 database/role 的客户端上限是
15；每个进程都允许 32 条连接，会让部署上限变成 `32 × Machine 数`，lazy 只能避免启动预建，
不能阻止并发峰值把 session 留满。

因此 worker `max_size=2`、API `1+1`、controller `1`；再计入 qualification `1`、alert
delivery `1` 和 runtime-event-writer `1`，完整声明上限是
`3×2 + 2 + 1 + 1 + 1 + 1 = 12`，仍为精确恢复保留 3 条。12 条 lane 不会被删除，而是
通过最多 32 个 waiter 在 2 条短事务连接上复用；真实 PostgreSQL 测试用两轮 12 并发证明
连接数保持为 2。

`min_size=0` 很重要：controller、API 等 Machine 不应为了统一配置预建 session。API 的
operational/readiness owner 分别为 1/1；独立 readiness 不会把部署预算悄悄翻倍。超过
60 秒未使用的 burst session 会被释放：60 秒来自最慢 30 秒 controller 周期的两个完整
空闲周期，不是为掩盖故障随意增加的 timeout。

取连接的 `timeout` 和后台 `reconnect_timeout` 都等于现有 `connect_timeout_seconds`。这里
没有另造 29 秒、60 秒或 120 秒外层时钟；同一数据库 I/O authority 决定何时失败。

## 如何读 503 的池压力

`database_pool.operational` 与可选的 `readiness` 只包含：

- `pool_size`：当前池管理的物理连接数；
- `pool_available`：当前空闲连接数；
- `requests_waiting`：此刻正在排队等待连接的请求数；
- `requests_errors`：累计取连接失败数；
- `connections_errors`：累计物理建连失败数。

典型判断：

- API `healthz=200`、读模型 503、`connections_errors` 增长：进程活着，数据库建连失败；
- `requests_waiting>0` 且 `pool_available=0`：本进程内部出现池压力；
- `pool_available>0` 但 SQL 仍 503：更可能是 statement/permission/schema 问题；
- 连 `healthz` 都不可达：才进入 API Machine/process 层诊断。

这些累计计数不能单独证明当前仍坏；必须结合本次 503、当前 waiting 与后续恢复样本。它们
是诊断证据，不是新的健康判决 authority。

## 可中断与关闭所有权

池化会把“退出 `with` 就关闭物理连接”改成“退出 `with` 只归还连接”。因此必须新增明确
owner：`ScopedConnectionFactory.close()` 关闭池，`PostgresControlPlane.close()` 对双池
去重关闭，API 在 Uvicorn 返回后、CLI 在每个 command 分支返回后都于 `finally` 调用它。
qualification service 复用已经完成角色校验的同一个 factory，不再暗中创建第二个池。析构
只做异常退出的最后兜底，不能代替正常关闭路径。

测试也暴露了同一类序列问题：fixture 在 `TRUNCATE` 事务提交前 `yield`，等于把表锁持有
到整个测试结束，随后所有写入只会表现成随机 lock timeout。正确顺序是先退出事务提交，
再把 control-plane 交给测试。timeout 不是根因，资源所有权和任务顺序才是根因。

## 设计取舍

1. **复用同步 pool，而不全面改 asyncpg。** 现有 repository 是同步事务并通过有界线程桥
   调用；改 async 数据层会扩大迁移面，不能更直接地解决建连风暴。
2. **保留独立 readiness pool。** readiness 有更短 deadline，不能被 operational pool 的
   长事务挤占；API 关闭时两者由同一个 control-plane owner 收口。
3. **503 不降级为空数据。** 池化降低故障概率，但不能把数据库不可用伪装成零任务或零机会。
4. **累计错误不自动开新资格 epoch。** 数据库短暂不可用应暂停、告警并恢复；只有数据真值、
   围栏或 release/config 身份被破坏才作废历史。

## 自检题

1. 12 条 lane 每条每秒做四次数据库操作时，为什么 5 秒 connect timeout 仍可能制造风暴？
2. 为什么 lane/waiter 可以是 32，而 pool `max_size` 必须按整个部署的 15 条 session 反推？
3. 一个 `PoolTimeout` 为什么不能标成 `upstream.timeout` 或 `validation.failed`？
4. `requests_errors=3` 但当前 `requests_waiting=0`，能否断言数据库仍不可用？
5. 为什么 session bootstrap 必须放在 pool `configure`，不能因为 startup options 已设置就删掉？
6. 如果短命 CLI 不关闭 pool，为什么测试删库、角色轮换和进程优雅退出都会变得脆弱？

## FAQ 增量

### 为什么不用把 pool_size 直接设成默认 lane 数 12？

因为 lane 是可排队的短事务工作，session 是跨 Machine 共享的供应商稀缺资源。合法 lane
允许 1..32，并不意味着每条 lane 必须独占一条物理连接。固定 12 会让三类 worker 单独就
声明 36 条 session，远超当前 runtime 登录的 15 条额度；正确做法是保留 lane 并发，把
短事务复用在部署预算内的少量连接上。

### pool 能保证数据库永远不出错吗？

不能。它消除的是高频新建连接导致的自激故障，并把压力限制在进程和部署双重预算内。供应商故障、
权限漂移、statement timeout 和网络分区仍会发生；系统需要 typed 503、incident、恢复
预案和健康有效秒来处理这些预期故障，而不是要求 24 小时绝对零错误。
