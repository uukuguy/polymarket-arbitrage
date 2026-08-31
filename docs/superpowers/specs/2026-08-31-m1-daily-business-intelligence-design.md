# M1 日常业务情报操作设计

**日期：** 2026-08-31  
**状态：** 已获方向确认，待规格复核

## 目标

让操作者每天能用正式、只读的项目命令回答业务问题，而不是从 worker 日志或
遗留 L1 URL 猜测：

1. M1 是否能提供可信的市场观察？
2. 今天覆盖了哪些市场、报价链是否在推进？
3. 当前是否存在经过认证的套利机会？
4. 没有机会、资格暂停、数据不可用三种情况分别意味着什么？
5. 今天应记录什么结论，什么情况下才需要升级排障？

M1 仍是 observe-only 市场感知系统。本设计不创建订单、不提供交易授权，也不把
机会数量或 gross edge 表述为收益、成交或 P&L。

## 用户视角与信息层次

操作指南先呈现业务结论，技术状态仅作为结论可信度的证据：

```text
市场覆盖（Structure）
       ↓
可执行报价（Quote）
       ↓
认证机会（Opportunity projection）
       ↓
资格/异常证据（能否信任以上结论）
```

每次日常收集的结论必须使用明确状态，而不是空白推断：

| 状态 | 业务含义 | 操作结论 |
| --- | --- | --- |
| `available` + 机会数 > 0 | 已有当前认证机会 | 作为 M2 研究候选，仍不可下单 |
| `available` + 机会数 = 0 | 已完成读取，当前无认证机会 | 记录“暂无机会”，不是故障 |
| `paused` | 系统仍运行，但资格门暂未满足 | 记录暂停原因，观察恢复 |
| `unavailable` / HTTP 503 | 无法证明机会真值 | 记录“业务数据不可用”，排障而非报零 |

## 正式命令入口

### 1. 公共可达性

保留既有命令：

```bash
make smoke-control-plane-prod
```

它只能证明 public control API 可达和 readiness 已认证；它不回答是否有机会。

### 2. 业务运行与资格证据

保留既有命令：

```bash
make control-plane-status limit=20
```

它从 durable authority 读取 Structure、Quote、qualification、active tasks、incident
与 watchdog 证据。指南将提供面向业务的 `jq` 摘要和逐字段解释，不要求操作者从
完整 JSON 中寻找结论。

### 3. 当前认证机会

新增正式只读入口：

```bash
make control-plane-opportunities limit=50
```

它固定访问当前生产 authority：
`https://polyarb-control-api.fly.dev/perception/opportunities`，可选
`limit=1..500` 与 `after_group_id=`。命令必须：

- 使用 bounded timeout，禁用 curl config；
- 保留 HTTP 非 2xx 失败语义，绝不能将 503 渲染为空机会页；
- 原样格式化经过认证的 projection；
- 不读取本地 SQLite、不需要密钥、不触发 scheduler、wallet、order 或 trade。

遗留的 `perception-opportunities` 仍指向已退役的 `polyarb-l1.fly.dev`，不再能作为
生产业务入口。此次只增加正确入口；是否在后续单独 retire 它，取决于其是否仍有
本地/历史兼容用途，避免静默改变旧接口。

## 文档产物

1. `docs/learning/106-M1日常业务情报操作指南.md`
   - 30 秒业务心智模型；
   - 每日三步检查、建议频率和直接可运行的 Make 命令；
   - Structure / Quote / Opportunity / qualification / incident 的业务释义；
   - 业务判断矩阵、日报模板、升级条件；
   - 明确非结论（无订单、无成交、无 P&L）；
   - 代码地图、取舍、自检题和 FAQ 增量区，遵循 learning 文档体例。
2. `docs/learning/00-INDEX.md`：加入第 106 篇及阅读目的。
3. `docs/ops/m1-daily-business-intelligence-log.md`：append-only 日报模板及第一条
   空白记录。每天只追加一条带北京时间与命令证据的观察，不重写历史。

## 验收标准

- `make help` 可发现 `control-plane-opportunities`，并准确声明只读业务用途。
- 新目标对生产 control API 的实际 200 响应能输出 JSON；对 503 保持非零退出，不能
  伪造成 `items: []`。
- Makefile 合约测试覆盖 URL、分页参数、无 secret 与错误语义。
- 操作指南可让未读源码的用户区分：系统可达、业务链推进、零机会、资格暂停、数据
  不可用。
- 日报模板不会把认证机会误写成成交、利润或可直接执行订单。

## 非目标

- 自动定时任务、Telegram 推送、数据库 schema 变更；
- M2 下单、仓位、成交或 P&L 统计；
- 仅凭控制面任务数推断市场机会。

## 风险与取舍

- 直接暴露完整 projection 优先保证审计真值；首版不在 shell 中发明摘要/评分逻辑，
  防止展示层把未知或跨字段推断伪造成业务事实。
- 日报是人工附证据的观察账本，不是自动化历史数据库；若后续需要自动日报，应另立
  功能并定义保留、时区、告警与失败投递语义。
