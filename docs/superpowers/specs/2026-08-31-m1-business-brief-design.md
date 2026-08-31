# M1 业务情报简报设计

**日期：** 2026-08-31  
**状态：** 已获方向确认，待规格复核

## 目标

将 M1 已有的只读权威事实收束为一个日常业务入口：

```bash
make control-plane-business-brief
```

让操作者在一屏内得到当天结论，再按 Structure、Quote、资格、认证机会、异常与恢复
分区追问；同时支持同一份已收缩事实的机器可读 JSON，作为日报自动化的稳定输入。

## 范围与边界

简报只组合两个已有权威读：

1. `make control-plane-status limit=20` 的 durable M1 状态；
2. `GET /perception/opportunities?limit=50` 的当前认证机会 projection。

它不新增数据库 schema、状态计算、调度、部署、secret、钱包、订单或交易。M1 仍是
observe-only：任何 opportunity/edge/size 都是 M2 研究候选，不是成交、收益、P&L 或
下单授权。

若任一权威读不可用，简报必须显式写 `业务数据不可用` 并以非零退出；不得把失败、
缺字段或零行转换为“暂无机会”。

## 单一命令，三个阅读层

| 模式 | 用法 | 输出 |
| --- | --- | --- |
| 默认 | `make control-plane-business-brief` | 一行业务结论 + 五个分区表格 |
| JSON | `make control-plane-business-brief format=json` | 同一摘要的稳定 JSON 对象 |
| 审计下钻 | `make control-plane-status limit=20` / `make control-plane-opportunities limit=50` | 原始权威事实与完整机会页 |

默认输出的固定分区：

1. **今日结论**：可用/暂停/不可用、认证机会数、是否需要升级；
2. **市场覆盖（Structure）**：最新 manifest/generation、记录数、发布时间或 freshness；
3. **报价（Quote）**：current pointer、父 Structure、record count、发布时间；
4. **资格与机会**：`eligibility_state` / reason、机会 count、最多 5 个 group/event/edge/size；
5. **异常与恢复**：open/runtime incidents、recovery actions、watchdog current/recent event。

JSON 使用上述相同五块，字段名固定、显式 `status` 和 `unavailable_reason`。首版不计算
额外风险评分、趋势或 P&L。

## 实现

增加一个本地 Python CLI 模块，负责：以已有命令使用的 scoped DSN 读取 status，再以
当前控制 API 读取 opportunities；对两个响应执行最小 shape validation；构造一个纯
business-brief dict；按 `format=text|json` 渲染。Makefile 只暴露该 CLI，`format` 只允许
`text`（默认）或 `json`。

机会 HTTP 请求必须继续使用已验证的安全 transport 原则：bounded timeout、GET、参数
URL encoding、失败非零。不得重新引入 Make/shell 参数插值。

## 操作指南

更新 `docs/learning/106-M1日常业务情报操作指南.md`：

- 将 brief 作为每日默认第三步；
- 解释三种阅读层的用途与何时下钻到原始 status/opportunities；
- 给出一份默认 text 输出的逐区解释及 JSON mode 的自动日报用途；
- 保留并引用现有 08:30 基线、09:00–23:00 每 15 分钟的 active-session 节奏与升级规则。

## 验收

- `make help` 暴露 `control-plane-business-brief`；
- fixture/假响应测试证明 text 与 JSON 来自同一摘要，含五个固定分区；
- 一个 `available + 0` 响应显式显示“暂无认证机会”；
- status 或 opportunities 不可用时非零退出，并显示“业务数据不可用”；
- 机会输入仅显示前 5 个，原始审计命令仍可获得完整页；
- 指南说明 brief / audit / JSON 三层及 observe-only 边界；
- 新命令不写库、不触发 scheduler、不访问 wallet/order/trade。
