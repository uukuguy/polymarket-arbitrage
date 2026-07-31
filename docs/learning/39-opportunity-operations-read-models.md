# 机会运维读模型：Dashboard 不是数据库浏览器

## 30 秒心智模型

M1 Dashboard 的任务不是“把表里的东西画出来”，而是回答三个可操作问题：

1. 现在有哪些经过认证、仍可执行的机会？
2. 全市场发现与 reconciliation 最近推进到哪里？
3. 某个组为什么进入、离开机会状态，异常是否真正恢复？

答案必须来自后端认证的有界读模型。前端只负责严格校验和展示，不自行拼接多次请求、不把未知换成零，也不从浏览器时钟推导服务端状态。

```text
durable authorities
        ↓ one bounded authenticated response
strict Dashboard validator
        ↓
operator decision, never execution
```

## 代码地图

- `src/polyarb/perception/store.py:6213`：认证当前机会页，绑定 current authority。
- `src/polyarb/perception/store.py:6847`：当前 reconciliation 及已验证时长/diff。
- `src/polyarb/perception/store.py:7735`：同一事务读取 group timeline 的三类 candidate 来源。
- `src/polyarb/http/perception.py:872`：group-bound canonical cursor。
- `src/polyarb/http/perception.py:1324`：四源 timeline HTTP 契约。
- `dashboard/app/perception/page.tsx:165`：当前机会到 group timeline 的直达入口。
- `dashboard/app/perception/[group_id]/page.tsx:72`：四类证据的统一运维呈现。
- `scripts/perception_dashboard_fixture.py`：真实 store API 构造的确定性视觉验收数据。

## “0”与“不可用”为什么必须分开

`0 opportunities` 是完成读取后的业务事实；`unavailable` 是无法证明业务事实。二者若画成同一个空列表，操作员会在数据链路故障时误判“市场没有机会”。

因此页面只接受两种显式分支：

```text
available + authenticated empty page → 0
timeout / invalid contract / 503      → unavailable
```

同理，后端没有历史分布时只能写“not tracked”，不能用当前窗口伪造 p95。

## 为什么资源 TTL 使用 server_time_ms

浏览器时间可能漂移，SSR 与 hydration 也可能发生在不同时刻。资源策略的 `decided_at_ms` 和 `valid_until_ms` 都属于服务端权威，所以 age/TTL 必须以同一响应的 `status.server_time_ms` 为基准：

```text
policy_age = server_time_ms - decided_at_ms
ttl_left   = valid_until_ms - server_time_ms
```

这保证截图、重放和操作判断对同一份响应有唯一答案。

## fixture 为什么也要走真实写入 API

视觉验收若手写 JSON，很容易展示生产永远不会生成的字段组合。fixture 通过 `begin_reconciliation → publish_reconciliation_batch → apply_reconciliation_diff` 和真实 incident 状态机写入 SQLite，再由正式 HTTP 读模型读出。

它证明的是：

- 页面能渲染真实合同；
- 长 ID、四类 timeline、unavailable 在桌面与 375 px 下可用；
- reconciliation 的 `duration_ms` 与 diff 不是 mock 文案。

它不证明云端进程健康，也不授权部署。

## Incident 为什么先展示字段、再展示 raw JSON

操作员首先需要 action、retry count、next retry、success receipt 和 verification。整段 JSON 是审计兜底，不应成为第一阅读层。因此已知字段提升为标签行，完整 evidence 放在折叠区；未知字段仍不丢失。

## 设计取舍

- 当前机会 ID 必须直达 timeline，不能依赖另一个有界 groups 页“碰巧包含它”。
- `duration_ms` 只代表当前已验证 reconciliation 窗口；历史分布尚未跟踪。
- incident authority 证明生命周期证据，不证明外部通知已经送达。
- 横向表格在移动端使用局部滚动容器；页面本身不得横向溢出。
- Dashboard 是 observer-only，所有读模型都不能触发 wallet、order 或 trade。

## 自检题

1. API 超时时为什么不能显示 `0 opportunities`？
2. 为什么当前机会必须直接链接 timeline，而不是只在 groups 页提供链接？
3. 用 `Date.now()` 算服务端策略 TTL 会破坏什么性质？
4. 手写 fixture JSON 可能绕过哪些生产不变量？
5. 为什么结构化 incident 字段和 raw evidence 两层都需要保留？

## FAQ 增量

暂无。
