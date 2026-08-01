# Quote 投影与阶段收据

## 30 秒心智模型

Quote 一轮采集不能先把整个 Structure 世界搬进 Python，再从中挑目标。生产路径现在分成
三件事：用索引 SQL 只投影可报价的 standard/complete-supported 组；用不可变收据把这次
投影绑定到 Structure revision；用持久化阶段 checkpoint 说明采集卡在 universe、fetch、
persist 还是 certify。上一轮已认证 feed 在新轮完整认证前继续服务，新轮异常则留下可告警、
可追踪的失败事实。

```text
Structure truth ──目标索引投影──▶ projection receipt
                                     │ O(1) admission fence
                                     ▼
universe → admission → fetch → transform → persist → certify → projection
                                     │
                          新版本原子切换；旧版本此前继续服务
```

## 关键代码地图

- `src/polyarb/routing/neg_risk_quote_store.py:1133`：只读取目标组的认证 universe；
  augmented/unsupported 只生成组级 rejection，不展开全部成员。
- `src/polyarb/routing/neg_risk_quote_store.py:347`：开始运行时验证 Structure 收据和 revision，
  不再重复扫描 universe。
- `src/polyarb/routing/neg_risk_quote_store.py:875`：从 run-bound legs/source receipt 重建已认证
  projection，新运行不进行第三次 Structure 扫描。
- `src/polyarb/routing/neg_risk_quote_store.py:171`：持久化 collection attempt 与阶段 checkpoint。
- `src/polyarb/daemon/quote_worker.py:320`：父进程创建 attempt、校验子进程的阶段与耗时合同。
- `src/polyarb/http/health.py:1474`：checkpoint 45 秒未推进为 warn，120 秒未推进为 fail。
- `src/polyarb/storage/sqlite_store.py:2366`：legacy Structure 在所有源行写完、COMMIT 前清除
  revision dirty，形成真实原子发布边界。

## 为什么 revision 不能每行递增

Structure 一次会覆盖十万级市场。若每行都更新同一 revision 行，就会制造一个写热点。
触发器采用“第一次变更才写”的 coalesced fence：批量写期间只发生一次 revision update 和
一次 dirty insert；完成发布时清 dirty。Quote 投影只接受 clean revision，投影后若又发生
任何源变更，admission 会 fail closed。

这里最容易犯的错误是把 `market_view_published=1` 的 metadata INSERT 当成发布完成。实际
writer 随后仍写 truth、membership 和 markets；真正对外可见的边界是事务末 COMMIT。因此
clean 动作必须紧邻 COMMIT，而不能由 metadata INSERT 触发。

## 为什么旧 feed 可以继续服务

采集中的新版本不是当前版本。当前 feed 只引用同一个已认证 run 的 Structure identity、legs
和 quotes；新轮只有通过 persist、certify、projection 后才整体切换。因而“继续服务旧 feed”
不是混用新旧数据，但旧 feed 自己仍受 240 秒预警、300 秒 fail-closed 时钟约束。

## 自检题

1. 为什么 unsupported 组应保留 rejection，却不应读取它的十万级成员？
2. projection receipt 已有 universe hash，为什么 admission 仍要检查 revision fence？
3. 为什么 collection attempt 必须在父进程 spawn 前落库？
4. 新采集卡在 fetch 时，为什么当前已认证 feed 不应立刻降级为不可用？
5. 为什么 Structure clean 只能发生在全部源行写完、COMMIT 前？

## FAQ 增量

### Q：阶段 checkpoint 能不能代替 child timeout？

不能。checkpoint 提供“卡在哪里、卡了多久”的证据，timeout 提供强制资源边界；两者分别
解决可诊断性和有界执行问题。
