# 事件内嵌 Structure 源

## 30 秒心智模型

旧的事务源先封存所有开放 event，再把每个 market ID 分成数千个批次重新向
Gamma 查询。这在数据库语义上很严谨，却不是一个可在线完成的快照：市场在几十
分钟的二次抓取过程中会关闭，冻结成员集因此必然失真。

Gamma 的 `/events/keyset` 响应本身已经带着每个嵌套 market 的 Structure 字段。新
路径把**已封存的 event 页**作为唯一原始证据，在物化时确定性地展开 market；父
event 的 `negRiskMarketID` 是唯一需要向子 market 继承的字段。于是事件 cursor
终态直接释放 materializer，不再创建数千个可变的二次 market job。

```text
events:0 → events:1 → ... → events:terminal
                                      ↓
                         complete + materialize lease
                                      ↓
                     Structure range → certify → Quote
```

## 代码地图

- [`gamma_client.py`](../../src/polyarb/clients/gamma_client.py:793) 保留嵌套
  market 的 token、价格、状态和流动性字段。
- [`structure_source.py`](../../src/polyarb/control_plane/structure_source.py:258)
  从封存 event 记录展开 market，并从父 event 注入组 ID。
- [`postgres.py`](../../src/polyarb/control_plane/postgres.py:909) 在同一事务中
  把 terminal event receipt 转成 `complete` 窗口和唯一 materializer job。

## 设计取舍

- **不是放宽完整性**：每个 event 页仍有 opaque cursor、R2 HEAD 验证、fenced
  PostgreSQL receipt；任何 malformed nested market 或 event/market truth 冲突仍
  会阻止 bundle 产生。
- **不是隐式切换旧窗口**：已有 `gamma-source-window-v1` 双流 evidence 仍可物化；
  新窗口使用显式 `gamma-source-window-events-v2` 身份。
- **消除可变二次读取窗口**：这不是靠无限增大并发掩盖 race，而是让 Structure
  使用一条同源的 Gamma evidence 链。

## 自检题

1. 为什么 `event_embedded_markets=True` 必须只能出现在 terminal event 页？
2. 父 event 的组 ID 为什么可以确定性注入 child market，而不能从另一轮 API 查询？
3. 为什么 v2 bundle 不能伪装成 v1 identity？

## FAQ 增量

### 旧的八 lane market pool 是否被删除？

没有。它仍可安全处理已落库的旧 scoped market job；但新的 event-rooted 窗口不再
产生这种 job。线上吞吐的改进来自缩短正确性边界，而不是提高无界并发数。
