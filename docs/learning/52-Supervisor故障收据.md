# Supervisor 故障收据：把“重启了”变成可诊断事实

## 30 秒心智模型

Quote 有三层边界：抓取 child、常驻 worker、外层 supervisor。任何一层失败都不该
只留下“重启中”。supervisor 会写一条有界 receipt；Dashboard 将它与最新 Quote
checkpoint 放在一起，所以可以判断失败发生在启动 child 前，还是 child 已运行后。

## 代码地图

- `src/polyarb/perception/supervisor.py:ProducerSupervisor.run` — spawn/control
  异常只写稳定异常类型，绝不写异常消息。
- `src/polyarb/perception/store.py:latest_producer_receipt` — `LIMIT 1` 读取最近
  receipt，避免故障面板扫描无限增长的历史。
- `src/polyarb/http/perception.py:_producer_progress` — 将 receipt 与 Quote
  尝试、hydration 以及 Structure 恢复窗口合并为一个只读投影。

## 设计取舍

`OSError` 的 message 经常包含路径、连接串或环境变量内容。操作员真正需要的是
“这是 spawn 失败，异常类别是 OSError”，而不是原始 message。因此收据记录
`supervisor-spawn-error:OSError`，而不记录 message；后者既不安全，也不稳定。

## 自检题

1. 为什么 Dashboard 要同时看 Quote attempt 和 supervisor receipt？
2. 为什么异常类型比原始异常消息更适合持久化？
3. 为什么 receipt 查询必须是 `LIMIT 1`？

## FAQ 增量

- **有错误类型后还需要日志吗？** 需要。receipt 给出稳定的分类与关联边界；日志
  用于进一步排查，但 Dashboard 不应依赖日志仍在内存或留存期内。
