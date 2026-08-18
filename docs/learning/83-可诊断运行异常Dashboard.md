# 可诊断运行异常 Dashboard

## 30 秒心智模型

Telegram 是把人叫醒的铃；Dashboard 是让人醒来后不用猜的事故记录。
两者都从同一份 Postgres incident ledger 读取事实：一个运行时异常必须有
“谁发现、何时发现、影响什么、为何失败、是否恢复”五个答案。页面不能另建
一份日志，否则它会和告警说出不同的故事。

## 代码地图

- `src/polyarb/control_plane/runtime_event_writer.py`：把经过认证且有界的
  watchdog transition 写成 incident event。
- `src/polyarb/control_plane/postgres.py`：只读地把 incident identity 与
  event detail 投影给控制面 API。
- `dashboard/lib/control-plane.ts`：运行时验证 API 契约；字段缺失即将页面
  变为不可用，绝不静默省略诊断。
- `dashboard/app/control-plane/page.tsx`：渲染活动事故、证据新鲜度和检测/恢复
  时间线。

## 关键取舍

一个恢复事件的 `failures` 为空不表示没有事故：它表示同一个 incident 已经
恢复。因此时间线保留 `incident_key`、summary、source 与 severity，把红色的
detected 和绿色的 recovered 连成一个可审计生命周期。活动事故则置顶，包含
完整受影响检查项；没有活动事故不抹掉历史。

## 自检题

1. 页面显示 “Recovered” 但 failures 为空，为什么不能把它当作无意义记录？
2. 为什么 Dashboard 不能自己保存一份独立异常日志？
3. 当 API 少了 `source` 字段时，为什么显示部分页面比声明 unavailable 更危险？

## FAQ 增量

**“Dashboard 的新鲜度是谁计算的？”** API 保存不可变的采样时间；页面以服务
端当前时间计算年龄。这样既能审计原始时间，也能让操作员一眼看出采样是否过旧。
