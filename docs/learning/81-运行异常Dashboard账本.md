# 运行异常 Dashboard 账本

## 30 秒心智模型

Telegram 是唤醒人的即时通道，不是可查询的系统记录。M1 的异常转变还要进入
云端 Dashboard：每一次 `detected` 记录故障码和发生时间；匹配的 `recovered`
关闭同一条 incident，并保留恢复时间。页面把“没有事件”和“读模型失联”明确区分，
后者绝不能伪装成健康。

alert 进程仍然不持有数据库凭据。它只把经过认证、带幂等键的转变发给专用 writer；
writer 才使用只能触碰 incident 两张表的 PostgreSQL 角色。因此 Telegram、采集和
Dashboard 账本不会因为共用一个失败域而同时失明。

## 关键代码

- `src/polyarb/cli_control_plane.py:549`：每个运行时状态转变先生成
  `detected`/`recovered` 事实、SHA-256 幂等键，并投递给 writer；若 writer 不可用，
  Telegram 仍会立刻报告“账本不可用”。
- `src/polyarb/control_plane/runtime_event_writer.py:31`：只接受受限故障码、20 条以内
  的失败列表和 64 位幂等键；重复投递返回原 receipt，不会生成第二条事件。
- `src/polyarb/control_plane/postgres.py:3724`：control API 将当前未解决 incident 与
  最近事件投影为 `runtime_watchdog`，Dashboard 不直接连数据库。
- `dashboard/app/control-plane/page.tsx:6`：红色当前故障卡和最近检测/恢复账本；读模型
  不可用时页面明确显示 `Unavailable`。

## 设计取舍

把 Telegram 发信和数据库写入塞入同一 worker，会让数据库故障同时阻塞告警；把
Dashboard 直接接 PostgreSQL，又会把读凭据暴露给前端。这里多一个 256MB 的 writer
应用，换来三个独立边界：告警仍可送达、账本权限最小、Dashboard 只读 control API。

首个健康检查没有之前的故障可关闭，所以 writer 记为 `noop`，而不是错误地制造
“账本写入失败”报警。相反，任何真正的异常转变必须先得到 writer 的 `201`，再发送
正常 Telegram 文本，保证页面和手机看到的是同一份故障事实。

## 自检题

1. 为什么 Dashboard API 失联时不能显示“当前没有故障”？
2. 为什么 writer 的数据库角色不应拥有 `m1_jobs` 或 R2 权限？
3. 同一状态转变网络超时后重投，哪一项保证不会出现两条恢复记录？
4. Telegram 成功而 writer 失败时，值班人会看到哪一类额外信息？

## FAQ 增量

- **Dashboard 会不会吞掉 Telegram？** 不会。writer 在 Telegram 之外；writer 写失败
  只会额外产生“账本不可用”的 Telegram 提示，正常运行时告警仍通过独立路径发送。
