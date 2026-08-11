# Structure 写忙恢复重试：checkpoint 不是完成

## 30 秒心智模型

Structure 的成员关系补算和 Quote 报价共用 SQLite。Quote 正在写时，Structure 子进程不硬抢锁：
它保存当前位置并返回 `writer-busy`。但“已安全保存 checkpoint”只说明可恢复，**不代表这轮
恢复已经完成**。若把它当作完成并等待正常五分钟 cadence，持续报价会把恢复拉长到数小时。

现在 `writer-busy` 会走 scheduler 已有的五秒受限 defer loop：每次都重新竞争，不持锁等待，
Quote 仍优先；一旦出现写入空隙，Structure 从已保存的位置继续。

## 代码地图

- `src/polyarb/daemon/scheduler.py:_maybe_advance_structure_event_members` — 将 deferred
  checkpoint 交回 `_tick` 的 defer loop。
- `src/polyarb/daemon/scheduler.py:_tick` — 每次 defer 等待
  `STRUCTURE_DEFER_RETRY_DELAY_S`，而不是直接进入正常 cadence。
- `tests/m1-perception/test_scheduler.py:test_event_member_writer_busy_checkpoint_retries_within_the_bounded_defer_loop`
  — 先 busy、后 sealed 的生产回归。

## 设计取舍

不能让 Structure 无限 tight-loop：那会制造子进程风暴，反过来伤害 Quote。因此复用五秒边界；
它既比五分钟恢复快两个数量级，又让每次 SQLite 竞争都是短、可审计、可取消的。

## 自检题

1. 为什么 `writer-busy` 不是失败，也不能被伪装成成功？
2. 为什么这里选择五秒重试而不是立刻循环？
3. Quote 一直忙时，Structure 是否会丢失已经写入的 member progress？为什么？
