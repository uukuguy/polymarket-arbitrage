# Sealed 后立即认证：恢复证据完成不等于恢复已证明

member/group-truth `sealed` 证明原始关系证据完整，但 Structure P1 的关闭还需要一份
**失败之后产生的、认证发布的** Structure snapshot。因此 sealed 后若等普通五分钟 cadence，
只是把真实恢复无意义地延后。

`SnapshotScheduler` 现在把 sealed 同样视为 checkpoint continuation：两秒后重新走 Quote
仲裁，再启动 fresh snapshot。它不抢占 Quote，也不跳过 admission；只是避免恢复链在证据已齐时空转。
