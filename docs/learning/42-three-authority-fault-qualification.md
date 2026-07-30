# 三权分立的故障资格链

## 30 秒心智模型

一次 fault 实验不是“脚本返回 0”就结束，而是三段互不信任的证明：

```text
execute（ordinary + fault control）
  → RECOVERED immutable evidence
evaluator（evaluator secret only）
  → signed candidate PASS
finalizer（ordinary + fault control, no evaluator secret）
  → VERIFIED
re-export + evaluator
  → final PASS
```

`RECOVERED` 只证明 producer 清理并恢复业务写入；`VERIFIED` 才证明独立
evaluator 的 exact verdict 被源 authority 接受。任一进程同时持有三份 secret，
就可以自写证据、自签 PASS、自行盖章，资格链失去意义。

## 代码地图

- `scripts/perception_chaos.py:301`：真实 HTTP transport；只负责 baseline、
  arm、observe、cleanup、recovery 和 export。
- `scripts/perception_chaos.py:515`：所有 `BaseException` 后仍清理，cleanup
  proof 失败优先报错并冻结后续 matrix。
- `scripts/perception_fault_readonly.py:38`：用 SQLite read-only snapshot 导出
  intent、完整 event hash chain 和 recovery writer。
- `scripts/perception_fault_acceptance.py:62`：重算每个 digest/hash，任何缺失
  都产生 named FAIL reason。
- `scripts/perception_fault_acceptance.py:309`：只有 candidate PASS 才用第三份
  evaluator secret 签名。
- `src/polyarb/http/perception_faults.py:460`：HTTP finalizer 的双 control
  authentication 和 artifact contract。
- `src/polyarb/perception/fault_authority.py:1191`：绑定 exact RECOVERED tail，
  append-only 写入 `VERIFIED(verdict_id, verdict_digest)`。

## 关键取舍

### 为什么 cleanup 在 `BaseException` 边界

`asyncio.CancelledError`、`KeyboardInterrupt` 和 `SystemExit` 不应被普通
`except Exception` 漏掉。arm 一旦成功，cleanup 就成为不可跳过的安全义务。
若 cleanup receipt 不可信，原始 detection error 不能掩盖它；系统必须冻结
剩余 fault，避免在未知残留注入上继续叠加实验。

### 为什么 candidate evidence 停在 RECOVERED

execute 进程拥有 mutation authority。如果它还能写 `VERIFIED`，等于自己出题、
自己判卷。只读 evaluator 先签 candidate，control finalizer 再验证签名和 exact
source tail，最后 evaluator 重读 source；这让任何单点都无法独立伪造 PASS。

### 为什么 recovery 必须来自业务 writer

“cleanup endpoint 返回 200”只证明控制请求被接受。真正恢复必须看到 Discovery
batch、Reconciliation window、Candidate success receipt 或 notification attempt
中的对应新写入，而且发生时间晚于 injection 和 cleanup。日志、sleep 或不同
component 的成功不能替代。

## 自检题

1. evaluator 已签 PASS，但 source 仍停在 `RECOVERED`，能否宣布资格通过？
2. detection timeout 与 cleanup receipt 缺失同时发生，应优先处理哪一个？
3. finalizer 为什么不能读取 evaluator secret？
4. Candidate fault 恢复后出现新 Discovery batch，为什么仍然 FAIL？
5. exact artifact 重试与复用相同 nonce 重试，哪个允许幂等？

## FAQ 增量

### 为什么不让 evaluator 直接调用 finalize endpoint？

因为那会把“判卷权”和“请求盖章权”放进同一进程。当前流程要求 operator/control
侧显式提交 evaluator artifact，final evaluator 又没有 HTTP mutation capability，
从进程和 secret 两层维持分权。
