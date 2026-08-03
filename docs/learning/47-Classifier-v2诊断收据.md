# Classifier-v2：把“不同”变成可恢复的认证事实

## 30 秒心智模型

Classifier-v2 不是再跑一次集合 diff。它先从已封存的 event-member sidecar
独立重建“市场现在应该是什么”，再与 generation 和 legacy 比较。每一段工作都
checkpoint；成功写授权收据，确定性失败写 terminal 收据。状态页只有在收据重算
通过后才公开分类、诊断和样本，所以“看到一个 stale”同时意味着“知道为什么 stale”。

## 代码地图

- `src/polyarb/storage/sqlite_store.py:6536`：有界推进完整 fresh projection，保存 union cursor、count、root 和诊断。
- `src/polyarb/storage/sqlite_store.py:7713`：用 classifier-v2 分类 generation/legacy member，并在同一个 CAS 中保存诊断。
- `src/polyarb/storage/sqlite_store.py:7194`：最终比较 group truth，原子写 sealed 或 stale receipt。
- `src/polyarb/storage/sqlite_store.py:8138`：只读取当前 classifier，并在公开证据前验证对应收据。
- `src/polyarb/perception/structure_drift.py:238`：给“fresh 有、generation 没有”生成完整诊断 envelope。

## 为什么多一个 `fresh-projection-members` phase

旧实现从 generation 行出发投影 expected row。这样 generation 漏掉一个市场时，
expected 侧也永远看不到它。新 phase 从封存 sidecar 的完整 keyspace 出发，先得到
独立 count/root；随后必须满足：

```text
fresh count == generation count == generation comparison count
fresh root  == generation comparison root
```

limit=1、17、500，以及 market→event-only 边界重启，都必须产生同一个 root 和 receipt。

## stale 为什么必须和 terminal receipt 同事务

如果先把 progress 写成 stale、再写诊断收据，第二步失败会留下“永远不重试、但没有
原因”的死状态。最终事务的顺序是：验证全部 commitment → INSERT immutable terminal
receipt → CAS progress 到 stale → COMMIT。任何一步失败，progress 仍留在 pre-terminal
phase，自动恢复可以继续。

## 三个容易混淆的取舍

1. `fresh-group-ineligible` 只进入 legacy reconstruction。它证明旧成员为何能安全移除，
   不能凭空增加 generation。
2. 样本最多每 code 三条，只为值班可读性；授权依赖完整 count/root，而不是样本。
3. v1 行仍是审计证据，但 current status 只接受 classifier-v2 receipt；历史 sealed 不能
   继续授权新 gate。

## 自检题

1. generation 漏掉一行时，为什么 generation-driven projection 永远发现不了？
2. terminal receipt INSERT 失败后，progress 应停在哪个状态，为什么？
3. active sibling 因组内另一个成员不可交易而消失，应进入哪个 class？
4. 为什么三条 diagnostic sample 不能作为授权依据？
5. v1 sealed receipt 为什么仍可查询、却不能授权当前 read-mode？

## FAQ 增量

暂无。后续答疑只追加到这里；同一问题出现三次再提升到正文。
