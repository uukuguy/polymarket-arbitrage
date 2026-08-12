# 事务型 Structure 源窗口

## 30 秒心智模型

“事务型 worker”不是把旧 SQLite 已完成的结果上传到云端，而是把**上游
Gamma 的每一页**本身变成可接管的事务：某个 worker 取一页、把这页不可变
证据上传到 R2、再在 Postgres 中一次性确认它并释放下一页。进程在确认前死掉
就重取这页；在确认后死掉，替代 worker 从下一页开始。因此不会跳过 cursor，
也不会依赖任何 Fly volume。

## 代码地图

- `src/polyarb/control_plane/models.py:StructureSourcePageSpec`：页的稳定身份。
- `alembic/versions/010_m1_transactional_structure_source.py`：window、页输入、页
  回执三类 durable authority。
- `src/polyarb/control_plane/postgres.py:record_structure_source_page`：R2 已认证后
  的 fenced cursor 提交和 successor 创建。
- `tests/m1-perception/test_control_plane_postgres.py`：真实 PostgreSQL 对 cursor
  接管、events→markets 交接的证明。

## 为什么 job key 不能包含 cursor

页的 stable identity 是 `window + stream + ordinal`，例如：

```text
source-window:one:fetch:events:0
```

cursor 是 Gamma 发来的不透明 continuation，属于这个页的已认证输入，不能由
我们解析或生成。把 cursor 编进 job key 会把一次上游响应细节误当作“另一个
工作”；把它完全丢掉则无法证明接管者读取的是同一页。当前模型保留两者：

```python
@property
def job_key(self) -> str:
    return f"{self.window_key}:fetch:{self.stream}:{self.ordinal}"

@property
def input_identity(self) -> str:
    return f"{self.window_key}:{self.stream}:{self.ordinal}:{self.requested_cursor}"
```

## 核心提交边界

`record_structure_source_page` 只接受当前 epoch 的 `structure-fetch` lease。它在
一笔数据库事务中：

1. 将当前 job 标为 `succeeded` 并绑定 artifact digest；
2. 写 checkpoint receipt 和 source-page receipt；
3. 非末页时创建同一 stream 的下一 ordinal；
4. events 的末页才把 window 转为 `events-complete` 并创建 `markets:0`。

这意味着“event 页成功但 market 页尚未开始”是显式可观察状态，而不是内存里
的一个布尔变量。

## 设计取舍

- 不并行 events/markets：市场页需要整个 event source contract，先后顺序比
  并发吞吐更重要。
- 不在 Postgres 存原始大 payload：Postgres 存 cursor、digest、时序和 receipt；
  R2 存可复验的大对象。
- 不做 exactly-once HTTP：失败边界可能重发请求；保证的是单一 durable effect。

## 自检题

1. worker 在 R2 PUT+HEAD 后、Postgres receipt 前崩溃，替代 worker 为什么不会
   从下一 cursor 开始？
2. 为什么 events 结束时才允许 `markets:0` 变成 runnable？
3. 为什么一个旧 lease 即使拿到了有效 Gamma 响应，也不能覆盖新 worker 的页？

## FAQ 增量

暂无。你读到实际 Gamma/R2 worker 时若对“重取同一页”是否会浪费或重复有疑问，
把具体场景贴出来；答案会追加在这里。
