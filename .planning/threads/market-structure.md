---
slug: market-structure
title: Polymarket Market Structure (composition / API tiers / negrisk)
status: open
created: 2026-04-28
updated: 2026-04-28
---

# Thread: Polymarket Market Structure

> 累积对 Polymarket 微观结构的所有理解。代码遇到任何"为什么这样设计"的问题，记在这里。
> 跨 phase 累积，永久存活。

## 已知（来自前期调研）

### 合约类型

- **Binary YES/NO**：单一二元市场
- **Categorical / Multi-outcome**：天气、选举名次等，N 档 token，∑prices=1 应成立
- **Negative-Risk (negrisk)**：mutually exclusive 合约组，**官方有 neg-risk-ctf-adapter**
    - 关键：negrisk 把 N 个相关市场绑成一个组，套利要识别 group 关系

### API 层级

- **Gamma API** (`gamma-api.polymarket.com`)：市场元数据、聚合价格、event/market 关系
- **CLOB API** (`clob.polymarket.com`)：订单簿、下单、L2 HMAC 认证
- **CLOB WebSocket** (`ws-subscriptions-clob.polymarket.com`)：实时单簿、用户订单

### 结算 / Oracle

- 用 UMA Optimistic Oracle
- 关键漏洞：Paris CDG 气象站被吹风机篡改 → $37k 套利（合法但揭露 oracle 单点风险）
- MOOV2 升级：proposer 白名单制

### 已知 Latency 事实

- 2026-02 移除 ~500ms taker 人为延迟
- → MM 风险变高，HFT 门槛降低

## 待研究（M1 期间填充）

- [ ] CLOB token_id 与 outcome 的精确映射规则
- [ ] negrisk group 的 API 表现形式（如何从 Gamma 数据反推关系）
- [ ] WS 增量更新的序列号 / 重连协议
- [ ] 流动性深度的真实分布（哪些市场只是挂单陷阱）
- [ ] 历史成交数据接入方式
- [ ] Rate limit 实际值（官方文档不全）
