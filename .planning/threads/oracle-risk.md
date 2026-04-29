---
slug: oracle-risk
title: Oracle / Resolution Risk (UMA / 案例库 / 防御策略)
status: open
created: 2026-04-28
updated: 2026-04-28
---

# Thread: Oracle / Resolution Risk

> 所有与市场结算相关的风险、案例、防御策略累积在此。

## 已知风险案例

### 2026-04 Paris 吹风机事件

- 攻击者用吹风机/加热器物理篡改 CDG 机场气象站
- Polymarket 天气合约结算源依赖该站点 Wunderground 数据
- 套利者赚 $37k
- Polymarket 切换到 Le Bourget 站，但被批评"换了一个同样脆弱的单点"
- 法国警方介入

### UMA 治理风险

- Whale 持有大量 UMA token 可影响 dispute 投票
- 历史上有过争议性结算被 whale 推翻

## 防御策略（待 M1+ 验证）

- [ ] 单合约最大敞口 ≤ 总资金 3%（限制 oracle 操纵的最大损失）
- [ ] 避开已发生过 incident 的城市（M4 weather strategy 时生效）
- [ ] 监控 UMA 提议状态，dispute window 内不持仓未结算合约
- [ ] 解析源透明度评分（NWS 直推 > Wunderground 转发 > 第三方平台）

## 待研究

- [ ] UMA dispute 流程的 API 接入方式（监控提议、dispute 状态）
- [ ] negrisk adapter 升级历史，看哪些合约结构有过破坏性变更
- [ ] 历史上 UMA 翻案的案例库
