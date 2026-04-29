# Polymarket 套利开源项目全景调研

> 调研日期：2026-04-28
> 调研范围：35+ GitHub 仓库，逐个用 `gh` CLI 验证 star / 活跃度 / 语言 / 最近更新
> 调研背景：用户已 clone `clawfirm` + `polymarket-kalshi-weather-bot` 到 `3th-party/`，扩展寻找天气以外的套利项目

## 关键事实（影响架构决策）

1. **2026 年 2 月 Polymarket 移除了 taker 订单 ~500ms 人为延迟**
   - 之前的延迟是 maker 友好设计，taker 抢不到 stale quote
   - 现在 taker 可以毫秒级抢单 → maker 必须更快撤单避免 adverse selection
   - **MM 类策略风险变高，HFT 类门槛降低**
   - 之前的套利数据需要重新校准

2. **学术研究确认 $40M/年套利空间真实存在**
   - IMDEA Networks 论文（arXiv 2508.03474）"Unravelling the Probabilistic Forest"
   - 分析 Polymarket 2024-04 至 2025-04 共 86M 笔交易
   - Top 3 钱包合计赚 $4.2M
   - 套利分两类：
     - **Market Rebalancing Arbitrage**（单市场内 YES+NO≠1）
     - **Combinatorial Arbitrage**（跨市场，同一事件不同合约价格不一致）

3. **主流套利方向**
   - cross-platform：Polymarket vs Kalshi（受 CFTC 监管，流动性深）
   - 内部 combinatorial：multi-outcome 市场的概率和约束

---

## T0 — 基础设施（直接当依赖用）

| 项目 | 星 | 语言 | 用途 | 推荐 clone |
|---|---|---|---|---|
| [Polymarket/py-clob-client](https://github.com/Polymarket/py-clob-client) | 1.2k | Python | 官方 CLOB SDK，EIP-712 签名 + L2 HMAC | ✅ |
| [Polymarket/clob-client](https://github.com/Polymarket/clob-client) | 512 | TypeScript | 官方 TS 版 | 视语言选 |
| [Polymarket/rs-clob-client](https://github.com/Polymarket/rs-clob-client) | 695 | Rust | 官方 Rust 版 | 视语言选 |
| [pmxt-dev/pmxt](https://github.com/qoery-com/pmxt) | **1.6k** | TypeScript | **CCXT for prediction markets**——跨 Polymarket+Kalshi+Robinhood 统一 API | ✅ 强推 |
| [Polymarket/agents](https://github.com/Polymarket/agents) | **3.3k** | Python | 官方 AI Agent 框架，含 Gamma 客户端 + LLM 集成 | ✅ |
| [Polymarket/neg-risk-ctf-adapter](https://github.com/Polymarket/neg-risk-ctf-adapter) | 84 | Solidity | **理解多档位 categorical 合约的合约级实现**（合约结构必读） | ✅ 读源码 |

---

## T1 — 强参考实现（精读）

### 跨平台套利（Polymarket × Kalshi）

| 项目 | 星 | 语言 | 备注 | 推荐 |
|---|---|---|---|---|
| [taetaehoho/poly-kalshi-arb](https://github.com/taetaehoho/poly-kalshi-arb) | **428** | Rust | 高 star、Rust 实现、活跃维护 | ✅ clone |
| [ImMike/polymarket-arbitrage](https://github.com/ImMike/polymarket-arbitrage) | 108 | Python | 监控 10k+ 市场，跨平台 + 平台内 + MM | ✅ clone |
| [CarlosIbCu/polymarket-kalshi-btc-arbitrage-bot](https://github.com/CarlosIbCu/polymarket-kalshi-btc-arbitrage-bot) | 176 | Python+Next.js | BTC 1h 跨平台，FastAPI + Dashboard | ✅ |
| [TopTrenDev/polymarket-kalshi-arbitrage-bot](https://github.com/TopTrenDev/polymarket-kalshi-arbitrage-bot) | 35 | Rust | 教学性质，README 自承"非生产" | 看思路 |

### 做市 / Maker（吃 spread + rebate）

| 项目 | 星 | 语言 | 备注 | 推荐 |
|---|---|---|---|---|
| [warproxxx/poly-maker](https://github.com/warproxxx/poly-maker) | **1.1k** | Python | **最热门 MM bot**，Google Sheets 配置 | ✅ clone |
| [Polymarket/poly-market-maker](https://github.com/Polymarket/poly-market-maker) | 282 | Python | 官方 keeper，bands/AMM 双策略 | ✅ |
| [direkturcrypto/polymarket-terminal](https://github.com/direkturcrypto/polymarket-terminal) | 238 | JavaScript | maker rebate MM + sniper + copy | 三合一参考 |

### 多策略综合 / 教学

| 项目 | 星 | 语言 | 备注 | 推荐 |
|---|---|---|---|---|
| [ent0n29/polybot](https://github.com/ent0n29/polybot) | **482** | Java | "reverse-engineer every polymarket strategy"，含 Up/Down 完整套利策略 | ✅ |
| [chainstacklabs/polyclaw](https://github.com/chainstacklabs/polyclaw) | 332 | Python | OpenClaw skill，**含 LLM-powered hedge discovery (contrapositive logic)** | ✅ 思路新颖 |
| [Polymarket/agents](https://github.com/Polymarket/agents) | 3.3k | Python | 官方 LLM Agent，作为决策层参考 | ✅ |

---

## T2 — HFT / 低延迟（Polymarket 删除 500ms taker 延迟后的新格局）

| 项目 | 星 | 语言 | 备注 | 推荐 |
|---|---|---|---|---|
| [floor-licker/polyfill-rs](https://github.com/floor-licker/polyfill-rs) | 183 | Rust | "The Fastest Polymarket Rust Client"，SIMD JSON 解析 + HTTP/2 调优 | ✅ 性能基准 |
| [TheOverLordEA/polymarket-hft-engine](https://github.com/TheOverLordEA/polymarket-hft-engine) | 45 | Rust | 5 分钟加密合约，Rust + AWS eu-west-1 + Alchemy 流水线 | 看架构 |
| [TechieBoy/polymarket-rs-client](https://github.com/TechieBoy/polymarket-rs-client) | 81 | Rust | 早期 Rust 客户端 | 备选 |
| [nevuamarkets/poly-websockets](https://github.com/nevuamarkets/poly-websockets) | 70 | TypeScript | Plug-and-play WS 价格订阅 | ✅ 起步快 |

---

## T3 — 数据 / 分析 / CLI 工具

| 项目 | 星 | 语言 | 备注 |
|---|---|---|---|
| [warproxxx/poly_data](https://github.com/warproxxx/poly_data) | **1.7k** | Python | 数据抓取层（市场+订单事件+成交） |
| [NYTEMODEONLY/polyterm](https://github.com/NYTEMODEONLY/polyterm) | 218 | Python | CLI 工具，含 20+ 分析特性（鲸鱼追踪、insider 检测、跨平台 arb 扫描） |
| [vesper-astrena/polymarket-scanner-api](https://github.com/vesper-astrena/polymarket-scanner-api) | 3 | Python | REST API，扫 12k+ 市场找 YES+NO≠1 + ladder 矛盾 |
| [ivanzzeth/polymarket-go-gamma-client](https://github.com/ivanzzeth/polymarket-go-gamma-client) | 30 | Go | Go SDK，**含 find-negrisk-opportunities + find-related-markets-arbitrage 示例** |

---

## T4 — Copy Trading（跟单）

| 项目 | 星 | 语言 | 备注 |
|---|---|---|---|
| [GiordanoSouza/polymarket-copy-trading-bot](https://github.com/GiordanoSouza/polymarket-copy-trading-bot) | 40 | Python | Supabase + Python，监控 + 复制 |
| [realfishsam/Polymarket-Copy-Trader](https://github.com/realfishsam/Polymarket-Copy-Trader) | 22 | Python | CLI 配置即用 |
| [gamma-trade-lab/polymarket-copy-trading-bot](https://github.com/gamma-trade-lab/polymarket-copy-trading-bot) | 10 | Rust | Rust 版增强 |

---

## T5 — 索引 / Awesome 列表

| 项目 | 星 | 备注 |
|---|---|---|
| [aarora4/Awesome-Prediction-Market-Tools](https://github.com/aarora4/Awesome-Prediction-Market-Tools) | **298** | 最全的 curated 目录（AI Agent / API / 套利工具 / 数据 / Bot） |
| [harish-garg/Awesome-Polymarket-Tools](https://github.com/harish-garg/Awesome-Polymarket-Tools) | 61 | 第二个 awesome list |

---

## T6 — 学术 / 研究

- **IMDEA Networks 论文**（arXiv 2508.03474）："Unravelling the Probabilistic Forest: Arbitrage in Prediction Markets"
  - 分析 Polymarket 2024-04 至 2025-04 共 **86M 笔交易**
  - 总套利利润 **$40M**
  - 分两类：
    - **Market Rebalancing Arbitrage**（单市场内 YES+NO≠1）
    - **Combinatorial Arbitrage**（跨市场，同一事件不同合约价格不一致）
  - Top 3 钱包合计赚 $4.2M

链接：https://arxiv.org/abs/2508.03474

---

## T7 — 不推荐（farming / 低质量）

- README 是"polymarket bot polymarket bot polymarket bot..."关键词堆砌的：
  - `RaymondDakus/...`
  - `PolyScripts/...`
  - `samanalalokaya/...`
  - `unitmargaretaustin/...`
  - `llogiq33/...`
  - 等一系列重复仓库
- 这是典型的 SEO / star-farm 行为，代码质量不可信，慎 clone
- `b1rdmania/polymarket-ai-trading` (4★)、`speedyhughes/kalshi-poly-arb` (0★) 等几乎无活跃

---

## 给你的具体行动建议

当前 `3th-party/` 已有 `clawfirm/` + `polymarket-kalshi-weather-bot/`。**建议再 clone 这 5 个**作为不同维度参考：

```bash
cd 3th-party/

# 1. 官方 SDK + Agent 框架（基础设施）
git clone https://github.com/Polymarket/py-clob-client
git clone https://github.com/Polymarket/agents

# 2. 跨平台 Python 套利标杆（看怎么扫 10k 市场）
git clone https://github.com/ImMike/polymarket-arbitrage

# 3. 学习多档位 negrisk 套利（Go 实现，思路清晰）
git clone https://github.com/ivanzzeth/polymarket-go-gamma-client

# 4. 学习 LLM 决策与执行解耦（最匹配"用智能体做决策"需求）
git clone https://github.com/chainstacklabs/polyclaw

# 5. 跨平台统一 API（如果想做 Polymarket + Kalshi）
git clone https://github.com/qoery-com/pmxt
```

**优先级**：
- 先看 **`ImMike/polymarket-arbitrage`** —— 和 weather-bot 同语言（Python），但做的是单市场+跨市场套利，能直接对照学
- 再看 **`chainstacklabs/polyclaw`** —— "LLM-powered hedge discovery via contrapositive logic" 是"用智能体做决策"的最直接落地形式

---

## 调研参考链接

- [aarora4/Awesome-Prediction-Market-Tools](https://github.com/aarora4/Awesome-Prediction-Market-Tools) — 索引主入口
- [Polymarket WSS 文档](https://docs.polymarket.com/developers/CLOB/websocket/wss-overview)
- [Polymarket Authentication & EIP-712](https://docs.polymarket.com/developers/CLOB/authentication)
- [IMDEA Arbitrage Paper (arXiv 2508.03474)](https://arxiv.org/abs/2508.03474)
- [QuantVPS - Polymarket HFT 分析](https://www.quantvps.com/blog/polymarket-hft-traders-use-ai-arbitrage-mispricing)
