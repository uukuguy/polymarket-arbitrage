# Polymarket 钱包 & 出入金操作指南

> 写给中国用户的实操手册 — OKX 钱包作为主推方案。
> 最后更新：2026-06-07

---

## 一、前置条件

| 你需要 | 说明 |
|---|---|
| OKX App | 已注册、已 KYC、至少有少量余额 |
| 科学上网 | Polymarket 网站被 GFW 墙 |
| 15 分钟 | 整条链路走通的时间 |

---

## 二、链的概念（一句话版）

USDC 可以在**多条链**上存在（以太坊主网 / Polygon / Arbitrum / BSC 等等）。它们是**独立**的账本，就像同一张银行卡号在不同的银行系统里互不相通。

**你必须确保所有操作都在同一条链上。** 本指南全部走 Polygon 链，因为 Polymarket 用 Polygon 结算。

---

## 三、创建 OKX 钱包

OKX 自带 Web3 钱包，不需要额外装任何东西。

1. 打开 OKX App
2. 底部导航条 → **"Web3"** 或 **"钱包"**
3. 如果首次使用 → 点 **"创建钱包"**
4. **备份助记词**（12 个英文单词）
   - ⚠️ 用纸笔抄下来，不要截图，不要复制粘贴
   - ⚠️ 不要存云盘 / 微信 / 印象笔记
   - ⚠️ 这 12 个词 = 你钱包的最终控制权，丢了任何人都救不了
5. 验证助记词（App 会让你选 2-3 个词确认你抄对了）
6. 钱包创建完成

---

## 四、往钱包充 USDC（从 OKX 交易所转入）

OKX 交易所余额和 OKX 钱包余额是**两个独立账户**，需要提币操作。

### 第一步：确保选对 Polygon 链 — $1 测试

1. OKX App → **资产** → **提币** → 搜索 **USDC**
2. 填写提币信息：

   | 字段 | 填什么 |
   |---|---|
   | 链 | **Polygon**（⚠️ 不是 ERC20，不是 BSC，不是 Arbitrum） |
   | 地址 | 你的 OKX 钱包地址（打开 Web3 钱包页，顶部那串 `0x...` 点复制） |
   | 金额 | **1 USDC**（$1 测试，亏了不心疼） |

3. 确认提币（输入资金密码 + 验证码）
4. 等 1-2 分钟
5. 打开 OKX 钱包，看余额 → 应该多出 $1 USDC

**如果没到账：**
- 去 OKX 提币记录找这笔的 **TxID**（交易哈希）
- 用浏览器打开 https://polygonscan.com ，搜索这个 TxID
- 显示 "Success" → 钱在链上，钱包可能没显示 USDC 代币
  - 解决：钱包里手动添加令牌 → 搜索 USDC → 合约地址填 `0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359`
- 显示 "Fail" → 联系 OKX 客服

### 第二步：$1 确认到账后 → 提正式金额

6. 重复上面的步骤，金额填你实际需要的量（建议首次转 $100-200 测试）
7. 链必须是 Polygon，地址不能变

---

## 五、连接 Polymarket 并创建 API Key

### 5.1 连接钱包

1. 打开 OKX App → **Web3 钱包** → 底部 **"发现"**（或浏览器图标）
2. 在地址栏输入 `https://polymarket.com`
3. 点页面右上角 **"Log In"** 或 **"Connect Wallet"**
4. 选 **WalletConnect**
5. 弹回 OKX 钱包 → 点 **"连接"**
6. **签名一条消息**（这是免费的身份验证，不花 gas 费，不花 USDC）
7. 页面刷新 → 右上角显示你的钱包地址 → 连接成功

### 5.2 创建 API Key

API Key 是给程序（我们的套利系统）用的，不是网页操作需要的。

1. Polymarket 网站 → 右上角头像 → **Settings**
2. 找到 **API Keys** 页面
3. 点 **"Create API Key"**
4. 记下弹窗里的三个值（⚠️ **只显示这一次，关了再也看不到**）：

   ```
   名称:      polyarb-api-key
   key:       XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
   secret:    XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
   passphrase: XXXXXXXXXXXXXXXX
   ```

5. 把这三个值保存到本项目的 `.env` 文件：
   ```bash
   POLYMARKET_API_KEY=你的key
   POLYMARKET_API_SECRET=你的secret
   POLYMARKET_API_PASSPHRASE=你的passphrase
   ```

> ⚠️ `.env` 文件在 `.gitignore` 里，不会提交到 GitHub。不要把这些值发到聊天里。

---

## 六、把钱从 Polymarket 提回 OKX

### 6.1 从 Polymarket 提回钱包

1. Polymarket 网站 → 右上角钱包余额 → **"Withdraw"**
2. 输入金额 → 确认交易的 Metamask 弹窗
3. Gas 费是 MATIC，Polymarket 一般会帮你垫付（免费提现）

### 6.2 从钱包提回 OKX 交易所

1. OKX App → Web3 钱包 → 点 USDC 余额 → **"发送"**
2. 填接收地址：OKX App → 资产 → 存款 → 搜 USDC → **链选 Polygon** → 复制充值地址
3. OKX 钱包里粘贴这个地址 → 输入金额 → 确认
4. 等 2-5 分钟，OKX 交易所余额到账

**⚠️ 提现时链必须还是 Polygon，不要切到别的链。**

---

## 七、常见踩坑集合

| 问题 | 原因 | 解决 |
|---|---|---|
| USDC 转出去了但钱包里看不见 | 钱包没添加 USDC 代币 | 手动添加令牌，合约地址见第四章 |
| 提币到账但金额不对 | 跨链提币走了桥，扣了桥费 | 你在 OKX 提币页面选的链必须是 Polygon，不是"跨链桥" |
| 连接 Polymarket 时 WalletConnect 弹不出来 | OKX App 内嵌浏览器兼容问题 | 用手机系统自带浏览器打开 polymarket.com，点 WalletConnect 后会唤起 OKX App |
| API 下单失败返回 403 | API key 没激活或网络被墙 | 先 curl `https://polymarket.com/api/geoblock` 看是否 blocked |
| Gas 不够无法交易 | 钱包里只有 USDC 没有 MATIC | 提 $1-2 MATIC 到钱包（同样走 Polygon 链） |

---

## 八、钱包地址速查

| 用途 | 地址（Polygon 链） |
|---|---|
| USDC 代币合约 | `0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359` |
| Polymarket CLOB 合约 | 无需直接交互，py-clob-client 封装好了 |

---

## 九、下一步

钱包备好后：
1. 跑 `make eval-arb mid=0.45 stake=1000` 确认 paper mode 正常工作
2. 我需要写 `VenueAdapter` 把 `leg_executor` 和 `fill_provider` 接到 py-clob-client
3. 用 $20-50 小额实盘测试一整轮（评估 → 下单 → 成交 → 关仓 → PnL），确认链路全通后再放大
