# 三省六部回测内阁 - 操作手册 (User Guide)

欢迎使用【三省六部回测内阁】。这是一个基于中国古代官制架构设计的、高度模块化、风控严格的量化交易/回测系统。

## 🏛️ 核心架构

本系统模拟“三省六部”制，确保策略执行的独立性与风控的绝对权威。

*   **三省 (决策核心)**
    *   **中书省 (Secretariat)**: 负责**生成信号**。管理所有策略，根据行情独立发出买卖请求。
    *   **门下省 (Chancellery)**: 负责**风控审核**。拥有“一票否决权”，拦截所有触犯红线的交易。
    *   **尚书省 (State Affairs)**: 负责**执行落地**。调度六部进行模拟成交、持仓管理与资金清算。

*   **六部 (职能支撑)**
    *   **吏部**: 策略管理。
    *   **户部**: 资金/成本核算。
    *   **礼部**: 业绩报表/排行榜。
    *   **兵部**: 交易撮合/止盈止损。
    *   **刑部**: 违规/熔断记录。
    *   **工部**: 数据清洗与处理。

---

## 🚀 快速开始

### 1. 环境准备

确保已安装 Python 3.8+。

```bash
# 安装依赖
pip install -r requirements.txt
# 额外安装数据源依赖 (推荐)
pip install tushare akshare
```

### 2. 配置文件

系统核心配置位于根目录下的 `config.json`。

```json
{
  "system": {
    "mode": "live",          // 运行模式: live (实盘/模拟盘), backtest (回测)
    "initial_capital": 1000000.0
  },
  "data_provider": {
    "source": "akshare",     // 默认数据源: akshare (免费), tushare (需Token), default (API)
    "tushare_token": ""      // 如使用 Tushare，请在此填入 Token
  },
  "targets": [
    "601899.SH"              // 监控标的代码
  ]
}
```

### 3. 启动系统

#### 方式一：实盘监控模式 (Live Mode)
连接实时数据源，盘中实时计算信号。

```bash
python3 run_live.py
```

*   **指定数据源启动**：
    ```bash
    # 强制使用 Tushare
    export DATA_PROVIDER=tushare
    export TUSHARE_TOKEN=你的token
    python3 run_live.py
    ```

#### 方式二：历史回测模式 (Backtest Mode)
使用历史数据进行全量策略验证。

```bash
python3 main.py
```

---

## 📊 数据源指南

系统支持多源切换，适配不同需求：

| 数据源 | 费用 | 实时性 | 推荐场景 | 配置方法 |
| :--- | :--- | :--- | :--- | :--- |
| **Akshare** | 免费 | 较高 (爬虫) | **默认推荐**，低频实盘/盘后分析 | `"source": "akshare"` |
| **Tushare Pro** | 收费/积分 | 高 (官方接口) | 生产环境，需稳定数据 | `"source": "tushare"` + 填入Token |
| **Default API** | 自定义 | 取决于服务端 | 对接私有行情服务器 | `"source": "default"` + 填入URL |

*注意：实盘运行时，系统会自动将当日接收到的数据归档到 `data/history/` 目录下。*

---

## 📈 策略库说明

目前内阁已部署 **7套** 经过验证的策略：

1.  **Strategy 01: 三周期共振波段**
    *   *逻辑*: 周线趋势向上 + 日线缩量回调 + 60分钟MACD金叉。
    *   *适用*: 大级别趋势波段。

2.  **Strategy 02: 短线弱转强烂板**
    *   *逻辑*: 前日烂板放量 + 次日高开弱转强。
    *   *适用*: 超短线打板接力。

3.  **Strategy 03: ETF行业轮动**
    *   *逻辑*: 均线趋势跟踪，轮动持有强势品种。
    *   *适用*: 行业ETF配置。

4.  **Strategy 04: 龙头首阴反包**
    *   *逻辑*: 连板龙头首次分歧收阴后，次日反包修复。
    *   *适用*: 情绪博弈。

5.  **Strategy 05: 3N法则主升浪**
    *   *逻辑*: 30分钟级别平台突破 + 指标共振。
    *   *适用*: 捕捉主升浪启动点。

6.  **Strategy 06: 海豚交易法**
    *   *逻辑*: 均线趋势 + MACD水上金叉 + 移动止损。
    *   *适用*: 强趋势行情。

7.  **Strategy 07: 跳空交易系统 (Larry Williams)**
    *   *逻辑*: 捕捉向下跳空后的情绪反转（低开高走）。
    *   *适用*: 震荡市或超跌反弹。

---

## 🛡️ 风控红线 (门下省)

所有策略必须通过以下风控检查，否则信号将被**一票否决**：

1.  **单笔止损**: 不得超过 1%。
2.  **单票仓位**: 不得超过 10%。
3.  **总仓位**: 不得超过 50%。
4.  **熔断机制**: 单日亏损超过 2% 时，当日**只平不开**。
5.  **连续亏损**: 策略连续亏损 3 笔，暂停开仓。

---

## 🛠️ 二次开发

### 如何添加新策略？

1.  在 `src/strategies/implemented_strategies.py` 中新建一个类，继承 `BaseImplementedStrategy`。
2.  实现 `on_bar(self, kline)` 方法，定义买卖逻辑。
3.  在 `src/strategies/strategy_factory.py` 中注册该策略。
4.  在 `config.json` 的 `strategies.active_ids` 中添加新策略ID。

### 如何修改风控参数？

直接修改 `config.json` 中的 `risk_control` 字段即可生效，无需重启代码（如果是实盘运行建议重启）。
