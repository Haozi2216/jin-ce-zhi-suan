"""全流程演示示例策略适配器。

该模块用于“策略进化系统 V1”的演示场景，提供两类能力：
1) 生成可直接运行的示例策略代码（兼容 BaseImplementedStrategy）；
2) 将示例策略一键写入策略库（存在则更新，不存在则新增）。

设计目标：
- 最小侵入：不改回测引擎与策略基类；
- 可复现：策略ID固定、参数清晰；
- A股约束：在策略层体现 T+1、涨停不追、跌停不卖（由回测引擎最终裁决）。
"""

from __future__ import annotations

from datetime import datetime
from textwrap import dedent
from typing import Any, Dict

from src.strategies import strategy_manager_repo as strategy_repo


# 演示策略固定ID，便于全流程重复执行时稳定覆盖更新。
DEMO_STRATEGY_ID = "E2E01"
# 演示策略默认名称，前端策略列表中可直接检索。
DEMO_STRATEGY_NAME = "E2E演示-主板MA动量T+1"


def build_demo_strategy_code(
    strategy_id: str = DEMO_STRATEGY_ID,
    strategy_name: str = DEMO_STRATEGY_NAME,
) -> str:
    """构建示例策略代码文本。

    说明：
    - 采用日线触发，逻辑足够简单，便于稳定跑通；
    - 买入：MA5 上穿 MA20，且不在涨停附近；
    - 卖出：MA5 下穿 MA20 或止损/止盈，且遵守 T+1 与跌停不卖；
    - 仅做演示，不追求收益最优。
    """
    # 统一转成字符串，避免外部传入非字符串导致模板拼接异常。
    sid = str(strategy_id or DEMO_STRATEGY_ID).strip() or DEMO_STRATEGY_ID
    sname = str(strategy_name or DEMO_STRATEGY_NAME).strip() or DEMO_STRATEGY_NAME
    # 使用 dedent 保持生成代码的缩进整洁，便于前端代码查看。
    code_text = f"""
from src.strategies.implemented_strategies import BaseImplementedStrategy
from src.utils.indicators import Indicators
import pandas as pd


class E2EDemoMomentumStrategy(BaseImplementedStrategy):
    \"\"\"全流程演示策略：主板MA动量 + A股约束示例。\"\"\"

    def __init__(self):
        # 使用日线触发，降低噪声并提升演示稳定性。
        super().__init__("{sid}", "{sname}", trigger_timeframe="D")
        # 按股票维护历史K线，用于计算均线。
        self.history = {{}}
        # 记录买入日，用于 T+1 约束。
        self.last_buy_day = {{}}
        # 记录入场价，便于止盈止损演示。
        self.entry_price_local = {{}}

    def _is_main_board(self, code):
        # 主板限定：600/601/603/605/000/001/002。
        c = str(code or "").split(".", 1)[0].strip().upper()
        return c.startswith(("600", "601", "603", "605", "000", "001", "002"))

    def _is_limit_up(self, kline):
        # 以涨跌幅近似判断涨停，用于“涨停不追”。
        pre_close = float(kline.get("pre_close", 0.0) or 0.0)
        close = float(kline.get("close", 0.0) or 0.0)
        if pre_close <= 0:
            return False
        pct = (close - pre_close) / pre_close * 100.0
        return abs(pct - 10.0) < 0.1 or abs(pct - 20.0) < 0.1

    def _is_limit_down(self, kline):
        # 以涨跌幅近似判断跌停，用于“跌停不卖”。
        pre_close = float(kline.get("pre_close", 0.0) or 0.0)
        close = float(kline.get("close", 0.0) or 0.0)
        if pre_close <= 0:
            return False
        pct = (close - pre_close) / pre_close * 100.0
        return abs(pct + 10.0) < 0.1 or abs(pct + 20.0) < 0.1

    def _same_trade_day(self, code, dt_value):
        # 将 datetime 归一到 yyyy-mm-dd 文本，便于做 T+1 判断。
        day_text = str(pd.to_datetime(dt_value, errors="coerce").strftime("%Y-%m-%d"))
        return self.last_buy_day.get(code) == day_text, day_text

    def on_bar(self, kline):
        # 读取基础字段，兼容回测引擎输入字典。
        code = str(kline.get("code", "") or "").strip()
        if not code:
            return None

        # 非主板标的直接跳过，降低演示复杂度。
        if not self._is_main_board(code):
            return None

        # 累积历史K线（保留最近800条即可满足均线与回测性能）。
        if code not in self.history:
            self.history[code] = pd.DataFrame()
        self.history[code] = pd.concat([self.history[code], pd.DataFrame([kline])], ignore_index=True).tail(800)
        df = self.history[code]
        if len(df) < 25:
            return None

        # 计算核心信号：MA5 与 MA20。
        close = df["close"].astype(float)
        ma5 = Indicators.MA(close, 5)
        ma20 = Indicators.MA(close, 20)
        if len(ma5) < 2 or len(ma20) < 2:
            return None

        curr_close = float(kline.get("close", 0.0) or 0.0)
        if curr_close <= 0:
            return None

        # 当前持仓数量（策略上下文由回测引擎维护）。
        qty = int(self.positions.get(code, 0) or 0)
        # 策略参数可在 runtime_params 中覆盖。
        stop_loss_pct = float(self._cfg("stop_loss_pct", 0.03))
        take_profit_pct = float(self._cfg("take_profit_pct", 0.06))
        max_hold_bars = int(self._cfg("max_hold_bars", 20))

        # 买入条件：空仓 + MA5金叉MA20 + 当前价在MA20上方 + 非涨停。
        if qty <= 0:
            golden_cross = float(ma5.iloc[-2]) <= float(ma20.iloc[-2]) and float(ma5.iloc[-1]) > float(ma20.iloc[-1])
            if golden_cross and curr_close > float(ma20.iloc[-1]) and (not self._is_limit_up(kline)):
                buy_qty = int(self._qty())
                # 若动态仓位未配置，兜底 100 股，便于演示一定能成交。
                if buy_qty <= 0:
                    buy_qty = 100
                same_day, day_text = self._same_trade_day(code, kline.get("dt"))
                # 记录买入日与入场价，用于后续 T+1 与风控。
                self.last_buy_day[code] = day_text
                self.entry_price_local[code] = curr_close
                return {{
                    "strategy_id": self.id,
                    "code": code,
                    "dt": kline["dt"],
                    "direction": "BUY",
                    "price": curr_close,
                    "qty": buy_qty,
                    "stop_loss": curr_close * (1 - stop_loss_pct),
                    "take_profit": curr_close * (1 + take_profit_pct),
                }}
            return None

        # 卖出逻辑：先做 T+1 约束（当日买入不可卖）。
        same_day, _day_text = self._same_trade_day(code, kline.get("dt"))
        if same_day:
            return None

        # A股限制：跌停日不主动卖出，等待下一交易日处理。
        if self._is_limit_down(kline):
            return None

        # 退出条件：MA5死叉MA20 / 止损 / 止盈 / 持仓超时。
        death_cross = float(ma5.iloc[-2]) >= float(ma20.iloc[-2]) and float(ma5.iloc[-1]) < float(ma20.iloc[-1])
        entry_price = float(self.entry_price_local.get(code, curr_close) or curr_close)
        stop_loss_hit = curr_close <= entry_price * (1 - stop_loss_pct)
        take_profit_hit = curr_close >= entry_price * (1 + take_profit_pct)

        # 更新持仓时长并检查超时退出。
        self.update_holding_time(code)
        timeout_exit = self.check_max_holding_time(code, max_hold_bars)

        if death_cross or stop_loss_hit or take_profit_hit or timeout_exit:
            reason_parts = []
            if death_cross:
                reason_parts.append("MA Death Cross")
            if stop_loss_hit:
                reason_parts.append("Stop Loss")
            if take_profit_hit:
                reason_parts.append("Take Profit")
            if timeout_exit:
                reason_parts.append("Time Exit")
            reason_text = " | ".join(reason_parts) if reason_parts else "Rule Exit"
            return self.create_exit_signal(kline, qty, reason_text)

        return None
"""
    return dedent(code_text).strip() + "\n"


def build_demo_strategy_payload(
    strategy_id: str = DEMO_STRATEGY_ID,
    strategy_name: str = DEMO_STRATEGY_NAME,
) -> Dict[str, Any]:
    """构建策略库入库 payload。"""
    # 生成代码文本，并保持 class_name 与代码一致。
    code_text = build_demo_strategy_code(strategy_id=strategy_id, strategy_name=strategy_name)
    sid = str(strategy_id or DEMO_STRATEGY_ID).strip() or DEMO_STRATEGY_ID
    sname = str(strategy_name or DEMO_STRATEGY_NAME).strip() or DEMO_STRATEGY_NAME
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 统一策略意图结构，兼容 strategy_manager_repo 的标准化要求。
    intent = {
        "source": "human",
        "strategy_type": "trend_following",
        "logic": "主板金叉入场，T+1与涨跌停约束，死叉/止盈止损/超时退出",
        "indicators": ["MA5", "MA20"],
        "entry": "MA5上穿MA20且收盘价站上MA20，且非涨停",
        "exit": "死叉或达到止损止盈或持仓超时，且遵守T+1和跌停不卖",
        "risk_profile": "balanced",
        "confidence": 0.70,
    }
    # 返回可直接传给 add_custom_strategy/update_custom_strategy 的字段。
    return {
        "id": sid,
        "name": sname,
        "class_name": "E2EDemoMomentumStrategy",
        "code": code_text,
        "kline_type": "1day",
        "template_text": "E2E演示策略模板：主板MA动量 + A股约束",
        "analysis_text": f"E2E演示策略，更新时间 {now_text}。",
        "source": "human",
        "protect_level": "custom",
        "immutable": False,
        "depends_on": [],
        "raw_requirement_title": "全流程跑通演示策略",
        "raw_requirement": "用于条件筛选->策略管理->回测全链路演示，非实盘建议。",
        "strategy_intent": intent,
    }


def upsert_demo_strategy(
    strategy_id: str = DEMO_STRATEGY_ID,
    strategy_name: str = DEMO_STRATEGY_NAME,
) -> Dict[str, Any]:
    """将演示策略写入策略库（存在即更新，不存在即新增）。"""
    # 先构建标准 payload，后续 add/update 复用同一份数据。
    payload = build_demo_strategy_payload(strategy_id=strategy_id, strategy_name=strategy_name)
    sid = str(payload.get("id", "")).strip()
    # 从全量策略元信息查找目标ID，决定执行 add 还是 update。
    rows = strategy_repo.list_all_strategy_meta()
    exists = any(str(r.get("id", "")).strip() == sid for r in rows if isinstance(r, dict))
    if exists:
        # update 接口不允许修改 id，本处剔除 id 后更新其余字段。
        update_payload = dict(payload)
        update_payload.pop("id", None)
        update_payload["id"] = sid
        strategy_repo.update_custom_strategy(update_payload)
        return {
            "status": "success",
            "action": "updated",
            "strategy_id": sid,
            "strategy_name": str(payload.get("name", "")),
            "kline_type": str(payload.get("kline_type", "")),
        }
    # 不存在时执行新增，首次演示可直接使用。
    strategy_repo.add_custom_strategy(payload)
    return {
        "status": "success",
        "action": "created",
        "strategy_id": sid,
        "strategy_name": str(payload.get("name", "")),
        "kline_type": str(payload.get("kline_type", "")),
    }
