"""AI execution_rules 适配器。

该模块负责把 AI 解析得到的 execution_rules（可能是文本/字典混合）
统一归一化为可执行计划 execution_plan，供回测或执行层后续接入。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List


def _collect_rule_text(execution_rules: Any, user_prompt: str) -> str:
    """聚合规则文本，兼容 list[str] / list[dict] / dict / str。"""
    parts: List[str] = []
    if isinstance(user_prompt, str) and user_prompt.strip():
        parts.append(user_prompt.strip())

    if isinstance(execution_rules, str) and execution_rules.strip():
        parts.append(execution_rules.strip())
    elif isinstance(execution_rules, dict):
        # 将字典值拼接为可检索文本，便于鲁棒匹配关键词。
        for v in execution_rules.values():
            if isinstance(v, (str, int, float)):
                parts.append(str(v))
    elif isinstance(execution_rules, list):
        for item in execution_rules:
            if isinstance(item, str) and item.strip():
                parts.append(item.strip())
                continue
            if isinstance(item, dict):
                for v in item.values():
                    if isinstance(v, (str, int, float)):
                        parts.append(str(v))
    return "；".join(parts)


def _extract_time(text: str, default_time: str = "") -> str:
    """从文本中提取 HH:MM 时间（如 14:55）。"""
    if not text:
        return default_time
    m = re.search(r"\b([01]?\d|2[0-3])[:：]([0-5]\d)\b", text)
    if not m:
        return default_time
    hour = int(m.group(1))
    minute = int(m.group(2))
    return f"{hour:02d}:{minute:02d}"


def _extract_all_percent_numbers(text: str) -> List[float]:
    """提取文本内百分比阈值数字，兼容“4个点/4%”。"""
    if not text:
        return []
    numbers: List[float] = []
    # 匹配“4个点”“8点”“6%”这类常见表达。
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*(?:个点|点|%)", text):
        try:
            numbers.append(float(m.group(1)))
        except Exception:
            continue
    return numbers


def _extract_top_n(text: str, default_n: int = 3) -> int:
    """提取选股数量，如“选出3只股票”。"""
    if not text:
        return default_n
    m = re.search(r"选(?:出)?\s*(\d+)\s*只", text)
    if not m:
        return default_n
    try:
        return max(1, int(m.group(1)))
    except Exception:
        return default_n


def _extract_position_pct(text: str, default_pct: float = 0.30) -> float:
    """提取单票仓位，如“三成仓/30%/0.3仓”。"""
    if not text:
        return default_pct
    if "三成" in text:
        return 0.30
    m_pct = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if m_pct:
        try:
            return max(0.01, min(float(m_pct.group(1)) / 100.0, 1.0))
        except Exception:
            return default_pct
    m_dec = re.search(r"(\d+(?:\.\d+)?)\s*成仓", text)
    if m_dec:
        try:
            return max(0.01, min(float(m_dec.group(1)) / 10.0, 1.0))
        except Exception:
            return default_pct
    return default_pct


def build_execution_plan(execution_rules: Any, user_prompt: str = "") -> Dict[str, Any]:
    """构建统一 execution_plan。

    说明：
    - 本计划是“可执行结构”，不直接下单；
    - 回测/实盘层可按字段逐步接入，避免一次性侵入核心交易引擎。
    """
    merged_text = _collect_rule_text(execution_rules, user_prompt)
    all_points = _extract_all_percent_numbers(merged_text)

    # 依据语义拆分止盈/止损阈值，若提取失败则保持空列表。
    profit_targets: List[Dict[str, float]] = []
    stop_losses: List[Dict[str, float]] = []
    if all_points:
        # 约定：前两个正向点位用于止盈（半仓/全仓），后两个用于止损（半仓/全仓）。
        # 这是面向当前策略的MVP规则，后续可用LLM结构化字段替换。
        if len(all_points) >= 2:
            profit_targets.append({"trigger_pct": abs(all_points[0]), "sell_ratio": 0.5})
            profit_targets.append({"trigger_pct": abs(all_points[1]), "sell_ratio": 1.0})
        if len(all_points) >= 4:
            stop_losses.append({"trigger_pct": -abs(all_points[2]), "sell_ratio": 0.5})
            stop_losses.append({"trigger_pct": -abs(all_points[3]), "sell_ratio": 1.0})

    entry_time = _extract_time(merged_text, default_time="14:55")
    sell_in_afternoon = ("下午" in merged_text) and ("卖" in merged_text)
    top_n = _extract_top_n(merged_text, default_n=3)
    per_stock_pct = _extract_position_pct(merged_text, default_pct=0.30)

    # 输出统一执行计划，包含A股强约束与时点语义。
    return {
        "constraints": {
            "t_plus_one": True,
            "no_buy_on_limit_up": True,
            "no_sell_on_limit_down": True,
        },
        "entry": {
            "mode": "intraday_fixed_time",
            "time": entry_time,
            "description": "尾盘固定时点入场",
        },
        "exit": {
            "profit_targets": profit_targets,
            "stop_losses": stop_losses,
            "fallback": {
                "enabled": bool(sell_in_afternoon),
                "mode": "afternoon_high_point_attempt",
                "time_window": ["13:00", "14:57"],
                "description": "若未触发止盈止损，次日下午择高点退出",
            },
        },
        "selection": {
            "rebalance_time": "14:55",
            "top_n": int(top_n),
            "logic": "daily_close_screening",
        },
        "position": {
            "per_stock_pct": float(per_stock_pct),
            "max_total_pct": 1.0,
        },
        "source_text": merged_text[:1000],
    }

