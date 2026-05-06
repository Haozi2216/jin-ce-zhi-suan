"""策略描述自然语言 → 筛选器结构化条件 + 执行规则 的 LLM 解析模块。

用户用自然语言描述策略（如"五日内有涨停，涨停后缩量至一半以下"），
本模块调用大模型将描述解析为：
1. screener_conditions：筛选器可执行的查询条件
2. execution_rules：回测引擎执行的买卖/仓控规则
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from src.utils.config_loader import ConfigLoader

# 大模型系统提示词 —— 约束输出格式，提高解析准确率
SYSTEM_PROMPT = """你是一个量化策略解析引擎。请将用户的自然语言策略描述解析为结构化 JSON 输出。

输出必须包含两个主要部分：
1. "screen_conditions": 筛选器可执行的条件列表
2. "execution_rules": 回测引擎执行的买卖规则（筛选器不执行）
3. "strategy_meta": 策略元信息

## 筛选器条件字段规范
每个条件对象必须包含：
- "key": 条件标识符（英文，如 "limit_up_5d", "volume_shrink", "price"）
- "tab": 所属指标tab（market/technical/financial）
- "operator": 运算符（gt/gte/lt/lte/between/toggle/has_event/formula）
- "label": 中文描述（用于前端展示）
- "value": 值（可选）
- "value2": 第二值（between 时用）
- "unit": 单位（如 "%"、"元"）
- "enabled": 是否可被筛选器直接执行（true/false）

## 支持的关键 condition key 列表
行情tab可用 key：
  price / change_pct / change_5d / change_20d / change_60d /
  volume / amount / turnover / amplitude / float_mv / total_mv /
  limit_up / limit_down / is_margin / listing_days / open_pct / high_pct / low_pct

技术指标tab可用 key：
  ma5 / ma10 / ma20 / ma60 / price_vs_ma20 /
  macd_dif / macd_dea / macd_hist / macd_golden_cross / macd_death_cross /
  kdj_k / kdj_d / kdj_j / rsi_6 / rsi_12 / rsi_24 /
  boll_upper / boll_mid / boll_lower / atr_14

## 特殊运算符说明
- "has_event": 近N日发生过某事件（如涨停）。value=N
- "formula": 需要计算的关系表达式。formula="volume < volume_limit_up * 0.5"
- "between": 区间范围。value=最小, value2=最大
- "toggle": 布尔开关

## 输出格式要求
必须输出合法 JSON，不要 markdown 代码块包裹，不要额外文字。"""


def _extract_json(text: str) -> Optional[dict]:
    """从大模型回复中提取 JSON（容错：去掉 markdown 包裹）。"""
    if not text:
        return None
    text = text.strip()
    # 去掉 ```json ... ``` 包裹
    m = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    if m:
        text = m.group(1).strip()
    # 找第一个 { 到最后一个 }
    start = text.find("{")
    if start == -1:
        return None
    end = text.rfind("}")
    if end == -1 or end <= start:
        return None
    body = text[start:end + 1]
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def _classify_condition(key: str) -> Tuple[str, bool]:
    """判断条件所属 tab 和是否可筛选器直接执行。"""
    market_keys = {
        "price", "change_pct", "change_5d", "change_20d", "change_60d",
        "volume", "amount", "turnover", "amplitude", "float_mv", "total_mv",
        "limit_up", "limit_down", "is_margin", "listing_days", "open_pct",
        "high_pct", "low_pct", "hk_hold_ratio", "net_inflow", "total_shares",
        "float_shares", "listing_days2", "trading_days",
        # 时序条件
        "limit_up_5d", "limit_up_10d", "limit_up_20d",
        "volume_shrink", "volume_expand",
        "price_break_ma", "price_below_ma",
        "ma_golden_cross", "ma_death_cross",
        "multi_day_change", "consecutive_up", "consecutive_down",
        "high_5d", "low_5d", "high_20d", "low_20d",
        "avg_volume_5d", "avg_volume_20d",
    }
    technical_keys = {
        "ma5", "ma10", "ma20", "ma60", "price_vs_ma20",
        "macd_dif", "macd_dea", "macd_hist", "macd_golden_cross", "macd_death_cross",
        "kdj_k", "kdj_d", "kdj_j",
        "rsi_6", "rsi_12", "rsi_24",
        "boll_upper", "boll_mid", "boll_lower", "atr_14",
    }
    financial_keys = {
        "pe_ttm", "pb", "ps_ttm", "roe", "roa", "gross_margin",
        "net_margin", "revenue_yoy", "profit_yoy",
    }

    if key in technical_keys:
        return "technical", True
    if key in financial_keys:
        return "financial", True
    if key in market_keys:
        return "market", True
    # 未知 key 归到行情
    return "market", False


def parse_strategy_to_conditions(text: str) -> Dict[str, Any]:
    """解析策略描述为结构化条件。

    返回结构：
    {
        "status": "success" | "error",
        "data": {
            "screen_conditions": [...],
            "execution_rules": [...],
            "strategy_meta": {"name": "...", "description": "..."},
            "llm_info": {"provider": "...", "model": "..."}
        },
        "raw_llm": "..."
    }
    """
    raw_text = str(text or "").strip()
    if not raw_text:
        return {"status": "error", "msg": "策略描述不能为空"}

    # 构建 LLM 客户端（复用已有 build_unified_llm_client，先 strategy_manager 域再 unified）
    cfg = ConfigLoader()
    try:
        from src.evolution.adapters.llm_gateway_adapter import build_unified_llm_client
        client = build_unified_llm_client(cfg, scope="strategy_manager")
        if not client.cfg.is_ready():
            client = build_unified_llm_client(cfg, scope="unified")
        if not client.cfg.is_ready():
            return {
                "status": "error",
                "msg": "大模型未配置，请在 config.json 的 evolution.llm 或 data_provider 中配置 LLM 信息",
            }
    except Exception as e:
        return {"status": "error", "msg": f"LLM 客户端初始化失败: {e}"}

    # 构建提示词
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"""请将以下策略描述解析为结构化 JSON：

{raw_text}

要求：
1. 筛选条件要精确映射到支持的 key（如 price、change_pct、limit_up_5d 等）
2. 买卖时机、止损止盈、仓位管理等放到 execution_rules
3. 时间相关的筛选条件用 operator="has_event" 或 operator="formula"
4. 确保每个条件的 operator 和 value 类型匹配（数值用 number，布尔用 toggle）"""},
    ]

    # 调用 LLM
    try:
        result = client.complete(messages, temperature=0.1, max_tokens=2048)
        raw_llm = result.get("content", "")
        parsed = _extract_json(raw_llm)
    except Exception as e:
        return {"status": "error", "msg": f"LLM 调用失败: {e}"}

    if parsed is None:
        return {
            "status": "error",
            "msg": "大模型返回结果格式异常，请重新描述策略",
            "raw_llm": raw_llm,
        }

    # 验证和标准化
    screen_conditions = _normalize_conditions(parsed.get("screen_conditions", []))
    execution_rules = parsed.get("execution_rules", [])
    strategy_meta = parsed.get("strategy_meta", {"name": "未命名策略", "description": raw_text[:100]})

    return {
        "status": "success",
        "data": {
            "screen_conditions": screen_conditions,
            "execution_rules": execution_rules,
            "strategy_meta": strategy_meta,
            "llm_info": {
                "provider": result.get("provider", ""),
                "model": result.get("model", ""),
            },
        },
        "raw_llm": raw_llm,
    }


def _normalize_conditions(conditions: list) -> List[Dict[str, Any]]:
    """标准化筛选条件：补全缺省字段，校验类型。"""
    if not isinstance(conditions, list):
        return []

    normalized = []
    for cond in conditions:
        if not isinstance(cond, dict):
            continue
        key = str(cond.get("key", "")).strip()
        if not key:
            continue

        tab, executable = _classify_condition(key)
        operator = str(cond.get("operator", "")).strip()
        label = str(cond.get("label", key)).strip()

        item = {
            "key": key,
            "tab": tab,
            "operator": operator,
            "label": label,
            "value": cond.get("value"),
            "value2": cond.get("value2"),
            "unit": cond.get("unit", ""),
            "enabled": executable,
        }

        # 处理 toggle 类型
        if operator == "toggle":
            item["value"] = True
            item["unit"] = ""

        # 处理 has_event 类型（近N日事件检测）
        if operator == "has_event":
            v = cond.get("value")
            if isinstance(v, (int, float)):
                item["value"] = int(v)
            else:
                # value 可能是天数，默认 5 日
                item["value"] = 5

        # 处理 formula 类型
        if operator == "formula":
            item["formula"] = str(cond.get("formula", ""))

        normalized.append(item)

    return normalized
