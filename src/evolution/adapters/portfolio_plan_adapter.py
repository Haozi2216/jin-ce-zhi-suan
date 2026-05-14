"""筛选结果组合计划适配器。

根据 execution_plan 的 top_n 与单票仓位，从筛选结果构建可执行的组合建议。
"""

from __future__ import annotations

from typing import Any, Dict, List


def build_portfolio_plan(filter_result: Dict[str, Any], execution_plan: Dict[str, Any]) -> Dict[str, Any]:
    """构建组合计划（不下单，只返回结构化建议）。"""
    rows = []
    if isinstance(filter_result, dict):
        raw_rows = filter_result.get("data", [])
        if isinstance(raw_rows, list):
            rows = raw_rows

    selection = execution_plan.get("selection", {}) if isinstance(execution_plan, dict) else {}
    position = execution_plan.get("position", {}) if isinstance(execution_plan, dict) else {}
    top_n = int(selection.get("top_n", 3) or 3)
    per_stock_pct = float(position.get("per_stock_pct", 0.30) or 0.30)
    max_total_pct = float(position.get("max_total_pct", 1.0) or 1.0)

    if top_n <= 0:
        top_n = 3
    candidates = rows[:top_n]
    if not candidates:
        return {
            "selected": [],
            "suggested_total_pct": 0.0,
            "reason": "筛选结果为空",
        }

    # 若总仓超过上限，自动回退为等权，避免组合建议不可执行。
    total_pct = per_stock_pct * len(candidates)
    if total_pct > max_total_pct:
        per_stock_pct = round(max_total_pct / len(candidates), 4)
        total_pct = round(per_stock_pct * len(candidates), 4)
    else:
        total_pct = round(total_pct, 4)

    selected: List[Dict[str, Any]] = []
    for row in candidates:
        code = str(
            row.get("code", "")
            or row.get("stock_code", "")
            or row.get("ts_code", "")
            or row.get("symbol", "")
        ).strip().upper()
        selected.append(
            {
                "code": code,
                "name": str(row.get("name", "") or row.get("stock_name", "") or ""),
                "target_weight": per_stock_pct,
                "price": row.get("price"),
                "change_pct": row.get("change_pct"),
            }
        )

    return {
        "selected": selected,
        "suggested_total_pct": total_pct,
        "top_n": len(selected),
        "position_per_stock_pct": per_stock_pct,
    }

