"""E2E 演示脚本：示例策略入库并执行一次回测评估。

使用方式（项目根目录）：
    python -m src.evolution.runner.run_e2e_demo_flow --stock 000001.SZ --timeframe 1day

说明：
- 该脚本只依赖现有适配器，不修改回测引擎；
- 默认会先 upsert 演示策略，再调用 BacktestAdapter 评估；
- 输出 JSON，方便对接后续看板或自动化流水线。
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Dict

from src.evolution.adapters.backtest_adapter import BacktestAdapter
from src.evolution.adapters.e2e_demo_strategy_adapter import (
    DEMO_STRATEGY_ID,
    DEMO_STRATEGY_NAME,
    build_demo_strategy_payload,
    upsert_demo_strategy,
)


def run_demo_once(stock_code: str, timeframe: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """执行一次完整演示：策略入库 -> 回测评估 -> 输出结果。"""
    # 第一步：将示例策略写入策略库（存在则更新）。
    upsert_result = upsert_demo_strategy(strategy_id=DEMO_STRATEGY_ID, strategy_name=DEMO_STRATEGY_NAME)
    # 第二步：构建策略代码并调用回测适配器执行评估。
    payload = build_demo_strategy_payload(strategy_id=DEMO_STRATEGY_ID, strategy_name=DEMO_STRATEGY_NAME)
    strategy_code = str(payload.get("code", "") or "")
    adapter = BacktestAdapter()
    # 解析时间范围，格式固定 yyyy-mm-dd，便于自动化脚本稳定输入。
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    # 执行单场景评估，timeframe 使用列表形式复用适配器能力。
    metrics = adapter.run_backtest(
        strategy_code=strategy_code,
        stock_code=str(stock_code or "").strip(),
        timeframes=[str(timeframe or "").strip()],
        start_date=start_dt,
        end_date=end_dt,
    )
    # 返回结构化结果，方便命令行和其他模块统一消费。
    return {
        "status": "success",
        "upsert_result": upsert_result,
        "run_config": {
            "stock_code": str(stock_code or "").strip(),
            "timeframe": str(timeframe or "").strip(),
            "start_date": start_date,
            "end_date": end_date,
        },
        "metrics": metrics,
    }


def main() -> None:
    """命令行入口。"""
    # 通过参数化配置演示标的与周期，便于多场景复用。
    parser = argparse.ArgumentParser(description="运行 E2E 演示策略全流程")
    parser.add_argument("--stock", default="000001.SZ", help="回测标的，例如 000001.SZ")
    parser.add_argument("--timeframe", default="1day", help="回测周期，例如 1day/1min")
    parser.add_argument("--start", default="2024-01-01", help="开始日期，格式 yyyy-mm-dd")
    parser.add_argument("--end", default="2024-12-31", help="结束日期，格式 yyyy-mm-dd")
    args = parser.parse_args()
    # 运行并输出 JSON；ensure_ascii=False 便于中文阅读。
    result = run_demo_once(
        stock_code=args.stock,
        timeframe=args.timeframe,
        start_date=args.start,
        end_date=args.end,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    # 直接脚本执行时进入主流程。
    main()

