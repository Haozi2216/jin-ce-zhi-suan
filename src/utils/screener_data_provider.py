"""条件筛选器数据访问层。

从 DuckDB/stock_manager 等来源获取标的列表、行情指标、技术指标，
为 /api/screener/* 接口提供数据。
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np

from src.utils.stock_manager import stock_manager

CACHE_DIR = os.path.join("data", "screener_cache")
CACHE_TTL_SEC = 300  # 5 分钟


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _read_cache(key: str, ttl: int = CACHE_TTL_SEC) -> Optional[Any]:
    try:
        path = os.path.join(CACHE_DIR, f"{key}.json")
        if not os.path.exists(path):
            return None
        mtime = os.path.getmtime(path)
        if time.time() - mtime > ttl:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_cache(key: str, data: Any):
    _ensure_cache_dir()
    path = os.path.join(CACHE_DIR, f"{key}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# 交易所派生（基于代码前缀，无需外部依赖）
# ---------------------------------------------------------------------------
def _infer_exchange(code: str) -> str:
    """根据股票代码前缀推断交易所。"""
    c = str(code).strip().upper()
    if c.startswith("6"):
        return "上交所"
    if c.startswith("0") or c.startswith("3"):
        return "深交所"
    if c.startswith("8") or c.startswith("4"):
        return "北交所"
    return "未知"


# ---------------------------------------------------------------------------
# 前置过滤选项
# ---------------------------------------------------------------------------
def get_filter_options() -> Dict[str, List[str]]:
    """返回4个前置下拉框的可选项。"""
    stock_manager.ensure_loaded()
    stocks = stock_manager.stocks
    exchanges = sorted({_infer_exchange(s.get("code", "")) for s in stocks})
    # Phase 1: 地域/企业性质/两融 返回占位，Phase 2 接 Tushare
    return {
        "exchange": ["全部"] + [e for e in exchanges if e != "未知"],
        "region": [],       # TODO: Tushare stock_basic.area
        "enterprise_type": [],  # TODO: Tushare stock_basic.list_date / 性质
        "margin_trading": ["全部", "融资", "融券", "两融"],  # TODO: Tushare margin trading
    }


# ---------------------------------------------------------------------------
# 筛选条件目录树（8 个 tab 下的分类与字段）
# ---------------------------------------------------------------------------
def get_catalog() -> Dict[str, List[Dict[str, Any]]]:
    """返回全部筛选条件的目录结构。

    每个 tab 下是 category 列表，每个 category 包含：
      - category: 分类名
      - fields: 字段列表，每个字段有 {key, label, type, unit, options, ...}
    """
    return {
        "market": [
            {
                "category": "股票价格",
                "fields": [
                    {"key": "price", "label": "最新价", "type": "range", "unit": "元", "min_val": 0, "max_val": 9999},
                ],
            },
            {
                "category": "股价涨幅",
                "fields": [
                    {"key": "change_pct", "label": "涨跌幅", "type": "range", "unit": "%", "min_val": -20, "max_val": 20, "step": 0.1},
                    {"key": "change_5d", "label": "近5日涨幅", "type": "range", "unit": "%", "min_val": -50, "max_val": 50},
                    {"key": "change_20d", "label": "近20日涨幅", "type": "range", "unit": "%", "min_val": -50, "max_val": 50},
                    {"key": "change_60d", "label": "近60日涨幅", "type": "range", "unit": "%", "min_val": -50, "max_val": 50},
                ],
            },
            {
                "category": "涨跌停标记",
                "fields": [
                    {"key": "limit_up", "label": "涨停", "type": "toggle"},
                    {"key": "limit_down", "label": "跌停", "type": "toggle"},
                ],
            },
            {
                "category": "融资融券",
                "fields": [
                    {"key": "is_margin", "label": "融资融券标的", "type": "toggle"},
                ],
            },
            {
                "category": "股价振幅",
                "fields": [
                    {"key": "amplitude", "label": "振幅", "type": "range", "unit": "%", "min_val": 0, "max_val": 20},
                ],
            },
            {
                "category": "成交额",
                "fields": [
                    {"key": "amount", "label": "成交额", "type": "range", "unit": "万元", "min_val": 0, "max_val": 99999999},
                ],
            },
            {
                "category": "成交量",
                "fields": [
                    {"key": "volume", "label": "成交量", "type": "range", "unit": "手", "min_val": 0, "max_val": 99999999},
                ],
            },
            {
                "category": "换手率",
                "fields": [
                    {"key": "turnover", "label": "换手率", "type": "range", "unit": "%", "min_val": 0, "max_val": 50},
                ],
            },
            {
                "category": "股本和市值",
                "fields": [
                    {"key": "total_mv", "label": "总市值", "type": "range", "unit": "亿元", "min_val": 0, "max_val": 999999},
                    {"key": "float_mv", "label": "流通市值", "type": "range", "unit": "亿元", "min_val": 0, "max_val": 999999},
                    {"key": "total_shares", "label": "总股本", "type": "range", "unit": "亿股", "min_val": 0, "max_val": 99999},
                    {"key": "float_shares", "label": "流通股本", "type": "range", "unit": "亿股", "min_val": 0, "max_val": 99999},
                ],
            },
            {
                "category": "资金净流入",
                "fields": [
                    {"key": "net_inflow", "label": "主力净流入", "type": "range", "unit": "万元", "min_val": -999999, "max_val": 999999},
                ],
            },
            {
                "category": "港资持股",
                "fields": [
                    {"key": "hk_hold_ratio", "label": "港资持股比例", "type": "range", "unit": "%", "min_val": 0, "max_val": 50},
                ],
            },
            {
                "category": "日内行情",
                "fields": [
                    {"key": "open_pct", "label": "开盘涨幅", "type": "range", "unit": "%", "min_val": -20, "max_val": 20},
                    {"key": "high_pct", "label": "最高涨幅", "type": "range", "unit": "%", "min_val": -20, "max_val": 20},
                    {"key": "low_pct", "label": "最低涨幅", "type": "range", "unit": "%", "min_val": -20, "max_val": 20},
                ],
            },
            {
                "category": "新股指标",
                "fields": [
                    {"key": "is_new", "label": "上市新股(近60日)", "type": "toggle"},
                    {"key": "listing_days", "label": "上市天数", "type": "range", "min_val": 1, "max_val": 10000},
                ],
            },
            {
                "category": "AH股溢价率",
                "fields": [
                    {"key": "ah_premium", "label": "AH溢价率", "type": "range", "unit": "%", "min_val": -100, "max_val": 500},
                ],
            },
            {
                "category": "上市天数",
                "fields": [
                    {"key": "listing_days2", "label": "上市天数", "type": "range", "min_val": 1, "max_val": 10000},
                ],
            },
            {
                "category": "交易天数",
                "fields": [
                    {"key": "trading_days", "label": "近N日交易天数", "type": "range", "min_val": 1, "max_val": 250},
                ],
            },
        ],
        "technical": [
            {
                "category": "均线系统",
                "fields": [
                    {"key": "ma5", "label": "MA5", "type": "range", "unit": "元"},
                    {"key": "ma10", "label": "MA10", "type": "range", "unit": "元"},
                    {"key": "ma20", "label": "MA20", "type": "range", "unit": "元"},
                    {"key": "ma60", "label": "MA60", "type": "range", "unit": "元"},
                    {"key": "price_vs_ma20", "label": "股价相对MA20", "type": "range", "unit": "%"},
                ],
            },
            {
                "category": "MACD",
                "fields": [
                    {"key": "macd_dif", "label": "DIF", "type": "range"},
                    {"key": "macd_dea", "label": "DEA", "type": "range"},
                    {"key": "macd_hist", "label": "MACD柱", "type": "range"},
                    {"key": "macd_golden_cross", "label": "金叉", "type": "toggle"},
                    {"key": "macd_death_cross", "label": "死叉", "type": "toggle"},
                ],
            },
            {
                "category": "KDJ",
                "fields": [
                    {"key": "kdj_k", "label": "K值", "type": "range", "min_val": 0, "max_val": 100},
                    {"key": "kdj_d", "label": "D值", "type": "range", "min_val": 0, "max_val": 100},
                    {"key": "kdj_j", "label": "J值", "type": "range", "min_val": 0, "max_val": 100},
                ],
            },
            {
                "category": "RSI",
                "fields": [
                    {"key": "rsi_6", "label": "RSI6", "type": "range", "min_val": 0, "max_val": 100},
                    {"key": "rsi_12", "label": "RSI12", "type": "range", "min_val": 0, "max_val": 100},
                    {"key": "rsi_24", "label": "RSI24", "type": "range", "min_val": 0, "max_val": 100},
                ],
            },
            {
                "category": "BOLL",
                "fields": [
                    {"key": "boll_upper", "label": "上轨", "type": "range", "unit": "元"},
                    {"key": "boll_mid", "label": "中轨", "type": "range", "unit": "元"},
                    {"key": "boll_lower", "label": "下轨", "type": "range", "unit": "元"},
                ],
            },
            {
                "category": "ATR",
                "fields": [
                    {"key": "atr_14", "label": "ATR14", "type": "range", "unit": "元"},
                ],
            },
        ],
        "financial": [
            {
                "category": "估值指标",
                "fields": [
                    {"key": "pe_ttm", "label": "PE(TTM)", "type": "range"},
                    {"key": "pb", "label": "PB", "type": "range"},
                    {"key": "ps_ttm", "label": "PS(TTM)", "type": "range"},
                ],
            },
            {
                "category": "盈利指标",
                "fields": [
                    {"key": "roe", "label": "ROE", "type": "range", "unit": "%"},
                    {"key": "roa", "label": "ROA", "type": "range", "unit": "%"},
                    {"key": "gross_margin", "label": "毛利率", "type": "range", "unit": "%"},
                    {"key": "net_margin", "label": "净利率", "type": "range", "unit": "%"},
                ],
            },
            {
                "category": "成长性",
                "fields": [
                    {"key": "revenue_yoy", "label": "营收同比增长", "type": "range", "unit": "%"},
                    {"key": "profit_yoy", "label": "净利润同比增长", "type": "range", "unit": "%"},
                ],
            },
        ],
        "report": [
            {"category": "财报条目", "fields": [
                {"key": "report_note", "label": "说明", "type": "text", "placeholder": "Phase 2 支持"},
            ]},
        ],
        "company": [
            {"category": "公司信息", "fields": [
                {"key": "company_note", "label": "说明", "type": "text", "placeholder": "Phase 2 支持"},
            ]},
        ],
        "analyst": [
            {"category": "分析师评级", "fields": [
                {"key": "analyst_note", "label": "说明", "type": "text", "placeholder": "Phase 2 支持"},
            ]},
        ],
        "index": [
            {"category": "大盘指标", "fields": [
                {"key": "index_note", "label": "说明", "type": "text", "placeholder": "Phase 2 支持"},
            ]},
        ],
        "custom": [
            {"category": "自定义条件", "fields": [
                {"key": "custom_expr", "label": "表达式", "type": "text", "placeholder": "例如: price < 10 AND turnover > 3"},
            ]},
        ],
    }


# ---------------------------------------------------------------------------
# 核心筛选逻辑
# ---------------------------------------------------------------------------
def get_duckdb_conn():
    """获取 DuckDB 连接（复用已有 provider）。"""
    try:
        from src.utils.duckdb_provider import DuckDbProvider
        provider = DuckDbProvider()
        return provider.get_connection()
    except Exception:
        return None


def _compute_technical_indicators(df: pd.DataFrame) -> Dict[str, float]:
    """对日K线 DataFrame 计算常用技术指标，返回 dict。"""
    result = {}
    if df is None or df.empty or len(df) < 20:
        return result

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    volume = df["volume"].astype(float)

    # MA
    for n in [5, 10, 20, 60]:
        ma = close.rolling(window=n).mean()
        result[f"ma{n}"] = float(ma.iloc[-1]) if not ma.iloc[-1] != ma.iloc[-1] else None  # noqa: E711

    # MACD (12, 26, 9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    hist = 2 * (dif - dea)
    result["macd_dif"] = float(dif.iloc[-1]) if not dif.iloc[-1] != dif.iloc[-1] else None
    result["macd_dea"] = float(dea.iloc[-1]) if not dea.iloc[-1] != dea.iloc[-1] else None
    result["macd_hist"] = float(hist.iloc[-1]) if not hist.iloc[-1] != hist.iloc[-1] else None
    if len(dif) >= 2:
        result["macd_golden_cross"] = bool(dif.iloc[-1] > dea.iloc[-1] and dif.iloc[-2] <= dea.iloc[-2])
        result["macd_death_cross"] = bool(dif.iloc[-1] < dea.iloc[-1] and dif.iloc[-2] >= dea.iloc[-2])

    # KDJ (9, 3, 3)
    low_n = low.rolling(window=9).min()
    high_n = high.rolling(window=9).max()
    rsv = (close - low_n) / (high_n - low_n) * 100
    k = rsv.ewm(com=2, adjust=False).mean()
    d = k.ewm(com=2, adjust=False).mean()
    j = 3 * k - 2 * d
    result["kdj_k"] = float(k.iloc[-1]) if not k.iloc[-1] != k.iloc[-1] else None
    result["kdj_d"] = float(d.iloc[-1]) if not d.iloc[-1] != d.iloc[-1] else None
    result["kdj_j"] = float(j.iloc[-1]) if not j.iloc[-1] != j.iloc[-1] else None

    # RSI (6, 12, 24)
    for period in [6, 12, 24]:
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - 100 / (1 + rs)
        result[f"rsi_{period}"] = float(rsi.iloc[-1]) if not rsi.iloc[-1] != rsi.iloc[-1] else None

    # BOLL (20, 2)
    ma20 = close.rolling(window=20).mean()
    std20 = close.rolling(window=20).std()
    result["boll_upper"] = float(ma20.iloc[-1] + 2 * std20.iloc[-1]) if not std20.iloc[-1] != std20.iloc[-1] else None
    result["boll_mid"] = float(ma20.iloc[-1]) if not ma20.iloc[-1] != ma20.iloc[-1] else None
    result["boll_lower"] = float(ma20.iloc[-1] - 2 * std20.iloc[-1]) if not std20.iloc[-1] != std20.iloc[-1] else None

    # ATR (14)
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=14).mean()
    result["atr_14"] = float(atr.iloc[-1]) if not atr.iloc[-1] != atr.iloc[-1] else None

    return result


def fetch_latest_metrics() -> List[Dict[str, Any]]:
    """获取所有标的最新行情指标，带文件缓存。"""
    cached = _read_cache("latest_metrics")
    if cached is not None:
        return cached

    stock_manager.ensure_loaded()
    stocks = stock_manager.stocks
    conn = get_duckdb_conn()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    results = []
    for s in stocks:
        code = str(s.get("code", "")).strip()
        name = str(s.get("name", "")).strip()
        if not code:
            continue
        exchange = _infer_exchange(code)

        # 补齐 .SH/.SZ 后缀用于 DuckDB 查询
        db_code = code
        if "." not in db_code:
            if db_code.startswith("6"):
                db_code = f"{db_code}.SH"
            elif db_code.startswith("0") or db_code.startswith("3"):
                db_code = f"{db_code}.SZ"
            elif db_code.startswith("8") or db_code.startswith("4"):
                db_code = f"{db_code}.BJ"

        row = {"code": code, "name": name, "exchange": exchange, "screener_time": now_str}

        if conn:
            try:
                df = conn.execute(
                    "SELECT * FROM dat_day WHERE code = ? ORDER BY trade_date DESC LIMIT 1",
                    [db_code]
                ).fetchdf()
                if df is not None and not df.empty and len(df) > 0:
                    latest = df.iloc[0]
                    close_val = float(latest.get("close", 0) or 0)
                    open_val = float(latest.get("open", 0) or 0)
                    high_val = float(latest.get("high", 0) or 0)
                    low_val = float(latest.get("low", 0) or 0)
                    volume_val = float(latest.get("volume", 0) or 0)
                    amount_val = float(latest.get("amount", 0) or 0)
                    turnover_val = float(latest.get("turnover", 0) or 0)
                    pre_close = float(latest.get("pre_close", 0) or 0)
                    if pre_close <= 0:
                        pre_close = close_val

                    row["price"] = round(close_val, 2)
                    row["open_pct"] = round((open_val - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0
                    row["high_pct"] = round((high_val - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0
                    row["low_pct"] = round((low_val - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0
                    row["change_pct"] = round((close_val - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0
                    row["volume"] = int(volume_val)
                    row["amount"] = round(amount_val / 10000, 2)  # 转为万元
                    row["turnover"] = round(turnover_val, 2)
                    row["amplitude"] = round((high_val - low_val) / pre_close * 100, 2) if pre_close > 0 else 0
                    row["limit_up"] = abs(row["change_pct"] - 10.0) < 0.1 or abs(row["change_pct"] - 20.0) < 0.1
                    row["limit_down"] = abs(row["change_pct"] + 10.0) < 0.1 or abs(row["change_pct"] + 20.0) < 0.1

                    # 计算技术指标
                    tech = _compute_technical_indicators(df)
                    row.update(tech)

                    # 近5/20/60日涨幅
                    for days, key in [(5, "change_5d"), (20, "change_20d"), (60, "change_60d")]:
                        df_hist = conn.execute(
                            "SELECT close FROM dat_day WHERE code = ? ORDER BY trade_date DESC LIMIT ?",
                            [db_code, days + 1]
                        ).fetchdf()
                        if df_hist is not None and len(df_hist) > 1:
                            old_close = float(df_hist.iloc[-1]["close"])
                            if old_close > 0:
                                row[key] = round((close_val - old_close) / old_close * 100, 2)
                            else:
                                row[key] = 0
                        else:
                            row[key] = 0
            except Exception:
                pass

        results.append(row)

    _write_cache("latest_metrics", results)
    return results


def apply_filters(
    exchange: Optional[str] = None,
    region: Optional[str] = None,
    enterprise_type: Optional[str] = None,
    margin_trading: Optional[str] = None,
    market_conditions: Optional[List[Dict[str, Any]]] = None,
    technical_conditions: Optional[List[Dict[str, Any]]] = None,
    financial_conditions: Optional[List[Dict[str, Any]]] = None,
    logic_mode: str = "AND",
    page: int = 1,
    page_size: int = 50,
    sort_by: Optional[str] = None,
    sort_order: str = "desc",
) -> Dict[str, Any]:
    """应用筛选条件，返回分页结果。"""
    metrics = fetch_latest_metrics()

    # 前置过滤
    filtered = metrics
    if exchange and exchange != "全部":
        filtered = [r for r in filtered if r.get("exchange") == exchange]

    # Phase 1: region/enterprise/margin 无数据源，直接跳过或按标记过滤
    if margin_trading and margin_trading != "全部":
        # Phase 1 无两融数据，返回空
        filtered = []

    # 合并所有条件
    all_conditions = []
    if market_conditions:
        all_conditions.extend(market_conditions)
    if technical_conditions:
        all_conditions.extend(technical_conditions)
    if financial_conditions:
        all_conditions.extend(financial_conditions)

    # 分离需要 DuckDB 时序查询的条件
    time_series_keys = {
        "limit_up_5d", "limit_up_10d", "limit_up_20d",
        "volume_shrink", "volume_expand",
        "consecutive_up", "consecutive_down",
        "high_5d", "low_5d", "high_20d", "low_20d",
        "avg_volume_5d", "avg_volume_20d",
        "multi_day_change",
    }

    simple_conditions = [c for c in all_conditions if c.get("key") not in time_series_keys and c.get("operator") not in ("has_event", "formula")]
    ts_conditions = [c for c in all_conditions if c.get("key") in time_series_keys or c.get("operator") in ("has_event", "formula")]

    # 先过滤简单条件
    def _match_field(row: Dict, cond: Dict) -> bool:
        key = cond.get("key", "")
        op = cond.get("operator", "")
        val = cond.get("value")
        val2 = cond.get("value2")
        actual = row.get(key)

        if actual is None:
            return False

        try:
            actual = float(actual)
        except (ValueError, TypeError):
            return False

        if op == "gt":
            return actual > float(val) if val is not None else False
        if op == "gte":
            return actual >= float(val) if val is not None else False
        if op == "lt":
            return actual < float(val) if val is not None else False
        if op == "lte":
            return actual <= float(val) if val is not None else False
        if op == "eq":
            return actual == float(val) if val is not None else False
        if op == "between":
            if val is not None and val2 is not None:
                return float(val) <= actual <= float(val2)
            if val is not None:
                return actual >= float(val)
            if val2 is not None:
                return actual <= float(val2)
            return False
        if op == "toggle":
            return bool(actual)
        return True

    def _match_simple(row: Dict) -> bool:
        if not simple_conditions:
            return True
        if logic_mode == "OR":
            return any(_match_field(row, c) for c in simple_conditions)
        return all(_match_field(row, c) for c in simple_conditions)

    filtered = [r for r in filtered if _match_simple(r)]

    # 对过滤后的候选标的执行时序条件查询
    if ts_conditions:
        conn = get_duckdb_conn()
        if conn:
            filtered = _apply_time_series_conditions(filtered, ts_conditions, conn, logic_mode)
        else:
            # 无 DuckDB，时序条件全部不通过
            filtered = []

    # 排序
    if sort_by:
        reverse = sort_order == "desc"
        filtered.sort(key=lambda r: r.get(sort_by, 0) or 0, reverse=reverse)

    # 分页
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    page_data = filtered[start:end]

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size),
        "data": page_data,
    }


# ---------------------------------------------------------------------------
# 时序查询引擎
# ---------------------------------------------------------------------------
def _is_limit_up(change_pct: float) -> bool:
    """判断是否涨停：主板 ±10%，科创/创业 ±20%。"""
    return abs(change_pct - 10.0) < 0.1 or abs(change_pct - 20.0) < 0.1


def _apply_time_series_conditions(
    rows: List[Dict],
    conditions: List[Dict],
    conn,
    logic_mode: str,
) -> List[Dict]:
    """对候选标的执行时序条件（基于 DuckDB dat_day 历史数据）。"""
    results = []
    for row in rows:
        code = str(row.get("code", "")).strip()
        if not code:
            continue
        db_code = code
        if "." not in db_code:
            if db_code.startswith("6"):
                db_code = f"{db_code}.SH"
            elif db_code.startswith("0") or db_code.startswith("3"):
                db_code = f"{db_code}.SZ"
            elif db_code.startswith("8") or db_code.startswith("4"):
                db_code = f"{db_code}.BJ"

        # 按需查询历史数据
        history_cache = {}
        condition_results = []

        for cond in conditions:
            key = cond.get("key", "")
            op = cond.get("operator", "")
            val = cond.get("value")

            try:
                passed = _evaluate_time_series(code, db_code, key, op, val, conn, history_cache)
            except Exception:
                passed = False

            condition_results.append(passed)

        if logic_mode == "OR":
            if any(condition_results):
                results.append(row)
        else:
            if all(condition_results):
                results.append(row)

    return results


def _evaluate_time_series(
    code: str,
    db_code: str,
    key: str,
    op: str,
    value: Any,
    conn,
    history_cache: Dict[str, Any],
) -> bool:
    """评估单个时序条件。"""
    # 获取历史数据（带缓存）
    if "history" not in history_cache:
        df = conn.execute(
            "SELECT trade_date, open, high, low, close, pre_close, volume, amount, turnover "
            "FROM dat_day WHERE code = ? ORDER BY trade_date DESC LIMIT 100",
            [db_code]
        ).fetchdf()
        history_cache["history"] = df if df is not None and not df.empty else None

    df = history_cache.get("history")
    if df is None or df.empty:
        return False

    close = df["close"].astype(float)
    volume = df["volume"].astype(float)
    pre_close = df["pre_close"].astype(float)
    change_pct = ((close - pre_close) / pre_close * 100).fillna(0)

    # --- has_event: 近N日发生过某事件 ---
    if op == "has_event":
        n_days = int(value or 5)
        if key in ("limit_up_5d", "limit_up_10d", "limit_up_20d"):
            n_days_map = {"limit_up_5d": 5, "limit_up_10d": 10, "limit_up_20d": 20}
            n_days = n_days_map.get(key, int(value or 5))
        recent_change = change_pct.iloc[:n_days]
        return any(_is_limit_up(c) for c in recent_change)

    # --- limit_up_Nd: 近N日有涨停（has_event 的别名） ---
    if key in ("limit_up_5d", "limit_up_10d", "limit_up_20d"):
        n_days_map = {"limit_up_5d": 5, "limit_up_10d": 10, "limit_up_20d": 20}
        n_days = n_days_map.get(key, 5)
        recent_change = change_pct.iloc[:n_days]
        return any(_is_limit_up(c) for c in recent_change)

    # --- volume_shrink: 涨停后N日内成交量 < 涨停日成交量 * 比例 ---
    if key == "volume_shrink":
        n_days = int(value or 5)
        # 找到最近一次涨停日
        limit_up_idx = None
        for i in range(min(20, len(change_pct))):
            if _is_limit_up(change_pct.iloc[i]):
                limit_up_idx = i
                break
        if limit_up_idx is None:
            return False
        # 涨停日成交量
        lu_volume = volume.iloc[limit_up_idx]
        if lu_volume <= 0:
            return False
        # 检查涨停后N日内是否有某日成交量 < 涨停日的一半
        formula = str(value or "0.5")  # 默认一半
        try:
            threshold = float(formula)
        except ValueError:
            threshold = 0.5
        for i in range(1, min(n_days + 1, len(volume) - limit_up_idx)):
            if volume.iloc[limit_up_idx + i] < lu_volume * threshold:
                return True
        return False

    # --- volume_expand: 近N日成交量放大 ---
    if key == "volume_expand":
        n_days = int(value or 5)
        if len(volume) < n_days + 1:
            return False
        avg_recent = volume.iloc[:n_days].mean()
        avg_prev = volume.iloc[n_days: n_days * 2].mean()
        return avg_recent > avg_prev * 1.5  # 放量50%

    # --- consecutive_up: 近N日连涨 ---
    if key == "consecutive_up":
        n_days = int(value or 5)
        recent_change = change_pct.iloc[:n_days]
        return all(c > 0 for c in recent_change)

    # --- consecutive_down: 近N日连跌 ---
    if key == "consecutive_down":
        n_days = int(value or 5)
        recent_change = change_pct.iloc[:n_days]
        return all(c < 0 for c in recent_change)

    # --- high_5d / low_5d / high_20d / low_20d: 当前价创N日新高/新低 ---
    if key in ("high_5d", "low_5d", "high_20d", "low_20d"):
        n_days_map = {"high_5d": 5, "low_5d": 5, "high_20d": 20, "low_20d": 20}
        n_days = n_days_map.get(key, 5)
        recent_high = df["high"].astype(float).iloc[:n_days].max()
        recent_low = df["low"].astype(float).iloc[:n_days].min()
        current_close = close.iloc[0]
        if "high" in key:
            return abs(current_close - recent_high) / recent_high < 0.01  # 接近新高
        return abs(current_close - recent_low) / recent_low < 0.01  # 接近新低

    # --- avg_volume_5d / avg_volume_20d ---
    if key in ("avg_volume_5d", "avg_volume_20d"):
        n_days_map = {"avg_volume_5d": 5, "avg_volume_20d": 20}
        n_days = n_days_map.get(key, 5)
        avg_vol = volume.iloc[:n_days].mean()
        actual_val = float(value or 0)
        if actual_val <= 0:
            return True
        return avg_vol > actual_val

    # --- multi_day_change: 近N日累计涨幅 ---
    if key == "multi_day_change":
        n_days = int(value or 5)
        if len(close) < n_days + 1:
            return False
        old_close = close.iloc[n_days]
        current_close = close.iloc[0]
        pct = (current_close - old_close) / old_close * 100
        actual_val = float(value or 0) if isinstance(value, (int, float)) else 0
        return pct > actual_val

    # --- formula: 自定义公式 ---
    if op == "formula":
        formula = str(value or "")
        if not formula:
            return True
        # 支持的变量：price, volume, change_pct, pre_close, high, low, close
        # 涨停缩量公式示例：volume[i] < volume[limit_up_day] * 0.5
        # 简化版：仅判断最新一根K线的变量关系
        try:
            local_vars = {
                "price": close.iloc[0],
                "volume": volume.iloc[0],
                "change_pct": change_pct.iloc[0],
                "pre_close": pre_close.iloc[0],
                "high": float(df["high"].astype(float).iloc[0]),
                "low": float(df["low"].astype(float).iloc[0]),
                "close": close.iloc[0],
            }
            return bool(eval(formula, {"__builtins__": {}}, local_vars))
        except Exception:
            return False

    return True
