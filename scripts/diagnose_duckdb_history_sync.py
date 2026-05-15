import argparse
import os
import sys
from datetime import datetime


# 脚本从项目根目录运行时，补齐 src 导入路径，便于直接复用现有 provider 实现。
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.config_loader import ConfigLoader
from src.utils.duckdb_provider import DuckDbProvider


def _parse_args():
    # 默认参数对齐当前增量同步常见故障场景，优先排查 dat_1mins 的判重查询。
    parser = argparse.ArgumentParser(description="诊断增量同步使用的 DuckDB 判重查询")
    parser.add_argument("--table", default="dat_1mins", help="待检查的 DuckDB 表名")
    parser.add_argument("--start", default="2026-03-02 09:30:00", help="开始时间，格式 YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", default="2026-03-02 15:00:00", help="结束时间，格式 YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--codes", default="000001.SZ,000002.SZ,000004.SZ,000006.SZ", help="逗号分隔的股票代码列表")
    parser.add_argument("--batch-size", type=int, default=20, help="模拟 history_sync 批量判重的 codes 数量")
    return parser.parse_args()


def _normalize_codes(raw_codes):
    # 保持和主流程一致，去掉空值并保留顺序，方便直接复制前台股票列表做复现。
    out = []
    seen = set()
    for item in str(raw_codes or "").split(","):
        code = str(item or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _build_variant_codes(provider, codes):
    # 复用 provider 的 code 变体口径，避免脚本与主流程查到的 code 集合不一致。
    query_codes = []
    reverse_map = {}
    seen = set()
    for code in codes:
        for item in provider._code_variants(code):
            variant = str(item or "").strip().upper()
            if not variant:
                continue
            reverse_map[variant] = code
            if variant in seen:
                continue
            seen.add(variant)
            query_codes.append(variant)
    return query_codes, reverse_map


def _table_exists(conn, provider, table):
    # 通过 information_schema 检查表存在性，避免直接查表时把“表不存在”和“表损坏”混为一谈。
    sql = (
        "SELECT COUNT(*) AS cnt "
        "FROM information_schema.tables "
        "WHERE table_name = ?"
    )
    row = conn.execute(sql, [table]).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _run_basic_probe(conn, provider, table):
    # 先做最轻量的 count/sample 探针，判断是不是连最基本的元数据读取都会失败。
    count_sql = f"SELECT COUNT(*) AS cnt FROM {provider._quoted_table(table)}"
    sample_sql = (
        f"SELECT code, trade_time "
        f"FROM {provider._quoted_table(table)} "
        f"ORDER BY CAST(trade_time AS VARCHAR) DESC "
        f"LIMIT 5"
    )
    total_rows = conn.execute(count_sql).fetchone()[0]
    sample_rows = conn.execute(sample_sql).fetchall()
    return total_rows, sample_rows


def _run_history_sync_like_batch_probe(conn, provider, table, start_text, end_text, codes):
    # 使用和增量同步接近的 SQL 口径复现判重查询，便于确认是不是该批量查询路径触发 internal error。
    query_codes, reverse_map = _build_variant_codes(provider, codes)
    if not query_codes:
        return []
    parse_expr = provider._trade_time_parse_expr()
    placeholders = ", ".join(["?"] * len(query_codes))
    sql = (
        f"SELECT code, trade_time "
        f"FROM {provider._quoted_table(table)} "
        f"WHERE code IN ({placeholders}) "
        f"AND {parse_expr} >= CAST(? AS DATE) "
        f"AND {parse_expr} <= CAST(? AS DATE)"
    )
    params = list(query_codes) + [
        provider._query_date_text(start_text),
        provider._query_date_text(end_text),
    ]
    rows = conn.execute(sql, params).fetchall()
    # 只返回前几行映射结果，避免输出过长影响终端阅读。
    preview = []
    for row in rows[:10]:
        raw_code = str((row[0] if len(row) > 0 else "") or "").strip().upper()
        trade_time = row[1] if len(row) > 1 else None
        preview.append({
            "query_code": raw_code,
            "owner_code": reverse_map.get(raw_code, raw_code),
            "trade_time": str(trade_time),
        })
    return preview


def main():
    # 主流程只做只读诊断，不修改 DuckDB 文件，方便在线下安全排查。
    args = _parse_args()
    codes = _normalize_codes(args.codes)
    if not codes:
        raise SystemExit("请至少传入一个 --codes")
    batch_size = max(1, int(args.batch_size or 1))
    selected_codes = codes[:batch_size]
    cfg = ConfigLoader.reload()
    provider = DuckDbProvider(db_path=cfg.get("data_provider.duckdb_path", ""))
    db_path = provider._resolve_db_path()
    print(f"[INFO] DuckDB path: {db_path}")
    print(f"[INFO] Table: {args.table}")
    print(f"[INFO] Time Range: {args.start} -> {args.end}")
    print(f"[INFO] Codes({len(selected_codes)}): {selected_codes}")
    conn = provider._connect(read_only=True)
    if conn is None:
        raise SystemExit(f"[ERROR] {provider.last_error or 'DuckDB 连接失败'}")
    try:
        if not _table_exists(conn, provider, args.table):
            raise SystemExit(f"[ERROR] 表不存在: {args.table}")
        print("[INFO] 表存在，开始基础探针...")
        total_rows, sample_rows = _run_basic_probe(conn, provider, args.table)
        print(f"[INFO] 基础 COUNT 成功，总行数: {total_rows}")
        print(f"[INFO] 最新 5 行预览: {sample_rows}")
        print("[INFO] 开始模拟 history_sync 批量判重查询...")
        preview = _run_history_sync_like_batch_probe(
            conn=conn,
            provider=provider,
            table=args.table,
            start_text=args.start,
            end_text=args.end,
            codes=selected_codes,
        )
        print(f"[INFO] 批量判重查询成功，前 10 行预览: {preview}")
    except Exception as e:
        print(f"[ERROR] 诊断失败: {e}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
