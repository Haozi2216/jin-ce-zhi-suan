"""通达信公式批量回测编排适配器。

该模块只做“流程编排”，不改动现有回测引擎逻辑：
1) 批量导入通达信公式到策略库；
2) 导入 BLK 到标的池；
3) 同步策略池；
4) 生成批量任务；
5) 启动并可选等待批量回测结束。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass
class TdxFormulaBatchRunConfig:
    """批量编排运行配置。"""

    # 回测服务地址。
    base_url: str = "http://127.0.0.1:8000"
    # 任务文件路径（需位于 data/batch_tasks 下，服务端会校验）。
    tasks_csv: str = "data/batch_tasks/tdx_formula_batch_tasks.csv"
    # 输出文件路径。
    results_csv: str = "data/批量回测结果.csv"
    summary_csv: str = "data/策略汇总评分.csv"
    # 轮询参数。
    poll_seconds: int = 5
    max_wait_seconds: int = 7200


class TdxFormulaBatchAdapter:
    """将通达信公式工具链路拼接为一键批量回测流程。"""

    def __init__(self, cfg: Optional[TdxFormulaBatchRunConfig] = None) -> None:
        # 默认配置可覆盖，便于服务端按请求动态传参。
        self.cfg = cfg or TdxFormulaBatchRunConfig()

    def run_pipeline(
        self,
        formula_items: List[Dict[str, Any]],
        blk_file_path: str = "",
        blk_content: str = "",
        strategy_pool_mode: str = "append",
        blk_import_mode: str = "append",
        generate_mode: str = "append",
        wait_until_done: bool = False,
    ) -> Dict[str, Any]:
        """执行一键编排主流程。"""
        # 先导入公式，拿到可执行策略ID。
        imported_strategy_ids = self.import_tdx_formulas(
            items=formula_items,
            skip_existing=True,
        )
        # 有 BLK 输入时，导入标的池。
        blk_result: Dict[str, Any] = {"status": "skipped", "reason": "no_blk_input"}
        if str(blk_file_path or "").strip() or str(blk_content or "").strip():
            blk_result = self.import_blk_to_stock_pool(
                blk_file_path=blk_file_path,
                blk_content=blk_content,
                import_mode=blk_import_mode,
            )
        # 同步策略池，确保任务生成器能读取到策略ID。
        strategy_pool_result = self.sync_strategy_pool(
            strategy_ids=imported_strategy_ids,
            mode=strategy_pool_mode,
        )
        # 生成批量任务文件。
        generate_result = self.generate_tasks(
            generate_mode=generate_mode,
        )
        # 启动批量回测进程。
        start_result = self.start_batch_run()
        # 组装基础返回。
        result = {
            "status": "success",
            "imported_strategy_ids": imported_strategy_ids,
            "imported_strategy_count": len(imported_strategy_ids),
            "blk_result": blk_result,
            "strategy_pool_result": strategy_pool_result,
            "generate_result": generate_result,
            "start_result": start_result,
            "tasks_csv": self.cfg.tasks_csv,
            "results_csv": self.cfg.results_csv,
            "summary_csv": self.cfg.summary_csv,
        }
        # 如果调用方要求等待，则轮询到任务结束。
        if bool(wait_until_done):
            result["final_status"] = self.wait_until_done()
        return result

    def import_tdx_formulas(self, items: List[Dict[str, Any]], skip_existing: bool = True) -> List[str]:
        """调用现有接口批量导入通达信公式。"""
        payload = {
            "items": list(items or []),
            "skip_existing": bool(skip_existing),
            "stop_on_error": False,
        }
        ok, resp, err = self._safe_request("POST", "/api/tdx/import_pack", body=payload)
        if not ok:
            raise RuntimeError(f"调用 /api/tdx/import_pack 失败: {err}")
        status = str(resp.get("status", "")).strip().lower()
        if status not in {"success", "partial_success"}:
            raise RuntimeError(f"导入通达信公式失败: {resp}")
        imported_rows = resp.get("imported", []) if isinstance(resp.get("imported"), list) else []
        strategy_ids: List[str] = []
        for row in imported_rows:
            if not isinstance(row, dict):
                continue
            sid = str(row.get("strategy_id", "") or "").strip()
            if sid:
                strategy_ids.append(sid)
        # 若全部被 skip，也允许从 skipped 中提取策略ID并继续流程。
        if not strategy_ids:
            skipped_rows = resp.get("skipped", []) if isinstance(resp.get("skipped"), list) else []
            for row in skipped_rows:
                if not isinstance(row, dict):
                    continue
                sid = str(row.get("strategy_id", "") or "").strip()
                if sid:
                    strategy_ids.append(sid)
        # 无策略ID就不能继续后续任务生成。
        uniq_ids = self._uniq_keep_order(strategy_ids)
        if not uniq_ids:
            raise RuntimeError(f"导入后未得到可用策略ID: {resp}")
        return uniq_ids

    def import_blk_to_stock_pool(
        self,
        blk_file_path: str = "",
        blk_content: str = "",
        import_mode: str = "append",
    ) -> Dict[str, Any]:
        """导入 BLK 到标的池。"""
        payload: Dict[str, Any] = {
            "encoding": "auto",
            "normalize_symbol": True,
            "import_mode": str(import_mode or "append").strip().lower(),
            "market_tag": "主板",
            "industry_tag": "BLK导入",
            "size_tag": "未知",
            "enabled": True,
            "stock_pool_csv": "data/任务生成_标的池.csv",
        }
        if str(blk_file_path or "").strip():
            payload["file_path"] = str(blk_file_path).strip()
        if str(blk_content or "").strip():
            payload["content"] = str(blk_content)
        ok, resp, err = self._safe_request("POST", "/api/blk/import_stock_pool", body=payload)
        if not ok:
            raise RuntimeError(f"调用 /api/blk/import_stock_pool 失败: {err}")
        if str(resp.get("status", "")).strip().lower() != "success":
            raise RuntimeError(f"导入 BLK 失败: {resp}")
        return resp

    def sync_strategy_pool(self, strategy_ids: List[str], mode: str = "append") -> Dict[str, Any]:
        """将策略ID同步到任务生成策略池。"""
        payload = {
            "strategy_pool_csv": "data/任务生成_策略池.csv",
            "strategy_ids": self._uniq_keep_order(strategy_ids),
            "use_all_enabled": False,
            "mode": str(mode or "append").strip().lower(),
        }
        ok, resp, err = self._safe_request("POST", "/api/batch/strategy_pool/sync", body=payload)
        if not ok:
            raise RuntimeError(f"调用 /api/batch/strategy_pool/sync 失败: {err}")
        if str(resp.get("status", "")).strip().lower() != "success":
            raise RuntimeError(f"同步策略池失败: {resp}")
        return resp

    def generate_tasks(self, generate_mode: str = "append") -> Dict[str, Any]:
        """调用任务生成接口，构建批量回测任务。"""
        payload = {
            "generate_mode": str(generate_mode or "append").strip().lower(),
            "generate_max_tasks": 0,
            "tasks_csv": self.cfg.tasks_csv,
            "generator_strategies_csv": "data/任务生成_策略池.csv",
            "generator_stocks_csv": "data/任务生成_标的池.csv",
            "generator_windows_csv": "data/任务生成_区间池.csv",
            "generator_scenarios_csv": "data/任务生成_场景池.csv",
        }
        ok, resp, err = self._safe_request("POST", "/api/batch/generate_tasks", body=payload)
        if not ok:
            raise RuntimeError(f"调用 /api/batch/generate_tasks 失败: {err}")
        if str(resp.get("status", "")).strip().lower() != "success":
            raise RuntimeError(f"生成任务失败: {resp}")
        return resp

    def start_batch_run(self) -> Dict[str, Any]:
        """启动批量回测。"""
        payload = {
            "tasks_csv": self.cfg.tasks_csv,
            "results_csv": self.cfg.results_csv,
            "summary_csv": self.cfg.summary_csv,
            "batch_no_filter": "",
            "archive_completed": False,
            "archive_tasks_csv": "data/批量回测任务.archive.csv",
            "max_tasks": 0,
            "parallel_workers": 1,
            "base_url": self.cfg.base_url,
            "base_urls": "",
            "rate_limit_interval_seconds": 0.0,
            "poll_seconds": 3,
            "status_log_seconds": 90,
            "max_wait_seconds": self.cfg.max_wait_seconds,
            "retry_sleep_seconds": 3,
            "ai_analyze": False,
            "ai_analyze_only": False,
            "ai_analysis_output_md": "data/批量回测AI分析.md",
            "ai_analysis_system_prompt": "",
            "ai_analysis_prompt": "",
            "ai_analysis_max_results": 200,
            "ai_analysis_max_strategies": 80,
            "ai_analysis_temperature": -1.0,
            "ai_analysis_max_tokens": 1400,
            "ai_analysis_timeout_sec": 60,
        }
        ok, resp, err = self._safe_request("POST", "/api/batch/run/start", body=payload)
        if not ok:
            raise RuntimeError(f"调用 /api/batch/run/start 失败: {err}")
        if str(resp.get("status", "")).strip().lower() != "success":
            raise RuntimeError(f"启动批量回测失败: {resp}")
        return resp

    def query_batch_status(self) -> Dict[str, Any]:
        """查询批量回测状态。"""
        query = urlencode({"tasks_csv": self.cfg.tasks_csv, "log_limit": 100})
        ok, resp, err = self._safe_request("GET", f"/api/batch/run/status?{query}", body=None)
        if not ok:
            raise RuntimeError(f"调用 /api/batch/run/status 失败: {err}")
        if str(resp.get("status", "")).strip().lower() != "success":
            raise RuntimeError(f"查询批量回测状态失败: {resp}")
        return resp

    def wait_until_done(self) -> Dict[str, Any]:
        """轮询等待批量回测结束。"""
        start_ts = time.time()
        while True:
            status = self.query_batch_status()
            # 以 running=false 作为完成判定，兼容现有状态定义。
            if not bool(status.get("running", False)):
                return status
            # 超时控制，防止无限等待。
            elapsed = time.time() - start_ts
            if elapsed > max(30, int(self.cfg.max_wait_seconds or 7200)):
                raise TimeoutError("等待批量回测完成超时")
            time.sleep(max(1, int(self.cfg.poll_seconds or 5)))

    def _safe_request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Dict[str, Any], str]:
        """安全请求封装，统一错误处理。"""
        try:
            payload = self._request(method=method, path=path, body=body)
            return True, payload, ""
        except HTTPError as e:
            try:
                detail = e.read().decode("utf-8", errors="ignore")
            except Exception:
                detail = str(e)
            return False, {}, f"HTTPError {e.code}: {detail}"
        except URLError as e:
            return False, {}, f"URLError: {e}"
        except Exception as e:
            return False, {}, str(e)

    def _request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """HTTP JSON 请求。"""
        url = f"{self.cfg.base_url.rstrip('/')}{path}"
        payload_bytes = None
        if body is not None:
            payload_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = Request(
            url=url,
            method=str(method or "GET").strip().upper(),
            headers={"Content-Type": "application/json"},
            data=payload_bytes,
        )
        with urlopen(req, timeout=60) as resp:
            text = resp.read().decode("utf-8", errors="ignore")
            if not text:
                return {}
            return json.loads(text)

    def _uniq_keep_order(self, values: List[str]) -> List[str]:
        """按原顺序去重。"""
        out: List[str] = []
        seen = set()
        for raw in values or []:
            item = str(raw or "").strip()
            if not item or item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out
