from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List


class ScreenerHistoryStore:
    """文件存储：记录AI策略解析与筛选交互历史，供页面查询展示。"""

    INDEX_FILE_NAME = "screener_history_index.json"

    def __init__(self, root_dir: str = "data/evolution/screener_history"):
        # 初始化根目录与索引路径，保证首次运行即可写入。
        self.root_dir = os.path.abspath(root_dir)
        self.index_dir = os.path.join(self.root_dir, "indexes")
        self.index_path = os.path.join(self.index_dir, self.INDEX_FILE_NAME)
        os.makedirs(self.index_dir, exist_ok=True)

    def _json_default(self, value: Any):
        # 避免复杂对象序列化报错，统一降级为字符串。
        return str(value)

    def _read_json(self, path: str, default: Any):
        # 读取JSON文件，异常时回退默认值，避免接口报错中断。
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return deepcopy(default)

    def _write_json(self, path: str, payload: Any) -> None:
        # 原子性要求较低（索引小文件），直接覆盖写入即可。
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=self._json_default)

    def _load_index(self) -> List[Dict[str, Any]]:
        # 统一返回列表结构，便于分页切片。
        payload = self._read_json(self.index_path, [])
        return payload if isinstance(payload, list) else []

    def _save_index(self, items: List[Dict[str, Any]]) -> None:
        # 按时间倒序保存，保证前端默认看到最新记录。
        rows = [x for x in items if isinstance(x, dict)]
        rows.sort(key=lambda x: str(x.get("created_at", "")), reverse=True)
        self._write_json(self.index_path, rows)

    def append_event(self, event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """追加一条历史记录并返回写入后的条目。"""
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        et = str(event_type or "").strip() or "unknown"
        item = {
            "history_id": f"scr_hist_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{os.urandom(3).hex()}",
            "event_type": et,
            "created_at": now_iso,
            "payload": payload if isinstance(payload, dict) else {},
        }
        rows = self._load_index()
        rows.append(item)
        # 保留最近5000条，防止索引无限膨胀。
        if len(rows) > 5000:
            rows = rows[-5000:]
        self._save_index(rows)
        return item

    def list_events(self, page: int = 1, page_size: int = 20, event_type: str = "") -> Dict[str, Any]:
        """分页读取历史记录，可按事件类型过滤。"""
        rows = self._load_index()
        et = str(event_type or "").strip()
        if et:
            rows = [x for x in rows if str(x.get("event_type", "")).strip() == et]
        safe_page = max(1, int(page or 1))
        safe_size = max(1, min(int(page_size or 20), 200))
        start = (safe_page - 1) * safe_size
        end = start + safe_size
        return {
            "items": rows[start:end],
            "total": len(rows),
            "page": safe_page,
            "page_size": safe_size,
        }

