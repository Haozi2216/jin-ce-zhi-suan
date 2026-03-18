import json
import os
import re
from datetime import datetime
from src.strategies.implemented_strategies import (
    Strategy00, Strategy01, Strategy02, Strategy03, Strategy04, Strategy05,
    Strategy06, Strategy07, Strategy08, Strategy09, BaseImplementedStrategy
)
from src.utils.indicators import Indicators
import pandas as pd
import numpy as np
from src.utils.runtime_params import get_value
from src.strategy_intent.intent_engine import StrategyIntentEngine


def _project_root():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _data_dir():
    return os.path.join(_project_root(), "data", "strategies")


def custom_store_path():
    return os.path.join(_data_dir(), "custom_strategies.json")


def state_store_path():
    return os.path.join(_data_dir(), "strategy_state.json")


def ensure_strategy_store():
    os.makedirs(_data_dir(), exist_ok=True)
    if not os.path.exists(custom_store_path()):
        with open(custom_store_path(), "w", encoding="utf-8") as f:
            json.dump({"strategies": []}, f, ensure_ascii=False, indent=2)
    if not os.path.exists(state_store_path()):
        with open(state_store_path(), "w", encoding="utf-8") as f:
            json.dump({"disabled_ids": [], "deleted_ids": []}, f, ensure_ascii=False, indent=2)


def _load_state_payload():
    ensure_strategy_store()
    try:
        with open(state_store_path(), "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    if not isinstance(payload.get("disabled_ids"), list):
        payload["disabled_ids"] = []
    if not isinstance(payload.get("deleted_ids"), list):
        payload["deleted_ids"] = []
    return payload


def _save_state_payload(payload):
    ensure_strategy_store()
    data = payload if isinstance(payload, dict) else {}
    if not isinstance(data.get("disabled_ids"), list):
        data["disabled_ids"] = []
    if not isinstance(data.get("deleted_ids"), list):
        data["deleted_ids"] = []
    with open(state_store_path(), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def list_builtin_strategy_meta():
    items = [
        Strategy00(), Strategy01(), Strategy02(), Strategy03(), Strategy04(),
        Strategy05(), Strategy06(), Strategy07(), Strategy08(), Strategy09()
    ]
    return [{"id": str(s.id), "name": str(s.name), "builtin": True} for s in items]


def load_custom_strategies():
    ensure_strategy_store()
    try:
        with open(custom_store_path(), "r", encoding="utf-8") as f:
            payload = json.load(f)
        rows = payload.get("strategies", [])
        return [r for r in rows if isinstance(r, dict)]
    except Exception:
        return []


def save_custom_strategies(rows):
    ensure_strategy_store()
    safe_rows = [r for r in rows if isinstance(r, dict)]
    with open(custom_store_path(), "w", encoding="utf-8") as f:
        json.dump({"strategies": safe_rows}, f, ensure_ascii=False, indent=2)


def load_disabled_ids():
    try:
        payload = _load_state_payload()
        rows = payload.get("disabled_ids", [])
        return set(str(x) for x in rows if str(x).strip())
    except Exception:
        return set()


def save_disabled_ids(ids):
    unique_ids = sorted(set(str(x) for x in ids if str(x).strip()))
    payload = _load_state_payload()
    payload["disabled_ids"] = unique_ids
    _save_state_payload(payload)


def load_deleted_ids():
    try:
        payload = _load_state_payload()
        rows = payload.get("deleted_ids", [])
        return set(str(x) for x in rows if str(x).strip())
    except Exception:
        return set()


def save_deleted_ids(ids):
    unique_ids = sorted(set(str(x) for x in ids if str(x).strip()))
    payload = _load_state_payload()
    payload["deleted_ids"] = unique_ids
    _save_state_payload(payload)


def list_all_strategy_meta():
    builtin = list_builtin_strategy_meta()
    custom = load_custom_strategies()
    disabled = load_disabled_ids()
    deleted = load_deleted_ids()
    out = []
    for b in builtin:
        sid = str(b["id"])
        if sid in deleted:
            continue
        out.append({
            "id": sid,
            "name": str(b["name"]),
            "builtin": True,
            "enabled": sid not in disabled,
            "deletable": True,
            "editable": False,
            "source": "builtin",
            "source_label": "内置策略",
            "analysis_text": "",
            "code": "",
            "raw_requirement_title": "原始需求",
            "raw_requirement": ""
        })
    for c in custom:
        sid = str(c.get("id", "")).strip()
        if not sid:
            continue
        if sid in deleted:
            continue
        source = str(c.get("source", "")).strip().lower()
        if source not in {"human", "market"}:
            source = str(((c.get("strategy_intent") or {}).get("source", ""))).strip().lower()
        if source not in {"human", "market"}:
            source = "human"
        source_label = "用户输入" if source == "human" else "行情驱动"
        raw_requirement_title = str(c.get("raw_requirement_title", "")).strip()
        if not raw_requirement_title:
            raw_requirement_title = "策略模板" if source == "human" else "行情状态"
        raw_requirement = str(c.get("raw_requirement", "")).strip()
        if not raw_requirement:
            raw_requirement = str(c.get("template_text", "")).strip()
        out.append({
            "id": sid,
            "name": str(c.get("name", sid)),
            "builtin": False,
            "enabled": sid not in disabled,
            "deletable": True,
            "editable": True,
            "source": source,
            "source_label": source_label,
            "analysis_text": str(c.get("analysis_text", "")),
            "code": str(c.get("code", "")),
            "raw_requirement_title": raw_requirement_title,
            "raw_requirement": raw_requirement
        })
    out.sort(key=lambda x: x["id"])
    return out


def next_custom_strategy_id():
    used_numeric = set()
    deleted = load_deleted_ids()
    for b in list_builtin_strategy_meta():
        sid = str(b["id"]).strip()
        if sid in deleted:
            continue
        if sid.isdigit():
            used_numeric.add(int(sid))
    for c in load_custom_strategies():
        sid = str(c.get("id", "")).strip()
        if sid in deleted:
            continue
        if sid.isdigit():
            used_numeric.add(int(sid))
    i = 0
    while True:
        if i not in used_numeric:
            sid = f"{i:02d}" if i < 100 else str(i)
            return sid
        i += 1


def _sanitize_class_name(raw):
    txt = re.sub(r"[^0-9a-zA-Z_]", "", str(raw or ""))
    if not txt:
        txt = "GeneratedStrategy"
    if txt[0].isdigit():
        txt = f"S{txt}"
    return txt


def normalize_strategy_intent(payload):
    engine = StrategyIntentEngine()
    intent = engine.normalize(payload)
    return intent.to_dict(), intent.explain()


def build_fallback_strategy_code(strategy_id, strategy_name, template_text):
    cls = _sanitize_class_name(f"GeneratedStrategy{strategy_id}")
    title = str(strategy_name or f"AI策略{strategy_id}")
    return f"""from src.strategies.implemented_strategies import BaseImplementedStrategy
import pandas as pd
from src.utils.indicators import Indicators

class {cls}(BaseImplementedStrategy):
    def __init__(self):
        super().__init__(\"{strategy_id}\", \"{title}\", trigger_timeframe=\"1min\")
        self.history = {{}}

    def on_bar(self, kline):
        code = kline['code']
        if code not in self.history:
            self.history[code] = pd.DataFrame()
        self.history[code] = pd.concat([self.history[code], pd.DataFrame([kline])], ignore_index=True).tail(2000)
        df = self.history[code]
        if len(df) < 80:
            return None
        close = df['close']
        ma_fast = Indicators.MA(close, 12)
        ma_slow = Indicators.MA(close, 36)
        if len(ma_fast) < 2 or len(ma_slow) < 2:
            return None
        qty = int(self.positions.get(code, 0))
        c = float(kline['close'])
        stop_loss_pct = float(self._cfg(\"stop_loss_pct\", 0.03))
        if qty <= 0 and float(ma_fast.iloc[-2]) <= float(ma_slow.iloc[-2]) and float(ma_fast.iloc[-1]) > float(ma_slow.iloc[-1]):
            buy_qty = int(self._qty())
            if buy_qty <= 0:
                return None
            return {{
                'strategy_id': self.id,
                'code': code,
                'dt': kline['dt'],
                'direction': 'BUY',
                'price': c,
                'qty': buy_qty,
                'stop_loss': c * (1 - stop_loss_pct),
                'take_profit': None
            }}
        if qty > 0 and float(ma_fast.iloc[-2]) >= float(ma_slow.iloc[-2]) and float(ma_fast.iloc[-1]) < float(ma_slow.iloc[-1]):
            return self.create_exit_signal(kline, qty, \"MA Cross Exit\")
        return None
"""


def add_custom_strategy(entry):
    rows = load_custom_strategies()
    sid = str(entry.get("id", "")).strip()
    if not sid:
        raise ValueError("strategy id is required")
    if any(str(r.get("id", "")).strip() == sid for r in rows):
        raise ValueError(f"strategy id already exists: {sid}")
    intent_payload = entry.get("strategy_intent")
    if not isinstance(intent_payload, dict):
        raise ValueError("strategy_intent is required")
    strategy_intent, intent_explain = normalize_strategy_intent(intent_payload)
    source = str(entry.get("source", "")).strip().lower()
    if source not in {"human", "market"}:
        source = str(strategy_intent.get("source", "")).strip().lower()
    if source not in {"human", "market"}:
        source = "human"
    raw_requirement_title = str(entry.get("raw_requirement_title", "")).strip()
    if not raw_requirement_title:
        raw_requirement_title = "策略模板" if source == "human" else "行情状态"
    raw_requirement = str(entry.get("raw_requirement", "")).strip()
    if not raw_requirement:
        raw_requirement = str(entry.get("template_text", "")).strip()
    now = datetime.now().isoformat(timespec="seconds")
    row = {
        "id": sid,
        "name": str(entry.get("name", sid)),
        "class_name": str(entry.get("class_name", "")),
        "code": str(entry.get("code", "")),
        "template_text": str(entry.get("template_text", "")),
        "analysis_text": str(entry.get("analysis_text", "")),
        "source": source,
        "raw_requirement_title": raw_requirement_title,
        "raw_requirement": raw_requirement,
        "strategy_intent": strategy_intent,
        "intent_explain": intent_explain,
        "created_at": now,
        "updated_at": now
    }
    rows.append(row)
    save_custom_strategies(rows)


def delete_custom_strategy(strategy_id):
    sid = str(strategy_id or "").strip()
    if not sid:
        return False
    rows = load_custom_strategies()
    new_rows = [r for r in rows if str(r.get("id", "")).strip() != sid]
    changed = len(new_rows) != len(rows)
    if changed:
        save_custom_strategies(new_rows)
    disabled = load_disabled_ids()
    if sid in disabled:
        disabled.remove(sid)
        save_disabled_ids(disabled)
    return changed


def delete_strategy(strategy_id):
    sid = str(strategy_id or "").strip()
    if not sid:
        return False
    builtin_ids = {str(x.get("id", "")).strip() for x in list_builtin_strategy_meta()}
    if sid in builtin_ids:
        deleted = load_deleted_ids()
        if sid in deleted:
            return False
        deleted.add(sid)
        save_deleted_ids(deleted)
        disabled = load_disabled_ids()
        if sid in disabled:
            disabled.remove(sid)
            save_disabled_ids(disabled)
        return True
    changed = delete_custom_strategy(sid)
    if changed:
        deleted = load_deleted_ids()
        if sid in deleted:
            deleted.remove(sid)
            save_deleted_ids(deleted)
    return changed


def set_strategy_enabled(strategy_id, enabled):
    sid = str(strategy_id or "").strip()
    if not sid:
        raise ValueError("strategy id is required")
    all_ids = {x["id"] for x in list_all_strategy_meta()}
    if sid not in all_ids:
        raise ValueError(f"strategy not found: {sid}")
    disabled = load_disabled_ids()
    if enabled:
        disabled.discard(sid)
    else:
        disabled.add(sid)
    save_disabled_ids(disabled)


def update_custom_strategy(entry):
    sid = str(entry.get("id", "")).strip()
    if not sid:
        raise ValueError("strategy id is required")
    rows = load_custom_strategies()
    idx = -1
    for i, r in enumerate(rows):
        if str(r.get("id", "")).strip() == sid:
            idx = i
            break
    if idx < 0:
        raise ValueError(f"strategy not found: {sid}")
    row = rows[idx]
    if "name" in entry:
        row["name"] = str(entry.get("name", sid)).strip() or sid
    if "class_name" in entry:
        row["class_name"] = str(entry.get("class_name", "")).strip()
    if "code" in entry:
        row["code"] = str(entry.get("code", ""))
    if "analysis_text" in entry:
        row["analysis_text"] = str(entry.get("analysis_text", ""))
    if "raw_requirement" in entry:
        row["raw_requirement"] = str(entry.get("raw_requirement", ""))
    if "raw_requirement_title" in entry:
        row["raw_requirement_title"] = str(entry.get("raw_requirement_title", "")).strip() or "原始需求"
    if "source" in entry:
        source = str(entry.get("source", "")).strip().lower()
        if source in {"human", "market"}:
            row["source"] = source
    if "strategy_intent" in entry and isinstance(entry.get("strategy_intent"), dict):
        strategy_intent, intent_explain = normalize_strategy_intent(entry.get("strategy_intent"))
        row["strategy_intent"] = strategy_intent
        row["intent_explain"] = intent_explain
    row["updated_at"] = datetime.now().isoformat(timespec="seconds")
    rows[idx] = row
    save_custom_strategies(rows)
    return True


def instantiate_custom_strategy(entry):
    code = str(entry.get("code", "") or "")
    if not code.strip():
        return None
    class_name = str(entry.get("class_name", "")).strip()
    ns = {
        "BaseImplementedStrategy": BaseImplementedStrategy,
        "Indicators": Indicators,
        "pd": pd,
        "np": np,
        "get_value": get_value
    }
    exec(code, ns, ns)
    if not class_name:
        class_candidates = [
            k for k, v in ns.items()
            if isinstance(v, type) and issubclass(v, BaseImplementedStrategy) and v is not BaseImplementedStrategy
        ]
        if not class_candidates:
            return None
        class_name = class_candidates[0]
    cls = ns.get(class_name)
    if not isinstance(cls, type):
        return None
    inst = cls()
    sid = str(entry.get("id", "")).strip()
    sname = str(entry.get("name", "")).strip()
    if sid:
        inst.id = sid
    if sname:
        inst.name = sname
    return inst
