# src/utils/config_loader.py
import json
import os

class ConfigLoader:
    _instance = None
    _config = {}
    _default_private_override_paths = {
        "data_provider.tushare_token",
        "data_provider.default_api_key",
        "data_provider.llm_api_key",
        "data_provider.strategy_llm_api_key",
        "data_provider.api_key",
        "data_provider.mysql_password",
    }
    _default_private_passthrough_paths = {
        "targets",
        "strategies.active_ids",
    }

    def __new__(cls, config_path="config.json"):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
            cls._instance.load_config(config_path)
        return cls._instance

    def load_config(self, config_path):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(base_dir))
        base_config_path = config_path if os.path.exists(config_path) else os.path.join(project_root, "config.json")
        base_config = self._load_json_config(base_config_path)
        private_config_path = str(
            os.environ.get("CONFIG_PRIVATE_PATH", "")
            or self._get_path_value(base_config, "system.private_config_path", "")
            or os.path.join(project_root, "config.private.json")
        ).strip()
        if private_config_path and (not os.path.isabs(private_config_path)):
            private_config_path = os.path.join(project_root, private_config_path)
        private_config = self._load_json_config(private_config_path, silent=True)
        private_override_paths = self.resolve_private_override_paths(base_config)
        private_config = self._filter_private_override_config(private_config, private_override_paths)
        private_passthrough = self._extract_private_passthrough_config(private_config)
        merged_private = self._deep_merge_dict(private_config, private_passthrough)
        self._config = self._deep_merge_dict(base_config, merged_private)

    def _load_json_config(self, config_path, silent=False):
        import re
        if not os.path.exists(config_path):
            if not silent:
                print(f"Config file not found: {config_path}")
            return {}
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                content = f.read()
                pattern = r'("[^"]*")|(\/\/.*)'
                def replace(match):
                    if match.group(1):
                        return match.group(1)
                    return ""
                content = re.sub(pattern, replace, content)
                return json.loads(content)
        except Exception as e:
            if not silent:
                print(f"Error loading config: {e}")
            return {}

    def _deep_merge_dict(self, base, override):
        if not isinstance(base, dict):
            return override if override is not None else base
        if not isinstance(override, dict):
            return base
        merged = dict(base)
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(merged.get(k), dict):
                merged[k] = self._deep_merge_dict(merged[k], v)
            else:
                merged[k] = v
        return merged

    def _path_exists(self, payload, path):
        if not isinstance(payload, dict):
            return False
        cur = payload
        for key in str(path).split('.'):
            if not isinstance(cur, dict) or key not in cur:
                return False
            cur = cur.get(key)
        return True

    def _get_path_value(self, payload, path, default=None):
        if not isinstance(payload, dict):
            return default
        cur = payload
        for key in str(path).split('.'):
            if not isinstance(cur, dict) or key not in cur:
                return default
            cur = cur.get(key)
        return cur

    def _set_path_value(self, payload, path, value):
        if not isinstance(payload, dict):
            return
        keys = str(path).split('.')
        cur = payload
        for key in keys[:-1]:
            nxt = cur.get(key)
            if not isinstance(nxt, dict):
                nxt = {}
                cur[key] = nxt
            cur = nxt
        cur[keys[-1]] = value

    @classmethod
    def resolve_private_override_paths(cls, payload=None):
        env_value = str(os.environ.get("CONFIG_PRIVATE_OVERRIDE_PATHS", "") or "").strip()
        if env_value:
            return {p.strip() for p in env_value.split(",") if p and p.strip()}
        cfg_value = []
        if isinstance(payload, dict):
            cur = payload
            ok = True
            for key in "system.private_override_paths".split('.'):
                if not isinstance(cur, dict) or key not in cur:
                    ok = False
                    break
                cur = cur.get(key)
            if ok:
                cfg_value = cur
        if isinstance(cfg_value, list):
            normalized = {str(p or "").strip() for p in cfg_value}
            normalized = {p for p in normalized if p}
            if normalized:
                return normalized
        if isinstance(cfg_value, str) and cfg_value.strip():
            return {p.strip() for p in cfg_value.split(",") if p and p.strip()}
        return set(cls._default_private_override_paths)

    def _filter_private_override_config(self, payload, paths):
        if not isinstance(payload, dict):
            return {}
        filtered = {}
        for path in paths:
            if self._path_exists(payload, path):
                self._set_path_value(filtered, path, self._get_path_value(payload, path, ""))
        return filtered

    def _extract_private_passthrough_config(self, payload):
        if not isinstance(payload, dict):
            return {}
        out = {}
        for path in self._default_private_passthrough_paths:
            if self._path_exists(payload, path):
                self._set_path_value(out, path, self._get_path_value(payload, path, None))
        return out

    def get(self, key, default=None):
        keys = key.split('.')
        value = self._config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    @classmethod
    def reload(cls, config_path="config.json"):
        """Force reload the config from disk"""
        cls._instance = None
        return cls(config_path)

    def set(self, key, value):
        """
        Set a config value by dot notation key (e.g. "data_provider.source")
        """
        keys = key.split('.')
        current = self._config
        for k in keys[:-1]:
            current = current.setdefault(k, {})
        current[keys[-1]] = value

    def to_dict(self):
        return json.loads(json.dumps(self._config, ensure_ascii=False))

    def save(self, config_path="config.json"):
        if not os.path.exists(config_path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(base_dir))
            config_path = os.path.join(project_root, "config.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(self._config, f, ensure_ascii=False, indent=2)
