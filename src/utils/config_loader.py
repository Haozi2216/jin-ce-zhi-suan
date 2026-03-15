# src/utils/config_loader.py
import json
import os

class ConfigLoader:
    _instance = None
    _config = None

    def __new__(cls, config_path="config.json"):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
            cls._instance._load_config(config_path)
        return cls._instance

    def _load_config(self, config_path):
        if not os.path.exists(config_path):
            # Try to find it relative to project root
            root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
            config_path = os.path.join(root_path, "config.json")
            
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    # Remove comments (lines starting with // or content after //)
                    content = f.read()
                    # Use regex to remove comments while preserving URLs in strings
                    import re
                    # Pattern matches strings (double quotes) OR comments (//)
                    # If it's a string, we keep it. If it's a comment, we discard it.
                    pattern = r'(".*?")|//.*'
                    
                    def replace(match):
                        if match.group(1):
                            return match.group(1)  # It's a string, keep it
                        return ""  # It's a comment, remove it
                        
                    clean_content = re.sub(pattern, replace, content)
                    self._config = json.loads(clean_content)
            except Exception as e:
                print(f"⚠️ Error parsing config.json: {e}. Using empty config.")
                self._config = {}
        else:
            print(f"⚠️ Config file not found at {config_path}. Using defaults.")
            self._config = {}

    def get(self, key, default=None):
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default

    @classmethod
    def reload(cls, config_path="config.json"):
        cls._instance = None
        return cls(config_path)
