from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.utils.config_loader import ConfigLoader


@dataclass
class UnifiedLLMConfig:
    """统一的大模型配置，优先读取 evolution.llm，其次兼容 data_provider 历史配置。"""

    provider: str = "openai_compatible"
    api_key: str = ""
    base_url: str = ""
    model: str = ""
    timeout_seconds: int = 120
    retry_times: int = 1
    default_temperature: float = 0.2
    default_max_tokens: int = 1200
    provider_source: str = ""
    api_key_source: str = ""
    base_url_source: str = ""
    model_source: str = ""

    @classmethod
    def from_config(cls, cfg: ConfigLoader) -> "UnifiedLLMConfig":
        return cls.from_config_scope(cfg=cfg, scope="unified")

    @classmethod
    def from_config_scope(cls, cfg: ConfigLoader, scope: str = "unified") -> "UnifiedLLMConfig":
        def _pick_path_value(pairs: List[tuple[str, Any]], default: str = "") -> tuple[str, str]:
            # 统一按优先级选取第一个有效配置，同时记录命中来源路径用于可观测性。
            for source, raw in pairs:
                text = str(raw or "").strip()
                if text:
                    return text, source
            return str(default or "").strip(), ""

        scope_norm = str(scope or "unified").strip().lower()
        # 按配置域选取模型配置，支持“每个配置块独立测试”。
        if scope_norm == "evolution":
            provider, provider_source = _pick_path_value([
                ("env:EVOLUTION_LLM_PROVIDER", os.environ.get("EVOLUTION_LLM_PROVIDER", "")),
                ("evolution.llm.provider", cfg.get("evolution.llm.provider", "")),
            ], default="openai_compatible")
            api_key, api_key_source = _pick_path_value([
                ("env:EVOLUTION_LLM_API_KEY", os.environ.get("EVOLUTION_LLM_API_KEY", "")),
                ("evolution.llm.api_key", cfg.get("evolution.llm.api_key", "")),
            ])
            base_url, base_url_source = _pick_path_value([
                ("env:EVOLUTION_LLM_BASE_URL", os.environ.get("EVOLUTION_LLM_BASE_URL", "")),
                ("evolution.llm.base_url", cfg.get("evolution.llm.base_url", "")),
            ])
            model, model_source = _pick_path_value([
                ("env:EVOLUTION_LLM_MODEL", os.environ.get("EVOLUTION_LLM_MODEL", "")),
                ("evolution.llm.model", cfg.get("evolution.llm.model", "")),
            ])
        elif scope_norm == "strategy_manager":
            provider, provider_source = _pick_path_value([
                ("data_provider.strategy_llm_provider", cfg.get("data_provider.strategy_llm_provider", "")),
            ], default="openai_compatible")
            api_key, api_key_source = _pick_path_value([
                ("data_provider.strategy_llm_api_key", cfg.get("data_provider.strategy_llm_api_key", "")),
            ])
            base_url, base_url_source = _pick_path_value([
                ("data_provider.strategy_llm_api_url", cfg.get("data_provider.strategy_llm_api_url", "")),
            ])
            model, model_source = _pick_path_value([
                ("data_provider.strategy_llm_model", cfg.get("data_provider.strategy_llm_model", "")),
            ])
        elif scope_norm == "data_provider":
            provider, provider_source = _pick_path_value([
                ("data_provider.llm_provider", cfg.get("data_provider.llm_provider", "")),
            ], default="openai_compatible")
            api_key, api_key_source = _pick_path_value([
                ("data_provider.llm_api_key", cfg.get("data_provider.llm_api_key", "")),
                ("data_provider.api_key", cfg.get("data_provider.api_key", "")),
                ("data_provider.default_api_key", cfg.get("data_provider.default_api_key", "")),
            ])
            base_url, base_url_source = _pick_path_value([
                ("data_provider.llm_api_url", cfg.get("data_provider.llm_api_url", "")),
                ("data_provider.default_api_url", cfg.get("data_provider.default_api_url", "")),
            ])
            model, model_source = _pick_path_value([
                ("data_provider.llm_model", cfg.get("data_provider.llm_model", "")),
            ])
        else:
            # unified: 优先 evolution.llm，再回退 data_provider 历史配置。
            provider, provider_source = _pick_path_value([
                ("env:EVOLUTION_LLM_PROVIDER", os.environ.get("EVOLUTION_LLM_PROVIDER", "")),
                ("evolution.llm.provider", cfg.get("evolution.llm.provider", "")),
            ], default="openai_compatible")
            api_key, api_key_source = _pick_path_value([
                ("env:EVOLUTION_LLM_API_KEY", os.environ.get("EVOLUTION_LLM_API_KEY", "")),
                ("evolution.llm.api_key", cfg.get("evolution.llm.api_key", "")),
                ("data_provider.strategy_llm_api_key", cfg.get("data_provider.strategy_llm_api_key", "")),
                ("data_provider.llm_api_key", cfg.get("data_provider.llm_api_key", "")),
                ("data_provider.api_key", cfg.get("data_provider.api_key", "")),
                ("data_provider.default_api_key", cfg.get("data_provider.default_api_key", "")),
            ])
            base_url, base_url_source = _pick_path_value([
                ("env:EVOLUTION_LLM_BASE_URL", os.environ.get("EVOLUTION_LLM_BASE_URL", "")),
                ("evolution.llm.base_url", cfg.get("evolution.llm.base_url", "")),
                ("data_provider.strategy_llm_api_url", cfg.get("data_provider.strategy_llm_api_url", "")),
                ("data_provider.llm_api_url", cfg.get("data_provider.llm_api_url", "")),
                ("data_provider.default_api_url", cfg.get("data_provider.default_api_url", "")),
            ])
            model, model_source = _pick_path_value([
                ("env:EVOLUTION_LLM_MODEL", os.environ.get("EVOLUTION_LLM_MODEL", "")),
                ("evolution.llm.model", cfg.get("evolution.llm.model", "")),
                ("data_provider.strategy_llm_model", cfg.get("data_provider.strategy_llm_model", "")),
                ("data_provider.llm_model", cfg.get("data_provider.llm_model", "")),
            ])
        timeout_seconds = int(
            cfg.get("evolution.llm.timeout_seconds", 0)
            or cfg.get("data_provider.strategy_llm_timeout_sec", 0)
            or cfg.get("data_provider.llm_timeout_sec", 0)
            or 120
        )
        retry_times = int(
            cfg.get("evolution.llm.retry_times", 0)
            or cfg.get("data_provider.strategy_llm_retry_times", 0)
            or 1
        )
        default_temperature = float(
            cfg.get("evolution.llm.temperature", 0.2)
            or cfg.get("data_provider.strategy_llm_temperature", 0.2)
            or 0.2
        )
        default_max_tokens = int(
            cfg.get("evolution.llm.max_tokens", 1200)
            or cfg.get("data_provider.strategy_llm_max_tokens", 1200)
            or 1200
        )
        # 对 provider 做容错，避免非法值导致请求分支异常。
        provider_norm = str(provider or "openai_compatible").strip().lower()
        if provider_norm in {"zhipuai", "glm"}:
            provider_norm = "zhipu"
        if provider_norm not in {"openai_compatible", "zhipu"}:
            provider_norm = "openai_compatible"
        return cls(
            provider=provider_norm,
            api_key=api_key,
            base_url=base_url,
            model=model,
            timeout_seconds=max(5, min(int(timeout_seconds or 120), 300)),
            retry_times=max(0, int(retry_times or 0)),
            default_temperature=float(default_temperature),
            default_max_tokens=max(64, int(default_max_tokens or 1200)),
            provider_source=provider_source,
            api_key_source=api_key_source,
            base_url_source=base_url_source,
            model_source=model_source,
        )

    def is_ready(self) -> bool:
        # zhipu 使用官方 SDK，不依赖 base_url；openai_compatible 需要 base_url。
        if not self.api_key or not self.model:
            return False
        if self.provider == "zhipu":
            return True
        return bool(self.base_url)


class UnifiedLLMClient:
    """统一的聊天调用适配器，支持 openai_compatible 与 zhipu。"""

    def __init__(self, cfg: UnifiedLLMConfig):
        self.cfg = cfg
        # 记录最后一次调用元数据，便于排障与看板展示。
        self.last_call_meta: Dict[str, Any] = {}

    def complete(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> Dict[str, Any]:
        # 统一重试逻辑，屏蔽不同 provider 的异常细节差异。
        last_error: Exception | None = None
        for _ in range(self.cfg.retry_times + 1):
            try:
                content = self._complete_once(messages, temperature=temperature, max_tokens=max_tokens)
                self.last_call_meta = {
                    "provider": self.cfg.provider,
                    "model": self.cfg.model,
                    "fallback_used": False,
                    "path": "direct",
                }
                return {
                    "content": content,
                    "provider": self.cfg.provider,
                    "model": self.cfg.model,
                }
            except Exception as exc:
                last_error = exc
        self.last_call_meta = {
            "provider": self.cfg.provider,
            "model": self.cfg.model,
            "fallback_used": False,
            "path": "direct",
            "error": str(last_error),
        }
        raise RuntimeError(f"统一LLM调用失败: {last_error}")

    def _complete_once(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        if self.cfg.provider == "zhipu":
            return self._complete_zhipu(messages, temperature=temperature, max_tokens=max_tokens)
        return self._complete_openai_compatible(messages, temperature=temperature, max_tokens=max_tokens)

    def _complete_openai_compatible(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        # 使用 urllib 保持与项目当前依赖一致，避免额外引入 SDK。
        url = self.cfg.base_url.rstrip("/")
        if not url.endswith("/chat/completions"):
            if url.endswith("/v1"):
                url = f"{url}/chat/completions"
            else:
                url = f"{url}/v1/chat/completions"
        payload = {
            "model": self.cfg.model,
            "temperature": float(self.cfg.default_temperature if temperature is None else temperature),
            "max_tokens": int(self.cfg.default_max_tokens if max_tokens is None else max_tokens),
            "messages": messages,
        }
        req = urllib.request.Request(
            url=url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Authorization": f"Bearer {self.cfg.api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.cfg.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore") if exc.fp else str(exc)
            raise RuntimeError(f"HTTP {int(exc.code)}: {detail[:300]}") from exc
        data = json.loads(raw)
        content = str((((data.get("choices") or [{}])[0].get("message") or {}).get("content", "") or "")).strip()
        if not content:
            raise RuntimeError("openai_compatible 响应内容为空")
        return content

    def _complete_zhipu(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        # 只走代码生成与文本生成，不启用 thinking，避免高延迟。
        try:
            from zhipuai import ZhipuAI
        except Exception as exc:
            raise RuntimeError(f"未安装 zhipuai 依赖: {exc}") from exc
        client = ZhipuAI(api_key=self.cfg.api_key)
        response = client.chat.completions.create(
            model=self.cfg.model,
            messages=messages,
            temperature=float(self.cfg.default_temperature if temperature is None else temperature),
            max_tokens=int(self.cfg.default_max_tokens if max_tokens is None else max_tokens),
            timeout=int(self.cfg.timeout_seconds),
        )
        choices = getattr(response, "choices", None) or []
        if not choices:
            raise RuntimeError("zhipu 响应缺少 choices")
        message = getattr(choices[0], "message", None)
        content = str(getattr(message, "content", "") or "").strip()
        if not content:
            raise RuntimeError("zhipu 响应内容为空")
        return content


def build_unified_llm_client(cfg: Optional[ConfigLoader] = None, scope: str = "unified") -> UnifiedLLMClient:
    # 统一构建入口，支持按配置域进行探活与调用。
    loader = cfg or ConfigLoader.reload()
    return UnifiedLLMClient(UnifiedLLMConfig.from_config_scope(loader, scope=scope))
