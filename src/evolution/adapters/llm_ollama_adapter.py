from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from typing import Any, Dict


class OllamaStrategyLLM:
    """Ollama 本地模型适配器，提供与 Researcher 兼容的 generate 接口。"""

    def __init__(
        self,
        model: str,
        base_url: str = "",
        api_key: str = "",
        temperature: float = 0.2,
        max_tokens: int = 1200,
        timeout_seconds: int = 30,
        retry_times: int = 1,
        system_prompt: str = "",
    ):
        # 允许不传 base_url，默认走本地 Ollama 服务端口。
        self.model = str(model or "").strip()
        self.base_url = str(base_url or "").strip()
        # 兼容经反向代理部署的鉴权场景，默认可为空。
        self.api_key = str(api_key or "").strip()
        self.temperature = float(temperature)
        self.max_tokens = max(1, int(max_tokens))
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.retry_times = max(0, int(retry_times))
        self.system_prompt = str(system_prompt or "").strip()
        # 用于看板展示最近一次调用路径与状态。
        self.last_call_meta: Dict[str, Any] = {}

    def generate(self, prompt: str, context: Dict[str, Any]) -> str:
        if not self.model:
            raise RuntimeError("Ollama 模型名为空，请配置 evolution.llm.model")
        endpoint = self._endpoint()
        body = self._build_request_body(prompt=prompt, context=context)
        last_error: Exception | None = None
        for _ in range(self.retry_times + 1):
            try:
                content = self._request_once(endpoint=endpoint, body=body)
                code = self._extract_code(content)
                if not code.strip():
                    raise RuntimeError("Ollama 返回内容为空")
                self.last_call_meta = {
                    "provider": "ollama",
                    "model": self.model,
                    "endpoint": endpoint,
                    "fallback_used": False,
                    "path": "direct",
                }
                return code
            except Exception as exc:
                last_error = exc
        self.last_call_meta = {
            "provider": "ollama",
            "model": self.model,
            "endpoint": endpoint,
            "fallback_used": False,
            "path": "direct",
            "error": str(last_error),
        }
        raise RuntimeError(f"Ollama 调用失败: {last_error}")

    def _endpoint(self) -> str:
        base = str(self.base_url or "").strip().rstrip("/")
        # Ollama 原生接口默认地址，兼容无配置本地开发场景。
        if not base:
            base = "http://127.0.0.1:11434"
        if base.endswith("/api/chat"):
            return base
        return f"{base}/api/chat"

    def _build_request_body(self, prompt: str, context: Dict[str, Any]) -> Dict[str, Any]:
        seed_code = str(context.get("seed_code", "") or "")
        user_prompt = (
            f"{str(prompt or '').strip()}\n\n"
            "请基于以下种子策略做改写，保持策略风格但不要完全重复：\n"
            f"{seed_code}\n\n"
            f"目标上下文：{json.dumps(context, ensure_ascii=False)}\n"
            "只输出Python代码。"
        )
        return {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt or "你是量化策略生成助手，只输出Python代码。"},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            # num_predict 与 max_tokens 语义接近，用于控制输出上限。
            "options": {
                "temperature": float(self.temperature),
                "num_predict": int(self.max_tokens),
            },
        }

    def _request_once(self, endpoint: str, body: Dict[str, Any]) -> str:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        req = urllib.request.Request(endpoint, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore") if exc.fp else str(exc)
            raise RuntimeError(f"HTTP {int(exc.code)}: {detail[:300]}") from exc
        return self._extract_content(data)

    def _extract_content(self, data: Dict[str, Any]) -> str:
        # 优先兼容 Ollama 原生返回：{"message": {"content": "..."}}
        message = data.get("message", {}) if isinstance(data.get("message"), dict) else {}
        content = str(message.get("content", "") or "").strip()
        if content:
            return content
        # 兼容 OpenAI 代理风格返回，提升适配弹性。
        choices = data.get("choices", []) if isinstance(data.get("choices"), list) else []
        if choices and isinstance(choices[0], dict):
            msg = choices[0].get("message", {}) if isinstance(choices[0].get("message"), dict) else {}
            content = str(msg.get("content", "") or "").strip()
            if content:
                return content
        raise RuntimeError("Ollama 响应内容为空")

    def _extract_code(self, text: str) -> str:
        # 兼容 ```python 包裹与纯代码输出。
        content = str(text or "")
        match = re.search(r"```(?:python)?\s*([\s\S]*?)```", content, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return content.strip()
