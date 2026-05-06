from __future__ import annotations

import re
from typing import Any, Dict


class ZhipuStrategyLLM:
    """智谱 LLM 适配器，提供与 Researcher 兼容的 generate 接口。"""

    def __init__(
        self,
        api_key: str,
        model: str,
        temperature: float = 0.2,
        max_tokens: int = 1200,
        timeout_seconds: int = 30,
        retry_times: int = 1,
        system_prompt: str = "",
        thinking_enabled: bool = False,
    ):
        # 保存基础配置，保持与现有 LLM 配置字段一致，减少侵入。
        self.api_key = str(api_key or "").strip()
        self.model = str(model or "").strip()
        self.temperature = float(temperature)
        self.max_tokens = max(1, int(max_tokens))
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.retry_times = max(0, int(retry_times))
        self.system_prompt = str(system_prompt or "").strip()
        # 当前策略进化链路固定关闭 thinking，优先保证代码生成吞吐与稳定性。
        self.thinking_enabled = False
        # 用于对外上报最近一次调用元信息，供事件看板使用。
        self.last_call_meta: Dict[str, Any] = {}

    def generate(self, prompt: str, context: Dict[str, Any]) -> str:
        # 在运行时导入依赖，避免未安装 zhipuai 时影响其它 provider。
        try:
            from zhipuai import ZhipuAI
        except Exception as exc:
            self.last_call_meta = {
                "provider": "zhipu",
                "model": self.model,
                "fallback_used": False,
                "path": "direct",
                "error": f"missing_dependency:{exc}",
            }
            raise RuntimeError(f"未安装 zhipuai 依赖: {exc}") from exc

        if not self.api_key:
            raise RuntimeError("智谱 API Key 为空，请配置 evolution.llm.api_key 或环境变量 EVOLUTION_LLM_API_KEY")
        if not self.model:
            raise RuntimeError("智谱模型名为空，请配置 evolution.llm.model")

        # 组装与当前进化流程一致的上下文提示，确保输出策略可复现。
        user_prompt = self._build_user_prompt(prompt=prompt, context=context)
        messages = []
        if self.system_prompt:
            messages.append({"role": "system", "content": self.system_prompt})
        messages.append({"role": "user", "content": user_prompt})

        client = ZhipuAI(api_key=self.api_key)
        last_error: Exception | None = None
        for _ in range(self.retry_times + 1):
            try:
                kwargs: Dict[str, Any] = {
                    "model": self.model,
                    "messages": messages,
                    "temperature": float(self.temperature),
                    "max_tokens": int(self.max_tokens),
                    "timeout": int(self.timeout_seconds),
                }
                resp = client.chat.completions.create(**kwargs)
                content = self._extract_content(resp)
                code = self._extract_code(content)
                if not code.strip():
                    raise RuntimeError("智谱返回内容为空")
                self.last_call_meta = {
                    "provider": "zhipu",
                    "model": self.model,
                    "fallback_used": False,
                    "path": "direct",
                }
                return code
            except Exception as exc:
                last_error = exc
        self.last_call_meta = {
            "provider": "zhipu",
            "model": self.model,
            "fallback_used": False,
            "path": "direct",
            "error": str(last_error),
        }
        raise RuntimeError(f"智谱 LLM 调用失败: {last_error}")

    def _build_user_prompt(self, prompt: str, context: Dict[str, Any]) -> str:
        # 复用原有提示词结构，避免改变策略生成语义。
        seed_code = str(context.get("seed_code", "") or "")
        return (
            f"{str(prompt or '').strip()}\n\n"
            "请基于以下种子策略做改写，保持策略风格但不要完全重复：\n"
            f"{seed_code}\n\n"
            f"目标上下文：{context}\n"
            "只输出Python代码。"
        )

    def _extract_content(self, response: Any) -> str:
        # 兼容 SDK 对象返回与字典返回两种形态。
        if hasattr(response, "choices"):
            choices = getattr(response, "choices", None) or []
            if choices:
                first = choices[0]
                message = getattr(first, "message", None)
                if message is not None:
                    text = getattr(message, "content", "")
                    return str(text or "")
        if isinstance(response, dict):
            choices = response.get("choices", [])
            if isinstance(choices, list) and choices:
                message = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
                return str(message.get("content", "") or "")
        return ""

    def _extract_code(self, text: str) -> str:
        # 兼容 ```python 包裹与纯代码输出两种场景。
        content = str(text or "")
        match = re.search(r"```(?:python)?\s*([\s\S]*?)```", content, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return content.strip()
