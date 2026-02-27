"""
OpenAI API client implementing the LLMClient interface.

Requires: pip install openai
Environment: OPENAI_API_KEY
"""

import os
import time
from typing import List, Optional

from agents.base import AgentConfig, LLMMessage, LLMResponse, LLMClient
from core.logging import get_logger

logger = get_logger(__name__)


class OpenAIClient(LLMClient):
    """OpenAI API client (chat completions) with retries and timeout."""

    def __init__(self, config: Optional[AgentConfig] = None):
        self._config = config or AgentConfig(provider="openai")
        self._model = (
            self._config.model
            or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        )
        api_key_env = self._config.api_key_env_var or "OPENAI_API_KEY"
        self._api_key = os.getenv(api_key_env)
        if not self._api_key:
            raise ValueError(
                f"Missing {api_key_env}. Set it in environment or .env."
            )
        self._client = None

    def _get_client(self):
        if self._client is None:
            import openai
            self._client = openai.OpenAI(api_key=self._api_key)
        return self._client

    @property
    def provider_name(self) -> str:
        return "openai"

    @property
    def model_id(self) -> str:
        return self._model

    def complete(
        self,
        messages: List[LLMMessage],
        *,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> LLMResponse:
        max_tokens = max_tokens or self._config.max_tokens
        temperature = temperature if temperature is not None else self._config.temperature

        openai_messages = [
            {"role": m.role, "content": m.content}
            for m in messages
        ]

        last_error = None
        for attempt in range(self._config.max_retries):
            try:
                resp = self._get_client().chat.completions.create(
                    model=self._model,
                    messages=openai_messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    timeout=self._config.timeout_seconds,
                )
                choice = resp.choices[0]
                usage = None
                if resp.usage:
                    usage = {
                        "input_tokens": getattr(resp.usage, "prompt_tokens", 0),
                        "output_tokens": getattr(resp.usage, "completion_tokens", 0),
                        "total_tokens": getattr(resp.usage, "total_tokens", 0),
                    }
                return LLMResponse(
                    content=choice.message.content or "",
                    model=self._model,
                    usage=usage,
                    finish_reason=getattr(choice, "finish_reason", None),
                    raw={"id": resp.id} if resp else None,
                )
            except Exception as e:
                last_error = e
                logger.warning(
                    "OpenAI request failed (attempt %s/%s): %s",
                    attempt + 1,
                    self._config.max_retries,
                    e,
                )
                if attempt < self._config.max_retries - 1:
                    time.sleep(self._config.retry_backoff_factor ** attempt)
        raise last_error  # type: ignore

    def complete_text(self, prompt: str, *, system: Optional[str] = None) -> str:
        messages: List[LLMMessage] = []
        if system:
            messages.append(LLMMessage(role="system", content=system))
        messages.append(LLMMessage(role="user", content=prompt))
        response = self.complete(messages)
        return response.content
