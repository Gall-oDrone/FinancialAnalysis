"""
Anthropic Claude API client implementing the LLMClient interface.

Requires: pip install anthropic
Environment: ANTHROPIC_API_KEY
"""

import os
import time
from typing import List, Optional

from agents.base import AgentConfig, LLMMessage, LLMResponse, LLMClient
from core.logging import get_logger

logger = get_logger(__name__)


class ClaudeClient(LLMClient):
    """Anthropic Claude API client with retries and timeout."""

    def __init__(self, config: Optional[AgentConfig] = None):
        self._config = config or AgentConfig(provider="claude")
        self._model = (
            self._config.model
            or os.getenv("ANTHROPIC_MODEL", "claude-3-5-haiku-20241022")
        )
        api_key_env = self._config.api_key_env_var or "ANTHROPIC_API_KEY"
        self._api_key = os.getenv(api_key_env)
        if not self._api_key:
            raise ValueError(
                f"Missing {api_key_env}. Set it in environment or .env."
            )
        self._client = None

    def _get_client(self):
        if self._client is None:
            import anthropic
            self._client = anthropic.Anthropic(api_key=self._api_key)
        return self._client

    @property
    def provider_name(self) -> str:
        return "claude"

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

        system = None
        anthropic_messages: List[dict] = []
        for m in messages:
            if m.role == "system":
                system = m.content
            else:
                anthropic_messages.append({"role": m.role, "content": m.content})

        last_error = None
        for attempt in range(self._config.max_retries):
            try:
                kwargs = {
                    "model": self._model,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "messages": anthropic_messages,
                    "timeout": self._config.timeout_seconds,
                }
                if system:
                    kwargs["system"] = system

                resp = self._get_client().messages.create(**kwargs)

                content = ""
                if resp.content:
                    for block in resp.content:
                        if hasattr(block, "text"):
                            content += block.text

                usage = None
                if resp.usage:
                    usage = {
                        "input_tokens": getattr(resp.usage, "input_tokens", 0),
                        "output_tokens": getattr(resp.usage, "output_tokens", 0),
                        "total_tokens": (
                            getattr(resp.usage, "input_tokens", 0)
                            + getattr(resp.usage, "output_tokens", 0)
                        ),
                    }
                return LLMResponse(
                    content=content,
                    model=self._model,
                    usage=usage,
                    finish_reason=getattr(resp, "stop_reason", None),
                    raw={"id": resp.id} if resp else None,
                )
            except Exception as e:
                last_error = e
                logger.warning(
                    "Claude request failed (attempt %s/%s): %s",
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
