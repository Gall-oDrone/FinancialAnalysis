"""
Base abstractions for Agentic AI (LLM clients, config, response types).

Follows OOP: abstract interface for any provider (Claude, OpenAI, etc.)
so ETL transform tasks can use LLMs in a provider-agnostic way.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from core.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Response and message types
# ============================================================================


@dataclass
class LLMMessage:
    """Single message in a chat/completion request."""
    role: str  # "user", "assistant", "system"
    content: str

    def to_dict(self) -> Dict[str, str]:
        return {"role": self.role, "content": self.content}


@dataclass
class LLMResponse:
    """Normalized response from any LLM provider."""
    content: str
    model: str
    usage: Optional[Dict[str, int]] = None  # input_tokens, output_tokens, total_tokens
    finish_reason: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "content": self.content,
            "model": self.model,
            "usage": self.usage,
            "finish_reason": self.finish_reason,
        }


# ============================================================================
# Configuration
# ============================================================================


class LLMProvider(str, Enum):
    """Supported LLM providers."""
    OPENAI = "openai"
    CLAUDE = "claude"
    # Extensible: add ANTHROPIC, BEDROCK, etc. as needed


@dataclass
class AgentConfig:
    """Configuration for LLM/agent usage in ETL (production-ready)."""
    provider: str = "openai"  # openai, claude
    model: Optional[str] = None  # provider-specific model id
    api_key_env_var: Optional[str] = None  # e.g. OPENAI_API_KEY
    max_tokens: int = 1024
    temperature: float = 0.0  # deterministic for ETL
    timeout_seconds: float = 60.0
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    # Rate limiting (optional): max requests per minute
    rate_limit_rpm: Optional[int] = None


# ============================================================================
# Abstract LLM client interface
# ============================================================================


class LLMClient(ABC):
    """
    Abstract base class for LLM API clients.

    Implementations (OpenAIClient, ClaudeClient) handle provider-specific
    request/response formats. ETL code depends only on this interface.
    """

    @abstractmethod
    def complete(
        self,
        messages: List[LLMMessage],
        *,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> LLMResponse:
        """Send messages and return normalized completion."""
        pass

    @abstractmethod
    def complete_text(self, prompt: str, *, system: Optional[str] = None) -> str:
        """Convenience: single user prompt, return content string."""
        pass

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Provider identifier for logging."""
        pass

    @property
    @abstractmethod
    def model_id(self) -> str:
        """Model identifier in use."""
        pass

    def __enter__(self) -> "LLMClient":
        return self

    def __exit__(self, *args: Any) -> None:
        pass
