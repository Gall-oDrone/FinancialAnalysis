"""
Registry for LLM client implementations (factory + registration).

Allows pipeline/transform code to request a client by provider name
without depending on concrete classes. New providers register here.
"""

from typing import Dict, Optional, Type

from agents.base import AgentConfig, LLMClient

_registry: Dict[str, Type[LLMClient]] = {}


def register_llm_client(provider: str, client_class: Type[LLMClient]) -> None:
    """Register an LLM client class for a provider name."""
    _registry[provider.lower()] = client_class


def get_llm_client(
    provider: str = "openai",
    config: Optional[AgentConfig] = None,
) -> LLMClient:
    """
    Factory: return an LLM client for the given provider.

    Args:
        provider: "openai", "claude", or any registered name.
        config: Optional overrides; if None, defaults from env are used.

    Returns:
        LLMClient instance.

    Raises:
        KeyError: If provider is not registered.
        ValueError: If required API key is missing.
    """
    from config.settings import get_settings

    # Ensure concrete clients are registered (optional deps)
    if not _registry:
        try:
            import agents.clients  # noqa: F401
        except ImportError:
            pass

    provider = provider.lower()
    if provider not in _registry:
        raise KeyError(
            f"Unknown LLM provider: {provider}. "
            f"Available: {list(_registry.keys())}. "
            "Install optional deps and ensure clients are imported to register."
        )

    settings = get_settings()
    agent_settings = getattr(settings, "agent", None)
    if config is None and agent_settings is not None:
        config = AgentConfig(
            provider=agent_settings.provider or provider,
            model=agent_settings.model,
            api_key_env_var=agent_settings.api_key_env_var,
            max_tokens=agent_settings.max_tokens,
            temperature=agent_settings.temperature,
            timeout_seconds=agent_settings.timeout_seconds,
            max_retries=agent_settings.max_retries,
            retry_backoff_factor=agent_settings.retry_backoff_factor,
            rate_limit_rpm=agent_settings.rate_limit_rpm,
        )
    if config is None:
        config = AgentConfig(provider=provider)

    return _registry[provider](config)
