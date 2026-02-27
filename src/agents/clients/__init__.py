"""
Concrete LLM client implementations.

Register providers so get_llm_client("openai") / get_llm_client("claude") work.
Optional dependencies: install openai and/or anthropic for the provider you use.
"""

from agents.registry import register_llm_client

# Register available clients (import triggers registration)
try:
    from agents.clients.openai_client import OpenAIClient
    register_llm_client("openai", OpenAIClient)
except ImportError:
    pass

try:
    from agents.clients.claude_client import ClaudeClient
    register_llm_client("claude", ClaudeClient)
except ImportError:
    pass
