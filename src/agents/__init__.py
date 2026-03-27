"""
Agentic AI module for ETL transform tasks.

Provides:
- LLM client abstractions (Claude, OpenAI, extensible via OOP)
- Optional agentic transform stage (LLM-based enrichment)
- RAG components (vector stores, chunking, retrieval)
- MCP (Model Context Protocol) integration points

Usage:
    from agents import get_llm_client, AgenticTextEnricher
    client = get_llm_client("openai")
    enricher = AgenticTextEnricher(client)
"""

from agents.base import (
    LLMClient,
    LLMResponse,
    LLMMessage,
    AgentConfig,
)
from agents.registry import get_llm_client, register_llm_client
from agents.transforms.agentic_transform import AgenticTextEnricher

__all__ = [
    "LLMClient",
    "LLMResponse",
    "LLMMessage",
    "AgentConfig",
    "get_llm_client",
    "register_llm_client",
    "AgenticTextEnricher",
]
