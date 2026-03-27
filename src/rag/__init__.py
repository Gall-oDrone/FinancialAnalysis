"""
RAG (Retrieval-Augmented Generation) module for financial analysis.

Provides:
- Vector store abstraction (in-memory, extensible to pgvector/Pinecone)
- Document chunking for GenAI/transformed articles
- Retriever interface for use with Agentic AI pipelines and MCP
"""

from rag.base import (
    VectorStore,
    DocumentChunk,
    RetrieverResult,
)
from rag.chunking import TextChunker
from rag.stores.memory import InMemoryVectorStore

__all__ = [
    "VectorStore",
    "DocumentChunk",
    "RetrieverResult",
    "TextChunker",
    "InMemoryVectorStore",
]
