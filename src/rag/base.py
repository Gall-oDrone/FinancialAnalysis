"""
RAG base abstractions: vector store interface, document chunk, retriever result.

OOP: ETL and agents depend on these interfaces; concrete stores (in-memory,
pgvector, Pinecone) implement VectorStore.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np


@dataclass
class DocumentChunk:
    """Single chunk of text with optional metadata and embedding."""
    id: str
    text: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[np.ndarray] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "text": self.text,
            "metadata": self.metadata,
            "embedding": self.embedding.tolist() if self.embedding is not None else None,
        }


@dataclass
class RetrieverResult:
    """Result of a retrieval query (chunk + score)."""
    chunk: DocumentChunk
    score: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk": self.chunk.to_dict(),
            "score": self.score,
        }


class VectorStore(ABC):
    """Abstract vector store for RAG: add chunks, search by vector or text."""

    @abstractmethod
    def add(self, chunks: List[DocumentChunk]) -> None:
        """Add document chunks (with or without precomputed embeddings)."""
        pass

    @abstractmethod
    def search(
        self,
        query_embedding: np.ndarray,
        top_k: int = 5,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[RetrieverResult]:
        """Return top_k chunks by similarity to query_embedding."""
        pass

    @abstractmethod
    def search_by_text(
        self,
        query_text: str,
        top_k: int = 5,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[RetrieverResult]:
        """Embed query_text and run search (requires embedding function)."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Remove all chunks (for testing or refresh)."""
        pass
