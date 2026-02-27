"""
In-memory vector store for RAG (production-ready for small/medium corpora).

Uses cosine similarity. Embeddings can be supplied at add() or via an
embedding function at search_by_text. Integrates with existing
sentence-transformers usage in genai_export.
"""

from typing import Any, Callable, Dict, List, Optional

import numpy as np

from rag.base import DocumentChunk, RetrieverResult, VectorStore


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    an = a / (np.linalg.norm(a) + 1e-12)
    bn = b / (np.linalg.norm(b) + 1e-12)
    return float(np.dot(an, bn))


class InMemoryVectorStore(VectorStore):
    """In-memory vector store with numpy; optional embedding function for search_by_text."""

    def __init__(
        self,
        embedding_fn: Optional[Callable[[List[str]], np.ndarray]] = None,
    ):
        self._chunks: List[DocumentChunk] = []
        self._embeddings: Optional[np.ndarray] = None
        self._embedding_fn = embedding_fn

    def add(self, chunks: List[DocumentChunk]) -> None:
        for c in chunks:
            self._chunks.append(c)
        # Rebuild embedding matrix if any chunk has embedding
        self._rebuild_embeddings()

    def _rebuild_embeddings(self) -> None:
        has_emb = any(c.embedding is not None for c in self._chunks)
        if not has_emb or not self._chunks:
            self._embeddings = None
            return
        self._embeddings = np.vstack([
            c.embedding if c.embedding is not None else np.zeros(384)
            for c in self._chunks
        ])

    def search(
        self,
        query_embedding: np.ndarray,
        top_k: int = 5,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[RetrieverResult]:
        if not self._chunks:
            return []
        if self._embeddings is None:
            return []
        scores = np.array([
            _cosine_similarity(query_embedding, self._embeddings[i])
            for i in range(len(self._chunks))
        ])
        indices = np.argsort(scores)[::-1][:top_k]
        results: List[RetrieverResult] = []
        for i in indices:
            chunk = self._chunks[i]
            if filter_metadata:
                if not all(
                    chunk.metadata.get(k) == v
                    for k, v in filter_metadata.items()
                ):
                    continue
            results.append(RetrieverResult(chunk=chunk, score=float(scores[i])))
            if len(results) >= top_k:
                break
        return results

    def search_by_text(
        self,
        query_text: str,
        top_k: int = 5,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[RetrieverResult]:
        if self._embedding_fn is None:
            raise ValueError(
                "InMemoryVectorStore needs embedding_fn for search_by_text. "
                "Pass a function List[str] -> np.ndarray (e.g. sentence-transformers)."
            )
        vec = self._embedding_fn([query_text])
        if vec.ndim == 2:
            vec = vec[0]
        return self.search(vec, top_k=top_k, filter_metadata=filter_metadata)

    def clear(self) -> None:
        self._chunks = []
        self._embeddings = None
