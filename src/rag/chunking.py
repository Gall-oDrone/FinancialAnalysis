"""
Document chunking for RAG: split articles or GenAI export into fixed-size chunks.

Works with DataFrame rows (e.g. transformed articles) or raw text. Produces
DocumentChunk instances ready for vector store ingestion.
"""

from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from rag.base import DocumentChunk

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


class TextChunker:
    """
    Split text or dataframe rows into chunks for RAG.

    Strategies: fixed character count, sentence boundary (simple), or custom.
    """

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        separator: str = "\n",
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separator = separator

    def chunk_text(
        self,
        text: str,
        doc_id: str = "doc",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[DocumentChunk]:
        """Split a single text into chunks."""
        if not text or not text.strip():
            return []
        metadata = metadata or {}
        chunks: List[DocumentChunk] = []
        start = 0
        idx = 0
        while start < len(text):
            end = min(start + self.chunk_size, len(text))
            # Try to break at separator
            if end < len(text):
                last_sep = text.rfind(self.separator, start, end + 1)
                if last_sep >= start:
                    end = last_sep + 1
            piece = text[start:end].strip()
            if piece:
                chunk_id = f"{doc_id}_chunk_{idx}"
                chunks.append(
                    DocumentChunk(id=chunk_id, text=piece, metadata={**metadata})
                )
                idx += 1
            start = end - self.chunk_overlap if end < len(text) else len(text)
        return chunks

    def chunk_dataframe(
        self,
        df: pd.DataFrame,
        text_columns: List[str],
        id_column: str = "id",
        metadata_columns: Optional[List[str]] = None,
    ) -> List[DocumentChunk]:
        """
        Build chunks from a DataFrame (e.g. transformed articles or GenAI export).

        Concatenates text_columns for each row and chunks; each chunk gets
        row id and optional metadata_columns in chunk.metadata.
        """
        all_chunks: List[DocumentChunk] = []
        metadata_columns = metadata_columns or []
        for _, row in df.iterrows():
            doc_id = str(row.get(id_column, len(all_chunks)))
            parts = [str(row.get(c, "")) for c in text_columns if c in row]
            text = self.separator.join(p for p in parts if p)
            meta = {
                k: row[k] for k in metadata_columns
                if k in row and pd.notna(row.get(k))
            }
            for k, v in meta.items():
                if hasattr(v, "tolist"):
                    meta[k] = v.tolist()
                elif isinstance(v, (list, dict)):
                    try:
                        meta[k] = str(v)
                    except Exception:
                        meta[k] = v
            chunk_list = self.chunk_text(text, doc_id=doc_id, metadata=meta)
            all_chunks.extend(chunk_list)
        return all_chunks
