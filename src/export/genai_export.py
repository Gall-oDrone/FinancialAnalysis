"""
GenAI Export Module

Export transformed data in formats suitable for GenAI applications:
- JSONL (JSON Lines) for RAG, fine-tuning, prompt context
- Optional embeddings for vector search
- Structured text with metadata

Usage:
    from export.genai_export import export_to_jsonl, generate_embeddings
    
    # Export to JSONL
    export_to_jsonl(transformed_df, "output/news.jsonl")
    
    # With embeddings
    df_with_embeddings = generate_embeddings(transformed_df)
    export_to_jsonl(df_with_embeddings, "output/news_embedded.jsonl")
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, asdict

import pandas as pd
import numpy as np

from core.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class GenAIArticle:
    """Article formatted for GenAI consumption."""
    id: str
    title: str
    summary: Optional[str]
    body: str
    metadata: Dict
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": str(self.id),
            "title": self.title,
            "summary": self.summary or "",
            "body": self.body,
            "metadata": self.metadata
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), ensure_ascii=False)


# ============================================================================
# Text Preparation
# ============================================================================

class GenAITextPreparator:
    """Prepare text for GenAI applications."""
    
    @staticmethod
    def clean_content(content: str) -> str:
        """
        Clean content for GenAI consumption.
        
        Removes:
        - Yahoo Finance stock tickers at start ("In this article:\nBTC-USD\n+0.22%...")
        - Excess whitespace
        - Special formatting artifacts
        
        Args:
            content: Raw content text
        
        Returns:
            Cleaned content
        """
        if not content:
            return ""
        
        # Remove "In this article:" section (Yahoo Finance specific)
        lines = content.split('\n')
        cleaned_lines = []
        skip_mode = False
        
        for line in lines:
            stripped = line.strip()
            
            # Skip "In this article:" section
            if stripped.startswith("In this article:"):
                skip_mode = True
                continue
            
            # End skip mode after ticker section
            if skip_mode:
                # Ticker lines are short (< 20 chars) or percentage changes
                if len(stripped) > 20 and not ('+' in stripped or '-' in stripped[:5]):
                    skip_mode = False
                    cleaned_lines.append(line)
            elif stripped:
                cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines).strip()
    
    @staticmethod
    def create_metadata(row: pd.Series, include_fields: List[str] = None) -> Dict:
        """
        Create metadata dictionary from DataFrame row.
        
        Args:
            row: DataFrame row
            include_fields: List of fields to include (default: all relevant)
        
        Returns:
            Metadata dictionary
        """
        if include_fields is None:
            include_fields = [
                'source', 'datetime', 'url', 'tickers',
                'sentiment_label', 'sentiment_score',
                'primary_intent', 'intent_confidence',
                'keywords', 'entities', 'author', 'word_count',
                # Flattened llm_financial_metrics (agentic) fields
                'llm_summary', 'llm_themes', 'llm_entities',
                'llm_ticker', 'llm_event_type', 'llm_overall_sentiment', 'llm_forward_sentiment',
                'llm_surprise_score', 'llm_risk_score', 'llm_uncertainty_score', 'llm_impact_strength',
                'llm_immediacy', 'llm_impact_horizon', 'llm_confidence', 'llm_novelty_score', 'llm_sentiment_label',
                'llm_impact_level', 'llm_signal', 'llm_actionable', 'llm_sectors', 'llm_key_facts',
            ]
        
        metadata = {}
        
        # Map of internal field names to export names
        field_map = {
            'href': 'url',
            'sentiment_label': 'sentiment',
            'primary_intent': 'intent'
        }
        
        for field in include_fields:
            # Use mapped export name when source field has one
            source_field = next((k for k, v in field_map.items() if v == field), field)
            export_name = field_map.get(source_field, field)

            # Skip only the nested llm_financial_metrics object (flattened fields are included above)
            if source_field == "llm_financial_metrics":
                continue

            if source_field not in row.index:
                continue
            value = row[source_field]
            # Avoid ambiguous truth for arrays/lists: use explicit checks
            if value is None:
                continue
            if isinstance(value, (list, np.ndarray)):
                if len(value) == 0:
                    continue
            elif pd.isna(value):
                continue

            # Convert numpy types to Python types
            if isinstance(value, (np.integer, np.floating)):
                value = value.item()
            elif isinstance(value, np.ndarray):
                value = value.tolist()

            if source_field == "datetime":
                from storage.postgres.news_dataframe import format_news_datetime_for_export

                value = format_news_datetime_for_export(value)

            metadata[export_name] = value

        return metadata


# ============================================================================
# Embeddings Generation
# ============================================================================

class EmbeddingGenerator:
    """Generate embeddings for text using sentence transformers."""
    
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        """
        Initialize embedding generator.
        
        Args:
            model_name: HuggingFace model name for embeddings
        """
        self.model_name = model_name
        self._model = None
        self._initialize_model()
    
    def _initialize_model(self):
        """Initialize the sentence transformer model."""
        try:
            from sentence_transformers import SentenceTransformer
            
            logger.info(f"Loading embedding model: {self.model_name}")
            self._model = SentenceTransformer(self.model_name)
            logger.info("Embedding model loaded successfully")
            
        except ImportError:
            logger.error(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )
            raise
    
    def encode(self, texts: List[str], batch_size: int = 32) -> np.ndarray:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: List of text strings
            batch_size: Batch size for encoding
        
        Returns:
            NumPy array of embeddings (n_texts, embedding_dim)
        """
        if not self._model:
            raise RuntimeError("Embedding model not initialized")
        
        logger.info(f"Generating embeddings for {len(texts)} texts...")
        
        embeddings = self._model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=True,
            convert_to_numpy=True
        )
        
        logger.info(f"Generated embeddings with shape: {embeddings.shape}")
        
        return embeddings
    
    def encode_dataframe(
        self,
        df: pd.DataFrame,
        text_column: str = 'cleaned_text',
        output_column: str = 'embedding'
    ) -> pd.DataFrame:
        """
        Add embeddings column to DataFrame.
        
        Args:
            df: DataFrame with text data
            text_column: Column containing text to embed
            output_column: Column name for embeddings
        
        Returns:
            DataFrame with embeddings column added
        """
        df = df.copy()
        
        # Get texts
        texts = df[text_column].fillna('').tolist()
        
        # Generate embeddings
        embeddings = self.encode(texts)
        
        # Add to DataFrame (as list for JSON serialization)
        df[output_column] = embeddings.tolist()
        
        return df


# ============================================================================
# Export Functions
# ============================================================================

def export_to_jsonl(
    df: pd.DataFrame,
    output_path: Union[str, Path],
    title_col: str = 'headline',
    summary_col: str = 'summary',
    body_col: str = 'cleaned_text',
    id_col: str = 'id',
    include_embeddings: bool = False
) -> int:
    """
    Export DataFrame to JSONL format for GenAI applications.
    
    Args:
        df: Transformed DataFrame
        output_path: Output file path (.jsonl)
        title_col: Column name for title
        summary_col: Column name for summary
        body_col: Column name for body text
        id_col: Column name for ID
        include_embeddings: Include embedding vectors if present
    
    Returns:
        Number of records exported
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Exporting {len(df)} records to JSONL: {output_path}")
    
    count = 0
    preparator = GenAITextPreparator()
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for _, row in df.iterrows():
            # Create GenAI article
            article = GenAIArticle(
                id=str(row.get(id_col, '')),
                title=str(row.get(title_col, '')),
                summary=(
                    str(row.get(summary_col, ''))
                    if row.get(summary_col) is not None
                    and not (isinstance(row.get(summary_col), float) and pd.isna(row.get(summary_col)))
                    else None
                ),
                body=preparator.clean_content(str(row.get(body_col, ''))),
                metadata=preparator.create_metadata(row)
            )
            
            # Add embedding if requested and available
            article_dict = article.to_dict()
            emb = row.get('embedding')
            has_embedding = (
                include_embeddings and emb is not None
                and (not isinstance(emb, (list, np.ndarray)) or len(emb) > 0)
            )
            if has_embedding:
                article_dict['embedding'] = row['embedding']
            
            # Write as single JSON line
            f.write(json.dumps(article_dict, ensure_ascii=False) + '\n')
            count += 1
    
    logger.info(f"Exported {count} records to {output_path}")
    return count


def export_to_s3_jsonl(
    df: pd.DataFrame,
    bucket_name: str,
    prefix_path: str,
    file_name: str,
    include_embeddings: bool = False
) -> str:
    """
    Export DataFrame to JSONL and upload to S3.
    
    Args:
        df: Transformed DataFrame
        bucket_name: S3 bucket name
        prefix_path: S3 prefix path
        file_name: File name (without extension)
        include_embeddings: Include embedding vectors
    
    Returns:
        S3 URI of uploaded file
    """
    from Storage.CloudStorage import CloudStorageProvider
    import tempfile
    
    # Export to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as tmp:
        tmp_path = tmp.name
    
    export_to_jsonl(df, tmp_path, include_embeddings=include_embeddings)
    
    # Normalize prefix path to include format=jsonl segment if not already present
    if "format=" not in prefix_path:
        prefix_path = f"{prefix_path.rstrip('/')}/format=jsonl"

    # Upload to S3
    aws = CloudStorageProvider.AWS()
    s3_key = f"{prefix_path}/{file_name}.jsonl"
    
    logger.info(f"Uploading to s3://{bucket_name}/{s3_key}")
    
    # Upload file
    import boto3
    s3_client = boto3.client('s3')
    s3_client.upload_file(tmp_path, bucket_name, s3_key)
    
    # Clean up temp file
    Path(tmp_path).unlink()
    
    s3_uri = f"s3://{bucket_name}/{s3_key}"
    logger.info(f"Upload complete: {s3_uri}")
    
    return s3_uri


def generate_embeddings(
    df: pd.DataFrame,
    text_column: str = 'cleaned_text',
    model_name: str = 'all-MiniLM-L6-v2'
) -> pd.DataFrame:
    """
    Generate embeddings for DataFrame texts.
    
    Args:
        df: DataFrame with text data
        text_column: Column containing text to embed
        model_name: Sentence transformer model name
    
    Returns:
        DataFrame with 'embedding' column added
    """
    generator = EmbeddingGenerator(model_name=model_name)
    return generator.encode_dataframe(df, text_column=text_column)


def create_rag_dataset(
    df: pd.DataFrame,
    output_path: Union[str, Path],
    chunk_size: Optional[int] = None,
    include_embeddings: bool = True
) -> int:
    """
    Create a RAG-ready dataset with optional chunking.
    
    Args:
        df: Transformed DataFrame
        output_path: Output JSONL path
        chunk_size: Max characters per chunk (None = no chunking)
        include_embeddings: Generate embeddings
    
    Returns:
        Number of records (chunks) exported
    """
    if chunk_size:
        logger.info(f"Chunking articles with max size: {chunk_size} chars")
        df = chunk_articles(df, max_chunk_size=chunk_size)
    
    if include_embeddings:
        logger.info("Generating embeddings for RAG dataset...")
        df = generate_embeddings(df)
    
    return export_to_jsonl(df, output_path, include_embeddings=include_embeddings)


def chunk_articles(
    df: pd.DataFrame,
    max_chunk_size: int = 1000,
    text_column: str = 'cleaned_text'
) -> pd.DataFrame:
    """
    Split long articles into chunks for RAG.
    
    Args:
        df: DataFrame with articles
        max_chunk_size: Maximum characters per chunk
        text_column: Column to chunk
    
    Returns:
        DataFrame with chunks (multiple rows per article)
    """
    chunks = []
    
    for _, row in df.iterrows():
        text = row.get(text_column, '')
        
        if not text or len(text) <= max_chunk_size:
            # Keep as single chunk
            chunk_row = row.copy()
            chunk_row['chunk_index'] = 0
            chunks.append(chunk_row)
        else:
            # Split into chunks
            words = text.split()
            current_chunk = []
            current_size = 0
            chunk_index = 0
            
            for word in words:
                word_size = len(word) + 1  # +1 for space
                
                if current_size + word_size > max_chunk_size and current_chunk:
                    # Save current chunk
                    chunk_row = row.copy()
                    chunk_row[text_column] = ' '.join(current_chunk)
                    chunk_row['chunk_index'] = chunk_index
                    chunk_row['id'] = f"{row['id']}_chunk_{chunk_index}"
                    chunks.append(chunk_row)
                    
                    # Start new chunk
                    current_chunk = [word]
                    current_size = word_size
                    chunk_index += 1
                else:
                    current_chunk.append(word)
                    current_size += word_size
            
            # Save final chunk
            if current_chunk:
                chunk_row = row.copy()
                chunk_row[text_column] = ' '.join(current_chunk)
                chunk_row['chunk_index'] = chunk_index
                chunk_row['id'] = f"{row['id']}_chunk_{chunk_index}"
                chunks.append(chunk_row)
    
    logger.info(f"Chunked {len(df)} articles into {len(chunks)} chunks")
    
    return pd.DataFrame(chunks)


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Example: Create sample data
    sample_articles = pd.DataFrame([
        {
            "id": "1",
            "headline": "Bitcoin Surges Past $100,000",
            "summary": "Bitcoin reached a new all-time high today.",
            "cleaned_text": "Bitcoin surged past $100,000 today, driven by institutional demand...",
            "tickers": ["BTC-USD"],
            "sentiment_label": "positive",
            "sentiment_score": 0.8,
            "primary_intent": "market_update",
            "keywords": ["bitcoin", "surge", "institutional"],
            "datetime": "2024-01-15T10:30:00Z",
            "source": "Yahoo Finance",
            "href": "https://example.com/article1"
        },
        {
            "id": "2",
            "headline": "SEC Delays Ethereum ETF Decision",
            "summary": "Regulatory uncertainty continues for crypto markets.",
            "cleaned_text": "The SEC announced a delay in its decision on Ethereum ETF applications...",
            "tickers": ["ETH-USD"],
            "sentiment_label": "negative",
            "sentiment_score": -0.3,
            "primary_intent": "regulatory_news",
            "keywords": ["sec", "ethereum", "etf", "delay"],
            "datetime": "2024-01-15T14:00:00Z",
            "source": "CoinDesk",
            "href": "https://example.com/article2"
        }
    ])
    
    print("GenAI Export Example")
    print("=" * 60)
    
    # Export to JSONL
    output_path = Path("output/sample_genai.jsonl")
    count = export_to_jsonl(sample_articles, output_path)
    
    print(f"\nExported {count} articles to {output_path}")
    
    # Show sample output
    if output_path.exists():
        print("\nSample JSONL output:")
        print("-" * 40)
        with open(output_path, 'r') as f:
            for i, line in enumerate(f):
                if i < 2:  # Show first 2 records
                    data = json.loads(line)
                    print(f"\nRecord {i+1}:")
                    print(f"  Title: {data['title'][:50]}...")
                    print(f"  Metadata keys: {list(data['metadata'].keys())}")
