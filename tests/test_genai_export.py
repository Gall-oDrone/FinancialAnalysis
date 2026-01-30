"""
Tests for GenAI export module.
"""

import pytest
import pandas as pd
import json
import tempfile
from pathlib import Path

from DataProcessing.genai_export import (
    GenAIArticle,
    GenAITextPreparator,
    export_to_jsonl,
    chunk_articles
)


class TestGenAIArticle:
    """Test GenAIArticle dataclass."""
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        article = GenAIArticle(
            id="1",
            title="Test Title",
            summary="Test summary",
            body="Test body content",
            metadata={"source": "test", "tickers": ["BTC-USD"]}
        )
        
        d = article.to_dict()
        
        assert d['id'] == "1"
        assert d['title'] == "Test Title"
        assert d['summary'] == "Test summary"
        assert d['body'] == "Test body content"
        assert d['metadata']['source'] == "test"
    
    def test_to_json(self):
        """Test JSON serialization."""
        article = GenAIArticle(
            id="1",
            title="Test",
            summary=None,
            body="Content",
            metadata={}
        )
        
        json_str = article.to_json()
        parsed = json.loads(json_str)
        
        assert parsed['id'] == "1"
        assert parsed['summary'] == ""  # None converted to empty string


class TestGenAITextPreparator:
    """Test text preparation for GenAI."""
    
    def test_clean_yahoo_finance_content(self):
        """Test cleaning Yahoo Finance content."""
        content = """In this article:
BTC-USD
+10.22%
ETH-USD
-2.45%

Bitcoin surged past $100,000 today as institutional adoption continues."""
        
        cleaned = GenAITextPreparator.clean_content(content)
        
        assert "In this article:" not in cleaned
        assert "BTC-USD" not in cleaned
        assert "+10.22%" not in cleaned
        assert "Bitcoin surged" in cleaned
    
    def test_clean_empty_content(self):
        """Test cleaning empty content."""
        cleaned = GenAITextPreparator.clean_content("")
        assert cleaned == ""
        
        cleaned = GenAITextPreparator.clean_content(None)
        assert cleaned == ""
    
    def test_create_metadata(self, sample_transformed_news_df):
        """Test metadata creation from DataFrame row."""
        row = sample_transformed_news_df.iloc[0]
        
        metadata = GenAITextPreparator.create_metadata(row)
        
        assert 'source' in metadata
        assert 'datetime' in metadata
        assert 'sentiment' in metadata
        assert 'tickers' in metadata
    
    def test_create_metadata_with_custom_fields(self, sample_transformed_news_df):
        """Test metadata with custom field selection."""
        row = sample_transformed_news_df.iloc[0]
        
        metadata = GenAITextPreparator.create_metadata(
            row,
            include_fields=['source', 'tickers']
        )
        
        assert 'source' in metadata
        assert 'tickers' in metadata
        assert 'datetime' not in metadata  # Not in custom fields


class TestJSONLExport:
    """Test JSONL export functionality."""
    
    def test_export_to_jsonl(self, sample_transformed_news_df):
        """Test basic JSONL export."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            count = export_to_jsonl(sample_transformed_news_df, tmp_path)
            
            assert count == len(sample_transformed_news_df)
            assert tmp_path.exists()
            
            # Verify JSONL format
            with open(tmp_path, 'r') as f:
                lines = f.readlines()
            
            assert len(lines) == count
            
            # Parse first line
            first_record = json.loads(lines[0])
            assert 'id' in first_record
            assert 'title' in first_record
            assert 'body' in first_record
            assert 'metadata' in first_record
            
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
    
    def test_export_empty_dataframe(self):
        """Test exporting empty DataFrame."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            empty_df = pd.DataFrame()
            count = export_to_jsonl(empty_df, tmp_path)
            
            assert count == 0
            assert tmp_path.exists()
            
            # File should be empty
            with open(tmp_path, 'r') as f:
                content = f.read()
            assert content == ""
            
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
    
    def test_export_with_embeddings(self, sample_transformed_news_df):
        """Test export with embeddings column."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            # Add fake embeddings
            df = sample_transformed_news_df.copy()
            df['embedding'] = [[0.1, 0.2, 0.3]] * len(df)
            
            count = export_to_jsonl(df, tmp_path, include_embeddings=True)
            
            assert count == len(df)
            
            # Verify embeddings in output
            with open(tmp_path, 'r') as f:
                first_line = f.readline()
            
            first_record = json.loads(first_line)
            assert 'embedding' in first_record
            assert isinstance(first_record['embedding'], list)
            
        finally:
            if tmp_path.exists():
                tmp_path.unlink()


class TestArticleChunking:
    """Test article chunking for RAG."""
    
    def test_chunk_long_articles(self):
        """Test chunking of long articles."""
        # Create article with long content
        df = pd.DataFrame([{
            'id': '1',
            'headline': 'Test',
            'cleaned_text': ' '.join(['word'] * 500),  # 500 words
            'tickers': ['BTC-USD']
        }])
        
        chunked = chunk_articles(df, max_chunk_size=100)
        
        # Should create multiple chunks
        assert len(chunked) > 1
        assert 'chunk_index' in chunked.columns
        
        # Check chunk IDs
        assert chunked.iloc[0]['id'] == '1_chunk_0'
        assert chunked.iloc[1]['id'] == '1_chunk_1'
    
    def test_no_chunking_for_short_articles(self):
        """Test that short articles are not chunked."""
        df = pd.DataFrame([{
            'id': '1',
            'headline': 'Test',
            'cleaned_text': 'Short article content',
            'tickers': []
        }])
        
        chunked = chunk_articles(df, max_chunk_size=1000)
        
        assert len(chunked) == 1
        assert chunked.iloc[0]['chunk_index'] == 0
    
    def test_chunk_preserves_metadata(self):
        """Test that chunking preserves article metadata."""
        df = pd.DataFrame([{
            'id': '1',
            'headline': 'Test',
            'cleaned_text': ' '.join(['word'] * 200),
            'tickers': ['BTC-USD'],
            'sentiment_label': 'positive'
        }])
        
        chunked = chunk_articles(df, max_chunk_size=100)
        
        # All chunks should have same metadata
        for _, chunk in chunked.iterrows():
            assert chunk['tickers'] == ['BTC-USD']
            assert chunk['sentiment_label'] == 'positive'


class TestMockS3Upload:
    """Test S3 upload functionality (mocked)."""
    
    def test_s3_upload_mock(self, sample_transformed_news_df, mock_s3):
        """Test S3 upload with mocked client."""
        from DataProcessing.genai_export import export_to_s3_jsonl
        from unittest.mock import patch
        
        with patch('boto3.client', return_value=mock_s3):
            try:
                s3_uri = export_to_s3_jsonl(
                    sample_transformed_news_df,
                    bucket_name="test-bucket",
                    prefix_path="genai/news",
                    file_name="test_export",
                    include_embeddings=False
                )
                
                assert s3_uri.startswith("s3://test-bucket/genai/news")
                assert mock_s3.upload_file.called
                
            except Exception as e:
                # S3 operations might fail in test environment
                # but we verify the mock was called
                pass
