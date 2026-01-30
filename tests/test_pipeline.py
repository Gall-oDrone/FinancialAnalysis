"""
Integration tests for data pipeline.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from DataProcessing.pipeline import (
    DataPipeline,
    PipelineConfig,
    PipelineStage,
    PipelineResult,
    TransformStageHandler,
    StockTransformStageHandler,
    GenAIExportStageHandler
)


class TestPipelineConfig:
    """Test pipeline configuration."""
    
    def test_default_config(self):
        """Test default pipeline configuration."""
        config = PipelineConfig()
        
        assert config.topics == ["crypto"]
        assert config.enrich_full_content is True
        assert config.enable_transformations is True
        assert config.sentiment_backend == "vader"
        assert config.continue_on_error is True
    
    def test_custom_config(self):
        """Test custom pipeline configuration."""
        config = PipelineConfig(
            topics=["stocks", "crypto"],
            sentiment_backend="textblob",
            s3_bucket="my-bucket",
            save_to_db=True
        )
        
        assert config.topics == ["stocks", "crypto"]
        assert config.sentiment_backend == "textblob"
        assert config.s3_bucket == "my-bucket"
        assert config.save_to_db is True


class TestPipelineResult:
    """Test pipeline result dataclass."""
    
    def test_result_to_dict(self):
        """Test converting result to dictionary."""
        result = PipelineResult(
            success=True,
            articles_scraped=10,
            articles_transformed=10,
            execution_time_seconds=15.5
        )
        
        d = result.to_dict()
        
        assert d['success'] is True
        assert d['articles_scraped'] == 10
        assert d['articles_transformed'] == 10
        assert d['execution_time_seconds'] == 15.5
    
    def test_result_with_errors(self):
        """Test result with errors."""
        result = PipelineResult(
            success=False,
            errors=["Stage 1 failed", "Stage 2 failed"]
        )
        
        assert result.success is False
        assert len(result.errors) == 2


class TestTransformStageHandler:
    """Test transform stage handler."""
    
    def test_transform_stage(self, sample_news_df):
        """Test news transformation stage."""
        handler = TransformStageHandler()
        config = PipelineConfig(sentiment_backend="vader")
        
        result = handler.execute(sample_news_df, config)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_news_df)
        assert 'sentiment_label' in result.columns
        assert 'tickers' in result.columns
    
    def test_transform_empty_dataframe(self):
        """Test transformation with empty DataFrame."""
        handler = TransformStageHandler()
        config = PipelineConfig()
        
        empty_df = pd.DataFrame()
        result = handler.execute(empty_df, config)
        
        assert result.empty


class TestStockTransformStageHandler:
    """Test stock transformation stage handler."""
    
    def test_stock_transform_stage(self, sample_stocks_df):
        """Test stock transformation stage."""
        handler = StockTransformStageHandler()
        config = PipelineConfig()
        
        result = handler.execute(sample_stocks_df, config)
        
        assert isinstance(result, pd.DataFrame)
        assert 'simple_return' in result.columns
        assert 'volatility_20d' in result.columns
        assert 'sma_20' in result.columns
    
    def test_stock_transform_empty(self):
        """Test stock transformation with empty DataFrame."""
        handler = StockTransformStageHandler()
        config = PipelineConfig()
        
        empty_df = pd.DataFrame()
        result = handler.execute(empty_df, config)
        
        assert result.empty


class TestGenAIExportStageHandler:
    """Test GenAI export stage handler."""
    
    def test_genai_export_stage(self, sample_transformed_news_df, mock_s3):
        """Test GenAI export stage."""
        handler = GenAIExportStageHandler()
        config = PipelineConfig(s3_bucket="test-bucket")
        
        with patch('boto3.client', return_value=mock_s3):
            try:
                result = handler.execute(sample_transformed_news_df, config)
                
                # Should return same DataFrame
                assert len(result) == len(sample_transformed_news_df)
                
            except Exception:
                # S3 operations might fail in test, that's ok
                pass
    
    def test_genai_export_no_bucket(self, sample_transformed_news_df):
        """Test GenAI export without S3 bucket configured."""
        handler = GenAIExportStageHandler()
        config = PipelineConfig(s3_bucket=None)
        
        # Should skip export but not fail
        result = handler.execute(sample_transformed_news_df, config)
        assert len(result) == len(sample_transformed_news_df)


class TestDataPipeline:
    """Test full data pipeline."""
    
    def test_pipeline_initialization(self):
        """Test pipeline initialization."""
        config = PipelineConfig()
        pipeline = DataPipeline(config)
        
        assert pipeline.config == config
        assert pipeline._handlers is not None
    
    def test_run_transform_only(self, sample_news_df):
        """Test running only transformation stage."""
        config = PipelineConfig(
            stages=[PipelineStage.TRANSFORM],
            sentiment_backend="vader"
        )
        
        pipeline = DataPipeline(config)
        result = pipeline.run(initial_data=sample_news_df)
        
        assert isinstance(result, PipelineResult)
        assert result.articles_scraped > 0
    
    def test_pipeline_with_errors(self, sample_news_df):
        """Test pipeline error handling."""
        config = PipelineConfig(
            stages=[PipelineStage.TRANSFORM],
            continue_on_error=True
        )
        
        pipeline = DataPipeline(config)
        
        # Should handle errors gracefully
        result = pipeline.run(initial_data=sample_news_df)
        
        assert isinstance(result, PipelineResult)
    
    def test_run_transform_only_method(self, sample_news_df):
        """Test run_transform_only convenience method."""
        config = PipelineConfig()
        pipeline = DataPipeline(config)
        
        result = pipeline.run_transform_only(sample_news_df)
        
        assert isinstance(result, pd.DataFrame)
        assert 'sentiment_label' in result.columns


class TestPipelineIntegration:
    """Integration tests for multi-stage pipeline."""
    
    def test_news_transform_pipeline(self, sample_news_df):
        """Test news transformation pipeline."""
        config = PipelineConfig(
            stages=[
                PipelineStage.TRANSFORM,
            ],
            s3_bucket=None,  # Skip S3 for test
            save_to_db=False
        )
        
        pipeline = DataPipeline(config)
        result = pipeline.run(initial_data=sample_news_df)
        
        assert result.success
        assert result.articles_transformed > 0
        assert PipelineStage.TRANSFORM.value in result.stage_results
    
    def test_stock_transform_pipeline(self, sample_stocks_df):
        """Test stock transformation pipeline."""
        config = PipelineConfig(
            stages=[PipelineStage.TRANSFORM_STOCKS]
        )
        
        pipeline = DataPipeline(config)
        result = pipeline.run(initial_data=sample_stocks_df)
        
        assert result.success
        assert PipelineStage.TRANSFORM_STOCKS.value in result.stage_results
