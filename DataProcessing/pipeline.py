"""
Data Pipeline Orchestrator

A production-ready pipeline that orchestrates:
1. Web scraping (news collection)
2. S3 upload (raw data storage)
3. Text transformation (ML/AI preparation)
4. Transformed data storage (S3 and/or PostgreSQL)

Usage:
    from DataProcessing.pipeline import DataPipeline, PipelineConfig
    
    config = PipelineConfig(
        topics=["crypto", "stocks"],
        s3_bucket="my-financial-data",
        enable_transformations=True
    )
    
    pipeline = DataPipeline(config)
    results = pipeline.run()
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional, Any, Callable
import json
import time

import pandas as pd

from config.settings import get_settings
from utils.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Pipeline Configuration
# ============================================================================

class PipelineStage(Enum):
    """Pipeline execution stages."""
    SCRAPE = "scrape"
    UPLOAD_RAW = "upload_raw"
    TRANSFORM = "transform"
    UPLOAD_TRANSFORMED = "upload_transformed"
    SAVE_DB = "save_db"


@dataclass
class PipelineConfig:
    """Configuration for the data pipeline."""
    
    # Scraping configuration
    topics: List[str] = field(default_factory=lambda: ["crypto"])
    enrich_full_content: bool = True
    
    # S3 configuration
    s3_bucket: Optional[str] = None
    s3_raw_prefix: str = "raw/news"
    s3_transformed_prefix: str = "transformed/news"
    
    # Transformation configuration
    enable_transformations: bool = True
    sentiment_backend: str = "vader"  # vader, textblob, transformers
    use_transformer_intents: bool = False
    keyword_method: str = "tfidf"  # tfidf, spacy, rake
    keyword_top_n: int = 10
    
    # Database configuration
    save_to_db: bool = False
    db_table_name: str = "financial_news_transformed"
    
    # Pipeline behavior
    stages: List[PipelineStage] = field(default_factory=lambda: [
        PipelineStage.SCRAPE,
        PipelineStage.UPLOAD_RAW,
        PipelineStage.TRANSFORM,
        PipelineStage.UPLOAD_TRANSFORMED,
    ])
    continue_on_error: bool = True
    
    def __post_init__(self):
        """Load defaults from settings if not provided."""
        settings = get_settings()
        if self.s3_bucket is None:
            self.s3_bucket = settings.aws.default_bucket


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    success: bool
    stage_results: Dict[str, Any] = field(default_factory=dict)
    articles_scraped: int = 0
    articles_transformed: int = 0
    errors: List[str] = field(default_factory=list)
    execution_time_seconds: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            "success": self.success,
            "stage_results": self.stage_results,
            "articles_scraped": self.articles_scraped,
            "articles_transformed": self.articles_transformed,
            "errors": self.errors,
            "execution_time_seconds": self.execution_time_seconds
        }


# ============================================================================
# Pipeline Stage Handlers
# ============================================================================

class PipelineStageHandler(ABC):
    """Abstract base class for pipeline stage handlers."""
    
    @abstractmethod
    def execute(self, data: Any, config: PipelineConfig) -> Any:
        """Execute the pipeline stage."""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Stage name for logging."""
        pass


class ScrapeStageHandler(PipelineStageHandler):
    """Handles the web scraping stage."""
    
    @property
    def name(self) -> str:
        return "Scrape"
    
    def execute(self, data: Any, config: PipelineConfig) -> pd.DataFrame:
        """Execute scraping stage."""
        from WebScraping.src.news_collector_refactored_example import (
            NewsCollector, 
            NewsScrapingConfig
        )
        
        logger.info(f"Starting scraping for topics: {config.topics}")
        
        all_articles = []
        
        with NewsCollector() as collector:
            for topic in config.topics:
                try:
                    articles = collector.collect_news_for_topic(topic)
                    
                    if config.enrich_full_content:
                        articles = collector.enrich_articles_with_full_content(articles)
                    
                    # Convert to dictionaries
                    for article in articles:
                        article_dict = article.to_dict()
                        article_dict['topic'] = topic
                        article_dict['scraped_at'] = datetime.now().isoformat()
                        all_articles.append(article_dict)
                    
                    logger.info(f"Scraped {len(articles)} articles for topic: {topic}")
                    
                except Exception as e:
                    logger.error(f"Error scraping topic {topic}: {e}")
                    if not config.continue_on_error:
                        raise
        
        df = pd.DataFrame(all_articles)
        logger.info(f"Total articles scraped: {len(df)}")
        
        return df


class UploadRawStageHandler(PipelineStageHandler):
    """Handles uploading raw data to S3."""
    
    @property
    def name(self) -> str:
        return "Upload Raw to S3"
    
    def execute(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        """Upload raw scraped data to S3."""
        if data.empty:
            logger.warning("No data to upload")
            return data
        
        if not config.s3_bucket:
            logger.warning("No S3 bucket configured, skipping upload")
            return data
        
        from Storage.CloudStorage import CloudStorageProvider
        
        aws = CloudStorageProvider.AWS()
        
        # Generate timestamp-based path
        now = datetime.now()
        date_path = f"year={now.year}/month={now.month:02}/day={now.day:02}"
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        
        # Upload as CSV
        prefix_path = f"{config.s3_raw_prefix}/{date_path}"
        file_name = f"news_raw_{timestamp}"
        
        try:
            aws.upload_dataframe_to_csv(
                dataframe=data,
                bucket_name=config.s3_bucket,
                file_name=file_name,
                prefix_path=prefix_path
            )
            logger.info(f"Raw data uploaded to s3://{config.s3_bucket}/{prefix_path}/{file_name}.csv")
        except Exception as e:
            logger.error(f"Failed to upload raw data to S3: {e}")
            if not config.continue_on_error:
                raise
        
        return data


class TransformStageHandler(PipelineStageHandler):
    """Handles text transformation for ML/AI."""
    
    @property
    def name(self) -> str:
        return "Transform for ML"
    
    def execute(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        """Transform articles for ML consumption."""
        if data.empty:
            logger.warning("No data to transform")
            return data
        
        from DataProcessing.text_transformers import TextTransformationPipeline
        
        # Initialize transformation pipeline
        transform_pipeline = TextTransformationPipeline(
            sentiment_backend=config.sentiment_backend,
            use_transformer_intents=config.use_transformer_intents,
            keyword_method=config.keyword_method,
            keyword_top_n=config.keyword_top_n
        )
        
        # Transform articles
        transformed_df = transform_pipeline.transform(data)
        
        logger.info(f"Transformed {len(transformed_df)} articles")
        
        return transformed_df


class UploadTransformedStageHandler(PipelineStageHandler):
    """Handles uploading transformed data to S3."""
    
    @property
    def name(self) -> str:
        return "Upload Transformed to S3"
    
    def execute(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        """Upload transformed data to S3."""
        if data.empty:
            logger.warning("No data to upload")
            return data
        
        if not config.s3_bucket:
            logger.warning("No S3 bucket configured, skipping upload")
            return data
        
        from Storage.CloudStorage import CloudStorageProvider
        
        aws = CloudStorageProvider.AWS()
        
        # Generate timestamp-based path
        now = datetime.now()
        date_path = f"year={now.year}/month={now.month:02}/day={now.day:02}"
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        
        # Upload transformed data as CSV
        prefix_path = f"{config.s3_transformed_prefix}/{date_path}"
        file_name = f"news_transformed_{timestamp}"
        
        try:
            # Convert complex columns to JSON strings for CSV storage
            df_export = data.copy()
            for col in ['keywords', 'entities', 'secondary_intents']:
                if col in df_export.columns:
                    df_export[col] = df_export[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                    )
            
            aws.upload_dataframe_to_csv(
                dataframe=df_export,
                bucket_name=config.s3_bucket,
                file_name=file_name,
                prefix_path=prefix_path
            )
            logger.info(f"Transformed data uploaded to s3://{config.s3_bucket}/{prefix_path}/{file_name}.csv")
        except Exception as e:
            logger.error(f"Failed to upload transformed data to S3: {e}")
            if not config.continue_on_error:
                raise
        
        return data


class SaveToDBStageHandler(PipelineStageHandler):
    """Handles saving data to PostgreSQL."""
    
    @property
    def name(self) -> str:
        return "Save to Database"
    
    def execute(self, data: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
        """Save transformed data to PostgreSQL."""
        if data.empty:
            logger.warning("No data to save")
            return data
        
        from Storage.pgConn import PgConn
        
        db_conn = PgConn()
        db_conn.set_table(config.db_table_name)
        
        # Define columns for the transformed table
        header = [
            "id", "source", "headline", "href", "summary", "content", 
            "datetime", "topic", "scraped_at", "cleaned_text", "word_count",
            "sentiment_label", "sentiment_score", "positive_score", 
            "negative_score", "neutral_score", "primary_intent", 
            "intent_confidence", "keywords"
        ]
        
        saved_count = 0
        for _, row in data.iterrows():
            try:
                row_dict = {
                    col: (json.dumps(row[col]) if isinstance(row.get(col), (list, dict)) else row.get(col))
                    for col in header if col in row.index
                }
                db_conn.save_to_postgres(row_dict, list(row_dict.keys()))
                saved_count += 1
            except Exception as e:
                logger.error(f"Failed to save article to DB: {e}")
                if not config.continue_on_error:
                    raise
        
        logger.info(f"Saved {saved_count}/{len(data)} articles to database")
        db_conn.close_connection()
        
        return data


# ============================================================================
# Main Pipeline
# ============================================================================

class DataPipeline:
    """
    Main data pipeline orchestrator.
    
    Coordinates the execution of multiple stages:
    1. Scraping
    2. Raw data upload to S3
    3. Text transformation
    4. Transformed data upload to S3
    5. Database storage (optional)
    """
    
    # Stage handler mapping
    STAGE_HANDLERS = {
        PipelineStage.SCRAPE: ScrapeStageHandler,
        PipelineStage.UPLOAD_RAW: UploadRawStageHandler,
        PipelineStage.TRANSFORM: TransformStageHandler,
        PipelineStage.UPLOAD_TRANSFORMED: UploadTransformedStageHandler,
        PipelineStage.SAVE_DB: SaveToDBStageHandler,
    }
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        """
        Initialize the data pipeline.
        
        Args:
            config: Pipeline configuration. If None, uses defaults.
        """
        self.config = config or PipelineConfig()
        self._handlers = {
            stage: handler() 
            for stage, handler in self.STAGE_HANDLERS.items()
        }
    
    def run(self, initial_data: Optional[pd.DataFrame] = None) -> PipelineResult:
        """
        Execute the full pipeline.
        
        Args:
            initial_data: Optional DataFrame to start with (skips scraping if provided)
        
        Returns:
            PipelineResult with execution details
        """
        logger.info("=" * 60)
        logger.info("Starting Data Pipeline Execution")
        logger.info("=" * 60)
        
        start_time = time.time()
        result = PipelineResult(success=True)
        
        data = initial_data if initial_data is not None else pd.DataFrame()
        
        for stage in self.config.stages:
            try:
                # Skip scrape if initial data provided
                if stage == PipelineStage.SCRAPE and initial_data is not None:
                    logger.info(f"Skipping {stage.value} - initial data provided")
                    continue
                
                handler = self._handlers.get(stage)
                if not handler:
                    logger.warning(f"No handler for stage: {stage.value}")
                    continue
                
                logger.info(f"\n{'='*40}")
                logger.info(f"Executing stage: {handler.name}")
                logger.info(f"{'='*40}")
                
                stage_start = time.time()
                data = handler.execute(data, self.config)
                stage_time = time.time() - stage_start
                
                result.stage_results[stage.value] = {
                    "success": True,
                    "records": len(data) if isinstance(data, pd.DataFrame) else 0,
                    "time_seconds": round(stage_time, 2)
                }
                
                logger.info(f"Stage '{handler.name}' completed in {stage_time:.2f}s")
                
            except Exception as e:
                error_msg = f"Stage '{stage.value}' failed: {str(e)}"
                logger.error(error_msg, exc_info=True)
                result.errors.append(error_msg)
                result.stage_results[stage.value] = {
                    "success": False,
                    "error": str(e)
                }
                
                if not self.config.continue_on_error:
                    result.success = False
                    break
        
        # Populate result summary
        result.execution_time_seconds = round(time.time() - start_time, 2)
        
        if isinstance(data, pd.DataFrame) and not data.empty:
            result.articles_scraped = len(data)
            if 'sentiment_label' in data.columns:
                result.articles_transformed = len(data[data['sentiment_label'].notna()])
        
        result.success = len(result.errors) == 0
        
        logger.info("\n" + "=" * 60)
        logger.info("Pipeline Execution Complete")
        logger.info(f"Total time: {result.execution_time_seconds}s")
        logger.info(f"Articles processed: {result.articles_scraped}")
        logger.info(f"Errors: {len(result.errors)}")
        logger.info("=" * 60)
        
        return result
    
    def run_scrape_only(self) -> pd.DataFrame:
        """Run only the scraping stage."""
        handler = self._handlers[PipelineStage.SCRAPE]
        return handler.execute(None, self.config)
    
    def run_transform_only(self, data: pd.DataFrame) -> pd.DataFrame:
        """Run only the transformation stage."""
        handler = self._handlers[PipelineStage.TRANSFORM]
        return handler.execute(data, self.config)
    
    def run_upload_only(self, data: pd.DataFrame, transformed: bool = False) -> pd.DataFrame:
        """Run only the S3 upload stage."""
        if transformed:
            handler = self._handlers[PipelineStage.UPLOAD_TRANSFORMED]
        else:
            handler = self._handlers[PipelineStage.UPLOAD_RAW]
        return handler.execute(data, self.config)


# ============================================================================
# Convenience Functions
# ============================================================================

def run_full_pipeline(
    topics: List[str] = None,
    s3_bucket: str = None,
    enable_transformations: bool = True,
    save_to_db: bool = False
) -> PipelineResult:
    """
    Convenience function to run the full pipeline with common options.
    
    Args:
        topics: List of topics to scrape
        s3_bucket: S3 bucket name for storage
        enable_transformations: Whether to run ML transformations
        save_to_db: Whether to save to PostgreSQL
    
    Returns:
        PipelineResult with execution details
    """
    stages = [PipelineStage.SCRAPE]
    
    if s3_bucket:
        stages.append(PipelineStage.UPLOAD_RAW)
    
    if enable_transformations:
        stages.append(PipelineStage.TRANSFORM)
        if s3_bucket:
            stages.append(PipelineStage.UPLOAD_TRANSFORMED)
    
    if save_to_db:
        stages.append(PipelineStage.SAVE_DB)
    
    config = PipelineConfig(
        topics=topics or ["crypto"],
        s3_bucket=s3_bucket,
        enable_transformations=enable_transformations,
        save_to_db=save_to_db,
        stages=stages
    )
    
    pipeline = DataPipeline(config)
    return pipeline.run()


def transform_existing_data(
    data: pd.DataFrame,
    sentiment_backend: str = "vader",
    upload_to_s3: bool = False,
    s3_bucket: str = None
) -> pd.DataFrame:
    """
    Transform existing data without scraping.
    
    Args:
        data: DataFrame with articles
        sentiment_backend: Sentiment analysis backend
        upload_to_s3: Whether to upload results to S3
        s3_bucket: S3 bucket for upload
    
    Returns:
        Transformed DataFrame
    """
    stages = [PipelineStage.TRANSFORM]
    
    if upload_to_s3 and s3_bucket:
        stages.append(PipelineStage.UPLOAD_TRANSFORMED)
    
    config = PipelineConfig(
        sentiment_backend=sentiment_backend,
        s3_bucket=s3_bucket,
        stages=stages
    )
    
    pipeline = DataPipeline(config)
    result = pipeline.run(initial_data=data)
    
    # Return the transformed data
    return result.stage_results.get(PipelineStage.TRANSFORM.value, {}).get('data', data)


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Example 1: Full pipeline with all features
    print("\n" + "="*60)
    print("Running Full Pipeline Example")
    print("="*60)
    
    config = PipelineConfig(
        topics=["crypto"],
        s3_bucket="my-financial-data",  # Replace with your bucket
        enable_transformations=True,
        sentiment_backend="vader",
        keyword_method="tfidf",
        save_to_db=False,
        stages=[
            PipelineStage.SCRAPE,
            PipelineStage.UPLOAD_RAW,
            PipelineStage.TRANSFORM,
            PipelineStage.UPLOAD_TRANSFORMED,
        ]
    )
    
    pipeline = DataPipeline(config)
    result = pipeline.run()
    
    print("\nPipeline Result:")
    print(json.dumps(result.to_dict(), indent=2))

