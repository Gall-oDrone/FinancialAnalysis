"""
Application settings and configuration management.

This module handles loading configuration from environment variables
with sensible defaults.
"""

import os
from typing import Optional
from functools import lru_cache

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class DatabaseConfig:
    """PostgreSQL database configuration."""

    def __init__(self):
        self.dbname: str = os.getenv("PGDBNAME", "financial_db")
        self.user: str = os.getenv("PGDBUSER", "postgres")
        self.password: Optional[str] = os.getenv("PGDBPASS")
        self.host: str = os.getenv("PGDBHOST", "localhost")
        self.port: str = os.getenv("PGDBPORT", "5432")

    def __repr__(self) -> str:
        return (
            f"DatabaseConfig(dbname={self.dbname}, "
            f"user={self.user}, host={self.host}, port={self.port})"
        )


class AWSConfig:
    """AWS S3 configuration."""

    def __init__(self):
        self.access_key_id: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_access_key: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.default_bucket: Optional[str] = os.getenv("AWS_DEFAULT_BUCKET")

    def __repr__(self) -> str:
        return f"AWSConfig(region={self.region}, bucket={self.default_bucket})"


class LoggingConfig:
    """Logging configuration."""

    def __init__(self):
        self.level: str = os.getenv("LOG_LEVEL", "INFO").upper()
        self.format: str = os.getenv(
            "LOG_FORMAT",
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.file: Optional[str] = os.getenv("LOG_FILE", "logs/app.log")
        self.max_bytes: int = int(os.getenv("LOG_MAX_BYTES", "10485760"))  # 10MB
        self.backup_count: int = int(os.getenv("LOG_BACKUP_COUNT", "5"))

    def __repr__(self) -> str:
        return f"LoggingConfig(level={self.level}, file={self.file})"


class ScrapingConfig:
    """Web scraping configuration."""

    def __init__(self):
        self.debug: bool = os.getenv("SCRAPING_DEBUG", "False").lower() == "true"
        self.headless: bool = os.getenv("SCRAPING_HEADLESS", "True").lower() == "true"
        self.timeout: int = int(os.getenv("SCRAPING_TIMEOUT", "30"))
        self.user_agent: str = os.getenv(
            "SCRAPING_USER_AGENT",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/114.0.5735.90 Safari/537.36",
        )

    def __repr__(self) -> str:
        return f"ScrapingConfig(debug={self.debug}, headless={self.headless})"


class MLConfig:
    """Machine Learning and NLP configuration."""

    def __init__(self):
        # Sentiment analysis settings
        self.sentiment_backend: str = os.getenv("ML_SENTIMENT_BACKEND", "vader")
        # Options: vader (fast), textblob (fast), transformers (accurate, slow)
        
        # Intent extraction settings
        self.use_transformer_intents: bool = os.getenv(
            "ML_USE_TRANSFORMER_INTENTS", "False"
        ).lower() == "true"
        
        # Keyword extraction settings
        self.keyword_method: str = os.getenv("ML_KEYWORD_METHOD", "tfidf")
        # Options: tfidf, spacy, rake
        self.keyword_top_n: int = int(os.getenv("ML_KEYWORD_TOP_N", "10"))
        
        # Model paths (for custom models)
        self.sentiment_model_path: Optional[str] = os.getenv("ML_SENTIMENT_MODEL_PATH")
        self.spacy_model: str = os.getenv("ML_SPACY_MODEL", "en_core_web_sm")

    def __repr__(self) -> str:
        return (
            f"MLConfig(sentiment_backend={self.sentiment_backend}, "
            f"keyword_method={self.keyword_method})"
        )


class PipelineConfig:
    """Data pipeline configuration."""

    def __init__(self):
        # Pipeline behavior
        self.default_topics: list = os.getenv(
            "PIPELINE_DEFAULT_TOPICS", "crypto"
        ).split(",")
        self.enrich_full_content: bool = os.getenv(
            "PIPELINE_ENRICH_CONTENT", "True"
        ).lower() == "true"
        self.continue_on_error: bool = os.getenv(
            "PIPELINE_CONTINUE_ON_ERROR", "True"
        ).lower() == "true"
        
        # S3 paths
        self.s3_raw_prefix: str = os.getenv("PIPELINE_S3_RAW_PREFIX", "raw/news")
        self.s3_transformed_prefix: str = os.getenv(
            "PIPELINE_S3_TRANSFORMED_PREFIX", "transformed/news"
        )
        
        # Database settings
        self.db_table_name: str = os.getenv(
            "PIPELINE_DB_TABLE", "financial_news_transformed"
        )

    def __repr__(self) -> str:
        return f"PipelineConfig(topics={self.default_topics})"


class Settings:
    """Main application settings."""

    def __init__(self):
        self.database = DatabaseConfig()
        self.aws = AWSConfig()
        self.logging = LoggingConfig()
        self.scraping = ScrapingConfig()
        self.ml = MLConfig()
        self.pipeline = PipelineConfig()
        self.debug: bool = os.getenv("DEBUG", "False").lower() == "true"
        self.environment: str = os.getenv("ENVIRONMENT", "development")

    def __repr__(self) -> str:
        return f"Settings(environment={self.environment}, debug={self.debug})"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance.

    Returns:
        Settings: Application settings instance
    """
    return Settings()
