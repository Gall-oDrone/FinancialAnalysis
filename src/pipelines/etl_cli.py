#!/usr/bin/env python3
"""
ETL Command Line Interface

Provides CLI commands for production ETL operations:
- ingest-stocks: Read stocks from PostgreSQL with date filters
- ingest-news: Read news from PostgreSQL with date filters  
- transform-stocks: Apply stock transformations
- transform-news: Apply NLP transformations
- export-genai: Export to JSONL for GenAI/RAG

Usage:
    PYTHONPATH=src python -m pipelines.etl_cli ingest-stocks --since 2026-01-01 --until 2026-01-28
    PYTHONPATH=src python -m pipelines.etl_cli transform-news --date 2026-01-27
    PYTHONPATH=src python -m pipelines.etl_cli export-genai --date 2026-01-27 --output genai.jsonl

    Or after pip install -e .:
    python -m pipelines.etl_cli transform-news --date 2026-01-27
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

import pandas as pd

from config.settings import get_settings
from core.logging import get_logger
from storage.postgres.pgConn import PgConn
from storage.postgres import PostgresSQL_table_queries
from storage.cloud.CloudStorage import CloudStorageProvider

logger = get_logger(__name__)


# ============================================================================
# Ingestion Commands
# ============================================================================

def ingest_stocks(
    since: Optional[str] = None,
    until: Optional[str] = None,
    books: Optional[List[str]] = None,
    output: Optional[str] = None,
    upload_s3: bool = False
) -> pd.DataFrame:
    """
    Ingest stocks from PostgreSQL with optional date filtering.
    
    Args:
        since: Start date (YYYY-MM-DD)
        until: End date (YYYY-MM-DD)
        books: List of book symbols to filter
        output: Optional CSV output path
        upload_s3: Upload to S3
    
    Returns:
        DataFrame with stock data
    """
    logger.info("=" * 60)
    logger.info("INGEST STOCKS")
    logger.info("=" * 60)
    
    # Connect to database
    pg_conn = PgConn(PostgresSQL_table_queries.HISTORICAL_CRYPTO_STOCKS_TABLE_NAME)
    
    # Get data with optional book filter
    logger.info(f"Fetching stocks from database...")
    if books:
        logger.info(f"  Books: {books}")
        df = pg_conn.get_stocks_prices(book_names=books)
    else:
        df = pg_conn.get_stocks_prices()
    
    logger.info(f"Retrieved {len(df)} records")
    
    # Date filtering
    if since or until:
        df['date'] = pd.to_datetime(df['date'])
        
        if since:
            since_date = pd.to_datetime(since)
            df = df[df['date'] >= since_date]
            logger.info(f"Filtered since {since}: {len(df)} records")
        
        if until:
            until_date = pd.to_datetime(until)
            df = df[df['date'] <= until_date]
            logger.info(f"Filtered until {until}: {len(df)} records")
    
    # Save to CSV if requested
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved to {output_path}")
    
    # Upload to S3 if requested
    if upload_s3:
        settings = get_settings()
        if settings.aws.default_bucket:
            aws = CloudStorageProvider.AWS()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"stocks_raw_{timestamp}"
            prefix = "raw/stocks"
            
            aws.upload_dataframe_to_csv(
                df,
                settings.aws.default_bucket,
                file_name,
                prefix
            )
            logger.info(f"Uploaded to s3://{settings.aws.default_bucket}/{prefix}/{file_name}.csv")
    
    pg_conn.close_connection()
    
    logger.info(f"Ingestion complete: {len(df)} records")
    return df


def ingest_news(
    date: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    output: Optional[str] = None,
    upload_s3: bool = False
) -> pd.DataFrame:
    """
    Ingest news from PostgreSQL with optional date filtering.
    
    Args:
        date: Specific date to filter (YYYY-MM-DD)
        since: Start date (YYYY-MM-DD)
        until: End date (YYYY-MM-DD)
        output: Optional CSV output path
        upload_s3: Upload to S3
    
    Returns:
        DataFrame with news data
    """
    logger.info("=" * 60)
    logger.info("INGEST NEWS")
    logger.info("=" * 60)
    
    # Connect to database
    table_name = PostgresSQL_table_queries.FINANCIAL_NEWS_TABLE_NAME
    pg_conn = PgConn(table_name)
    
    # Get data
    logger.info(f"Fetching news from database table: {table_name}...")
    df = pg_conn.get_financial_news()
    
    logger.info(f"Retrieved {len(df)} records")
    
    # Date filtering
    if date or since or until:
        # Convert datetime column to date
        df['date'] = pd.to_datetime(df['datetime']).dt.date
        
        if date:
            filter_date = pd.to_datetime(date).date()
            df = df[df['date'] == filter_date]
            logger.info(f"Filtered for date {date}: {len(df)} records")
        else:
            if since:
                since_date = pd.to_datetime(since).date()
                df = df[df['date'] >= since_date]
                logger.info(f"Filtered since {since}: {len(df)} records")
            
            if until:
                until_date = pd.to_datetime(until).date()
                df = df[df['date'] <= until_date]
                logger.info(f"Filtered until {until}: {len(df)} records")
        
        # Drop temporary date column
        df = df.drop(columns=['date'])
    
    # Save to CSV if requested
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved to {output_path}")
    
    # Upload to S3 if requested
    if upload_s3:
        settings = get_settings()
        if settings.aws.default_bucket:
            aws = CloudStorageProvider.AWS()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"news_raw_{timestamp}"
            prefix = "raw/news"
            
            aws.upload_dataframe_to_csv(
                df,
                settings.aws.default_bucket,
                file_name,
                prefix
            )
            logger.info(f"Uploaded to s3://{settings.aws.default_bucket}/{prefix}/{file_name}.csv")
    
    pg_conn.close_connection()
    
    logger.info(f"Ingestion complete: {len(df)} records")
    return df


# ============================================================================
# Transformation Commands
# ============================================================================

def transform_stocks(
    input_file: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    books: Optional[List[str]] = None,
    output: Optional[str] = None,
    upload_s3: bool = False
) -> pd.DataFrame:
    """
    Transform stock data with returns, volatility, and indicators.
    
    Args:
        input_file: Optional CSV input file (if None, ingests from DB)
        since: Start date for DB ingestion
        until: End date for DB ingestion
        books: Book symbols to filter
        output: Output CSV path
        upload_s3: Upload to S3
    
    Returns:
        Transformed DataFrame
    """
    logger.info("=" * 60)
    logger.info("TRANSFORM STOCKS")
    logger.info("=" * 60)
    
    # Load data
    if input_file:
        logger.info(f"Loading data from {input_file}")
        df = pd.read_csv(input_file)
    else:
        df = ingest_stocks(since=since, until=until, books=books)
    
    # Transform
    from transform.stocks.stock_transformers import StockTransformationPipeline
    
    pipeline = StockTransformationPipeline()
    transformed = pipeline.transform(df)
    
    logger.info(f"Transformation complete: {len(transformed)} records")
    
    # Save output
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        transformed.to_csv(output_path, index=False)
        logger.info(f"Saved transformed data to {output_path}")
    
    # Upload to S3
    if upload_s3:
        settings = get_settings()
        if settings.aws.default_bucket:
            aws = CloudStorageProvider.AWS()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"stocks_transformed_{timestamp}"
            prefix = "transformed/stocks"
            
            aws.upload_dataframe_to_csv(
                transformed,
                settings.aws.default_bucket,
                file_name,
                prefix
            )
            logger.info(f"Uploaded to s3://{settings.aws.default_bucket}/{prefix}/{file_name}.csv")
    
    return transformed


def transform_news(
    input_file: Optional[str] = None,
    date: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    output: Optional[str] = None,
    upload_s3: bool = False,
    sentiment_backend: str = "vader"
) -> pd.DataFrame:
    """
    Transform news data with NLP (sentiment, intent, keywords, tickers).
    
    Args:
        input_file: Optional CSV input file
        date: Specific date to filter
        since: Start date
        until: End date
        output: Output CSV path
        upload_s3: Upload to S3
        sentiment_backend: Sentiment analysis backend
    
    Returns:
        Transformed DataFrame
    """
    logger.info("=" * 60)
    logger.info("TRANSFORM NEWS")
    logger.info("=" * 60)
    
    # Load data
    if input_file:
        logger.info(f"Loading data from {input_file}")
        df = pd.read_csv(input_file)
    else:
        df = ingest_news(date=date, since=since, until=until)
    
    # Transform
    from transform.news.text_transformers import TextTransformationPipeline
    
    pipeline = TextTransformationPipeline(
        sentiment_backend=sentiment_backend,
        extract_tickers=True
    )
    transformed = pipeline.transform(df)
    
    logger.info(f"Transformation complete: {len(transformed)} records")
    
    from pipelines.etl_transform import _prepare_news_dataframe_for_csv
    df_for_csv = _prepare_news_dataframe_for_csv(transformed)
    
    # Save output (one logical row per line for correct column alignment)
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_for_csv.to_csv(output_path, index=False)
        logger.info(f"Saved transformed data to {output_path}")
    
    # Upload to S3
    if upload_s3:
        settings = get_settings()
        if settings.aws.default_bucket:
            aws = CloudStorageProvider.AWS()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"news_transformed_{timestamp}"
            prefix = "transformed/news"
            
            aws.upload_dataframe_to_csv(
                df_for_csv,
                settings.aws.default_bucket,
                file_name,
                prefix
            )
            logger.info(f"Uploaded to s3://{settings.aws.default_bucket}/{prefix}/{file_name}.csv")
    
    return transformed


# ============================================================================
# Export Commands
# ============================================================================

def export_genai(
    input_file: Optional[str] = None,
    date: Optional[str] = None,
    output: Optional[str] = None,
    upload_s3: bool = False,
    include_embeddings: bool = False
) -> int:
    """
    Export news to JSONL format for GenAI/RAG applications.
    
    Args:
        input_file: Transformed news CSV file
        date: Date to filter (if ingesting from DB)
        output: Output JSONL path
        upload_s3: Upload to S3
        include_embeddings: Generate and include embeddings
    
    Returns:
        Number of records exported
    """
    logger.info("=" * 60)
    logger.info("EXPORT FOR GENAI")
    logger.info("=" * 60)
    
    # Load data
    if input_file:
        logger.info(f"Loading data from {input_file}")
        df = pd.read_csv(input_file)
    else:
        # Get transformed data
        logger.info("Fetching and transforming news...")
        df = transform_news(date=date)
    
    # Generate embeddings if requested
    if include_embeddings:
        from export.genai_export import generate_embeddings
        logger.info("Generating embeddings...")
        df = generate_embeddings(df)
    
    # Export to JSONL
    from export.genai_export import export_to_jsonl
    
    if not output:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output = f"output/genai_news_{timestamp}.jsonl"
    
    count = export_to_jsonl(
        df,
        output,
        include_embeddings=include_embeddings
    )
    
    # Upload to S3 if requested
    if upload_s3:
        from export.genai_export import export_to_s3_jsonl
        
        settings = get_settings()
        if settings.aws.default_bucket:
            now = datetime.now()
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            # Use partitioned path similar to CSV exports, but with format=jsonl
            date_path = f"year={now.year}/month={now.month:02}/day={now.day:02}"
            file_name = f"news_genai_{timestamp}"
            prefix = f"genai/news/{date_path}"
            
            export_to_s3_jsonl(
                df,
                settings.aws.default_bucket,
                prefix,
                file_name,
                include_embeddings=include_embeddings
            )
    
    logger.info(f"GenAI export complete: {count} records")
    return count


def export_genai_to_s3_from_db(
    date: Optional[str] = None,
    include_embeddings: bool = False,
) -> str:
    """
    End-to-end helper: extract news from Postgres, transform, export to JSONL, and upload to S3.

    This is equivalent to running:
      1) ingest-news/transform-news for the given date
      2) export to JSONL
      3) upload the JSONL file to S3 under a partitioned path with format=jsonl.

    Returns:
        S3 URI of the uploaded JSONL file.
    """
    # Reuse transform_news so we leverage the existing ETL transformation logic.
    df = transform_news(date=date)

    if include_embeddings:
        from export.genai_export import generate_embeddings
        logger.info("Generating embeddings for GenAI export...")
        df = generate_embeddings(df)

    from export.genai_export import export_to_s3_jsonl

    settings = get_settings()
    bucket = settings.aws.default_bucket
    if not bucket:
        raise RuntimeError("AWS default bucket is not configured; cannot export GenAI JSONL to S3.")

    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    date_path = f"year={now.year}/month={now.month:02}/day={now.day:02}"
    prefix = f"genai/news/{date_path}"
    file_name = f"news_genai_{timestamp}"

    logger.info("Exporting GenAI JSONL to S3 from DB (date=%s)", date)
    s3_uri = export_to_s3_jsonl(
        df,
        bucket_name=bucket,
        prefix_path=prefix,
        file_name=file_name,
        include_embeddings=include_embeddings,
    )
    logger.info("GenAI JSONL export complete: %s", s3_uri)
    return s3_uri


# ============================================================================
# CLI Setup
# ============================================================================

def parse_book_list(value: str) -> List[str]:
    """Parse comma-separated book list."""
    return [b.strip() for b in value.split(',') if b.strip()]


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='ETL Command Line Interface for Financial Data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest stocks for date range
  python -m pipelines.etl_cli ingest-stocks --since 2026-01-01 --until 2026-01-28
  
  # Ingest specific books
  python -m pipelines.etl_cli ingest-stocks --books btc-usd,eth-usd
  
  # Transform stocks and upload to S3
  python -m pipelines.etl_cli transform-stocks --since 2026-01-01 --upload-s3
  
  # Ingest today's news
  python -m pipelines.etl_cli ingest-news --date 2026-01-27
  
  # Transform news with specific sentiment backend
  python -m pipelines.etl_cli transform-news --sentiment vader
  
  # Export for GenAI with embeddings
  python -m pipelines.etl_cli export-genai --date 2026-01-27 --embeddings
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Ingest stocks command
    ingest_stocks_parser = subparsers.add_parser(
        'ingest-stocks',
        help='Ingest stocks from PostgreSQL'
    )
    ingest_stocks_parser.add_argument('--since', help='Start date (YYYY-MM-DD)')
    ingest_stocks_parser.add_argument('--until', help='End date (YYYY-MM-DD)')
    ingest_stocks_parser.add_argument('--books', type=parse_book_list, help='Comma-separated book list')
    ingest_stocks_parser.add_argument('--output', help='Output CSV file path')
    ingest_stocks_parser.add_argument('--upload-s3', action='store_true', help='Upload to S3')
    
    # Ingest news command
    ingest_news_parser = subparsers.add_parser(
        'ingest-news',
        help='Ingest news from PostgreSQL'
    )
    ingest_news_parser.add_argument('--date', help='Specific date (YYYY-MM-DD)')
    ingest_news_parser.add_argument('--since', help='Start date (YYYY-MM-DD)')
    ingest_news_parser.add_argument('--until', help='End date (YYYY-MM-DD)')
    ingest_news_parser.add_argument('--output', help='Output CSV file path')
    ingest_news_parser.add_argument('--upload-s3', action='store_true', help='Upload to S3')
    
    # Transform stocks command
    transform_stocks_parser = subparsers.add_parser(
        'transform-stocks',
        help='Transform stock data with indicators'
    )
    transform_stocks_parser.add_argument('--input', help='Input CSV file (if not ingesting)')
    transform_stocks_parser.add_argument('--since', help='Start date for ingestion')
    transform_stocks_parser.add_argument('--until', help='End date for ingestion')
    transform_stocks_parser.add_argument('--books', type=parse_book_list, help='Book symbols')
    transform_stocks_parser.add_argument('--output', help='Output CSV file path')
    transform_stocks_parser.add_argument('--upload-s3', action='store_true', help='Upload to S3')
    
    # Transform news command
    transform_news_parser = subparsers.add_parser(
        'transform-news',
        help='Transform news with NLP (sentiment, intent, keywords)'
    )
    transform_news_parser.add_argument('--input', help='Input CSV file')
    transform_news_parser.add_argument('--date', help='Specific date for ingestion')
    transform_news_parser.add_argument('--since', help='Start date for ingestion')
    transform_news_parser.add_argument('--until', help='End date for ingestion')
    transform_news_parser.add_argument('--sentiment', default='vader', choices=['vader', 'textblob', 'transformers'])
    transform_news_parser.add_argument('--output', help='Output CSV file path')
    transform_news_parser.add_argument('--upload-s3', action='store_true', help='Upload to S3')
    
    # Export GenAI command
    export_genai_parser = subparsers.add_parser(
        'export-genai',
        help='Export to JSONL for GenAI/RAG'
    )
    export_genai_parser.add_argument('--input', help='Transformed news CSV')
    export_genai_parser.add_argument('--date', help='Date to filter')
    export_genai_parser.add_argument('--output', help='Output JSONL file path')
    export_genai_parser.add_argument('--embeddings', action='store_true', help='Generate embeddings')
    export_genai_parser.add_argument('--upload-s3', action='store_true', help='Upload to S3')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        # Execute command
        if args.command == 'ingest-stocks':
            ingest_stocks(
                since=args.since,
                until=args.until,
                books=args.books,
                output=args.output,
                upload_s3=args.upload_s3
            )
        
        elif args.command == 'ingest-news':
            ingest_news(
                date=args.date,
                since=args.since,
                until=args.until,
                output=args.output,
                upload_s3=args.upload_s3
            )
        
        elif args.command == 'transform-stocks':
            transform_stocks(
                input_file=args.input,
                since=args.since,
                until=args.until,
                books=args.books,
                output=args.output,
                upload_s3=args.upload_s3
            )
        
        elif args.command == 'transform-news':
            transform_news(
                input_file=args.input,
                date=args.date,
                since=args.since,
                until=args.until,
                output=args.output,
                upload_s3=args.upload_s3,
                sentiment_backend=args.sentiment
            )
        
        elif args.command == 'export-genai':
            export_genai(
                input_file=args.input,
                date=args.date,
                output=args.output,
                upload_s3=args.upload_s3,
                include_embeddings=args.embeddings
            )
        
        logger.info("\n" + "=" * 60)
        logger.info("SUCCESS")
        logger.info("=" * 60)
        return 0
        
    except Exception as e:
        logger.error(f"Command failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
