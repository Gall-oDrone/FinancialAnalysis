"""
ETL Transform Pipeline: Transform data from Postgres, save back to Postgres, publish to S3.

- News: per-article transform; S3 upload per-article and/or batch (run, week, month, year).
- Stocks: per-book transform with warmup window; S3 upload per book/day.

S3 path conventions:
  News per-article: s3://{bucket}/news/crypto/[agentic=true|false/]year=.../format=csv/{id}.csv
  News batch:       s3://{bucket}/news/transformed/crypto/[agentic=true|false/]batch=run|year=... (run/week/month/year)
  Stocks:           s3://{bucket}/stocks/crypto/book={book}/year=YYYY/month=MM/day=DD/format=csv/YYYYMMDD-{book}.csv
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from core.logging import get_logger

logger = get_logger(__name__)

# S3 path builders
NEWS_PREFIX = "news/crypto"
NEWS_TRANSFORMED_PREFIX = "news/transformed/crypto"
STOCKS_PREFIX = "stocks/crypto"


def build_s3_key_news_per_article(
    article_id: str,
    dt: datetime,
    prefix: str = NEWS_PREFIX,
    agentic: Optional[bool] = None,
) -> str:
    """Build S3 key for a single transformed news article CSV.
    If agentic is True/False, inserts agentic=true/ or agentic=false/ in the path.
    """
    agentic_seg = ""
    if agentic is not None:
        agentic_seg = f"agentic={'true' if agentic else 'false'}/"
    return (
        f"{prefix}/{agentic_seg}year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        f"/hour={dt.hour:02d}/minute={dt.minute:02d}/second={dt.second:02d}"
        f"/format=csv/{article_id}.csv"
    )


def build_s3_key_news_batch(
    batch_type: str,  # "run" | "week" | "month" | "year"
    dt: datetime,
    suffix: str = "news_transformed",
    prefix: str = NEWS_TRANSFORMED_PREFIX,
    agentic: Optional[bool] = None,
) -> str:
    """Build S3 key for a batch transformed news CSV.
    If agentic is True/False, inserts agentic=true/ or agentic=false/ in the path.
    """
    agentic_seg = ""
    if agentic is not None:
        agentic_seg = f"agentic={'true' if agentic else 'false'}/"
    if batch_type == "run":
        ts = dt.strftime("%Y%m%d_%H%M%S")
        return f"{prefix}/{agentic_seg}batch=run/format=csv/{suffix}_{ts}.csv"
    if batch_type == "week":
        iso = dt.isocalendar()
        return f"{prefix}/{agentic_seg}year={dt.year}/week={iso[1]:02d}/format=csv/{suffix}_y{dt.year}_w{iso[1]:02d}.csv"
    if batch_type == "month":
        return f"{prefix}/{agentic_seg}year={dt.year}/month={dt.month:02d}/format=csv/{suffix}_y{dt.year}_m{dt.month:02d}.csv"
    if batch_type == "year":
        return f"{prefix}/{agentic_seg}year={dt.year}/format=csv/{suffix}_y{dt.year}.csv"
    raise ValueError(f"batch_type must be one of run, week, month, year; got {batch_type}")


def build_s3_key_stocks(
    book: str,
    date_val,
    prefix: str = STOCKS_PREFIX,
) -> str:
    """Build S3 key for transformed stocks CSV (one file per book per day)."""
    dt = pd.to_datetime(date_val)
    date_str = dt.strftime("%Y%m%d")
    return (
        f"{prefix}/book={str(book).lower()}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        f"/format=csv/{date_str}-{str(book).lower()}.csv"
    )


def _serialize_row_for_news_db(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert list/dict fields to JSON strings for Postgres JSONB."""
    out = dict(row)
    for key in ("tickers", "secondary_intents", "keywords", "entities", "llm_themes", "llm_entities", "llm_financial_metrics"):
        if key in out and out[key] is not None:
            v = out[key]
            if isinstance(v, (list, dict)):
                out[key] = json.dumps(v)
    return out


def _ensure_news_transformed_table(pg_conn) -> None:
    """Ensure financial_news_transformed table and indexes exist."""
    from storage.postgres import PostgresSQL_table_queries as q
    pg_conn.set_table(q.FINANCIAL_NEWS_TRANSFORMED_TABLE_NAME)
    pg_conn.create_table(q.FINANCIAL_NEWS_TRANSFORMED_TABLE_QUERY)
    try:
        cursor = pg_conn.connection.cursor()
        for stmt in q.FINANCIAL_NEWS_TRANSFORMED_INDEXES.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cursor.execute(stmt)
        pg_conn.connection.commit()
        cursor.close()
    except Exception as e:
        logger.warning("Could not create transformed news indexes: %s", e)


def _ensure_stocks_processed_table(pg_conn) -> None:
    """Ensure historical_processed table and indexes exist."""
    from storage.postgres import PostgresSQL_table_queries as q
    pg_conn.set_table(q.HISTORICAL_PROCESSED_TABLE_NAME)
    pg_conn.create_table(q.HISTORICAL_PROCESSED_TABLE_QUERY)
    try:
        cursor = pg_conn.connection.cursor()
        for stmt in q.HISTORICAL_PROCESSED_INDEXES.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cursor.execute(stmt)
        pg_conn.connection.commit()
        cursor.close()
    except Exception as e:
        logger.warning("Could not create processed stocks indexes: %s", e)


def save_transformed_news_to_postgres(
    pg_conn,
    transformed_df: pd.DataFrame,
    table_name: Optional[str] = None,
    agentic_enabled: bool = False,
) -> int:
    """
    Save transformed news DataFrame to Postgres (financial_news_transformed).
    Maps pipeline output columns to DB columns and serializes JSONB fields.
    When agentic_enabled is True, llm_* columns are written; otherwise they are NULL.
    Returns number of rows saved.
    """
    from storage.postgres import PostgresSQL_table_queries as q
    name = table_name or q.FINANCIAL_NEWS_TRANSFORMED_TABLE_NAME
    pg_conn.set_table(name)
    # DB columns (excluding created_at, updated_at - use defaults)
    db_columns = [
        "id", "source", "headline", "href", "summary", "content", "datetime",
        "cleaned_text", "word_count", "tickers",
        "sentiment_label", "sentiment_score", "positive_score", "negative_score", "neutral_score",
        "primary_intent", "intent_confidence", "secondary_intents",
        "keywords", "entities",
        "llm_summary", "llm_themes", "llm_entities", "llm_financial_metrics", "llm_error", "agentic_enabled",
    ]
    saved = 0
    for _, row in transformed_df.iterrows():
        row_dict = {}
        for col in db_columns:
            if col in row.index:
                val = row[col]
                if isinstance(val, (list, dict)):
                    val = json.dumps(val)
                if pd.isna(val):
                    val = None
                row_dict[col] = val
            elif col == "agentic_enabled":
                row_dict[col] = agentic_enabled
        if not row_dict.get("id"):
            continue
        # Ensure agentic_enabled is set from param when not in row
        row_dict["agentic_enabled"] = agentic_enabled
        row_dict = _serialize_row_for_news_db(row_dict)
        try:
            pg_conn.save_to_postgres(row_dict, list(row_dict.keys()))
            saved += 1
        except Exception as e:
            logger.debug("Skip save row %s: %s", row_dict.get("id"), e)
    return saved


def save_transformed_stocks_to_postgres(
    pg_conn,
    transformed_df: pd.DataFrame,
    table_name: Optional[str] = None,
) -> int:
    """
    Upsert transformed stocks into historical_processed.
    Returns number of rows upserted.
    """
    from psycopg2.extras import execute_values
    from storage.postgres import PostgresSQL_table_queries as q
    name = table_name or q.HISTORICAL_PROCESSED_TABLE_NAME
    pg_conn.set_table(name)
    cols = [
        "reference", "book", "date", "open", "high", "low", "close", "adj_close", "volume",
        "simple_return", "log_return",
        "volatility_20d", "volatility_60d", "volatility_parkinson", "volatility_gk",
        "sma_20", "sma_50", "sma_200", "ema_12", "ema_26",
        "rsi_14", "macd", "macd_signal", "macd_histogram", "bb_upper", "bb_middle", "bb_lower",
    ]
    # Align with pgConn.get_stocks_prices columns: ref, book, date, ...
    col_map = {"ref": "reference"}
    df = transformed_df.copy()
    if "ref" in df.columns and "reference" not in df.columns:
        df["reference"] = df["ref"]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    available = [c for c in cols if c in df.columns]
    if not available:
        logger.warning("No matching columns for historical_processed")
        return 0
    values = []
    for _, r in df.iterrows():
        row = []
        for c in cols:
            if c in df.columns:
                v = r[c]
                if pd.isna(v):
                    row.append(None)
                else:
                    row.append(v)
            else:
                row.append(None)
        values.append(tuple(row))
    if not values:
        return 0
    try:
        cursor = pg_conn.connection.cursor()
        template = "(" + ", ".join(["%s"] * len(cols)) + ")"
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in ("book", "date"))
        insert_sql = (
            f"INSERT INTO {name} ({', '.join(cols)}) VALUES %s "
            f"ON CONFLICT (book, date) DO UPDATE SET {update_set}"
        )
        execute_values(cursor, insert_sql, values, template=template, page_size=500)
        pg_conn.connection.commit()
        cursor.close()
        return len(values)
    except Exception as e:
        logger.exception("Upsert stocks failed: %s", e)
        if pg_conn.connection:
            pg_conn.connection.rollback()
        return 0


def upload_dataframe_to_s3_key(
    s3_client,
    bucket: str,
    key: str,
    df: pd.DataFrame,
    content_type: str = "text/csv",
) -> None:
    """Upload a DataFrame as CSV to S3 at the given key."""
    from io import StringIO
    buf = StringIO()
    df.to_csv(buf, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType=content_type,
    )


def run_news_etl(
    since: Optional[str] = None,
    until: Optional[str] = None,
    date: Optional[str] = None,
    *,
    news_bucket: Optional[str] = None,
    save_to_postgres: bool = True,
    upload_s3_per_article: bool = False,
    upload_s3_batch: Optional[List[str]] = None,  # e.g. ["run", "week", "month", "year"]
    sentiment_backend: str = "vader",
    extract_tickers: bool = True,
    agentic_enabled: bool = False,
    pg_conn=None,
) -> pd.DataFrame:
    """
    Ingest news from Postgres, transform with TextTransformationPipeline,
    optionally save to financial_news_transformed, then upload to S3.

    When agentic_enabled is True, llm_* columns are persisted and S3 paths include agentic=true/.
    S3: per-article keys and/or batch keys (run, week, month, year) under news bucket.
    """
    from config.settings import get_settings
    from storage.postgres.pgConn import PgConn
    from storage.postgres import PostgresSQL_table_queries as q
    from storage.cloud.CloudStorage import CloudStorageProvider
    from pipelines.etl_cli import ingest_news
    from transform.news.text_transformers import TextTransformationPipeline

    settings = get_settings()
    bucket = news_bucket or getattr(settings.aws, "news_bucket", None) or settings.aws.default_bucket
    if not bucket and (upload_s3_per_article or upload_s3_batch):
        logger.warning("No news S3 bucket configured; skipping S3 uploads")

    df = ingest_news(date=date, since=since, until=until)
    if df is None or df.empty:
        logger.warning("No news data to transform")
        return pd.DataFrame()

    pipeline = TextTransformationPipeline(
        sentiment_backend=sentiment_backend,
        extract_tickers=extract_tickers,
    )
    transformed = pipeline.transform(df)
    logger.info("Transformed %d news articles", len(transformed))

    if save_to_postgres:
        conn = pg_conn or PgConn(q.FINANCIAL_NEWS_TABLE_NAME)
        _ensure_news_transformed_table(conn)
        n = save_transformed_news_to_postgres(conn, transformed, agentic_enabled=agentic_enabled)
        logger.info("Saved %d rows to %s", n, q.FINANCIAL_NEWS_TRANSFORMED_TABLE_NAME)
        if pg_conn is None:
            conn.close_connection()

    if (upload_s3_per_article or upload_s3_batch) and bucket:
        aws = CloudStorageProvider.AWS()
        if upload_s3_per_article:
            for _, row in transformed.iterrows():
                aid = str(row.get("id", ""))
                dt_str = row.get("datetime")
                try:
                    dt = pd.to_datetime(dt_str) if dt_str else datetime.utcnow()
                except Exception:
                    dt = datetime.utcnow()
                key = build_s3_key_news_per_article(aid, dt, agentic=agentic_enabled)
                one = pd.DataFrame([row])
                upload_dataframe_to_s3_key(aws.s3_client, bucket, key, one)
            logger.info("Uploaded %d per-article CSVs to s3://%s/", len(transformed), bucket)
        if upload_s3_batch:
            now = datetime.utcnow()
            for batch_type in upload_s3_batch:
                key = build_s3_key_news_batch(batch_type, now, agentic=agentic_enabled)
                upload_dataframe_to_s3_key(aws.s3_client, bucket, key, transformed)
                logger.info("Uploaded batch %s to s3://%s/%s", batch_type, bucket, key)

    return transformed


def run_stocks_etl(
    since: str,
    until: Optional[str] = None,
    books: Optional[List[str]] = None,
    warmup_days: int = 252,
    *,
    stocks_bucket: Optional[str] = None,
    save_to_postgres: bool = True,
    upload_s3: bool = True,
    pg_conn=None,
) -> pd.DataFrame:
    """
    Ingest stocks from Postgres, transform per book with warmup window,
    save to historical_processed, then upload per book/day to S3.
    """
    from config.settings import get_settings
    from storage.postgres.pgConn import PgConn
    from storage.postgres import PostgresSQL_table_queries as q
    from storage.cloud.CloudStorage import CloudStorageProvider
    from pipelines.etl_cli import ingest_stocks
    from transform.stocks.stock_transformers import StockTransformationPipeline

    settings = get_settings()
    bucket = stocks_bucket or getattr(settings.aws, "stocks_bucket", None) or settings.aws.default_bucket
    if not bucket and upload_s3:
        logger.warning("No stocks S3 bucket configured; skipping S3 upload")

    since_dt = pd.to_datetime(since)
    until_dt = pd.to_datetime(until) if until else since_dt
    warmup_start = since_dt - pd.Timedelta(days=warmup_days)
    warmup_start_str = warmup_start.strftime("%Y-%m-%d")
    until_str = until_dt.strftime("%Y-%m-%d") if until else since

    df = ingest_stocks(since=warmup_start_str, until=until_str, books=books)
    if df is None or df.empty:
        logger.warning("No stock data to transform")
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    pipeline = StockTransformationPipeline()
    transformed = pipeline.transform(df, price_col="close", group_by="book")
    # Keep only requested date range for output
    mask = (transformed["date"] >= since_dt) & (transformed["date"] <= until_dt)
    transformed = transformed.loc[mask].copy()
    logger.info("Transformed %d stock records (%s to %s)", len(transformed), since, until_str)

    if save_to_postgres:
        conn = pg_conn or PgConn(q.HISTORICAL_CRYPTO_STOCKS_TABLE_NAME)
        _ensure_stocks_processed_table(conn)
        n = save_transformed_stocks_to_postgres(conn, transformed)
        logger.info("Saved %d rows to %s", n, q.HISTORICAL_PROCESSED_TABLE_NAME)
        if pg_conn is None:
            conn.close_connection()

    if upload_s3 and bucket:
        aws = CloudStorageProvider.AWS()
        grouped = transformed.groupby(["book", "date"])
        for (book, date_val), group in grouped:
            key = build_s3_key_stocks(book, date_val)
            upload_dataframe_to_s3_key(aws.s3_client, bucket, key, group)
        logger.info("Uploaded %d group files to s3://%s/", grouped.ngroups, bucket)

    return transformed
