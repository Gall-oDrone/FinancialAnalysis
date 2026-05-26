"""
ETL Transform Pipeline: Transform data from Postgres, save back to Postgres, publish to S3.

- News: per-article transform; S3 upload per-article and/or batch (run, week, month, year).
- Stocks: per-book transform with warmup window; S3 upload per book/day.

S3 path conventions:
  News per-article: s3://{bucket}/news/crypto/[agentic=true|false/]year=.../format=csv/{id}.csv
  News batch:       s3://{bucket}/news/transformed/crypto/[agentic=true|false/]batch=run|year=... (run; year/month/week/day partitioned by article date)
  Stocks (raw):     s3://{bucket}/stocks/crypto/book={book}/...
  Stocks (transformed): s3://{bucket}/stocks/transformed/crypto/book={book}/year=YYYY/month=MM/day=DD/format=csv/YYYYMMDD-{book}.csv
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from core.logging import get_logger

logger = get_logger(__name__)

# S3 path builders
NEWS_PREFIX = "news/crypto"
NEWS_TRANSFORMED_PREFIX = "news/transformed/crypto"
STOCKS_PREFIX = "stocks/crypto"
STOCKS_TRANSFORMED_PREFIX = "stocks/transformed/crypto"


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
    batch_type: str,  # "run" | "week" | "month" | "year" | "day"
    dt: datetime,
    suffix: str = "news_transformed",
    prefix: str = NEWS_TRANSFORMED_PREFIX,
    agentic: Optional[bool] = None,
) -> str:
    """Build S3 key for a batch transformed news CSV.
    If agentic is True/False, inserts agentic=true/ or agentic=false/ in the path.
    For year/month/week/day, dt is the partition date (article date); for run, dt is run time.
    """
    agentic_seg = ""
    if agentic is not None:
        agentic_seg = f"agentic={'true' if agentic else 'false'}/"
    if batch_type == "run":
        ts = dt.strftime("%Y%m%d_%H%M%S")
        return f"{prefix}/{agentic_seg}batch=run/format=csv/{suffix}_{ts}.csv"
    if batch_type == "week":
        # Use calendar year and Monday-based week (0-53) so 2024-12-30/31 → y2024_w52, not y2025_w01
        week_num = int(dt.strftime("%W"))
        return f"{prefix}/{agentic_seg}year={dt.year}/week={week_num:02d}/format=csv/{suffix}_y{dt.year}_w{week_num:02d}.csv"
    if batch_type == "month":
        return f"{prefix}/{agentic_seg}year={dt.year}/month={dt.month:02d}/format=csv/{suffix}_y{dt.year}_m{dt.month:02d}.csv"
    if batch_type == "year":
        return f"{prefix}/{agentic_seg}year={dt.year}/format=csv/{suffix}_y{dt.year}.csv"
    if batch_type == "day":
        return (
            f"{prefix}/{agentic_seg}year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
            f"/format=csv/{suffix}_y{dt.year}_m{dt.month:02d}_d{dt.day:02d}.csv"
        )
    raise ValueError(f"batch_type must be one of run, week, month, year, day; got {batch_type}")


def build_s3_key_stocks(
    book: str,
    date_val,
    prefix: str = STOCKS_TRANSFORMED_PREFIX,
) -> str:
    """Build S3 key for transformed stocks CSV (one file per book per day)."""
    dt = pd.to_datetime(date_val)
    date_str = dt.strftime("%Y%m%d")
    return (
        f"{prefix}/book={str(book).lower()}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        f"/format=csv/{date_str}-{str(book).lower()}.csv"
    )


def build_s3_key_stocks_batch(
    book: str,
    dt: datetime,
    batch_type: str,  # "run" | "week" | "month" | "year" | "day"
    prefix: str = STOCKS_TRANSFORMED_PREFIX,
) -> str:
    """Build S3 key for a batch transformed stocks CSV (one file per book per partition).
    For run, book is ignored and one key is returned for the whole run (caller may pass empty string).
    """
    book_lower = str(book).lower() if book else ""
    if batch_type == "run":
        ts = dt.strftime("%Y%m%d_%H%M%S")
        return f"{prefix}/batch=run/format=csv/stocks_{ts}.csv"
    if batch_type == "week":
        week_num = int(dt.strftime("%W"))
        return (
            f"{prefix}/book={book_lower}/year={dt.year}/week={week_num:02d}/format=csv/"
            f"y{dt.year}_w{week_num:02d}-{book_lower}.csv"
        )
    if batch_type == "month":
        return (
            f"{prefix}/book={book_lower}/year={dt.year}/month={dt.month:02d}/format=csv/"
            f"y{dt.year}_m{dt.month:02d}-{book_lower}.csv"
        )
    if batch_type == "year":
        return f"{prefix}/book={book_lower}/year={dt.year}/format=csv/y{dt.year}-{book_lower}.csv"
    if batch_type == "day":
        date_str = dt.strftime("%Y%m%d")
        return (
            f"{prefix}/book={book_lower}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
            f"/format=csv/{date_str}-{book_lower}.csv"
        )
    raise ValueError(f"batch_type must be one of run, week, month, year, day; got {batch_type}")


def _serialize_row_for_news_db(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert list/dict fields to JSON strings for Postgres JSONB."""
    out = dict(row)
    for key in ("tickers", "secondary_intents", "keywords", "entities", "llm_themes", "llm_entities", "llm_financial_metrics", "llm_sectors", "llm_key_facts"):
        if key in out and out[key] is not None:
            v = out[key]
            if isinstance(v, (list, dict)):
                out[key] = json.dumps(v)
    return out


# Columns that may be list/dict and must be serialized to a single string for CSV (avoids commas/newlines breaking columns)
_CSV_JSON_COLUMNS = (
    "tickers", "secondary_intents", "keywords", "entities",
    "llm_themes", "llm_entities", "llm_financial_metrics", "llm_sectors", "llm_key_facts",
)


def _prepare_news_dataframe_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a transformed-news DataFrame for CSV export so each logical row is one physical line.
    - Formats ``datetime`` as ``YYYY-MM-DD``.
    - Serializes list/dict columns to compact JSON (no newlines).
    - Replaces newlines and carriage returns in string columns with a space.
    This prevents misalignment when opening in Excel or other tools that do not handle quoted newlines.
    """
    from storage.postgres.news_dataframe import format_news_datetime_for_export

    out = df.copy()
    if "datetime" in out.columns:
        out["datetime"] = out["datetime"].apply(format_news_datetime_for_export)
    for col in out.columns:
        if col in _CSV_JSON_COLUMNS:
            out[col] = out[col].apply(
                lambda x: json.dumps(x, separators=(",", ":")) if isinstance(x, (list, dict)) else x
            )
    for col in out.columns:
        ser = out[col]
        if ser.dtype == object or ser.dtype.name == "string":
            out[col] = ser.astype(str).str.replace("\r\n", " ", regex=False).str.replace("\n", " ", regex=False).str.replace("\r", " ", regex=False)
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


def agentic_result_has_failures(transformed_df: pd.DataFrame) -> bool:
    """
    Return True if the agentic enrichment produced any per-row errors (llm_error set).
    Use for reporting or filtering; all rows (including those with llm_error) are
    typically saved so partial results are not lost.
    """
    if transformed_df is None or transformed_df.empty:
        return False
    if "llm_error" not in transformed_df.columns:
        return False
    # Treat only non-empty strings as errors (robust to None/pd.NA/NaN across pandas versions)
    err = transformed_df["llm_error"].fillna("").astype(str).str.strip()
    return (err != "").any()


def save_transformed_news_to_postgres(
    pg_conn,
    transformed_df: pd.DataFrame,
    table_name: Optional[str] = None,
    agentic_enabled: bool = False,
) -> int:
    """
    Save transformed news DataFrame to Postgres (financial_news_transformed).
    Each row is keyed by id (article id): one row per article, so transformations
    are joined to the source article by id.
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
        "llm_ticker", "llm_event_type", "llm_overall_sentiment", "llm_forward_sentiment",
        "llm_surprise_score", "llm_risk_score", "llm_uncertainty_score", "llm_impact_strength",
        "llm_immediacy", "llm_impact_horizon", "llm_confidence", "llm_novelty_score", "llm_sentiment_label",
        "llm_impact_level", "llm_signal", "llm_actionable", "llm_sectors", "llm_key_facts",
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
    if content_type == "text/csv" and not df.empty:
        df = _prepare_news_dataframe_for_csv(df)
    buf = StringIO()
    df.to_csv(buf, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType=content_type,
    )


# Batch types that are partitioned by article (data) date; "run" uses run time.
_DATA_PARTITION_BATCH_TYPES = ("year", "month", "week", "day")


def _group_stocks_by_partition(
    transformed_df: pd.DataFrame,
    batch_type: str,
):
    """Yield (book, partition_dt, sub_df) for each (book, partition). batch_type in ('year','month','week','day').
    Partition is derived from stock date column.
    """
    if batch_type not in _DATA_PARTITION_BATCH_TYPES or transformed_df.empty:
        return
    if "date" not in transformed_df.columns or "book" not in transformed_df.columns:
        return
    df = transformed_df.copy()
    df["_date"] = pd.to_datetime(df["date"], errors="coerce")
    valid = df["_date"].notna()
    if not valid.any():
        return
    subset = df.loc[valid]
    d = subset["_date"]
    for book, book_df in subset.groupby("book"):
        bd = d.loc[book_df.index]
        if batch_type == "year":
            for year, group in book_df.groupby(bd.dt.year):
                part_dt = datetime(int(year), 1, 1)
                yield book, part_dt, group.drop(columns=["_date"], errors="ignore")
        elif batch_type == "month":
            for (year, month), group in book_df.groupby([bd.dt.year, bd.dt.month]):
                part_dt = datetime(int(year), int(month), 1)
                yield book, part_dt, group.drop(columns=["_date"], errors="ignore")
        elif batch_type == "week":
            week_num = bd.dt.strftime("%W").astype(int)
            for (year, week), group in book_df.groupby([bd.dt.year, week_num]):
                part_dt = group["_date"].min()
                if pd.isna(part_dt):
                    part_dt = datetime(int(year), 1, 1)
                else:
                    part_dt = part_dt.to_pydatetime() if hasattr(part_dt, "to_pydatetime") else part_dt
                yield book, part_dt, group.drop(columns=["_date"], errors="ignore")
        elif batch_type == "day":
            for (year, month, day), group in book_df.groupby([bd.dt.year, bd.dt.month, bd.dt.day]):
                part_dt = datetime(int(year), int(month), int(day))
                yield book, part_dt, group.drop(columns=["_date"], errors="ignore")


def upload_news_batches_to_s3(
    s3_client,
    bucket: str,
    transformed_df: pd.DataFrame,
    batch_types: List[str],
    agentic: Optional[bool] = None,
) -> None:
    """Upload batch CSVs: one run file (by run time) and/or one file per partition (by article date)."""
    now = datetime.utcnow()
    for batch_type in batch_types:
        if batch_type == "run":
            key = build_s3_key_news_batch("run", now, agentic=agentic)
            df_export = _prepare_news_dataframe_for_csv(transformed_df)
            upload_dataframe_to_s3_key(s3_client, bucket, key, df_export)
            logger.info("Uploaded batch run to s3://%s/%s", bucket, key)
        elif batch_type in _DATA_PARTITION_BATCH_TYPES:
            for part_dt, sub_df in _group_transformed_by_partition(transformed_df, batch_type):
                key = build_s3_key_news_batch(batch_type, part_dt, agentic=agentic)
                df_export = _prepare_news_dataframe_for_csv(sub_df)
                upload_dataframe_to_s3_key(s3_client, bucket, key, df_export)
                logger.info("Uploaded batch %s (partition %s) to s3://%s/%s", batch_type, part_dt.date(), bucket, key)
        else:
            logger.warning("Unknown batch_type %s; skipping", batch_type)


def upload_stocks_batches_to_s3(
    s3_client,
    bucket: str,
    transformed_df: pd.DataFrame,
    batch_types: List[str],
) -> None:
    """Upload batch CSVs: one run file (all books) and/or one file per (book, partition) for year/month/week/day."""
    now = datetime.utcnow()
    for batch_type in batch_types:
        if batch_type == "run":
            key = build_s3_key_stocks_batch("", now, "run")
            upload_dataframe_to_s3_key(s3_client, bucket, key, transformed_df)
            logger.info("Uploaded stocks batch run to s3://%s/%s", bucket, key)
        elif batch_type in _DATA_PARTITION_BATCH_TYPES:
            for book, part_dt, sub_df in _group_stocks_by_partition(transformed_df, batch_type):
                key = build_s3_key_stocks_batch(book, part_dt, batch_type)
                upload_dataframe_to_s3_key(s3_client, bucket, key, sub_df)
                logger.info("Uploaded stocks batch %s (book=%s, partition %s) to s3://%s/%s",
                            batch_type, book, part_dt.date(), bucket, key)
        else:
            logger.warning("Unknown batch_type %s; skipping", batch_type)


def _group_transformed_by_partition(
    transformed_df: pd.DataFrame,
    batch_type: str,
):
    """Yield (partition_dt, sub_df) for each partition. batch_type in ('year','month','week','day').
    Partition is derived from article datetime (when the article was issued).
    """
    if batch_type not in _DATA_PARTITION_BATCH_TYPES or transformed_df.empty:
        return
    if "datetime" not in transformed_df.columns:
        return
    dt = pd.to_datetime(transformed_df["datetime"], errors="coerce")
    valid = dt.notna()
    if not valid.any():
        return
    subset = transformed_df.loc[valid]
    d = dt.loc[valid]
    if batch_type == "year":
        for year, group in subset.groupby(d.dt.year):
            yield datetime(int(year), 1, 1), group
    elif batch_type == "month":
        for (year, month), group in subset.groupby([d.dt.year, d.dt.month]):
            yield datetime(int(year), int(month), 1), group
    elif batch_type == "week":
        # Group by calendar year and Monday-based week (0-53) so 2024-12-30/31 stay in 2024
        week_num = d.dt.strftime("%W").astype(int)
        for (year, week), group in subset.groupby([d.dt.year, week_num]):
            part_dt = pd.to_datetime(group["datetime"], errors="coerce").min()
            if pd.isna(part_dt):
                part_dt = datetime(int(year), 1, 1)
            else:
                part_dt = part_dt.to_pydatetime() if hasattr(part_dt, "to_pydatetime") else part_dt
            yield part_dt, group
    elif batch_type == "day":
        for (year, month, day), group in subset.groupby([d.dt.year, d.dt.month, d.dt.day]):
            yield datetime(int(year), int(month), int(day)), group


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
    S3: per-article keys and/or batch keys (run; year/month/week/day partitioned by article date) under news bucket.
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
                one = _prepare_news_dataframe_for_csv(pd.DataFrame([row]))
                upload_dataframe_to_s3_key(aws.s3_client, bucket, key, one)
            logger.info("Uploaded %d per-article CSVs to s3://%s/", len(transformed), bucket)
        if upload_s3_batch:
            upload_news_batches_to_s3(
                aws.s3_client, bucket, transformed, upload_s3_batch, agentic=agentic_enabled
            )

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
    upload_s3_batch: Optional[List[str]] = None,  # e.g. ["run", "week", "month", "year"]
    pg_conn=None,
) -> pd.DataFrame:
    """
    Ingest stocks from Postgres, transform per book with warmup window,
    save to historical_processed, then upload to S3 (per book/day and/or batch by week/month/year).
    """
    from config.settings import get_settings
    from storage.postgres.pgConn import PgConn
    from storage.postgres import PostgresSQL_table_queries as q
    from storage.cloud.CloudStorage import CloudStorageProvider
    from pipelines.etl_cli import ingest_stocks
    from transform.stocks.stock_transformers import StockTransformationPipeline

    settings = get_settings()
    bucket = stocks_bucket or getattr(settings.aws, "stocks_bucket", None) or settings.aws.default_bucket
    if not bucket and (upload_s3 or upload_s3_batch):
        logger.warning("No stocks S3 bucket configured; skipping S3 upload")

    since_dt = pd.to_datetime(since)
    until_dt = pd.to_datetime(until) if until else since_dt
    warmup_start = since_dt - pd.Timedelta(days=warmup_days)
    warmup_start_str = warmup_start.strftime("%Y-%m-%d")
    until_str = until_dt.strftime("%Y-%m-%d") if until else since

    logger.info("Ingesting stocks (warmup %s to %s)...", warmup_start_str, until_str)
    df = ingest_stocks(since=warmup_start_str, until=until_str, books=books)
    if df is None or df.empty:
        logger.warning("No stock data to transform")
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    logger.info("Transforming (returns, volatility, technical indicators)...")
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

    if (upload_s3 or upload_s3_batch) and bucket:
        aws = CloudStorageProvider.AWS()
        if upload_s3:
            grouped = transformed.groupby(["book", "date"])
            logger.info("Uploading %d book/day files to s3://%s/...", grouped.ngroups, bucket)
            for (book, date_val), group in grouped:
                key = build_s3_key_stocks(book, date_val)
                upload_dataframe_to_s3_key(aws.s3_client, bucket, key, group)
            logger.info("Uploaded %d group files to s3://%s/", grouped.ngroups, bucket)
        if upload_s3_batch:
            upload_stocks_batches_to_s3(aws.s3_client, bucket, transformed, upload_s3_batch)

    return transformed
