"""
Tool implementations: execute each tool from parsed arguments.

Each function accepts kwargs matching the corresponding schema and returns
a JSON-serializable dict (or list) for Claude/MCP.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional


def extract_tickers(
    text: Optional[str] = None,
    headline: Optional[str] = None,
    summary: Optional[str] = None,
    content: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Extract ticker symbols from text or article fields."""
    from transform.news.ticker_extractor import TickerExtractor

    if text:
        combined = text
    else:
        parts = [h for h in (headline, summary, (content[:2000] if content else None)) if h]
        combined = " ".join(parts) if parts else ""

    extractor = TickerExtractor()
    result = extractor.extract_from_text(combined or "")
    return {
        "tickers": list(result.tickers),
        "confidence": float(result.confidence),
        "extraction_method": result.extraction_method,
    }


def analyze_sentiment(
    text: str,
    backend: str = "vader",
    **kwargs: Any,
) -> Dict[str, Any]:
    """Analyze sentiment of text."""
    from transform.news.text_transformers import SentimentAnalyzer

    analyzer = SentimentAnalyzer(backend=backend)
    result = analyzer.transform(text or "")
    return {
        "sentiment_label": result.label.value,
        "sentiment_score": result.score,
        "positive_score": result.positive_score,
        "negative_score": result.negative_score,
        "neutral_score": result.neutral_score,
    }


def extract_intent(
    text: str,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Extract primary intent from text."""
    from transform.news.text_transformers import IntentExtractor

    extractor = IntentExtractor(use_transformers=False)
    result = extractor.transform(text or "")
    secondary = []
    for item in result.secondary_intents or []:
        for k, v in item.items():
            secondary.append({"intent": k, "score": v})
    return {
        "primary_intent": result.primary_intent.value,
        "intent_confidence": result.confidence,
        "secondary_intents": secondary,
    }


def extract_keywords(
    text: str,
    top_n: int = 10,
    method: str = "tfidf",
    **kwargs: Any,
) -> Dict[str, Any]:
    """Extract keywords and entities from text."""
    from transform.news.text_transformers import KeywordExtractor

    extractor = KeywordExtractor(method=method, top_n=top_n)
    result = extractor.transform(text or "")
    return {
        "keywords": result.keywords,
        "keyword_scores": result.keyword_scores,
        "entities": result.entities,
    }


def stock_risk_metrics(
    prices: List[float],
    window: int = 252,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Compute risk metrics from a price series."""
    import pandas as pd
    from transform.stocks.stock_transformers import (
        ReturnsCalculator,
        calculate_max_drawdown,
        calculate_risk_metrics,
    )

    if not prices or len(prices) < 2:
        return {"error": "Need at least 2 prices"}
    df = pd.DataFrame({"close": prices})
    df = ReturnsCalculator.add_returns(df, price_col="close")
    if len(df) < window:
        window = len(df)
    metrics = calculate_risk_metrics(df, window=window)
    metrics["max_drawdown"] = float(calculate_max_drawdown(df, price_col="close"))
    return {k: float(v) if isinstance(v, (int, float)) else v for k, v in metrics.items()}


def build_s3_key_news(
    article_id: Optional[str] = None,
    datetime_str: Optional[str] = None,
    batch_type: Optional[str] = None,
    agentic: Optional[bool] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Build S3 key for news article or batch."""
    from pipelines.etl_transform import (
        build_s3_key_news_batch,
        build_s3_key_news_per_article,
    )

    try:
        dt = datetime.utcnow()
        if datetime_str:
            dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
    except Exception:
        pass

    if batch_type:
        key = build_s3_key_news_batch(batch_type, dt, agentic=agentic)
        return {"s3_key": key, "type": "batch", "batch_type": batch_type}
    if article_id:
        key = build_s3_key_news_per_article(article_id, dt, agentic=agentic)
        return {"s3_key": key, "type": "per_article", "article_id": article_id}
    return {"error": "Provide article_id or batch_type"}


def build_s3_key_stocks(
    book: str,
    date: str,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Build S3 key for transformed stocks (one file per book per day)."""
    from pipelines.etl_transform import build_s3_key_stocks as build_key

    key = build_key(book, date)
    return {"s3_key": key, "book": book, "date": date}


def build_s3_key_stocks_batch(
    book: str,
    date: str,
    batch_type: str,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Build S3 key for a batch transformed stocks file (run/week/month/year/day)."""
    from datetime import datetime
    from pipelines.etl_transform import build_s3_key_stocks_batch as build_key

    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        dt = datetime.utcnow()
    # For 'run' the pipeline ignores book; we still accept book for API consistency
    key = build_key(book, dt, batch_type)
    return {"s3_key": key, "book": book, "date": date, "batch_type": batch_type}


def ingest_news(
    date: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Load raw news from Postgres."""
    from pipelines.etl_cli import ingest_news as _ingest

    df = _ingest(date=date, since=since, until=until)
    if df is None or df.empty:
        return {"row_count": 0, "message": "No news data found"}
    # Return summary + small sample (first 2 rows, key columns only)
    cols = [c for c in ["id", "headline", "datetime", "source"] if c in df.columns]
    sample = df[cols].head(2).to_dict("records") if cols else []
    return {
        "row_count": len(df),
        "columns": list(df.columns),
        "sample": sample,
    }


def ingest_stocks(
    since: str,
    until: Optional[str] = None,
    books: Optional[List[str]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Load raw stocks from Postgres."""
    from pipelines.etl_cli import ingest_stocks as _ingest

    df = _ingest(since=since, until=until, books=books)
    if df is None or df.empty:
        return {"row_count": 0, "message": "No stock data found"}
    cols = [c for c in ["book", "date", "close", "volume"] if c in df.columns]
    sample = df[cols].head(5).to_dict("records") if cols else []
    return {
        "row_count": len(df),
        "columns": list(df.columns),
        "sample": sample,
    }


def run_news_transform(
    date: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    save_to_postgres: bool = True,
    sentiment_backend: str = "vader",
    extract_tickers: bool = True,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Run news ETL transform pipeline."""
    from pipelines.etl_transform import run_news_etl

    df = run_news_etl(
        date=date,
        since=since,
        until=until,
        save_to_postgres=save_to_postgres,
        upload_s3_per_article=False,
        upload_s3_batch=None,
        sentiment_backend=sentiment_backend,
        extract_tickers=extract_tickers,
        agentic_enabled=False,
    )
    if df is None or df.empty:
        return {"row_count": 0, "message": "No news transformed"}
    return {
        "row_count": len(df),
        "columns": list(df.columns),
        "message": f"Transformed {len(df)} articles; save_to_postgres={save_to_postgres}",
    }


def run_stocks_transform(
    since: str,
    until: Optional[str] = None,
    books: Optional[List[str]] = None,
    warmup_days: int = 252,
    save_to_postgres: bool = True,
    upload_s3: bool = True,
    upload_s3_batch: Optional[List[str]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Run stocks ETL transform pipeline."""
    from pipelines.etl_transform import run_stocks_etl

    df = run_stocks_etl(
        since=since,
        until=until,
        books=books,
        warmup_days=warmup_days,
        save_to_postgres=save_to_postgres,
        upload_s3=upload_s3,
        upload_s3_batch=upload_s3_batch,
    )
    if df is None or df.empty:
        return {"row_count": 0, "message": "No stock data transformed"}
    return {
        "row_count": len(df),
        "columns": list(df.columns),
        "message": f"Transformed {len(df)} records; save_to_postgres={save_to_postgres}, upload_s3={upload_s3}, upload_s3_batch={upload_s3_batch}",
    }


def export_genai_jsonl(
    date: Optional[str] = None,
    include_embeddings: bool = False,
    batch_types: Optional[List[str]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Export transformed financial news from Postgres to JSONL on S3 for GenAI/RAG.

    This calls the production helper export_genai_to_s3_from_db, which:
    - loads news from PostgreSQL for the given date
    - runs the standard news transformation pipeline
    - optionally generates embeddings
    - uploads a partitioned JSONL file to S3 under genai/news/year=YYYY/month=MM/day=DD/format=jsonl/
    """
    from pipelines.etl_cli import export_genai_to_s3_from_db

    # If caller does not specify batch_types, mirror the full transformed-news
    # batch options: run, week, month, year, day.
    if batch_types is None:
        batch_types = ["run", "week", "month", "year", "day"]

    try:
        uris = export_genai_to_s3_from_db(
            date=date,
            include_embeddings=include_embeddings,
            batch_types=batch_types,
        )
        return {
            "s3_uris": uris,
            "date": date,
            "include_embeddings": include_embeddings,
            "batch_types": batch_types,
            "message": "GenAI JSONL export completed",
        }
    except Exception as e:
        return {
            "error": str(e),
            "date": date,
            "include_embeddings": include_embeddings,
            "batch_types": batch_types,
        }


def enrich_article(
    headline: str,
    content: Optional[str] = None,
    summary: Optional[str] = None,
    tickers: Optional[List[str]] = None,
    task: str = "summary_themes",
    **kwargs: Any,
) -> Dict[str, Any]:
    """Enrich one article with LLM (summary/themes or financial metrics)."""
    from agents.registry import get_llm_client
    from agents.transforms.agentic_transform import (
        AgenticTextEnricher,
        FinancialMetricsTask,
        SummaryAndThemesTask,
    )

    task_impl = FinancialMetricsTask() if task == "financial_metrics" else SummaryAndThemesTask()
    try:
        client = get_llm_client()
    except Exception as e:
        return {"error": f"LLM client not available: {e}"}

    row = {
        "headline": headline,
        "content": content or "",
        "summary": summary or "",
        "tickers": tickers or [],
    }
    enricher = AgenticTextEnricher(client=client, task=task_impl)
    result = enricher.enrich_row(row)
    # Ensure JSON-serializable
    out = {}
    for k, v in result.items():
        if hasattr(v, "value"):
            out[k] = v.value
        elif isinstance(v, (list, dict, str, int, float, bool, type(None))):
            out[k] = v
        else:
            out[k] = str(v)
    return out


def batch_tool(
    invocations: List[Dict[str, Any]],
    **kwargs: Any,
) -> Dict[str, Any]:
    """Run multiple tool invocations and return combined results."""
    from agents.tools import get_handler_by_name

    results = []
    for inv in invocations:
        name = inv.get("name")
        arguments_str = inv.get("arguments", "{}")
        if not name:
            results.append({"error": "Missing 'name' in invocation"})
            continue
        try:
            arguments = json.loads(arguments_str) if isinstance(arguments_str, str) else arguments_str
        except json.JSONDecodeError as e:
            results.append({"tool": name, "error": f"Invalid JSON arguments: {e}"})
            continue
        handler = get_handler_by_name(name)
        if not handler:
            results.append({"tool": name, "error": f"Unknown tool: {name}"})
            continue
        try:
            out = handler(**arguments)
            results.append({"tool": name, "result": out})
        except Exception as e:
            results.append({"tool": name, "error": str(e)})
    return {"invocations": results}
