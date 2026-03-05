"""
Claude/OpenAI-style tool schemas for ETL and transform operations.

Each schema has: name, description, input_schema (JSON Schema).
Use with Messages API tools= parameter or for MCP tool definitions.
"""

# ---------------------------------------------------------------------------
# Computation / utility tools (pure, no side effects)
# ---------------------------------------------------------------------------

EXTRACT_TICKERS_SCHEMA = {
    "name": "extract_tickers",
    "description": (
        "Extract cryptocurrency and stock ticker symbols from text. "
        "Handles formats like BTC-USD, ETH-USD, (CRYPTO: BTC), $BTC, and common names (bitcoin -> BTC-USD). "
        "Use when the user asks which assets or tickers are mentioned in a headline, article, or snippet."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "text": {
                "type": "string",
                "description": "The text to extract tickers from (e.g. headline, summary, or full content).",
            },
            "headline": {
                "type": "string",
                "description": "Optional headline; if provided with summary/content, all are combined for extraction.",
            },
            "summary": {
                "type": "string",
                "description": "Optional summary text.",
            },
            "content": {
                "type": "string",
                "description": "Optional body content (first 2000 chars used if very long).",
            },
        },
        "required": [],
    },
}

ANALYZE_SENTIMENT_SCHEMA = {
    "name": "analyze_sentiment",
    "description": (
        "Analyze the sentiment of a text snippet (positive, negative, neutral) with scores. "
        "Use when the user wants to know the sentiment of a headline, paragraph, or article. "
        "Backend 'vader' is fast and rule-based; 'textblob' is alternative; 'transformers' uses FinBERT (slower)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "text": {
                "type": "string",
                "description": "The text to analyze.",
            },
            "backend": {
                "type": "string",
                "description": "Sentiment backend: 'vader', 'textblob', or 'transformers'. Default 'vader'.",
                "enum": ["vader", "textblob", "transformers"],
            },
        },
        "required": ["text"],
    },
}

EXTRACT_INTENT_SCHEMA = {
    "name": "extract_intent",
    "description": (
        "Extract the primary intent/category of financial news text (e.g. market_update, regulatory_news, "
        "price_prediction, company_news, breaking_news). Use when the user wants to classify what kind of news a text is."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "text": {
                "type": "string",
                "description": "The text to classify.",
            },
        },
        "required": ["text"],
    },
}

EXTRACT_KEYWORDS_SCHEMA = {
    "name": "extract_keywords",
    "description": (
        "Extract keywords and optionally named entities from text. "
        "Use when the user wants key terms or entities from a headline or article."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "text": {
                "type": "string",
                "description": "The text to extract keywords from.",
            },
            "top_n": {
                "type": "integer",
                "description": "Number of top keywords to return. Default 10.",
            },
            "method": {
                "type": "string",
                "description": "Extraction method: 'tfidf', 'spacy', or 'rake'. Default 'tfidf'.",
                "enum": ["tfidf", "spacy", "rake"],
            },
        },
        "required": ["text"],
    },
}

STOCK_RISK_METRICS_SCHEMA = {
    "name": "stock_risk_metrics",
    "description": (
        "Calculate risk metrics (mean return, volatility, Sharpe ratio, max drawdown, VaR 95%, CVaR 95%) "
        "from a series of closing prices. Use when the user asks for risk or performance metrics of a price series."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "prices": {
                "type": "array",
                "items": {"type": "number"},
                "description": "List of closing prices in chronological order.",
            },
            "window": {
                "type": "integer",
                "description": "Window size in periods (e.g. 252 for one trading year). Default 252.",
            },
        },
        "required": ["prices"],
    },
}

BUILD_S3_KEY_NEWS_SCHEMA = {
    "name": "build_s3_key_news",
    "description": (
        "Build the S3 object key for a news article or batch under the project's convention. "
        "Use when the user needs to know where a file would be stored (e.g. for debugging or integration)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "article_id": {
                "type": "string",
                "description": "Article ID (for per-article key).",
            },
            "datetime_str": {
                "type": "string",
                "description": "ISO datetime string for the article (e.g. 2026-01-27T10:30:00).",
            },
            "batch_type": {
                "type": "string",
                "description": "If building batch key: 'run', 'week', 'month', 'year', or 'day'. Omit for per-article.",
                "enum": ["run", "week", "month", "year", "day"],
            },
            "agentic": {
                "type": "boolean",
                "description": "Whether the path is for agentic-enriched data (adds agentic=true|false/ to path).",
            },
        },
        "required": [],
    },
}

BUILD_S3_KEY_STOCKS_SCHEMA = {
    "name": "build_s3_key_stocks",
    "description": (
        "Build the S3 object key for transformed stocks (one file per book per day). "
        "Use when the user needs the storage path for a book/date."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "book": {
                "type": "string",
                "description": "Book/symbol name (e.g. btc-usd).",
            },
            "date": {
                "type": "string",
                "description": "Date in YYYY-MM-DD format.",
            },
        },
        "required": ["book", "date"],
    },
}

# ---------------------------------------------------------------------------
# Action / side-effect tools
# ---------------------------------------------------------------------------

INGEST_NEWS_SCHEMA = {
    "name": "ingest_news",
    "description": (
        "Load raw financial news from PostgreSQL for a given date or date range. "
        "Use when the user wants to fetch or inspect news data for specific dates. "
        "Returns a summary of row count and optional sample; does not modify data."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "date": {
                "type": "string",
                "description": "Single date to filter (YYYY-MM-DD).",
            },
            "since": {
                "type": "string",
                "description": "Start date for range (YYYY-MM-DD).",
            },
            "until": {
                "type": "string",
                "description": "End date for range (YYYY-MM-DD).",
            },
        },
        "required": [],
    },
}

INGEST_STOCKS_SCHEMA = {
    "name": "ingest_stocks",
    "description": (
        "Load raw stock/crypto price data from PostgreSQL for a date range and optional books. "
        "Use when the user wants to fetch OHLCV data for analysis or transformation."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "since": {
                "type": "string",
                "description": "Start date (YYYY-MM-DD).",
            },
            "until": {
                "type": "string",
                "description": "End date (YYYY-MM-DD). If omitted, uses since.",
            },
            "books": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional list of book symbols to filter (e.g. ['btc-usd', 'eth-usd']).",
            },
        },
        "required": ["since"],
    },
}

RUN_NEWS_TRANSFORM_SCHEMA = {
    "name": "run_news_transform",
    "description": (
        "Run the news transformation pipeline: load news from Postgres for the given date/range, "
        "apply sentiment, intent, keyword and ticker extraction. Optionally save to Postgres and/or upload to S3. "
        "Use when the user wants to transform news data (with or without persisting)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "date": {
                "type": "string",
                "description": "Single date (YYYY-MM-DD).",
            },
            "since": {
                "type": "string",
                "description": "Start date for range.",
            },
            "until": {
                "type": "string",
                "description": "End date for range.",
            },
            "save_to_postgres": {
                "type": "boolean",
                "description": "Whether to save transformed rows to financial_news_transformed. Default true.",
            },
            "sentiment_backend": {
                "type": "string",
                "description": "Sentiment backend: vader, textblob, or transformers. Default vader.",
                "enum": ["vader", "textblob", "transformers"],
            },
            "extract_tickers": {
                "type": "boolean",
                "description": "Whether to extract ticker symbols. Default true.",
            },
        },
        "required": [],
    },
}

RUN_STOCKS_TRANSFORM_SCHEMA = {
    "name": "run_stocks_transform",
    "description": (
        "Run the stocks transformation pipeline: load OHLCV from Postgres, compute returns, "
        "volatility and technical indicators (SMA, EMA, RSI, MACD, Bollinger). "
        "Optionally save to historical_processed and upload to S3."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "since": {
                "type": "string",
                "description": "Start date (YYYY-MM-DD).",
            },
            "until": {
                "type": "string",
                "description": "End date (YYYY-MM-DD). Defaults to since.",
            },
            "books": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional list of book symbols.",
            },
            "warmup_days": {
                "type": "integer",
                "description": "Warmup days for indicators (e.g. 252). Default 252.",
            },
            "save_to_postgres": {
                "type": "boolean",
                "description": "Whether to save to historical_processed. Default true.",
            },
            "upload_s3": {
                "type": "boolean",
                "description": "Whether to upload per book/day to S3. Default true.",
            },
        },
        "required": ["since"],
    },
}

ENRICH_ARTICLE_SCHEMA = {
    "name": "enrich_article",
    "description": (
        "Enrich a single article with LLM-derived fields: one-line summary, themes, and optionally "
        "trading-grade financial metrics (sentiment scores, event type, signal, sectors, key facts). "
        "Use when the user wants to add AI summary or structured extraction to one article. "
        "Requires LLM provider to be configured (e.g. OpenAI or Claude)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "headline": {
                "type": "string",
                "description": "Article headline.",
            },
            "content": {
                "type": "string",
                "description": "Article body (or summary if no body).",
            },
            "summary": {
                "type": "string",
                "description": "Optional summary.",
            },
            "tickers": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional list of ticker symbols already extracted.",
            },
            "task": {
                "type": "string",
                "description": "Enrichment task: 'summary_themes' or 'financial_metrics'. Default summary_themes.",
                "enum": ["summary_themes", "financial_metrics"],
            },
        },
        "required": ["headline"],
    },
}

# ---------------------------------------------------------------------------
# Batch / meta tool
# ---------------------------------------------------------------------------

BATCH_TOOL_SCHEMA = {
    "name": "batch_tool",
    "description": (
        "Invoke multiple tool calls in one request. Pass a list of invocations, each with 'name' and 'arguments' (JSON string). "
        "Use when the user wants to run several operations at once (e.g. ingest_news and run_news_transform for two dates)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "invocations": {
                "type": "array",
                "description": "List of tool invocations.",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Tool name."},
                        "arguments": {
                            "type": "string",
                            "description": "JSON string of arguments for the tool.",
                        },
                    },
                    "required": ["name", "arguments"],
                },
            },
        },
        "required": ["invocations"],
    },
}


def get_all_schemas():
    """Return list of all tool schemas (Claude/OpenAI format)."""
    return [
        EXTRACT_TICKERS_SCHEMA,
        ANALYZE_SENTIMENT_SCHEMA,
        EXTRACT_INTENT_SCHEMA,
        EXTRACT_KEYWORDS_SCHEMA,
        STOCK_RISK_METRICS_SCHEMA,
        BUILD_S3_KEY_NEWS_SCHEMA,
        BUILD_S3_KEY_STOCKS_SCHEMA,
        INGEST_NEWS_SCHEMA,
        INGEST_STOCKS_SCHEMA,
        RUN_NEWS_TRANSFORM_SCHEMA,
        RUN_STOCKS_TRANSFORM_SCHEMA,
        ENRICH_ARTICLE_SCHEMA,
        BATCH_TOOL_SCHEMA,
    ]


def get_schema_by_name(name: str):
    """Return schema dict for the given tool name, or None."""
    for s in get_all_schemas():
        if s["name"] == name:
            return s
    return None
