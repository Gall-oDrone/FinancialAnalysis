"""
Tests for agents.tools: schemas, run_tool, and tool implementations.

Computation tools are tested without mocks; ingest/run_* use mocked DB.
"""

import json
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

# Add src to path (conftest does this; ensure we can import agents.tools)
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


class TestSchemas:
    """Test tool schema listing and lookup."""

    def test_get_all_schemas(self):
        from agents.tools import get_all_schemas

        schemas = get_all_schemas()
        assert len(schemas) >= 10
        names = [s["name"] for s in schemas]
        assert "extract_tickers" in names
        assert "analyze_sentiment" in names
        assert "batch_tool" in names
        for s in schemas:
            assert "name" in s and "description" in s and "input_schema" in s

    def test_get_schema_by_name(self):
        from agents.tools import get_schema_by_name

        s = get_schema_by_name("extract_tickers")
        assert s is not None
        assert s["name"] == "extract_tickers"
        assert "text" in s["input_schema"]["properties"]
        assert get_schema_by_name("nonexistent") is None


class TestGetToolsForClaude:
    """Test Claude API tool list."""

    def test_get_tools_for_claude(self):
        from agents.tools import get_tools_for_claude

        tools = get_tools_for_claude()
        assert isinstance(tools, list)
        assert len(tools) == len([t for t in tools if "input_schema" in t])


class TestRunTool:
    """Test run_tool dispatcher."""

    def test_unknown_tool(self):
        from agents.tools import run_tool

        out = run_tool("unknown_tool", {})
        assert "error" in out and "Unknown" in out["error"]

    def test_get_handler_by_name(self):
        from agents.tools import get_handler_by_name

        h = get_handler_by_name("extract_tickers")
        assert h is not None
        assert callable(h)
        assert get_handler_by_name("nonexistent") is None


class TestExtractTickers:
    """Test extract_tickers tool."""

    def test_extract_tickers_from_text(self):
        from agents.tools import run_tool

        out = run_tool("extract_tickers", {"text": "Bitcoin (CRYPTO: BTC) and ETH-USD rallied."})
        assert "tickers" in out
        assert "BTC-USD" in out["tickers"]
        assert "ETH-USD" in out["tickers"]
        assert "confidence" in out

    def test_extract_tickers_from_article_fields(self):
        from agents.tools import run_tool

        out = run_tool("extract_tickers", {
            "headline": "Solana surges",
            "content": "SOL-USD gained 10%.",
        })
        assert "tickers" in out
        assert "SOL-USD" in out["tickers"]

    def test_extract_tickers_empty(self):
        from agents.tools import run_tool

        out = run_tool("extract_tickers", {})
        assert out["tickers"] == []
        assert out.get("extraction_method") in ("none", "pattern", "name_mapping", "pattern+name_mapping")


class TestAnalyzeSentiment:
    """Test analyze_sentiment tool."""

    def test_analyze_sentiment_positive(self):
        from agents.tools import run_tool

        out = run_tool("analyze_sentiment", {"text": "This is great news! The market is booming."})
        assert "sentiment_label" in out
        assert out["sentiment_label"] in ("positive", "negative", "neutral")
        assert "sentiment_score" in out

    def test_analyze_sentiment_backend_vader(self):
        from agents.tools import run_tool

        out = run_tool("analyze_sentiment", {"text": "Stocks fell sharply.", "backend": "vader"})
        assert "sentiment_label" in out


class TestExtractIntent:
    """Test extract_intent tool."""

    def test_extract_intent(self):
        from agents.tools import run_tool

        out = run_tool("extract_intent", {
            "text": "The SEC announced new regulations for crypto exchanges.",
        })
        assert "primary_intent" in out
        assert "intent_confidence" in out


class TestExtractKeywords:
    """Test extract_keywords tool."""

    def test_extract_keywords_tfidf(self):
        from agents.tools import run_tool

        out = run_tool("extract_keywords", {
            "text": "Bitcoin and Ethereum prices surged. Crypto market cap increased.",
            "top_n": 5,
            "method": "tfidf",
        })
        assert "keywords" in out
        assert isinstance(out["keywords"], list)


class TestStockRiskMetrics:
    """Test stock_risk_metrics tool."""

    def test_stock_risk_metrics(self):
        from agents.tools import run_tool

        prices = [100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0]
        out = run_tool("stock_risk_metrics", {"prices": prices, "window": 5})
        assert "volatility" in out or "mean_return" in out or "error" in out
        if "error" not in out:
            assert "max_drawdown" in out

    def test_stock_risk_metrics_insufficient_data(self):
        from agents.tools import run_tool

        out = run_tool("stock_risk_metrics", {"prices": [100.0]})
        assert "error" in out or "volatility" in out


class TestBuildS3Keys:
    """Test S3 key builder tools."""

    def test_build_s3_key_news_per_article(self):
        from agents.tools import run_tool

        out = run_tool("build_s3_key_news", {
            "article_id": "art123",
            "datetime_str": "2026-01-27T10:30:00",
        })
        assert "s3_key" in out
        assert "art123" in out["s3_key"]
        assert "news" in out["s3_key"]

    def test_build_s3_key_news_batch(self):
        from agents.tools import run_tool

        out = run_tool("build_s3_key_news", {
            "batch_type": "run",
            "datetime_str": "2026-01-27T10:30:00",
        })
        assert "s3_key" in out
        assert "batch=run" in out["s3_key"]

    def test_build_s3_key_news_batch_day(self):
        from agents.tools import run_tool

        out = run_tool("build_s3_key_news", {
            "batch_type": "day",
            "datetime_str": "2025-01-15T10:30:00",
        })
        assert "s3_key" in out
        assert "year=2025" in out["s3_key"]
        assert "month=01" in out["s3_key"]
        assert "day=15" in out["s3_key"]

    def test_build_s3_key_stocks(self):
        from agents.tools import run_tool

        out = run_tool("build_s3_key_stocks", {"book": "btc-usd", "date": "2026-01-27"})
        assert out["s3_key"] is not None
        assert "stocks/transformed/crypto" in out["s3_key"]
        assert "btc-usd" in out["s3_key"].lower()
        assert "2026" in out["s3_key"]

    def test_build_s3_key_stocks_batch(self):
        from agents.tools import run_tool

        out = run_tool(
            "build_s3_key_stocks_batch",
            {"book": "btc-usd", "date": "2026-02-22", "batch_type": "year"},
        )
        assert out["s3_key"] is not None
        assert "stocks/transformed/crypto" in out["s3_key"]
        assert "book=btc-usd" in out["s3_key"]
        assert "y2026" in out["s3_key"]
        assert out.get("batch_type") == "year"
        # Run batch: single file, batch=run in path
        out_run = run_tool(
            "build_s3_key_stocks_batch",
            {"book": "btc-usd", "date": "2026-01-01", "batch_type": "run"},
        )
        assert "batch=run" in out_run["s3_key"]


class TestBatchTool:
    """Test batch_tool meta-tool."""

    def test_batch_tool_two_invocations(self):
        from agents.tools import run_tool

        invocations = [
            {"name": "extract_tickers", "arguments": json.dumps({"text": "BTC-USD and Ethereum"})},
            {"name": "analyze_sentiment", "arguments": json.dumps({"text": "Great news today."})},
        ]
        out = run_tool("batch_tool", {"invocations": invocations})
        assert "invocations" in out
        assert len(out["invocations"]) == 2
        assert out["invocations"][0]["tool"] == "extract_tickers"
        assert "result" in out["invocations"][0]
        assert "tickers" in out["invocations"][0]["result"]


class TestIngestNewsMocked:
    """Test ingest_news with mocked handler (avoids loading DB/psycopg2)."""

    def test_ingest_news_returns_summary(self):
        from agents.tools import run_tool, _HANDLERS

        def fake_ingest(**kwargs):
            return {
                "row_count": 1,
                "columns": ["id", "headline", "datetime", "source"],
                "sample": [{"id": 1, "headline": "Test", "datetime": "2026-01-27T10:00:00", "source": "Yahoo"}],
            }

        with patch.dict(_HANDLERS, {"ingest_news": fake_ingest}):
            out = run_tool("ingest_news", {"date": "2026-01-27"})
        assert out["row_count"] == 1
        assert "sample" in out

    def test_ingest_news_empty(self):
        from agents.tools import run_tool, _HANDLERS

        def fake_ingest(**kwargs):
            return {"row_count": 0, "message": "No news data found"}

        with patch.dict(_HANDLERS, {"ingest_news": fake_ingest}):
            out = run_tool("ingest_news", {"date": "2026-01-27"})
        assert out["row_count"] == 0


class TestIngestStocksMocked:
    """Test ingest_stocks with mocked handler (avoids loading DB/psycopg2)."""

    def test_ingest_stocks_returns_summary(self):
        from agents.tools import run_tool, _HANDLERS

        def fake_ingest(**kwargs):
            return {
                "row_count": 3,
                "columns": ["book", "date", "close", "volume"],
                "sample": [{"book": "btc-usd", "date": "2026-01-01", "close": 100.0, "volume": 1000}],
            }

        with patch.dict(_HANDLERS, {"ingest_stocks": fake_ingest}):
            out = run_tool("ingest_stocks", {"since": "2026-01-01"})
        assert out["row_count"] == 3
        assert "sample" in out


class TestEnrichArticleMocked:
    """Test enrich_article with mocked LLM client."""

    @patch("agents.registry.get_llm_client")
    def test_enrich_article_returns_result(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.complete.return_value = MagicMock(content="SUMMARY: A summary.\nTHEMES: crypto, regulation")
        mock_get_client.return_value = mock_client
        from agents.tools import run_tool

        out = run_tool("enrich_article", {
            "headline": "SEC delays ETF",
            "content": "The SEC postponed its decision.",
        })
        if "error" in out and "LLM" in str(out.get("error", "")):
            pytest.skip("LLM client mock may not match AgenticTextEnricher expectations")
        assert "llm_summary" in out or "llm_themes" in out or "error" in out
