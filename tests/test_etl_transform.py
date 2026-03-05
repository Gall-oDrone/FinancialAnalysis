"""
Tests for ETL transform pipeline (news and stocks: transform, Postgres save, S3 paths).
"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock

try:
    import psycopg2  # noqa: F401
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

from pipelines.etl_transform import (
    build_s3_key_news_per_article,
    build_s3_key_news_batch,
    build_s3_key_stocks,
    _serialize_row_for_news_db,
    agentic_result_has_failures,
    save_transformed_news_to_postgres,
    upload_dataframe_to_s3_key,
)


class TestS3PathBuilders:
    """Test S3 key builders for news and stocks."""

    def test_build_s3_key_news_per_article(self):
        dt = datetime(2026, 2, 26, 18, 31, 22)
        key = build_s3_key_news_per_article("316878550224202608217373602922154345297", dt)
        assert "news/crypto" in key
        assert "year=2026" in key
        assert "month=02" in key
        assert "day=26" in key
        assert "hour=18" in key
        assert "minute=31" in key
        assert "second=22" in key
        assert "format=csv" in key
        assert key.endswith("316878550224202608217373602922154345297.csv")

    def test_build_s3_key_news_per_article_agentic(self):
        dt = datetime(2026, 2, 26, 18, 31, 22)
        key_false = build_s3_key_news_per_article("id1", dt, agentic=False)
        key_true = build_s3_key_news_per_article("id1", dt, agentic=True)
        assert "agentic=false" in key_false
        assert "agentic=true" in key_true
        assert "year=2026" in key_false and "year=2026" in key_true

    def test_build_s3_key_news_batch_run(self):
        dt = datetime(2026, 2, 26, 18, 31, 22)
        key = build_s3_key_news_batch("run", dt)
        assert "news/transformed/crypto" in key
        assert "batch=run" in key
        assert "format=csv" in key
        assert "20260226_183122" in key

    def test_build_s3_key_news_batch_agentic(self):
        dt = datetime(2026, 2, 26, 18, 31, 22)
        key_false = build_s3_key_news_batch("run", dt, agentic=False)
        key_true = build_s3_key_news_batch("run", dt, agentic=True)
        assert "agentic=false" in key_false
        assert "agentic=true" in key_true
        assert "batch=run" in key_false and "batch=run" in key_true

    def test_build_s3_key_news_batch_week_month_year(self):
        dt = datetime(2026, 2, 26)
        w = build_s3_key_news_batch("week", dt)
        m = build_s3_key_news_batch("month", dt)
        y = build_s3_key_news_batch("year", dt)
        assert "year=2026" in w and "week=" in w
        assert "year=2026" in m and "month=02" in m
        assert "year=2026" in y and ("y2026" in y or y.endswith("y2026.csv"))

    def test_build_s3_key_news_batch_invalid(self):
        with pytest.raises(ValueError, match="batch_type"):
            build_s3_key_news_batch("invalid", datetime(2026, 2, 26))

    def test_build_s3_key_stocks(self):
        key = build_s3_key_stocks("btc-usd", datetime(2026, 2, 22))
        assert "stocks/crypto" in key
        assert "book=btc-usd" in key
        assert "year=2026" in key
        assert "month=02" in key
        assert "day=22" in key
        assert "format=csv" in key
        assert "20260222-btc-usd.csv" in key

    def test_build_s3_key_stocks_pandas_timestamp(self):
        ts = pd.Timestamp("2026-02-22")
        key = build_s3_key_stocks("eth-usd", ts)
        assert "book=eth-usd" in key
        assert "20260222" in key


class TestAgenticResultHasFailures:
    """Test agentic_result_has_failures: do not persist when any row has llm_error."""

    def test_empty_dataframe(self):
        assert agentic_result_has_failures(pd.DataFrame()) is False

    def test_none_handling(self):
        assert agentic_result_has_failures(None) is False

    def test_no_llm_error_column(self):
        df = pd.DataFrame([{"id": "1", "headline": "H", "llm_summary": "Ok"}])
        assert agentic_result_has_failures(df) is False

    def test_llm_error_all_nan(self):
        df = pd.DataFrame([
            {"id": "1", "llm_error": pd.NA},
            {"id": "2", "llm_error": None},
        ])
        assert not agentic_result_has_failures(df)

    def test_llm_error_one_failure(self):
        df = pd.DataFrame([
            {"id": "1", "llm_error": None},
            {"id": "2", "llm_error": "No module named 'openai'"},
        ])
        assert agentic_result_has_failures(df)

    def test_llm_error_all_failures(self):
        df = pd.DataFrame([
            {"id": "1", "llm_error": "OpenAI request failed"},
            {"id": "2", "llm_error": "Timeout"},
        ])
        assert agentic_result_has_failures(df)


class TestSerializeRowForNewsDb:
    """Test JSON serialization for news DB JSONB columns."""

    def test_serialize_lists_and_dicts(self):
        row = {
            "id": "1",
            "tickers": ["BTC-USD", "ETH-USD"],
            "secondary_intents": [{"intent": "market_update", "score": 0.9}],
            "keywords": ["bitcoin", "crypto"],
            "entities": [{"text": "Bitcoin", "label": "ORG"}],
        }
        out = _serialize_row_for_news_db(row)
        assert out["id"] == "1"
        assert isinstance(out["tickers"], str) and "BTC-USD" in out["tickers"]
        assert isinstance(out["secondary_intents"], str)
        assert isinstance(out["keywords"], str)
        assert isinstance(out["entities"], str)

    def test_serialize_ignores_none(self):
        row = {"id": "1", "tickers": None}
        out = _serialize_row_for_news_db(row)
        assert out["tickers"] is None

    def test_serialize_llm_themes(self):
        row = {"id": "1", "llm_themes": ["earnings", "regulation", "crypto"]}
        out = _serialize_row_for_news_db(row)
        assert isinstance(out["llm_themes"], str) and "earnings" in out["llm_themes"]

    def test_serialize_llm_financial_metrics(self):
        row = {
            "id": "1",
            "llm_financial_metrics": {
                "event_type": "macro",
                "overall_sentiment": 0.5,
                "confidence": 0.85,
            },
        }
        out = _serialize_row_for_news_db(row)
        assert isinstance(out["llm_financial_metrics"], str)
        assert "macro" in out["llm_financial_metrics"]
        assert "0.5" in out["llm_financial_metrics"]


class TestSaveTransformedNewsToPostgres:
    """Test save_transformed_news_to_postgres with agentic_enabled (mocked DB)."""

    @pytest.mark.skipif(not PSYCOPG2_AVAILABLE, reason="psycopg2 not available (imports storage)")
    def test_save_includes_agentic_enabled_false(self):
        mock_conn = MagicMock()
        df = pd.DataFrame([
            {"id": "art1", "headline": "H", "content": "C", "sentiment_label": "neutral"},
        ])
        n = save_transformed_news_to_postgres(mock_conn, df, agentic_enabled=False)
        assert n == 1
        call_args = mock_conn.save_to_postgres.call_args
        row_dict = call_args[0][0]
        assert row_dict.get("agentic_enabled") is False
        assert "llm_summary" not in row_dict or row_dict.get("llm_summary") is None

    @pytest.mark.skipif(not PSYCOPG2_AVAILABLE, reason="psycopg2 not available (imports storage)")
    def test_save_includes_agentic_enabled_true(self):
        mock_conn = MagicMock()
        df = pd.DataFrame([
            {
                "id": "art2", "headline": "H", "content": "C", "sentiment_label": "neutral",
                "llm_summary": "One-line summary.", "llm_themes": ["crypto", "regulation"],
            },
        ])
        n = save_transformed_news_to_postgres(mock_conn, df, agentic_enabled=True)
        assert n == 1
        call_args = mock_conn.save_to_postgres.call_args
        row_dict = call_args[0][0]
        assert row_dict.get("agentic_enabled") is True
        assert row_dict.get("llm_summary") == "One-line summary."


class TestUploadDataframeToS3Key:
    """Test DataFrame upload to S3 key."""

    def test_upload_calls_put_object(self):
        mock_s3 = MagicMock()
        df = pd.DataFrame([{"a": 1, "b": 2}])
        upload_dataframe_to_s3_key(mock_s3, "bucket", "prefix/file.csv", df)
        mock_s3.put_object.assert_called_once()
        call = mock_s3.put_object.call_args
        kwargs = call[1] if len(call) > 1 else {}
        assert kwargs.get("Bucket") == "bucket"
        assert kwargs.get("Key") == "prefix/file.csv"
        body = kwargs.get("Body", "")
        assert "a" in body and "b" in body


@pytest.mark.skipif(not PSYCOPG2_AVAILABLE, reason="psycopg2 not available")
class TestRunNewsEtl:
    """Test run_news_etl with mocks."""

    @pytest.fixture
    def sample_news_df(self):
        return pd.DataFrame([
            {"id": "1", "source": "Test", "headline": "H", "href": "https://x.com", "summary": "S", "content": "C", "datetime": "2026-01-27T10:00:00.000Z"},
        ])

    @patch("pipelines.etl_cli.ingest_news")
    @patch("pipelines.etl_transform.TextTransformationPipeline")
    @patch("storage.postgres.pgConn.PgConn")
    def test_run_news_etl_no_s3(
        self, mock_pg, mock_pipeline_cls, mock_ingest, sample_news_df
    ):
        from pipelines.etl_transform import run_news_etl

        mock_ingest.return_value = sample_news_df
        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = sample_news_df.assign(
            cleaned_text="C", word_count=1, sentiment_label="neutral", sentiment_score=0.0
        )
        mock_pipeline_cls.return_value = mock_pipeline
        mock_conn = MagicMock()
        mock_pg.return_value = mock_conn
        mock_conn.connection = MagicMock()
        mock_conn.set_table = MagicMock()
        mock_conn.create_table = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.connection.cursor.return_value = mock_cursor

        with patch("pipelines.etl_transform.get_settings") as mock_settings:
            mock_settings.return_value.aws.default_bucket = None
            mock_settings.return_value.aws.news_bucket = None

            result = run_news_etl(
                since="2026-01-01",
                until="2026-01-28",
                save_to_postgres=True,
                upload_s3_per_article=False,
                upload_s3_batch=None,
            )

        assert not result.empty
        mock_ingest.assert_called_once()
        mock_pipeline.transform.assert_called_once()
        assert mock_conn.set_table.called


@pytest.mark.skipif(not PSYCOPG2_AVAILABLE, reason="psycopg2 not available")
class TestRunStocksEtl:
    """Test run_stocks_etl with mocks."""

    @pytest.fixture
    def sample_stocks_df(self):
        dates = pd.date_range("2025-06-01", periods=300, freq="D")
        return pd.DataFrame({
            "ref": ["https://finance.yahoo.com"] * 300,
            "book": ["btc-usd"] * 300,
            "date": dates,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "adj_close": 100.0,
            "volume": 1_000_000,
        })

    @patch("pipelines.etl_cli.ingest_stocks")
    @patch("pipelines.etl_transform.StockTransformationPipeline")
    @patch("storage.postgres.pgConn.PgConn")
    def test_run_stocks_etl_no_s3(
        self, mock_pg, mock_pipeline_cls, mock_ingest, sample_stocks_df
    ):
        from pipelines.etl_transform import run_stocks_etl

        transformed = sample_stocks_df.copy()
        transformed["simple_return"] = 0.0
        transformed["log_return"] = 0.0
        transformed["volatility_20d"] = 0.01
        transformed["volatility_60d"] = 0.01
        transformed["sma_20"] = 100.0
        transformed["sma_50"] = 100.0
        transformed["sma_200"] = 100.0
        transformed["ema_12"] = 100.0
        transformed["ema_26"] = 100.0
        transformed["rsi_14"] = 50.0
        transformed["macd"] = 0.0
        transformed["macd_signal"] = 0.0
        transformed["macd_histogram"] = 0.0
        transformed["bb_upper"] = 101.0
        transformed["bb_middle"] = 100.0
        transformed["bb_lower"] = 99.0
        transformed["volatility_parkinson"] = 0.01
        transformed["volatility_gk"] = 0.01
        mask = (transformed["date"] >= "2026-01-01") & (transformed["date"] <= "2026-01-28")
        mock_ingest.return_value = sample_stocks_df
        mock_pipeline = MagicMock()
        mock_pipeline.transform.return_value = transformed
        mock_pipeline_cls.return_value = mock_pipeline
        mock_conn = MagicMock()
        mock_conn.connection = MagicMock()
        mock_conn.set_table = MagicMock()
        mock_conn.create_table = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.connection.cursor.return_value = mock_cursor
        mock_pg.return_value = mock_conn

        with patch("pipelines.etl_transform.get_settings") as mock_settings:
            mock_settings.return_value.aws.default_bucket = None
            mock_settings.return_value.aws.stocks_bucket = None

            result = run_stocks_etl(
                since="2026-01-01",
                until="2026-01-28",
                save_to_postgres=True,
                upload_s3=False,
            )

        assert not result.empty
        mock_ingest.assert_called_once()
        mock_pipeline.transform.assert_called_once()
