"""
Tests for ETL CLI helpers (especially GenAI JSONL export from Postgres to S3).
"""

from datetime import datetime
try:
    from unittest.mock import patch, MagicMock  # Python 3
except ImportError:
    from mock import patch, MagicMock  # Python 2 fallback

import pandas as pd

from pipelines.etl_cli import export_genai_to_s3_from_db


class TestExportGenaiToS3FromDb:
    """Test end-to-end helper that exports GenAI JSONL from Postgres to S3."""

    @patch("pipelines.etl_cli.export_to_s3_jsonl")
    @patch("pipelines.etl_cli.transform_news")
    @patch("pipelines.etl_cli.get_settings")
    def test_export_uses_partitioned_format_jsonl_path(
        self,
        mock_get_settings,
        mock_transform_news,
        mock_export_to_s3_jsonl,
    ):
        # Arrange: minimal transformed DataFrame
        df = pd.DataFrame(
            [
                {
                    "id": "1",
                    "headline": "H",
                    "summary": "S",
                    "cleaned_text": "Body",
                }
            ]
        )
        mock_transform_news.return_value = df

        settings = MagicMock()
        settings.aws.default_bucket = "test-bucket"
        mock_get_settings.return_value = settings

        mock_export_to_s3_jsonl.return_value = "s3://test-bucket/genai/news/year=2026/month=03/day=17/format=jsonl/news_genai_20260317_000000.jsonl"

        # Act
        uri = export_genai_to_s3_from_db(date="2026-01-27")

        # Assert
        assert uri == mock_export_to_s3_jsonl.return_value

        # Inspect call to export_to_s3_jsonl to verify prefix structure
        args, kwargs = mock_export_to_s3_jsonl.call_args
        # Signature: (df, bucket_name, prefix_path, file_name, include_embeddings=False)
        prefix_path = kwargs.get("prefix_path") or args[2]

        assert "genai/news" in prefix_path
        assert "year=" in prefix_path
        assert "month=" in prefix_path
        assert "day=" in prefix_path
        assert "format=jsonl" in prefix_path

