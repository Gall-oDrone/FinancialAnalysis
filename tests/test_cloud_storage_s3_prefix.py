"""Tests for S3 Hive-style datetime prefix builder."""

from storage.cloud.CloudStorage import build_s3_datetime_partition_prefix


class TestBuildS3DatetimePartitionPrefix:
    def test_day_only_skips_empty_hour_minute(self):
        prefix = build_s3_datetime_partition_prefix(
            "news/crypto/",
            year="2026",
            month="05",
            day="24",
            hour="",
            minute="",
        )
        assert prefix == "news/crypto/year=2026/month=05/day=24/"

    def test_full_partition(self):
        prefix = build_s3_datetime_partition_prefix(
            "news/crypto",
            year=2026,
            month=5,
            day=24,
            hour=4,
            minute=50,
            second=0,
        )
        assert prefix == "news/crypto/year=2026/month=05/day=24/hour=04/minute=50/second=00/"
