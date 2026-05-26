"""Tests for financial news DataFrame helpers."""

from datetime import date, datetime

import pandas as pd
import pytest

from storage.postgres.news_dataframe import (
    filter_financial_news_by_date,
    format_news_datetime_for_export,
    get_financial_news_content_by_id,
    normalize_datetime_to_iso_z,
    normalize_financial_news_datetime_column,
)


class TestFormatNewsDatetimeForExport:
    def test_space_separated_input(self):
        assert format_news_datetime_for_export("2026-05-25 18:00:00") == "2026-05-25T18:00:00.000Z"

    def test_matches_normalize(self):
        assert format_news_datetime_for_export("2025-11-25T18:30:00.000Z") == "2025-11-25T18:30:00.000Z"


class TestNormalizeDatetimeToIsoZ:
    def test_iso_z_preserved(self):
        assert normalize_datetime_to_iso_z("2026-05-02T06:52:14.000Z") == "2026-05-02T06:52:14.000Z"

    def test_locale_string_converted(self):
        result = normalize_datetime_to_iso_z("5/23/2026  8:32:15 AM")
        assert result == "2026-05-23T08:32:15.000Z"

    def test_datetime_object_converted(self):
        dt = datetime(2026, 5, 2, 6, 52, 14)
        assert normalize_datetime_to_iso_z(dt) == "2026-05-02T06:52:14.000Z"

    def test_none_and_na(self):
        assert normalize_datetime_to_iso_z(None) is None
        assert normalize_datetime_to_iso_z(pd.NA) is None


class TestFilterFinancialNewsByDate:
    def test_filters_iso_strings_by_calendar_day(self):
        df = pd.DataFrame(
            {
                "id": ["1", "2", "3"],
                "datetime": [
                    "2026-05-24T10:00:00.000Z",
                    "2026-05-23T08:32:15.000Z",
                    "2026-05-23T22:00:00.000Z",
                ],
            }
        )
        out = filter_financial_news_by_date(df, on_date="2026-05-23")
        assert len(out) == 2
        assert set(out["id"]) == {"2", "3"}

    def test_filters_after_locale_normalization(self):
        df = pd.DataFrame({"id": ["1"], "datetime": ["5/23/2026  8:32:15 AM"]})
        normalized = normalize_financial_news_datetime_column(df)
        out = filter_financial_news_by_date(normalized, on_date="2026-05-23")
        assert len(out) == 1

    def test_startswith_on_space_separated_datetime(self):
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "datetime": ["2026-05-22 16:02:26", "2026-05-23 08:36:29"],
            }
        )
        out = filter_financial_news_by_date(df, on_date="2026-05-22")
        assert len(out) == 1
        assert out["id"].iloc[0] == "1"

    def test_default_on_date_is_today(self, monkeypatch):
        class FixedDate(date):
            @classmethod
            def today(cls):
                return cls(2026, 5, 23)

        monkeypatch.setattr("storage.postgres.news_dataframe.date", FixedDate)
        df = pd.DataFrame({"datetime": ["2026-05-23T10:00:00.000Z", "2026-05-22T10:00:00.000Z"]})
        out = filter_financial_news_by_date(df)
        assert len(out) == 1

    def test_startswith_behavior_on_iso_strings(self):
        df = pd.DataFrame(
            {
                "datetime": [
                    "2026-04-07T12:00:00.000Z",
                    "2026-04-06T12:00:00.000Z",
                ],
            }
        )
        prefix = "2026-04-07"
        by_startswith = df[df["datetime"].str.startswith(prefix)]
        by_helper = filter_financial_news_by_date(df, on_date="2026-04-07")
        pd.testing.assert_frame_equal(by_startswith.reset_index(drop=True), by_helper.reset_index(drop=True))


class TestGetFinancialNewsContentById:
    def test_finds_content_with_string_target_and_int_id(self):
        df = pd.DataFrame(
            {
                "id": [1221589746717124508, 999],
                "content": ["Article body", "Other"],
            }
        )
        assert get_financial_news_content_by_id(df, "1221589746717124508") == ["Article body"]

    def test_finds_content_from_string_ids(self):
        df = pd.DataFrame({"id": ["1221589746717124508"], "content": ["From S3 csv"]})
        assert get_financial_news_content_by_id(df, 1221589746717124508) == ["From S3 csv"]

    def test_missing_id_returns_empty_list(self):
        df = pd.DataFrame({"id": ["1"], "content": ["x"]})
        assert get_financial_news_content_by_id(df, "missing") == []


class TestNormalizeFinancialNewsDatetimeColumn:
    def test_normalizes_column(self):
        df = pd.DataFrame({"datetime": ["5/23/2026  8:32:15 AM"]})
        out = normalize_financial_news_datetime_column(df)
        assert out["datetime"].iloc[0] == "2026-05-23T08:32:15.000Z"
