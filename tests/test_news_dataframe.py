"""Tests for financial news DataFrame helpers."""

from datetime import datetime

import pandas as pd
import pytest

from storage.postgres.news_dataframe import (
    filter_financial_news_by_date,
    normalize_datetime_to_iso_z,
    normalize_financial_news_datetime_column,
)


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


class TestNormalizeFinancialNewsDatetimeColumn:
    def test_normalizes_column(self):
        df = pd.DataFrame({"datetime": ["5/23/2026  8:32:15 AM"]})
        out = normalize_financial_news_datetime_column(df)
        assert out["datetime"].iloc[0] == "2026-05-23T08:32:15.000Z"
