"""Helpers for financial news DataFrames loaded from PostgreSQL."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional, Union

import pandas as pd

DateLike = Union[date, datetime, str, None]


def filter_financial_news_by_date(
    df: pd.DataFrame,
    on_date: DateLike = None,
    *,
    date_column: str = "datetime",
    datetime_column: Optional[str] = None,
) -> pd.DataFrame:
    """Return rows whose ``date_column`` falls on the given calendar day.

    Works whether the column is stored as strings (e.g. ISO-8601) or ``datetime64``.

    For ingestion/export of rows scraped today, pass ``date_column="created_at"``.
    For rows whose *article* was published on a day, use ``date_column="datetime"`` (default).
    """
    if datetime_column is not None:
        date_column = datetime_column

    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()

    if date_column not in df.columns:
        raise KeyError(
            f"Column {date_column!r} not in DataFrame. "
            f"Available: {list(df.columns)}. "
            "Use date_column='created_at' for ingestion date or 'datetime' for article date."
        )

    target = _coerce_to_date(on_date or datetime.today())
    parsed = pd.to_datetime(df[date_column], errors="coerce")
    mask = parsed.dt.date == target
    return df.loc[mask].copy()


def filter_financial_news_ingested_today(
    df: pd.DataFrame,
    on_date: DateLike = None,
) -> pd.DataFrame:
    """Rows inserted into Postgres on the given day (``created_at`` column)."""
    return filter_financial_news_by_date(df, on_date, date_column="created_at")


def _coerce_to_date(value: DateLike) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return pd.to_datetime(value).date()
    raise TypeError(f"Unsupported date value: {value!r}")
