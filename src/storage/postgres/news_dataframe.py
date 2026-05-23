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
    datetime_column: str = "datetime",
) -> pd.DataFrame:
    """Return rows whose ``datetime`` column falls on the given calendar day.

    Works whether the column is stored as strings (e.g. ISO-8601) or ``datetime64``.
    """
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()

    target = _coerce_to_date(on_date or datetime.today())
    parsed = pd.to_datetime(df[datetime_column], errors="coerce")
    mask = parsed.dt.date == target
    return df.loc[mask].copy()


def _coerce_to_date(value: DateLike) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return pd.to_datetime(value).date()
    raise TypeError(f"Unsupported date value: {value!r}")
