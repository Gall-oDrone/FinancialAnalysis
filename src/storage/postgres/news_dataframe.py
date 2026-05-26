"""Helpers for financial news DataFrames loaded from PostgreSQL."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Optional, Union

import pandas as pd

DateLike = Union[date, datetime, str, None]


def normalize_datetime_to_iso_z(value: Any) -> Optional[str]:
    """Serialize a datetime value to ``YYYY-MM-DDTHH:MM:SS.sssZ`` (UTC).

    Matches legacy financial news storage (e.g. ``2026-05-02T06:52:14.000Z``).
    Accepts ISO strings, locale strings (``5/23/2026  8:32:15 AM``), and datetime objects.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
    else:
        text = value

    dt = pd.to_datetime(text, utc=True, errors="coerce")
    if pd.isna(dt):
        return None

    if hasattr(dt, "to_pydatetime"):
        dt = dt.to_pydatetime()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    ms = dt.microsecond // 1000
    return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}.{ms:03d}Z"


def format_news_datetime_for_export(value: Any) -> Optional[str]:
    """Format ``datetime`` for S3 CSV/JSONL export (``YYYY-MM-DDTHH:MM:SS.sssZ`` UTC)."""
    return normalize_datetime_to_iso_z(value)


def normalize_financial_news_datetime_column(
    df: pd.DataFrame,
    column: str = "datetime",
) -> pd.DataFrame:
    """Return a copy with ``column`` normalized to ISO-8601 UTC strings."""
    if df is None or column not in df.columns:
        return df.copy() if df is not None else pd.DataFrame()

    out = df.copy()
    out[column] = out[column].apply(normalize_datetime_to_iso_z)
    return out


def filter_financial_news_by_date(
    df: pd.DataFrame,
    on_date: DateLike = None,
    *,
    date_column: str = "datetime",
    datetime_column: Optional[str] = None,
) -> pd.DataFrame:
    """Return rows whose ``date_column`` falls on the given calendar day.

    * ``on_date=None`` (default): uses **today's local calendar date** (``date.today()``).
    * ``date_column="datetime"`` (default): article publish time; matches webscraping
      ``df['datetime'].str.startswith('YYYY-MM-DD')`` plus parsed-date fallback.
    * ``date_column="created_at"``: use ``filter_financial_news_ingested_today`` instead.
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

    target = _default_target_date(on_date)
    prefix = target.strftime("%Y-%m-%d")

    if date_column == "datetime":
        str_col = df[date_column].astype(str)
        mask = str_col.str.startswith(prefix, na=False)
        parsed = pd.to_datetime(df[date_column], errors="coerce")
        if parsed.notna().any():
            mask = mask | (parsed.dt.date == target)
    else:
        parsed = pd.to_datetime(df[date_column], errors="coerce")
        mask = parsed.dt.date == target

    return df.loc[mask].copy()


def filter_financial_news_published_today(
    df: pd.DataFrame,
    on_date: DateLike = None,
) -> pd.DataFrame:
    """Rows whose article ``datetime`` is on the given day (default: today, local)."""
    return filter_financial_news_by_date(df, on_date, date_column="datetime")


def filter_financial_news_ingested_today(
    df: pd.DataFrame,
    on_date: DateLike = None,
) -> pd.DataFrame:
    """Rows inserted into Postgres on the given day (``created_at`` column)."""
    return filter_financial_news_by_date(df, on_date, date_column="created_at")


def get_financial_news_content_by_id(
    df: pd.DataFrame,
    article_id: Union[str, int],
    *,
    id_column: str = "id",
    content_column: str = "content",
) -> list:
    """Return article body text for ``article_id`` (compares ids as strings)."""
    if df is None or df.empty:
        return []
    if id_column not in df.columns or content_column not in df.columns:
        raise KeyError(
            f"Expected columns {id_column!r} and {content_column!r}; got {list(df.columns)}"
        )
    target = str(article_id).strip()
    mask = df[id_column].astype(str).str.strip() == target
    series = df.loc[mask, content_column].dropna()
    return series.tolist()


def _default_target_date(on_date: DateLike) -> date:
    """Current local calendar day when ``on_date`` is omitted."""
    if on_date is not None:
        return _coerce_to_date(on_date)
    return date.today()


def _coerce_to_date(value: DateLike) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return pd.to_datetime(value).date()
    raise TypeError(f"Unsupported date value: {value!r}")
