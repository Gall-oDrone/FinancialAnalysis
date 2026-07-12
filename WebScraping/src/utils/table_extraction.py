"""
Table row extraction utilities for Selenium-scraped Yahoo Finance history tables.

Yahoo Finance lazy-renders the history table: rows outside the viewport return
an empty string for ``WebElement.text`` even though the value is present in the
DOM (``textContent`` / ``innerText`` / ``innerHTML``). Reading ``.text`` alone
causes those rows to look like ``['', '', '', '', '', '', '']`` and be skipped
as "stale/empty". These helpers fall back to the DOM attributes so every row in
the requested range is recovered.

The helpers intentionally avoid importing selenium so they stay unit-testable
with lightweight fakes; ``_BY_TAG_NAME`` is the literal value of
``selenium.webdriver.common.by.By.TAG_NAME``.
"""

import re

_HTML_TAG_RE = re.compile(r"<[^>]+>")

# Value of selenium.webdriver.common.by.By.TAG_NAME (kept literal to avoid the
# selenium import in environments/tests that don't have a browser stack).
_BY_TAG_NAME = "tag name"

# Attributes tried, in order, when WebElement.text is empty.
_CELL_FALLBACK_ATTRS = ("textContent", "innerText", "innerHTML")

EXPECTED_COLUMN_COUNT = 7


def clean_cell_text(value):
    """Reduce a raw DOM value (possibly containing markup) to plain text."""
    if value is None:
        return ""
    text = _HTML_TAG_RE.sub("", str(value))
    return text.replace("\xa0", " ").strip()


def extract_cell_value(cell):
    """Return a cell's value, falling back to DOM attributes when ``.text`` is empty.

    Selenium's ``.text`` is empty for elements rendered outside the viewport, so
    fall back to ``textContent`` -> ``innerText`` -> ``innerHTML``.
    """
    try:
        text = (cell.text or "").strip()
    except Exception:
        text = ""
    if text:
        return text

    for attr in _CELL_FALLBACK_ATTRS:
        try:
            value = cell.get_attribute(attr)
        except Exception:
            value = None
        cleaned = clean_cell_text(value)
        if cleaned:
            return cleaned
    return ""


def extract_row_cells(row):
    """Extract non-placeholder cell values from a table row ``WebElement``.

    Mirrors the notebook's original behaviour (dropping ``"-"`` placeholder
    cells) but recovers lazily-rendered values via :func:`extract_cell_value`.
    """
    columns = row.find_elements(_BY_TAG_NAME, "td")
    values = []
    for col in columns:
        value = extract_cell_value(col)
        if value != "-":
            values.append(value)
    return values


def is_stale_row_data(row_data, expected_cols=EXPECTED_COLUMN_COUNT):
    """True when a row has the wrong shape or is entirely empty."""
    if not row_data or len(row_data) != expected_cols:
        return True
    return all(not str(cell).strip() for cell in row_data)
