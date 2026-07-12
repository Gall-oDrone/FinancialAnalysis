"""
Tests for table row extraction fallback logic (StockCollector notebook fix).

Reproduces the DYDX-USD debug dump where Yahoo Finance lazy-renders history
rows: ``WebElement.text`` is empty for off-screen rows while the value is still
present in ``innerHTML`` / ``textContent``. The extraction helpers must recover
those values so rows are not skipped as "stale/empty".
"""

import pytest

from WebScraping.src.utils.table_extraction import (
    clean_cell_text,
    extract_cell_value,
    extract_row_cells,
    is_stale_row_data,
)


class FakeCell:
    """Minimal stand-in for a Selenium ``td`` WebElement."""

    def __init__(self, text="", attributes=None):
        self._text = text
        self._attributes = attributes or {}

    @property
    def text(self):
        return self._text

    def get_attribute(self, name):
        return self._attributes.get(name)


class FakeRow:
    """Minimal stand-in for a Selenium ``tr`` WebElement."""

    def __init__(self, cells):
        self._cells = cells

    def find_elements(self, by, value):  # signature matches selenium
        return self._cells


def _row_from_innerhtml(values):
    """Off-screen row: empty .text, values only in innerHTML (the real bug)."""
    return FakeRow([FakeCell(text="", attributes={"innerHTML": v}) for v in values])


def _row_from_text(values):
    """Visible row: values present in .text."""
    return FakeRow([FakeCell(text=v) for v in values])


# Real values from the reported DYDX-USD debug dump.
VISIBLE_VALUES = ["Jul 12, 2026", "0.1278", "0.1343", "0.1268", "0.1331", "0.1331", "5,387,090"]
OFFSCREEN_VALUES = ["Jul 9, 2026", "0.1396", "0.1419", "0.1333", "0.1352", "0.1352", "9,362,457"]


class TestExtractCellValue:
    def test_uses_text_when_present(self):
        cell = FakeCell(text="0.1278", attributes={"innerHTML": "SHOULD_NOT_BE_USED"})
        assert extract_cell_value(cell) == "0.1278"

    def test_falls_back_to_innerhtml_when_text_empty(self):
        cell = FakeCell(text="", attributes={"innerHTML": "0.1396"})
        assert extract_cell_value(cell) == "0.1396"

    def test_prefers_textcontent_over_innerhtml(self):
        cell = FakeCell(text="", attributes={"textContent": "0.1396", "innerHTML": "<b>x</b>"})
        assert extract_cell_value(cell) == "0.1396"

    def test_strips_markup_from_innerhtml_fallback(self):
        cell = FakeCell(text="", attributes={"innerHTML": "<span>Jul 9, 2026</span>"})
        assert extract_cell_value(cell) == "Jul 9, 2026"

    def test_returns_empty_when_no_value_anywhere(self):
        assert extract_cell_value(FakeCell(text="", attributes={})) == ""

    def test_get_attribute_exception_is_swallowed(self):
        class Boom(FakeCell):
            def get_attribute(self, name):
                raise RuntimeError("stale element")

        assert extract_cell_value(Boom(text="")) == ""


class TestExtractRowCells:
    def test_recovers_offscreen_row(self):
        """The exact bug: a row whose .text is all empty must be recovered."""
        row = _row_from_innerhtml(OFFSCREEN_VALUES)
        assert extract_row_cells(row) == OFFSCREEN_VALUES

    def test_visible_row_unchanged(self):
        row = _row_from_text(VISIBLE_VALUES)
        assert extract_row_cells(row) == VISIBLE_VALUES

    def test_drops_placeholder_dash_cells(self):
        row = FakeRow([
            FakeCell(text="Jul 9, 2026"),
            FakeCell(text="-"),
            FakeCell(text="0.1396"),
        ])
        assert extract_row_cells(row) == ["Jul 9, 2026", "0.1396"]


class TestIsStaleRowData:
    def test_all_empty_is_stale(self):
        assert is_stale_row_data(["", "", "", "", "", "", ""]) is True

    def test_wrong_length_is_stale(self):
        assert is_stale_row_data(["Jul 9, 2026", "0.1396"]) is True

    def test_empty_list_is_stale(self):
        assert is_stale_row_data([]) is True

    def test_recovered_row_is_not_stale(self):
        assert is_stale_row_data(OFFSCREEN_VALUES) is False


class TestIntegration:
    def test_offscreen_row_no_longer_stale_after_fallback(self):
        """End-to-end: a row that was 'stale/empty' via .text is now valid."""
        row = _row_from_innerhtml(OFFSCREEN_VALUES)

        # Old behaviour (text only) would have produced all-empty -> stale.
        text_only = [c.text.strip() for c in row.find_elements("tag name", "td")]
        assert is_stale_row_data(text_only) is True

        # New behaviour recovers the values.
        recovered = extract_row_cells(row)
        assert is_stale_row_data(recovered) is False
        assert recovered == OFFSCREEN_VALUES


def test_clean_cell_text_handles_nbsp_and_none():
    assert clean_cell_text(None) == ""
    assert clean_cell_text("1,234\xa0") == "1,234"
