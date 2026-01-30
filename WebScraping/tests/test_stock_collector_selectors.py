"""
Tests for stock_collector_selectors: centralized selectors and fallback helpers.
"""

import pytest
from unittest.mock import MagicMock, patch

from WebScraping.src.selectors.stock_collector_selectors import (
    YahooFinanceStockSelectors,
    find_element_with_fallbacks,
    find_element_with_fallbacks_return_selector,
    find_element_with_fallbacks_or_save_dom,
)


class TestYahooFinanceStockSelectors:
    """Test centralized selector definitions and getters."""

    def test_historical_tab_selectors_non_empty(self):
        sel = YahooFinanceStockSelectors.get_historical_tab_selectors()
        assert isinstance(sel, list)
        assert len(sel) >= 1
        for s in sel:
            assert isinstance(s, str)
            assert s.strip()

    def test_historical_menu_selectors_non_empty(self):
        sel = YahooFinanceStockSelectors.get_historical_menu_selectors()
        assert isinstance(sel, list)
        assert len(sel) >= 1

    def test_table_selectors_non_empty(self):
        sel = YahooFinanceStockSelectors.get_table_selectors()
        assert isinstance(sel, list)
        assert len(sel) >= 1

    def test_table_body_selectors_non_empty(self):
        sel = YahooFinanceStockSelectors.get_table_body_selectors()
        assert isinstance(sel, list)
        assert len(sel) >= 1

    def test_no_results_selectors_non_empty(self):
        sel = YahooFinanceStockSelectors.get_no_results_selectors()
        assert isinstance(sel, list)
        assert len(sel) >= 1

    def test_quote_nav_selectors_non_empty(self):
        sel = YahooFinanceStockSelectors.get_quote_nav_selectors()
        assert isinstance(sel, list)
        assert len(sel) >= 1

    def test_search_input_reject_selectors_non_empty(self):
        sel = YahooFinanceStockSelectors.get_search_input_reject_selectors()
        assert isinstance(sel, list)
        assert len(sel) >= 1

    def test_table_selectors_include_semantic_first(self):
        """Prefer data-test or semantic XPath as first option."""
        sel = YahooFinanceStockSelectors.get_table_selectors()
        assert any("data-test" in s or "//" in s for s in sel)

    def test_getters_return_copies(self):
        """Getters should return new lists so caller cannot mutate class state."""
        a = YahooFinanceStockSelectors.get_table_body_selectors()
        b = YahooFinanceStockSelectors.get_table_body_selectors()
        assert a is not b
        assert a == b


class TestFindElementWithFallbacks:
    """Test find_element_with_fallbacks with a mock driver."""

    def test_returns_element_when_first_selector_matches(self):
        mock_el = MagicMock()
        driver = MagicMock()
        driver.find_element.return_value = mock_el

        result = find_element_with_fallbacks(
            driver,
            ["//first", "//second"],
        )
        assert result is mock_el
        driver.find_element.assert_called_once()

    def test_returns_element_when_second_selector_matches(self):
        mock_el = MagicMock()
        driver = MagicMock()
        driver.find_element.side_effect = [Exception("not found"), mock_el]

        result = find_element_with_fallbacks(
            driver,
            ["//first", "//second"],
        )
        assert result is mock_el
        assert driver.find_element.call_count == 2

    def test_returns_none_when_all_fail(self):
        driver = MagicMock()
        driver.find_element.side_effect = Exception("not found")

        result = find_element_with_fallbacks(
            driver,
            ["//first", "//second"],
        )
        assert result is None
        assert driver.find_element.call_count == 2


class TestFindElementWithFallbacksReturnSelector:
    """Test find_element_with_fallbacks_return_selector."""

    def test_returns_element_and_selector_when_found(self):
        mock_el = MagicMock()
        driver = MagicMock()
        driver.find_element.return_value = mock_el

        el, sel = find_element_with_fallbacks_return_selector(
            driver,
            ["//first", "//second"],
        )
        assert el is mock_el
        assert sel == "//first"

    def test_returns_none_none_when_all_fail(self):
        driver = MagicMock()
        driver.find_element.side_effect = Exception("not found")

        el, sel = find_element_with_fallbacks_return_selector(
            driver,
            ["//first", "//second"],
        )
        assert el is None
        assert sel is None


class TestFindElementWithFallbacksOrSaveDom:
    """Test find_element_with_fallbacks_or_save_dom (element found path)."""

    def test_returns_element_and_none_path_when_found(self):
        mock_el = MagicMock()
        driver = MagicMock()
        driver.find_element.return_value = mock_el
        driver.page_source = "<html></html>"
        driver.current_url = "https://example.com"

        result_el, result_path = find_element_with_fallbacks_or_save_dom(
            driver,
            ["//table//tbody"],
            url="https://example.com",
        )
        assert result_el is mock_el
        assert result_path is None

    def test_returns_none_and_path_when_all_fail_and_dom_saved(self):
        driver = MagicMock()
        driver.find_element.side_effect = Exception("not found")
        driver.page_source = "<html><body></body></html>"
        driver.current_url = "https://finance.yahoo.com/quote/BTC-USD/history"

        with patch(
            "WebScraping.src.selectors.stock_collector_selectors.HTMLDOMUTILS_AVAILABLE",
            True,
        ), patch(
            "WebScraping.src.selectors.stock_collector_selectors.HTMLDOMUtils",
        ) as mock_utils_class:
            mock_utils = MagicMock()
            mock_utils.save_dom_tree.return_value = {"raw": "/tmp/dom_tree_xyz.html"}
            mock_utils_class.return_value = mock_utils

            result_el, result_path = find_element_with_fallbacks_or_save_dom(
                driver,
                ["//invalid", "//also-invalid"],
                url="https://example.com",
            )
            assert result_el is None
            assert result_path == "/tmp/dom_tree_xyz.html"
            mock_utils.save_dom_tree.assert_called_once()
