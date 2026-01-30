"""
Centralized selectors for Yahoo Finance Stock / Historical data collection.

Provides semantic and fallback XPaths so that when Yahoo changes its DOM,
scrapers can try alternatives without manual code changes. Integrates with
HTMLDOMUtils to save DOM and analyze alternatives when all selectors fail
(repair workflow).
"""

from typing import List, Optional, Tuple

# Optional HTMLDOMUtils for repair-on-failure (save DOM, get alternatives)
try:
    from WebScraping.src.utils.HTMLDOMUtils import HTMLDOMUtils
    HTMLDOMUTILS_AVAILABLE = True
except ImportError:
    try:
        from ..utils.HTMLDOMUtils import HTMLDOMUtils
        HTMLDOMUTILS_AVAILABLE = True
    except ImportError:
        HTMLDOMUTILS_AVAILABLE = False


class YahooFinanceStockSelectors:
    """
    Centralized selector definitions for Yahoo Finance stock/historical pages.

    Each concept (e.g. historical tab, table body) has an ordered list of
    selectors: try the first, then fallbacks. Prefer semantic selectors
    (data-test, contains(@class,...)) over absolute paths.
    """

    # --- Historical tab link (click to open Historical Data) ---
    # Primary: newer layout nav; Fallback: older layout
    HISTORICAL_TAB_LINK = [
        "/html/body/div[1]/main/section/section/aside/section/nav/ul/li[5]/a",
        "/html/body/div[1]/div/div/div[1]/div/div[2]/div/div/div[7]/div/div/section/div/ul/li[4]/a",
    ]

    # --- Historical dropdown menu container (period/range selector) ---
    HISTORICAL_MENU_CONTAINER = [
        "/html/body/div[1]/main/section/section/section/article/div[1]/div[1]/div[1]",
        "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section",
    ]

    # --- Main section (stock data area) ---
    MAIN_SECTION_STOCK_DATA = [
        "/html/body/div[1]/main/section/section/section",
    ]

    # --- Historical prices table (full table element) ---
    STOCKS_HTML_TABLE = [
        "//table[@data-test='historical-prices']",
        "//article//table[.//tbody]",
        "/html/body/div[1]/main/section/section/section/article/div[1]/div[3]/table",
    ]

    # --- Table body (tbody) for row iteration ---
    STOCKS_HTML_TABLE_BODY = [
        "//table[@data-test='historical-prices']/tbody",
        "//article//table//tbody",
        "/html/body/div[1]/main/section/section/section/article/div[1]/div[3]/table/tbody",
    ]

    # --- No results / lookup not found ---
    NO_RESULTS_FOUND = [
        "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/section/div/div/span/span",
    ]

    # --- Quote nav (tab header presence check) ---
    QUOTE_NAV = [
        "//*[@id='quote-nav']",
    ]

    # --- Search bar (lookup) ---
    SEARCH_INPUT_REJECT_AREA = [
        "/html/body/div[1]/div/div/div[1]/div/div[3]/div[2]/div/div/div/div/div/div[1]/div/div/div/form/input",
    ]

    @classmethod
    def get_historical_tab_selectors(cls) -> List[str]:
        return list(cls.HISTORICAL_TAB_LINK)

    @classmethod
    def get_historical_menu_selectors(cls) -> List[str]:
        return list(cls.HISTORICAL_MENU_CONTAINER)

    @classmethod
    def get_table_selectors(cls) -> List[str]:
        return list(cls.STOCKS_HTML_TABLE)

    @classmethod
    def get_table_body_selectors(cls) -> List[str]:
        return list(cls.STOCKS_HTML_TABLE_BODY)

    @classmethod
    def get_no_results_selectors(cls) -> List[str]:
        return list(cls.NO_RESULTS_FOUND)

    @classmethod
    def get_quote_nav_selectors(cls) -> List[str]:
        return list(cls.QUOTE_NAV)

    @classmethod
    def get_search_input_reject_selectors(cls) -> List[str]:
        return list(cls.SEARCH_INPUT_REJECT_AREA)


def find_element_with_fallbacks(
    driver,
    selectors: List[str],
    by_xpath: bool = True,
):
    """
    Find the first element that matches any of the given selectors.

    Args:
        driver: Selenium WebDriver instance.
        selectors: Ordered list of XPath or CSS selector strings.
        by_xpath: If True, use By.XPATH; else By.CSS_SELECTOR.

    Returns:
        The first found WebElement, or None if none match.
    """
    from selenium.webdriver.common.by import By

    by = By.XPATH if by_xpath else By.CSS_SELECTOR
    for sel in selectors:
        try:
            el = driver.find_element(by, sel)
            if el is not None:
                return el
        except Exception:
            continue
    return None


def find_element_with_fallbacks_return_selector(
    driver,
    selectors: List[str],
    by_xpath: bool = True,
) -> Tuple[Optional[object], Optional[str]]:
    """
    Find the first element that matches any selector; return the element
    and the selector string that worked (useful for building child paths).

    Returns:
        (element, selector_that_worked) or (None, None).
    """
    from selenium.webdriver.common.by import By

    by = By.XPATH if by_xpath else By.CSS_SELECTOR
    for sel in selectors:
        try:
            el = driver.find_element(by, sel)
            if el is not None:
                return el, sel
        except Exception:
            continue
    return None, None


def find_element_with_fallbacks_or_save_dom(
    driver,
    selectors: List[str],
    url: Optional[str] = None,
    data_folder: str = "data",
    by_xpath: bool = True,
):
    """
    Find element using fallback selectors. If all fail and HTMLDOMUtils
    is available, save the current page DOM to disk for later analysis
    (e.g. with analyze_xpath_alternatives or an AI repair step).

    Args:
        driver: Selenium WebDriver instance.
        selectors: Ordered list of XPath (or CSS) selector strings.
        url: Optional URL for naming the saved DOM file.
        data_folder: Folder for saved DOM files (used by HTMLDOMUtils).
        by_xpath: If True, use By.XPATH; else By.CSS_SELECTOR.

    Returns:
        Tuple (element or None, saved_dom_path or None).
        If element found: (element, None).
        If all fail and DOM was saved: (None, path_to_raw_html).
        If all fail and DOM not saved: (None, None).
    """
    el = find_element_with_fallbacks(driver, selectors, by_xpath=by_xpath)
    if el is not None:
        return el, None

    if not HTMLDOMUTILS_AVAILABLE:
        return None, None

    try:
        utils = HTMLDOMUtils(data_folder=data_folder)
        url_to_use = url or (getattr(driver, "current_url", None) if driver else None)
        saved = utils.save_dom_tree(
            driver.page_source,
            url=url_to_use,
            prettify=True,
        )
        return None, saved.get("raw")
    except Exception:
        return None, None
