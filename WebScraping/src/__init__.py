"""
WebScraping Source Code Package

Production-ready structure:
- scrapers: core WebScraper, StocksScrapper, NewsScrapper, DataframeStore
- selectors: centralized Yahoo Finance selectors (stock, news)
- utils: HTMLDOMUtils for DOM save/validate/repair
- collectors: news and stock collector examples

Import scrapers explicitly to avoid loading Selenium/DB when not needed:
  from WebScraping.src.scrapers import Scrapper, StocksScrapper, DataFrameStore
"""

__all__ = [
    "Scrapper",
    "StocksScrapper",
    "NewsScrapper",
    "DataFrameStore",
]


def __getattr__(name):
    """Lazy import of scrapers so that selectors/utils can be used without pgConn etc."""
    if name in ("Scrapper", "StocksScrapper", "NewsScrapper", "DataFrameStore"):
        from .scrapers import Scrapper, StocksScrapper, NewsScrapper, DataFrameStore
        return {
            "Scrapper": Scrapper,
            "StocksScrapper": StocksScrapper,
            "NewsScrapper": NewsScrapper,
            "DataFrameStore": DataFrameStore,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
