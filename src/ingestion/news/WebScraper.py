"""
Compatibility shim: re-export from scrapers so notebooks/scripts
that add src/ to path and do "import WebScraper" or "from WebScraper import Scrapper" still work.
"""

try:
    from .scrapers.WebScraper import Scrapper, StocksScrapper, NewsScrapper
except ImportError:
    from scrapers.WebScraper import Scrapper, StocksScrapper, NewsScrapper

__all__ = ["Scrapper", "StocksScrapper", "NewsScrapper"]
