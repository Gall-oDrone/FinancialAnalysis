"""
Scrapers package: core web scraping classes and base scraper logic.
"""

from .WebScraper import Scrapper, StocksScrapper, NewsScrapper
from .DataframeStore import DataFrameStore

__all__ = [
    "Scrapper",
    "StocksScrapper",
    "NewsScrapper",
    "DataFrameStore",
]
