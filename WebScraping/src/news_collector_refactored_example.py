"""
Production-ready News Collector - Refactored Example

This module demonstrates how the NewsCollector-Staging.ipynb notebook
should be refactored following OOP best practices and production standards.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import wraps
from typing import List, Optional, Dict, Generator
import time
import uuid

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

from config.settings import get_settings
from utils.logging import get_logger
from Storage.pgConn import PgConn

# Initialize logger
logger = get_logger(__name__)


# ============================================================================
# Custom Exceptions
# ============================================================================

class ScrapingError(Exception):
    """Base exception for scraping operations."""
    pass


class ElementNotFoundError(ScrapingError):
    """Raised when an expected element is not found."""
    pass


class PageLoadError(ScrapingError):
    """Raised when a page fails to load."""
    pass


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class NewsArticle:
    """Data class representing a news article."""
    id: int
    source: str
    headline: str
    href: str
    summary: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    minsread: Optional[str] = None
    datetime: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for database storage."""
        return {
            "id": self.id,
            "source": self.source,
            "headline": self.headline,
            "href": self.href,
            "summary": self.summary,
            "content": self.content,
            "author": self.author,
            "minsread": self.minsread,
            "datetime": self.datetime
        }


# ============================================================================
# Configuration
# ============================================================================

class NewsScrapingConfig:
    """Configuration for news scraping operations."""
    
    def __init__(self):
        settings = get_settings()
        scraping_config = settings.scraping
        
        self.debug: bool = scraping_config.debug
        self.headless: bool = scraping_config.headless
        self.timeout: int = scraping_config.timeout
        self.user_agent: str = scraping_config.user_agent
        
        # News-specific settings
        self.max_retries: int = 3
        self.retry_delay: float = 1.0
        self.scroll_delay: float = 3.0
        self.max_scrolls: int = 10
        self.topics: List[str] = ["crypto"]
        self.timeframe_filter: List[str] = ["yesterday", "days"]
        
        # Selector timeouts
        self.element_wait_timeout: float = 3.0
        self.page_load_timeout: int = 30


# ============================================================================
# Decorators
# ============================================================================

def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """Decorator to retry function on failure with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (TimeoutException, NoSuchElementException, PageLoadError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}, retrying...",
                            extra={"function": func.__name__, "attempt": attempt + 1}
                        )
                        time.sleep(delay * (2 ** attempt))  # Exponential backoff
                    else:
                        logger.error(
                            f"All {max_retries} attempts failed for {func.__name__}",
                            extra={"function": func.__name__},
                            exc_info=True
                        )
                except Exception as e:
                    # Don't retry on unexpected errors
                    logger.error(
                        f"Unexpected error in {func.__name__}",
                        exc_info=True
                    )
                    raise
            
            if last_exception:
                raise last_exception
        return wrapper
    return decorator


# ============================================================================
# Selector Strategies
# ============================================================================

class SelectorStrategy(ABC):
    """Abstract base class for element selection strategies."""
    
    @abstractmethod
    def find_element(self, driver, selector: str):
        """Find a single element."""
        pass
    
    @abstractmethod
    def find_elements(self, driver, selector: str):
        """Find multiple elements."""
        pass


class XPathSelectorStrategy(SelectorStrategy):
    """XPath-based selector strategy."""
    
    def find_element(self, driver, selector: str):
        try:
            return driver.find_element(By.XPATH, selector)
        except NoSuchElementException:
            raise ElementNotFoundError(f"Element not found with XPath: {selector}")
    
    def find_elements(self, driver, selector: str):
        return driver.find_elements(By.XPATH, selector)


class CSSSelectorStrategy(SelectorStrategy):
    """CSS selector-based strategy."""
    
    def find_element(self, driver, selector: str):
        try:
            return driver.find_element(By.CSS_SELECTOR, selector)
        except NoSuchElementException:
            raise ElementNotFoundError(f"Element not found with CSS: {selector}")
    
    def find_elements(self, driver, selector: str):
        return driver.find_elements(By.CSS_SELECTOR, selector)


# ============================================================================
# Selector Configuration
# ============================================================================

class YahooFinanceSelectors:
    """Centralized selector definitions for Yahoo Finance."""
    
    # List selectors
    STREAM_ITEMS = "//ul[contains(@class, 'stream-items')]"
    NEWS_ITEM = ".//li[contains(@class, 'stream-item')]"
    
    # Content selectors
    HEADLINE_LARGE = ".//a[contains(@class, 'titles-link')]"
    HEADLINE_SMALL = ".//a[contains(@class, 'subtle-link')]"
    SUMMARY = ".//div[contains(@class, 'summary')]"
    CONTENT = ".//div[contains(@class, 'body-wrap')]"
    
    # Metadata selectors
    SOURCE_AND_TIMESTAMP = ".//div[contains(@class, 'publishing')]"
    AUTHOR = ".//div[contains(@class, 'byline-attr-author')]"
    DATETIME = ".//time[@datetime]"
    MINS_READ = ".//span[contains(@class, 'minsread')]"
    
    # Full article selectors
    FULL_ARTICLE = "article"
    READ_MORE_BUTTON = ".//button[contains(@class, 'readmore-button')]"
    
    @classmethod
    def get_headline_selector(cls, page_variant: str = "default") -> str:
        """Get headline selector based on page variant."""
        selectors = {
            "default": cls.HEADLINE_LARGE,
            "large": cls.HEADLINE_LARGE,
            "small": cls.HEADLINE_SMALL
        }
        return selectors.get(page_variant, cls.HEADLINE_LARGE)


# ============================================================================
# WebDriver Management
# ============================================================================

class WebDriverManager:
    """Manages WebDriver lifecycle."""
    
    def __init__(self, config: NewsScrapingConfig):
        self.config = config
        self.driver: Optional[webdriver.Chrome] = None
    
    def create_driver(self) -> webdriver.Chrome:
        """Create and configure WebDriver."""
        if self.driver:
            return self.driver
        
        options = webdriver.ChromeOptions()
        options.add_argument(f"--user-agent={self.config.user_agent}")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-extensions")
        options.add_argument("--start-maximized")
        
        if self.config.headless:
            options.add_argument('--headless')
        
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--ignore-certificate-errors')
        
        service = ChromeService(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.set_page_load_timeout(self.config.page_load_timeout)
        
        logger.info("WebDriver created successfully")
        return self.driver
    
    def quit(self):
        """Quit WebDriver."""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("WebDriver closed")


# ============================================================================
# Page Scraper
# ============================================================================

class PageScraper:
    """Handles page navigation and interaction."""
    
    def __init__(self, driver: webdriver.Chrome, config: NewsScrapingConfig):
        self.driver = driver
        self.config = config
    
    @retry_on_failure(max_retries=3, delay=1.0)
    def load_url(self, url: str) -> bool:
        """Load URL with retry logic."""
        try:
            logger.info(f"Loading URL: {url}")
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, self.config.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Verify we're on the correct URL
            if self.driver.current_url != url:
                logger.warning(
                    f"URL mismatch. Expected: {url}, Got: {self.driver.current_url}"
                )
                return False
            
            logger.info(f"Successfully loaded URL: {url}")
            return True
            
        except TimeoutException as e:
            logger.error(f"Timeout loading URL: {url}", exc_info=True)
            raise PageLoadError(f"Failed to load URL: {url}") from e
    
    def scroll_to_bottom(self, max_scrolls: Optional[int] = None):
        """Scroll page to load all content."""
        max_scrolls = max_scrolls or self.config.max_scrolls
        logger.info("Scrolling to bottom of page...")
        
        last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
        scrolls = 0
        
        while scrolls < max_scrolls:
            # Scroll down
            self.driver.execute_script(
                "window.scrollTo(0, document.documentElement.scrollHeight);"
            )
            
            # Wait for new content to load
            time.sleep(self.config.scroll_delay)
            
            # Calculate new scroll height
            new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            
            if new_height == last_height:
                logger.info("Reached bottom of page")
                break
            
            last_height = new_height
            scrolls += 1
        
        logger.info(f"Scrolling completed after {scrolls} scrolls")


# ============================================================================
# News Extractor
# ============================================================================

class NewsExtractor:
    """Extracts news data from HTML elements."""
    
    def __init__(self, selector_strategy: SelectorStrategy, config: NewsScrapingConfig):
        self.selector = selector_strategy
        self.config = config
        self.selectors = YahooFinanceSelectors()
    
    def extract_text_safe(self, element) -> Optional[str]:
        """Safely extract text from element."""
        try:
            return element.text.strip() if element.text else None
        except Exception as e:
            logger.warning("Failed to extract text from element", exc_info=True)
            return None
    
    def extract_attribute_safe(self, element, attribute: str) -> Optional[str]:
        """Safely extract attribute from element."""
        try:
            return element.get_attribute(attribute)
        except Exception as e:
            logger.warning(f"Failed to extract attribute {attribute}", exc_info=True)
            return None
    
    def extract_source_and_timestamp(self, element) -> tuple[Optional[str], Optional[str]]:
        """Extract source and timestamp from publishing element."""
        try:
            text = self.extract_text_safe(element)
            if not text:
                return None, None
            
            # Split by bullet point
            parts = text.split('•')
            if len(parts) == 2:
                source = parts[0].strip()
                timestamp = parts[1].strip()
                return source, timestamp
            
            return None, None
        except Exception as e:
            logger.warning("Failed to extract source and timestamp", exc_info=True)
            return None, None
    
    def is_in_timeframe(self, timestamp: Optional[str]) -> bool:
        """Check if timestamp is within configured timeframe."""
        if not timestamp:
            return False
        
        for timeframe in self.config.timeframe_filter:
            if timeframe.lower() in timestamp.lower():
                logger.debug(f"Article timestamp '{timestamp}' matches timeframe '{timeframe}'")
                return True
        
        return False
    
    def extract_article_from_element(self, element, driver) -> Optional[NewsArticle]:
        """Extract article data from a list item element."""
        try:
            article = NewsArticle(
                id=uuid.uuid4().int,
                source="",
                headline="",
                href=""
            )
            
            # Extract headline and href
            try:
                headline_elem = self.selector.find_element(element, self.selectors.HEADLINE_LARGE)
                article.headline = self.extract_text_safe(headline_elem)
                article.href = self.extract_attribute_safe(headline_elem, "href")
            except ElementNotFoundError:
                try:
                    headline_elem = self.selector.find_element(element, self.selectors.HEADLINE_SMALL)
                    article.headline = self.extract_text_safe(headline_elem)
                    article.href = self.extract_attribute_safe(headline_elem, "href")
                except ElementNotFoundError:
                    logger.warning("Could not find headline element")
                    return None
            
            # Extract summary
            try:
                summary_elem = self.selector.find_element(element, self.selectors.SUMMARY)
                article.summary = self.extract_text_safe(summary_elem)
            except ElementNotFoundError:
                pass  # Summary is optional
            
            # Extract source and timestamp
            try:
                source_elem = self.selector.find_element(element, self.selectors.SOURCE_AND_TIMESTAMP)
                source, timestamp = self.extract_source_and_timestamp(source_elem)
                
                if timestamp and self.is_in_timeframe(timestamp):
                    logger.info(f"Article filtered out due to timeframe: {timestamp}")
                    return None
                
                article.source = source or ""
            except ElementNotFoundError:
                pass  # Source is optional
            
            if not article.headline or not article.href:
                logger.warning("Article missing required fields (headline or href)")
                return None
            
            logger.debug(f"Extracted article: {article.headline[:50]}...")
            return article
            
        except Exception as e:
            logger.error("Failed to extract article from element", exc_info=True)
            return None
    
    def extract_articles_from_page(self, driver: webdriver.Chrome) -> List[NewsArticle]:
        """Extract all articles from the current page."""
        articles = []
        
        try:
            # Find the stream items container
            stream_container = self.selector.find_element(
                driver, 
                YahooFinanceSelectors.STREAM_ITEMS
            )
            
            # Find all news items
            news_items = self.selector.find_elements(
                stream_container,
                YahooFinanceSelectors.NEWS_ITEM
            )
            
            logger.info(f"Found {len(news_items)} news items on page")
            
            for item in news_items:
                article = self.extract_article_from_element(item, driver)
                if article:
                    articles.append(article)
            
            logger.info(f"Successfully extracted {len(articles)} articles")
            
        except ElementNotFoundError as e:
            logger.error("Could not find news items container", exc_info=True)
        except Exception as e:
            logger.error("Failed to extract articles from page", exc_info=True)
        
        return articles
    
    def extract_full_article_content(self, driver: webdriver.Chrome, article: NewsArticle) -> NewsArticle:
        """Extract full content from article page."""
        try:
            # Click read more button if present
            try:
                read_more = driver.find_element(By.CSS_SELECTOR, ".readmore-button")
                read_more.click()
                time.sleep(1)
            except NoSuchElementException:
                pass  # Button might not exist
            
            # Extract content
            try:
                content_elem = driver.find_element(By.CSS_SELECTOR, ".body-wrap")
                article.content = self.extract_text_safe(content_elem)
            except NoSuchElementException:
                logger.warning("Could not find article content")
            
            # Extract author
            try:
                author_elem = driver.find_element(By.CSS_SELECTOR, ".byline-attr-author")
                article.author = self.extract_text_safe(author_elem)
            except NoSuchElementException:
                pass
            
            # Extract datetime
            try:
                datetime_elem = driver.find_element(By.CSS_SELECTOR, "time[datetime]")
                article.datetime = self.extract_attribute_safe(datetime_elem, "datetime")
            except NoSuchElementException:
                pass
            
            # Extract mins read
            try:
                minsread_elem = driver.find_element(By.CSS_SELECTOR, ".minsread")
                article.minsread = self.extract_text_safe(minsread_elem)
            except NoSuchElementException:
                pass
            
            logger.debug(f"Extracted full content for article: {article.headline[:50]}...")
            
        except Exception as e:
            logger.error("Failed to extract full article content", exc_info=True)
        
        return article


# ============================================================================
# Data Repository
# ============================================================================

class NewsRepository:
    """Handles data persistence for news articles."""
    
    def __init__(self, db_conn: PgConn):
        self.db_conn = db_conn
        self.header = ["id", "source", "headline", "href", "summary", "content", "author", "minsread", "datetime"]
    
    @contextmanager
    def get_connection(self) -> Generator:
        """Context manager for database operations."""
        try:
            self.db_conn.reopen_connection()
            yield self.db_conn
        except Exception as e:
            logger.error("Database connection error", exc_info=True)
            raise
        finally:
            # Don't close here, let the caller manage lifecycle
            pass
    
    def save_article(self, article: NewsArticle) -> bool:
        """Save a single article to database."""
        try:
            with self.get_connection() as conn:
                article_dict = article.to_dict()
                self.db_conn.save_to_postgres(article_dict, self.header)
                logger.debug(f"Saved article: {article.headline[:50]}...")
                return True
        except Exception as e:
            logger.error(f"Failed to save article: {article.headline[:50]}...", exc_info=True)
            return False
    
    def save_articles(self, articles: List[NewsArticle]) -> int:
        """Save multiple articles to database."""
        saved_count = 0
        for article in articles:
            if self.save_article(article):
                saved_count += 1
        logger.info(f"Saved {saved_count}/{len(articles)} articles to database")
        return saved_count


# ============================================================================
# Main News Collector
# ============================================================================

class NewsCollector:
    """Main orchestrator for news collection."""
    
    def __init__(self, config: Optional[NewsScrapingConfig] = None):
        self.config = config or NewsScrapingConfig()
        self.driver_manager: Optional[WebDriverManager] = None
        self.page_scraper: Optional[PageScraper] = None
        self.extractor: Optional[NewsExtractor] = None
        self.repository: Optional[NewsRepository] = None
    
    def __enter__(self):
        """Context manager entry."""
        self._initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self._cleanup()
    
    def _initialize(self):
        """Initialize all components."""
        logger.info("Initializing NewsCollector...")
        
        # Initialize WebDriver
        self.driver_manager = WebDriverManager(self.config)
        driver = self.driver_manager.create_driver()
        
        # Initialize page scraper
        self.page_scraper = PageScraper(driver, self.config)
        
        # Initialize extractor with XPath strategy
        selector_strategy = XPathSelectorStrategy()
        self.extractor = NewsExtractor(selector_strategy, self.config)
        
        logger.info("NewsCollector initialized successfully")
    
    def _cleanup(self):
        """Cleanup resources."""
        logger.info("Cleaning up NewsCollector...")
        
        if self.driver_manager:
            self.driver_manager.quit()
        
        if self.repository and self.repository.db_conn:
            self.repository.db_conn.close_connection()
        
        logger.info("Cleanup completed")
    
    def set_repository(self, db_conn: PgConn):
        """Set the database repository."""
        self.repository = NewsRepository(db_conn)
    
    def collect_news_for_topic(self, topic: str) -> List[NewsArticle]:
        """Collect news for a specific topic."""
        articles = []
        
        try:
            # Build URL
            url = f"https://finance.yahoo.com/topic/{topic}/"
            
            # Load page
            if not self.page_scraper.load_url(url):
                logger.error(f"Failed to load page for topic: {topic}")
                return articles
            
            # Scroll to load all content
            self.page_scraper.scroll_to_bottom()
            
            # Extract articles
            articles = self.extractor.extract_articles_from_page(self.driver_manager.driver)
            
            logger.info(f"Collected {len(articles)} articles for topic: {topic}")
            
        except Exception as e:
            logger.error(f"Failed to collect news for topic: {topic}", exc_info=True)
        
        return articles
    
    def enrich_articles_with_full_content(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Enrich articles by fetching full content from their pages."""
        enriched = []
        
        for article in articles:
            try:
                logger.info(f"Fetching full content for: {article.headline[:50]}...")
                
                # Load article page
                if not self.page_scraper.load_url(article.href):
                    logger.warning(f"Failed to load article page: {article.href}")
                    enriched.append(article)  # Add without enrichment
                    continue
                
                # Extract full content
                enriched_article = self.extractor.extract_full_article_content(
                    self.driver_manager.driver,
                    article
                )
                enriched.append(enriched_article)
                
                # Small delay between requests
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to enrich article: {article.href}", exc_info=True)
                enriched.append(article)  # Add without enrichment
        
        logger.info(f"Enriched {len(enriched)} articles with full content")
        return enriched
    
    def collect_and_save(self, topics: Optional[List[str]] = None) -> Dict[str, int]:
        """Collect news and save to database."""
        topics = topics or self.config.topics
        results = {}
        
        try:
            for topic in topics:
                logger.info(f"Starting collection for topic: {topic}")
                
                # Collect articles
                articles = self.collect_news_for_topic(topic)
                
                # Enrich with full content
                enriched_articles = self.enrich_articles_with_full_content(articles)
                
                # Save to database
                if self.repository:
                    saved_count = self.repository.save_articles(enriched_articles)
                    results[topic] = saved_count
                else:
                    logger.warning("No repository configured, articles not saved")
                    results[topic] = len(enriched_articles)
            
            logger.info("News collection completed", extra={"results": results})
            
        except Exception as e:
            logger.error("Failed during news collection", exc_info=True)
            raise
        
        return results


# ============================================================================
# Usage Example
# ============================================================================

def main():
    """Example usage of the refactored NewsCollector."""
    
    # Initialize database connection
    from Storage import PostgresSQL_table_queries
    
    db_conn = PgConn()
    db_conn.set_table(PostgresSQL_table_queries.FINANCIAL_NEWS_TABLE_NAME)
    db_conn.init_db(PostgresSQL_table_queries.HISTORICAL_FINANCIAL_NEWS_TABLE_QUERY_241118)
    
    # Use NewsCollector with context manager
    with NewsCollector() as collector:
        # Set repository
        collector.set_repository(db_conn)
        
        # Collect and save news
        results = collector.collect_and_save(topics=["crypto"])
        
        logger.info(f"Collection completed: {results}")
    
    # Database connection cleanup
    db_conn.close_connection()


if __name__ == "__main__":
    main()

