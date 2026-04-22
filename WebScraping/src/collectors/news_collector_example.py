"""
Production-ready News Collector Example Implementation

This module demonstrates how the NewsCollector-Staging.ipynb could be refactored
following OOP best practices and production standards.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Iterator
from contextlib import contextmanager
import logging

from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config.settings import get_settings
from utils.logging import get_logger


# ============================================================================
# Domain Models
# ============================================================================

@dataclass
class NewsArticle:
    """Domain model representing a news article."""
    id: int
    source: str
    headline: str
    href: str
    summary: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    minsread: Optional[str] = None
    datetime: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate article data."""
        if not self.headline or not self.headline.strip():
            raise ValueError("Headline cannot be empty")
        if not self.href or 'yahoo.com' not in self.href:
            raise ValueError("Invalid href: must be a Yahoo Finance URL")
    
    def to_dict(self) -> Dict:
        """Convert article to dictionary for storage."""
        return {
            'id': self.id,
            'source': self.source,
            'headline': self.headline,
            'href': self.href,
            'summary': self.summary,
            'content': self.content,
            'author': self.author,
            'minsread': self.minsread,
            'datetime': self.datetime.isoformat() if self.datetime else None
        }


# ============================================================================
# Exceptions
# ============================================================================

class NewsScrapingError(Exception):
    """Base exception for news scraping errors."""
    pass


class ArticleExtractionError(NewsScrapingError):
    """Raised when article extraction fails."""
    pass


class TimeframeExceededError(NewsScrapingError):
    """Raised when article is outside desired timeframe."""
    pass


# ============================================================================
# Repository Pattern
# ============================================================================

class NewsRepository(ABC):
    """Abstract repository for news articles."""
    
    @abstractmethod
    def save(self, article: NewsArticle) -> None:
        """Save a news article."""
        pass
    
    @abstractmethod
    def find_by_href(self, href: str) -> Optional[NewsArticle]:
        """Find article by href."""
        pass
    
    @abstractmethod
    def find_missing_content(self) -> List[NewsArticle]:
        """Find articles missing full content."""
        pass


class PostgreSQLNewsRepository(NewsRepository):
    """PostgreSQL implementation of NewsRepository."""
    
    def __init__(self, db_connection, table_name: str):
        self.db_connection = db_connection
        self.table_name = table_name
        self.logger = get_logger(__name__)
    
    def save(self, article: NewsArticle) -> None:
        """Save article to database."""
        try:
            data = article.to_dict()
            header = list(data.keys())
            self.db_connection.save_to_postgres(data, header)
            self.logger.debug(f"Saved article: {article.headline[:50]}...")
        except Exception as e:
            self.logger.error(f"Failed to save article {article.id}: {e}")
            raise
    
    def find_by_href(self, href: str) -> Optional[NewsArticle]:
        """Find article by href (implementation needed)."""
        # TODO: Implement database query
        return None
    
    def find_missing_content(self) -> List[NewsArticle]:
        """Find articles missing full content (implementation needed)."""
        # TODO: Implement database query
        return []


# ============================================================================
# WebDriver Management
# ============================================================================

class WebDriverManager:
    """Manages WebDriver lifecycle with proper resource cleanup."""
    
    def __init__(self, config):
        self.config = config
        self.driver: Optional[webdriver.Chrome] = None
        self.logger = get_logger(__name__)
    
    def __enter__(self):
        """Context manager entry."""
        self.driver = self._create_driver()
        return self.driver
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup."""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("WebDriver closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing WebDriver: {e}")
        return False
    
    def _create_driver(self) -> webdriver.Chrome:
        """Create and configure Chrome WebDriver."""
        from selenium.webdriver.chrome.service import Service as ChromeService
        from webdriver_manager.chrome import ChromeDriverManager
        
        options = webdriver.ChromeOptions()
        options.add_argument(f"--user-agent={self.config.scraping.user_agent}")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-extensions")
        options.add_argument("--start-maximized")
        
        if not self.config.scraping.debug:
            options.add_argument('--headless')
        
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--ignore-certificate-errors')
        
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        self.logger.info("WebDriver created successfully")
        return driver


# ============================================================================
# HTML Parsing & Extraction
# ============================================================================

class ArticleExtractor:
    """Extracts news articles from HTML elements."""
    
    def __init__(self, config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.timeframe_keywords = config.get('article_timeframe', ['yesterday', 'days'])
    
    def extract_article(self, element: WebElement) -> Optional[NewsArticle]:
        """
        Extract a single news article from HTML element.
        
        Args:
            element: WebElement containing article data
            
        Returns:
            NewsArticle if extraction successful, None otherwise
        """
        try:
            headline = self._extract_headline(element)
            href = self._extract_href(element)
            source, timestamp = self._extract_metadata(element)
            
            # Check if article is within desired timeframe
            if self._is_outside_timeframe(timestamp):
                raise TimeframeExceededError(f"Article outside timeframe: {timestamp}")
            
            summary = self._extract_summary(element)
            
            return NewsArticle(
                id=self._generate_id(),
                headline=headline,
                href=href,
                source=source,
                summary=summary,
                datetime=None  # Will be populated from full article page
            )
        except TimeframeExceededError:
            raise
        except Exception as e:
            self.logger.warning(f"Failed to extract article: {e}")
            raise ArticleExtractionError(f"Extraction failed: {e}") from e
    
    def _extract_headline(self, element: WebElement) -> str:
        """Extract headline from element."""
        try:
            # Try multiple selectors for robustness
            selectors = [
                ".//a[@class='titles-link']",
                ".//h3",
                ".//div[@class='headline']"
            ]
            
            for selector in selectors:
                try:
                    headline_elem = element.find_element(By.XPATH, selector)
                    headline = headline_elem.text.strip()
                    if headline:
                        return headline
                except NoSuchElementException:
                    continue
            
            raise NoSuchElementException("Could not find headline")
        except Exception as e:
            self.logger.error(f"Error extracting headline: {e}")
            raise
    
    def _extract_href(self, element: WebElement) -> str:
        """Extract article URL from element."""
        try:
            link = element.find_element(By.XPATH, ".//a")
            href = link.get_attribute("href")
            if not href or 'yahoo.com' not in href:
                raise ValueError(f"Invalid href: {href}")
            return href
        except Exception as e:
            self.logger.error(f"Error extracting href: {e}")
            raise
    
    def _extract_metadata(self, element: WebElement) -> tuple[str, str]:
        """Extract source and timestamp from element."""
        try:
            metadata_elem = element.find_element(By.XPATH, ".//div[@class='publishing']")
            text = metadata_elem.text.strip()
            
            # Parse "Source • timestamp" format
            if '•' in text:
                parts = text.split('•')
                source = parts[0].strip()
                timestamp = parts[1].strip() if len(parts) > 1 else ""
            else:
                source = text
                timestamp = ""
            
            return source, timestamp
        except Exception as e:
            self.logger.warning(f"Error extracting metadata: {e}")
            return "Unknown", ""
    
    def _extract_summary(self, element: WebElement) -> Optional[str]:
        """Extract summary from element."""
        try:
            summary_elem = element.find_element(By.XPATH, ".//div[@class='summary']")
            return summary_elem.text.strip()
        except NoSuchElementException:
            return None
    
    def _is_outside_timeframe(self, timestamp: str) -> bool:
        """Check if timestamp is outside desired timeframe."""
        if not timestamp:
            return False
        
        for keyword in self.timeframe_keywords:
            if keyword in timestamp.lower():
                return True
        return False
    
    def _generate_id(self) -> int:
        """Generate unique ID for article."""
        import uuid
        return uuid.uuid4().int


class FullArticleExtractor:
    """Extracts full article content from article page."""
    
    def __init__(self, driver: webdriver.Chrome, logger: logging.Logger):
        self.driver = driver
        self.logger = logger
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((TimeoutException, NoSuchElementException))
    )
    def extract_full_content(self, article: NewsArticle) -> NewsArticle:
        """Extract full content from article page."""
        try:
            self.driver.get(article.href)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
            
            # Extract full content
            content = self._extract_content()
            author = self._extract_author()
            datetime_str = self._extract_datetime()
            minsread = self._extract_minsread()
            
            # Update article with full content
            article.content = content
            article.author = author
            article.minsread = minsread
            if datetime_str:
                article.datetime = self._parse_datetime(datetime_str)
            
            return article
        except Exception as e:
            self.logger.error(f"Failed to extract full content for {article.href}: {e}")
            raise
    
    def _extract_content(self) -> str:
        """Extract article body content."""
        try:
            content_elem = self.driver.find_element(By.CLASS_NAME, "caas-body-content")
            return content_elem.text.strip()
        except NoSuchElementException:
            self.logger.warning("Content element not found, trying alternative selector")
            try:
                content_elem = self.driver.find_element(By.XPATH, ".//article//div[@class='body']")
                return content_elem.text.strip()
            except NoSuchElementException:
                return ""
    
    def _extract_author(self) -> Optional[str]:
        """Extract article author."""
        try:
            author_elem = self.driver.find_element(By.CLASS_NAME, "byline-attr-author")
            return author_elem.text.strip()
        except NoSuchElementException:
            return None
    
    def _extract_datetime(self) -> Optional[str]:
        """Extract article publication datetime."""
        try:
            time_elem = self.driver.find_element(By.TAG_NAME, "time")
            return time_elem.get_attribute("datetime")
        except NoSuchElementException:
            return None
    
    def _extract_minsread(self) -> Optional[str]:
        """Extract estimated reading time."""
        try:
            minsread_elem = self.driver.find_element(By.XPATH, ".//span[contains(text(), 'min read')]")
            return minsread_elem.text.strip()
        except NoSuchElementException:
            return None
    
    def _parse_datetime(self, datetime_str: str) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        try:
            from dateutil import parser
            return parser.parse(datetime_str)
        except Exception as e:
            self.logger.warning(f"Failed to parse datetime {datetime_str}: {e}")
            return None


# ============================================================================
# Main Service
# ============================================================================

class NewsScrapingService:
    """Main service for scraping news articles."""
    
    def __init__(
        self,
        driver: webdriver.Chrome,
        extractor: ArticleExtractor,
        full_extractor: FullArticleExtractor,
        repository: NewsRepository,
        config: Dict,
        logger: logging.Logger
    ):
        self.driver = driver
        self.extractor = extractor
        self.full_extractor = full_extractor
        self.repository = repository
        self.config = config
        self.logger = logger
    
    def scrape_topic(self, topic: str) -> List[NewsArticle]:
        """
        Scrape news articles for a given topic.
        
        Args:
            topic: Topic to scrape (e.g., 'crypto')
            
        Returns:
            List of scraped NewsArticle objects
        """
        self.logger.info(f"Starting scrape for topic: {topic}")
        
        try:
            # Load topic page
            url = f"https://finance.yahoo.com/topic/{topic}/"
            self._load_url(url)
            
            # Scroll to load more articles
            if not self.config.get('skip_scrolling', False):
                self._scroll_to_bottom()
            
            # Extract article headers
            articles = self._extract_article_headers()
            self.logger.info(f"Found {len(articles)} articles")
            
            # Extract full content for each article
            for article in articles:
                try:
                    full_article = self.full_extractor.extract_full_content(article)
                    self.repository.save(full_article)
                    self.logger.debug(f"Saved article: {full_article.headline[:50]}...")
                except Exception as e:
                    self.logger.error(f"Failed to process article {article.href}: {e}")
                    # Continue with next article instead of failing completely
            
            self.logger.info(f"Completed scrape for topic: {topic}")
            return articles
            
        except Exception as e:
            self.logger.error(f"Error scraping topic {topic}: {e}", exc_info=True)
            raise NewsScrapingError(f"Failed to scrape topic {topic}") from e
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _load_url(self, url: str) -> None:
        """Load URL with retry logic."""
        try:
            self.driver.get(url)
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            self.logger.debug(f"Successfully loaded: {url}")
        except Exception as e:
            self.logger.error(f"Failed to load URL {url}: {e}")
            raise
    
    def _scroll_to_bottom(self) -> None:
        """Scroll page to bottom to load more articles."""
        self.logger.debug("Scrolling to bottom...")
        last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
        
        while True:
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight)")
            import time
            time.sleep(self.config.get('scroll_timeout', 3))
            
            new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        
        self.logger.debug("Finished scrolling")
    
    def _extract_article_headers(self) -> List[NewsArticle]:
        """Extract article headers from current page."""
        articles = []
        
        try:
            # Find all article elements
            article_elements = self.driver.find_elements(
                By.XPATH, 
                "//li[contains(@class, 'stream-item')]"
            )
            
            self.logger.debug(f"Found {len(article_elements)} article elements")
            
            for element in article_elements:
                try:
                    article = self.extractor.extract_article(element)
                    articles.append(article)
                except TimeframeExceededError:
                    self.logger.info("Reached articles outside timeframe, stopping extraction")
                    break
                except ArticleExtractionError as e:
                    self.logger.warning(f"Skipping article due to extraction error: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error extracting article headers: {e}")
            raise
        
        return articles


# ============================================================================
# Factory Functions
# ============================================================================

def create_news_scraping_service(
    config: Optional[Dict] = None,
    repository: Optional[NewsRepository] = None
) -> NewsScrapingService:
    """
    Factory function to create NewsScrapingService with dependencies.
    
    Args:
        config: Optional configuration dict
        repository: Optional repository instance
        
    Returns:
        Configured NewsScrapingService instance
    """
    settings = get_settings()
    logger = get_logger(__name__)
    
    # Use provided config or create default
    if config is None:
        config = {
            'topics': ['crypto'],
            'article_timeframe': ['yesterday', 'days'],
            'scroll_timeout': 3,
            'skip_scrolling': False
        }
    
    # Create WebDriver
    with WebDriverManager(settings) as driver:
        # Create extractors
        extractor = ArticleExtractor(config, logger)
        full_extractor = FullArticleExtractor(driver, logger)
        
        # Create repository if not provided
        if repository is None:
            # This would need actual DB connection
            # repository = PostgreSQLNewsRepository(db_conn, table_name)
            raise ValueError("Repository must be provided")
        
        # Create and return service
        return NewsScrapingService(
            driver=driver,
            extractor=extractor,
            full_extractor=full_extractor,
            repository=repository,
            config=config,
            logger=logger
        )


# ============================================================================
# Usage Example
# ============================================================================

def main():
    """Example usage of the refactored news scraper."""
    settings = get_settings()
    logger = get_logger(__name__)
    
    # Configuration
    config = {
        'topics': ['crypto'],
        'article_timeframe': ['yesterday', 'days'],
        'scroll_timeout': 3,
        'skip_scrolling': False
    }
    
    # Create repository (would need actual DB connection)
    # repository = PostgreSQLNewsRepository(db_conn, "financial_news")
    
    # Use context manager for WebDriver
    with WebDriverManager(settings) as driver:
        # Create extractors
        extractor = ArticleExtractor(config, logger)
        full_extractor = FullArticleExtractor(driver, logger)
        
        # Create service
        # service = NewsScrapingService(
        #     driver=driver,
        #     extractor=extractor,
        #     full_extractor=full_extractor,
        #     repository=repository,
        #     config=config,
        #     logger=logger
        # )
        
        # Scrape topics
        # for topic in config['topics']:
        #     articles = service.scrape_topic(topic)
        #     logger.info(f"Scraped {len(articles)} articles for topic: {topic}")


if __name__ == "__main__":
    main()

