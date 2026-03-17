"""
Pytest configuration and shared fixtures.
"""

import pytest
import pandas as pd

# Ensure NLTK data is available for text transformer tests
def _ensure_nltk_data():
    """Download NLTK vader_lexicon if not present."""
    try:
        import nltk
        import ssl
        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            pass
        else:
            ssl._create_default_https_context = _create_unverified_https_context
        nltk.download("vader_lexicon", quiet=True)
    except Exception:
        pass

_ensure_nltk_data()
import numpy as np
from datetime import datetime, timedelta
try:
    # Python 3 standard library
    from unittest.mock import Mock, patch
except ImportError:
    # Fallback for environments that provide mock as a separate package
    from mock import Mock, patch

# Add src to path for imports (production layout)
import sys
try:
    from pathlib import Path  # Python 3
except ImportError:
    from pathlib2 import Path  # Python 2 fallback

project_root = Path(__file__).parent.parent
src_path = project_root / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))
sys.path.insert(0, str(project_root))


@pytest.fixture
def mock_settings():
    """Mock settings for testing."""
    with patch("config.settings.get_settings") as mock:
        mock_settings = Mock()
        mock_settings.database.dbname = "test_db"
        mock_settings.database.user = "test_user"
        mock_settings.database.password = "test_pass"
        mock_settings.database.host = "localhost"
        mock_settings.database.port = "5432"
        mock_settings.logging.level = "DEBUG"
        mock_settings.logging.file = "logs/test.log"
        mock_settings.aws.default_bucket = "test-bucket"
        mock.return_value = mock_settings
        yield mock_settings


@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    with patch("core.logging.get_logger") as mock:
        mock_logger = Mock()
        mock.return_value = mock_logger
        yield mock_logger


@pytest.fixture
def sample_news_df():
    """Sample news DataFrame for testing."""
    return pd.DataFrame([
        {
            "id": "1",
            "source": "Yahoo Finance",
            "headline": "Bitcoin Surges Past $100,000 as Institutional Adoption Grows",
            "summary": "Bitcoin reached a new all-time high today as major institutions announce crypto investments.",
            "content": """Bitcoin (CRYPTO: BTC) surged past the $100,000 mark today, setting a new all-time high 
            as institutional adoption continues to accelerate. Major financial institutions including 
            Goldman Sachs and Morgan Stanley announced significant cryptocurrency investment products. 
            BTC-USD rallied 15% in early trading. Analysts predict the rally could continue as regulatory 
            clarity improves and the SEC considers approving more spot Bitcoin ETFs.""",
            "href": "https://finance.yahoo.com/news/bitcoin-surges-100k",
            "author": "John Doe",
            "datetime": "2026-01-27T10:30:00.000Z"
        },
        {
            "id": "2",
            "source": "CoinDesk",
            "headline": "SEC Delays Decision on Ethereum ETF Applications",
            "summary": "The Securities and Exchange Commission has postponed its ruling on multiple Ethereum ETF proposals.",
            "content": """The SEC announced today that it will delay its decision on several pending 
            Ethereum (CRYPTO: ETH) ETF applications. The delay affects applications from major asset managers 
            including BlackRock and Fidelity. Market analysts suggest the delay could impact 
            ETH-USD prices in the short term, though long-term sentiment remains bullish. 
            The regulatory uncertainty continues to be a concern for crypto investors.""",
            "href": "https://coindesk.com/news/sec-ethereum-etf-delay",
            "author": "Jane Smith",
            "datetime": "2026-01-27T14:45:00.000Z"
        },
        {
            "id": "3",
            "source": "Bloomberg",
            "headline": "DeFi Protocol Suffers $50M Hack in Smart Contract Exploit",
            "summary": "A major DeFi platform lost millions in a security breach targeting its lending contracts.",
            "content": """A prominent DeFi lending protocol suffered a devastating $50 million hack 
            today due to a vulnerability in its smart contracts. Security researchers identified 
            the exploit as a flash loan attack that manipulated the protocol's price oracles. 
            This marks the third major DeFi hack this month, raising concerns about smart contract 
            security. Bitcoin and Ethereum prices both dropped on the news.""",
            "href": "https://bloomberg.com/news/defi-hack-50m",
            "author": "Bob Johnson",
            "datetime": "2026-01-27T09:15:00.000Z"
        }
    ])


@pytest.fixture
def sample_stocks_df():
    """Sample stock DataFrame for testing."""
    # Generate 100 days of sample OHLCV data
    dates = pd.date_range(start='2025-10-01', periods=100, freq='D')
    
    np.random.seed(42)
    initial_price = 100
    returns = np.random.normal(0.001, 0.02, 100)
    close_prices = initial_price * np.exp(returns.cumsum())
    
    return pd.DataFrame({
        'ref': ['https://finance.yahoo.com'] * 100,
        'book': ['btc-usd'] * 100,
        'date': dates,
        'open': close_prices * (1 + np.random.uniform(-0.01, 0.01, 100)),
        'high': close_prices * (1 + np.random.uniform(0, 0.02, 100)),
        'low': close_prices * (1 - np.random.uniform(0, 0.02, 100)),
        'close': close_prices,
        'adj_close': close_prices,
        'volume': np.random.randint(1000000, 5000000, 100)
    })


@pytest.fixture
def sample_transformed_news_df(sample_news_df):
    """Sample transformed news DataFrame for testing."""
    df = sample_news_df.copy()
    
    # Add transformed fields
    df['cleaned_text'] = df['content']
    df['word_count'] = df['content'].str.split().str.len()
    df['tickers'] = [['BTC-USD'], ['ETH-USD'], ['BTC-USD', 'ETH-USD']]
    df['sentiment_label'] = ['positive', 'negative', 'negative']
    df['sentiment_score'] = [0.8, -0.3, -0.5]
    df['positive_score'] = [0.8, 0.1, 0.1]
    df['negative_score'] = [0.1, 0.6, 0.7]
    df['neutral_score'] = [0.1, 0.3, 0.2]
    df['primary_intent'] = ['market_update', 'regulatory_news', 'breaking_news']
    df['intent_confidence'] = [0.9, 0.85, 0.75]
    df['keywords'] = [
        ['bitcoin', 'surge', 'institutional'],
        ['sec', 'ethereum', 'etf', 'delay'],
        ['defi', 'hack', 'security']
    ]
    df['entities'] = [[], [], []]
    
    return df


@pytest.fixture
def mock_s3():
    """Mock S3 client for testing."""
    with patch('boto3.client') as mock:
        mock_client = Mock()
        mock_client.upload_file.return_value = None
        mock.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_db_connection():
    """Mock database connection for testing."""
    with patch('storage.postgres.pgConn.PgConn') as mock:
        mock_conn = Mock()
        mock_conn.get_stocks_prices.return_value = pd.DataFrame()
        mock_conn.get_financial_news.return_value = pd.DataFrame()
        mock_conn.save_to_postgres.return_value = None
        mock_conn.close_connection.return_value = None
        mock.return_value = mock_conn
        yield mock_conn

