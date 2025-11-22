"""
Pytest configuration and shared fixtures.
"""

import pytest
from unittest.mock import Mock, patch

# Add project root to path for imports
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
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
        mock.return_value = mock_settings
        yield mock_settings


@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    with patch("utils.logging.get_logger") as mock:
        mock_logger = Mock()
        mock.return_value = mock_logger
        yield mock_logger

