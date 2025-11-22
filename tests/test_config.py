"""
Tests for configuration module.
"""

import os
import pytest
from unittest.mock import patch

from config.settings import DatabaseConfig, AWSConfig, LoggingConfig, Settings, get_settings


class TestDatabaseConfig:
    """Test DatabaseConfig class."""

    def test_default_values(self):
        """Test default database configuration values."""
        with patch.dict(os.environ, {}, clear=True):
            config = DatabaseConfig()
            assert config.dbname == "financial_db"
            assert config.user == "postgres"
            assert config.host == "localhost"
            assert config.port == "5432"
            assert config.password is None

    def test_custom_values(self):
        """Test custom database configuration values."""
        env_vars = {
            "PGDBNAME": "custom_db",
            "PGDBUSER": "custom_user",
            "PGDBPASS": "custom_pass",
            "PGDBHOST": "custom_host",
            "PGDBPORT": "5433",
        }
        with patch.dict(os.environ, env_vars):
            config = DatabaseConfig()
            assert config.dbname == "custom_db"
            assert config.user == "custom_user"
            assert config.password == "custom_pass"
            assert config.host == "custom_host"
            assert config.port == "5433"


class TestSettings:
    """Test Settings class."""

    def test_settings_initialization(self):
        """Test Settings class initialization."""
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings()
            assert settings.database is not None
            assert settings.aws is not None
            assert settings.logging is not None
            assert settings.scraping is not None
            assert settings.debug is False
            assert settings.environment == "development"


class TestGetSettings:
    """Test get_settings function."""

    def test_get_settings_cached(self):
        """Test that get_settings returns cached instance."""
        with patch.dict(os.environ, {}, clear=True):
            settings1 = get_settings()
            settings2 = get_settings()
            assert settings1 is settings2

    def test_get_settings_clear_cache(self):
        """Test clearing settings cache."""
        with patch.dict(os.environ, {}, clear=True):
            get_settings.cache_clear()
            settings1 = get_settings()
            get_settings.cache_clear()
            settings2 = get_settings()
            assert settings1 is not settings2

