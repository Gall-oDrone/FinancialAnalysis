"""
Logging configuration and utilities.

This module provides structured logging setup for the application.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from config.settings import get_settings


def setup_logging(
    name: Optional[str] = None,
    log_file: Optional[str] = None,
    level: Optional[str] = None,
) -> logging.Logger:
    """Set up logging configuration.

    Args:
        name: Logger name (defaults to root logger)
        log_file: Path to log file (uses config default if not provided)
        level: Log level (uses config default if not provided)

    Returns:
        logging.Logger: Configured logger instance
    """
    settings = get_settings()
    log_config = settings.logging

    # Determine log level
    log_level = getattr(logging, level or log_config.level, logging.INFO)

    # Get logger
    logger_name = name or __name__
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (if log file is specified)
    log_file_path = log_file or log_config.file
    if log_file_path:
        # Create logs directory if it doesn't exist
        log_dir = Path(log_file_path).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=log_config.max_bytes,
            backupCount=log_config.backup_count,
        )
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name.

    Args:
        name: Logger name (typically __name__)

    Returns:
        logging.Logger: Logger instance
    """
    # Check if root logger is configured
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        setup_logging()

    return logging.getLogger(name)

