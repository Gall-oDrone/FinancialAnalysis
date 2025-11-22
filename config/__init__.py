"""
Configuration module for Financial Analysis Toolkit.

This module provides centralized configuration management using environment
variables and default settings.
"""

from .settings import Settings, get_settings

__all__ = ["Settings", "get_settings"]

