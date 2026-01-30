"""
Compatibility shim: re-export from scrapers so "from DataframeStore import DataFrameStore" still works.
"""

try:
    from .scrapers.DataframeStore import DataFrameStore
except ImportError:
    from scrapers.DataframeStore import DataFrameStore

__all__ = ["DataFrameStore"]
