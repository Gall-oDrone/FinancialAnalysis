"""
ETL and transform tools for Claude / OpenAI Messages API and MCP.

Provides:
- get_tools_for_claude() -> list of {name, description, input_schema} for Messages API
- get_handler_by_name(name) -> callable that takes **kwargs and returns dict
- run_tool(name, arguments_dict) -> execute tool and return result
"""

from typing import Any, Callable, Dict, List, Optional

from agents.tools.schemas import get_all_schemas, get_schema_by_name
from agents.tools import implementations as impl

# Map tool name -> handler function (same signature: **kwargs -> dict)
_HANDLERS: Dict[str, Callable[..., Dict[str, Any]]] = {
    "extract_tickers": impl.extract_tickers,
    "analyze_sentiment": impl.analyze_sentiment,
    "extract_intent": impl.extract_intent,
    "extract_keywords": impl.extract_keywords,
    "stock_risk_metrics": impl.stock_risk_metrics,
    "build_s3_key_news": impl.build_s3_key_news,
    "build_s3_key_stocks": impl.build_s3_key_stocks,
    "build_s3_key_stocks_batch": impl.build_s3_key_stocks_batch,
    "ingest_news": impl.ingest_news,
    "ingest_stocks": impl.ingest_stocks,
    "run_news_transform": impl.run_news_transform,
    "run_stocks_transform": impl.run_stocks_transform,
    "enrich_article": impl.enrich_article,
    "batch_tool": impl.batch_tool,
}


def get_tools_for_claude() -> List[Dict[str, Any]]:
    """Return tool definitions for Claude Messages API (tools= parameter)."""
    return get_all_schemas()


def get_handler_by_name(name: str) -> Optional[Callable[..., Dict[str, Any]]]:
    """Return the handler callable for the given tool name, or None."""
    return _HANDLERS.get(name)


def run_tool(name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a tool by name with the given arguments.
    Returns a JSON-serializable dict (or list inside dict).
    """
    handler = get_handler_by_name(name)
    if not handler:
        return {"error": f"Unknown tool: {name}"}
    return handler(**arguments)


__all__ = [
    "get_tools_for_claude",
    "get_handler_by_name",
    "run_tool",
    "get_all_schemas",
    "get_schema_by_name",
    "_HANDLERS",  # for tests that patch handler dict
]
