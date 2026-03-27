"""
Test parsing for the specific article that was failing with "Failed to parse JSON from response":
  id: 270218805936899518274975867363475896779
  headline: 3 Reasons Bitcoin Is a Must-Buy for Long-Term ...
  source: Motley Fool

Run with: PYTHONPATH=src python -m pytest tests/test_motley_fool_article_parse.py -v -s
Or live call: PYTHONPATH=src python tests/test_motley_fool_article_parse.py
"""

import os
import sys

import pandas as pd
try:
    import pytest
except ImportError:
    pytest = None

# Article content (abbreviated) matching the failing row
MOTLEY_FOOL_ARTICLE_ROW = {
    "id": "270218805936899518274975867363475896779",
    "source": "Motley Fool",
    "headline": "3 Reasons Bitcoin Is a Must-Buy for Long-Term Investors",
    "href": "https://finance.yahoo.com/news/3-reasons-bitcoin-is-a-must-buy-for-long-term-investors",
    "summary": "Bitcoin (CRYPTO: BTC) has had a phenomenal year...",
    "content": "Bitcoin (CRYPTO: BTC) has had a phenomenal year. The cryptocurrency has surged to new all-time highs...",
    "tickers": ["BTC"],
}


def _get_task():
    from agents.transforms.agentic_transform import FinancialMetricsTask
    return FinancialMetricsTask()


def _call_llm_and_parse():
    """Call real LLM for the article and return (raw_response, parse_result_dict)."""
    from agents.registry import get_llm_client
    from agents.transforms.agentic_transform import AgenticTextEnricher, FinancialMetricsTask
    from agents.base import LLMMessage

    row = pd.Series(MOTLEY_FOOL_ARTICLE_ROW)
    task = FinancialMetricsTask()
    client = get_llm_client("openai")
    prompt = task.build_prompt(row)
    messages = [LLMMessage(role="system", content=task.system_prompt), LLMMessage(role="user", content=prompt)]
    response = client.complete(messages)
    raw = response.content
    out = task.parse_response(raw)
    return raw, out


def test_parse_response_for_motley_fool_article_with_real_response():
    """
    Call the real API once for the failing article; assert parse succeeds and capture raw response.
    Skip if OPENAI_API_KEY not set or openai not installed.
    """
    if not os.environ.get("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY not set; cannot call real API")  # type: ignore[union-attr]
    try:
        import openai  # noqa: F401
    except ImportError:
        pytest.skip("openai not installed")  # type: ignore[union-attr]
    raw, out = _call_llm_and_parse()
    print("\n--- Raw LLM response (first 1500 chars) ---")
    print(raw[:1500])
    print("\n--- Parse result llm_error ---")
    print(out.get("llm_error"))
    print("\n--- llm_financial_metrics present? ---")
    print(out.get("llm_financial_metrics") is not None)
    assert out.get("llm_error") is None, f"Parse failed: {out.get('llm_error')}; raw tail: {raw[-500:]}"
    assert out.get("llm_financial_metrics") is not None


def test_parse_response_for_motley_fool_article_with_saved_response():
    """
    Test with a saved raw response that previously failed (trailing comma).
    """
    SAVED_RESPONSE = (
        '{"ticker": "BTC", "event_type": "other", "overall_sentiment": 0.3, '
        '"forward_sentiment": 0.2, "surprise_score": 0.0, "risk_score": 0.0, '
        '"uncertainty_score": 0.1, "impact_strength": 0.4, "immediacy": 0.5, '
        '"impact_horizon": "long_term", "confidence": 0.85, "sentiment_label": "positive", '
        '"impact_level": "medium", "signal": "bullish", "actionable": true, '
        '"sectors": ["crypto"], "entities": ["Bitcoin", "BTC"], "key_facts": ["Bitcoin must-buy for long-term"],}'
    )
    task = _get_task()
    out = task.parse_response(SAVED_RESPONSE)
    assert out.get("llm_error") is None
    assert out.get("llm_financial_metrics") is not None
    assert out["llm_financial_metrics"]["ticker"] == "BTC"


def test_parse_response_single_quoted_python_dict():
    """Some models return Python-style single-quoted dicts."""
    task = _get_task()
    raw = "{'ticker': 'BTC', 'event_type': 'other', 'overall_sentiment': 0.2, 'confidence': 0.8, 'sectors': ['crypto'], 'entities': ['Bitcoin'], 'key_facts': []}"
    out = task.parse_response(raw)
    assert out.get("llm_error") is None
    assert out["llm_financial_metrics"]["ticker"] == "BTC"


def test_parse_response_truncated_json():
    """Truncated JSON (model hit token limit) gets closed and parsed if possible."""
    task = _get_task()
    # Ends mid-object without closing brace
    raw = '{"ticker": "BTC", "event_type": "other", "overall_sentiment": 0.3, "confidence": 0.9'
    out = task.parse_response(raw)
    # Parser may or may not succeed depending on heuristic; at least should not crash
    if out.get("llm_error") is None:
        assert out["llm_financial_metrics"] is not None


if __name__ == "__main__":
    # Run live: fetch real response and print it for debugging
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    if not os.environ.get("OPENAI_API_KEY"):
        print("Set OPENAI_API_KEY to call real API")
        sys.exit(1)
    raw, out = _call_llm_and_parse()
    print("RAW RESPONSE:")
    print(raw)
    print("\nPARSE llm_error:", out.get("llm_error"))
    print("PARSE llm_financial_metrics is not None:", out.get("llm_financial_metrics") is not None)
    sys.exit(0 if out.get("llm_error") is None else 1)
