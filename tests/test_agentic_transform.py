"""
Tests for agentic transform tasks (FinancialMetricsTask, SummaryAndThemesTask).
"""

import json
import pytest
import pandas as pd

# conftest adds src to path
from agents.transforms.agentic_transform import (
    FinancialMetricsTask,
    SummaryAndThemesTask,
    FINANCIAL_EVENT_TYPES,
    IMPACT_HORIZONS,
    SENTIMENT_LABELS,
    IMPACT_LEVELS,
    SIGNALS,
    SECTORS,
)


class TestFinancialMetricsTask:
    """Tests for FinancialMetricsTask (trading-grade JSON extraction)."""

    @pytest.fixture
    def task(self):
        return FinancialMetricsTask()

    @pytest.fixture
    def sample_row(self):
        return pd.Series({
            "headline": "Bitcoin soars past 100k on Fed news",
            "content": "Markets rallied as the Fed signaled rate cuts. BTC-USD up 5%.",
            "summary": "Crypto rally continues.",
            "tickers": ["BTC", "BTC-USD"],
        })

    def test_system_prompt_contains_rules(self, task):
        assert "JSON" in task.system_prompt
        assert "-1.0" in task.system_prompt and "1.0" in task.system_prompt
        assert "overall_sentiment" in task.system_prompt
        assert "event_type" in task.system_prompt or "earnings" in task.system_prompt

    def test_build_prompt_includes_title_and_content(self, task, sample_row):
        prompt = task.build_prompt(sample_row)
        assert "Bitcoin soars" in prompt
        assert "Markets rallied" in prompt or "100k" in prompt
        assert "overall_sentiment" in prompt
        assert "event_type" in prompt
        assert "impact_horizon" in prompt
        assert "confidence" in prompt
        assert "sentiment_label" in prompt
        assert "signal" in prompt
        assert "actionable" in prompt
        assert "sectors" in prompt
        assert "entities" in prompt
        assert "key_facts" in prompt

    def test_build_prompt_uses_first_ticker_from_list(self, task, sample_row):
        prompt = task.build_prompt(sample_row)
        assert "BTC" in prompt

    def test_build_prompt_handles_missing_tickers(self, task):
        row = pd.Series({"headline": "Title", "content": "Body"})
        prompt = task.build_prompt(row)
        assert "Title" in prompt and "Body" in prompt

    def test_parse_response_valid_json(self, task):
        payload = {
            "ticker": "BTC",
            "event_type": "macro",
            "overall_sentiment": 0.5,
            "forward_sentiment": 0.3,
            "surprise_score": 0.0,
            "risk_score": -0.1,
            "uncertainty_score": 0.2,
            "impact_strength": 0.4,
            "immediacy": 0.7,
            "impact_horizon": "short_term",
            "confidence": 0.85,
        }
        out = task.parse_response(json.dumps(payload))
        assert "llm_financial_metrics" in out
        m = out["llm_financial_metrics"]
        assert m["event_type"] == "macro"
        assert m["impact_horizon"] == "short_term"
        assert m["overall_sentiment"] == 0.5
        assert m["confidence"] == 0.85
        assert m["ticker"] == "BTC"
        # New fields default when omitted
        assert m.get("sentiment_label") is None
        assert m.get("sectors") == []
        assert m.get("entities") == []
        assert m.get("key_facts") == []

    def test_parse_response_clamps_scores_to_minus1_to_1(self, task):
        payload = {
            "ticker": "",
            "event_type": "other",
            "overall_sentiment": 1.5,
            "forward_sentiment": -2.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.0,
            "immediacy": 0.0,
            "impact_horizon": "short_term",
            "confidence": 0.9,
        }
        out = task.parse_response(json.dumps(payload))
        m = out["llm_financial_metrics"]
        assert m["overall_sentiment"] == 1.0
        assert m["forward_sentiment"] == -1.0

    def test_parse_response_clamps_confidence_to_0_to_1(self, task):
        payload = {
            "ticker": "",
            "event_type": "other",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.0,
            "immediacy": 0.0,
            "impact_horizon": "other",
            "confidence": 1.5,
        }
        out = task.parse_response(json.dumps(payload))
        m = out["llm_financial_metrics"]
        assert m["confidence"] == 1.0

    def test_parse_response_invalid_event_type_defaults_to_other(self, task):
        payload = {
            "ticker": "",
            "event_type": "invalid_event",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.0,
            "immediacy": 0.0,
            "impact_horizon": "short_term",
            "confidence": 0.8,
        }
        out = task.parse_response(json.dumps(payload))
        assert out["llm_financial_metrics"]["event_type"] == "other"

    def test_parse_response_invalid_impact_horizon_becomes_none(self, task):
        payload = {
            "ticker": "",
            "event_type": "other",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.0,
            "immediacy": 0.0,
            "impact_horizon": "invalid_horizon",
            "confidence": 0.8,
        }
        out = task.parse_response(json.dumps(payload))
        assert out["llm_financial_metrics"]["impact_horizon"] is None

    def test_parse_response_markdown_code_block(self, task):
        payload = {
            "ticker": "ETH",
            "event_type": "regulation",
            "overall_sentiment": -0.2,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.1,
            "uncertainty_score": 0.5,
            "impact_strength": 0.3,
            "immediacy": 0.6,
            "impact_horizon": "medium_term",
            "confidence": 0.7,
        }
        wrapped = "```json\n" + json.dumps(payload) + "\n```"
        out = task.parse_response(wrapped)
        assert out["llm_financial_metrics"] is not None
        assert out["llm_financial_metrics"]["event_type"] == "regulation"
        assert out["llm_financial_metrics"]["ticker"] == "ETH"

    def test_parse_response_trailing_comma_and_extra_text(self, task):
        # LLMs sometimes return trailing commas or text before/after JSON
        raw = 'Here is the analysis:\n{"ticker": "BTC", "event_type": "other", "overall_sentiment": 0.2, "confidence": 0.9,}\n'
        out = task.parse_response(raw)
        assert out.get("llm_error") is None
        assert out["llm_financial_metrics"] is not None
        assert out["llm_financial_metrics"]["ticker"] == "BTC"
        assert out["llm_financial_metrics"]["confidence"] == 0.9

    def test_parse_response_invalid_json_returns_error_key(self, task):
        out = task.parse_response("not valid json at all")
        assert "llm_financial_metrics" in out
        assert out["llm_financial_metrics"] is None
        assert "llm_error" in out
        assert "parse" in out["llm_error"].lower() or "JSON" in out["llm_error"]

    def test_event_types_and_horizons_constants(self):
        assert "earnings" in FINANCIAL_EVENT_TYPES
        assert "macro" in FINANCIAL_EVENT_TYPES
        assert "other" in FINANCIAL_EVENT_TYPES
        assert "intraday" in IMPACT_HORIZONS
        assert "short_term" in IMPACT_HORIZONS
        assert "long_term" in IMPACT_HORIZONS

    def test_parse_response_sentiment_label_impact_level_signal(self, task):
        payload = {
            "ticker": "",
            "event_type": "other",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.0,
            "immediacy": 0.0,
            "impact_horizon": "short_term",
            "confidence": 0.8,
            "novelty_score": 0.6,
            "sentiment_label": "positive",
            "impact_level": "high",
            "signal": "bullish",
            "actionable": True,
            "sectors": ["crypto", "macro"],
            "entities": ["Fed", "Bitcoin"],
            "key_facts": ["Fed signaled rate cuts.", "BTC-USD up 5%"],
        }
        out = task.parse_response(json.dumps(payload))
        m = out["llm_financial_metrics"]
        assert m["novelty_score"] == 0.6
        assert m["sentiment_label"] == "positive"
        assert m["impact_level"] == "high"
        assert m["signal"] == "bullish"
        assert m["actionable"] is True
        assert m["sectors"] == ["crypto", "macro"]
        assert m["entities"] == ["Fed", "Bitcoin"]
        assert m["key_facts"] == ["Fed signaled rate cuts.", "BTC-USD up 5%"]
        assert out["llm_entities"] == ["Fed", "Bitcoin"]
        # Flattened columns (one per feature for DB/S3)
        assert out["llm_overall_sentiment"] == 0.0
        assert out["llm_novelty_score"] == 0.6
        assert out["llm_sentiment_label"] == "positive"
        assert out["llm_signal"] == "bullish"
        assert out["llm_actionable"] is True
        assert out["llm_sectors"] == ["crypto", "macro"]
        assert out["llm_key_facts"] == ["Fed signaled rate cuts.", "BTC-USD up 5%"]

    def test_parse_response_invalid_sentiment_label_and_signal_become_none(self, task):
        payload = {
            "ticker": "",
            "event_type": "other",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.0,
            "immediacy": 0.0,
            "impact_horizon": "short_term",
            "confidence": 0.8,
            "sentiment_label": "invalid_label",
            "impact_level": "invalid_level",
            "signal": "invalid_signal",
            "actionable": "not_bool",
            "sectors": ["crypto", "invalid_sector"],
            "entities": ["Fed"],
            "key_facts": [],
        }
        out = task.parse_response(json.dumps(payload))
        m = out["llm_financial_metrics"]
        assert m["sentiment_label"] is None
        assert m["impact_level"] is None
        assert m["signal"] is None
        assert m["actionable"] is None
        assert m["sectors"] == ["crypto"]
        assert m["entities"] == ["Fed"]
        assert out["llm_entities"] == ["Fed"]

    def test_parse_response_key_facts_capped(self, task):
        payload = {
            "ticker": "",
            "event_type": "other",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.0,
            "immediacy": 0.0,
            "impact_horizon": "short_term",
            "confidence": 0.8,
            "sentiment_label": "neutral",
            "impact_level": "medium",
            "signal": "neutral",
            "actionable": False,
            "sectors": [],
            "entities": [],
            "key_facts": [f"fact_{i}" for i in range(15)],
        }
        out = task.parse_response(json.dumps(payload))
        m = out["llm_financial_metrics"]
        assert len(m["key_facts"]) <= 10

    def test_new_constants(self):
        assert SENTIMENT_LABELS == ["positive", "negative", "neutral"]
        assert "high" in IMPACT_LEVELS and "low" in IMPACT_LEVELS
        assert "bullish" in SIGNALS and "bearish" in SIGNALS
        assert "crypto" in SECTORS and "DeFi" in SECTORS and "other" in SECTORS

    # --- Tests for the 3 articles that failed with "Failed to parse JSON from response" (by id/headline) ---

    @pytest.mark.parametrize(
        "article_id,headline",
        [
            (
                "128760326000617418605525084146200490339",
                "Bitcoin surpasses $107,000 to hit new high amid 'relentless'...",
            ),
            (
                "150476629987280880526515602205867776431",
                "Here's how bitcoin could hit $225,000: Analyst Mark ...",
            ),
            (
                "153078721426213179978784704911365399202",
                "Trump appoints Bo Hines, an ex-college football player, to p...",
            ),
        ],
    )
    def test_parse_response_three_articles_with_llm_error(self, task, article_id, headline):
        """Parse valid JSON for the 3 articles that had llm_error 'Failed to parse JSON from response'."""
        payload = {
            "ticker": "BTC",
            "event_type": "macro",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.3,
            "immediacy": 0.0,
            "impact_horizon": "short_term",
            "confidence": 0.8,
            "sentiment_label": "neutral",
            "impact_level": "medium",
            "signal": "neutral",
            "actionable": False,
            "sectors": ["crypto", "macro"],
            "entities": [],
            "key_facts": [],
        }
        raw = json.dumps(payload)
        out = task.parse_response(raw)
        assert out.get("llm_error") is None, f"Expected no llm_error for id={article_id!r} headline={headline!r}"
        assert out["llm_financial_metrics"] is not None
        assert out["llm_financial_metrics"]["ticker"] == "BTC"

    # --- Tests for the 5 articles that previously failed with "Failed to parse JSON from response" ---

    @pytest.mark.parametrize(
        "headline",
        [
            "Fed's preferred inflation gauge highlights holiday-shortened week",
            "Here's how bitcoin could hit $225,000: Analyst Mark",
            "MicroStrategy Buys Another $2.1 Billion Worth of Bitcoin",
            "Stock market today: Wall Street rises with Nvidia as bitcoin",
            "Trump appoints Bo Hines, an ex-college football player, to advisory board",
        ],
    )
    def test_parse_response_five_formerly_failing_articles(self, task, headline):
        """Parse valid JSON for the 5 articles that previously got llm_error (no parse failure)."""
        payload = {
            "ticker": "BTC",
            "event_type": "macro",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.3,
            "immediacy": 0.0,
            "impact_horizon": "short_term",
            "confidence": 0.8,
            "sentiment_label": "neutral",
            "impact_level": "medium",
            "signal": "neutral",
            "actionable": False,
            "sectors": ["crypto", "macro"],
            "entities": [],
            "key_facts": [],
        }
        raw = json.dumps(payload)
        out = task.parse_response(raw)
        assert out.get("llm_error") is None, f"Expected no llm_error for headline: {headline!r}"
        assert out["llm_financial_metrics"] is not None
        assert out["llm_financial_metrics"]["ticker"] == "BTC"

    def test_parse_response_json_with_literal_newline_in_string(self, task):
        """LLM sometimes returns unescaped newlines inside string values; parser normalizes and parses."""
        # Invalid JSON: newline inside "key_facts" string
        raw = (
            '{"ticker": "BTC", "event_type": "other", "overall_sentiment": 0.0, '
            '"forward_sentiment": 0.0, "surprise_score": 0.0, "risk_score": 0.0, '
            '"uncertainty_score": 0.0, "impact_strength": 0.0, "immediacy": 0.0, '
            '"impact_horizon": "short_term", "confidence": 0.8, "sentiment_label": "neutral", '
            '"impact_level": "medium", "signal": "neutral", "actionable": false, '
            '"sectors": [], "entities": [], "key_facts": ["Fact with\nnewline"]}'
        )
        out = task.parse_response(raw)
        assert out.get("llm_error") is None
        assert out["llm_financial_metrics"] is not None
        assert "Fact with" in (out["llm_financial_metrics"].get("key_facts") or [""])[0]

    def test_parse_response_trailing_prose_after_closing_brace(self, task):
        """Response with text after the JSON closing brace; parser uses first { to last }."""
        payload = {
            "ticker": "ETH",
            "event_type": "regulation",
            "overall_sentiment": -0.1,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.2,
            "immediacy": 0.0,
            "impact_horizon": "medium_term",
            "confidence": 0.7,
            "sentiment_label": "neutral",
            "impact_level": "medium",
            "signal": "neutral",
            "actionable": False,
            "sectors": ["crypto"],
            "entities": [],
            "key_facts": [],
        }
        raw = json.dumps(payload) + "\n\nHope this analysis helps. Let me know if you need more."
        out = task.parse_response(raw)
        assert out.get("llm_error") is None
        assert out["llm_financial_metrics"] is not None
        assert out["llm_financial_metrics"]["ticker"] == "ETH"
        assert out["llm_financial_metrics"]["event_type"] == "regulation"

    def test_parse_response_leading_prose_and_trailing_comma(self, task):
        """Leading prose + trailing comma (simulates real LLM output)."""
        raw = (
            "Here is the analysis:\n"
            '{"ticker": "BTC", "event_type": "macro", "overall_sentiment": 0.2, '
            '"forward_sentiment": 0.0, "surprise_score": 0.0, "risk_score": 0.0, '
            '"uncertainty_score": 0.0, "impact_strength": 0.4, "immediacy": 0.0, '
            '"impact_horizon": "short_term", "confidence": 0.9, "sentiment_label": "positive", '
            '"impact_level": "medium", "signal": "neutral", "actionable": false, '
            '"sectors": ["crypto"], "entities": [], "key_facts": [],}'
        )
        out = task.parse_response(raw)
        assert out.get("llm_error") is None
        assert out["llm_financial_metrics"]["ticker"] == "BTC"
        assert out["llm_financial_metrics"]["confidence"] == 0.9

    def test_parse_response_control_char_inside_json(self, task):
        """JSON with control character (e.g. \\x00) is stripped and parses."""
        payload = {
            "ticker": "BTC",
            "event_type": "other",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.0,
            "immediacy": 0.0,
            "impact_horizon": "short_term",
            "confidence": 0.8,
            "sentiment_label": "neutral",
            "impact_level": "medium",
            "signal": "neutral",
            "actionable": False,
            "sectors": [],
            "entities": [],
            "key_facts": [],
        }
        raw = json.dumps(payload)
        # Insert control char that breaks strict JSON
        raw_bad = raw[:30] + "\x00" + raw[30:]
        out = task.parse_response(raw_bad)
        assert out.get("llm_error") is None
        assert out["llm_financial_metrics"] is not None
        assert out["llm_financial_metrics"]["ticker"] == "BTC"

    def test_parse_response_truncated_json_missing_closing_brace(self, task):
        """Truncated JSON missing closing brace is repaired and parses."""
        payload = {
            "ticker": "BTC",
            "event_type": "macro",
            "overall_sentiment": 0.0,
            "forward_sentiment": 0.0,
            "surprise_score": 0.0,
            "risk_score": 0.0,
            "uncertainty_score": 0.0,
            "impact_strength": 0.3,
            "immediacy": 0.0,
            "impact_horizon": "short_term",
            "confidence": 0.9,
            "sentiment_label": "neutral",
            "impact_level": "medium",
            "signal": "neutral",
            "actionable": False,
            "sectors": ["crypto"],
            "entities": [],
            "key_facts": [],
        }
        raw = json.dumps(payload)
        truncated = raw.rstrip()
        if truncated.endswith("}"):
            truncated = truncated[:-1]
        out = task.parse_response(truncated)
        assert out.get("llm_error") is None
        assert out["llm_financial_metrics"] is not None
        assert out["llm_financial_metrics"]["ticker"] == "BTC"

    def test_parse_response_key_facts_unquoted_string(self, task):
        """LLM sometimes omits opening quote on key_facts entries; repair parses."""
        raw = (
            '{"ticker": "BTC", "event_type": "macro", "overall_sentiment": -0.5, '
            '"forward_sentiment": -0.6, "surprise_score": -0.4, "risk_score": 0.5, '
            '"uncertainty_score": 0.7, "impact_strength": 0.6, "immediacy": 0.8, '
            '"impact_horizon": "short_term", "confidence": 0.7, "sentiment_label": "negative", '
            '"impact_level": "high", "signal": "bearish", "actionable": true, '
            '"sectors": ["crypto", "fx"], "entities": ["Bitcoin", "AUD"], '
            '"key_facts": [\n'
            '"BTC\'s rally has stalled.",\n'
            'Expectations of a BOJ rate hike in December are growing.",\n'
            '"The yen is strengthening."\n'
            ']}'
        )
        out = task.parse_response(raw)
        assert out.get("llm_error") is None
        assert out["llm_financial_metrics"] is not None
        kf = out["llm_financial_metrics"].get("key_facts") or []
        assert any("BOJ" in str(f) for f in kf)
        assert any("yen" in str(f).lower() for f in kf)


class TestSummaryAndThemesTask:
    """Smoke tests for default SummaryAndThemesTask."""

    def test_system_prompt_non_empty(self):
        t = SummaryAndThemesTask()
        assert len(t.system_prompt) > 0
        assert "analyst" in t.system_prompt.lower() or "financial" in t.system_prompt.lower()

    def test_build_prompt_uses_headline_and_content(self):
        t = SummaryAndThemesTask()
        row = pd.Series({"headline": "Fed raises rates", "content": "The Fed hiked by 25bp."})
        prompt = t.build_prompt(row)
        assert "Fed" in prompt
        assert "SUMMARY" in prompt and "THEMES" in prompt

    def test_parse_response_extracts_summary_and_themes(self):
        t = SummaryAndThemesTask()
        content = "1. SUMMARY: The Fed raised interest rates.\n2. THEMES: rates, macro, inflation"
        out = t.parse_response(content)
        assert "llm_summary" in out
        assert "llm_themes" in out
        assert "Fed" in out["llm_summary"] or "rates" in out["llm_summary"]
        assert "macro" in out["llm_themes"] or "rates" in out["llm_themes"]
