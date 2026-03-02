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
