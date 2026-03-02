"""
Agentic text enrichment for ETL transform stage.

Uses an LLM client (OpenAI, Claude, etc.) to add fields to transformed articles:
- Short summary (one-line)
- Extracted entities or themes
- Optional custom tasks via prompt templates

Composes with the existing TextTransformationPipeline: run after it or as a
separate pipeline stage. OOP: depends on LLMClient interface only.
"""

from abc import ABC, abstractmethod
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from agents.base import LLMClient, LLMMessage
from core.logging import get_logger

logger = get_logger(__name__)

# Allowed values for structured financial extraction (for validation)
FINANCIAL_EVENT_TYPES = [
    "earnings", "guidance", "m&a", "analyst_action", "macro", "litigation",
    "regulation", "product", "management_change", "other",
]
IMPACT_HORIZONS = ["intraday", "short_term", "medium_term", "long_term"]


# ============================================================================
# Enrichment result and task interface
# ============================================================================


@dataclass
class EnrichmentResult:
    """Result of LLM-based enrichment for one article."""
    llm_summary: Optional[str] = None
    llm_entities: Optional[List[str]] = None
    llm_themes: Optional[List[str]] = None
    raw_response: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "llm_summary": self.llm_summary,
            "llm_entities": self.llm_entities or [],
            "llm_themes": self.llm_themes or [],
            "llm_error": self.error,
        }


class EnrichmentTask(ABC):
    """Abstract task: build prompt from row, parse LLM response into structured result."""

    @abstractmethod
    def build_prompt(self, row: pd.Series) -> str:
        """Build the user prompt from a dataframe row."""
        pass

    @abstractmethod
    def parse_response(self, content: str) -> Dict[str, Any]:
        """Parse LLM response into dict to merge into row."""
        pass

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        """Optional system prompt."""
        pass


# ============================================================================
# Default tasks
# ============================================================================


class SummaryAndThemesTask(EnrichmentTask):
    """Default: one-line summary and comma-separated themes."""

    @property
    def system_prompt(self) -> str:
        return (
            "You are a financial news analyst. Respond only with the requested structured output. "
            "Be concise and factual."
        )

    def build_prompt(self, row: pd.Series) -> str:
        title = row.get("headline") or ""
        body = row.get("content") or row.get("summary") or ""
        text = (title + "\n" + body).strip()[:4000]
        return (
            "For this financial news text, provide:\n"
            "1. SUMMARY: one short sentence summarizing the main point.\n"
            "2. THEMES: comma-separated list of 3-5 themes (e.g. earnings, regulation, crypto).\n\n"
            f"Text:\n{text}"
        )

    def parse_response(self, content: str) -> Dict[str, Any]:
        summary = None
        themes: List[str] = []
        for line in content.strip().split("\n"):
            line = line.strip()
            if line.upper().startswith("SUMMARY:") or line.upper().startswith("1."):
                summary = line.split(":", 1)[-1].strip().lstrip("12.").strip()
            elif line.upper().startswith("THEMES:") or line.upper().startswith("2."):
                raw = line.split(":", 1)[-1].strip().lstrip("12.").strip()
                themes = [t.strip() for t in raw.split(",") if t.strip()]
        if not summary and content:
            summary = content.strip()[:500]
        return {
            "llm_summary": summary,
            "llm_themes": themes,
        }


# ============================================================================
# Financial metrics task (trading-grade extraction)
# ============================================================================


class FinancialMetricsTask(EnrichmentTask):
    """
    Production-grade financial feature extraction for trading/ML.

    Extracts: overall_sentiment, forward_sentiment, surprise_score, risk_score,
    uncertainty_score, impact_strength, immediacy, confidence, event_type, impact_horizon.
    All numeric scores in [-1, 1], confidence in [0, 1]. Returns strict JSON only.
    """

    MAX_TEXT_LENGTH = 6000

    @property
    def system_prompt(self) -> str:
        return (
            "You are a financial information extraction engine. "
            "Your task is to analyze financial news articles and extract structured quantitative trading features.\n\n"
            "Follow these rules strictly:\n"
            "- Return only valid JSON.\n"
            "- No explanations.\n"
            "- No commentary.\n"
            "- All numeric scores must be between -1.0 and 1.0.\n"
            "- Confidence values must be between 0.0 and 1.0.\n"
            "- If a field is not applicable, use null.\n"
            "- Be consistent and deterministic.\n\n"
            "Scoring definitions:\n"
            "- overall_sentiment: overall tone of the article (raw tone, comparable to FinBERT).\n"
            "- forward_sentiment: tone regarding future expectations (forward guidance).\n"
            "- surprise_score: degree of unexpectedness vs expectations (positive=beat, negative=miss).\n"
            "- risk_score: perceived increase in company risk (bankruptcy, litigation, downgrade, liquidity).\n"
            "- uncertainty_score: ambiguity or lack of clarity (high => expect volatility).\n"
            "- impact_strength: expected magnitude of market reaction.\n"
            "- immediacy: expected speed of price reaction (1.0=instant, 0.2=slow thematic).\n"
            "- confidence: your self-assessed clarity of the signal (0–1).\n\n"
            f"Event types allowed: {json.dumps(FINANCIAL_EVENT_TYPES)}\n"
            f"Impact horizon allowed: {json.dumps(IMPACT_HORIZONS)}"
        )

    def build_prompt(self, row: pd.Series) -> str:
        headline = (row.get("headline") or "").strip()
        body = (row.get("content") or row.get("summary") or "").strip()
        full_text = (headline + "\n\n" + body).strip()[: self.MAX_TEXT_LENGTH]
        tickers = row.get("tickers")
        if isinstance(tickers, list) and tickers:
            ticker = str(tickers[0]) if tickers else ""
        elif isinstance(tickers, str):
            ticker = tickers
        else:
            ticker = ""
        return (
            "Analyze the following financial news article.\n\n"
            "[TITLE]\n"
            f"{headline or '(no title)'}\n\n"
            "[CONTENT]\n"
            f"{full_text or '(no content)'}\n\n"
            "Return JSON in this exact format (no other text):\n"
            "{\n"
            f'"ticker": "{ticker}",\n'
            '"event_type": "",\n'
            '"overall_sentiment": 0.0,\n'
            '"forward_sentiment": 0.0,\n'
            '"surprise_score": 0.0,\n'
            '"risk_score": 0.0,\n'
            '"uncertainty_score": 0.0,\n'
            '"impact_strength": 0.0,\n'
            '"immediacy": 0.0,\n'
            '"impact_horizon": "",\n'
            '"confidence": 0.0\n'
            "}"
        )

    def _extract_json(self, content: str) -> Optional[Dict[str, Any]]:
        """Extract JSON from response, tolerating markdown code blocks."""
        text = content.strip()
        # Remove optional markdown code fence
        match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
        if match:
            text = match.group(1).strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Try to find first { ... } block
            brace = text.find("{")
            if brace != -1:
                depth = 0
                end = brace
                for i, c in enumerate(text[brace:], start=brace):
                    if c == "{":
                        depth += 1
                    elif c == "}":
                        depth -= 1
                        if depth == 0:
                            end = i
                            break
                try:
                    return json.loads(text[brace : end + 1])
                except json.JSONDecodeError:
                    pass
        return None

    def _clamp_score(self, v: Any) -> Optional[float]:
        """Clamp numeric score to [-1, 1] or return None."""
        if v is None:
            return None
        try:
            x = float(v)
            if x != x:  # NaN
                return None
            return max(-1.0, min(1.0, x))
        except (TypeError, ValueError):
            return None

    def _clamp_confidence(self, v: Any) -> Optional[float]:
        """Clamp confidence to [0, 1] or return None."""
        if v is None:
            return None
        try:
            x = float(v)
            if x != x:
                return None
            return max(0.0, min(1.0, x))
        except (TypeError, ValueError):
            return None

    def parse_response(self, content: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        obj = self._extract_json(content)
        if not obj:
            return {"llm_financial_metrics": None, "llm_error": "Failed to parse JSON from response"}
        # Build validated payload for llm_financial_metrics
        event_type = obj.get("event_type")
        if event_type not in FINANCIAL_EVENT_TYPES:
            event_type = "other"
        impact_horizon = obj.get("impact_horizon")
        if impact_horizon not in IMPACT_HORIZONS:
            impact_horizon = None
        metrics = {
            "ticker": obj.get("ticker"),
            "event_type": event_type,
            "overall_sentiment": self._clamp_score(obj.get("overall_sentiment")),
            "forward_sentiment": self._clamp_score(obj.get("forward_sentiment")),
            "surprise_score": self._clamp_score(obj.get("surprise_score")),
            "risk_score": self._clamp_score(obj.get("risk_score")),
            "uncertainty_score": self._clamp_score(obj.get("uncertainty_score")),
            "impact_strength": self._clamp_score(obj.get("impact_strength")),
            "immediacy": self._clamp_score(obj.get("immediacy")),
            "impact_horizon": impact_horizon,
            "confidence": self._clamp_confidence(obj.get("confidence")),
        }
        out["llm_financial_metrics"] = metrics
        return out


# ============================================================================
# Agentic enricher
# ============================================================================


class AgenticTextEnricher:
    """
    Enriches a DataFrame of articles using an LLM (provider-agnostic).

    Production-ready: optional batching, skip on missing client, per-row error
    handling so one failure does not fail the whole batch.
    """

    def __init__(
        self,
        client: LLMClient,
        task: Optional[EnrichmentTask] = None,
        text_column: str = "content",
        skip_on_error: bool = True,
    ):
        self.client = client
        self.task = task or SummaryAndThemesTask()
        self.text_column = text_column
        self.skip_on_error = skip_on_error

    def enrich_row(self, row: pd.Series) -> Dict[str, Any]:
        """Enrich a single row; returns dict to merge."""
        try:
            prompt = self.task.build_prompt(row)
            messages = [LLMMessage(role="user", content=prompt)]
            if self.task.system_prompt:
                messages.insert(
                    0,
                    LLMMessage(role="system", content=self.task.system_prompt),
                )
            response = self.client.complete(messages)
            return self.task.parse_response(response.content)
        except Exception as e:
            logger.warning("Agentic enrich failed for row: %s", e)
            if self.skip_on_error:
                return {"llm_error": str(e)}
            raise

    def enrich_dataframe(
        self,
        df: pd.DataFrame,
        max_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Add LLM-derived columns to the DataFrame.

        New columns: llm_summary, llm_themes, (and any from task.parse_response).
        """
        if df.empty:
            return df
        out = df.copy()
        to_process = len(df)
        if max_rows is not None:
            to_process = min(to_process, max_rows)
        for idx in range(to_process):
            row = df.iloc[idx]
            try:
                parsed = self.enrich_row(row)
                for key, value in parsed.items():
                    if key not in out.columns:
                        out[key] = None
                    out.iloc[idx, out.columns.get_loc(key)] = value
            except Exception as e:
                if self.skip_on_error:
                    if "llm_error" not in out.columns:
                        out["llm_error"] = None
                    out.iloc[idx, out.columns.get_loc("llm_error")] = str(e)
                else:
                    raise
        return out


def agentic_enrich_pipeline(
    df: pd.DataFrame,
    client: LLMClient,
    task: Optional[EnrichmentTask] = None,
    max_rows: Optional[int] = None,
) -> pd.DataFrame:
    """
    Convenience: run agentic enrichment on a transformed articles DataFrame.

    Use after TextTransformationPipeline.transform(df).
    """
    enricher = AgenticTextEnricher(client=client, task=task)
    return enricher.enrich_dataframe(df, max_rows=max_rows)
