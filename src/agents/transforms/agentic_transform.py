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
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from agents.base import LLMClient, LLMMessage
from core.logging import get_logger

logger = get_logger(__name__)


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
                return {"llm_summary": None, "llm_themes": [], "llm_error": str(e)}
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
