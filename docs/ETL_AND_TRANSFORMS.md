# ETL and Transformation Pipeline

Production-ready ETL for financial data (stocks and news) lives under `src/`: pipelines, transform, and export.

## Layout

- **News**: `src/transform/news/` (text_transformers, ticker_extractor)
- **Stocks**: `src/transform/stocks/` (stock_transformers)
- **Pipeline**: `src/pipelines/` (pipeline, etl_cli, etl_transform)
- **Export**: `src/export/` (genai_export)

Run from repo root with `PYTHONPATH=src` or after `pip install -e .`.

## CLI

```bash
# Ingest
python -m pipelines.etl_cli ingest-stocks --since 2026-01-01 --until 2026-01-28
python -m pipelines.etl_cli ingest-news --date 2026-01-27

# Transform
python -m pipelines.etl_cli transform-stocks --since 2026-01-01 --output stocks.csv
python -m pipelines.etl_cli transform-news --date 2026-01-27 --sentiment vader

# Export for GenAI
python -m pipelines.etl_cli export-genai --date 2026-01-27 --embeddings
```

## Imports

```python
from transform.news.text_transformers import TextTransformationPipeline
from transform.stocks.stock_transformers import StockTransformationPipeline
from transform.news.ticker_extractor import TickerExtractor
from export.genai_export import export_to_jsonl, generate_embeddings
from pipelines.pipeline import DataPipeline, PipelineConfig, PipelineStage
```

## Notebooks

- `notebooks/etl/` — ETL Transform (news, stocks)
- `notebooks/ingestion/` — Data ingestion (text, stocks)

## See also

- [README.md](../README.md) — Setup and usage
- [AGENTIC_AI_AND_BRANCHING.md](AGENTIC_AI_AND_BRANCHING.md) — Agentic (LLM) enrichment
