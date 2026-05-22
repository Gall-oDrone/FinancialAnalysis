# Financial Analysis Toolkit

A production-ready Python toolkit for financial data collection, processing, and analysis. This project provides modules for web scraping financial news and stock data, API integration with cryptocurrency exchanges, data storage in PostgreSQL and AWS S3, and financial analysis libraries.

## Features

- **Web Scraping**: Automated collection of financial news and stock market data
- **API Integration**: Bitso cryptocurrency exchange API integration
- **Data Storage**: PostgreSQL and AWS S3 storage solutions
- **Data Transformation**: Production-ready ETL pipeline with ML/NLP transformations
  - **News**: Sentiment analysis, intent classification, keyword extraction, ticker extraction
  - **Agentic AI (optional)**: LLM-based enrichment for news (one-line summary, themes) via OpenAI/Claude; opt-in via `enable_agentic_transform`. See [docs/AGENTIC_AI_AND_BRANCHING.md](docs/AGENTIC_AI_AND_BRANCHING.md).
  - **Stocks**: Returns, volatility, technical indicators (SMA, RSI, MACD, etc.)
  - **GenAI Export**: JSONL format with optional embeddings for RAG applications
- **Financial Analysis**: Advanced financial modeling and analysis libraries
- **CLI Tools**: Command-line interface for ETL operations

## Project Structure (production layout)

```
financial_analysis/
├── src/                    # Application code (canonical)
│   ├── config/             # Configuration (settings)
│   ├── core/               # Shared utilities (logging)
│   ├── agents/             # LLM clients (OpenAI, Claude), agentic transform, MCP placeholder
│   ├── rag/                # RAG (chunking, in-memory vector store)
│   ├── storage/            # PostgreSQL and S3 (postgres/, cloud/)
│   ├── ingestion/          # Data ingestion
│   │   └── news/           # News scrapers, collectors, selectors
│   ├── transform/          # ETL transforms
│   │   ├── news/           # Text transformers, ticker extraction
│   │   └── stocks/         # Stock transformers (returns, indicators)
│   ├── export/             # GenAI/JSONL export
│   └── pipelines/         # Pipeline orchestration and ETL CLI
├── notebooks/              # All notebooks
│   ├── ingestion/          # Data ingestion (text, stocks)
│   ├── scraping/           # News and stock collectors
│   └── analysis/           # Financial analysis
├── tests/                  # Unit and integration tests
├── BitsoApi/               # Bitso API integration (standalone)
├── FinancialAnalysis/     # Financial modeling libraries (standalone)
├── scripts/                # Operational scripts
├── docs/                   # Documentation
├── pyproject.toml
└── README.md
```

## Installation

### Prerequisites

- Python 3.8 or higher
- PostgreSQL (for database storage)
- AWS Account (for S3 storage, optional)
- Chrome/Firefox browser (for web scraping)

### Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd financial_analysis
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -e .
   pip install -e ".[dev]"  # For development dependencies
   ```

4. **Configure environment variables:**
   ```bash
   cp env.example .env
   ```
   Edit `.env` with your database credentials and other settings. See `env.example` for all options (PostgreSQL, AWS, ML/ETL, scraping).

### Docker (PostgreSQL + scraping)

```bash
docker compose up -d postgres
docker compose run --rm scraper python /app/docker/notebook_smoke_test.py
```

For Jupyter in Docker, see [docs/jupyter-docker.md](docs/jupyter-docker.md). Documentation index: [docs/README.md](docs/README.md).

## Usage

### Web Scraping

```python
from WebScraping import Scraper

# Initialize scraper
scraper = Scraper(debug=False, topics=["crypto", "stocks"])

# Initialize database connection
scraper.initDB(
    db_type='postgres',
    tablename='financial_news',
    dbname='financial_db',
    user='db_user',
    table_query='CREATE TABLE...'
)

# Perform scraping operations
# ... your scraping logic here
```

### Database Operations

```python
from Storage import PgConn
from Storage import PostgresSQL_table_queries

# Initialize PostgreSQL connection
pg_conn = PgConn(
    tablename="stocks",
    dbname="financial_db",
    user="db_user"
)

# Initialize database table
pg_conn.init_db(PostgresSQL_table_queries.STOCKS_TABLE_QUERY)

# Save data
pg_conn.save_to_postgres(row_data, headers)

# Retrieve data
stocks_df = pg_conn.get_stocks_prices()
```

### Cloud Storage

```python
from Storage import CloudStorageProvider

# Initialize AWS S3 client
s3 = CloudStorageProvider.AWS()

# Upload DataFrame
s3.upload_dataframe_to_csv(
    dataframe=df,
    bucket_name="my-bucket",
    file_name="stocks_data",
    prefix_path="stocks"
)
```

### Data Transformation

Use the production package layout (`src/`). Run from repo root with `PYTHONPATH=src` or after `pip install -e .`:

```python
# News transformation with NLP
from transform.news.text_transformers import TextTransformationPipeline

pipeline = TextTransformationPipeline(
    sentiment_backend="vader",
    extract_tickers=True
)
transformed_news = pipeline.transform(news_df)

# Stock transformation with technical indicators
from transform.stocks.stock_transformers import StockTransformationPipeline

stock_pipeline = StockTransformationPipeline()
transformed_stocks = stock_pipeline.transform(stocks_df)

# Export for GenAI/RAG
from export.genai_export import export_to_jsonl

export_to_jsonl(transformed_news, "output/news.jsonl")
```

### ETL CLI

From repo root with `PYTHONPATH=src` or after `pip install -e .`:

```bash
# Ingest and transform stocks
python -m pipelines.etl_cli ingest-stocks --since 2026-01-01 --until 2026-01-28
python -m pipelines.etl_cli transform-stocks --since 2026-01-01 --output transformed.csv

# Transform news with sentiment analysis
python -m pipelines.etl_cli transform-news --date 2026-01-27 --sentiment vader

# News ETL with optional agentic (LLM) enrichment: set enable_agentic_transform=True
# or LLM_ENABLE_AGENTIC_TRANSFORM=true and OPENAI_API_KEY or ANTHROPIC_API_KEY.
# Postgres: financial_news_transformed has llm_summary, llm_themes, agentic_enabled.
# S3: paths include agentic=true/ or agentic=false/ for comparison.

# Export for GenAI with embeddings
python -m pipelines.etl_cli export-genai --date 2026-01-27 --embeddings
```

See [docs/ETL_AND_TRANSFORMS.md](docs/ETL_AND_TRANSFORMS.md) for CLI and import reference; `notebooks/etl/` and `notebooks/ingestion/` for examples.  
For agentic AI design and branching, see [docs/AGENTIC_AI_AND_BRANCHING.md](docs/AGENTIC_AI_AND_BRANCHING.md).

## Development

### Running Tests

```bash
pytest
```

With coverage:
```bash
pytest --cov=. --cov-report=html
```

### Code Formatting

```bash
black .
isort .
```

### Linting

```bash
flake8 .
pylint .
mypy .
```

### Pre-commit Hooks

Install pre-commit hooks:
```bash
pre-commit install
```

## Configuration

The application uses environment variables for configuration. Key settings can be found in:

- `.env` file for local development
- Environment variables for production deployment
- `config/` directory for application-specific settings

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, questions, or contributions, please open an issue on the GitHub repository.

## Authors

- Diego Gallo Valenzuela

## Acknowledgments

- Bitso API integration based on the Bitso Python SDK
- Financial models inspired by Lopez de Prado's research

