# WebScraping Module

Production-ready web scraping module for financial data collection.

## Directory Structure

```
WebScraping/
├── src/                    # Source code
│   ├── __init__.py
│   ├── WebScraper.py      # Main scraper class
│   ├── DataframeStore.py  # DataFrame storage utility
│   ├── YahooFinanceHTMLElements.py  # HTML element selectors
│   ├── news_collector_example.py    # News collector example
│   └── news_collector_refactored_example.py  # Refactored news collector
├── notebooks/              # Jupyter notebooks for experimentation
│   ├── TestScrapper.ipynb
│   ├── NewsCollector-Staging.ipynb
│   ├── StockCollector.ipynb
│   └── StockCollector-CoinMarketCap.ipynb
├── docs/                   # Documentation
│   ├── NEWS_COLLECTOR_ANALYSIS.md
│   └── REFACTORING_QUICK_REFERENCE.md
├── data/                   # Generated data files (HTML dumps, etc.)
│   ├── dom_tree_*.html
│   └── dom_tree_prettified_*.html
├── tests/                  # Unit tests (to be added)
│   └── __init__.py
└── README.md               # This file
```

## Usage

### Importing the main scraper:

```python
from src import Scrapper

# Or directly
from src.WebScraper import Scrapper
```

### Importing utilities:

```python
from src import DataFrameStore
from src.DataframeStore import DataFrameStore
```

## Development

- **Source code**: All production code lives in `src/`
- **Experimentation**: Use notebooks in `notebooks/` for testing and exploration
- **Documentation**: Analysis and reference docs are in `docs/`
- **Generated files**: Output files like HTML dumps go in `data/`

