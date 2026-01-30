# WebScraping Module

Production-ready web scraping module for financial data collection.

## Directory Structure (production-ready)

```
WebScraping/
├── src/
│   ├── scrapers/           # Core scraping: WebScraper, DataframeStore
│   │   ├── WebScraper.py
│   │   └── DataframeStore.py
│   ├── selectors/          # Centralized Yahoo Finance selectors
│   │   ├── stock_collector_selectors.py
│   │   └── YahooFinanceHTMLElements.py
│   ├── utils/              # DOM utilities (save/validate/repair)
│   │   ├── HTMLDOMUtils.py
│   │   └── HTMLDOMUtils_example.py
│   ├── collectors/         # News/stock collector examples
│   │   ├── news_collector_example.py
│   │   └── news_collector_refactored_example.py
│   ├── WebScraper.py       # Compatibility: re-exports from scrapers
│   └── DataframeStore.py   # Compatibility: re-exports from scrapers
├── notebooks/
├── docs/
├── data/
└── tests/
    └── test_stock_collector_selectors.py
```

## Usage

### From project root (recommended)

```python
from WebScraping.src.scrapers import Scrapper, StocksScrapper, NewsScrapper, DataFrameStore
from WebScraping.src.selectors import YahooFinanceStockSelectors, find_element_with_fallbacks
from WebScraping.src.utils import HTMLDOMUtils, validate_xpath
```

### With `src` on path (notebooks / legacy)

```python
from WebScraper import Scrapper, StocksScrapper
from DataframeStore import DataFrameStore
```

## Development

- **Source code**: All production code lives in `src/`
- **Experimentation**: Use notebooks in `notebooks/` for testing and exploration
- **Documentation**: Analysis and reference docs are in `docs/`
- **Generated files**: Output files like HTML dumps go in `data/`

