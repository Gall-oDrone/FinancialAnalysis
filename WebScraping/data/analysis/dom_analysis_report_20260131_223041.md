# NewsCollector DOM Analysis Report

**Generated:** 2026-01-31T22:30:41.567108
**URL:** https://finance.yahoo.com/topic/crypto/

## Summary

- Total XPaths checked: 7
- OK: 1
- FAIL: 6

## Saved DOM Files

- Raw HTML: `/Users/diegogallovalenzuela/financial_analysis/WebScraping/data/dom_tree_finance_yahoo_com_topic_crypto_20260131_220443.html`
- Prettified: ``

## XPath Validation Results

### main_headline_h1 [✗]

- **Status:** FAIL
- **XPath:** `/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div[1]/h1`
- **Error:** XPath matched 0 elements

### news_list_ul [✗]

- **Status:** FAIL
- **XPath:** `/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[3]/div/div/div/ul`
- **Error:** XPath matched 0 elements

### extractToText_fallback_h1 [✗]

- **Status:** FAIL
- **XPath:** `/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/div/div[1]/h1`
- **Error:** XPath matched 0 elements

### ARTICLE_GRIDLAYOUT [✗]

- **Status:** FAIL
- **XPath:** `/html/body/div[2]/main/section/section/section/article`
- **Error:** XPath matched 0 elements

### SECTION_TOPICHERO [✗]

- **Status:** FAIL
- **XPath:** `/html/body/div[2]/main/section/section/section/article/section[1]`
- **Error:** XPath matched 0 elements

### UL_STREAM_ITEMS_yf_1drgw5l [✗]

- **Status:** FAIL
- **XPath:** `/html/body/div[2]/main/section/section/section/section/section/div/div/div/div/ul`
- **Error:** XPath matched 0 elements

### UL_STREAM_ITEMS_yf_9xydx9 [✓]

- **Status:** OK
- **XPath:** `/html/body/div[2]/div[3]/main/section/section/section/section/section/div/div[1]/div/div/ul`
- **Match count:** 1

## Element Candidates

### H1 (main headline)

- No h1 elements found.

### UL (news list)

- No ul elements found.

## Files to Update

When XPaths fail, update one of:

- `WebScraping/src/selectors/YahooFinanceHTMLElements.py`
- `WebScraping/notebooks/NewsCollector-Staging.ipynb` (hardcoded XPaths in `selectUnorderList`, `scrap_data`, etc.)
