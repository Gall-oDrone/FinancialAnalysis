# HTMLDOMUtils Module Guide

## Overview

The `HTMLDOMUtils` module provides powerful utilities for working with HTML DOM trees, particularly useful for:
- **Saving HTML DOM trees** to files for analysis
- **Validating XPath expressions** to check if they exist in the DOM
- **Extracting full XPath** from HTML elements based on tags, attributes, or content
- **Building AI agents** that can dynamically discover and set correct HTML paths

## Why This Approach?

### 1. **lxml Library**
We use `lxml` instead of BeautifulSoup because:
- **Native XPath support**: lxml has built-in, robust XPath evaluation
- **Performance**: Faster parsing and XPath evaluation than BeautifulSoup
- **Accuracy**: Better handling of malformed HTML
- **XPath generation**: Can programmatically build and validate XPath expressions

### 2. **Selenium Integration**
- Works seamlessly with existing Selenium WebDriver instances
- Can also create headless browsers automatically
- Captures fully rendered DOM (including JavaScript-generated content)

### 3. **Flexible Element Finding**
- Find by tag name, attributes, text content, or combinations
- Supports exact match, contains match, and starts-with match
- Returns full XPath paths that can be used directly in Selenium

## Installation

```bash
pip install lxml selenium webdriver-manager
```

Or install from requirements:
```bash
pip install -r requirements.txt
```

## Quick Start

### 1. Save DOM Tree from URL

```python
from HTMLDOMUtils import HTMLDOMUtils

# Create utils instance
utils = HTMLDOMUtils(data_folder="data")

# Save DOM tree from URL
url = "https://finance.yahoo.com/quote/BTC-USD/history"
saved_files = utils.save_dom_from_url(url, prettify=True)
print(f"Saved to: {saved_files['raw']}")
print(f"Prettified: {saved_files['prettified']}")
```

### 2. Validate XPath

```python
# Check if an XPath exists
xpath = "/html/body/div[1]/main/section/section/section/article/div[1]/div[3]/table/tbody"
exists, error_msg, match_count = utils.validate_xpath(xpath, html_file="data/dom_tree_*.html")

if exists:
    print(f"✓ XPath exists! Matched {match_count} element(s)")
else:
    print(f"✗ XPath not found: {error_msg}")
```

### 3. Find XPath by Tag Name

```python
# Find all tbody elements and get their XPaths
tbody_xpaths = utils.get_xpath_by_tag("tbody", html_file="data/dom_tree_*.html", return_all=True)

for xpath in tbody_xpaths:
    print(f"Found tbody at: {xpath}")
```

### 4. Find XPath by Attribute

```python
# Find element by data-test attribute
xpath = utils.get_xpath_by_attribute(
    "data-test",
    "historical-prices",
    html_file="data/dom_tree_*.html"
)
print(f"XPath: {xpath}")
```

### 5. Find XPath with Multiple Criteria

```python
# Find tbody with specific class
xpaths = utils.find_element_xpath(
    tag_name="tbody",
    attributes={"class": "*table"},  # Contains 'table' in class
    html_file="data/dom_tree_*.html",
    return_all=True
)
```

### 6. Analyze XPath and Get Alternatives

```python
# Analyze a failing XPath and get alternatives
xpath = "/html/body/div[2]/div[2]/main/section/section/section/section/div[1]/div[3]/table/tbody"
analysis = utils.analyze_xpath_alternatives(xpath, html_file="data/dom_tree_*.html")

if not analysis['exists']:
    print("Original XPath doesn't exist. Alternatives:")
    for alt in analysis['alternatives']:
        print(f"  - {alt}")
```

## Use Cases

### Use Case 1: Debugging XPath Issues

When your scraper fails with "element not found", save the DOM and analyze:

```python
# In your scraper, when XPath fails:
utils = HTMLDOMUtils()
saved_files = utils.save_dom_tree(driver.page_source, url=driver.current_url)

# Analyze the failing XPath
analysis = utils.analyze_xpath_alternatives(failing_xpath, html_file=saved_files['raw'])
print(analysis['alternatives'])  # Get working alternatives
```

### Use Case 2: Dynamic XPath Discovery for AI Agents

AI agents can discover XPaths dynamically:

```python
# AI agent wants to find the table body
utils = HTMLDOMUtils()

# Save current page
saved_files = utils.save_dom_from_url(current_url)

# Find tbody XPath
tbody_xpath = utils.get_xpath_by_tag("tbody", html_file=saved_files['raw'])

# Use the XPath in Selenium
table_body = driver.find_element(By.XPATH, tbody_xpath)
```

### Use Case 3: XPath Validation Before Use

Validate XPath before using it in production:

```python
# Test XPath against saved DOM
xpath = "/html/body/div[1]/main/section/section/section/article/div[1]/div[3]/table/tbody"
exists, _, count = utils.validate_xpath(xpath, html_file="data/latest_dom.html")

if exists and count > 0:
    # XPath is valid, use it
    element = driver.find_element(By.XPATH, xpath)
else:
    # Find alternative
    alt_xpath = utils.get_xpath_by_tag("tbody", html_file="data/latest_dom.html")
    element = driver.find_element(By.XPATH, alt_xpath)
```

## API Reference

### HTMLDOMUtils Class

#### `__init__(data_folder="data")`
Initialize the utility class.

#### `save_dom_tree(html_content, url=None, filename=None, prettify=True)`
Save HTML content to file(s).

**Returns:** Dictionary with 'raw' and optionally 'prettified' file paths

#### `save_dom_from_url(url, driver=None, wait_time=2.0, prettify=True)`
Load URL and save its DOM tree.

**Returns:** Dictionary with saved file paths

#### `validate_xpath(xpath, html_content=None, html_file=None)`
Validate if XPath exists in HTML.

**Returns:** Tuple of (exists: bool, error_message: str, match_count: int)

#### `find_element_xpath(tag_name=None, attributes=None, text_content=None, html_content=None, html_file=None, return_all=False)`
Find element(s) and return XPath(s).

**Returns:** XPath string or list of XPaths

#### `get_xpath_by_tag(tag_name, html_content=None, html_file=None, return_all=False)`
Find XPath by tag name.

#### `get_xpath_by_attribute(attribute_name, attribute_value, html_content=None, html_file=None, exact_match=True)`
Find XPath by attribute.

#### `analyze_xpath_alternatives(xpath, html_content=None, html_file=None)`
Analyze XPath and provide alternatives if it fails.

**Returns:** Dictionary with analysis results

## Best Practices

1. **Save DOM trees regularly** when debugging XPath issues
2. **Use data attributes** (`data-test`, `data-id`) for more stable XPaths
3. **Validate XPaths** before using them in production code
4. **Use `analyze_xpath_alternatives`** when XPath fails to get suggestions
5. **Store DOM trees** in version control (if small) or document which DOM version your XPaths work with

## Integration with StockCollector

You can integrate this into your StockCollector notebook:

```python
from HTMLDOMUtils import HTMLDOMUtils

utils = HTMLDOMUtils(data_folder="data")

# When XPath fails, save DOM and find correct XPath
if table is None:
    # Save current page
    saved_files = utils.save_dom_tree(driver.page_source, url=target_url)
    
    # Find tbody XPath
    tbody_xpath = utils.get_xpath_by_tag("tbody", html_file=saved_files['raw'])
    
    if tbody_xpath:
        table = driver.find_element(By.XPATH, tbody_xpath)
```

## File Structure

```
WebScraping/
├── src/
│   ├── HTMLDOMUtils.py          # Main utility module
│   └── HTMLDOMUtils_example.py  # Usage examples
├── data/                         # Saved DOM trees
│   ├── dom_tree_*.html
│   └── dom_tree_*_prettified.html
└── docs/
    └── HTMLDOMUtils_GUIDE.md    # This file
```

## Troubleshooting

### Issue: "lxml not available"
**Solution:** Install lxml: `pip install lxml`

### Issue: XPath not found but element exists
**Solution:** 
- Check if HTML is fully loaded (wait for JavaScript)
- Try using `analyze_xpath_alternatives` to find alternatives
- Use more generic XPath (e.g., `//tbody` instead of full path)

### Issue: Multiple elements match
**Solution:** 
- Use `return_all=True` to get all matches
- Add more specific attributes to narrow down
- Use `find_element_xpath` with multiple criteria

## Future Enhancements

Potential improvements:
- [ ] Support for CSS selectors
- [ ] XPath optimization (shortest path)
- [ ] Visual diff of DOM trees
- [ ] XPath learning from examples
- [ ] Integration with AI models for XPath generation
