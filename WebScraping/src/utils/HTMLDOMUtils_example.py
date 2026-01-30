"""
Example usage of HTMLDOMUtils

This file demonstrates how to use the HTMLDOMUtils class for:
1. Saving HTML DOM trees
2. Validating XPath expressions
3. Finding and extracting XPath from elements
"""

try:
    from WebScraping.src.utils.HTMLDOMUtils import (
        HTMLDOMUtils,
        save_dom_from_url,
        validate_xpath,
        get_xpath_by_tag,
    )
except ImportError:
    from .HTMLDOMUtils import (
        HTMLDOMUtils,
        save_dom_from_url,
        validate_xpath,
        get_xpath_by_tag,
    )
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def example_save_dom_from_url():
    """Example: Save DOM tree from a URL"""
    print("=" * 60)
    print("Example 1: Saving DOM tree from URL")
    print("=" * 60)
    
    url = "https://finance.yahoo.com/quote/BTC-USD/history"
    
    # Method 1: Using convenience function
    saved_files = save_dom_from_url(url, data_folder="data", prettify=True)
    print(f"Saved files: {saved_files}")
    
    # Method 2: Using class instance
    utils = HTMLDOMUtils(data_folder="data")
    saved_files = utils.save_dom_from_url(url, prettify=True)
    print(f"Saved files: {saved_files}")


def example_save_dom_from_selenium():
    """Example: Save DOM tree from existing Selenium driver"""
    print("\n" + "=" * 60)
    print("Example 2: Saving DOM tree from Selenium driver")
    print("=" * 60)
    
    # Create driver
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    try:
        url = "https://finance.yahoo.com/quote/BTC-USD/history"
        driver.get(url)
        
        # Get HTML content
        html_content = driver.page_source
        
        # Save using utils
        utils = HTMLDOMUtils(data_folder="data")
        saved_files = utils.save_dom_tree(html_content, url=url, prettify=True)
        print(f"Saved files: {saved_files}")
        
    finally:
        driver.quit()


def example_validate_xpath():
    """Example: Validate XPath expressions"""
    print("\n" + "=" * 60)
    print("Example 3: Validating XPath expressions")
    print("=" * 60)
    
    # First, save a DOM tree
    url = "https://finance.yahoo.com/quote/BTC-USD/history"
    utils = HTMLDOMUtils(data_folder="data")
    saved_files = utils.save_dom_from_url(url, prettify=True)
    html_file = saved_files['raw']
    
    # Test XPath expressions
    xpaths_to_test = [
        "/html/body/div[1]/main/section/section/section/article/div[1]/div[3]/table/tbody",
        "/html/body/div[2]/main/section/section/section/article/div[1]/div[3]/table/tbody",
        "//table[@data-test='historical-prices']/tbody",
        "//table/tbody",
        "//tbody",
        "/invalid/xpath/that/does/not/exist"
    ]
    
    for xpath in xpaths_to_test:
        exists, error_msg, match_count = utils.validate_xpath(xpath, html_file=html_file)
        status = "✓ EXISTS" if exists else "✗ NOT FOUND"
        print(f"{status}: {xpath}")
        if exists:
            print(f"  → Matched {match_count} element(s)")
        else:
            print(f"  → {error_msg}")


def example_find_xpath_by_tag():
    """Example: Find XPath by tag name"""
    print("\n" + "=" * 60)
    print("Example 4: Finding XPath by tag name")
    print("=" * 60)
    
    # Save DOM tree
    url = "https://finance.yahoo.com/quote/BTC-USD/history"
    utils = HTMLDOMUtils(data_folder="data")
    saved_files = utils.save_dom_from_url(url, prettify=True)
    html_file = saved_files['raw']
    
    # Find tbody elements
    print("Finding all 'tbody' elements:")
    tbody_xpaths = utils.get_xpath_by_tag("tbody", html_file=html_file, return_all=True)
    
    if tbody_xpaths:
        print(f"Found {len(tbody_xpaths)} tbody element(s):")
        for i, xpath in enumerate(tbody_xpaths[:5], 1):  # Show first 5
            print(f"  {i}. {xpath}")
    else:
        print("  No tbody elements found")
    
    # Find first table element
    print("\nFinding first 'table' element:")
    table_xpath = utils.get_xpath_by_tag("table", html_file=html_file, return_all=False)
    if table_xpath:
        print(f"  → {table_xpath}")
    else:
        print("  → No table elements found")


def example_find_xpath_by_attribute():
    """Example: Find XPath by attribute"""
    print("\n" + "=" * 60)
    print("Example 5: Finding XPath by attribute")
    print("=" * 60)
    
    # Save DOM tree
    url = "https://finance.yahoo.com/quote/BTC-USD/history"
    utils = HTMLDOMUtils(data_folder="data")
    saved_files = utils.save_dom_from_url(url, prettify=True)
    html_file = saved_files['raw']
    
    # Find by data-test attribute
    print("Finding elements with data-test='historical-prices':")
    xpath = utils.get_xpath_by_attribute(
        "data-test",
        "historical-prices",
        html_file=html_file,
        exact_match=True
    )
    if xpath:
        print(f"  → {xpath}")
    else:
        print("  → Not found")
    
    # Find by class (contains match)
    print("\nFinding elements with class containing 'table':")
    xpaths = utils.get_xpath_by_attribute(
        "class",
        "table",
        html_file=html_file,
        exact_match=False  # Contains match
    )
    if xpaths:
        if isinstance(xpaths, list):
            print(f"  → Found {len(xpaths)} element(s):")
            for i, xp in enumerate(xpaths[:3], 1):  # Show first 3
                print(f"    {i}. {xp}")
        else:
            print(f"  → {xpaths}")
    else:
        print("  → Not found")


def example_find_xpath_by_multiple_criteria():
    """Example: Find XPath using multiple criteria"""
    print("\n" + "=" * 60)
    print("Example 6: Finding XPath with multiple criteria")
    print("=" * 60)
    
    # Save DOM tree
    url = "https://finance.yahoo.com/quote/BTC-USD/history"
    utils = HTMLDOMUtils(data_folder="data")
    saved_files = utils.save_dom_from_url(url, prettify=True)
    html_file = saved_files['raw']
    
    # Find tbody within table
    print("Finding tbody elements with specific attributes:")
    xpaths = utils.find_element_xpath(
        tag_name="tbody",
        attributes={"class": "*table"},  # Contains 'table' in class
        html_file=html_file,
        return_all=True
    )
    
    if xpaths:
        print(f"Found {len(xpaths)} matching element(s):")
        for i, xpath in enumerate(xpaths[:3], 1):
            print(f"  {i}. {xpath}")
    else:
        print("  → No matching elements found")


def example_analyze_xpath():
    """Example: Analyze XPath and get alternatives"""
    print("\n" + "=" * 60)
    print("Example 7: Analyzing XPath and getting alternatives")
    print("=" * 60)
    
    # Save DOM tree
    url = "https://finance.yahoo.com/quote/BTC-USD/history"
    utils = HTMLDOMUtils(data_folder="data")
    saved_files = utils.save_dom_from_url(url, prettify=True)
    html_file = saved_files['raw']
    
    # Analyze a potentially failing XPath
    xpath = "/html/body/div[2]/div[2]/main/section/section/section/section/div[1]/div[3]/table/tbody"
    
    analysis = utils.analyze_xpath_alternatives(xpath, html_file=html_file)
    
    print(f"Original XPath: {analysis['original_xpath']}")
    print(f"Exists: {analysis['exists']}")
    print(f"Match count: {analysis['match_count']}")
    
    if analysis['error']:
        print(f"Error: {analysis['error']}")
    
    if analysis['alternatives']:
        print(f"\nAlternative XPaths ({len(analysis['alternatives'])}):")
        for i, alt in enumerate(analysis['alternatives'], 1):
            print(f"  {i}. {alt}")
    
    if analysis['suggestions']:
        print(f"\nSuggestions:")
        for i, suggestion in enumerate(analysis['suggestions'], 1):
            print(f"  {i}. {suggestion}")


if __name__ == "__main__":
    print("HTMLDOMUtils Examples")
    print("=" * 60)
    
    # Run examples
    try:
        example_save_dom_from_url()
        example_save_dom_from_selenium()
        example_validate_xpath()
        example_find_xpath_by_tag()
        example_find_xpath_by_attribute()
        example_find_xpath_by_multiple_criteria()
        example_analyze_xpath()
        
        print("\n" + "=" * 60)
        print("All examples completed!")
        print("=" * 60)
    
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()
