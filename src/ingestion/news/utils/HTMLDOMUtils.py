"""
HTML DOM Utilities Module

This module provides utilities for:
1. Saving HTML DOM trees to files
2. Validating XPath expressions
3. Extracting full XPath from HTML elements/attributes
4. Building XPath dynamically for AI agents

Author: Financial Analysis Toolkit
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Union, Tuple
from urllib.parse import urlparse

try:
    from lxml import etree, html
    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False
    print("Warning: lxml not available. Some features may be limited. Install with: pip install lxml")

try:
    from selenium import webdriver
    from selenium.webdriver.remote.webdriver import WebDriver
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Warning: selenium not available. Install with: pip install selenium")


class HTMLDOMUtils:
    """
    Utility class for HTML DOM operations including saving, validating XPath,
    and extracting XPath from elements.
    """
    
    def __init__(self, data_folder: str = "data"):
        """
        Initialize HTML DOM Utilities.
        
        Args:
            data_folder: Path to folder where HTML files will be saved.
                        Can be relative to WebScraping directory or absolute path.
        """
        # Determine the base directory (WebScraping folder)
        current_file = Path(__file__).resolve()
        # Go up from utils/ to src/ to WebScraping/
        webscraping_dir = current_file.parent.parent.parent
        
        # Set data folder path
        if os.path.isabs(data_folder):
            self.data_folder = Path(data_folder)
        else:
            self.data_folder = webscraping_dir / data_folder
        
        # Create data folder if it doesn't exist
        self.data_folder.mkdir(parents=True, exist_ok=True)
        
        if not LXML_AVAILABLE:
            raise ImportError("lxml is required for HTMLDOMUtils. Install with: pip install lxml")
    
    def save_dom_tree(
        self,
        html_content: str,
        url: Optional[str] = None,
        filename: Optional[str] = None,
        prettify: bool = True
    ) -> Dict[str, str]:
        """
        Save HTML DOM tree to file(s).
        
        Args:
            html_content: HTML content as string (from driver.page_source or similar)
            url: Optional URL for naming the file
            filename: Optional custom filename (without extension)
            prettify: If True, also save a prettified version
        
        Returns:
            Dictionary with paths to saved files:
            {
                'raw': '/path/to/file.html',
                'prettified': '/path/to/file_prettified.html'  # if prettify=True
            }
        """
        # Generate filename
        if filename:
            base_name = filename
        elif url:
            # Extract domain and path from URL for filename
            parsed = urlparse(url)
            domain = parsed.netloc.replace('.', '_')
            path = parsed.path.strip('/').replace('/', '_') or 'index'
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = f"dom_tree_{domain}_{path}_{timestamp}"
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = f"dom_tree_{timestamp}"
        
        saved_files = {}
        
        # Save raw HTML
        raw_file = self.data_folder / f"{base_name}.html"
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        saved_files['raw'] = str(raw_file)
        
        # Save prettified version if requested
        if prettify:
            try:
                # Parse and prettify HTML
                doc = html.fromstring(html_content)
                prettified_html = etree.tostring(
                    doc,
                    pretty_print=True,
                    encoding='unicode',
                    method='html'
                )
                
                prettified_file = self.data_folder / f"{base_name}_prettified.html"
                with open(prettified_file, 'w', encoding='utf-8') as f:
                    f.write(prettified_html)
                saved_files['prettified'] = str(prettified_file)
            except Exception as e:
                print(f"Warning: Could not prettify HTML: {e}")
        
        return saved_files
    
    def save_dom_from_url(
        self,
        url: str,
        driver: Optional[WebDriver] = None,
        wait_time: float = 2.0,
        prettify: bool = True
    ) -> Dict[str, str]:
        """
        Load a URL using Selenium and save its DOM tree.
        
        Args:
            url: URL to load
            driver: Optional Selenium WebDriver instance. If None, creates a headless Chrome driver.
            wait_time: Time to wait after page load (seconds)
            prettify: If True, also save a prettified version
        
        Returns:
            Dictionary with paths to saved files
        """
        if not SELENIUM_AVAILABLE:
            raise ImportError("selenium is required for save_dom_from_url. Install with: pip install selenium")
        
        # Create driver if not provided
        create_driver = driver is None
        if create_driver:
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options
            )
        
        try:
            # Load the page
            driver.get(url)
            
            # Wait for page to load
            import time
            time.sleep(wait_time)
            
            # Get page source
            html_content = driver.page_source
            
            # Save to file
            return self.save_dom_tree(html_content, url=url, prettify=prettify)
        
        finally:
            # Close driver if we created it
            if create_driver:
                driver.quit()
    
    def validate_xpath(
        self,
        xpath: str,
        html_content: Optional[str] = None,
        html_file: Optional[str] = None
    ) -> Tuple[bool, Optional[str], int]:
        """
        Validate if an XPath exists in the HTML DOM.
        
        Args:
            xpath: XPath expression to validate
            html_content: HTML content as string
            html_file: Path to HTML file (alternative to html_content)
        
        Returns:
            Tuple of (exists: bool, error_message: Optional[str], match_count: int)
            - exists: True if XPath matches at least one element
            - error_message: Error message if XPath is invalid
            - match_count: Number of elements matched
        """
        # Load HTML
        if html_file:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
        
        if not html_content:
            return False, "No HTML content provided", 0
        
        try:
            # Parse HTML
            doc = html.fromstring(html_content)
            
            # Try to evaluate XPath
            try:
                elements = doc.xpath(xpath)
                match_count = len(elements)
                exists = match_count > 0
                error_msg = None if exists else f"XPath matched 0 elements"
                return exists, error_msg, match_count
            except etree.XPathEvalError as e:
                return False, f"Invalid XPath expression: {str(e)}", 0
        
        except Exception as e:
            return False, f"Error parsing HTML: {str(e)}", 0
    
    def find_element_xpath(
        self,
        tag_name: Optional[str] = None,
        attributes: Optional[Dict[str, str]] = None,
        text_content: Optional[str] = None,
        html_content: Optional[str] = None,
        html_file: Optional[str] = None,
        return_all: bool = False
    ) -> Union[str, List[str], None]:
        """
        Find element(s) by tag name, attributes, or text content and return their XPath(s).
        
        Args:
            tag_name: HTML tag name (e.g., 'tbody', 'div', 'table')
            attributes: Dictionary of attribute name-value pairs (e.g., {'class': 'table', 'id': 'main'})
            text_content: Exact or partial text content to match
            html_content: HTML content as string
            html_file: Path to HTML file (alternative to html_content)
            return_all: If True, return all matching XPaths; if False, return only the first
        
        Returns:
            XPath string (or list of XPaths if return_all=True), or None if not found
        """
        # Load HTML
        if html_file:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
        
        if not html_content:
            return None
        
        try:
            # Parse HTML
            doc = html.fromstring(html_content)
            
            # Build XPath query
            xpath_parts = []
            
            if tag_name:
                xpath_parts.append(tag_name)
            else:
                xpath_parts.append('*')
            
            # Add attribute conditions
            if attributes:
                attr_conditions = []
                for attr_name, attr_value in attributes.items():
                    # Handle different attribute matching strategies
                    if attr_value.startswith('*'):
                        # Contains
                        attr_conditions.append(f"contains(@{attr_name}, '{attr_value[1:]}')")
                    elif attr_value.startswith('^'):
                        # Starts with
                        attr_conditions.append(f"starts-with(@{attr_name}, '{attr_value[1:]}')")
                    else:
                        # Exact match
                        attr_conditions.append(f"@{attr_name}='{attr_value}'")
                
                if attr_conditions:
                    xpath_parts.append('[' + ' and '.join(attr_conditions) + ']')
            
            # Add text content condition
            if text_content:
                if '[' not in ''.join(xpath_parts):
                    xpath_parts.append('[]')
                # Modify the last bracket to include text condition
                text_condition = f"contains(text(), '{text_content}')"
                if xpath_parts[-1].startswith('['):
                    # Append to existing conditions
                    xpath_parts[-1] = xpath_parts[-1].rstrip(']') + f" and {text_condition}]"
                else:
                    xpath_parts.append(f"[{text_condition}]")
            
            # Build full XPath
            search_xpath = ''.join(xpath_parts)
            
            # Find elements
            elements = doc.xpath(search_xpath)
            
            if not elements:
                return None
            
            # Generate full XPath for each element
            xpaths = []
            for element in elements:
                xpath = self._get_element_xpath(element)
                xpaths.append(xpath)
            
            if return_all:
                return xpaths
            else:
                return xpaths[0] if xpaths else None
        
        except Exception as e:
            print(f"Error finding element XPath: {e}")
            return None
    
    def _get_element_xpath(self, element) -> str:
        """
        Generate the full XPath for a given element.
        
        Args:
            element: lxml element
        
        Returns:
            Full XPath string (e.g., /html/body/div[2]/table/tbody)
        """
        parts = []
        current = element
        
        while current is not None:
            # Get tag name
            tag = current.tag
            
            # Skip root element (html)
            if tag == 'html':
                parts.insert(0, '/html')
                break
            
            # Count siblings with same tag name
            parent = current.getparent()
            if parent is not None:
                siblings = [s for s in parent if s.tag == tag]
                if len(siblings) > 1:
                    # Multiple siblings, need index
                    index = siblings.index(current) + 1
                    parts.insert(0, f"{tag}[{index}]")
                else:
                    # Only one sibling, no index needed
                    parts.insert(0, tag)
            else:
                parts.insert(0, tag)
            
            current = parent
        
        return '/' + '/'.join(parts)
    
    def get_xpath_by_attribute(
        self,
        attribute_name: str,
        attribute_value: str,
        html_content: Optional[str] = None,
        html_file: Optional[str] = None,
        exact_match: bool = True
    ) -> Union[str, List[str], None]:
        """
        Find element(s) by attribute and return XPath(s).
        
        Args:
            attribute_name: Attribute name (e.g., 'id', 'class', 'data-test')
            attribute_value: Attribute value to match
            html_content: HTML content as string
            html_file: Path to HTML file
            exact_match: If True, exact match; if False, contains match
        
        Returns:
            XPath string (or list if multiple matches), or None
        """
        if exact_match:
            attributes = {attribute_name: attribute_value}
        else:
            attributes = {attribute_name: f"*{attribute_value}"}  # Use contains syntax
        
        return self.find_element_xpath(
            attributes=attributes,
            html_content=html_content,
            html_file=html_file,
            return_all=not exact_match
        )
    
    def get_xpath_by_tag(
        self,
        tag_name: str,
        html_content: Optional[str] = None,
        html_file: Optional[str] = None,
        return_all: bool = False
    ) -> Union[str, List[str], None]:
        """
        Find element(s) by tag name and return XPath(s).
        
        Args:
            tag_name: HTML tag name (e.g., 'tbody', 'table', 'div')
            html_content: HTML content as string
            html_file: Path to HTML file
            return_all: If True, return all matching XPaths
        
        Returns:
            XPath string (or list if return_all=True), or None
        """
        return self.find_element_xpath(
            tag_name=tag_name,
            html_content=html_content,
            html_file=html_file,
            return_all=return_all
        )
    
    def analyze_xpath_alternatives(
        self,
        xpath: str,
        html_content: Optional[str] = None,
        html_file: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Analyze an XPath and provide alternative XPath expressions if the original fails.
        
        Args:
            xpath: Original XPath to analyze
            html_content: HTML content as string
            html_file: Path to HTML file
        
        Returns:
            Dictionary with analysis results including:
            - original_xpath: The original XPath
            - exists: Whether the XPath exists
            - match_count: Number of matches
            - alternatives: List of alternative XPath expressions
            - suggestions: Suggestions for improving the XPath
        """
        # Load HTML
        if html_file:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
        
        if not html_content:
            return {
                'original_xpath': xpath,
                'exists': False,
                'match_count': 0,
                'error': 'No HTML content provided',
                'alternatives': [],
                'suggestions': []
            }
        
        # Validate original XPath
        exists, error_msg, match_count = self.validate_xpath(xpath, html_content=html_content)
        
        result = {
            'original_xpath': xpath,
            'exists': exists,
            'match_count': match_count,
            'error': error_msg,
            'alternatives': [],
            'suggestions': []
        }
        
        if not exists:
            # Try to generate alternatives
            try:
                doc = html.fromstring(html_content)
                
                # Extract tag name from XPath
                tag_match = re.search(r'/([a-zA-Z]+)(?:\[|$|/)', xpath)
                if tag_match:
                    tag_name = tag_match.group(1)
                    
                    # Try finding by tag name
                    alt_xpaths = self.get_xpath_by_tag(
                        tag_name,
                        html_content=html_content,
                        return_all=True
                    )
                    
                    if alt_xpaths:
                        result['alternatives'] = alt_xpaths[:5]  # Limit to 5 alternatives
                        result['suggestions'].append(
                            f"Found {len(alt_xpaths)} elements with tag '{tag_name}'. "
                            f"Try using one of the alternative XPaths above."
                        )
                
                # Suggest using data attributes or classes
                result['suggestions'].append(
                    "Consider using data attributes (e.g., @data-test) or class names "
                    "for more stable XPath expressions."
                )
                
            except Exception as e:
                result['suggestions'].append(f"Could not generate alternatives: {str(e)}")
        
        return result


# Convenience functions for easy usage
def save_dom_from_url(url: str, data_folder: str = "data", **kwargs) -> Dict[str, str]:
    """
    Convenience function to save DOM tree from URL.
    
    Args:
        url: URL to load and save
        data_folder: Folder to save HTML files
        **kwargs: Additional arguments passed to save_dom_from_url
    
    Returns:
        Dictionary with paths to saved files
    """
    utils = HTMLDOMUtils(data_folder=data_folder)
    return utils.save_dom_from_url(url, **kwargs)


def validate_xpath(xpath: str, html_file: str, **kwargs) -> Tuple[bool, Optional[str], int]:
    """
    Convenience function to validate XPath.
    
    Args:
        xpath: XPath expression to validate
        html_file: Path to HTML file
        **kwargs: Additional arguments
    
    Returns:
        Tuple of (exists, error_message, match_count)
    """
    utils = HTMLDOMUtils()
    return utils.validate_xpath(xpath, html_file=html_file, **kwargs)


def get_xpath_by_tag(tag_name: str, html_file: str, **kwargs) -> Union[str, List[str], None]:
    """
    Convenience function to get XPath by tag name.
    
    Args:
        tag_name: HTML tag name
        html_file: Path to HTML file
        **kwargs: Additional arguments
    
    Returns:
        XPath string or list of XPaths
    """
    utils = HTMLDOMUtils()
    return utils.get_xpath_by_tag(tag_name, html_file=html_file, **kwargs)
