"""
Utils package: DOM and HTML utilities for scraping and repair workflows.
"""

from .HTMLDOMUtils import (
    HTMLDOMUtils,
    save_dom_from_url,
    validate_xpath,
    get_xpath_by_tag,
)

__all__ = [
    "HTMLDOMUtils",
    "save_dom_from_url",
    "validate_xpath",
    "get_xpath_by_tag",
]
