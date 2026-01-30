"""
Pytest configuration for WebScraping tests.
Ensures WebScraping package is importable when running from project root.
"""

import sys
from pathlib import Path

# Add project root so "from WebScraping.src.selectors ..." works
project_root = Path(__file__).resolve().parent.parent.parent
if project_root.exists() and str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
