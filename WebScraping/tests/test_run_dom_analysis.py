"""
Tests for run_dom_analysis.py: DOM save and XPath analysis for NewsCollector.

Uses mocked Selenium driver to avoid network calls. Tests the analysis logic
with sample HTML.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# Sample HTML for testing - has h1 and ul elements
SAMPLE_HTML = """<!DOCTYPE html>
<html>
<head><title>Yahoo Finance - Crypto</title></head>
<body>
  <div id="root">
    <main>
      <section>
        <h1>Crypto News</h1>
        <ul class="stream-items">
          <li><a href="/news/1">Article 1</a></li>
          <li><a href="/news/2">Article 2</a></li>
        </ul>
      </section>
    </main>
  </div>
</body>
</html>
"""


class TestRunDomAnalysisConstants:
    """Test that XPath constants are properly defined."""

    def test_news_collector_xpaths_defined(self):
        from WebScraping.run_dom_analysis import NEWS_COLLECTOR_XPATHS

        assert isinstance(NEWS_COLLECTOR_XPATHS, dict)
        assert len(NEWS_COLLECTOR_XPATHS) >= 1
        for name, xpath in NEWS_COLLECTOR_XPATHS.items():
            assert isinstance(name, str)
            assert isinstance(xpath, str)
            assert xpath.startswith("/") or "//" in xpath

    def test_yahoo_elements_defined(self):
        from WebScraping.run_dom_analysis import YAHOO_ELEMENTS

        assert isinstance(YAHOO_ELEMENTS, dict)
        assert len(YAHOO_ELEMENTS) >= 1
        for name, xpath in YAHOO_ELEMENTS.items():
            assert isinstance(name, str)
            assert isinstance(xpath, str)


class TestRunDomAnalysisLogic:
    """Test DOM analysis logic with mocked driver and sample HTML."""

    def test_main_with_mocked_driver_saves_dom_and_runs_analysis(self):
        """main() should run without error when driver is mocked with sample HTML."""
        mock_driver = MagicMock()
        mock_driver.page_source = SAMPLE_HTML
        mock_driver.current_url = "https://finance.yahoo.com/topic/crypto/"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("sys.argv", ["run_dom_analysis.py"]), patch(
                "WebScraping.run_dom_analysis.create_driver",
                return_value=mock_driver,
            ), patch("WebScraping.run_dom_analysis.time.sleep"), patch(
                "WebScraping.run_dom_analysis._script_dir", tmpdir
            ):
                # Patch _script_dir so data_folder is in tmpdir
                import WebScraping.run_dom_analysis as run_dom

                original_script_dir = run_dom._script_dir
                run_dom._script_dir = tmpdir

                try:
                    run_dom.main()
                finally:
                    run_dom._script_dir = original_script_dir

            # Verify data folder was created and contains HTML files
            data_folder = Path(tmpdir) / "data"
            assert data_folder.exists()
            html_files = list(data_folder.glob("*.html"))
            assert len(html_files) >= 1

            # Verify analysis folder was created with report files
            analysis_folder = data_folder / "analysis"
            assert analysis_folder.exists()
            report_files = list(analysis_folder.glob("dom_analysis_report_*.md"))
            assert len(report_files) >= 1
            json_reports = list(analysis_folder.glob("dom_analysis_report_*.json"))
            assert len(json_reports) >= 1

    def test_analysis_with_sample_html_finds_h1_and_ul(self):
        """HTMLDOMUtils should find h1 and ul in sample HTML via validate_xpath."""
        from WebScraping.src.utils.HTMLDOMUtils import HTMLDOMUtils

        with tempfile.TemporaryDirectory() as tmpdir:
            utils = HTMLDOMUtils(data_folder=tmpdir)

            # Save sample HTML
            saved = utils.save_dom_tree(SAMPLE_HTML, url="https://test.com", prettify=True)
            html_file = saved.get("raw")
            assert os.path.exists(html_file)

            # validate_xpath with //h1 and //ul should find elements
            exists, _, count = utils.validate_xpath("//h1", html_file=html_file)
            assert exists
            assert count >= 1

            exists, _, count = utils.validate_xpath("//ul", html_file=html_file)
            assert exists
            assert count >= 1

            # save_dom_tree should create both raw and optionally prettified
            assert "raw" in saved
            assert Path(saved["raw"]).suffix == ".html"

    def test_analyze_xpath_alternatives_returns_result(self):
        """analyze_xpath_alternatives should return dict with alternatives for failed XPath."""
        from WebScraping.src.utils.HTMLDOMUtils import HTMLDOMUtils

        with tempfile.TemporaryDirectory() as tmpdir:
            utils = HTMLDOMUtils(data_folder=tmpdir)
            saved = utils.save_dom_tree(SAMPLE_HTML, url="https://test.com")
            html_file = saved.get("raw")

            # XPath that won't exist in our simple HTML
            result = utils.analyze_xpath_alternatives(
                "/html/body/div[99]/nonexistent",
                html_file=html_file,
            )

            assert "original_xpath" in result
            assert "exists" in result
            assert result["exists"] is False
            assert "alternatives" in result
            assert "suggestions" in result
