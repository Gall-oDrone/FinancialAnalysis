#!/usr/bin/env python3
"""
Save Yahoo Finance news DOM and analyze HTML elements for NewsCollector.

When Yahoo Finance changes their DOM, this script:
1. Loads the news topic page (e.g. crypto) with Selenium, OR uses an existing HTML file
2. Saves the HTML DOM to a file for inspection
3. Validates current XPaths from YahooFinanceHTMLElements
4. Finds alternative XPaths for h1, ul, and article elements

Run from project root:
    python WebScraping/run_dom_analysis.py
    python WebScraping/run_dom_analysis.py --html-file WebScraping/data/dom_tree_xxx.html

Or from WebScraping directory:
    python run_dom_analysis.py
    python run_dom_analysis.py --html-file data/dom_tree_xxx.html
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root only (adding WebScraping/src would shadow stdlib 'selectors')
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_script_dir)
sys.path.insert(0, _project_root)

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except ImportError:
    print("Error: selenium is required. Install with: pip install selenium webdriver-manager")
    sys.exit(1)

try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    ChromeDriverManager = None


# XPaths used by NewsCollector - validate these and find alternatives
NEWS_COLLECTOR_XPATHS = {
    "main_headline_h1": "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div[1]/h1",
    "news_list_ul": "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[3]/div/div/div/ul",
    "extractToText_fallback_h1": "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/div/div[1]/h1",
}

# From YahooFinanceHTMLElements
YAHOO_ELEMENTS = {
    "ARTICLE_GRIDLAYOUT": "/html/body/div[2]/main/section/section/section/article",
    "SECTION_TOPICHERO": "/html/body/div[2]/main/section/section/section/article/section[1]",
    "UL_STREAM_ITEMS_yf_1drgw5l": "/html/body/div[2]/main/section/section/section/section/section/div/div/div/div/ul",
    "UL_STREAM_ITEMS_yf_9xydx9": "/html/body/div[2]/div[3]/main/section/section/section/section/section/div/div[1]/div/div/ul",
}


ANALYSIS_OUTPUT_FOLDER = "analysis"  # Relative to data folder


def _ensure_analysis_folder(data_folder: str) -> Path:
    """Create analysis output folder if it does not exist."""
    analysis_path = Path(data_folder) / ANALYSIS_OUTPUT_FOLDER
    analysis_path.mkdir(parents=True, exist_ok=True)
    return analysis_path


def _write_analysis_report(
    analysis_folder: Path,
    url: str,
    html_files: dict,
    xpath_results: list,
    h1_xpaths: list,
    ul_xpaths: list,
    failed_alternatives: dict,
) -> str:
    """Write analysis report to Markdown and JSON. Returns path to report file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_basename = f"dom_analysis_report_{timestamp}"

    # Build report content
    ok_count = sum(1 for r in xpath_results if r["status"] == "OK")
    fail_count = len(xpath_results) - ok_count

    md_lines = [
        "# NewsCollector DOM Analysis Report",
        "",
        f"**Generated:** {datetime.now().isoformat()}",
        f"**URL:** {url}",
        "",
        "## Summary",
        "",
        f"- Total XPaths checked: {len(xpath_results)}",
        f"- OK: {ok_count}",
        f"- FAIL: {fail_count}",
        "",
        "## Saved DOM Files",
        "",
        f"- Raw HTML: `{html_files.get('raw', 'N/A')}`",
        f"- Prettified: `{html_files.get('prettified', 'N/A')}`",
        "",
        "## XPath Validation Results",
        "",
    ]

    for r in xpath_results:
        status_icon = "✓" if r["status"] == "OK" else "✗"
        md_lines.append(f"### {r['name']} [{status_icon}]")
        md_lines.append("")
        md_lines.append(f"- **Status:** {r['status']}")
        md_lines.append(f"- **XPath:** `{r['xpath']}`")
        if r["status"] == "OK":
            md_lines.append(f"- **Match count:** {r['match_count']}")
        else:
            md_lines.append(f"- **Error:** {r['error_msg']}")
        if r["alternatives"]:
            md_lines.append("- **Alternatives:**")
            for alt in r["alternatives"]:
                md_lines.append(f"  - `{alt}`")
        md_lines.append("")

    md_lines.extend([
        "## Element Candidates",
        "",
        "### H1 (main headline)",
        "",
    ])
    if h1_xpaths:
        for xp in h1_xpaths:
            md_lines.append(f"- `{xp}`")
    else:
        md_lines.append("- No h1 elements found.")
    md_lines.extend(["", "### UL (news list)", ""])
    if ul_xpaths:
        for xp in ul_xpaths:
            md_lines.append(f"- `{xp}`")
    else:
        md_lines.append("- No ul elements found.")

    md_lines.extend([
        "",
        "## Files to Update",
        "",
        "When XPaths fail, update one of:",
        "",
        "- `WebScraping/src/selectors/YahooFinanceHTMLElements.py`",
        "- `WebScraping/notebooks/NewsCollector-Staging.ipynb` (hardcoded XPaths in `selectUnorderList`, `scrap_data`, etc.)",
        "",
    ])

    # Write Markdown report
    md_path = analysis_folder / f"{report_basename}.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    # Write JSON report (machine-readable)
    json_report = {
        "generated": datetime.now().isoformat(),
        "url": url,
        "html_files": html_files,
        "summary": {"total": len(xpath_results), "ok": ok_count, "fail": fail_count},
        "xpath_results": xpath_results,
        "h1_candidates": h1_xpaths,
        "ul_candidates": ul_xpaths,
        "failed_alternatives": failed_alternatives,
    }
    json_path = analysis_folder / f"{report_basename}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_report, f, indent=2)

    return str(md_path)


def create_driver(headless: bool = True):
    """Create Chrome WebDriver."""
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    if ChromeDriverManager:
        service = Service(ChromeDriverManager().install())
    else:
        service = Service()
    return webdriver.Chrome(service=service, options=options)


def _run_analysis(utils, html_file: str, html_files: dict, url: str, analysis_folder: Path):
    """Run XPath validation and write report. Shared by fetch and --html-file modes."""

    # Step 2: Validate current XPaths and collect results
    print("-" * 70)
    print("Step 2: Validating current XPaths...")
    print("-" * 70)
    all_xpaths = {**NEWS_COLLECTOR_XPATHS, **YAHOO_ELEMENTS}
    xpath_results = []
    failed_alternatives = {}

    for name, xpath in all_xpaths.items():
        exists, error_msg, count = utils.validate_xpath(xpath, html_file=html_file)
        status = "OK" if exists else "FAIL"
        print(f"  [{status}] {name}")
        print(f"       {xpath[:80]}..." if len(xpath) > 80 else f"       {xpath}")
        if exists:
            print(f"       -> Matched {count} element(s)")
            xpath_results.append({
                "name": name,
                "xpath": xpath,
                "status": status,
                "match_count": count,
                "error_msg": None,
                "alternatives": [],
            })
        else:
            print(f"       -> {error_msg}")
            result = utils.analyze_xpath_alternatives(xpath, html_file=html_file)
            alts = result.get("alternatives", [])[:5]
            xpath_results.append({
                "name": name,
                "xpath": xpath,
                "status": status,
                "match_count": 0,
                "error_msg": error_msg,
                "alternatives": alts,
            })
            if alts:
                failed_alternatives[name] = alts
    print()

    # Step 3: Find alternative XPaths
    print("-" * 70)
    print("Step 3: Finding alternative XPaths...")
    print("-" * 70)

    # Find h1 elements (for main headline)
    h1_xpaths = utils.get_xpath_by_tag("h1", html_file=html_file, return_all=True)
    h1_list = []
    if h1_xpaths:
        print("  H1 elements (candidates for main headline):")
        h1_list = h1_xpaths[:5] if isinstance(h1_xpaths, list) else [h1_xpaths]
        for i, xp in enumerate(h1_list[:5], 1):
            print(f"    {i}. {xp}")
    else:
        print("  No h1 elements found.")

    # Find ul elements (for news list)
    ul_xpaths = utils.get_xpath_by_tag("ul", html_file=html_file, return_all=True)
    ul_list = []
    if ul_xpaths:
        print("\n  UL elements (candidates for news list):")
        ul_list = ul_xpaths[:5] if isinstance(ul_xpaths, list) else [ul_xpaths]
        for i, xp in enumerate(ul_list[:5], 1):
            print(f"    {i}. {xp}")
    else:
        print("  No ul elements found.")

    # Analyze failed XPaths for suggestions
    print("\n  Analysis for failed XPaths:")
    for name, xpath in all_xpaths.items():
        exists, _, _ = utils.validate_xpath(xpath, html_file=html_file)
        if not exists:
            result = utils.analyze_xpath_alternatives(xpath, html_file=html_file)
            tag_match = result.get("alternatives", [])
            if tag_match:
                print(f"\n  {name} - alternatives:")
                for alt in tag_match[:3]:
                    print(f"    -> {alt}")

    # Step 4: Write analysis report
    report_path = _write_analysis_report(
        analysis_folder=analysis_folder,
        url=url,
        html_files=html_files,
        xpath_results=xpath_results,
        h1_xpaths=h1_list,
        ul_xpaths=ul_list,
        failed_alternatives=failed_alternatives,
    )

    print()
    print("=" * 70)
    print("Done!")
    print(f"  Analysis report: {report_path}")
    print("  Update YahooFinanceHTMLElements.py or the notebook with working XPaths.")
    print("=" * 70)
    return report_path


def main():
    parser = argparse.ArgumentParser(description="Analyze Yahoo Finance DOM for NewsCollector XPaths")
    parser.add_argument(
        "--html-file",
        type=str,
        default=None,
        help="Path to existing HTML file (skip Selenium fetch). Example: data/dom_tree_xxx.html",
    )
    args = parser.parse_args()

    url = "https://finance.yahoo.com/topic/crypto/"
    data_folder = os.path.join(_script_dir, "data")
    analysis_folder = _ensure_analysis_folder(data_folder)

    print("=" * 70)
    print("NewsCollector DOM Analysis")
    print("=" * 70)
    print(f"Data folder: {data_folder}")
    print(f"Analysis output: {analysis_folder}")
    print()

    from WebScraping.src.utils.HTMLDOMUtils import HTMLDOMUtils
    utils = HTMLDOMUtils(data_folder=data_folder)

    if args.html_file:
        # Use existing HTML file - resolve path from cwd, script dir, or project root
        html_path = Path(args.html_file)
        if not html_path.is_absolute():
            for base in [Path.cwd(), _script_dir, _project_root]:
                candidate = base / args.html_file
                if candidate.exists():
                    html_path = candidate
                    break
            else:
                # Strip WebScraping/ prefix if running from project root
                rel = args.html_file.replace("WebScraping/", "").lstrip("/")
                html_path = Path(_script_dir) / rel
        if not html_path.exists():
            print(f"Error: File not found: {html_path}")
            sys.exit(1)
        html_file = str(html_path)
        html_files = {"raw": html_file, "prettified": ""}
        print("-" * 70)
        print(f"Step 1: Using existing HTML file: {html_file}")
        print("-" * 70)
    else:
        # Fetch via Selenium
        print("-" * 70)
        print("Step 1: Loading page and saving DOM...")
        print("-" * 70)
        driver = create_driver()
        try:
            driver.set_page_load_timeout(90)
            driver.get(url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(3)
            page_source = driver.page_source
        finally:
            driver.quit()

        saved = utils.save_dom_tree(page_source, url=url, prettify=True)
        html_file = saved.get("raw")
        html_files = {"raw": html_file, "prettified": saved.get("prettified", "")}
        print(f"  Saved raw DOM: {html_file}")

    _run_analysis(utils, html_file, html_files, url, analysis_folder)


if __name__ == "__main__":
    main()
