#!/usr/bin/env bash
# Run the Yahoo Finance news collector against Postgres (compose stack).
# Usage:
#   ./scripts/run_news_collector.sh
#   NEWS_SCRAPE_MAX_ARTICLES=5 NEWS_SCRAPE_SKIP_SCROLLING=true ./scripts/run_news_collector.sh
#   ./scripts/run_news_collector.sh python WebScraping/src/collectors/news_collector_example.py   # explicit
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if docker compose version >/dev/null 2>&1; then
  DC=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DC=(docker-compose)
else
  echo "docker compose (plugin) or docker-compose is required" >&2
  exit 1
fi

exec "${DC[@]}" run --rm scraper "$@"
