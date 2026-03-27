#!/usr/bin/env bash
# Query scraped rows from financial_news_241118 (requires postgres service up).
# Usage:
#   ./scripts/query_financial_news.sh
#   ./scripts/query_financial_news.sh 100    # limit rows
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LIMIT="${1:-50}"

if docker compose version >/dev/null 2>&1; then
  DC=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DC=(docker-compose)
else
  echo "docker compose (plugin) or docker-compose is required" >&2
  exit 1
fi

# shellcheck disable=SC1091
[[ -f "$ROOT/.env" ]] && set -a && source "$ROOT/.env" && set +a || true

PGUSER="${POSTGRES_USER:-financial_user}"
PGDB="${POSTGRES_DB:-financial_db}"

SQL="SELECT id,
  LEFT(headline, 100) AS headline,
  LEFT(href, 90) AS href,
  author,
  datetime,
  created_at
FROM financial_news_241118
ORDER BY created_at DESC NULLS LAST, id DESC
LIMIT ${LIMIT};"

if [[ -n $("${DC[@]}" ps -q postgres 2>/dev/null) ]]; then
  exec "${DC[@]}" exec -T postgres psql -U "$PGUSER" -d "$PGDB" -c "$SQL"
fi
if docker ps --format '{{.Names}}' | grep -qx 'financial_postgres'; then
  exec docker exec financial_postgres psql -U "$PGUSER" -d "$PGDB" -c "$SQL"
fi
echo "Postgres not found. Start it with: docker compose up -d postgres" >&2
exit 1
