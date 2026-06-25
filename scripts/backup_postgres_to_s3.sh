#!/usr/bin/env bash
# Dump the compose Postgres database and upload to S3.
#
# Usage:
#   ./scripts/backup_postgres_to_s3.sh
#
# Requires in .env:
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
#   AWS_BACKUP_BUCKET (or AWS_DEFAULT_BUCKET)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# shellcheck disable=SC1091
source "$ROOT/scripts/lib/load_env.sh"
load_env_file "$ROOT/.env"

if docker compose version >/dev/null 2>&1; then
  DC=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DC=(docker-compose)
else
  echo "docker compose (plugin) or docker-compose is required" >&2
  exit 1
fi

command -v aws >/dev/null 2>&1 || { echo "aws CLI is required" >&2; exit 1; }
command -v gzip >/dev/null 2>&1 || { echo "gzip is required" >&2; exit 1; }

PGUSER="${POSTGRES_USER:-financial_user}"
PGDB="${POSTGRES_DB:-financial_db}"
export PGPASSWORD="${POSTGRES_PASSWORD:-${PGDBPASS:-}}"
BUCKET="${AWS_BACKUP_BUCKET:-${AWS_DEFAULT_BUCKET:-}}"
PREFIX="${POSTGRES_BACKUP_S3_PREFIX:-postgres-backups/financial_db}"
PREFIX="${PREFIX#/}"
PREFIX="${PREFIX%/}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$REGION"

if [[ -z "$BUCKET" ]]; then
  echo "Set AWS_BACKUP_BUCKET or AWS_DEFAULT_BUCKET in .env" >&2
  exit 1
fi
if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env" >&2
  exit 1
fi

postgres_cid=""
if docker ps --format '{{.Names}}' | grep -qx 'financial_postgres'; then
  postgres_cid="$(docker ps -q -f name=^financial_postgres$)"
elif cid="$("${DC[@]}" ps -q postgres 2>/dev/null | head -1)"; [[ -n "$cid" ]]; then
  postgres_cid="$cid"
else
  echo "Postgres not running. Start it with: docker compose up -d postgres" >&2
  exit 1
fi

TS="$(date -u +%Y%m%d_%H%M%S)"
STAMPED_NAME="${PGDB}_${TS}.sql.gz"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

STAMPED_PATH="${TMP_DIR}/${STAMPED_NAME}"
LATEST_PATH="${TMP_DIR}/latest.sql.gz"

echo "Dumping database ${PGDB} from Postgres container..."
docker exec "$postgres_cid" pg_dump \
  -U "$PGUSER" \
  -d "$PGDB" \
  --no-owner \
  --no-acl \
  --clean \
  --if-exists \
  | gzip -6 >"$STAMPED_PATH"
cp "$STAMPED_PATH" "$LATEST_PATH"

for name in "$STAMPED_NAME" "latest.sql.gz"; do
  local_path="${TMP_DIR}/${name}"
  s3_uri="s3://${BUCKET}/${PREFIX}/${name}"
  bytes="$(wc -c <"$local_path" | tr -d ' ')"
  aws s3 cp "$local_path" "$s3_uri"
  echo "Uploaded ${s3_uri} (${bytes} bytes)"
done

echo "Backup complete."
