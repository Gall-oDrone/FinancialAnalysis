#!/usr/bin/env bash
# Download a Postgres backup from S3 and restore into the running container.
#
# Usage:
#   ./scripts/restore_postgres_from_s3.sh
#   ./scripts/restore_postgres_from_s3.sh --key financial_db_20260625_120000.sql.gz
#   ./scripts/restore_postgres_from_s3.sh --key postgres-backups/financial_db/latest.sql.gz
#
# Typical fresh-environment flow:
#   docker compose up -d postgres
#   ./scripts/restore_postgres_from_s3.sh
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
command -v gunzip >/dev/null 2>&1 || { echo "gunzip is required" >&2; exit 1; }

PGUSER="${POSTGRES_USER:-financial_user}"
PGDB="${POSTGRES_DB:-financial_db}"
export PGPASSWORD="${POSTGRES_PASSWORD:-${PGDBPASS:-}}"
BUCKET="${AWS_BACKUP_BUCKET:-${AWS_DEFAULT_BUCKET:-}}"
PREFIX="${POSTGRES_BACKUP_S3_PREFIX:-postgres-backups/financial_db}"
PREFIX="${PREFIX#/}"
PREFIX="${PREFIX%/}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$REGION"

OBJECT_KEY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --key)
      OBJECT_KEY="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$BUCKET" ]]; then
  echo "Set AWS_BACKUP_BUCKET or AWS_DEFAULT_BUCKET in .env" >&2
  exit 1
fi
if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env" >&2
  exit 1
fi

if [[ -z "$OBJECT_KEY" ]]; then
  OBJECT_KEY="${PREFIX}/latest.sql.gz"
elif [[ "$OBJECT_KEY" != */* ]]; then
  OBJECT_KEY="${PREFIX}/${OBJECT_KEY}"
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

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
ARCHIVE_PATH="${TMP_DIR}/restore.sql.gz"
S3_URI="s3://${BUCKET}/${OBJECT_KEY}"

echo "Downloading ${S3_URI} ..."
aws s3 cp "$S3_URI" "$ARCHIVE_PATH"

echo "Restoring database ${PGDB} ..."
gunzip -c "$ARCHIVE_PATH" | docker exec -i "$postgres_cid" psql \
  -U "$PGUSER" \
  -d "$PGDB" \
  -v ON_ERROR_STOP=1

echo "Restore complete."
