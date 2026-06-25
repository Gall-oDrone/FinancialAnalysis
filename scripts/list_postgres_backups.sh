#!/usr/bin/env bash
# List Postgres backups stored in S3.
#
# Usage:
#   ./scripts/list_postgres_backups.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# shellcheck disable=SC1091
source "$ROOT/scripts/lib/load_env.sh"
load_env_file "$ROOT/.env"

command -v aws >/dev/null 2>&1 || { echo "aws CLI is required" >&2; exit 1; }

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

echo "Backups in s3://${BUCKET}/${PREFIX}/"
aws s3 ls "s3://${BUCKET}/${PREFIX}/" --human-readable --summarize | grep '\.sql\.gz$' || true
