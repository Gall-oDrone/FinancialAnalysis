#!/usr/bin/env bash
# Trigger the development EKS deploy workflow (requires: gh CLI, auth).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORKFLOW_FILE="deploy-development.yml"
REF="${1:-$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")}"

cd "$REPO_ROOT"
if [ -n "$REF" ]; then
  gh workflow run "$WORKFLOW_FILE" --ref "$REF"
else
  gh workflow run "$WORKFLOW_FILE"
fi
sleep 3
RUN_ID="$(gh run list --workflow="$WORKFLOW_FILE" --limit 1 --json databaseId --jq '.[0].databaseId')"
echo "Run ID: $RUN_ID"
gh run watch "$RUN_ID"
