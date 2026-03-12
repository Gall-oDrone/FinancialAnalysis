#!/usr/bin/env bash
# Apply financial-analysis pipeline to dev overlay (local kind/minikube).
# Usage: ./k8s/scripts/apply-dev.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"
echo "Applying k8s overlays/dev..."
kubectl apply -k k8s/overlays/dev
echo "Done. Check: kubectl get pods -n financial-analysis-dev"
