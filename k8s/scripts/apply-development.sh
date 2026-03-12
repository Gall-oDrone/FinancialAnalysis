#!/usr/bin/env bash
# Apply financial-analysis pipeline to development overlay (EKS or remote cluster).
# Ensure kubeconfig points to the target cluster.
# Usage: ./k8s/scripts/apply-development.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"
echo "Applying k8s overlays/development..."
kubectl apply -k k8s/overlays/development
echo "Done. Check: kubectl get pods -n financial-analysis-dev"
