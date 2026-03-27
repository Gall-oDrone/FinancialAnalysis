#!/bin/bash
# Complete deployment script for development EKS environment (Financial Analysis)
# Based on microservices-trading-bot deploy-development-eks.sh
# Usage: ./deploy-development-eks.sh [environment]
# Environment: development (default), staging, production

set -e

ENVIRONMENT="${1:-development}"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_DIR="$TERRAFORM_ROOT/envs/$ENVIRONMENT"
PROJECT_NAME="financial-analysis"
CLUSTER_FALLBACK="${PROJECT_NAME}-dev"

# Map env folder name to cluster suffix (dev, staging, prod)
get_cluster_suffix() {
  case "$ENVIRONMENT" in
    development) echo "dev" ;;
    staging)     echo "staging" ;;
    production)  echo "prod" ;;
    *)           echo "$ENVIRONMENT" ;;
  esac
}

print_info "Terraform root: $TERRAFORM_ROOT"
print_info "Environment: $ENVIRONMENT -> $ENV_DIR"

# Prerequisites
print_info "Checking prerequisites..."
command -v terraform &>/dev/null || { print_error "Terraform is not installed."; exit 1; }
command -v aws &>/dev/null || { print_error "AWS CLI is not installed."; exit 1; }
print_success "Terraform and AWS CLI installed"
command -v kubectl &>/dev/null || print_warning "kubectl not installed - some steps will be skipped"

if [ ! -d "$ENV_DIR" ]; then
  print_error "Environment directory not found: $ENV_DIR"
  exit 1
fi

# Remote state backend (S3 + DynamoDB) must exist before terraform init.
# Reuses the same logic as eks/infrastructure/scripts/setup-backend.sh
INFRA_ROOT="$(cd "$TERRAFORM_ROOT/.." && pwd)"
SETUP_BACKEND_SCRIPT="$INFRA_ROOT/scripts/setup-backend.sh"
if [ -f "$SETUP_BACKEND_SCRIPT" ]; then
  print_info "Ensuring Terraform remote state backend (S3 bucket + DynamoDB lock table)..."
  AWS_REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null)}"
  AWS_REGION="${AWS_REGION:-us-east-1}"
  export AWS_REGION
  bash "$SETUP_BACKEND_SCRIPT"
else
  print_error "Missing backend bootstrap script: $SETUP_BACKEND_SCRIPT"
  exit 1
fi

cd "$ENV_DIR"
print_info "Working directory: $(pwd)"

wait_for_cluster() {
  local cluster_name=$1
  local region=$2
  local max_wait=${3:-600}
  local elapsed=0
  local interval=30
  print_info "Waiting for EKS cluster '$cluster_name' to be ACTIVE (max ${max_wait}s)..."
  while [ $elapsed -lt $max_wait ]; do
    local status
    status=$(aws eks describe-cluster --name "$cluster_name" --region "$region" --query 'cluster.status' --output text 2>/dev/null || echo "NOT_FOUND")
    if [ "$status" = "ACTIVE" ]; then
      print_success "EKS cluster is active!"
      return 0
    fi
    [ "$status" = "NOT_FOUND" ] && print_warning "Cluster not found yet..."
    sleep $interval
    elapsed=$((elapsed + interval))
  done
  print_warning "Timeout waiting for cluster"
  return 1
}

wait_for_nodes() {
  local max_wait=${1:-300}
  local elapsed=0
  local interval=15
  print_info "Waiting for nodes to be Ready..."
  while [ $elapsed -lt $max_wait ]; do
    local ready total
    ready=$(kubectl get nodes --no-headers 2>/dev/null | grep -c " Ready " || echo "0")
    total=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [ -n "$total" ] && [ "${total:-0}" -gt 0 ] && [ "$ready" = "$total" ]; then
      print_success "All $ready node(s) are ready!"
      return 0
    fi
    [ "${total:-0}" -gt 0 ] && print_info "Nodes ready: $ready/$total"
    sleep $interval
    elapsed=$((elapsed + interval))
  done
  print_warning "Timeout waiting for nodes"
  return 1
}

# Stage 1: Init
print_info "Stage 1: Initializing Terraform..."
BACKEND_CONFIG="$ENV_DIR/backend.hcl"
if [ -f "$BACKEND_CONFIG" ]; then
  terraform init -upgrade -backend-config="$BACKEND_CONFIG"
else
  terraform init -upgrade
fi

# Stage 2: Deploy VPC + EKS (no module.ecr - ECR is inside module.eks)
print_info "Stage 2: Deploying VPC and EKS..."
timeout 1800 terraform apply -target=module.vpc -target=module.eks -auto-approve || {
  print_error "Terraform apply (vpc+eks) failed or timed out"
  exit 1
}

CLUSTER_NAME=$(terraform output -raw cluster_name 2>/dev/null || echo "")
AWS_REGION=$(terraform output -raw aws_region 2>/dev/null || echo "")
AWS_REGION=${AWS_REGION:-${AWS_REGION:-$AWS_DEFAULT_REGION}}
AWS_REGION=${AWS_REGION:-$(aws configure get region 2>/dev/null)}
AWS_REGION=${AWS_REGION:-us-east-1}

if [ -z "$CLUSTER_NAME" ]; then
  CLUSTER_NAME="${PROJECT_NAME}-$(get_cluster_suffix)"
  print_warning "Could not read cluster_name from outputs, using: $CLUSTER_NAME"
fi
print_success "Cluster: $CLUSTER_NAME, Region: $AWS_REGION"

wait_for_cluster "$CLUSTER_NAME" "$AWS_REGION"
print_info "Updating kubeconfig..."
aws eks update-kubeconfig --region "$AWS_REGION" --name "$CLUSTER_NAME"

if command -v kubectl &>/dev/null; then
  if kubectl get nodes &>/dev/null; then
    wait_for_nodes
  else
    print_warning "Could not access cluster yet, continuing..."
  fi
fi

# Stage 3: Deploy RDS + Security + rest
print_info "Stage 3: Deploying RDS, Security and remaining resources..."
timeout 1800 terraform apply -auto-approve || {
  print_error "Terraform apply (full) failed or timed out"
  exit 1
}

print_info "Waiting 30s for resources to stabilize..."
sleep 30

# Stage 4: Outputs
print_info "Stage 4: Outputs"
terraform output || true

print_success "Deployment complete for $ENVIRONMENT."
echo ""
print_info "Useful commands:"
echo "  aws eks update-kubeconfig --region $AWS_REGION --name $CLUSTER_NAME"
echo "  kubectl get nodes"
echo "  terraform output"
