#!/bin/bash
# Setup secrets in AWS Secrets Manager - Financial Analysis
# Run after Terraform infrastructure is deployed. External Secrets Operator will sync these to Kubernetes.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

validate_secret_value() {
  local secret_value=$1 secret_name=$2 allow_empty=${3:-false}
  [ -z "$secret_value" ] && { [ "$allow_empty" = "true" ] && return 0; print_error "$secret_name cannot be empty"; return 1; }
  local length=${#secret_value}
  [ $length -gt 65536 ] && { print_error "$secret_name exceeds AWS limit (65536 chars)"; return 1; }
  return 0
}

AWS_REGION="${AWS_REGION:-us-east-1}"
SECRET_PREFIX="financial-analysis"

BITSO_KEY=""
BITSO_SECRET=""
REDIS_PASSWORD=""

print_info "Setting up secrets in AWS Secrets Manager (region: $AWS_REGION, prefix: $SECRET_PREFIX)"

command -v aws &>/dev/null || { print_error "AWS CLI not installed."; exit 1; }
aws sts get-caller-identity &>/dev/null || { print_error "AWS credentials not configured."; exit 1; }
print_success "AWS CLI configured"

if [ -z "$BITSO_KEY" ]; then
  read -sp "Enter Bitso API Key: " BITSO_KEY; echo
  [ -z "$BITSO_KEY" ] && { print_error "Bitso API Key cannot be empty"; exit 1; }
fi
if [ -z "$BITSO_SECRET" ]; then
  read -sp "Enter Bitso API Secret: " BITSO_SECRET; echo
  [ -z "$BITSO_SECRET" ] && { print_error "Bitso API Secret cannot be empty"; exit 1; }
fi
if [ -z "$REDIS_PASSWORD" ]; then
  read -sp "Enter Redis Password (optional, Enter to skip): " REDIS_PASSWORD; echo
fi

create_or_update_secret() {
  local secret_name=$1 secret_value=$2 description=$3
  validate_secret_value "$secret_value" "$secret_name" || return 1
  if aws secretsmanager describe-secret --secret-id "$secret_name" --region "$AWS_REGION" &>/dev/null; then
    print_info "Updating secret: $secret_name"
    aws secretsmanager update-secret --secret-id "$secret_name" --secret-string "$secret_value" --region "$AWS_REGION" --description "$description" &>/dev/null || { print_error "Failed to update $secret_name"; return 1; }
  else
    print_info "Creating secret: $secret_name"
    aws secretsmanager create-secret --name "$secret_name" --secret-string "$secret_value" --region "$AWS_REGION" --description "$description" &>/dev/null || { print_error "Failed to create $secret_name"; return 1; }
  fi
  print_success "OK: $secret_name"
  return 0
}

create_or_update_secret "${SECRET_PREFIX}/bitso-api-key"   "$BITSO_KEY"   "Bitso API Key for Financial Analysis" || exit 1
create_or_update_secret "${SECRET_PREFIX}/bitso-api-secret" "$BITSO_SECRET" "Bitso API Secret for Financial Analysis" || exit 1

if [ -n "$REDIS_PASSWORD" ]; then
  create_or_update_secret "${SECRET_PREFIX}/redis-password" "$REDIS_PASSWORD" "Redis password for Financial Analysis" || exit 1
else
  print_info "Skipping Redis password (not provided)"
fi

print_info "Verifying secrets..."
for secret in "${SECRET_PREFIX}/bitso-api-key" "${SECRET_PREFIX}/bitso-api-secret"; do
  aws secretsmanager describe-secret --secret-id "$secret" --region "$AWS_REGION" &>/dev/null && print_success "Verified: $secret" || { print_error "Verification failed: $secret"; exit 1; }
done
[ -n "$REDIS_PASSWORD" ] && aws secretsmanager describe-secret --secret-id "${SECRET_PREFIX}/redis-password" --region "$AWS_REGION" &>/dev/null && print_success "Verified: ${SECRET_PREFIX}/redis-password" || true

echo ""
print_success "Secret setup complete."
print_info "Next: deploy Kubernetes manifests; External Secrets Operator will sync from AWS Secrets Manager paths above."
print_info "RDS master credentials: created by Terraform when enable_rds is true (see terraform output rds_secret_arn)."
print_info "GitHub Actions (ECR/EKS OIDC role) is NOT stored here — copy from Terraform after apply:"
print_info "  cd eks/infrastructure/terraform/envs/development && terraform output -raw github_actions_role_arn"
print_info "Paste that value into GitHub: Settings → Secrets → AWS_GITHUB_ACTIONS_ROLE_ARN"
print_info "If the k8s branch adds new ExternalSecret keys, add matching create_or_update_secret calls here."
