#!/bin/bash
# Cleanup AWS Secrets Manager secrets - Financial Analysis
# Deletes secrets created by setup-secrets.sh. Does not delete RDS-managed secrets.

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

AWS_REGION="${AWS_REGION:-us-east-1}"
SECRET_PREFIX="financial-analysis"

SECRETS=(
  "${SECRET_PREFIX}/bitso-api-key"
  "${SECRET_PREFIX}/bitso-api-secret"
  "${SECRET_PREFIX}/redis-password"
)

print_info "Cleaning up secrets in AWS Secrets Manager (region: $AWS_REGION, prefix: $SECRET_PREFIX)"

command -v aws &>/dev/null || { print_error "AWS CLI not installed."; exit 1; }
aws sts get-caller-identity &>/dev/null || { print_error "AWS credentials not configured."; exit 1; }

print_warning "The following secrets will be permanently deleted:"
for secret in "${SECRETS[@]}"; do echo " - $secret"; done
echo ""
read -p "Type 'yes' to confirm: " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
  print_info "Cancelled."
  exit 0
fi

delete_secret() {
  local secret_name=$1
  if ! aws secretsmanager describe-secret --secret-id "$secret_name" --region "$AWS_REGION" &>/dev/null; then
    print_warning "Secret '$secret_name' does not exist, skipping."
    return 0
  fi
  aws secretsmanager restore-secret --secret-id "$secret_name" --region "$AWS_REGION" 2>/dev/null || true
  if aws secretsmanager delete-secret --secret-id "$secret_name" --region "$AWS_REGION" --force-delete-without-recovery &>/dev/null; then
    print_success "Deleted: $secret_name"
    return 0
  fi
  print_error "Failed to delete: $secret_name"
  return 1
}

DELETED=0
FAILED=0
for secret in "${SECRETS[@]}"; do
  delete_secret "$secret" && DELETED=$((DELETED+1)) || FAILED=$((FAILED+1))
done

echo ""
if [ $FAILED -eq 0 ]; then
  print_success "Secret cleanup complete. Deleted: $DELETED."
else
  print_warning "Completed with errors. Deleted: $DELETED, Failed: $FAILED"
  exit 1
fi
print_info "RDS credentials are managed by Terraform and are not deleted here."
