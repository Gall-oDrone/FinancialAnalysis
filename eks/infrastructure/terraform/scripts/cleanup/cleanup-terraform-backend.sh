#!/bin/bash
# =============================================================================
# Delete Terraform remote state backend (S3 bucket + DynamoDB lock table)
# =============================================================================
# Created by scripts/setup-backend.sh or deploy-development-eks.sh bootstrap.
#
# WARNING: Only run after all Terraform-managed resources for every workspace
# using this backend are destroyed (or you accept losing remote state).
# Deleting the bucket removes all .tfstate objects stored there.
#
# Usage: ./cleanup-terraform-backend.sh [--yes|-y]
# Environment: AWS_REGION (default us-east-1), PROJECT_NAME (default financial-analysis)
# =============================================================================

set -euo pipefail

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
PROJECT_NAME="${PROJECT_NAME:-financial-analysis}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_NAME}-terraform-state}"
DYNAMODB_TABLE="${DYNAMODB_TABLE:-${PROJECT_NAME}-terraform-locks}"

AUTO_YES=false
for arg in "$@"; do
  case "$arg" in
    --yes|-y) AUTO_YES=true ;;
  esac
done

command -v aws &>/dev/null || { print_error "AWS CLI is not installed."; exit 1; }
command -v jq &>/dev/null || { print_error "jq is required (e.g. sudo yum install -y jq)."; exit 1; }

aws sts get-caller-identity &>/dev/null || { print_error "AWS credentials are not configured."; exit 1; }

print_info "Target region: $AWS_REGION"
print_info "S3 bucket:     $BUCKET_NAME"
print_info "DynamoDB table: $DYNAMODB_TABLE"

if [ "$AUTO_YES" != true ]; then
  echo ""
  print_warning "This permanently deletes the Terraform state bucket and lock table."
  read -r -p "Type 'delete' to confirm: " confirm
  if [ "$confirm" != "delete" ]; then
    print_info "Aborted."
    exit 0
  fi
fi

delete_dynamodb_if_exists() {
  if aws dynamodb describe-table --table-name "$DYNAMODB_TABLE" --region "$AWS_REGION" &>/dev/null; then
    print_info "Deleting DynamoDB table: $DYNAMODB_TABLE"
    aws dynamodb delete-table --table-name "$DYNAMODB_TABLE" --region "$AWS_REGION"
    print_info "Waiting for table deletion..."
    aws dynamodb wait table-not-exists --table-name "$DYNAMODB_TABLE" --region "$AWS_REGION" || true
    print_success "DynamoDB table deleted."
  else
    print_warning "DynamoDB table not found (skipping): $DYNAMODB_TABLE"
  fi
}

empty_versioned_bucket() {
  local bucket=$1
  local region=$2
  if ! aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
    print_warning "S3 bucket not found (skipping): $bucket"
    return 0
  fi
  print_info "Removing all object versions and delete markers from s3://$bucket ..."
  while true; do
    local resp
    resp=$(aws s3api list-object-versions --bucket "$bucket" --region "$region" --output json)
    local objs_json
    objs_json=$(echo "$resp" | jq '[(.Versions[]? | {Key: .Key, VersionId: .VersionId}), (.DeleteMarkers[]? | {Key: .Key, VersionId: .VersionId})]')
    local count
    count=$(echo "$objs_json" | jq 'length')
    if [ "$count" -eq 0 ]; then
      break
    fi
    # delete-objects accepts at most 1000 keys
    local chunk
    chunk=$(echo "$objs_json" | jq '.[0:1000]')
    aws s3api delete-objects --bucket "$bucket" --region "$region" --delete "$(echo "$chunk" | jq '{Objects: ., Quiet: true}')"
  done
  print_info "Deleting S3 bucket: $bucket"
  aws s3api delete-bucket --bucket "$bucket" --region "$region"
  print_success "S3 bucket deleted."
}

delete_dynamodb_if_exists
empty_versioned_bucket "$BUCKET_NAME" "$AWS_REGION"

print_success "Terraform backend cleanup complete."
