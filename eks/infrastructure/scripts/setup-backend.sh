#!/bin/bash
# =============================================================================
# Financial Analysis - Setup Terraform Backend
# =============================================================================
# Ensures S3 bucket + DynamoDB lock table exist (reuse if present, else create).
# If the default bucket name is taken globally by another account, creates
# financial-analysis-terraform-state-<AWS_ACCOUNT_ID> instead and records it in
# backend.auto.hcl for terraform init -backend-config.
# =============================================================================

set -e

AWS_REGION=${AWS_REGION:-us-east-1}
PROJECT_NAME="financial-analysis"
BUCKET_DEFAULT="${PROJECT_NAME}-terraform-state"
DYNAMODB_TABLE="${PROJECT_NAME}-terraform-locks"

AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
if [ -z "$AWS_ACCOUNT_ID" ] || [ "$AWS_ACCOUNT_ID" = "None" ]; then
  echo "[ERROR] Could not determine AWS account (aws sts get-caller-identity failed)." >&2
  exit 1
fi
BUCKET_FALLBACK="${PROJECT_NAME}-terraform-state-${AWS_ACCOUNT_ID}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log_info() {
  echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TERRAFORM_ROOT="$INFRA_ROOT/terraform"

# Create empty bucket only; returns 0=created, 2=BucketAlreadyExists (global name), 1=other error
s3_create_bucket_only() {
  local name=$1
  local err
  err="$(mktemp)"
  if [ "$AWS_REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$name" --region "$AWS_REGION" 2>"$err"
  else
    aws s3api create-bucket \
      --bucket "$name" \
      --region "$AWS_REGION" \
      --create-bucket-configuration LocationConstraint="$AWS_REGION" 2>"$err"
  fi
  local rc=$?
  if [ "$rc" -eq 0 ]; then
    rm -f "$err"
    return 0
  fi
  if grep -q 'BucketAlreadyExists' "$err"; then
    cat "$err" >&2
    rm -f "$err"
    return 2
  fi
  cat "$err" >&2
  rm -f "$err"
  return 1
}

harden_new_bucket() {
  local name=$1
  log_info "Enabling versioning on bucket"
  aws s3api put-bucket-versioning \
    --bucket "$name" \
    --versioning-configuration Status=Enabled

  log_info "Enabling server-side encryption"
  aws s3api put-bucket-encryption \
    --bucket "$name" \
    --server-side-encryption-configuration '{
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "aws:kms"
                    },
                    "BucketKeyEnabled": true
                }
            ]
        }'

  log_info "Blocking public access"
  aws s3api put-public-access-block \
    --bucket "$name" \
    --public-access-block-configuration '{
            "BlockPublicAcls": true,
            "IgnorePublicAcls": true,
            "BlockPublicPolicy": true,
            "RestrictPublicBuckets": true
        }'
}

# Resolve S3 bucket: use existing in this account, else create default, else create account-suffixed.
BUCKET_NAME=""
if aws s3api head-bucket --bucket "$BUCKET_DEFAULT" 2>/dev/null; then
  log_info "S3 bucket $BUCKET_DEFAULT already exists — using it"
  BUCKET_NAME="$BUCKET_DEFAULT"
else
  log_info "Creating S3 bucket: $BUCKET_DEFAULT"
  set +e
  s3_create_bucket_only "$BUCKET_DEFAULT"
  rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    harden_new_bucket "$BUCKET_DEFAULT"
    log_info "S3 bucket created successfully"
    BUCKET_NAME="$BUCKET_DEFAULT"
  elif [ "$rc" -eq 2 ]; then
    log_info "Default bucket name unavailable globally — using account-scoped name: $BUCKET_FALLBACK"
    if aws s3api head-bucket --bucket "$BUCKET_FALLBACK" 2>/dev/null; then
      log_info "S3 bucket $BUCKET_FALLBACK already exists — using it"
      BUCKET_NAME="$BUCKET_FALLBACK"
    else
      log_info "Creating S3 bucket: $BUCKET_FALLBACK"
      s3_create_bucket_only "$BUCKET_FALLBACK" || {
        log_error "Failed to create fallback bucket $BUCKET_FALLBACK"
        exit 1
      }
      harden_new_bucket "$BUCKET_FALLBACK"
      log_info "S3 bucket created successfully"
      BUCKET_NAME="$BUCKET_FALLBACK"
    fi
  else
    log_error "Failed to create S3 bucket $BUCKET_DEFAULT"
    exit 1
  fi
fi

# DynamoDB lock table (names are per-account; reuse if exists)
if aws dynamodb describe-table --table-name "$DYNAMODB_TABLE" --region "$AWS_REGION" &>/dev/null; then
  log_info "DynamoDB table $DYNAMODB_TABLE already exists — using it"
else
  log_info "Creating DynamoDB table: $DYNAMODB_TABLE"

  aws dynamodb create-table \
    --table-name "$DYNAMODB_TABLE" \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region "$AWS_REGION" \
    --tags Key=Project,Value="$PROJECT_NAME" Key=ManagedBy,Value=terraform

  log_info "Waiting for table to be active..."
  aws dynamodb wait table-exists --table-name "$DYNAMODB_TABLE" --region "$AWS_REGION"

  log_info "DynamoDB table created successfully"
fi

write_backend_auto_hcl() {
  local env_dir=$1
  local out="$env_dir/backend.auto.hcl"
  mkdir -p "$env_dir"
  cat >"$out" <<EOF
# Generated by eks/infrastructure/scripts/setup-backend.sh — do not edit.
bucket         = "$BUCKET_NAME"
dynamodb_table = "$DYNAMODB_TABLE"
EOF
  log_info "Wrote $out"
}

for env in development staging production; do
  if [ -d "$TERRAFORM_ROOT/envs/$env" ]; then
    write_backend_auto_hcl "$TERRAFORM_ROOT/envs/$env"
  fi
done

log_info "Terraform backend setup complete!"
log_info ""
log_info "Backend configuration:"
log_info "  S3 Bucket:      $BUCKET_NAME"
log_info "  DynamoDB Table: $DYNAMODB_TABLE"
log_info "  Region:         $AWS_REGION"
