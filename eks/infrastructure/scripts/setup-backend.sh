#!/bin/bash
# =============================================================================
# Financial Analysis - Setup Terraform Backend
# =============================================================================
# This script creates the S3 bucket and DynamoDB table for Terraform state
# =============================================================================

set -e

# Configuration
AWS_REGION=${AWS_REGION:-us-east-1}
PROJECT_NAME="financial-analysis"
BUCKET_NAME="${PROJECT_NAME}-terraform-state"
DYNAMODB_TABLE="${PROJECT_NAME}-terraform-locks"

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

# Check if bucket exists
if aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
    log_info "S3 bucket $BUCKET_NAME already exists"
else
    log_info "Creating S3 bucket: $BUCKET_NAME"
    
    # Create bucket
    if [ "$AWS_REGION" = "us-east-1" ]; then
        aws s3api create-bucket \
            --bucket "$BUCKET_NAME" \
            --region "$AWS_REGION"
    else
        aws s3api create-bucket \
            --bucket "$BUCKET_NAME" \
            --region "$AWS_REGION" \
            --create-bucket-configuration LocationConstraint="$AWS_REGION"
    fi
    
    # Enable versioning
    log_info "Enabling versioning on bucket"
    aws s3api put-bucket-versioning \
        --bucket "$BUCKET_NAME" \
        --versioning-configuration Status=Enabled
    
    # Enable encryption
    log_info "Enabling server-side encryption"
    aws s3api put-bucket-encryption \
        --bucket "$BUCKET_NAME" \
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
    
    # Block public access
    log_info "Blocking public access"
    aws s3api put-public-access-block \
        --bucket "$BUCKET_NAME" \
        --public-access-block-configuration '{
            "BlockPublicAcls": true,
            "IgnorePublicAcls": true,
            "BlockPublicPolicy": true,
            "RestrictPublicBuckets": true
        }'
    
    log_info "S3 bucket created successfully"
fi

# Check if DynamoDB table exists
if aws dynamodb describe-table --table-name "$DYNAMODB_TABLE" --region "$AWS_REGION" 2>/dev/null; then
    log_info "DynamoDB table $DYNAMODB_TABLE already exists"
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

log_info "Terraform backend setup complete!"
log_info ""
log_info "Backend configuration:"
log_info "  S3 Bucket:      $BUCKET_NAME"
log_info "  DynamoDB Table: $DYNAMODB_TABLE"
log_info "  Region:         $AWS_REGION"

