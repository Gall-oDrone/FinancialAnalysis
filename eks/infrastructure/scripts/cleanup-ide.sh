#!/bin/bash
# =============================================================================
# Financial Analysis - IDE Cleanup Script
# =============================================================================
# This script safely removes the CloudFront-based IDE infrastructure including
# EC2 instance, CloudFront distribution, VPC, and all associated resources.
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT=${ENVIRONMENT:-dev}
AWS_REGION=${AWS_REGION:-us-east-1}
PROJECT_NAME="financial-analysis"
STACK_NAME="${PROJECT_NAME}-${ENVIRONMENT}-ide"

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Cleanup (delete) the Financial Analysis IDE infrastructure.

Options:
  -e, --environment    Environment (dev, staging, prod). Default: dev
  -r, --region         AWS Region. Default: us-east-1
  -f, --force          Skip confirmation prompts
  --delete-secrets     Also delete associated secrets from Secrets Manager
  --keep-logs          Keep CloudWatch log groups
  -h, --help           Show this help message

Examples:
  $0 -e dev                    # Cleanup dev IDE with confirmation
  $0 -e staging -f             # Force cleanup staging IDE
  $0 -e dev --delete-secrets   # Cleanup and remove secrets

WARNING: This action is IRREVERSIBLE. All data will be lost.

EOF
}

check_prerequisites() {
    log_step "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured"
        exit 1
    fi
    
    log_info "Prerequisites check passed"
}

check_stack_exists() {
    log_step "Checking if stack exists..."
    
    STACK_STATUS=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "DOES_NOT_EXIST")
    
    if [ "$STACK_STATUS" == "DOES_NOT_EXIST" ]; then
        log_warn "Stack ${STACK_NAME} does not exist"
        return 1
    fi
    
    log_info "Found stack ${STACK_NAME} with status: ${STACK_STATUS}"
    return 0
}

get_stack_resources() {
    log_step "Retrieving stack resources..."
    
    # Get instance ID
    INSTANCE_ID=$(aws cloudformation describe-stack-resource \
        --stack-name "${STACK_NAME}" \
        --logical-resource-id "IdeInstance" \
        --region "${AWS_REGION}" \
        --query 'StackResourceDetail.PhysicalResourceId' \
        --output text 2>/dev/null || echo "")
    
    # Get CloudFront distribution ID
    CLOUDFRONT_ID=$(aws cloudformation describe-stack-resource \
        --stack-name "${STACK_NAME}" \
        --logical-resource-id "IdeCloudFrontDistribution" \
        --region "${AWS_REGION}" \
        --query 'StackResourceDetail.PhysicalResourceId' \
        --output text 2>/dev/null || echo "")
    
    # Get secret ARN
    SECRET_ARN=$(aws cloudformation describe-stack-resource \
        --stack-name "${STACK_NAME}" \
        --logical-resource-id "IdePassword" \
        --region "${AWS_REGION}" \
        --query 'StackResourceDetail.PhysicalResourceId' \
        --output text 2>/dev/null || echo "")
    
    echo ""
    log_info "Resources to be deleted:"
    echo "  - EC2 Instance: ${INSTANCE_ID:-'Not found'}"
    echo "  - CloudFront Distribution: ${CLOUDFRONT_ID:-'Not found'}"
    echo "  - Secret: ${SECRET_ARN:-'Not found'}"
    echo ""
}

confirm_deletion() {
    if [ "$FORCE" == "true" ]; then
        return 0
    fi
    
    echo ""
    log_warn "=============================================="
    log_warn "  WARNING: DESTRUCTIVE OPERATION"
    log_warn "=============================================="
    echo ""
    echo "You are about to delete the following stack:"
    echo "  Stack Name: ${STACK_NAME}"
    echo "  Environment: ${ENVIRONMENT}"
    echo "  Region: ${AWS_REGION}"
    echo ""
    log_warn "This action is IRREVERSIBLE. All data will be lost."
    echo ""
    
    read -p "Type 'delete' to confirm: " confirm
    if [ "$confirm" != "delete" ]; then
        log_info "Deletion cancelled"
        exit 0
    fi
}

disable_cloudfront_distribution() {
    if [ -z "$CLOUDFRONT_ID" ]; then
        return
    fi
    
    log_step "Disabling CloudFront distribution (may take a few minutes)..."
    
    # Check if distribution is enabled
    DIST_STATUS=$(aws cloudfront get-distribution \
        --id "${CLOUDFRONT_ID}" \
        --query 'Distribution.DistributionConfig.Enabled' \
        --output text 2>/dev/null || echo "false")
    
    if [ "$DIST_STATUS" == "true" ]; then
        # Get current config
        aws cloudfront get-distribution-config \
            --id "${CLOUDFRONT_ID}" \
            --output json > /tmp/cf-config.json
        
        ETAG=$(cat /tmp/cf-config.json | jq -r '.ETag')
        
        # Disable distribution
        cat /tmp/cf-config.json | jq '.DistributionConfig.Enabled = false' | jq '.DistributionConfig' > /tmp/cf-update.json
        
        aws cloudfront update-distribution \
            --id "${CLOUDFRONT_ID}" \
            --distribution-config "file:///tmp/cf-update.json" \
            --if-match "${ETAG}" > /dev/null 2>&1 || true
        
        log_info "CloudFront distribution disabled. Waiting for deployment..."
        
        # Wait for distribution to be deployed (disabled state)
        aws cloudfront wait distribution-deployed \
            --id "${CLOUDFRONT_ID}" 2>/dev/null || true
        
        rm -f /tmp/cf-config.json /tmp/cf-update.json
    fi
}

delete_stack() {
    log_step "Deleting CloudFormation stack..."
    
    aws cloudformation delete-stack \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}"
    
    log_info "Stack deletion initiated"
}

wait_for_deletion() {
    log_step "Waiting for stack deletion to complete..."
    
    # Monitor deletion progress
    while true; do
        STACK_STATUS=$(aws cloudformation describe-stacks \
            --stack-name "${STACK_NAME}" \
            --region "${AWS_REGION}" \
            --query 'Stacks[0].StackStatus' \
            --output text 2>/dev/null || echo "DELETE_COMPLETE")
        
        case "$STACK_STATUS" in
            DELETE_COMPLETE)
                log_info "Stack deleted successfully"
                break
                ;;
            DELETE_IN_PROGRESS)
                echo -n "."
                sleep 10
                ;;
            DELETE_FAILED)
                log_error "Stack deletion failed"
                # Get failure reason
                aws cloudformation describe-stack-events \
                    --stack-name "${STACK_NAME}" \
                    --region "${AWS_REGION}" \
                    --query 'StackEvents[?ResourceStatus==`DELETE_FAILED`].[LogicalResourceId,ResourceStatusReason]' \
                    --output table
                exit 1
                ;;
            *)
                log_error "Unexpected stack status: ${STACK_STATUS}"
                exit 1
                ;;
        esac
    done
    echo ""
}

delete_secrets() {
    if [ "$DELETE_SECRETS" != "true" ]; then
        log_info "Skipping secrets deletion (use --delete-secrets to include)"
        return
    fi
    
    log_step "Deleting secrets from Secrets Manager..."
    
    SECRET_NAME="${PROJECT_NAME}-${ENVIRONMENT}-ide-password"
    
    if aws secretsmanager describe-secret --secret-id "${SECRET_NAME}" --region "${AWS_REGION}" &>/dev/null; then
        aws secretsmanager delete-secret \
            --secret-id "${SECRET_NAME}" \
            --force-delete-without-recovery \
            --region "${AWS_REGION}"
        log_info "Secret ${SECRET_NAME} deleted"
    else
        log_info "Secret ${SECRET_NAME} not found"
    fi
}

cleanup_logs() {
    if [ "$KEEP_LOGS" == "true" ]; then
        log_info "Keeping CloudWatch log groups as requested"
        return
    fi
    
    log_step "Cleaning up CloudWatch log groups..."
    
    # Delete IDE-related log groups
    LOG_GROUPS=$(aws logs describe-log-groups \
        --log-group-name-prefix "/aws/lambda/${PROJECT_NAME}-${ENVIRONMENT}-ide" \
        --region "${AWS_REGION}" \
        --query 'logGroups[].logGroupName' \
        --output text 2>/dev/null || echo "")
    
    for LOG_GROUP in $LOG_GROUPS; do
        log_info "Deleting log group: ${LOG_GROUP}"
        aws logs delete-log-group \
            --log-group-name "${LOG_GROUP}" \
            --region "${AWS_REGION}" 2>/dev/null || true
    done
}

cleanup_output_files() {
    log_step "Cleaning up local output files..."
    
    OUTPUT_FILE="${SCRIPT_DIR}/../.ide-outputs-${ENVIRONMENT}.json"
    if [ -f "$OUTPUT_FILE" ]; then
        rm -f "$OUTPUT_FILE"
        log_info "Removed ${OUTPUT_FILE}"
    fi
}

# Parse arguments
FORCE=false
DELETE_SECRETS=false
KEEP_LOGS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -r|--region)
            AWS_REGION="$2"
            shift 2
            ;;
        -f|--force)
            FORCE=true
            shift
            ;;
        --delete-secrets)
            DELETE_SECRETS=true
            shift
            ;;
        --keep-logs)
            KEEP_LOGS=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Update stack name with environment
STACK_NAME="${PROJECT_NAME}-${ENVIRONMENT}-ide"

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    log_error "Invalid environment: $ENVIRONMENT. Must be one of: dev, staging, prod"
    exit 1
fi

# Production safeguard
if [ "$ENVIRONMENT" == "prod" ] && [ "$FORCE" != "true" ]; then
    log_warn "=============================================="
    log_warn "  PRODUCTION ENVIRONMENT DETECTED"
    log_warn "=============================================="
    echo ""
    echo "You are attempting to delete PRODUCTION resources."
    echo "This requires additional confirmation."
    echo ""
    read -p "Type the environment name 'prod' to continue: " prod_confirm
    if [ "$prod_confirm" != "prod" ]; then
        log_info "Deletion cancelled"
        exit 0
    fi
fi

# Main execution
echo ""
echo "=============================================="
echo "  Financial Analysis IDE Cleanup"
echo "=============================================="
echo ""
log_info "Environment: ${ENVIRONMENT}"
log_info "AWS Region: ${AWS_REGION}"
log_info "Stack Name: ${STACK_NAME}"
echo ""

check_prerequisites

if ! check_stack_exists; then
    log_info "Nothing to clean up"
    cleanup_output_files
    exit 0
fi

get_stack_resources
confirm_deletion

# Disable CloudFront before deletion (speeds up the process)
disable_cloudfront_distribution

delete_stack
wait_for_deletion
delete_secrets
cleanup_logs
cleanup_output_files

echo ""
echo "=============================================="
echo "  Cleanup Complete"
echo "=============================================="
echo ""
log_info "All IDE resources for ${ENVIRONMENT} have been deleted"
echo ""
