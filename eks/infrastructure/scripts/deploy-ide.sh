#!/bin/bash
# =============================================================================
# Financial Analysis - IDE Deployment Script
# =============================================================================
# This script deploys the CloudFront-based IDE with code-server and Jupyter
# Notebook for the Financial Analysis project.
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
INSTANCE_TYPE=${INSTANCE_TYPE:-t3.medium}
VOLUME_SIZE=${VOLUME_SIZE:-50}

# Repository defaults
REPO_OWNER=${REPO_OWNER:-"Gall-oDrone"}
REPO_NAME=${REPO_NAME:-"financial_analysis"}
REPO_REF=${REPO_REF:-"main"}

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CFN_DIR="${SCRIPT_DIR}/../cloudformation/ide"

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

Deploy the Financial Analysis IDE with code-server and Jupyter Notebook.

Options:
  -e, --environment    Environment (dev, staging, prod). Default: dev
  -r, --region         AWS Region. Default: us-east-1
  -t, --instance-type  EC2 instance type. Default: t3.medium
  -v, --volume-size    EBS volume size in GB. Default: 50
  --repo-owner         GitHub repository owner. Default: Gall-oDrone
  --repo-name          GitHub repository name. Default: financial_analysis
  --repo-ref           Git branch/tag reference. Default: main
  --skip-wait          Skip waiting for stack completion
  -h, --help           Show this help message

Examples:
  $0 -e dev                                    # Deploy dev IDE
  $0 -e staging -t t3.large -v 100             # Deploy staging with larger instance
  $0 -e prod --repo-ref v1.0.0                 # Deploy prod with specific tag

EOF
}

check_prerequisites() {
    log_step "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured. Please run 'aws configure' first."
        exit 1
    fi
    
    # Check jq
    if ! command -v jq &> /dev/null; then
        log_warn "jq is not installed. Some output formatting may be limited."
    fi
    
    # Verify template exists
    if [ ! -f "${CFN_DIR}/financial-analysis-ide-cfn.yaml" ]; then
        log_error "CloudFormation template not found at ${CFN_DIR}/financial-analysis-ide-cfn.yaml"
        exit 1
    fi
    
    log_info "All prerequisites met"
}

validate_template() {
    log_step "Validating CloudFormation template..."
    
    aws cloudformation validate-template \
        --template-body "file://${CFN_DIR}/financial-analysis-ide-cfn.yaml" \
        --region "${AWS_REGION}" > /dev/null
    
    if [ $? -eq 0 ]; then
        log_info "Template validation successful"
    else
        log_error "Template validation failed"
        exit 1
    fi
}

check_existing_stack() {
    log_step "Checking for existing stack..."
    
    STACK_STATUS=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "DOES_NOT_EXIST")
    
    if [ "$STACK_STATUS" != "DOES_NOT_EXIST" ]; then
        log_warn "Stack ${STACK_NAME} already exists with status: ${STACK_STATUS}"
        
        case "$STACK_STATUS" in
            CREATE_COMPLETE|UPDATE_COMPLETE|UPDATE_ROLLBACK_COMPLETE)
                read -p "Do you want to update the existing stack? (y/n) " -n 1 -r
                echo
                if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                    log_info "Deployment cancelled"
                    exit 0
                fi
                STACK_ACTION="update"
                ;;
            CREATE_IN_PROGRESS|UPDATE_IN_PROGRESS)
                log_error "Stack is currently being modified. Please wait and try again."
                exit 1
                ;;
            CREATE_FAILED|ROLLBACK_COMPLETE|DELETE_FAILED)
                log_warn "Stack is in failed state. Deleting and recreating..."
                aws cloudformation delete-stack \
                    --stack-name "${STACK_NAME}" \
                    --region "${AWS_REGION}"
                log_info "Waiting for stack deletion..."
                aws cloudformation wait stack-delete-complete \
                    --stack-name "${STACK_NAME}" \
                    --region "${AWS_REGION}"
                STACK_ACTION="create"
                ;;
            *)
                log_error "Stack is in unexpected state: ${STACK_STATUS}"
                exit 1
                ;;
        esac
    else
        STACK_ACTION="create"
    fi
}

deploy_stack() {
    log_step "Deploying IDE stack (${STACK_ACTION})..."
    
    PARAMS=(
        "ParameterKey=ProjectName,ParameterValue=${PROJECT_NAME}"
        "ParameterKey=Environment,ParameterValue=${ENVIRONMENT}"
        "ParameterKey=InstanceType,ParameterValue=${INSTANCE_TYPE}"
        "ParameterKey=InstanceVolumeSize,ParameterValue=${VOLUME_SIZE}"
        "ParameterKey=RepositoryOwner,ParameterValue=${REPO_OWNER}"
        "ParameterKey=RepositoryName,ParameterValue=${REPO_NAME}"
        "ParameterKey=RepositoryRef,ParameterValue=${REPO_REF}"
    )
    
    if [ "$STACK_ACTION" == "create" ]; then
        aws cloudformation create-stack \
            --stack-name "${STACK_NAME}" \
            --template-body "file://${CFN_DIR}/financial-analysis-ide-cfn.yaml" \
            --parameters "${PARAMS[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --region "${AWS_REGION}" \
            --tags \
                Key=Project,Value="${PROJECT_NAME}" \
                Key=Environment,Value="${ENVIRONMENT}" \
                Key=ManagedBy,Value="CloudFormation"
        
        log_info "Stack creation initiated"
    else
        aws cloudformation update-stack \
            --stack-name "${STACK_NAME}" \
            --template-body "file://${CFN_DIR}/financial-analysis-ide-cfn.yaml" \
            --parameters "${PARAMS[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --region "${AWS_REGION}"
        
        log_info "Stack update initiated"
    fi
}

wait_for_stack() {
    if [ "$SKIP_WAIT" == "true" ]; then
        log_warn "Skipping wait for stack completion"
        return
    fi
    
    log_step "Waiting for stack to complete (this may take 10-15 minutes)..."
    
    if [ "$STACK_ACTION" == "create" ]; then
        aws cloudformation wait stack-create-complete \
            --stack-name "${STACK_NAME}" \
            --region "${AWS_REGION}"
    else
        aws cloudformation wait stack-update-complete \
            --stack-name "${STACK_NAME}" \
            --region "${AWS_REGION}"
    fi
    
    if [ $? -eq 0 ]; then
        log_info "Stack ${STACK_ACTION} completed successfully"
    else
        log_error "Stack ${STACK_ACTION} failed"
        # Get stack events for debugging
        aws cloudformation describe-stack-events \
            --stack-name "${STACK_NAME}" \
            --region "${AWS_REGION}" \
            --query 'StackEvents[?ResourceStatus==`CREATE_FAILED` || ResourceStatus==`UPDATE_FAILED`].[LogicalResourceId,ResourceStatusReason]' \
            --output table
        exit 1
    fi
}

display_outputs() {
    log_step "Retrieving stack outputs..."
    
    OUTPUTS=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].Outputs')
    
    echo ""
    echo "=============================================="
    echo "  Financial Analysis IDE Deployment Complete"
    echo "=============================================="
    echo ""
    
    # Extract and display key outputs
    IDE_URL=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="IdeUrl") | .OutputValue' 2>/dev/null || echo "")
    JUPYTER_URL=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="JupyterUrl") | .OutputValue' 2>/dev/null || echo "")
    PASSWORD_SECRET=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="IdePasswordSecretConsoleUrl") | .OutputValue' 2>/dev/null || echo "")
    INSTANCE_ID=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="IdeInstanceId") | .OutputValue' 2>/dev/null || echo "")
    
    echo -e "${GREEN}IDE URL:${NC}           ${IDE_URL:-'See AWS Console'}"
    echo -e "${GREEN}Jupyter URL:${NC}       ${JUPYTER_URL:-'See AWS Console'}"
    echo -e "${GREEN}Instance ID:${NC}       ${INSTANCE_ID:-'See AWS Console'}"
    echo ""
    echo -e "${YELLOW}Password Secret:${NC}"
    echo "  ${PASSWORD_SECRET:-'See AWS Secrets Manager Console'}"
    echo ""
    echo "To retrieve the password, run:"
    echo -e "${BLUE}aws secretsmanager get-secret-value --secret-id ${PROJECT_NAME}-${ENVIRONMENT}-ide-password --query 'SecretString' --output text | jq -r '.password'${NC}"
    echo ""
    echo "=============================================="
    
    # Save outputs to file
    OUTPUT_FILE="${SCRIPT_DIR}/../.ide-outputs-${ENVIRONMENT}.json"
    echo "$OUTPUTS" > "$OUTPUT_FILE"
    log_info "Outputs saved to ${OUTPUT_FILE}"
}

# Parse arguments
SKIP_WAIT=false

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
        -t|--instance-type)
            INSTANCE_TYPE="$2"
            shift 2
            ;;
        -v|--volume-size)
            VOLUME_SIZE="$2"
            shift 2
            ;;
        --repo-owner)
            REPO_OWNER="$2"
            shift 2
            ;;
        --repo-name)
            REPO_NAME="$2"
            shift 2
            ;;
        --repo-ref)
            REPO_REF="$2"
            shift 2
            ;;
        --skip-wait)
            SKIP_WAIT=true
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

# Main execution
echo ""
echo "=============================================="
echo "  Financial Analysis IDE Deployment"
echo "=============================================="
echo ""
log_info "Environment: ${ENVIRONMENT}"
log_info "AWS Region: ${AWS_REGION}"
log_info "Instance Type: ${INSTANCE_TYPE}"
log_info "Volume Size: ${VOLUME_SIZE}GB"
log_info "Repository: ${REPO_OWNER}/${REPO_NAME}@${REPO_REF}"
echo ""

check_prerequisites
validate_template
check_existing_stack
deploy_stack
wait_for_stack
display_outputs

log_info "IDE deployment completed successfully!"
