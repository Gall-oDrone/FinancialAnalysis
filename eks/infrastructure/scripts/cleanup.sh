#!/bin/bash
# =============================================================================
# Financial Analysis - Cleanup Script
# =============================================================================

set -e

# Default values
ENVIRONMENT=${ENVIRONMENT:-dev}
AWS_REGION=${AWS_REGION:-us-east-1}
PROJECT_NAME="financial-analysis"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -e, --environment    Environment (dev, staging, prod). Default: dev"
    echo "  -r, --region         AWS Region. Default: us-east-1"
    echo "  -k, --kubernetes     Cleanup Kubernetes resources only"
    echo "  -t, --terraform      Destroy Terraform infrastructure"
    echo "  -a, --all            Cleanup everything"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "After a successful Terraform destroy, related CloudWatch log groups are removed:"
    echo "  - /aws/vpc/<project>-<env>/flow-logs (VPC flow logs)"
    echo "  - /aws/eks/<project>-<env>/... (EKS control plane logs)"
    echo "  - /aws/rds/instance/<project>-<env>-db/... (RDS exports, if RDS was enabled)"
}

# Terraform tfvars use: dev, staging, prod as the environment suffix in resource names.
get_cluster_name() {
    case "${ENVIRONMENT}" in
        dev)     echo "${PROJECT_NAME}-dev" ;;
        staging) echo "${PROJECT_NAME}-staging" ;;
        prod)    echo "${PROJECT_NAME}-prod" ;;
        *)       log_error "Invalid environment for cluster name: ${ENVIRONMENT}"; return 1 ;;
    esac
}

# Deletes CloudWatch log groups created by this stack (VPC flow logs, EKS cluster logging).
# Safe to call after terraform destroy; orphaned groups from partial/failed deploys are removed.
cleanup_cloudwatch_log_groups() {
    local cluster_name vpc_flow_log_group prefix groups
    cluster_name=$(get_cluster_name) || return 1
    vpc_flow_log_group="/aws/vpc/${cluster_name}/flow-logs"
    prefix="/aws/eks/${cluster_name}/"

    log_info "Deleting CloudWatch log groups for ${cluster_name} (region ${AWS_REGION})..."

    groups=$(aws logs describe-log-groups --region "${AWS_REGION}" \
        --log-group-name-prefix "${prefix}" \
        --query 'logGroups[*].logGroupName' --output text 2>/dev/null || true)

    if [ -n "${groups}" ] && [ "${groups}" != "None" ]; then
        echo "${groups}" | tr '\t' '\n' | while read -r lg; do
            [ -z "${lg}" ] && continue
            log_info "Deleting EKS log group: ${lg}"
            if aws logs delete-log-group --log-group-name "${lg}" --region "${AWS_REGION}" 2>/dev/null; then
                :
            else
                log_warn "Could not delete ${lg} (may require emptying streams or extra permissions)"
            fi
        done
    else
        log_info "No EKS log groups under prefix ${prefix}"
    fi

    local rds_prefix="/aws/rds/instance/${cluster_name}-db/"
    groups=$(aws logs describe-log-groups --region "${AWS_REGION}" \
        --log-group-name-prefix "${rds_prefix}" \
        --query 'logGroups[*].logGroupName' --output text 2>/dev/null || true)

    if [ -n "${groups}" ] && [ "${groups}" != "None" ]; then
        echo "${groups}" | tr '\t' '\n' | while read -r lg; do
            [ -z "${lg}" ] && continue
            log_info "Deleting RDS log group: ${lg}"
            aws logs delete-log-group --log-group-name "${lg}" --region "${AWS_REGION}" 2>/dev/null || \
                log_warn "Could not delete ${lg}"
        done
    else
        log_info "No RDS log groups under prefix ${rds_prefix}"
    fi

    if aws logs delete-log-group --log-group-name "${vpc_flow_log_group}" --region "${AWS_REGION}" 2>/dev/null; then
        log_info "Deleted VPC flow log group: ${vpc_flow_log_group}"
    else
        log_info "VPC flow log group not present or already removed: ${vpc_flow_log_group}"
    fi

    log_info "CloudWatch log group cleanup finished"
}

cleanup_kubernetes() {
    log_info "Cleaning up Kubernetes resources..."
    
    # Check cluster connectivity
    if ! kubectl cluster-info &> /dev/null; then
        log_warn "Cannot connect to Kubernetes cluster, skipping K8s cleanup"
        return
    fi
    
    # Delete namespace (this will delete all resources in it)
    if kubectl get namespace ${PROJECT_NAME} &> /dev/null; then
        log_info "Deleting namespace ${PROJECT_NAME}..."
        kubectl delete namespace ${PROJECT_NAME} --timeout=300s || true
    else
        log_info "Namespace ${PROJECT_NAME} does not exist"
    fi
    
    log_info "Kubernetes cleanup completed"
}

cleanup_ecr_images() {
    log_info "Cleaning up ECR images..."
    
    # Get all image IDs
    IMAGES=$(aws ecr list-images \
        --repository-name ${PROJECT_NAME} \
        --region ${AWS_REGION} \
        --query 'imageIds[*]' \
        --output json 2>/dev/null || echo "[]")
    
    if [ "$IMAGES" != "[]" ]; then
        log_info "Deleting ECR images..."
        aws ecr batch-delete-image \
            --repository-name ${PROJECT_NAME} \
            --region ${AWS_REGION} \
            --image-ids "$IMAGES" || true
    fi
    
    log_info "ECR cleanup completed"
}

destroy_terraform() {
    log_warn "This will destroy all Terraform-managed infrastructure for ${ENVIRONMENT}!"
    log_warn "This action is IRREVERSIBLE."
    
    read -p "Are you ABSOLUTELY sure? Type 'destroy' to confirm: " confirm
    if [ "$confirm" != "destroy" ]; then
        log_info "Destruction cancelled"
        return
    fi
    
    log_info "Destroying Terraform infrastructure for ${ENVIRONMENT}..."

    # Stack roots live under terraform/envs/ (not terraform/environments/).
    case "${ENVIRONMENT}" in
        dev)  TF_ENV_DIR="development" ;;
        staging) TF_ENV_DIR="staging" ;;
        prod) TF_ENV_DIR="production" ;;
        *)    log_error "Unhandled environment for Terraform path: ${ENVIRONMENT}"; return 1 ;;
    esac

    cd "$(dirname "$0")/../terraform/envs/${TF_ENV_DIR}"
    
    # Initialize Terraform
    terraform init
    
    # Destroy
    terraform destroy -auto-approve

    cd - > /dev/null

    log_info "Terraform infrastructure destroyed"

    cleanup_cloudwatch_log_groups
}

# Parse arguments
CLEANUP_KUBERNETES=false
DESTROY_TERRAFORM=false

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
        -k|--kubernetes)
            CLEANUP_KUBERNETES=true
            shift
            ;;
        -t|--terraform)
            DESTROY_TERRAFORM=true
            shift
            ;;
        -a|--all)
            CLEANUP_KUBERNETES=true
            DESTROY_TERRAFORM=true
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

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    log_error "Invalid environment: $ENVIRONMENT"
    exit 1
fi

# Main execution
log_info "Starting cleanup for environment: ${ENVIRONMENT}"

if $CLEANUP_KUBERNETES; then
    cleanup_kubernetes
    cleanup_ecr_images
fi

if $DESTROY_TERRAFORM; then
    destroy_terraform
fi

if ! $CLEANUP_KUBERNETES && ! $DESTROY_TERRAFORM; then
    log_warn "No cleanup action specified. Use -h for help."
fi

log_info "Cleanup completed!"

