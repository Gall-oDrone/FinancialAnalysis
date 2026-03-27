#!/bin/bash
# =============================================================================
# Financial Analysis - Deployment Script
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT=${ENVIRONMENT:-dev}
AWS_REGION=${AWS_REGION:-us-east-1}
PROJECT_NAME="financial-analysis"

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

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -e, --environment    Environment (dev, staging, prod). Default: dev"
    echo "  -r, --region         AWS Region. Default: us-east-1"
    echo "  -t, --terraform      Deploy Terraform infrastructure"
    echo "  -k, --kubernetes     Deploy Kubernetes manifests"
    echo "  -b, --build          Build and push Docker image"
    echo "  -a, --all            Deploy everything"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 -e dev -t          # Deploy Terraform for dev"
    echo "  $0 -e staging -k      # Deploy Kubernetes for staging"
    echo "  $0 -e prod -a         # Deploy everything for prod"
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed"
        exit 1
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        log_error "Terraform is not installed"
        exit 1
    fi
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed"
        exit 1
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    
    log_info "All prerequisites are met"
}

deploy_terraform() {
    log_info "Deploying Terraform infrastructure for ${ENVIRONMENT}..."
    
    cd "$(dirname "$0")/../terraform/environments/${ENVIRONMENT}"
    
    # Initialize Terraform
    log_info "Initializing Terraform..."
    terraform init -upgrade
    
    # Plan
    log_info "Planning Terraform changes..."
    terraform plan -out=tfplan
    
    # Ask for confirmation
    read -p "Do you want to apply these changes? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Applying Terraform changes..."
        terraform apply tfplan
    else
        log_warn "Terraform apply cancelled"
    fi
    
    # Update kubeconfig
    CLUSTER_NAME="${PROJECT_NAME}-${ENVIRONMENT}"
    log_info "Updating kubeconfig for ${CLUSTER_NAME}..."
    aws eks update-kubeconfig --name ${CLUSTER_NAME} --region ${AWS_REGION}
    
    cd - > /dev/null
}

build_and_push_image() {
    log_info "Building and pushing Docker image..."
    
    # Get ECR repository URL
    ECR_URL=$(aws ecr describe-repositories \
        --repository-names ${PROJECT_NAME} \
        --query 'repositories[0].repositoryUri' \
        --output text \
        --region ${AWS_REGION} 2>/dev/null || echo "")
    
    if [ -z "$ECR_URL" ]; then
        log_error "ECR repository not found. Please deploy Terraform first."
        exit 1
    fi
    
    # Login to ECR
    log_info "Logging into ECR..."
    aws ecr get-login-password --region ${AWS_REGION} | \
        docker login --username AWS --password-stdin ${ECR_URL}
    
    # Build image
    log_info "Building Docker image..."
    cd "$(dirname "$0")/../../.."
    docker build -t ${PROJECT_NAME}:${ENVIRONMENT} -f Dockerfile --target production .
    
    # Tag and push
    log_info "Pushing image to ECR..."
    docker tag ${PROJECT_NAME}:${ENVIRONMENT} ${ECR_URL}:${ENVIRONMENT}
    docker tag ${PROJECT_NAME}:${ENVIRONMENT} ${ECR_URL}:latest
    docker push ${ECR_URL}:${ENVIRONMENT}
    docker push ${ECR_URL}:latest
    
    log_info "Image pushed successfully: ${ECR_URL}:${ENVIRONMENT}"
    cd - > /dev/null
}

deploy_kubernetes() {
    log_info "Deploying Kubernetes manifests for ${ENVIRONMENT}..."
    
    # Check cluster connectivity
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    
    # Get ECR repository URL for image substitution
    ECR_URL=$(aws ecr describe-repositories \
        --repository-names ${PROJECT_NAME} \
        --query 'repositories[0].repositoryUri' \
        --output text \
        --region ${AWS_REGION})
    
    # Get application role ARN
    APP_ROLE_ARN=$(aws iam get-role \
        --role-name ${PROJECT_NAME}-${ENVIRONMENT}-application-role \
        --query 'Role.Arn' \
        --output text 2>/dev/null || echo "")
    
    cd "$(dirname "$0")/../kubernetes/overlays/${ENVIRONMENT}"
    
    # Apply with kustomize
    log_info "Applying Kubernetes manifests..."
    
    # Substitute environment variables and apply
    kubectl kustomize . | \
        sed "s|\${ECR_REPOSITORY_URL}|${ECR_URL}|g" | \
        sed "s|\${APPLICATION_ROLE_ARN}|${APP_ROLE_ARN}|g" | \
        sed "s|\${ENVIRONMENT}|${ENVIRONMENT}|g" | \
        kubectl apply -f -
    
    log_info "Kubernetes manifests applied successfully"
    
    # Wait for deployment to be ready
    log_info "Waiting for deployment to be ready..."
    kubectl rollout status deployment/financial-scraper -n ${PROJECT_NAME} --timeout=300s
    
    cd - > /dev/null
}

# Parse arguments
DEPLOY_TERRAFORM=false
DEPLOY_KUBERNETES=false
BUILD_IMAGE=false

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
        -t|--terraform)
            DEPLOY_TERRAFORM=true
            shift
            ;;
        -k|--kubernetes)
            DEPLOY_KUBERNETES=true
            shift
            ;;
        -b|--build)
            BUILD_IMAGE=true
            shift
            ;;
        -a|--all)
            DEPLOY_TERRAFORM=true
            BUILD_IMAGE=true
            DEPLOY_KUBERNETES=true
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
log_info "Starting deployment for environment: ${ENVIRONMENT}"
log_info "AWS Region: ${AWS_REGION}"

check_prerequisites

if $DEPLOY_TERRAFORM; then
    deploy_terraform
fi

if $BUILD_IMAGE; then
    build_and_push_image
fi

if $DEPLOY_KUBERNETES; then
    deploy_kubernetes
fi

if ! $DEPLOY_TERRAFORM && ! $BUILD_IMAGE && ! $DEPLOY_KUBERNETES; then
    log_warn "No deployment action specified. Use -h for help."
fi

log_info "Deployment completed successfully!"

