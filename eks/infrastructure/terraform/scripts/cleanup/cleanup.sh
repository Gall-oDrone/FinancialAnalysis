#!/bin/bash
# Bulletproof EKS Cleanup - Financial Analysis
# Ensures minimal manual AWS console cleanup. Adapts microservices-trading-bot cleanup.sh.

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

ENVIRONMENT=${1:-"development"}
AWS_REGION="${AWS_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TERRAFORM_DIR="$TERRAFORM_ROOT/envs/${ENVIRONMENT}"
PROJECT_NAME="financial-analysis"

# Map env folder name to cluster suffix
get_cluster_suffix() {
  case "$ENVIRONMENT" in
    development) echo "dev" ;;
    staging)     echo "staging" ;;
    production)  echo "prod" ;;
    *)           echo "$ENVIRONMENT" ;;
  esac
}

get_cluster_name() {
  local cluster_name=""
  if [ -d "$TERRAFORM_DIR" ]; then
    cd "$TERRAFORM_DIR"
    terraform init >/dev/null 2>&1 || true
    cluster_name=$(terraform output -raw cluster_name 2>/dev/null || echo "")
    cd - >/dev/null 2>&1
  fi
  if [ -z "$cluster_name" ] || [ "$cluster_name" = "" ]; then
    cluster_name="${PROJECT_NAME}-$(get_cluster_suffix)"
    print_warning "Could not retrieve cluster name from Terraform, using fallback: $cluster_name"
  else
    print_success "Retrieved cluster name from Terraform: $cluster_name"
  fi
  echo "$cluster_name"
}

CLUSTER_NAME=$(get_cluster_name)

print_info "Starting cleanup for environment: $ENVIRONMENT (cluster: $CLUSTER_NAME)"

command_exists() { command -v "$1" >/dev/null 2>&1; }

check_prerequisites() {
  print_info "Checking prerequisites..."
  command_exists aws    || { print_error "AWS CLI not found"; exit 1; }
  command_exists terraform || { print_error "Terraform not found"; exit 1; }
  if ! command_exists kubectl; then
    print_warning "kubectl not found - some Kubernetes cleanup steps will be skipped"
  fi
  aws sts get-caller-identity >/dev/null 2>&1 || { print_error "AWS credentials not configured"; exit 1; }
  print_success "Prerequisites OK"
}

force_delete_loadbalancers() {
  print_info "Force deleting LoadBalancer services..."
  command_exists kubectl || { print_warning "kubectl not available"; return 0; }
  aws eks update-kubeconfig --region "$AWS_REGION" --name "$CLUSTER_NAME" 2>/dev/null || {
    print_warning "Cannot configure kubectl - cluster may be gone"; return 0; }
  kubectl get nodes >/dev/null 2>&1 || { print_warning "Cluster not accessible"; return 0; }

  local lb_services=""
  if command_exists jq; then
    lb_services=$(kubectl get svc --all-namespaces -o json 2>/dev/null | jq -r '.items[] | select(.spec.type=="LoadBalancer") | "\(.metadata.namespace)/\(.metadata.name)"' 2>/dev/null || echo "")
  else
    lb_services=$(kubectl get svc -A -o jsonpath='{range .items[*]}{.spec.type}{" "}{.metadata.namespace}{"/"}{.metadata.name}{"\n"}{end}' 2>/dev/null | awk '$1=="LoadBalancer"{print $2}' || echo "")
  fi

  if [ -z "$lb_services" ]; then
    print_success "No LoadBalancer services found"
    return 0
  fi
  print_warning "Found LoadBalancer services:"
  echo "$lb_services"
  echo "$lb_services" | while read -r service_info; do
    [ -z "$service_info" ] && continue
    namespace=$(echo "$service_info" | cut -d'/' -f1)
    service=$(echo "$service_info" | cut -d'/' -f2)
    print_info "Deleting LoadBalancer: $namespace/$service"
    timeout 30 kubectl delete svc "$service" -n "$namespace" 2>/dev/null || {
      kubectl patch svc "$service" -n "$namespace" --type='merge' -p='{"metadata":{"finalizers":[]}}' 2>/dev/null || true
      kubectl delete svc "$service" -n "$namespace" --grace-period=0 --force 2>/dev/null || true
    }
  done
  print_info "Waiting 60s for AWS Load Balancers to start deletion..."
  sleep 60
}

nuke_all_ingresses() {
  print_info "Nuking ingresses..."
  command_exists kubectl || return 0
  local ingresses=$(kubectl get ingress -A -o jsonpath='{range .items[*]}{.metadata.namespace}{"/"}{.metadata.name}{"\n"}{end}' 2>/dev/null || echo "")
  [ -z "$ingresses" ] && { print_success "No ingresses found"; return 0; }
  echo "$ingresses" | while read -r ingress_info; do
    [ -z "$ingress_info" ] && continue
    namespace=$(echo "$ingress_info" | cut -d'/' -f1)
    ingress=$(echo "$ingress_info" | cut -d'/' -f2)
    kubectl patch ingress "$ingress" -n "$namespace" --type='merge' -p='{"metadata":{"finalizers":[]}}' 2>/dev/null || true
    kubectl delete ingress "$ingress" -n "$namespace" --grace-period=0 --force 2>/dev/null || true
  done
}

cleanup_aws_load_balancers() {
  print_info "Cleaning up AWS Load Balancers..."
  local lbs=$(aws elbv2 describe-load-balancers --region "$AWS_REGION" --query "LoadBalancers[?contains(LoadBalancerName, 'k8s') || contains(LoadBalancerName, '$CLUSTER_NAME')].LoadBalancerArn" --output text 2>/dev/null | tr '\t' '\n' || echo "")
  for lb_arn in $lbs; do
    [ -z "$lb_arn" ] || [ "$lb_arn" = "None" ] && continue
    print_info "Deleting Load Balancer: $lb_arn"
    aws elbv2 delete-load-balancer --load-balancer-arn "$lb_arn" --region "$AWS_REGION" 2>/dev/null || true
  done
  local classic_lbs=$(aws elb describe-load-balancers --region "$AWS_REGION" --query "LoadBalancerDescriptions[?contains(LoadBalancerName, 'k8s') || contains(LoadBalancerName, '$CLUSTER_NAME')].LoadBalancerName" --output text 2>/dev/null || echo "")
  for lb_name in $classic_lbs; do
    [ -z "$lb_name" ] && continue
    aws elb delete-load-balancer --load-balancer-name "$lb_name" --region "$AWS_REGION" 2>/dev/null || true
  done
}

cleanup_ecr_repositories() {
  print_info "Cleaning up ECR repositories..."
  local repos=("$PROJECT_NAME")
  for repo in "${repos[@]}"; do
    if aws ecr describe-repositories --repository-names "$repo" --region "$AWS_REGION" >/dev/null 2>&1; then
      print_warning "Deleting repository: $repo (and all images)"
      aws ecr delete-repository --repository-name "$repo" --region "$AWS_REGION" --force 2>/dev/null && print_success "Deleted $repo" || print_warning "Failed to delete $repo"
    fi
  done
}

cleanup_target_groups() {
  print_info "Cleaning up Target Groups..."
  local tgs=$(aws elbv2 describe-target-groups --region "$AWS_REGION" --query "TargetGroups[?contains(TargetGroupName, 'k8s')].TargetGroupArn" --output text 2>/dev/null | tr '\t' '\n' || echo "")
  for tg in $tgs; do
    [ -z "$tg" ] && continue
    aws elbv2 delete-target-group --target-group-arn "$tg" --region "$AWS_REGION" 2>/dev/null || true
  done
}

cleanup_kms_keys() {
  print_info "Cleaning up KMS keys..."
  [ ! -d "$TERRAFORM_DIR" ] && return 0
  cd "$TERRAFORM_DIR"
  terraform init >/dev/null 2>&1 || true
  # EKS module uses aws_kms_key.eks and aws_kms_alias.eks (no nested module)
  local kms_key_id=$(terraform state show 'module.eks.aws_kms_key.eks' 2>/dev/null | grep -E "^id\s+=" | awk '{print $3}' | tr -d '"' || echo "")
  cd - >/dev/null
  [ -z "$kms_key_id" ] && return 0
  if ! aws kms describe-key --key-id "$kms_key_id" --region "$AWS_REGION" >/dev/null 2>&1; then
    cd "$TERRAFORM_DIR"
    terraform state rm 'module.eks.aws_kms_key.eks' 2>/dev/null || true
    terraform state rm 'module.eks.aws_kms_alias.eks' 2>/dev/null || true
    cd - >/dev/null
    return 0
  fi
  local key_state=$(aws kms describe-key --key-id "$kms_key_id" --region "$AWS_REGION" --query 'KeyMetadata.KeyState' --output text 2>/dev/null || echo "")
  [ "$key_state" = "PendingDeletion" ] && return 0
  [ "$key_state" != "Disabled" ] && aws kms disable-key --key-id "$kms_key_id" --region "$AWS_REGION" 2>/dev/null || true
  sleep 2
  aws kms schedule-key-deletion --key-id "$kms_key_id" --pending-window-in-days 7 --region "$AWS_REGION" 2>/dev/null || true
  print_success "KMS key cleanup initiated"
}

cleanup_cloudwatch_logs() {
  print_info "Cleaning up CloudWatch Log Groups..."
  local log_groups=$(aws logs describe-log-groups --region "$AWS_REGION" --query "logGroups[?contains(logGroupName, '/aws/eks/$CLUSTER_NAME') || contains(logGroupName, '$CLUSTER_NAME')].logGroupName" --output text 2>/dev/null | tr '\t' '\n' || echo "")
  for lg in $log_groups; do
    [ -z "$lg" ] || [ "$lg" = "None" ] && continue
    aws logs delete-log-group --log-group-name "$lg" --region "$AWS_REGION" 2>/dev/null || true
  done
}

clean_terraform_state() {
  print_info "Cleaning Terraform state of problematic resources..."
  [ ! -d "$TERRAFORM_DIR" ] && return 0
  cd "$TERRAFORM_DIR"
  terraform init >/dev/null 2>&1 || true
  local problematic_resources=(
    "module.application.kubernetes_service.this"
    "module.application.kubernetes_ingress_v1.this"
    "module.application.kubernetes_namespace.this"
    "module.application.kubernetes_horizontal_pod_autoscaler.this"
    "module.application.kubernetes_deployment.this"
    "data.aws_eks_cluster.this"
    "data.aws_eks_cluster_auth.this"
  )
  for resource in "${problematic_resources[@]}"; do
    if terraform state list 2>/dev/null | grep -q "^${resource}$"; then
      print_warning "Removing from state: $resource"
      terraform state rm "$resource" 2>/dev/null || true
    fi
  done
  # Wildcard patterns
  for pattern in "kubernetes_namespace.*" "kubernetes_deployment.*" "helm_release.*"; do
    terraform state list 2>/dev/null | grep -E "${pattern//\*/.*}" | while read -r r; do
      [ -n "$r" ] && terraform state rm "$r" 2>/dev/null || true
    done
  done
  cd - >/dev/null
}

handle_state_lock() {
  [ ! -d "$TERRAFORM_DIR" ] && return 0
  cd "$TERRAFORM_DIR"
  local lock_info=$(terraform plan -destroy -no-color 2>&1 | grep -A 10 "Error acquiring the state lock" || echo "")
  if [ -n "$lock_info" ]; then
    local lock_id=$(echo "$lock_info" | grep -oE 'ID:[[:space:]]+[0-9a-f-]+' | awk '{print $2}' || echo "")
    [ -n "$lock_id" ] && terraform force-unlock -force "$lock_id" 2>/dev/null || true
  fi
  cd - >/dev/null
}

terraform_destroy() {
  print_info "Running Terraform destroy..."
  [ ! -d "$TERRAFORM_DIR" ] && { print_error "Terraform directory not found: $TERRAFORM_DIR"; exit 1; }
  cd "$TERRAFORM_DIR"
  handle_state_lock
  if ! terraform plan -destroy -out=destroy-plan 2>&1; then
    print_warning "Destroy plan failed, attempting direct destroy..."
    timeout 1800 terraform destroy -auto-approve || { cd - >/dev/null; return 1; }
    cd - >/dev/null
    return 0
  fi
  timeout 1800 terraform apply destroy-plan || { rm -f destroy-plan; cd - >/dev/null; return 1; }
  rm -f destroy-plan
  cd - >/dev/null
  print_success "Terraform destroy completed"
}

verify_cleanup() {
  print_info "Verifying cleanup..."
  local remaining_lbs=$(aws elbv2 describe-load-balancers --region "$AWS_REGION" --query "LoadBalancers[?contains(LoadBalancerName, 'k8s') || contains(LoadBalancerName, '$CLUSTER_NAME')].LoadBalancerName" --output text 2>/dev/null || echo "")
  [ -n "$remaining_lbs" ] && print_warning "Remaining Load Balancers: $remaining_lbs" || print_success "No remaining Load Balancers"
  if aws eks describe-cluster --region "$AWS_REGION" --name "$CLUSTER_NAME" >/dev/null 2>&1; then
    print_warning "EKS cluster still exists (may be deleting)"
  else
    print_success "EKS cluster is gone"
  fi
}

main() {
  print_info "EKS Cleanup - Financial Analysis"
  check_prerequisites
  force_delete_loadbalancers
  nuke_all_ingresses
  cleanup_aws_load_balancers
  cleanup_target_groups
  cleanup_ecr_repositories
  cleanup_cloudwatch_logs
  cleanup_kms_keys
  print_info "Waiting 2 minutes for AWS to process deletions..."
  sleep 120
  clean_terraform_state
  if terraform_destroy; then
    print_success "Terraform destroy completed"
  else
    print_warning "Terraform destroy had issues; re-running state clean and destroy..."
    clean_terraform_state
    cleanup_aws_load_balancers
    cleanup_target_groups
    cleanup_cloudwatch_logs
    terraform_destroy || true
  fi
  verify_cleanup
  print_success "Cleanup completed!"
}

if [ $# -eq 0 ]; then
  echo "Usage: $0 <environment>"
  echo "Example: $0 development"
  exit 1
fi

main "$@"
