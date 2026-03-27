#!/bin/bash
# Cleanup Kubernetes application resources - Financial Analysis
# Deletes application resources in the financial-analysis namespace.
# Run before full infrastructure cleanup to avoid stuck resources.

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
APP_NAMESPACE="financial-analysis"
PROJECT_NAME="financial-analysis"

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
  if [ -z "$cluster_name" ]; then
    cluster_name="${PROJECT_NAME}-$(get_cluster_suffix)"
    print_warning "Using fallback cluster name: $cluster_name"
  else
    print_success "Cluster: $cluster_name"
  fi
  echo "$cluster_name"
}

check_prerequisites() {
  print_info "Checking prerequisites..."
  command -v kubectl &>/dev/null || { print_error "kubectl is not installed."; exit 1; }
  command -v aws &>/dev/null || { print_error "AWS CLI is not installed."; exit 1; }
  aws sts get-caller-identity &>/dev/null || { print_error "AWS credentials not configured."; exit 1; }
  print_success "Prerequisites OK"
}

configure_kubectl() {
  local cluster_name=$1
  print_info "Updating kubeconfig for cluster: $cluster_name"
  aws eks update-kubeconfig --region "$AWS_REGION" --name "$cluster_name" 2>/dev/null || { print_error "Failed to update kubeconfig."; exit 1; }
  kubectl get nodes >/dev/null 2>&1 || { print_error "Cannot connect to cluster."; exit 1; }
  print_success "Connected to cluster"
}

namespace_exists() { kubectl get namespace "$1" &>/dev/null; }

remove_finalizers() {
  local resource_type=$1 resource_name=$2 namespace=$3
  kubectl patch "$resource_type" "$resource_name" -n "$namespace" --type='merge' -p='{"metadata":{"finalizers":[]}}' 2>/dev/null || true
}

cleanup_external_secrets() {
  local namespace=$1
  print_info "Cleaning up ExternalSecrets in namespace: $namespace"
  local list=$(kubectl get externalsecrets -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for es in $list; do
    [ -z "$es" ] && continue
    remove_finalizers "externalsecret" "$es" "$namespace"
    kubectl delete externalsecret "$es" -n "$namespace" --grace-period=0 --force 2>/dev/null || true
  done
  list=$(kubectl get secretstores -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for ss in $list; do
    [ -z "$ss" ] && continue
    remove_finalizers "secretstore" "$ss" "$namespace"
    kubectl delete secretstore "$ss" -n "$namespace" --grace-period=0 --force 2>/dev/null || true
  done
}

cleanup_ingresses() {
  local namespace=$1
  print_info "Cleaning up Ingresses in namespace: $namespace"
  local list=$(kubectl get ingress -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for ing in $list; do
    [ -z "$ing" ] && continue
    remove_finalizers "ingress" "$ing" "$namespace"
    kubectl delete ingress "$ing" -n "$namespace" --grace-period=0 --force 2>/dev/null || true
  done
  print_info "Waiting 30s for ALB cleanup..."
  sleep 30
}

cleanup_deployments() {
  local namespace=$1
  local list=$(kubectl get deployments -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for d in $list; do [ -n "$d" ] && kubectl delete deployment "$d" -n "$namespace" --grace-period=0 2>/dev/null || true; done
}

cleanup_services() {
  local namespace=$1
  local list=$(kubectl get svc -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for svc in $list; do
    [ "$svc" = "kubernetes" ] && continue
    [ -n "$svc" ] && kubectl delete svc "$svc" -n "$namespace" --grace-period=0 2>/dev/null || true
  done
}

cleanup_configmaps() {
  local namespace=$1
  local list=$(kubectl get configmap -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for cm in $list; do
    [ "$cm" = "kube-root-ca.crt" ] && continue
    [ -n "$cm" ] && kubectl delete configmap "$cm" -n "$namespace" --grace-period=0 2>/dev/null || true
  done
}

cleanup_secrets_k8s() {
  local namespace=$1
  local list=$(kubectl get secret -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for s in $list; do
    [[ "$s" == default-token-* ]] || [[ "$s" == sh.helm.* ]] && continue
    [ -n "$s" ] && kubectl delete secret "$s" -n "$namespace" --grace-period=0 2>/dev/null || true
  done
}

cleanup_pvcs() {
  local namespace=$1
  local list=$(kubectl get pvc -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for pvc in $list; do
    [ -z "$pvc" ] && continue
    remove_finalizers "pvc" "$pvc" "$namespace"
    kubectl delete pvc "$pvc" -n "$namespace" --grace-period=0 --force 2>/dev/null || true
  done
}

cleanup_pods() {
  local namespace=$1
  kubectl delete pods --all -n "$namespace" --grace-period=0 --force 2>/dev/null || true
}

cleanup_statefulsets() {
  local namespace=$1
  local list=$(kubectl get statefulset -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for sts in $list; do [ -n "$sts" ] && kubectl delete statefulset "$sts" -n "$namespace" --grace-period=0 2>/dev/null || true; done
}

cleanup_network_policies() {
  local namespace=$1
  local list=$(kubectl get networkpolicy -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
  for np in $list; do [ -n "$np" ] && kubectl delete networkpolicy "$np" -n "$namespace" --grace-period=0 2>/dev/null || true; done
}

cleanup_namespace() {
  local namespace=$1
  kubectl patch namespace "$namespace" --type='merge' -p='{"metadata":{"finalizers":[]}}' 2>/dev/null || true
  kubectl delete namespace "$namespace" --grace-period=0 --wait=false 2>/dev/null || true
}

wait_for_namespace_deletion() {
  local namespace=$1 max_wait=120 elapsed=0 interval=5
  while [ $elapsed -lt $max_wait ]; do
    namespace_exists "$namespace" || { print_success "Namespace $namespace deleted"; return 0; }
    sleep $interval
    elapsed=$((elapsed + interval))
  done
  print_warning "Namespace $namespace still exists after ${max_wait}s"
  return 1
}

cleanup_application() {
  local namespace=$1
  print_info "Cleaning up application in namespace: $namespace"
  if ! namespace_exists "$namespace"; then
    print_warning "Namespace $namespace does not exist."
    return 0
  fi
  kubectl get all -n "$namespace" 2>/dev/null || true
  cleanup_ingresses "$namespace"
  cleanup_external_secrets "$namespace"
  cleanup_network_policies "$namespace"
  cleanup_deployments "$namespace"
  cleanup_statefulsets "$namespace"
  cleanup_services "$namespace"
  cleanup_configmaps "$namespace"
  cleanup_secrets_k8s "$namespace"
  cleanup_pvcs "$namespace"
  cleanup_pods "$namespace"
  sleep 10
  cleanup_namespace "$namespace"
  wait_for_namespace_deletion "$namespace"
}

main() {
  print_info "Kubernetes Application Cleanup - Financial Analysis"
  print_info "Environment: $ENVIRONMENT | Namespace: $APP_NAMESPACE"
  check_prerequisites
  CLUSTER_NAME=$(get_cluster_name)
  configure_kubectl "$CLUSTER_NAME"
  print_warning "This will delete ALL resources in namespace: $APP_NAMESPACE"
  read -p "Type 'yes' to proceed: " -r response
  if [ "$response" != "yes" ]; then
    print_info "Cancelled."
    exit 0
  fi
  cleanup_application "$APP_NAMESPACE"
  print_success "Application cleanup completed."
  print_info "Next (optional): ./addons.sh $ENVIRONMENT to remove Helm add-ons, then ./cleanup.sh $ENVIRONMENT for full Terraform destroy."
}

if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  echo "Usage: $0 [environment]"
  echo "  environment: development (default), staging, production"
  echo "Deletes all application resources in the financial-analysis namespace."
  exit 0
fi

main "$@"
