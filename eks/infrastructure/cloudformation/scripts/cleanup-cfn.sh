#!/bin/bash
# Delete Financial Analysis IDE CloudFormation stacks in the correct order
# Matches microservices-trading-bot cleanup-ide.sh: CloudFront -> main -> IAM

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK_NAME="financial-analysis-ide"
IAM_STACK_NAME="financial-analysis-ide-iam"
CLOUDFRONT_STACK_NAME="financial-analysis-ide-cloudfront"
AWS_REGION="${AWS_REGION:-us-east-1}"
MAX_WAIT_TIME=3600
POLL_INTERVAL=30
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_FILE="${LOG_DIR}/cleanup-cfn.log"
FORCE_DELETE=0

mkdir -p "$LOG_DIR"

log_info() { echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"; }

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]
Delete Financial Analysis IDE CloudFormation stacks (CloudFront -> main -> IAM).
Options:
  -r, --region   AWS region (default: $AWS_REGION)
  -f, --force    Do not prompt for confirmation
  -h, --help     Show this help message
EOF
}

stack_exists() {
  aws cloudformation describe-stacks --stack-name "$1" --region "$AWS_REGION" &>/dev/null
}

get_stack_status() {
  aws cloudformation describe-stacks --stack-name "$1" --region "$AWS_REGION" \
    --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NONE"
}

wait_for_stack_deletion() {
  local stack_name=$1
  local start_time=$(date +%s)
  log_info "Waiting for stack '$stack_name' to delete..."
  while true; do
    local status=$(get_stack_status "$stack_name")
    if [ "$status" = "NONE" ]; then
      log_success "Stack '$stack_name' deleted."
      return 0
    fi
    case "$status" in
      DELETE_COMPLETE) log_success "Stack '$stack_name' deleted."; return 0 ;;
      *FAILED)         log_error "Stack '$stack_name' deletion failed."; exit 1 ;;
    esac
    if [ $(($(date +%s) - start_time)) -gt $MAX_WAIT_TIME ]; then
      log_error "Timeout waiting for stack '$stack_name' to delete."
      exit 1
    fi
    sleep $POLL_INTERVAL
  done
}

delete_stack() {
  local stack_name=$1
  if ! stack_exists "$stack_name"; then
    log_warning "Stack '$stack_name' does not exist. Skipping."
    return 0
  fi
  log_info "Deleting stack '$stack_name'..."
  aws cloudformation delete-stack --stack-name "$stack_name" --region "$AWS_REGION"
  wait_for_stack_deletion "$stack_name"
}

prompt_confirmation() {
  if [ $FORCE_DELETE -eq 1 ]; then
    return
  fi
  echo ""
  log_warning "This will delete all Financial Analysis IDE stacks:"
  log_warning "  - $CLOUDFRONT_STACK_NAME"
  log_warning "  - $STACK_NAME"
  log_warning "  - $IAM_STACK_NAME"
  echo ""
  read -p "Continue? (y/N) " -n 1 -r
  echo ""
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_info "Aborted by user."
    exit 0
  fi
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -r|--region) AWS_REGION="$2"; shift 2 ;;
      -f|--force)  FORCE_DELETE=1; shift ;;
      -h|--help)   usage; exit 0 ;;
      *)           log_error "Unknown option: $1"; usage; exit 1 ;;
    esac
  done

  log_info "=========================================="
  log_info "Financial Analysis IDE Cleanup"
  log_info "=========================================="
  log_info "Region: $AWS_REGION. Log: $LOG_FILE"

  command -v aws &>/dev/null || { log_error "AWS CLI not installed."; exit 1; }
  aws sts get-caller-identity --region "$AWS_REGION" &>/dev/null || { log_error "AWS credentials invalid."; exit 1; }

  prompt_confirmation

  delete_stack "$CLOUDFRONT_STACK_NAME"
  delete_stack "$STACK_NAME"
  delete_stack "$IAM_STACK_NAME"

  log_success "Cleanup completed. Full log: $LOG_FILE"
}

main "$@"
