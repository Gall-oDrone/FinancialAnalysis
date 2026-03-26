#!/bin/bash
# Deploy and monitor Financial Analysis IDE CloudFormation stack
# Matches microservices-trading-bot deploy-ide.sh flow: IAM -> main IDE -> CloudFront

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_FILE="${SCRIPT_DIR}/../financial-analysis-ide-cfn.yaml"
STACK_NAME="financial-analysis-ide"
IAM_TEMPLATE_FILE="${SCRIPT_DIR}/../financial-analysis-ide-iam-cfn.yaml"
IAM_STACK_NAME="financial-analysis-ide-iam"
CLOUDFRONT_TEMPLATE_FILE="${SCRIPT_DIR}/../financial-analysis-ide-cloudfront-cfn.yaml"
CLOUDFRONT_STACK_NAME="financial-analysis-ide-cloudfront"
CLOUDFRONT_PRICE_CLASS="${CLOUDFRONT_PRICE_CLASS:-PriceClass_All}"
AWS_REGION="${AWS_REGION:-us-east-1}"
MAX_WAIT_TIME=3600
POLL_INTERVAL=30
LOG_FILE="${SCRIPT_DIR}/deploy-cfn.log"
INSTANCE_PROFILE_NAME=""

REPOSITORY_OWNER="${REPOSITORY_OWNER:-Gall-oDrone}"
REPOSITORY_NAME="${REPOSITORY_NAME:-financial_analysis}"
REPOSITORY_REF="${REPOSITORY_REF:-main}"
INSTANCE_VOLUME_SIZE="${INSTANCE_VOLUME_SIZE:-50}"

log_info() { echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE" >&2; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE" >&2; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE" >&2; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE" >&2; }

stack_exists() { aws cloudformation describe-stacks --stack-name "$1" --region "$AWS_REGION" &>/dev/null; }
get_stack_status() {
  aws cloudformation describe-stacks --stack-name "$1" --region "$AWS_REGION" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NONE"
}

wait_for_stack() {
  local operation=$1
  local stack_name=$2
  local start_time=$(date +%s)
  local elapsed=0
  log_info "Waiting for stack '$stack_name' $operation to complete..."
  while [ $elapsed -lt $MAX_WAIT_TIME ]; do
    local status=$(get_stack_status "$stack_name")
    case "$status" in
      *COMPLETE) log_success "Stack '$stack_name' $operation completed!"; return 0 ;;
      *FAILED)   log_error "Stack '$stack_name' $operation failed!"; exit 1 ;;
      *ROLLBACK*) log_error "Stack '$stack_name' is rolling back!"; exit 1 ;;
    esac
    sleep $POLL_INTERVAL
    elapsed=$(($(date +%s) - start_time))
  done
  log_error "Timeout waiting for stack '$stack_name'"; exit 1
}

wait_for_stack_deletion() {
  local stack_name=$1
  local start_time=$(date +%s)
  local elapsed=0
  log_info "Waiting for stack '$stack_name' deletion to complete..."
  while [ $elapsed -lt $MAX_WAIT_TIME ]; do
    if stack_exists "$stack_name"; then
      sleep $POLL_INTERVAL
      elapsed=$(($(date +%s) - start_time))
    else
      log_success "Stack '$stack_name' deleted!"
      return 0
    fi
  done
  log_error "Timeout waiting for stack '$stack_name' deletion"; exit 1
}

upload_template_to_s3() {
  local template_file=$1
  local stack_name=$2
  local aws_account=$(aws sts get-caller-identity --query 'Account' --output text)
  local bucket_name="cfn-templates-${aws_account}-${AWS_REGION}"
  local template_key="financial-analysis-ide/${stack_name}-$(date +%Y%m%d-%H%M%S).yaml"
  if ! aws s3 ls "s3://${bucket_name}" &>/dev/null; then
    log_info "Creating S3 bucket: ${bucket_name}"
    if [ "$AWS_REGION" = "us-east-1" ]; then
      aws s3api create-bucket --bucket "$bucket_name" --region "$AWS_REGION" >/dev/null
    else
      aws s3api create-bucket --bucket "$bucket_name" --region "$AWS_REGION" --create-bucket-configuration LocationConstraint="$AWS_REGION" >/dev/null
    fi
  fi
  aws s3 cp "$template_file" "s3://${bucket_name}/${template_key}" --region "$AWS_REGION" --only-show-errors >/dev/null
  echo "https://${bucket_name}.s3.${AWS_REGION}.amazonaws.com/${template_key}"
}

check_prerequisites() {
  log_info "Checking prerequisites..."
  command -v aws &>/dev/null || { log_error "AWS CLI not installed."; exit 1; }
  aws sts get-caller-identity &>/dev/null || { log_error "AWS credentials not configured."; exit 1; }
  for f in "$TEMPLATE_FILE" "$IAM_TEMPLATE_FILE" "$CLOUDFRONT_TEMPLATE_FILE"; do
    [ -f "$f" ] || { log_error "Template not found: $f"; exit 1; }
  done
  log_success "Prerequisites OK"
}

deploy_iam_stack() {
  log_info "Deploying IAM stack: $IAM_STACK_NAME"
  local template_url=$(upload_template_to_s3 "$IAM_TEMPLATE_FILE" "$IAM_STACK_NAME")
  local recreate_needed=0
  if stack_exists "$IAM_STACK_NAME"; then
    local status=$(get_stack_status "$IAM_STACK_NAME")
    if [ "$status" = "ROLLBACK_COMPLETE" ] || [ "$status" = "UPDATE_ROLLBACK_COMPLETE" ]; then
      log_warning "IAM stack is $status; deleting and recreating..."
      aws cloudformation delete-stack --stack-name "$IAM_STACK_NAME" --region "$AWS_REGION" >/dev/null
      wait_for_stack_deletion "$IAM_STACK_NAME"
      recreate_needed=1
    else
      local update_out
      if update_out=$(aws cloudformation update-stack --stack-name "$IAM_STACK_NAME" --template-url "$template_url" \
        --capabilities CAPABILITY_NAMED_IAM --parameters "ParameterKey=ParentStackName,ParameterValue=$STACK_NAME" \
        --region "$AWS_REGION" 2>&1); then
        wait_for_stack "UPDATE" "$IAM_STACK_NAME"
      else
        case "$update_out" in
          *"No updates are to be performed"*)
            log_success "No IAM stack changes (already up to date)."
            ;;
          *)
            log_error "IAM stack update failed: $update_out"
            exit 1
            ;;
        esac
      fi
    fi
  else
    aws cloudformation create-stack --stack-name "$IAM_STACK_NAME" --template-url "$template_url" \
      --capabilities CAPABILITY_NAMED_IAM --parameters "ParameterKey=ParentStackName,ParameterValue=$STACK_NAME" \
      --region "$AWS_REGION" --query 'StackId' --output text
    wait_for_stack "CREATE" "$IAM_STACK_NAME"
  fi

  if [ "$recreate_needed" = "1" ]; then
    # If we reach here, we deleted a rollback-complete stack and must recreate.
    aws cloudformation create-stack --stack-name "$IAM_STACK_NAME" --template-url "$template_url" \
      --capabilities CAPABILITY_NAMED_IAM --parameters "ParameterKey=ParentStackName,ParameterValue=$STACK_NAME" \
      --region "$AWS_REGION" --query 'StackId' --output text
    wait_for_stack "CREATE" "$IAM_STACK_NAME"
  fi

  INSTANCE_PROFILE_NAME=$(aws cloudformation describe-stacks --stack-name "$IAM_STACK_NAME" --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='InstanceProfileName'].OutputValue" --output text)
  [ -n "$INSTANCE_PROFILE_NAME" ] && [ "$INSTANCE_PROFILE_NAME" != "None" ] || { log_error "InstanceProfileName not found."; exit 1; }
}

deploy_main_stack() {
  log_info "Deploying main IDE stack: $STACK_NAME"
  local template_url=$(upload_template_to_s3 "$TEMPLATE_FILE" "$STACK_NAME")
  if stack_exists "$STACK_NAME"; then
    local status=$(get_stack_status "$STACK_NAME")
    if [ "$status" = "ROLLBACK_COMPLETE" ] || [ "$status" = "UPDATE_ROLLBACK_COMPLETE" ]; then
      log_warning "Main stack is $status; deleting and recreating..."
      aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$AWS_REGION" >/dev/null
      wait_for_stack_deletion "$STACK_NAME"
    else
      local update_out
      if update_out=$(aws cloudformation update-stack --stack-name "$STACK_NAME" --template-url "$template_url" \
        --capabilities CAPABILITY_NAMED_IAM \
        --parameters \
          "ParameterKey=RepositoryOwner,ParameterValue=$REPOSITORY_OWNER" \
          "ParameterKey=RepositoryName,ParameterValue=$REPOSITORY_NAME" \
          "ParameterKey=RepositoryRef,ParameterValue=$REPOSITORY_REF" \
          "ParameterKey=InstanceVolumeSize,ParameterValue=$INSTANCE_VOLUME_SIZE" \
          "ParameterKey=InstanceProfileName,ParameterValue=$INSTANCE_PROFILE_NAME" \
        --region "$AWS_REGION" 2>&1); then
        wait_for_stack "UPDATE" "$STACK_NAME"
      else
        case "$update_out" in
          *"No updates are to be performed"*)
            log_success "No main stack changes (already up to date)."
            ;;
          *)
            log_error "Main stack update failed: $update_out"
            exit 1
            ;;
        esac
      fi
      return 0
    fi
  else
    aws cloudformation create-stack --stack-name "$STACK_NAME" --template-url "$template_url" \
      --capabilities CAPABILITY_NAMED_IAM \
      --parameters \
        "ParameterKey=RepositoryOwner,ParameterValue=$REPOSITORY_OWNER" \
        "ParameterKey=RepositoryName,ParameterValue=$REPOSITORY_NAME" \
        "ParameterKey=RepositoryRef,ParameterValue=$REPOSITORY_REF" \
        "ParameterKey=InstanceVolumeSize,ParameterValue=$INSTANCE_VOLUME_SIZE" \
        "ParameterKey=InstanceProfileName,ParameterValue=$INSTANCE_PROFILE_NAME" \
      --region "$AWS_REGION" --query 'StackId' --output text
    wait_for_stack "CREATE" "$STACK_NAME"
  fi

  # If we reach here, we deleted a rollback-complete stack and must recreate.
  aws cloudformation create-stack --stack-name "$STACK_NAME" --template-url "$template_url" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameters \
      "ParameterKey=RepositoryOwner,ParameterValue=$REPOSITORY_OWNER" \
      "ParameterKey=RepositoryName,ParameterValue=$REPOSITORY_NAME" \
      "ParameterKey=RepositoryRef,ParameterValue=$REPOSITORY_REF" \
      "ParameterKey=InstanceVolumeSize,ParameterValue=$INSTANCE_VOLUME_SIZE" \
      "ParameterKey=InstanceProfileName,ParameterValue=$INSTANCE_PROFILE_NAME" \
    --region "$AWS_REGION" --query 'StackId' --output text
  wait_for_stack "CREATE" "$STACK_NAME"
}

deploy_cloudfront_stack() {
  local instance_dns=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='InstancePublicDnsName'].OutputValue" --output text)
  [ -n "$instance_dns" ] && [ "$instance_dns" != "None" ] || { log_error "InstancePublicDnsName not found."; exit 1; }
  log_info "Deploying CloudFront stack: $CLOUDFRONT_STACK_NAME (origin: $instance_dns)"
  local template_url=$(upload_template_to_s3 "$CLOUDFRONT_TEMPLATE_FILE" "$CLOUDFRONT_STACK_NAME")
  if stack_exists "$CLOUDFRONT_STACK_NAME"; then
    local status=$(get_stack_status "$CLOUDFRONT_STACK_NAME")
    if [ "$status" = "ROLLBACK_COMPLETE" ] || [ "$status" = "UPDATE_ROLLBACK_COMPLETE" ]; then
      log_warning "CloudFront stack is $status; deleting and recreating..."
      aws cloudformation delete-stack --stack-name "$CLOUDFRONT_STACK_NAME" --region "$AWS_REGION" >/dev/null
      wait_for_stack_deletion "$CLOUDFRONT_STACK_NAME"
    else
      local update_out
      if update_out=$(aws cloudformation update-stack --stack-name "$CLOUDFRONT_STACK_NAME" --template-url "$template_url" \
        --parameters \
          "ParameterKey=ParentStackName,ParameterValue=$STACK_NAME" \
          "ParameterKey=InstancePublicDnsName,ParameterValue=$instance_dns" \
          "ParameterKey=PriceClass,ParameterValue=$CLOUDFRONT_PRICE_CLASS" \
        --region "$AWS_REGION" 2>&1); then
        wait_for_stack "UPDATE" "$CLOUDFRONT_STACK_NAME"
      else
        case "$update_out" in
          *"No updates are to be performed"*)
            log_success "No CloudFront stack changes (already up to date)."
            ;;
          *)
            log_error "CloudFront stack update failed: $update_out"
            exit 1
            ;;
        esac
      fi
      return 0
    fi
  else
    local create_out
    if create_out=$(aws cloudformation create-stack --stack-name "$CLOUDFRONT_STACK_NAME" --template-url "$template_url" \
      --parameters \
        "ParameterKey=ParentStackName,ParameterValue=$STACK_NAME" \
        "ParameterKey=InstancePublicDnsName,ParameterValue=$instance_dns" \
        "ParameterKey=PriceClass,ParameterValue=$CLOUDFRONT_PRICE_CLASS" \
      --region "$AWS_REGION" --query 'StackId' --output text 2>&1); then
      wait_for_stack "CREATE" "$CLOUDFRONT_STACK_NAME"
    else
      case "$create_out" in
        *"AlreadyExistsException"*)
          log_warning "CloudFront stack already exists; updating instead."
          aws cloudformation update-stack --stack-name "$CLOUDFRONT_STACK_NAME" --template-url "$template_url" \
            --parameters \
              "ParameterKey=ParentStackName,ParameterValue=$STACK_NAME" \
              "ParameterKey=InstancePublicDnsName,ParameterValue=$instance_dns" \
              "ParameterKey=PriceClass,ParameterValue=$CLOUDFRONT_PRICE_CLASS" \
            --region "$AWS_REGION"
          wait_for_stack "UPDATE" "$CLOUDFRONT_STACK_NAME"
          ;;
        *)
          log_error "CloudFront create failed: $create_out"
          exit 1
          ;;
      esac
    fi
  fi

  # If we reach here, we deleted a rollback-complete stack and must recreate.
  local recreate_out
  if recreate_out=$(aws cloudformation create-stack --stack-name "$CLOUDFRONT_STACK_NAME" --template-url "$template_url" \
    --parameters \
      "ParameterKey=ParentStackName,ParameterValue=$STACK_NAME" \
      "ParameterKey=InstancePublicDnsName,ParameterValue=$instance_dns" \
      "ParameterKey=PriceClass,ParameterValue=$CLOUDFRONT_PRICE_CLASS" \
    --region "$AWS_REGION" --query 'StackId' --output text 2>&1); then
    wait_for_stack "CREATE" "$CLOUDFRONT_STACK_NAME"
  else
    case "$recreate_out" in
      *"AlreadyExistsException"*)
        log_warning "CloudFront stack already exists (post-delete); updating instead."
        aws cloudformation update-stack --stack-name "$CLOUDFRONT_STACK_NAME" --template-url "$template_url" \
          --parameters \
            "ParameterKey=ParentStackName,ParameterValue=$STACK_NAME" \
            "ParameterKey=InstancePublicDnsName,ParameterValue=$instance_dns" \
            "ParameterKey=PriceClass,ParameterValue=$CLOUDFRONT_PRICE_CLASS" \
          --region "$AWS_REGION"
        wait_for_stack "UPDATE" "$CLOUDFRONT_STACK_NAME"
        ;;
      *)
        log_error "CloudFront recreate failed: $recreate_out"
        exit 1
        ;;
    esac
  fi
}

retrieve_password() {
  local secret_name="${STACK_NAME}-password"
  log_info "Retrieving IDE password from Secrets Manager..."
  local password
  if password=$(aws secretsmanager get-secret-value --secret-id "$secret_name" --region "$AWS_REGION" --query 'SecretString' --output text 2>/dev/null); then
    password=$(echo "$password" | jq -r '.password' 2>/dev/null || echo "$password")
    if [ -n "$password" ] && [ "$password" != "null" ]; then
      log_success "Password retrieved!"
      echo ""; echo "=========================================="
      echo -e "${GREEN}IDE Password:${NC} $password"
      echo "=========================================="; echo ""
      return 0
    fi
  fi
  log_warning "Retrieve password manually: aws secretsmanager get-secret-value --secret-id $secret_name --region $AWS_REGION --query SecretString --output text | jq -r .password"
  return 1
}

main() {
  log_info "=========================================="
  log_info "Financial Analysis IDE Deployment"
  log_info "=========================================="
  log_info "Log file: $LOG_FILE"
  check_prerequisites
  deploy_iam_stack
  deploy_main_stack
  deploy_cloudfront_stack
  local ide_url=$(aws cloudformation describe-stacks --stack-name "$CLOUDFRONT_STACK_NAME" --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='IdeUrl'].OutputValue" --output text 2>/dev/null)
  local jupyter_url=$(aws cloudformation describe-stacks --stack-name "$CLOUDFRONT_STACK_NAME" --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='JupyterUrl'].OutputValue" --output text 2>/dev/null)
  [ -n "$ide_url" ] && [ "$ide_url" != "None" ] && log_success "IDE URL: $ide_url"
  [ -n "$jupyter_url" ] && [ "$jupyter_url" != "None" ] && log_success "Jupyter URL: $jupyter_url"
  retrieve_password
  log_success "Deployment completed. Full log: $LOG_FILE"
}

main "$@"
