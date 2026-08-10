#!/bin/bash
# Reset IDE password to a simpler value and apply on the running instance
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="financial-analysis-ide"
SECRET_NAME="${STACK_NAME}-password"
INSTANCE_ID="${1:-$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text)}"

NEW_PW="FaIde-$(openssl rand -hex 8)"

echo "Updating Secrets Manager secret..."
aws secretsmanager put-secret-value \
  --secret-id "$SECRET_NAME" \
  --region "$REGION" \
  --secret-string "{\"username\":\"ec2-user\",\"password\":\"$NEW_PW\"}" >/dev/null

REMOTE=$(cat <<EOF
set -e
IDE_PW='$NEW_PW'
HASHED=\$(printf %s "\$IDE_PW" | argon2 saltItWithSalt -l 32 -e)
mkdir -p /home/ec2-user/.config/code-server
cat > /home/ec2-user/.config/code-server/config.yaml <<YAML
bind-addr: 0.0.0.0:8889
auth: password
hashed-password: "\$HASHED"
cert: false
YAML
chown -R ec2-user:ec2-user /home/ec2-user/.config/code-server
systemctl restart code-server@ec2-user
sleep 2
systemctl is-active code-server@ec2-user
EOF
)
B64=$(printf '%s' "$REMOTE" | base64 -w0)
CMD=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name AWS-RunShellScript \
  --parameters "commands=[\"echo $B64 | base64 -d | bash\"]" \
  --region "$REGION" \
  --query 'Command.CommandId' \
  --output text)
sleep 15
STATUS=$(aws ssm get-command-invocation --command-id "$CMD" --instance-id "$INSTANCE_ID" --region "$REGION" --query Status --output text)
SERVICE=$(aws ssm get-command-invocation --command-id "$CMD" --instance-id "$INSTANCE_ID" --region "$REGION" --query StandardOutputContent --output text | tail -1)

IDE_URL=$(aws cloudformation describe-stacks --stack-name financial-analysis-ide-cloudfront --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='IdeUrl'].OutputValue" --output text)

echo ""
echo "=========================================="
echo "Password reset complete (SSM: $STATUS, code-server: $SERVICE)"
echo "IDE URL:  $IDE_URL"
echo "Password: $NEW_PW"
echo "=========================================="
echo ""
echo "code-server only asks for PASSWORD (no username field)."
echo "$NEW_PW" > "$SCRIPT_DIR/ide-password.txt"
chmod 600 "$SCRIPT_DIR/ide-password.txt" 2>/dev/null || true
