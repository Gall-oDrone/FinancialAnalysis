#!/bin/bash
set -e
REGION=us-east-1
STACK=financial-analysis-ide
INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" --output text)
echo "InstanceId=$INSTANCE_ID"

# Run comparison on the instance: fetch secret, hash it, compare to config
REMOTE='set -e
CONFIG=/home/ec2-user/.config/code-server/config.yaml
echo "=== LIVE CONFIG ==="
cat "$CONFIG"
echo ""
echo "=== SERVICE ==="
systemctl is-active code-server@ec2-user || true
echo ""
echo "=== SECRET vs HASH ==="
SECRET_JSON=$(aws secretsmanager get-secret-value --secret-id financial-analysis-ide-password --region us-east-1 --query SecretString --output text)
PW=$(printf %s "$SECRET_JSON" | jq -r .password)
echo "password_len=${#PW}"
GEN=$(printf %s "$PW" | argon2 saltItWithSalt -l 32 -e)
STORED=$(grep hashed-password "$CONFIG" | sed "s/.*hashed-password: *\"\{0,1\}//;s/\"\{0,1\} *$//")
echo "stored=$STORED"
echo "generated=$GEN"
if [ "$STORED" = "$GEN" ]; then echo "HASH_MATCH=yes"; else echo "HASH_MATCH=no"; fi
'

B64=$(printf '%s' "$REMOTE" | base64 -w0 2>/dev/null || printf '%s' "$REMOTE" | base64 | tr -d '\n')
CMD_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name AWS-RunShellScript \
  --parameters "commands=[\"echo $B64 | base64 -d | bash\"]" \
  --region "$REGION" \
  --query 'Command.CommandId' --output text)
echo "CommandId=$CMD_ID"
sleep 12
aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" --region "$REGION" \
  --output json --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}'
