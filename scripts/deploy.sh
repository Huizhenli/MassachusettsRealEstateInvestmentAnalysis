#!/usr/bin/env bash
# Deploy the whole pipeline. Requires the AWS CLI configured for your account.
#   DB_PASSWORD='...' ./scripts/deploy.sh
set -euo pipefail
: "${DB_PASSWORD:?Set DB_PASSWORD (min 8 chars)}"
cd "$(dirname "$0")/.."
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ma-house-investment-data-lake-${ACCOUNT}"

aws cloudformation deploy --stack-name ma-house-storage-iam \
  --template-file CloudFormation/01-storage-iam.yaml --capabilities CAPABILITY_NAMED_IAM
aws cloudformation deploy --stack-name ma-house-database \
  --template-file CloudFormation/02-database.yaml \
  --parameter-overrides DBPassword="$DB_PASSWORD"

aws s3 cp PythonScript/ "s3://${BUCKET}/scripts/" --recursive --exclude "*" --include "*.py"
python scripts/download_zillow.py --out Data --bucket "$BUCKET"

aws cloudformation deploy --stack-name ma-house-etl \
  --template-file CloudFormation/03-full-etl.yaml \
  --parameter-overrides DBPassword="$DB_PASSWORD"

SFN=$(aws cloudformation describe-stack-resources --stack-name ma-house-etl \
  --query "StackResources[?ResourceType=='AWS::StepFunctions::StateMachine'].PhysicalResourceId" --output text)
aws stepfunctions start-execution --state-machine-arn "$SFN"
echo "Pipeline started. Watch it in the Step Functions console."
