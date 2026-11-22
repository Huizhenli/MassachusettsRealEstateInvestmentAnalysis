#!/usr/bin/env bash
# Delete everything the pipeline created, to stop AWS charges. IRREVERSIBLE: deletes the S3 data and the database.
#   ./scripts/teardown.sh
set -euo pipefail
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET="ma-house-investment-data-lake-${ACCOUNT}"
read -r -p "Delete stacks ma-house-* and bucket ${BUCKET} in account ${ACCOUNT}? Type 'delete' to confirm: " ok
[ "$ok" = "delete" ] || { echo "Aborted."; exit 1; }

aws cloudformation delete-stack --stack-name ma-house-etl
aws cloudformation wait stack-delete-complete --stack-name ma-house-etl
aws s3 rm "s3://${BUCKET}" --recursive || true        # bucket must be empty before deletion
aws cloudformation delete-stack --stack-name ma-house-database
aws cloudformation wait stack-delete-complete --stack-name ma-house-database
aws cloudformation delete-stack --stack-name ma-house-storage-iam
aws cloudformation wait stack-delete-complete --stack-name ma-house-storage-iam
echo "Teardown complete."
