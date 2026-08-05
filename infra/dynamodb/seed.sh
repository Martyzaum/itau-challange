#!/bin/bash
set -euo pipefail

ENDPOINT_URL="${DYNAMODB_ENDPOINT_URL:-http://dynamodb:8000}"
ACCOUNT_BALANCES_TABLE_NAME="${ACCOUNT_BALANCES_TABLE_NAME:-AccountBalances}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "Waiting for DynamoDB Local at ${ENDPOINT_URL}..."
until aws dynamodb list-tables --endpoint-url "${ENDPOINT_URL}" --region "${REGION}" >/dev/null 2>&1; do
  echo "  not ready yet, retrying in 2s..."
  sleep 2
done
echo "DynamoDB Local is ready."

if aws dynamodb describe-table --table-name "${ACCOUNT_BALANCES_TABLE_NAME}" --endpoint-url "${ENDPOINT_URL}" --region "${REGION}" >/dev/null 2>&1; then
  echo "Table '${ACCOUNT_BALANCES_TABLE_NAME}' already exists, skipping creation."
else
  echo "Creating table '${ACCOUNT_BALANCES_TABLE_NAME}'..."
  aws dynamodb create-table \
    --table-name "${ACCOUNT_BALANCES_TABLE_NAME}" \
    --attribute-definitions AttributeName=account_id,AttributeType=S \
    --key-schema AttributeName=account_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url "${ENDPOINT_URL}" \
    --region "${REGION}" >/dev/null
  aws dynamodb wait table-exists \
    --table-name "${ACCOUNT_BALANCES_TABLE_NAME}" \
    --endpoint-url "${ENDPOINT_URL}" \
    --region "${REGION}"
  echo "Table '${ACCOUNT_BALANCES_TABLE_NAME}' created."
fi

echo "Seed complete. Table '${ACCOUNT_BALANCES_TABLE_NAME}' is ready."
