#!/bin/bash
# Seeds fixed account balances used by Gatling (src/gatling/resources/account-ids.json).
set -euo pipefail

ENDPOINT_URL="${DYNAMODB_ENDPOINT_URL:-http://dynamodb:8000}"
TABLE="${ACCOUNT_BALANCES_TABLE_NAME:-AccountBalances}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
JSON="${ACCOUNT_IDS_JSON:-/gatling-resources/account-ids.json}"

if [[ ! -f "$JSON" ]]; then
  echo "JSON not found: $JSON"
  exit 1
fi

echo "Waiting for DynamoDB at ${ENDPOINT_URL}..."
until aws dynamodb list-tables --endpoint-url "${ENDPOINT_URL}" --region "${REGION}" >/dev/null 2>&1; do
  sleep 2
done

if ! aws dynamodb describe-table --table-name "${TABLE}" --endpoint-url "${ENDPOINT_URL}" --region "${REGION}" >/dev/null 2>&1; then
  echo "Table '${TABLE}' missing. Run make db-up / make up first."
  exit 1
fi

# Parse {"accountId":"..."} without jq/python (aws-cli image is minimal).
mapfile -t ACCOUNT_IDS < <(grep -oE '"accountId"[[:space:]]*:[[:space:]]*"[^"]+"' "$JSON" | sed -E 's/.*"([^"]+)"[[:space:]]*$/\1/')

if [[ ${#ACCOUNT_IDS[@]} -eq 0 ]]; then
  echo "No accountId entries in ${JSON}"
  exit 1
fi

now_us=$(date +%s%6N)
count=0

for account_id in "${ACCOUNT_IDS[@]}"; do
  owner="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || printf '00000000-0000-4000-8000-%012d' "$count")"
  tx_id="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || printf '00000000-0000-4000-8001-%012d' "$count")"
  amount="$(awk -v seed="$count$RANDOM" 'BEGIN { srand(seed); printf "%.2f", 100 + rand() * 9000 }')"

  aws dynamodb put-item \
    --table-name "${TABLE}" \
    --endpoint-url "${ENDPOINT_URL}" \
    --region "${REGION}" \
    --item "{
      \"account_id\": {\"S\": \"${account_id}\"},
      \"owner\": {\"S\": \"${owner}\"},
      \"balance_amount\": {\"N\": \"${amount}\"},
      \"balance_currency\": {\"S\": \"BRL\"},
      \"updated_at_micros\": {\"N\": \"${now_us}\"},
      \"last_transaction_id\": {\"S\": \"${tx_id}\"}
    }" >/dev/null

  count=$((count + 1))
  echo "  seeded ${account_id}"
done

echo "Load seed complete (${count} accounts from ${JSON})."
