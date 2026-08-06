#!/bin/bash
set -euo pipefail

BROKERS="${REDPANDA_BROKERS:-redpanda:9092}"
TRANSACTIONS_TOPIC_NAME="${TRANSACTIONS_TOPIC:-transacoes-financeiras-processadas}"
TRANSACTIONS_DLT_TOPIC_NAME="${TRANSACTIONS_DLT_TOPIC:-transacoes-financeiras-processadas.DLT}"
TRANSACTIONS_TOPIC_PARTITIONS="${TRANSACTIONS_TOPIC_PARTITIONS:-3}"
TRANSACTIONS_RETRY_MAX_ATTEMPTS="${TRANSACTIONS_RETRY_MAX_ATTEMPTS:-3}"

echo "Waiting for Redpanda broker at ${BROKERS}..."
until rpk cluster info --brokers "${BROKERS}" >/dev/null 2>&1; do
  echo "  not ready yet, retrying in 2s..."
  sleep 2
done
echo "Redpanda broker is ready."

create_topic_if_missing() {
  local topic_name="$1"
  if rpk topic describe "${topic_name}" --brokers "${BROKERS}" >/dev/null 2>&1; then
    echo "Topic '${topic_name}' already exists, skipping creation."
  else
    echo "Creating topic '${topic_name}' with ${TRANSACTIONS_TOPIC_PARTITIONS} partition(s)..."
    rpk topic create "${topic_name}" --brokers "${BROKERS}" --partitions "${TRANSACTIONS_TOPIC_PARTITIONS}" --replicas 1
  fi
}

create_topic_if_missing "${TRANSACTIONS_TOPIC_NAME}"
create_topic_if_missing "${TRANSACTIONS_DLT_TOPIC_NAME}"

for ((attempt = 1; attempt <= TRANSACTIONS_RETRY_MAX_ATTEMPTS; attempt++)); do
  create_topic_if_missing "${TRANSACTIONS_TOPIC_NAME}.retry-${attempt}"
done

echo "Transactions topic ready: '${TRANSACTIONS_TOPIC_NAME}'."
echo "Transactions DLT topic ready: '${TRANSACTIONS_DLT_TOPIC_NAME}'."
echo "Retry topics ready: ${TRANSACTIONS_TOPIC_NAME}.retry-1..${TRANSACTIONS_RETRY_MAX_ATTEMPTS}."
