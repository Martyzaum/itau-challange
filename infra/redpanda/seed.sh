#!/bin/bash
set -euo pipefail

BROKERS="${REDPANDA_BROKERS:-redpanda:9092}"
TRANSACTIONS_TOPIC_NAME="${TRANSACTIONS_TOPIC:-transacoes-financeiras-processadas}"
TRANSACTIONS_DLT_TOPIC_NAME="${TRANSACTIONS_DLT_TOPIC:-transacoes-financeiras-processadas.DLT}"
TRANSACTIONS_TOPIC_PARTITIONS="${TRANSACTIONS_TOPIC_PARTITIONS:-3}"

echo "Waiting for Redpanda broker at ${BROKERS}..."
until rpk cluster info --brokers "${BROKERS}" >/dev/null 2>&1; do
  echo "  not ready yet, retrying in 2s..."
  sleep 2
done
echo "Redpanda broker is ready."

if rpk topic describe "${TRANSACTIONS_TOPIC_NAME}" --brokers "${BROKERS}" >/dev/null 2>&1; then
  echo "Topic '${TRANSACTIONS_TOPIC_NAME}' already exists, skipping creation."
else
  echo "Creating topic '${TRANSACTIONS_TOPIC_NAME}' with ${TRANSACTIONS_TOPIC_PARTITIONS} partition(s)..."
  rpk topic create "${TRANSACTIONS_TOPIC_NAME}" --brokers "${BROKERS}" --partitions "${TRANSACTIONS_TOPIC_PARTITIONS}" --replicas 1
fi

if rpk topic describe "${TRANSACTIONS_DLT_TOPIC_NAME}" --brokers "${BROKERS}" >/dev/null 2>&1; then
  echo "Topic '${TRANSACTIONS_DLT_TOPIC_NAME}' already exists, skipping creation."
else
  echo "Creating DLT topic '${TRANSACTIONS_DLT_TOPIC_NAME}' with ${TRANSACTIONS_TOPIC_PARTITIONS} partition(s)..."
  rpk topic create "${TRANSACTIONS_DLT_TOPIC_NAME}" --brokers "${BROKERS}" --partitions "${TRANSACTIONS_TOPIC_PARTITIONS}" --replicas 1
fi

echo "Transactions topic ready: '${TRANSACTIONS_TOPIC_NAME}'."
echo "Transactions DLT topic ready: '${TRANSACTIONS_DLT_TOPIC_NAME}'."
