#!/bin/bash
set -euo pipefail

API="${TOXIPROXY_API:-http://localhost:8474}"
PROXY_NAME="${TOXIPROXY_DYNAMODB_PROXY_NAME:-dynamodb}"
TOXIC_NAME="${TOXIPROXY_LATENCY_TOXIC_NAME:-latency}"

if curl -sf -X DELETE "${API}/proxies/${PROXY_NAME}/toxics/${TOXIC_NAME}" >/dev/null; then
  echo "removed toxic ${TOXIC_NAME} from ${PROXY_NAME}"
else
  echo "toxic ${TOXIC_NAME} not present on ${PROXY_NAME} (ok)"
fi
