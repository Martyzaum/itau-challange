#!/bin/bash
set -euo pipefail

API="${TOXIPROXY_API:-http://localhost:8474}"
PROXY_NAME="${TOXIPROXY_DYNAMODB_PROXY_NAME:-dynamodb}"
TOXIC_NAME="${TOXIPROXY_LATENCY_TOXIC_NAME:-latency}"
LATENCY_MS="${LATENCY_MS:-2000}"
JITTER_MS="${JITTER_MS:-500}"

curl -sf -X DELETE "${API}/proxies/${PROXY_NAME}/toxics/${TOXIC_NAME}" >/dev/null 2>&1 || true

curl -sf -X POST "${API}/proxies/${PROXY_NAME}/toxics" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"${TOXIC_NAME}\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":${LATENCY_MS},\"jitter\":${JITTER_MS}}}"

echo "added toxic ${TOXIC_NAME} latency=${LATENCY_MS}ms jitter=${JITTER_MS}ms on ${PROXY_NAME}"
