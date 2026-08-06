#!/bin/sh
set -eu

TOXIPROXY_API="${TOXIPROXY_API:-http://toxiproxy:8474}"
PROXY_NAME="${TOXIPROXY_DYNAMODB_PROXY_NAME:-dynamodb}"
LISTEN="${TOXIPROXY_DYNAMODB_LISTEN:-0.0.0.0:8666}"
UPSTREAM="${TOXIPROXY_DYNAMODB_UPSTREAM:-dynamodb:8000}"

echo "waiting for toxiproxy at ${TOXIPROXY_API}..."
i=0
until curl -sf "${TOXIPROXY_API}/version" >/dev/null; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "toxiproxy not ready" >&2
    exit 1
  fi
  sleep 1
done

if curl -sf "${TOXIPROXY_API}/proxies/${PROXY_NAME}" >/dev/null; then
  echo "proxy ${PROXY_NAME} already exists"
  exit 0
fi

echo "creating proxy ${PROXY_NAME}: ${LISTEN} -> ${UPSTREAM}"
curl -sf -X POST "${TOXIPROXY_API}/proxies" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"${PROXY_NAME}\",\"listen\":\"${LISTEN}\",\"upstream\":\"${UPSTREAM}\",\"enabled\":true}"

echo "toxiproxy dynamodb proxy ready"
