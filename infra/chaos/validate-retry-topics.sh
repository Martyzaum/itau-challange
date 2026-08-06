#!/bin/bash
# Run from repo root via: make chaos-retry-topics
# Flow: pause DynamoDB → produce valid Kafka events → assert keys on retry-1
#       → unpause DynamoDB → assert GET /balances eventually 200.
set -euo pipefail

COMPOSE_BIN="${COMPOSE_CMD:-docker compose}"
TOPIC="${TRANSACTIONS_TOPIC:-transacoes-financeiras-processadas}"
RETRY_1="${TOPIC}.retry-1"
APP_URL="${APP_URL:-http://localhost:8080}"
COUNT="${COUNT:-3}"
WAIT_RETRY_SEC="${WAIT_RETRY_SEC:-60}"
WAIT_RECOVER_SEC="${WAIT_RECOVER_SEC:-120}"
WORK_DIR="$(mktemp -d)"
IDS_FILE="${WORK_DIR}/accounts.txt"
PAYLOAD_FILE="${WORK_DIR}/produce.tsv"

log() { echo "[chaos-retry] $*"; }
die() { echo "[chaos-retry] ERROR: $*" >&2; exit 1; }

cleanup() {
  log "ensuring dynamodb unpaused..."
  ${COMPOSE_BIN} unpause dynamodb 2>/dev/null || true
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

log "app=${APP_URL} topic=${TOPIC} retry1=${RETRY_1} count=${COUNT}"

curl -sf "${APP_URL}/actuator/health/liveness" >/dev/null \
  || die "app not healthy at ${APP_URL} (start with make up)"

: >"${IDS_FILE}"
: >"${PAYLOAD_FILE}"

NOW_US="$(date +%s%6N 2>/dev/null || echo $(($(date +%s) * 1000000)))"
CREATED_US=$((NOW_US - 86400000000))

log "building ${COUNT} valid events..."
for ((i = 1; i <= COUNT; i++)); do
  account_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  owner_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  tx_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  echo "${account_id}" >>"${IDS_FILE}"
  ts=$((NOW_US + i * 1000))
  amount="$(python3 -c "print(f'{100 + $i:.2f}')")"
  payload="$(printf '{"transaction":{"id":"%s","type":"CREDIT","amount":10.00,"currency":"BRL","status":"APPROVED","timestamp":%s},"account":{"id":"%s","owner":"%s","created_at":%s,"status":"ENABLED","balance":{"amount":%s,"currency":"BRL"}}}' \
    "$tx_id" "$ts" "$account_id" "$owner_id" "$CREATED_US" "$amount")"
  printf '%s\t%s\n' "$account_id" "$payload" >>"${PAYLOAD_FILE}"
done

log "pausing dynamodb..."
${COMPOSE_BIN} pause dynamodb

log "producing events while DynamoDB is down..."
${COMPOSE_BIN} run --rm -T --entrypoint /bin/bash redpanda-seed -c \
  "rpk topic produce ${TOPIC} --brokers redpanda:9092 -f '%k\t%v\n'" \
  <"${PAYLOAD_FILE}"

log "waiting up to ${WAIT_RETRY_SEC}s for account keys on ${RETRY_1}..."
found=0
deadline=$((SECONDS + WAIT_RETRY_SEC))
while ((SECONDS < deadline)); do
  found="$(
    ${COMPOSE_BIN} run --rm -T --entrypoint /bin/bash redpanda-seed -c \
      "timeout 8 rpk topic consume ${RETRY_1} --brokers redpanda:9092 --offset start --num 2000 --format '%k\n' || true" \
      | python3 -c '
import sys
wanted=set(open(sys.argv[1]).read().split())
seen=set(line.strip() for line in sys.stdin if line.strip() in wanted)
print(len(seen))
' "${IDS_FILE}"
  )"
  found="$(echo "${found}" | tail -1 | tr -d '[:space:]')"
  [[ "${found}" =~ ^[0-9]+$ ]] || found=0
  log "  retry-1 matched keys: ${found}/${COUNT}"
  if ((found >= COUNT)); then
    break
  fi
  sleep 2
done

if ((found < COUNT)); then
  die "expected >= ${COUNT} keys on ${RETRY_1}, got ${found} (is the app running this branch with async retry?)"
fi
log "OK: failures routed to retry-1"

log "unpausing dynamodb..."
${COMPOSE_BIN} unpause dynamodb
trap 'rm -rf "${WORK_DIR}"' EXIT

log "waiting up to ${WAIT_RECOVER_SEC}s for balances after retry processing..."
ok=0
deadline=$((SECONDS + WAIT_RECOVER_SEC))
while ((SECONDS < deadline)); do
  ok=0
  while IFS= read -r account_id; do
    [[ -z "${account_id}" ]] && continue
    code="$(curl -s -o /dev/null -w '%{http_code}' "${APP_URL}/balances/${account_id}" || true)"
    if [[ "${code}" == "200" ]]; then
      ok=$((ok + 1))
    fi
  done <"${IDS_FILE}"
  log "  recovered HTTP 200: ${ok}/${COUNT}"
  if ((ok >= COUNT)); then
    log "OK: all balances available after recovery"
    log "CHAOS RETRY VALIDATION PASSED"
    exit 0
  fi
  sleep 3
done

die "after recovery only ${ok}/${COUNT} balances returned 200"
