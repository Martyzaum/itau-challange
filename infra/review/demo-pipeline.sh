#!/usr/bin/env bash
# Reviewer demo pipeline: boots obs stack (optional), seeds data, runs load + chaos,
# and leaves SigNoz full of real signals. Invoked via: make review-demo
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

# DEMO_PROFILE (quick|full) — do not reuse Gatling/Kafka PROFILE.
DEMO_PROFILE="${DEMO_PROFILE:-${PROFILE:-quick}}"
SKIP_BOOTSTRAP="${SKIP_BOOTSTRAP:-0}"
SKIP_CHAOS="${SKIP_CHAOS:-0}"
SKIP_LOAD="${SKIP_LOAD:-0}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
SIGNOZ_URL="${SIGNOZ_URL:-http://localhost:3301}"
CACHE_ENABLED="${CACHE_ENABLED:-true}"
KEEP_GOING_ON_CHAOS="${KEEP_GOING_ON_CHAOS:-1}"
if [[ "${CACHE_ENABLED}" == "true" ]]; then
  CACHE_MODE_LABEL=on
else
  CACHE_MODE_LABEL=off
fi
# Prevent DEMO_PROFILE=quick from leaking into make load-* (Gatling PROFILE).
unset PROFILE || true

case "${DEMO_PROFILE}" in
  quick)
    MIXED_WORKERS="${MIXED_WORKERS:-4}"
    MIXED_DURATION="${MIXED_DURATION:-25s}"
    HTTP_VUS="${HTTP_VUS:-15}"
    HTTP_DURATION="${HTTP_DURATION:-20s}"
    HTTP_RAMP="${HTTP_RAMP:-5s}"
    CHAOS_HTTP_VUS="${CHAOS_HTTP_VUS:-8}"
    CHAOS_HTTP_DURATION="${CHAOS_HTTP_DURATION:-15s}"
    KAFKA_WORKERS="${KAFKA_WORKERS:-3}"
    KAFKA_DURATION="${KAFKA_DURATION:-20s}"
    RETRY_COUNT="${RETRY_COUNT:-3}"
    CB_COOLDOWN_SEC="${CB_COOLDOWN_SEC:-35}"
    ;;
  full)
    MIXED_WORKERS="${MIXED_WORKERS:-8}"
    MIXED_DURATION="${MIXED_DURATION:-60s}"
    HTTP_VUS="${HTTP_VUS:-40}"
    HTTP_DURATION="${HTTP_DURATION:-45s}"
    HTTP_RAMP="${HTTP_RAMP:-10s}"
    CHAOS_HTTP_VUS="${CHAOS_HTTP_VUS:-20}"
    CHAOS_HTTP_DURATION="${CHAOS_HTTP_DURATION:-30s}"
    KAFKA_WORKERS="${KAFKA_WORKERS:-6}"
    KAFKA_DURATION="${KAFKA_DURATION:-45s}"
    RETRY_COUNT="${RETRY_COUNT:-5}"
    CB_COOLDOWN_SEC="${CB_COOLDOWN_SEC:-40}"
    ;;
  *)
    echo "DEMO_PROFILE must be quick|full (got: ${DEMO_PROFILE})" >&2
    exit 2
    ;;
esac

PHASE=0
FAILED=0
STARTED_AT="$(date +%s)"
REPORT_DIR="${ROOT}/build/review-demo"
mkdir -p "${REPORT_DIR}"
LOG_FILE="${REPORT_DIR}/pipeline-$(date +%Y%m%d-%H%M%S).log"

exec > >(tee -a "${LOG_FILE}") 2>&1

log() { printf '\n\033[1;36m==> [%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }
ok() { printf '\033[32m    OK\033[0m %s\n' "$*"; }
warn() { printf '\033[33m    WARN\033[0m %s\n' "$*"; }
fail() { printf '\033[31m    FAIL\033[0m %s\n' "$*"; FAILED=$((FAILED + 1)); }
phase() {
  PHASE=$((PHASE + 1))
  log "phase ${PHASE}: $*"
}

run() {
  echo "+ $*"
  "$@"
}

run_soft() {
  echo "+ $*  (soft)"
  if "$@"; then
    return 0
  fi
  local rc=$?
  if [[ "${KEEP_GOING_ON_CHAOS}" == "1" ]]; then
    warn "command exited ${rc} (continuing; chaos phases may violate Gatling SLOs)"
    return 0
  fi
  return "${rc}"
}

cleanup_chaos() {
  log "cleanup: clearing chaos side-effects"
  make chaos-dynamodb-latency-clear >/dev/null 2>&1 || true
  make chaos-dynamodb-recover >/dev/null 2>&1 || true
  make chaos-redis-recover >/dev/null 2>&1 || true
}
trap cleanup_chaos EXIT

wait_http() {
  local url="$1"
  local timeout_sec="${2:-180}"
  local label="${3:-${url}}"
  local deadline=$((SECONDS + timeout_sec))
  while ((SECONDS < deadline)); do
    if curl -sf "${url}" >/dev/null 2>&1; then
      ok "${label} ready"
      return 0
    fi
    sleep 2
  done
  fail "${label} not ready within ${timeout_sec}s"
  return 1
}

sample_account_id() {
  python3 - <<'PY'
import json
from pathlib import Path
ids = json.loads(Path("src/gatling/resources/account-ids.json").read_text())
print(ids[0]["accountId"])
PY
}

print_banner() {
  cat <<EOF

╔══════════════════════════════════════════════════════════════╗
║  Itaú Balance API — reviewer demo pipeline                   ║
║  profile=${DEMO_PROFILE}  cache=${CACHE_ENABLED}  skip_boot=${SKIP_BOOTSTRAP}     ║
╚══════════════════════════════════════════════════════════════╝

This run exercises:
  • stack boot with SigNoz (OTLP metrics/traces/logs)
  • DynamoDB seed for Gatling accounts
  • Kafka ingest load + HTTP GET load (mixed)
  • chaos: poison→DLT, DynamoDB latency, Redis down, async retry recovery
  • final healthy traffic so dashboards settle green

SigNoz UI : ${SIGNOZ_URL}
App       : ${BASE_URL}
Log       : ${LOG_FILE}

EOF
}

print_summary() {
  local elapsed=$(( $(date +%s) - STARTED_AT ))
  local account
  account="$(sample_account_id 2>/dev/null || echo '<accountId>')"
  cat <<EOF

╔══════════════════════════════════════════════════════════════╗
║  DEMO COMPLETE  (${elapsed}s)   failures=${FAILED}                        ║
╚══════════════════════════════════════════════════════════════╝

Open SigNoz → ${SIGNOZ_URL}
  Dashboards (last 15–30 min):
    • Balance — Ingestion   (transactions saved/ignored/retried/dlt, PutItem)
    • Balance — API         (GET latency, 2xx/4xx/5xx, cache hit/miss if on)
    • Balance — Health/JVM  (CB state, lag, heap, readiness)

Quick curls:
  curl -s ${BASE_URL}/actuator/health | jq .
  curl -s ${BASE_URL}/balances/${account} | jq .

Kafka topics:
  make kafka-consume TOPIC=transacoes-financeiras-processadas.retry-1
  make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT

Gatling reports: build/reports/gatling/
Pipeline log:    ${LOG_FILE}

Tips:
  SKIP_BOOTSTRAP=1 make review-demo   # stack already up
  DEMO_PROFILE=full make review-demo  # longer load windows
  SKIP_CHAOS=1 make review-demo       # load only
  make review-demo-quick              # alias DEMO_PROFILE=quick

EOF
  if ((FAILED > 0)); then
    echo "Pipeline finished with ${FAILED} soft/hard failure(s)." >&2
    return 1
  fi
}

# --- phases ---

print_banner

phase "bootstrap (obs stack + cache)"
if [[ "${SKIP_BOOTSTRAP}" == "1" ]]; then
  warn "SKIP_BOOTSTRAP=1 — assuming stack is already up"
else
  run env BALANCE_CACHE_ENABLED="${CACHE_ENABLED}" make obs-up
fi
run wait_http "${BASE_URL}/actuator/health/liveness" 240 "app liveness"
run wait_http "${BASE_URL}/actuator/health/readiness" 120 "app readiness" || warn "readiness not green yet (continuing)"
# SigNoz UI can take a few minutes on first boot; don't hard-fail
if curl -sf "${SIGNOZ_URL}" >/dev/null 2>&1; then
  ok "SigNoz UI reachable"
else
  warn "SigNoz UI not reachable yet at ${SIGNOZ_URL} (OTLP may still be ingested)"
fi

phase "seed Gatling account balances into DynamoDB"
run make load-seed
ACCOUNT_ID="$(sample_account_id)"
ok "sample account ${ACCOUNT_ID}"
code="$(curl -s -o /dev/null -w '%{http_code}' "${BASE_URL}/balances/${ACCOUNT_ID}" || true)"
if [[ "${code}" == "200" ]]; then
  ok "GET /balances/${ACCOUNT_ID} → 200"
else
  warn "GET /balances/${ACCOUNT_ID} → ${code} (seed may still be propagating)"
fi

phase "warm-up traffic (small kafka + http)"
run make load-ingest INGEST_COUNT=30
run_soft make load-test PROFILE=custom VUS=3 DURATION=10s RAMP=2s \
  BASE_URL="${BASE_URL}" CACHE_MODE="${CACHE_MODE_LABEL}" \
  P99_MS=5000 MAX_FAIL_PCT=5

if [[ "${SKIP_LOAD}" != "1" ]]; then
  phase "baseline mixed load (kafka write + GET read)"
  run make load-mixed \
    WORKERS="${MIXED_WORKERS}" \
    DURATION="${MIXED_DURATION}" \
    CACHE_MODE="${CACHE_MODE_LABEL}" \
    BASE_URL="${BASE_URL}"

  phase "HTTP baseline (closed model)"
  run make load-test \
    PROFILE=custom \
    VUS="${HTTP_VUS}" \
    DURATION="${HTTP_DURATION}" \
    RAMP="${HTTP_RAMP}" \
    BASE_URL="${BASE_URL}" \
    CACHE_MODE="${CACHE_MODE_LABEL}" \
    P99_MS=3000 \
    MAX_FAIL_PCT=2
else
  warn "SKIP_LOAD=1"
fi

if [[ "${SKIP_CHAOS}" != "1" ]]; then
  phase "chaos: poison message → DLT"
  run make chaos-poison-dlt
  sleep 3
  ok "poison published (check DLT topic / balance.transactions result=dlt)"

  phase "chaos: DynamoDB latency under HTTP load (CB may open → 503)"
  run make chaos-dynamodb-latency LATENCY_MS=2000 JITTER_MS=400
  run_soft make load-test \
    PROFILE=custom \
    VUS="${CHAOS_HTTP_VUS}" \
    DURATION="${CHAOS_HTTP_DURATION}" \
    RAMP=3s \
    BASE_URL="${BASE_URL}" \
    CACHE_MODE=on \
    P99_MS=30000 \
    MAX_FAIL_PCT=100
  run make chaos-dynamodb-latency-clear
  log "waiting ${CB_COOLDOWN_SEC}s for circuit breakers to recover"
  sleep "${CB_COOLDOWN_SEC}"
  run wait_http "${BASE_URL}/actuator/health/readiness" 90 "readiness after latency" || true

  phase "chaos: Redis stop (cache fail-open — GET must keep working)"
  if [[ "${CACHE_ENABLED}" == "true" ]]; then
    run make chaos-redis-stop
    run_soft make load-test \
      PROFILE=custom \
      VUS="${CHAOS_HTTP_VUS}" \
      DURATION="${CHAOS_HTTP_DURATION}" \
      RAMP=3s \
      BASE_URL="${BASE_URL}" \
      CACHE_MODE=on \
      P99_MS=5000 \
      MAX_FAIL_PCT=5
    run make chaos-redis-recover
    sleep 3
  else
    warn "cache disabled — skipping redis chaos"
  fi

  phase "chaos: DynamoDB pause → async retry topics → recover"
  run_soft make chaos-retry-topics COUNT="${RETRY_COUNT}"
else
  warn "SKIP_CHAOS=1"
fi

phase "final healthy traffic (dashboards settle green)"
run make load-kafka WORKERS="${KAFKA_WORKERS}" DURATION="${KAFKA_DURATION}" PROFILE=custom
run make load-test \
  PROFILE=custom \
  VUS="${HTTP_VUS}" \
  DURATION="${HTTP_DURATION}" \
  RAMP="${HTTP_RAMP}" \
  BASE_URL="${BASE_URL}" \
  CACHE_MODE="${CACHE_MODE_LABEL}" \
  P99_MS=3000 \
  MAX_FAIL_PCT=2

cleanup_chaos
trap - EXIT

print_summary
