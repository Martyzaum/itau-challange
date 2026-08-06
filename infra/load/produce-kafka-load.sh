#!/bin/bash
# High-throughput Kafka load for transacoes-financeiras-processadas.
# Uses fixed accounts from account-ids.json (same feeder as Gatling GET).
#
# Env:
#   TOPIC              default transacoes-financeiras-processadas
#   BROKERS            default redpanda:9092
#   ACCOUNT_IDS_JSON   path to [{ "accountId": "..." }, ...]
#   WORKERS            parallel producers
#   DURATION           k6-style: 30, 30s, 1m, 2m30s
#   RPS                optional total target msgs/s (split across workers); empty = max
#   BATCH_SIZE         lines per rpk produce call (default 50)
#   ELIGIBLE_PCT       0-100 % APPROVED+ENABLED (default 100)
#   PROFILE            smoke|load|stress|spike|custom
set -euo pipefail

TOPIC="${TOPIC:-transacoes-financeiras-processadas}"
BROKERS="${REDPANDA_BROKERS:-${BROKERS:-redpanda:9092}}"
JSON="${ACCOUNT_IDS_JSON:-/gatling-resources/account-ids.json}"
BATCH_SIZE="${BATCH_SIZE:-50}"
ELIGIBLE_PCT="${ELIGIBLE_PCT:-100}"
PROFILE="${PROFILE:-custom}"

# Capture caller overrides before profile defaults.
WORKERS_IN="${WORKERS-}"
DURATION_IN="${DURATION-}"
RPS_IN="${RPS-}"

case "${PROFILE}" in
  smoke)
    DEF_WORKERS=1
    DEF_DURATION=15s
    ;;
  load)
    DEF_WORKERS=4
    DEF_DURATION=1m
    ;;
  stress)
    DEF_WORKERS=8
    DEF_DURATION=2m
    ;;
  spike)
    DEF_WORKERS=12
    DEF_DURATION=30s
    ;;
  custom | "")
    DEF_WORKERS=4
    DEF_DURATION=30s
    ;;
  *)
    echo "Unknown PROFILE=${PROFILE} (smoke|load|stress|spike|custom)"
    exit 1
    ;;
esac

WORKERS="${WORKERS_IN:-$DEF_WORKERS}"
DURATION_RAW="${DURATION_IN:-$DEF_DURATION}"
RPS="${RPS_IN-}"

parse_duration_seconds() {
  local s
  s="$(echo "$1" | tr '[:upper:]' '[:lower:]' | tr -d ' ')"
  if [[ "$s" =~ ^[0-9]+$ ]]; then
    echo "$s"
    return
  fi
  local h=0 m=0 sec=0
  [[ "$s" =~ ([0-9]+)h ]] && h="${BASH_REMATCH[1]}"
  [[ "$s" =~ ([0-9]+)m ]] && m="${BASH_REMATCH[1]}"
  [[ "$s" =~ ([0-9]+)s ]] && sec="${BASH_REMATCH[1]}"
  if [[ $((h + m + sec)) -eq 0 ]]; then
    echo "Invalid DURATION='$1' (use 30, 30s, 1m, 2m30s)" >&2
    exit 1
  fi
  echo $((h * 3600 + m * 60 + sec))
}

DURATION_SECS="$(parse_duration_seconds "$DURATION_RAW")"

if [[ ! -f "$JSON" ]]; then
  echo "JSON not found: $JSON"
  exit 1
fi

mapfile -t ACCOUNT_IDS < <(grep -oE '"accountId"[[:space:]]*:[[:space:]]*"[^"]+"' "$JSON" | sed -E 's/.*"([^"]+)"[[:space:]]*$/\1/')
if [[ ${#ACCOUNT_IDS[@]} -eq 0 ]]; then
  echo "No accountId in ${JSON}"
  exit 1
fi

if ! [[ "$WORKERS" =~ ^[1-9][0-9]*$ ]]; then
  echo "WORKERS must be a positive integer (got '$WORKERS')"
  exit 1
fi

uuid() {
  if [[ -r /proc/sys/kernel/random/uuid ]]; then
    cat /proc/sys/kernel/random/uuid
  else
    printf '%08x-%04x-%04x-%04x-%012x\n' \
      "$RANDOM$RANDOM" "$RANDOM" "$RANDOM" "$RANDOM" "$RANDOM$RANDOM$RANDOM"
  fi
}

owner_for() {
  printf '00000000-0000-4000-8000-%012d' "$1"
}

now_us() {
  date +%s%6N 2>/dev/null || echo $(($(date +%s) * 1000000))
}

STATS_DIR="$(mktemp -d)"
trap 'rm -rf "$STATS_DIR"' EXIT

produce_worker() {
  local worker_id=$1
  local end_ts=$2
  local rate_per_worker=$3
  local n_accounts=${#ACCOUNT_IDS[@]}
  local seq=0
  local base_us
  base_us="$(now_us)"
  local ts_pad=$((worker_id * 1000000000))
  local batch_lines=()
  local window_start
  window_start=$(date +%s)
  local window_count=0

  flush_batch() {
    if [[ ${#batch_lines[@]} -eq 0 ]]; then
      return
    fi
    printf '%s\n' "${batch_lines[@]}" | rpk topic produce "${TOPIC}" --brokers "${BROKERS}" -f '%k\t%v\n' >/dev/null
    batch_lines=()
  }

  while (( $(date +%s) < end_ts )); do
    local idx=$((seq % n_accounts))
    local account_id="${ACCOUNT_IDS[$idx]}"
    local owner
    owner="$(owner_for "$idx")"
    local tx_id
    tx_id="$(uuid)"
    local ts=$((base_us + ts_pad + seq))
    local amount
    amount="$(awk -v s="$seq$worker_id" 'BEGIN { srand(s); printf "%.2f", 10 + rand() * 5000 }')"
    local tx_amount
    tx_amount="$(awk -v s="$seq" 'BEGIN { srand(s + 1); printf "%.2f", 0.01 + rand() * 500 }')"
    local type="CREDIT"
    (( seq % 2 == 0 )) && type="DEBIT"

    local status="APPROVED"
    local account_status="ENABLED"
    if (( ELIGIBLE_PCT < 100 )); then
      local roll=$((RANDOM % 100))
      if (( roll >= ELIGIBLE_PCT )); then
        if (( roll % 2 == 0 )); then
          status="DECLINED"
        else
          account_status="DISABLED"
        fi
      fi
    fi

    local payload
    payload=$(printf '{"transaction":{"id":"%s","type":"%s","amount":%s,"currency":"BRL","status":"%s","timestamp":%s},"account":{"id":"%s","owner":"%s","created_at":%s,"status":"%s","balance":{"amount":%s,"currency":"BRL"}}}' \
      "$tx_id" "$type" "$tx_amount" "$status" "$ts" \
      "$account_id" "$owner" "$((base_us - 86400000000))" "$account_status" "$amount")

    batch_lines+=("${account_id}	${payload}")
    seq=$((seq + 1))
    window_count=$((window_count + 1))

    if (( ${#batch_lines[@]} >= BATCH_SIZE )); then
      flush_batch
    fi

    if [[ -n "$rate_per_worker" && "$rate_per_worker" != "0" ]]; then
      local now
      now=$(date +%s)
      if (( now > window_start )); then
        window_start=$now
        window_count=0
      fi
      if (( window_count >= rate_per_worker )); then
        flush_batch
        while (( $(date +%s) <= window_start )); do
          sleep 0.01
        done
        window_start=$(date +%s)
        window_count=0
      fi
    fi
  done

  flush_batch
  echo "$seq" >"${STATS_DIR}/w${worker_id}"
  echo "worker=${worker_id} produced=${seq}"
}

echo "Kafka load → topic=${TOPIC} brokers=${BROKERS}"
echo "  profile=${PROFILE} workers=${WORKERS} duration=${DURATION_RAW} (${DURATION_SECS}s) rps=${RPS:-max} batch=${BATCH_SIZE} accounts=${#ACCOUNT_IDS[@]} eligible_pct=${ELIGIBLE_PCT}"

end_ts=$(( $(date +%s) + DURATION_SECS ))
rate_per_worker=""
if [[ -n "${RPS}" && "${RPS}" != "0" ]]; then
  if ! [[ "$RPS" =~ ^[1-9][0-9]*$ ]]; then
    echo "RPS must be a positive integer (got '$RPS')"
    exit 1
  fi
  rate_per_worker=$((RPS / WORKERS))
  if (( rate_per_worker < 1 )); then
    rate_per_worker=1
  fi
fi

pids=()
for ((w = 0; w < WORKERS; w++)); do
  produce_worker "$w" "$end_ts" "$rate_per_worker" &
  pids+=($!)
done

failed=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    failed=1
  fi
done

total=0
for f in "${STATS_DIR}"/w*; do
  [[ -f "$f" ]] || continue
  total=$((total + $(cat "$f")))
done

echo "Kafka load complete: produced_total=${total} workers=${WORKERS} duration=${DURATION_SECS}s"
if (( failed != 0 )); then
  exit 1
fi
