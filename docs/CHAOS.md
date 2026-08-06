# Chaos experiments (local Compose)

Manual drills against the local stack. **Not** a CI gate.

```bash
make up   # app must be running (this branch includes async retry topics)
```

---

## Automated: DynamoDB down → retry topics → recovery

Validates Kafka **async retry** end-to-end with real infra:

1. Pause DynamoDB  
2. Produce valid transaction events  
3. Assert account keys appear on `….retry-1`  
4. Unpause DynamoDB  
5. Assert `GET /balances/{id}` returns **200** after retry consumers run  

Requires short DynamoDB SDK timeouts (defaults: 5s / 3s via `DYNAMODB_API_CALL_*_TIMEOUT_MS`).
Without them, `docker pause dynamodb` freezes TCP and the consumer hangs instead of failing into retry.

```bash
# optional: short retry delays so recovery is faster
TRANSACTIONS_RETRY_INITIAL_INTERVAL_MS=1000 \
TRANSACTIONS_RETRY_MULTIPLIER=1.0 \
TRANSACTIONS_RETRY_MAX_INTERVAL_MS=1000 \
make up --build

make chaos-retry-topics
# COUNT=5 WAIT_RETRY_SEC=90 make chaos-retry-topics
```

Script: `infra/chaos/validate-retry-topics.sh`.

---

## Manual drills

### DynamoDB pause only
```bash
make chaos-dynamodb-pause    # if available on chaos PR
docker compose pause dynamodb
docker compose unpause dynamodb
```

### Poison → DLT
```bash
printf 'poison\t{not-json\n' | docker compose run --rm -T --entrypoint bash redpanda-seed -c \
  "rpk topic produce transacoes-financeiras-processadas --brokers redpanda:9092 -f '%k\t%v\n'"
make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT
```

### Redis stop (cache on)
```bash
make up-cache
docker compose stop redis
# GET should still work (fail-open)
docker compose start redis
```

---

## Observe

| Signal | Where |
|--------|--------|
| App logs | `make logs` |
| Retry topics | `make kafka-consume TOPIC=transacoes-financeiras-processadas.retry-1` |
| DLT | `make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT` |
| Health | `curl localhost:8080/actuator/health` |
