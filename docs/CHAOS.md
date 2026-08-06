# Chaos experiments (local Compose)

Manual drills against the local stack. **Not** a CI gate.

Prereq:

```bash
make up          # or make up-cache / make obs-up
```

Each target prints what to observe. Restore with the matching `*-recover` target (or `make up`).

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

## Scenarios

### 1. DynamoDB pause

```bash
make chaos-dynamodb-pause
# GET /balances/* and Kafka ingest should fail/slow (store down)
# With short SDK timeouts: failures route to ….retry-N
# After enough failures: CB open → GET 503 DEPENDENCY_UNAVAILABLE
make chaos-dynamodb-recover
```

### 1b. DynamoDB latency (Toxiproxy)

App traffic goes through Toxiproxy (`:8000` host → proxy → DynamoDB). Seed/admin also use the proxy.

```bash
make chaos-dynamodb-latency              # LATENCY_MS=2000 JITTER_MS=500
# slow GetItem/PutItem; CB may open → GET 503
# metrics: resilience4j.circuitbreaker.* name=dynamodb
make chaos-dynamodb-latency-clear
```

Toxiproxy API: http://localhost:8474

### 2. Redis stop (cache on)

```bash
make up-cache
make chaos-redis-stop
# GET must keep working (fail-open → DynamoDB only)
# logs: balance_cache_get_failed / put_failed; after enough fails CB redis opens
# open redis CB: balance_cache_circuit_open (still no 503)
make chaos-redis-recover
```

### 3. Kafka / Redpanda stop

```bash
make chaos-kafka-stop
# ingest stops; GET still works if DynamoDB has data
# DLT/retry publish protected by CB kafka-produce (no hammer when open)
# lag grows on consumer group when broker returns
make chaos-kafka-recover
make kafka-seed   # ensure topics exist after full recreate
```

### 4. Poison → DLT

```bash
make chaos-poison-dlt
# publishes invalid JSON to main topic
# expect message on transacoes-financeiras-processadas.DLT (no long retry)
make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT
```

### 5. Ingestion flag off

```bash
make chaos-ingestion-flag-off
# consumer bean absent; new Kafka events not processed
# GET still serves existing balances
make chaos-ingestion-flag-recover
```

---

## Observe

| Signal | Where |
|--------|--------|
| App logs | `make logs` — `event=...` |
| Health | `curl localhost:8080/actuator/health` |
| Retry topics | `make kafka-consume TOPIC=transacoes-financeiras-processadas.retry-1` |
| DLT | `make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT` |
| SigNoz | `make obs-up` → http://localhost:3301 |
| Lag | Redpanda console :8081 |

---

## Safety

- Local only; pauses/stops **Compose** services for this project.
- Always run the matching recover target before leaving the machine.
