# Chaos experiments (local Compose)

Manual drills against the local stack. **Not** a CI gate.

Prereq:

```bash
make up          # or make up-cache / make obs-up
```

Each target prints what to observe. Restore with the matching `*-recover` target (or `make up`).

---

## Scenarios

### 1. DynamoDB pause

```bash
make chaos-dynamodb-pause
# GET /balances/* and Kafka ingest should fail/slow (store down)
# With CB (future): GET → 503; today: errors / retries
make chaos-dynamodb-recover
```

### 2. Redis stop (cache on)

```bash
make up-cache
make chaos-redis-stop
# GET must keep working (fail-open → DynamoDB only)
# logs: balance_cache_get_failed / put_failed
make chaos-redis-recover
```

### 3. Kafka / Redpanda stop

```bash
make chaos-kafka-stop
# ingest stops; GET still works if DynamoDB has data
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

Requires `TRANSACTIONS_INGESTION_ENABLED` (feature-flags PR) or set env manually:

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
| DLT | `make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT` |
| SigNoz | `make obs-up` → http://localhost:3301 |
| Lag | Redpanda console :8081 |

---

## Safety

- Local only; pauses/stops **Compose** services for this project.
- Always run the matching recover target before leaving the machine.
