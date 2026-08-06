# Dashboards — balance API

SigNoz UI: **http://localhost:3301** (after `make obs-up`).

Dashboards below are **recipes** (metric names + panel intent). Create them in  
**Dashboards → + New dashboard** (SigNoz JSON format varies by version; recipes stay stable).

Service filter: `service.name = itau-balance-api` (or `application = itau-balance-api` on Micrometer tags).

## 1. Ingestion (`balance-ingestion`)

| Panel | Type | Query / source |
|-------|------|----------------|
| Transactions saved/s | rate | metric `balance.transactions` where `result=saved` |
| Transactions ignored/s | rate | metric `balance.transactions` where `result=ignored` |
| Ignore ratio | formula | ignored / (saved + ignored) |
| Consume latency | histogram/p99 | span `kafka.consume` / listener (if present) or `dynamodb.put_item` |
| Errors on consume | traces | error spans under service |

## 2. API (`balance-api`)

| Panel | Type | Query / source |
|-------|------|----------------|
| Queries found/s | rate | `balance.queries` `result=found` |
| Queries not_found/s | rate | `balance.queries` `result=not_found` |
| HTTP GET latency p95/p99 | histogram | span `http.server` / `GET /balances/{id}` |
| HTTP error rate | rate | status ≥ 500 |
| DynamoDB GetItem p99 | histogram | span `dynamodb.get_item` |

## 3. Health / RED (`balance-health`)

| Panel | Type | Query / source |
|-------|------|----------------|
| Request rate | rate | HTTP server spans |
| Error rate | rate | failed spans |
| Duration p99 | histogram | HTTP + DynamoDB |
| JVM memory (if exported) | gauge | `jvm.memory.used` |
| Collector up | — | SigNoz services list |

## Traces to open first

1. **Kafka path:** produce a transaction → find span chain `kafka` → `dynamodb.put_item`.
2. **HTTP path:** `GET /balances/{id}` → child `dynamodb.get_item`.
3. Correlate via `traceId` in app JSON logs (MDC when span active).

## Metric names (app)

```
balance.transactions{result=saved|ignored}
balance.queries{result=found|not_found}
```

Micrometer common tags: `application=itau-balance-api`, `service=balance-api`.
