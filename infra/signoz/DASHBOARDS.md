# Dashboards SigNoz

Provisionados no start do `query-service` via `DASHBOARDS_PATH=/root/config/dashboards` (volume `infra/signoz/dashboards` em `docker-compose.yml`).

## Abrir a UI

```bash
make obs-up          # app + infra + SigNoz (OTLP)
# ou, se o stack já estiver no ar:
make obs-ui          # imprime a URL
```

UI: **http://localhost:3301** → menu **Dashboards**.

Primeiro boot pode levar 1–3 min (ClickHouse + migrator). Janela SQL dos painéis: **últimas 6h**. Serviço filtrado: `itau-balance-api`.

## Arquivos (todos em `infra/signoz/dashboards/`)

| Dashboard (título na UI) | Arquivo | O que mostra |
|--------------------------|---------|--------------|
| **Balance — Overview** | `balance-overview.json` | Snapshot E2E: saved/retry/DLT, API found, cache hit/miss, p99 GET/PutItem/GetItem, lag, error spans |
| **Balance — Ingestion** | `balance-ingestion.json` | Kafka ingest: `saved` / `ignored_ineligible` / `ignored_not_newer` / `retried` / `dlt`; PutItem p50/p99; spans de erro |
| **Balance — API** | `balance-api.json` | GET found/not_found; HTTP p50/p99; GetItem p50/p99; HTTP error spans |
| **Balance — Cache (Redis)** | `balance-cache.json` | hit / miss / `put_failed`; CB `redis` (state + calls). Exige cache ligado |
| **Balance — Resilience** | `balance-resilience.json` | CB `dynamodb-read` / `dynamodb-write` / `redis` / `kafka-produce`; not-permitted; lag; containers; retry/DLT |
| **Balance — Errors & degradation** | `balance-errors.json` | DLT, retries, spans de erro/5xx, not_found, put_failed, CB open, PutItem vs GetItem errors |
| **Balance — Health / JVM** | `balance-health.json` | `jvm.*`, `process.cpu.usage`, taxa de spans, `kafka.consumer.listener.containers` |

## Como popular com tráfego

**Caminho completo (recomendado para review):**

```bash
make review-demo              # DEMO_PROFILE=quick (default): obs-up + seed + load + chaos
make review-demo-quick        # alias quick (~5–8 min com stack quente)
make review-demo-full         # janelas maiores
# stack já com OTLP:
SKIP_BOOTSTRAP=1 make review-demo-quick
# só carga (sem chaos):
SKIP_BOOTSTRAP=1 SKIP_CHAOS=1 make review-demo-quick
```

**Manual (pontual):**

```bash
make obs-up
make up-cache                 # painéis de cache (hit/miss/put_failed + CB redis)
make load-seed                # contas Gatling no DynamoDB → GET 200
make load-ingest              # produce no tópico principal (COUNT via INGEST_COUNT)
make load-kafka PROFILE=smoke # carga Kafka
make load-test PROFILE=smoke  # Gatling GET /balances
make load-mixed               # Kafka + HTTP em paralelo

# produce avulso:
make kafka-produce-transactions-events TOPIC=transacoes-financeiras-processadas COUNT=100

# chaos (opcional — sobe retry/DLT/CB):
make chaos-poison-dlt
make chaos-dynamodb-pause     # depois: make chaos-dynamodb-recover
make chaos-dynamodb-latency   # depois: make chaos-dynamodb-latency-clear
make chaos-redis-stop         # depois: make chaos-redis-recover
```

Aguarde **~30–60s** o export OTLP e faça hard-refresh na UI.

## Sinais usados nas queries

| Sinal | Origem |
|-------|--------|
| `balance.transactions` (`saved` \| `ignored_ineligible` \| `ignored_not_newer` \| `retried` \| `dlt`) | `BalanceMetrics` |
| `balance.queries` (`found` \| `not_found`) | `BalanceMetrics` |
| `balance.cache` (`hit` \| `miss` \| `put_failed`) | `BalanceMetrics` |
| `resilience4j.circuitbreaker.state` / `.calls` / `.not.permitted.calls` | Resilience4j Micrometer |
| `kafka.consumer.listener.containers` | gauge custom (`KafkaConsumerMetricsConfig`) |
| métricas com nome `*lag*` / `*records*lag*` | binder Micrometer do client Kafka (se presente) |
| spans `dynamodb.get_item` / `dynamodb.put_item` | Observations → traces |
| span `http get /balances/{accountId}` | Spring MVC observation |
| `jvm.memory.*` / `jvm.threads.live` / `jvm.gc.pause` / `process.cpu.usage` | Micrometer JVM / process |

## Painéis vazios — checklist

1. **Sem tráfego** — rode `review-demo` ou `load-*` / `kafka-produce-*`; dashboards não inventam dados.
2. **OTLP off** — use `make obs-up` (overlay SigNoz). Confirme collector em `:4317`/`:4318`.
3. **Cache** — `balance-cache` fica vazio sem `make up-cache` (ou `CACHE_ENABLED=true` no `review-demo`).
4. **Lag** — só aparece se o consumer estiver ativo e o binder de lag do Kafka exportar métrica; query usa `metric_name LIKE '%lag%'`.
5. **Labels CB** — queries aceitam `name` ou `resilience4j.circuitbreaker` (+ fallback `positionCaseInsensitive`).
6. **Janela / refresh** — SQL fixo em 6h; hard-refresh após ~30–60s do export.
7. **Serviço** — filtros usam `service.name` / `serviceName` = `itau-balance-api` (`spring.application.name`).

Painel vazio ≠ dashboard inválido na maioria dos casos: falta sinal ou export, não JSON quebrado.
