# Dashboards SigNoz

Provisionados no start do query-service (`DASHBOARDS_PATH`).

UI: http://localhost:3301 → **Dashboards**

| Dashboard | Arquivo | O que mostra |
|-----------|---------|----------------|
| **Balance — Overview** | `balance-overview.json` | Pipeline E2E: saved/ignored/retry/DLT + API + cache + lag + latências |
| **Balance — Ingestion** | `balance-ingestion.json` | Kafka process: saved, ignored, **retried**, **dlt**; PutItem p50/p99; error spans |
| **Balance — API** | `balance-api.json` | GET found/not_found; HTTP p50/p99; GetItem p50/p99; HTTP errors |
| **Balance — Cache (Redis)** | `balance-cache.json` | hit / miss / **put_failed**; CB redis |
| **Balance — Resilience** | `balance-resilience.json` | CB **dynamodb-read/write**, redis, kafka-produce; not-permitted; lag; containers |
| **Balance — Errors** | `balance-errors.json` | DLT, retries, span errors, not_found, put_failed, CB open |
| **Balance — Health / JVM** | `balance-health.json` | JVM mem/threads/GC/CPU; span rate; containers |

Janela SQL: **últimas 6h**. Serviço: `itau-balance-api`.

## Como popular

```bash
make obs-up          # SigNoz + app com OTLP on
make up-cache        # se quiser painéis de cache
# traffic
make kafka-produce-transactions-events TOPIC=transacoes-financeiras-processadas COUNT=100
for i in $(seq 1 20); do curl -s "http://localhost:8080/balances/<uuid>" >/dev/null; done
# chaos (opcional)
make chaos-dynamodb-pause   # sobe retry/DLT + CB
make chaos-dynamodb-recover
```

Aguarde ~30–60s o export OTLP (`step` de métricas) e dê hard-refresh na UI.

## Métricas / spans usados

| Sinal | Origem |
|-------|--------|
| `balance.transactions{result=saved\|ignored\|retried\|dlt}` | `BalanceMetrics` |
| `balance.queries{result=found\|not_found}` | `BalanceMetrics` |
| `balance.cache{result=hit\|miss\|put_failed}` | `BalanceMetrics` |
| `resilience4j.circuitbreaker.*` | Resilience4j Micrometer |
| `kafka.consumer.*` / lag | `MicrometerConsumerListener` |
| `kafka.consumer.listener.containers` | gauge custom |
| `dynamodb.get_item` / `dynamodb.put_item` | Observations → traces |
| `http get /balances/{accountId}` | Spring MVC observation |
| `jvm.*` | Micrometer JVM binder |

## Notas

- Labels OTLP podem variar (`name` vs `resilience4j.circuitbreaker`); queries usam fallbacks `positionCaseInsensitive`.
- Lag fino depende do consumer estar ativo e do binder Micrometer no client Kafka.
- Painéis vazios = sem tráfego/export, não necessariamente dashboard inválido.
