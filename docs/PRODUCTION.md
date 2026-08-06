# Production readiness

Checklist operacional da API de saldo.

## Runtime

| Item | Status |
|------|--------|
| Container multi-stage (JRE 21) | Sim (`Dockerfile`) |
| Config por env vars | Sim |
| Sem secrets no código | Sim |
| Health liveness/readiness | Sim (`/actuator/health/*`) |
| Logs estruturados JSON | Sim |
| Métricas OTLP | Sim (desligado no compose local) |
| Cobertura unitária ≥ 90% | Sim (JaCoCo gate) |
| Testes de integração | Sim (DynamoDB + Kafka + Redis cache E2E) |
| Load test (Gatling) | Manual (`make load-test`) — fora do `check`/CI gate |
| CI (build/test/docker/codeql) | Sim (GitHub Actions) |

## Variáveis de ambiente

| Variável | Default local | Descrição |
|----------|---------------|-----------|
| `DYNAMODB_ENDPOINT` | `http://localhost:8000` | Vazio = AWS real |
| `DYNAMODB_REGION` | `us-east-1` | Região |
| `ACCOUNT_BALANCES_TABLE_NAME` | `AccountBalances` | Tabela |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Brokers |
| `KAFKA_CONSUMER_GROUP_ID` | `balance-transaction-consumer` | Consumer group |
| `TRANSACTIONS_TOPIC` | `transacoes-financeiras-processadas` | Tópico de entrada |
| `TRANSACTIONS_DLT_TOPIC` | `transacoes-financeiras-processadas.DLT` | Dead-letter |
| `TRANSACTIONS_RETRY_INITIAL_INTERVAL_MS` | `1000` | Delay do 1º retry topic (ms) |
| `TRANSACTIONS_RETRY_MULTIPLIER` | `5.0` | Multiplicador entre níveis |
| `TRANSACTIONS_RETRY_MAX_INTERVAL_MS` | `30000` | Teto do delay (ms) |
| `TRANSACTIONS_RETRY_MAX_ATTEMPTS` | `3` | Nº de tópicos `….retry-N` |
| `MANAGEMENT_OTLP_METRICS_EXPORT_ENABLED` | `true` (app) / `false` (compose) | Export métricas OTLP |
| `MANAGEMENT_OTLP_METRICS_EXPORT_STEP` | `30s` | Intervalo de export de métricas |
| `MANAGEMENT_TRACING_ENABLED` | `true` / `false` (compose/test) | Liga tracing |
| `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | `1.0` | Sample rate (0.0–1.0) |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `http://localhost:4317` | Traces OTLP **gRPC** |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_TRANSPORT` | `grpc` | Transporte traces |
| `MANAGEMENT_OPENTELEMETRY_METRICS_EXPORT_OTLP_ENDPOINT` | `http://localhost:4317` | Métricas OTLP **gRPC** |
| `MANAGEMENT_LOGGING_EXPORT_OTLP_ENABLED` | `false` / `true` (`obs-up`) | Export logs OTLP |
| `MANAGEMENT_OPENTELEMETRY_LOGGING_EXPORT_OTLP_ENDPOINT` | `http://localhost:4317` | Logs OTLP **gRPC** |
| `MANAGEMENT_OPENTELEMETRY_LOGGING_EXPORT_OTLP_TRANSPORT` | `grpc` | Transporte logs |
| `BALANCE_CACHE_ENABLED` | `false` | Liga cache-aside Redis |
| `BALANCE_CACHE_REDIS_HOST` | `localhost` / `redis` (compose) | Host Redis |
| `BALANCE_CACHE_REDIS_PORT` | `6379` | Porta Redis |
| `BALANCE_CACHE_REDIS_TIMEOUT_MS` | `200` | Timeout comandos |
| `BALANCE_CACHE_TTL_SECONDS` | `300` | TTL da chave (0 = sem TTL) |
| `BALANCE_CACHE_KEY_PREFIX` | `balance:account:` | Prefixo da chave |

## Cache Redis

- Default **off**. Compose sobe Redis em `:6379`; app só usa com `BALANCE_CACHE_ENABLED=true`.
- Fail-open em runtime (get/put). Com cache **on**, Redis precisa estar up no **startup** (conexão Lettuce).
- Métricas: `balance.cache{result=hit|miss}`.
- Comparar load: `BALANCE_CACHE_ENABLED=false|true` + `make load-test CACHE_MODE=off|on`.

## Deploy sugerido

1. Provisionar tabela DynamoDB (`account_id` HASH, on-demand).
2. Criar tópicos Kafka (entrada + DLT), partições conforme throughput.
3. (Opcional) Redis gerenciado se `BALANCE_CACHE_ENABLED=true`.
4. Rodar com IAM role (sem `DYNAMODB_ENDPOINT`).
5. Apontar OTLP para o collector do ambiente.
6. Probes:
   - liveness → `/actuator/health/liveness`
   - readiness → `/actuator/health/readiness`
6. Escalar horizontalmente o consumer (group id fixo; partições ≥ instâncias desejadas).

## Cenários adversos cobertos

| Cenário | Comportamento |
|---------|---------------|
| Redelivery (mesmo ts + mesmo `transaction.id`) | Ignorada (`saveIfNewer` = false) |
| Empate de µs, txs distintas | Desempate por `last_transaction_id` (string) |
| Evento fora de ordem (timestamp menor) | Ignorado |
| Evento mais novo (ts maior) | Sobrescreve atomicamente |
| `DECLINED` / conta `DISABLED` | Ignorado com sucesso |
| JSON/UUID/domínio inválido | Sem retry → DLT |
| DynamoDB indisponível | Publica em retry-1..3 (delay no tópico) → DLT se esgotar |
| Conta inexistente no GET | 404 JSON estável |
| UUID inválido no path | 400 JSON estável |
| Concorrência no mesmo `account_id` | Condição atômica no DynamoDB |

## Load test (local)

```bash
make up && make load-seed && make load-test
# cache on vs off:
# BALANCE_CACHE_ENABLED=false|true make up --build
# make load-test CACHE_MODE=off|on
```

Ver [`docs/LOAD.md`](LOAD.md).

Números de referência, SLOs e dimensionamento: [`docs/CAPACITY.md`](CAPACITY.md).

## Limitações conhecidas

- Retry assíncrono: main não dorme no backoff; retry topics aplicam delay antes de reprocessar.
- Readiness não exige Kafka up — GET saldo segue se o store estiver ok.
- Compose padrão mantém OTLP off; `make obs-up` liga SigNoz + export.
- DLT exige processo operacional de inspeção/reprocessamento.
- Gatling (`make load-test`) é manual — não é gate de CI.

## Runbook rápido

**Saldo desatualizado**
1. Lag do consumer group.
2. Mensagens no DLT.
3. Conferir se eventos mais novos estão chegando com timestamp maior.
4. Health readiness / DynamoDB.

**Pico de erros**
1. `/actuator/health` e logs JSON (`event=... result=...`).
2. Métricas `balance.transactions` / `balance.queries`.
3. Throttling DynamoDB / broker.

**Reprocessar DLT**
1. Inspecionar payload + headers de exceção no tópico DLT.
2. Corrigir causa (dado ou infra).
3. Republicar no tópico principal (ou ferramenta de replay).
