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
| Testes de integração | Sim (DynamoDB + Kafka + E2E) |
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
| `TRANSACTIONS_RETRY_*` | 500ms / 2.0 / 5s / 3 | Backoff |
| `MANAGEMENT_OTLP_METRICS_EXPORT_ENABLED` | `true` (app) / `false` (compose) | Export métricas OTLP |
| `MANAGEMENT_OTLP_METRICS_EXPORT_URL` | `http://localhost:4318/v1/metrics` | Métricas OTLP **HTTP** (Micrometer) |
| `MANAGEMENT_TRACING_ENABLED` | `true` / `false` (compose/test) | Liga tracing |
| `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | `1.0` | Sample rate (0.0–1.0) |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `http://localhost:4317` | Traces OTLP **gRPC** |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_TRANSPORT` | `grpc` | Transporte traces |

## Deploy sugerido

1. Provisionar tabela DynamoDB (`account_id` HASH, on-demand).
2. Criar tópicos Kafka (entrada + DLT), partições conforme throughput.
3. Rodar com IAM role (sem `DYNAMODB_ENDPOINT`).
4. Apontar OTLP para o collector do ambiente.
5. Probes:
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
| DynamoDB indisponível | Retry com backoff → DLT se esgotar |
| Conta inexistente no GET | 404 JSON estável |
| UUID inválido no path | 400 JSON estável |
| Concorrência no mesmo `account_id` | Condição atômica no DynamoDB |

## Limitações conhecidas / próximos passos

- Circuit breaker ainda não implementado (candidato natural no adapter DynamoDB).
- Feature flags não implementadas (ex.: pausar ingestão).
- Retry é síncrono por partição (lag sob falha prolongada).
- Readiness não exige Kafka up (GET saldo pode continuar se o store estiver ok).
- Compose padrão mantém OTLP off; stack SigNoz opcional via `make obs-up` (`infra/signoz/`).
- DLT requer processo operacional de reprocessamento/manual inspect.

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
