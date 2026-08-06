# API de Consulta de Saldo — Desafio Itaú

[![Build](../../actions/workflows/build.yml/badge.svg)](../../actions/workflows/build.yml)
[![Test & Coverage](../../actions/workflows/test.yml/badge.svg)](../../actions/workflows/test.yml)
[![Docker](../../actions/workflows/docker.yml/badge.svg)](../../actions/workflows/docker.yml)
[![CodeQL](../../actions/workflows/codeql.yml/badge.svg)](../../actions/workflows/codeql.yml)

Solução do desafio técnico Itaú Unibanco:

1. **Ingestão** — consumir `transacoes-financeiras-processadas` (Kafka) e persistir o saldo mais atual no DynamoDB  
2. **Exposição** — `GET /balances/{accountId}`

Documentação adicional:

- [Decisões de arquitetura](docs/DECISIONS.md)
- [Production readiness](docs/PRODUCTION.md)
- [OpenAPI](src/main/resources/static/openapi.yaml)

## Sumário

- [Stack](#stack)
- [Arquitetura](#arquitetura)
- [Fluxo](#fluxo)
- [API](#api)
- [Kafka](#kafka)
- [DynamoDB](#dynamodb)
- [Como rodar](#como-rodar)
- [Makefile](#makefile)
- [Testes](#testes)
- [Observabilidade](#observabilidade)
- [Decisões (resumo)](#decisões-resumo)
- [Evoluções futuras](#evoluções-futuras)

## Stack

| Categoria | Tecnologia |
|-|-|
| Linguagem | Kotlin 2.3 / Java 21 |
| Framework | Spring Boot 4.1 |
| Persistência | Amazon DynamoDB (AWS SDK v2) |
| Mensageria | Kafka (Spring Kafka) + Redpanda local |
| Testes | JUnit 5, MockMvc, Konsist, integração Docker |
| Cobertura | JaCoCo ≥ 90% |
| Observabilidade | Logs JSON, Micrometer métricas+tracing → OTLP, Actuator health |
| Containers | Docker multi-stage + Compose |

## Arquitetura

Hexagonal (ports & adapters):

```text
Kafka  → adapter/input/kafka  → ProcessTransactionEventUseCase → AccountBalanceRepository → DynamoDB
HTTP   → adapter/input/web    → GetAccountBalanceUseCase       → AccountBalanceProvider  → DynamoDB
```

```text
src/main/kotlin/br/com/itau/challenge/
├── Application.kt
├── config/                 # DynamoDbClient (local vs AWS)
└── balance/
    ├── domain/             # modelos + invariantes
    ├── port/               # input/output contracts
    ├── application/        # use cases
    └── adapter/
        ├── input/kafka/    # consumer + DTO + retry/DLT
        ├── input/web/      # REST + errors
        ├── output/dynamodb/# GetItem / saveIfNewer
        └── observability/  # metrics + health
```

Regra de dependência validada por `HexagonalArchitectureTest` (Konsist).

## Fluxo

1. Autorizador publica eventos em `transacoes-financeiras-processadas`.
2. Consumer desserializa, valida, aplica elegibilidade (`APPROVED` + `ENABLED`).
3. Persiste snapshot se a versão `(timestamp, transaction.id)` for mais nova (`saveIfNewer` atômico).
4. `GET /balances/{accountId}` lê com consistência forte e devolve ISO 8601.

O saldo do evento é **autoritativo** — a app não recalcula CREDIT/DEBIT.

## API

### `GET /balances/{accountId}`

**200**
```json
{
  "id": "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",
  "owner": "315e3cfe-f4af-4cd2-b298-a449e614349a",
  "balance": { "amount": 183.12, "currency": "BRL" },
  "updated_at": "2025-07-04T12:02:44.589998-03:00"
}
```

| Status | Código | Quando |
|--------|--------|--------|
| 400 | `INVALID_ACCOUNT_ID` | UUID inválido |
| 404 | `ACCOUNT_BALANCE_NOT_FOUND` | conta sem saldo persistido |

Exemplos: [`http/balances.http`](http/balances.http)

## Kafka

| Item | Valor |
|-|-|
| Tópico | `transacoes-financeiras-processadas` (3 partições) |
| DLT | `transacoes-financeiras-processadas.DLT` |
| Group | `balance-transaction-consumer` |
| Retry | exponential backoff (3x; 500ms → x2 → max 5s) |
| Not-retryable | JSON/UUID/domínio inválidos → DLT |

```bash
make kafka-produce-transactions-events TOPIC=transacoes-financeiras-processadas COUNT=50
```

Producers de teste usam **key = accountId** para favorecer ordenação por conta na partição.

## DynamoDB

| Item | Valor |
|-|-|
| Tabela | `AccountBalances` |
| PK | `account_id` (S) |
| SK / GSI | nenhum |
| Escrita | `PutItem` condicional por timestamp |
| Leitura | `GetItem` + `consistentRead=true` |

Condição:
```
attribute_not_exists(account_id) OR updated_at_micros < :newUpdatedAt
```

## Como rodar

Pré-requisito: Docker + Compose.

```bash
make up       # app + DynamoDB + Redpanda (seeds aguardados)
make logs
curl "http://localhost:8080/actuator/health/liveness"

# gerar eventos e consultar
make kafka-produce-transactions-events TOPIC=transacoes-financeiras-processadas COUNT=20
make db-scan
make stop
```

IDE / loop rápido:
```bash
make db-up
make kafka-up
./gradlew bootRun
```

Consoles locais:

| Serviço | URL |
|-|-|
| API | http://localhost:8080 |
| DynamoDB Admin | http://localhost:8001 |
| Redpanda Console | http://localhost:8081 |

## Makefile

| Comando | Descrição |
|-|-|
| `make up` / `stop` / `logs` | stack completa |
| `make db-up` / `db-scan` | DynamoDB Local |
| `make kafka-up` | Redpanda + tópicos |
| `make kafka-produce-transactions-events TOPIC=...` | eventos de teste |
| `make test` | unitários + cobertura (container) |
| `make integration-test` | integração real |
| `make http` | roda `http/*.http` |

## Testes

```bash
./gradlew check           # unitários + gate 90%
make integration-test     # DynamoDB + Kafka + E2E
```

Cobertura principal:

- domínio (validações, elegibilidade)
- concorrência / duplicado / stale no DynamoDB
- REST 200/400/404
- consumer + retry classification + DLT destination
- health probes
- E2E Kafka → DynamoDB → REST

## Observabilidade

| Recurso | Onde |
|-|-|
| Liveness | `GET /actuator/health/liveness` |
| Readiness | `GET /actuator/health/readiness` (DynamoDB) |
| Métricas OTLP | `management.otlp.metrics.export.url` → `/v1/metrics` |
| Traces OTLP | `management.otlp.tracing.endpoint` → `/v1/traces` |
| Logs | JSON (logstash) em stdout (+ `traceId`/`spanId` no MDC quando houver span) |

Counters: `balance.transactions{result}`, `balance.queries{result}`.

Spans: HTTP (MVC), Kafka listener, DynamoDB `GetItem`/`PutItem`.

No Compose local o export OTLP (métricas **e** traces) vem **desligado** para não spammar collector inexistente.

## Decisões (resumo)

1. Snapshot autoritativo por timestamp (não recalcula saldo).  
2. Escrita atômica condicional no DynamoDB.  
3. PK só `account_id` — acesso O(1) sem GSI.  
4. Payload inválido → DLT; falha técnica → retry/backoff → DLT.  
5. Leitura fortemente consistente no GET.  
6. Observabilidade OTLP-friendly (métricas + traces + logs JSON).  

Detalhes: [`docs/DECISIONS.md`](docs/DECISIONS.md)  
Operação: [`docs/PRODUCTION.md`](docs/PRODUCTION.md)

## Evoluções futuras

Itens conscientes **fora do MVP**, com motivadores:

| Item | Motivador |
|------|-----------|
| Circuit breaker no DynamoDB | Evitar storm de calls quando a store está DOWN |
| Feature flags | Kill switch de ingestão / rollout gradual |
| Retry topics assíncronos | Não bloquear partição durante backoff |
| SigNoz / collector local | UI de traces e métricas |
| Kafka no readiness | Fail-fast se ingestão for requisito do tráfego |

## Entrega

Repositório público na branch `kotlin`.  
Não incluir o PDF do desafio no repo.
