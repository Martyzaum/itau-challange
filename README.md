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
- [Capacity plan](docs/CAPACITY.md)
- [Load tests](docs/LOAD.md)
- [Chaos local](docs/CHAOS.md)
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
| Cache (opcional) | Redis cache-aside (`BALANCE_CACHE_ENABLED`) |
| Containers | Docker multi-stage + Compose |

## Arquitetura

Hexagonal (ports & adapters):

```text
Kafka  → adapter/input/kafka  → ProcessTransactionEventUseCase → AccountBalanceRepository → DynamoDB
HTTP   → adapter/input/web    → GetAccountBalanceUseCase       → AccountBalanceProvider  → DynamoDB
                                                                      └─ (opcional) Redis cache-aside
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
| `make load-seed` / `load-test` | Gatling GET `/balances` (manual; fora do `check`) |
| `make load-kafka` / `load-mixed` | Load ingest Kafka (+ misto com GET) |
| `make http` | roda `http/*.http` |

## Testes

```bash
./gradlew check           # unitários + gate 90%
make integration-test     # DynamoDB + Kafka + E2E

# Load — requer stack up; NÃO entra no check/CI gate
make load-seed
make load-smoke                              # GET: 1 VU / 15s
make load-test VUS=50 DURATION=1m RAMP=15s    # GET k6-like
make load-kafka WORKERS=4 DURATION=1m         # Kafka ingest load
make load-mixed WORKERS=4 DURATION=30s        # GET + Kafka juntos
```

Detalhes: [`docs/LOAD.md`](docs/LOAD.md).

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
| Métricas OTLP | **gRPC** → collector `:4317` (Micrometer → OTel bridge) |
| Traces OTLP | **gRPC** → collector `:4317` |
| Logs | JSON stdout + **OTLP/gRPC** `:4317` quando `make obs-up` |

Counters: `balance.transactions{result}`, `balance.queries{result}`.

Spans: HTTP (MVC), Kafka listener, DynamoDB `GetItem`/`PutItem`.

No Compose padrão o export OTLP vem **desligado**. `make obs-up` liga tudo (sem env extra).

### SigNoz local (opcional)

```bash
make obs-up    # app + infra + SigNoz; OTLP on automaticamente
# UI: http://localhost:3301
make obs-down
```

Detalhes: [`infra/signoz/README.md`](infra/signoz/README.md).

## Decisões (resumo)

1. Snapshot autoritativo por timestamp (não recalcula saldo).  
2. Escrita atômica condicional no DynamoDB.  
3. PK só `account_id` — acesso O(1) sem GSI.  
4. Payload inválido → DLT; falha técnica → retry/backoff → DLT.  
5. Leitura fortemente consistente no GET.  
6. Observabilidade OTLP-friendly (métricas + traces + logs JSON).  

Detalhes: [`docs/DECISIONS.md`](docs/DECISIONS.md)  
Operação: [`docs/PRODUCTION.md`](docs/PRODUCTION.md)  
Load: [`docs/LOAD.md`](docs/LOAD.md)

## Entrega

Repositório público na branch `kotlin`.  
Não incluir o PDF do desafio no repo.
