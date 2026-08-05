# API de Consulta de Saldo — Desafio Itaú

[![Build](../../actions/workflows/build.yml/badge.svg)](../../actions/workflows/build.yml)
[![Test & Coverage](../../actions/workflows/test.yml/badge.svg)](../../actions/workflows/test.yml)
[![Docker](../../actions/workflows/docker.yml/badge.svg)](../../actions/workflows/docker.yml)
[![CodeQL](../../actions/workflows/codeql.yml/badge.svg)](../../actions/workflows/codeql.yml)

Solução do desafio técnico Itaú Unibanco: **ingestão de transações financeiras via Kafka**, persistência do saldo mais atual no **DynamoDB** e exposição via **REST**.

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
- [Decisões](#decisões)

## Stack

| Categoria | Tecnologia |
|-|-|
| Linguagem | Kotlin 2.3 / Java 21 |
| Framework | Spring Boot 4.1 |
| Persistência | Amazon DynamoDB (AWS SDK v2) |
| Mensageria | Kafka via Spring Kafka (Redpanda local) |
| Testes | JUnit 5, MockMvc, Konsist, integração Docker |
| Cobertura | JaCoCo ≥ 90% |
| Observabilidade | Logs JSON, Micrometer → OTLP, Actuator health |

## Arquitetura

Hexagonal (ports & adapters):

```text
Kafka  → adapter/input/kafka  → ProcessTransactionEventUseCase → AccountBalanceRepository → DynamoDB
HTTP   → adapter/input/web    → GetAccountBalanceUseCase       → AccountBalanceProvider  → DynamoDB
```

Regra de dependência validada por `HexagonalArchitectureTest` (Konsist).

## Fluxo

1. O autorizador publica eventos em `transacoes-financeiras-processadas`.
2. O consumer valida o payload, aplica elegibilidade (`APPROVED` + `ENABLED`) e persiste o **snapshot de saldo** se o timestamp for mais novo (`saveIfNewer`).
3. `GET /balances/{accountId}` devolve o saldo mais atual com `updated_at` em ISO 8601.

O saldo do evento é **autoritativo** — a aplicação não recalcula CREDIT/DEBIT.

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

**400** — UUID inválido (`INVALID_ACCOUNT_ID`)  
**404** — conta inexistente (`ACCOUNT_BALANCE_NOT_FOUND`)

OpenAPI: [`src/main/resources/static/openapi.yaml`](src/main/resources/static/openapi.yaml)  
Exemplos HTTP: [`http/balances.http`](http/balances.http)

## Kafka

| Item | Valor |
|-|-|
| Tópico | `transacoes-financeiras-processadas` (3 partições) |
| DLT | `transacoes-financeiras-processadas.DLT` |
| Group | `balance-transaction-consumer` |
| Retry | exponential backoff (3 tentativas) |
| Not-retryable | JSON/UUID/domínio inválidos → DLT direto |

Produzir eventos de teste:
```bash
make kafka-produce-transactions-events TOPIC=transacoes-financeiras-processadas COUNT=50
```

## DynamoDB

| Item | Valor |
|-|-|
| Tabela | `AccountBalances` |
| PK | `account_id` (S) |
| Atributos | `owner`, `balance_amount`, `balance_currency`, `updated_at_micros` |
| Escrita | `PutItem` condicional: `attribute_not_exists(account_id) OR updated_at_micros < :new` |
| Leitura | `GetItem` com `consistentRead=true` |

## Como rodar

Pré-requisito: Docker + Docker Compose.

```bash
make up          # app + DynamoDB + Redpanda
make logs
curl "http://localhost:8080/balances/<accountId>"
make stop
```

Desenvolvimento rápido (IDE):
```bash
make db-up
make kafka-up
./gradlew bootRun
```

## Makefile

| Comando | Descrição |
|-|-|
| `make up` / `make stop` | sobe/desce a stack |
| `make db-up` | DynamoDB Local + tabela |
| `make kafka-up` | Redpanda + tópicos |
| `make kafka-produce-transactions-events TOPIC=...` | eventos de teste |
| `make test` | unitários + cobertura em container |
| `make integration-test` | integração com infra real |
| `make http` | roda `http/*.http` |

## Testes

```bash
./gradlew check              # unitários + gate 90%
make integration-test        # Kafka + DynamoDB reais
```

Cobertura de: domínio, concurrency/out-of-order/duplicate, REST, consumer, health, métricas.

## Observabilidade

| Recurso | Endpoint / destino |
|-|-|
| Liveness | `GET /actuator/health/liveness` |
| Readiness | `GET /actuator/health/readiness` (inclui DynamoDB) |
| Métricas | Micrometer → OTLP (`/v1/metrics`) |
| Logs | JSON estruturado (logstash) em stdout |

Counters: `balance.transactions{result}`, `balance.queries{result}`.

## Decisões

- Snapshot autoritativo por `transaction.timestamp` (microssegundos).
- Timestamp igual = duplicado; menor = stale (não sobrescreve).
- Endpoint DynamoDB opcional + default credential chain na AWS.
- Retry síncrono por partição; DLT para falhas definitivas/esgotadas.
- Circuit breaker / feature flags documentados como evolução (não no MVP).
