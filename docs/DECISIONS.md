# Decisões de arquitetura

ADRs da API de consulta de saldo. Identificadores técnicos em inglês como no código.

---

## 1. Arquitetura hexagonal (ports & adapters)

**Decisão:** camadas `domain` → `port` → `application` → `adapter`.

| Camada | Responsabilidade |
|--------|------------------|
| `domain` | `AccountBalance`, `TransactionEvent`, `Balance`, `SnapshotVersion`, elegibilidade |
| `port.input` | `GetAccountBalanceUseCase`, `ProcessTransactionEventUseCase` |
| `port.output` | `AccountBalanceRepository.saveIfNewer`, `AccountBalanceProvider.findByAccountId`, `AccountBalanceCache` |
| `application` | orquestra use cases sem I/O concreto |
| `adapter` | Kafka, REST, DynamoDB, Redis, métricas/health |

**Motivo:** domínio testável sem infra; troca de adapters sem reescrever regras.

**Enforcement:** `HexagonalArchitectureTest` (Konsist) — `domain` sem Spring; `application` não depende de `adapter`.

---

## 2. Snapshot autoritativo (sem recalcular saldo)

**Decisão:** o campo `account.balance` do evento Kafka é a fonte da verdade. A aplicação **não** aplica CREDIT/DEBIT localmente.

**Fluxo:** `ProcessTransactionEventService` monta `AccountBalance` a partir do evento e chama `saveIfNewer`.

**Motivo:** o autorizador já processou a transação e publicou o saldo atual; recalcular poderia abriria divergência.

---

## 3. Versão composta: `(updated_at_micros, last_transaction_id)`

**Decisão:** versão do snapshot = par:

- `updated_at_micros` ← `transaction.timestamp` (µs)
- `last_transaction_id` ← `transaction.id` (UUID)

Ordenação canônica em `SnapshotVersion.isNewerThan` (espelhada no Dynamo condition e no Lua Redis):

1. timestamp maior vence  
2. timestamp igual → `last_transaction_id` lexicograficamente maior (`toString()`) vence  
3. par idêntico → redelivery / dedupe  

**Motivo:**

- Kafka at-least-once e mensagens fora de ordem  
- autorizador **não** garante timestamp único por conta (colisão de µs)  
- só timestamp descartaria indevidamente a segunda tx no empate  

**Condition DynamoDB (`CONDITION_SAVE_IF_NEWER`):**

```
attribute_not_exists(#accountId)
OR #updatedAt < :newUpdatedAt
OR (
  #updatedAt = :newUpdatedAt
  AND (attribute_not_exists(#lastTxId) OR #lastTxId < :newLastTxId)
)
```

| Caso | Resultado |
|------|-----------|
| Conta nova | grava (`true`) |
| Timestamp maior | grava |
| Timestamp menor | ignora stale (`false` / `ConditionalCheckFailedException`) |
| Ts igual + mesmo `transaction.id` | ignora (redelivery) |
| Ts igual + `transaction.id` maior | grava (desempate) |
| Ts igual + `transaction.id` menor | ignora |

**Notas:**

- Comparação UUID é **lexicográfica**, estável entre workers, **não** cronológica  
- `last_transaction_id` é interno (não exposto no GET REST)  
- Sem tabela `processed_tx`: dedupe + newest-wins pela versão composta  

---

## 4. Modelagem DynamoDB

**Decisão:**

- Tabela `AccountBalances`
- PK `account_id` (S); sem SK; sem GSI
- Billing on-demand (`PAY_PER_REQUEST`)
- Atributos: `owner`, `balance_amount`, `balance_currency`, `updated_at_micros`, `last_transaction_id`

**Leitura:** `GetItem` com `consistentRead` configurável (`DYNAMODB_CONSISTENT_READ`, default `true`).

**Motivo:** único acesso exigido é por `accountId` (O(1)).

**Escrita:** `PutItem` condicional (`saveIfNewer`); `ConditionalCheckFailedException` → `false` (não é erro de negócio).

---

## 5. Elegibilidade de evento

**Decisão** (`TransactionEvent.isEligibleForBalanceUpdate`):

| Entrada | Resultado |
|---------|-----------|
| Válido + status `APPROVED` + conta `ENABLED` | tenta `saveIfNewer` |
| `DECLINED` / `REJECTED` ou conta `DISABLED` | `IgnoredIneligible` (ack com sucesso) |
| Payload inválido (JSON / UUID / domínio / NPE de payload) | falha definitiva → DLT (sem retry) |
| Snapshot de saldo ≤ 0 | **aceito** (authorizer decide) |
| Amount da **transação** ≤ 0 | inválido (`InvalidTransactionEventException`) |

**Dinheiro:** `Balance` normaliza scale ISO 4217 (`HALF_EVEN`); igualdade monetária via `compareTo`.

**Resultados de aplicação:** `Saved` | `IgnoredIneligible` | `IgnoredNotNewer`.

---

## 6. Kafka — retry async multi-tópico + DLT + backoff no retry listener

**Decisão:**

| Item | Valor |
|------|--------|
| Main | `transacoes-financeiras-processadas` |
| Retry | `{main}.retry-1..N` (`N = transactions.retry.max-attempts`, default 3) |
| DLT | `transacoes-financeiras-processadas.DLT` |
| Group | `balance-transaction-consumer` |
| Listeners | main + `#{@transactionRetryTopics}` no mesmo path de processamento |

**Error handler (main path):** `DefaultErrorHandler` + `FixedBackOff(0L, 0L)` — zero retries in-place no main (libera a partição na hora).

**Recoverer assíncrono:**

1. Falha técnica no main/retry → publica no próximo hop (`retry-1` … `retry-N`)  
2. Esgotou níveis **ou** exceção não-retryable → DLT  
3. Headers: `RETRY_ATTEMPT`, `RETRY_FAILED_AT_MS`, `RETRY_NOT_BEFORE_MS` (só hops), `DLT_ORIGINAL_TOPIC`, exception FQCN/message  
4. Publish protegido pelo CB `kafka-produce`  

**Backoff temporal:** exp + full jitter (`initial-interval-ms=1000`, `multiplier=2`, `max-interval-ms=30000`). O recoverer grava `x-retry-not-before-ms`; o **retry listener** faz `Thread.sleep` até o deadline. Main **não** dorme.

**Não-retryable (payload/domínio):** `JacksonException`, `InvalidTransactionEventException`, `InvalidBalanceException`, `InvalidAccountBalanceException`, `DeserializationException`.  
**Retryable:** falhas técnicas, inclusive `NPE`/`IAE` inesperados (não mandar bug de código direto pro DLT).

**Motivo:** main livre de HOL; espaçamento temporal nos hops evita martelar Dynamo em outage curto.

**Limite:** sleep no retry listener ainda ocupa thread daquele hop — ver `LIMITATIONS.md`. Evolução: delayed topics / `@RetryableTopic`.

**Keys:** producers de teste/seed usam key = `accountId`. `saveIfNewer` garante newest-wins mesmo cross-partition.

**Kill switch:** `TRANSACTIONS_INGESTION_ENABLED=false` desliga os listeners.

---

## 7. REST

**Decisão:**

- `GET /balances/{accountId}` — path tipado `UUID` → 400 `INVALID_ACCOUNT_ID`
- `AccountBalanceNotFoundException` → 404 `ACCOUNT_BALANCE_NOT_FOUND`
- `DependencyUnavailableException` → 503 `DEPENDENCY_UNAVAILABLE` + `Retry-After: 30`
- `updated_at` em ISO-8601 offset (`America/Sao_Paulo`) a partir de `updatedAtMicros`
- `Cache-Control: no-store`
- OpenAPI estático: `src/main/resources/static/openapi.yaml`

---

## 8. Auth API key + rate limit (edge mínimo)

**Decisão:**

- Auth lab on por default: header `X-API-Key`; keys em `api-keys.json` + CSV `API_AUTH_KEYS`
- Rate limit opcional in-memory, janela fixa 60s (`API_RATE_LIMIT_ENABLED`, N/min)
- Default **on** no lab (keys arquivo); rate-limit off; `make up-secure` liga rate-limit
- Públicos: `/actuator/**`, `/openapi.yaml`, `/error`
- Comparação de key em tempo constante (`MessageDigest.isEqual`)

**Motivo:** saldo é sensível (IDOR se aberto). Em produção: mTLS/JWT no gateway + rate limit distribuído.

---

## 9. Observabilidade

**Decisão:**

- Logs JSON (logstash) em stdout **e** OTLP/gRPC (`OpenTelemetryAppender`) — mesmo plano que metrics/traces (`:4317`); desligável por env / Compose sem collector
- Métricas Micrometer → bridge `opentelemetry-micrometer-1.5` → OTLP/gRPC `:4317`
- Tracing OpenTelemetry → OTLP/gRPC `:4317`
- Counters: `balance.transactions{result=saved|ignored_ineligible|ignored_not_newer|retried|dlt}`, `balance.queries{result=found|not_found}`, `balance.cache{result=hit|miss|put_failed}`
- Spans: HTTP (MVC), Kafka listener, DynamoDB GetItem/PutItem
- Health: liveness de processo; readiness via `DynamoDbHealthIndicator` (`DescribeTable`)
- Sampling default `1.0`; export OTLP **off** no Compose padrão e nos testes
- SigNoz opcional: `make obs-up`

---

## 10. Credenciais AWS

**Decisão:**

- Com `dynamodb.endpoint` preenchido → DynamoDB Local + credenciais estáticas `local`/`local`
- Sem endpoint → AWS real + `DefaultCredentialsProvider`
- Timeouts de client: `api-call-timeout-ms` / `api-call-attempt-timeout-ms`

---

## 11. Load test Gatling fora do `check`

**Decisão:** simulações em `src/gatling` rodam só via `./gradlew gatlingRun` / `make load-test`. Não entram em `check` nem CI gate.

**Motivo:** precisam de stack live; duração/rede local não devem quebrar cobertura.

**Cache on/off:** `CACHE_MODE` é label de relatório Gatling; o toggle real é `BALANCE_CACHE_ENABLED`.

---

## 12. Cache Redis multi-worker (cache-aside, desativável)

**Decisão:** cache é feature de **primeira classe** (port `AccountBalanceCache` + decorators), **ligada por default**. Desligável por env (`balance.cache.enabled` / `BALANCE_CACHE_ENABLED=false` / `make up-no-cache`) — kill switch / trade-off de custo, não “extra opcional”.

| Operação | Comportamento |
|----------|----------------|
| GET | Redis → miss → DynamoDB GetItem → `putIfNewer` |
| Write | DynamoDB `saveIfNewer` **primeiro**; Redis `putIfNewer` **somente se `saved=true`** |
| Put Redis falha / CB open após save | `invalidate` (DEL) da key — evita hit com saldo velho |
| Stale/duplicate no Dynamo | **não** escreve no cache |

- Versão no cache = par `(updatedAtMicros, lastTransactionId)` via script **Lua** atômico (`PUT_IF_NEWER_LUA_SCRIPT`)
- **Fail-open:** erro/CB open em get → miss → DynamoDB (nunca 503 só por cache)
- Cliente **Lettuce** direto (sem Spring Data Redis autoconfig) — beans só com cache on
- TTL default 300s (`BALANCE_CACHE_TTL_SECONDS`); chave `{prefix}{uuid}` (default `balance:account:`)

**Motivo:** reduzir GetItem sob leitura pesada multi-instância; DynamoDB permanece SoT e gate atômico de escrita.

---

## 13. Feature flags só por env

| Flag | Env | Default |
|------|-----|---------|
| ingestion | `TRANSACTIONS_INGESTION_ENABLED` | `true` |
| cache | `BALANCE_CACHE_ENABLED` | `true` |
| DynamoDB consistent read | `DYNAMODB_CONSISTENT_READ` | `true` |
| API auth | `API_AUTH_ENABLED` | `true` (keys: `api-keys.json`) |
| API rate limit | `API_RATE_LIMIT_ENABLED` | `false` |
| retry hops | `TRANSACTIONS_RETRY_MAX_ATTEMPTS` | `3` |

**Motivo:** kill switch e trade-offs de consistência/custo sem Unleash/Flagsmith. Mudança exige recreate da app.

---

## 14. Circuit breakers (Resilience4j) + bulkhead de leitura

**Decisão:** CBs nomeados em `CircuitBreakerNames`:

| CB | Uso | Open / falha |
|----|-----|----------------|
| `dynamodb-read` | GetItem | `DependencyUnavailableException` → GET **503** |
| `dynamodb-write` | `saveIfNewer` | exceção técnica → hop retry / DLT |
| `redis` | cache get/put/invalidate | **fail-open** / bypass; **nunca** 503 |
| `kafka-produce` | publish retry/DLT | recoverer falha; não martela broker |

- `SdkException` Dynamo e `CallNotPermittedException` → `DependencyUnavailableException` (`executeAndTranslateOpen`)
- Isolamento: tempestade de PutItem não derruba GET (e vice-versa)
- Config: `RESILIENCE_CB_*`; drills via Toxiproxy no Compose
- Métricas: `resilience4j.circuitbreaker.*` + counters de negócio

**Bulkhead de leitura (GET DynamoDB):** `Semaphore` (`DYNAMODB_READ_BULKHEAD_MAX_CONCURRENT` default 64, timeout `DYNAMODB_READ_BULKHEAD_TIMEOUT_MS` default 50ms). Sem permissão → `DependencyUnavailableException` (503), sem enfileirar threads HTTP.

**Motivo:** failure domains distintos para leitura, ingestão, cache (acelerador) e publish de falha.
