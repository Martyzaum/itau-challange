# Decisões de arquitetura

Registro das decisões principais da solução de consulta de saldo.

## 1. Arquitetura hexagonal

**Decisão:** manter ports & adapters do starter-kit (`domain` → `port` → `application` → `adapter`).

**Motivo:** domínio testável sem infra; aderência ao template avaliado; troca de adapters sem reescrever regras.

**Enforcement:** `HexagonalArchitectureTest` (Konsist).

## 2. Snapshot autoritativo (sem recalcular saldo)

**Decisão:** o campo `account.balance` do evento Kafka é a fonte da verdade. A aplicação **não** aplica CREDIT/DEBIT localmente.

**Motivo:** o autorizador já processou a transação e publicou o saldo atual; recalcular abriria divergência.

## 3. Versão composta: timestamp + transaction id

**Decisão:** a versão do snapshot é o par:
- `updated_at_micros` ← `transaction.timestamp`
- `last_transaction_id` ← `transaction.id`

**Motivo:**
- Mensagens fora de ordem / at-least-once (Kafka).
- O autorizador **não** garante timestamp único por conta: duas txs distintas podem colidir no mesmo µs.
- Só timestamp faria a segunda tx no empate ser descartada indevidamente.

**Regra de escrita (DynamoDB):**
```
attribute_not_exists(account_id)
OR updated_at_micros < :newTs
OR (
  updated_at_micros = :newTs
  AND (attribute_not_exists(last_transaction_id) OR last_transaction_id < :newTxId)
)
```

| Caso | Resultado |
|------|-----------|
| Conta nova | grava |
| Timestamp maior | grava |
| Timestamp menor | ignora (stale) |
| Ts igual + mesmo `transaction.id` | ignora (redelivery / dedupe) |
| Ts igual + `transaction.id` maior (string) | grava (desempate determinístico) |
| Ts igual + `transaction.id` menor | ignora |

**Notas:**
- Comparação de UUID é **lexicográfica** (`toString()`): estável entre workers, não cronológica.
- `last_transaction_id` é interno (não exposto no GET REST).
- Foco: **dedupe** de reentrega + **não perder** tx distinta no empate de µs.

## 4. Modelagem DynamoDB

**Decisão:**
- Tabela `AccountBalances`
- PK `account_id` (S)
- Sem sort key e sem GSI
- Billing on-demand
- Atributos: `owner`, `balance_amount`, `balance_currency`, `updated_at_micros`, `last_transaction_id`

**Motivo:** único acesso exigido é por `accountId` (`GetItem` O(1)).

**Leitura:** `consistentRead=true` para refletir o último write bem-sucedido.

## 5. Elegibilidade de evento

**Decisão:**
- Válido + `APPROVED` + conta `ENABLED` → tenta persistir
- `DECLINED` ou `DISABLED` → ignora com sucesso (`false`)
- Payload inválido (JSON/UUID/domínio) → falha definitiva → DLT (sem retry)

## 6. Kafka

**Decisão:**
- Tópico: `transacoes-financeiras-processadas` (3 partições)
- DLT: `transacoes-financeiras-processadas.DLT`
- Group: `balance-transaction-consumer`
- Retry: `DefaultErrorHandler` + exponential backoff (3 tentativas)
- Producers de teste/seed usam key = `accountId` (ordenação por conta na partição)

**Trade-off:** retry síncrono atrasa a partição durante o backoff; aceitável no escopo do desafio. Evolução: retry topics assíncronos.

## 7. REST

**Decisão:**
- `GET /balances/{accountId}`
- Path tipado como `UUID` → 400 estável
- `AccountBalanceNotFoundException` → 404 estável
- `updated_at` em ISO 8601 (`America/Sao_Paulo`)
- OpenAPI estático em `src/main/resources/static/openapi.yaml`

## 8. Observabilidade

**Decisão:**
- Logs JSON (logstash) em stdout — pipeline OTEL via collector/filelog
- Métricas Micrometer → bridge `opentelemetry-micrometer-1.5` → **OTLP/gRPC** `:4317`
- Tracing OpenTelemetry → **OTLP/gRPC** `:4317`
- Logs: JSON logstash no **stdout** + export **OTLP/gRPC** `:4317` (Logback OpenTelemetryAppender; off no compose default)
- Counters: `balance.transactions{result}`, `balance.queries{result}`
- Spans: HTTP (MVC), Kafka listener, DynamoDB GetItem/PutItem
- Health: liveness processo; readiness DynamoDB `DescribeTable`
- Sampling default `1.0`; export off no Compose padrão e nos testes
- SigNoz opcional: `make obs-up` (overlay já seta envs da app — sem config manual)

## 9. Credenciais AWS

**Decisão:**
- Com `dynamodb.endpoint` preenchido → DynamoDB Local + credenciais `local/local`
- Sem endpoint → AWS real + `DefaultCredentialsProvider`

## 10. Load test Gatling fora do `check`

**Decisão:** simulações Gatling (`src/gatling`) rodam só via `./gradlew gatlingRun` / `make load-test`. Não entram em `check` nem CI gate.

**Motivo:** precisam de stack live; duração e flakiness de rede local não devem quebrar o gate de cobertura.

**Cache on/off:** `CACHE_MODE` é label de relatório; com cache na app, compara-se dois runs relabelados.

