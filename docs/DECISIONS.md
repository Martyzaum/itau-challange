# Decisões de arquitetura

Registro das decisões principais da solução de consulta de saldo.

## 1. Arquitetura hexagonal

**Decisão:** manter ports & adapters do starter-kit (`domain` → `port` → `application` → `adapter`).

**Motivo:** domínio testável sem infra; aderência ao template avaliado; troca de adapters sem reescrever regras.

**Enforcement:** `HexagonalArchitectureTest` (Konsist).

## 2. Snapshot autoritativo (sem recalcular saldo)

**Decisão:** o campo `account.balance` do evento Kafka é a fonte da verdade. A aplicação **não** aplica CREDIT/DEBIT localmente.

**Motivo:** o autorizador já processou a transação e publicou o saldo atual; recalcular abriria divergência.

## 3. Timestamp como versão

**Decisão:** `transaction.timestamp` (microssegundos) vira `updated_at_micros` e define a versão do snapshot.

**Motivo:** mensagens podem chegar fora de ordem ou duplicadas (at-least-once).

**Regra de escrita (DynamoDB):**
```
attribute_not_exists(account_id) OR updated_at_micros < :newUpdatedAt
```

| Caso | Resultado |
|------|-----------|
| Conta nova | grava |
| Timestamp maior | grava |
| Timestamp igual | ignora (duplicado) |
| Timestamp menor | ignora (stale) |

## 4. Modelagem DynamoDB

**Decisão:**
- Tabela `AccountBalances`
- PK `account_id` (S)
- Sem sort key e sem GSI
- Billing on-demand
- Atributos: `owner`, `balance_amount`, `balance_currency`, `updated_at_micros`

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
- Métricas Micrometer exportadas por **OTLP/HTTP**
- Counters: `balance.transactions{result}`, `balance.queries{result}`
- Health:
  - liveness: processo up
  - readiness: DynamoDB/`AccountBalances` acessível (`DescribeTable`)

## 9. Credenciais AWS

**Decisão:**
- Com `dynamodb.endpoint` preenchido → DynamoDB Local + credenciais `local/local`
- Sem endpoint → AWS real + `DefaultCredentialsProvider`

## 10. Fora do MVP (evoluções conscientes)

Documentadas para a avaliação, não implementadas de propósito:

| Item | Motivador |
|------|-----------|
| Circuit breaker no DynamoDB | Evitar martelar dependência DOWN |
| Feature flags | Rollout gradual / kill switch de ingestão |
| Retry topics assíncronos | Não bloquear partição no backoff |
| Tracing OTLP | Correlacionar Kafka → DDB → HTTP |
| Kafka no readiness | Fail-fast se ingestão for crítica ao tráfego |
| Dedupe por `transaction.id` | Complementar ao timestamp em edge cases |
