# Limitações e o que não é production-grade

Inventário **honesto** do que este repositório entrega versus o que um serviço de saldo em produção exigiria.
Tudo abaixo foi cruzado com `application.yaml`, `KafkaConsumerConfig`, `docker-compose.yml`, filtros de segurança e adapters de cache/Dynamo.

Não é lista de “melhorias futuras suaves”: é o que **de fato não está** (ou está de propósito no nível de lab/demo).

---

## 1. Trust model (premissas)

| Camada | Premissa real neste código |
|--------|----------------------------|
| Kafka / authorizer | O authorizer é SoT do snapshot `account.balance`. Este serviço **não** recalcula CREDIT/DEBIT. ACLs, schema registry enforcement e keying por conta no broker **não** são responsabilidade deste repo. |
| API HTTP | Auth lab **on** por default (`API_AUTH_ENABLED=true`); rate-limit **off** por default. Não há IAM, JWT, mTLS nem authZ por conta. |
| Dedupe | Par `(updated_at_micros, last_transaction_id)` na condição Dynamo + Lua Redis. **Não** existe tabela `processed_tx` nem `TransactWrite` de identity por tx. |
| Lab local | Dependências single-node, efêmeras, sem HA (ver §5). |

---

## 2. Segurança da API — não é IAM

Código: `ApiKeyAuthFilter`, `RateLimitFilter`, `ApiSecurityProperties`, `application.yaml` (`api.auth.*`, `api.rate-limit.*`).

| Fato | Detalhe no código |
|------|-------------------|
| Auth lab (não bancária) | `API_AUTH_ENABLED=true` por default; keys em `classpath:api-keys.json` (`local-dev-key`, `reviewer-key`) e/ou CSV `API_AUTH_KEYS`. Rate-limit default **off**. |
| Só API key estática | Header configurável (`X-API-Key`). Comparação constant-time (`MessageDigest.isEqual`). Sem JWT/mTLS/authZ por conta. |
| Sem IAM / roles / scopes | Não há Spring Security resource server, JWT, OAuth2, mTLS client cert, nem mapeamento key→principal. |
| Sem authZ por conta | `GET /balances/{accountId}` devolve o saldo se a key (ou a ausência de auth) permitir a rota. Quem conhece o UUID lê o saldo (**IDOR** se auth off ou key compartilhada). Campo `owner` é só dado do snapshot, não gate de autorização. |
| Actuator/OpenAPI públicos | `ApiSecurityPaths.isPublic`: `/actuator/**`, `/openapi.yaml`, `/error` **nunca** passam pelo filtro de key. Em prod o actuator tem que ficar em rede privada / auth de ops. |
| Rate limit in-memory | `ConcurrentHashMap` + janela fixa 60s **por processo**. Não sobrevive a restart; **não** é coerente entre pods. Multi-instância → gateway/Redis/envoy. |
| Demo “segura” | `make up-secure` liga key + 120 req/min — ainda é lab, não borda bancária. |

**Em produção:** autenticação obrigatória no edge (JWT/mTLS), autorização por conta/titular, rate limit distribuído, actuator isolado, secrets em vault/KMS — nada disso está neste serviço.

---

## 3. Retry Kafka — async hops + backoff no listener (não delayed-topic)

Código: recoverer publica em `….retry-1..N` ou DLT; header `x-retry-not-before-ms` com exp backoff + full jitter; retry listeners aguardam o deadline antes de processar.

| Fato | Detalhe |
|------|---------|
| Zero retry local no main | `FixedBackOff(0L, 0L)` no error handler — main topic **não** dorme. |
| Hop multi-tópico | main → `{topic}.retry-1` → `…retry-N` → `{topic}.DLT` (`transactions.retry.max-attempts`, default 3). |
| Backoff temporal | `initial-interval-ms` (1s) × `multiplier` (2) até `max-interval-ms` (30s), full jitter. Aplicado **no listener de retry** (`Thread.sleep` até `not-before`), não no recoverer/main. |
| Poison / domínio → DLT | Só `JacksonException`, `Invalid*`, `DeserializationException`. **NPE/IAE não** são not-retryable (bugs de código retentam). |
| Sem tooling de replay DLT | Inspeção via Console / `make kafka-consume`. Republicar é manual. |
| CB `kafka-produce` | Recoverer sem outbox local se produce falhar. |

**Limite restante:** sleep no retry listener ainda ocupa a thread do consumer daquele hop (HOL só nos retry topics). Evolução: delayed topics / `@RetryableTopic` / pause com deadline.

---

## 4. Cache Redis — fail-open, stale possível, on por default

Código: `RedisAccountBalanceCache`, `CachingAccountBalanceProvider`, `CachingAccountBalanceRepository`; flag `balance.cache.enabled` default **true**.

| Fato | Detalhe |
|------|---------|
| Fail-open | Erro ou CB `redis` open em **get/put/invalidate** → log + continua. Get falho = miss → Dynamo. **Nunca** 503 só por cache. |
| Write path correto no happy path | Put Redis **só** se Dynamo `saveIfNewer` retornou `true`. Put falho → `invalidateIfNotNewer` (DEL versionado) + métrica `cache_put_failed`. |
| Stale ainda é possível | (1) TTL default 300s (`BALANCE_CACHE_TTL_SECONDS`) — GET pode servir snapshot ≤ TTL após write que não atualizou aquela réplica/key a tempo. (2) Se **put falha e invalidate também falha** (CB open / rede), a key antiga pode permanecer até TTL ou DEL eventual. (3) Race multi-writer coberta pelo Lua `putIfNewer`, não por invalidação global. |
| Default on | Compose sobe Redis e a app usa cache. Kill switch: `BALANCE_CACHE_ENABLED=false` / `make up-no-cache`. |
| Startup acoplado quando on | Com cache on, cliente Lettuce precisa conectar no boot; Redis down no startup derruba a app (fail-open é **runtime**, não bootstrap). |
| Redis do compose sem durabilidade | `redis-server --save "" --appendonly no` — reinício zera cache (aceitável p/ cache; não é store). |

DynamoDB continua SoT. Cache é acelerador com trade-off de frescura consciente.

---

## 5. Dependências locais — single-node, não HA

`docker-compose.yml` é **lab**, não topologia de produção:

| Serviço | Limitação explícita no compose |
|---------|--------------------------------|
| `dynamodb` (DynamoDB Local) | `-sharedDb -inMemory` — sem disco, sem multi-AZ, sem backups/PITR, sem capacity real. |
| `redpanda` | `--mode dev-container --smp 1` — um broker, um core, sem rack-awareness/replication de prod. |
| `redis` | Uma instância, sem sentinel/cluster, sem persistência. |
| `app` | Um container; rate limit e qualquer estado de processo são single-instance. |
| Rede | Portas expostas no host (8080, 19092, 6379, …) para DX — não é perimeter de prod. |

Não há Terraform/Helm/Kustomize de deploy multi-env neste repo. Image non-root + graceful shutdown (`server.shutdown=graceful`, `timeout-per-shutdown-phase: 30s`) existem; **orquestração, rolling update, HPA, network policy não**.

---

## 6. Modelo de dados e consistência

| Limitação | Realidade no código |
|-----------|---------------------|
| Só latest snapshot | Tabela `AccountBalances`, PK `account_id`, **sem SK/GSI**. Sem histórico de transações. Evolução: SK por timestamp, OpenSearch, ou Kafka Streams. |
| Tie-break UUID lexicográfico | Empate de `updated_at_micros`: vence maior `lastTransactionId.toString()`. Determinístico entre workers; **não** é ordem causal/monotonic sequence do authorizer. |
| Sem `processed_tx` | Dedupe = mesma versão composta rejeitada pelo condition expression. Não há identity store append-only por `transaction.id`. |
| Saldo de snapshot pode ser ≤ 0 | Authorizer manda; domínio aceita. `amount` da **transação** no evento deve ser > 0. |
| Leitura default consistent | `DYNAMODB_CONSISTENT_READ=true` — custo/latência maiores; desligar é trade-off de frescura RCU. |
| Ordering cross-partition | Seed/teste keya por `accountId`. Authorizer real pode não keyar assim; `saveIfNewer` ainda impõe newest-wins, mas ordem de apply entre partições não é total. |

---

## 7. Observabilidade e ops — lab-ready, não ops-completo

| Item | Estado |
|------|--------|
| Logs JSON + OTLP traces/metrics | Sim na app; **OTLP off no compose default** (`MANAGEMENT_OTLP_*=false`). `make obs-up` liga SigNoz. |
| Sampling default | `1.0` — ok p/ demo; em prod queima volume. |
| Lag consumer | MicrometerConsumerListener (records-lag por partição). |
| Readiness | Group `readiness` = `readinessState` + `dynamoDb` (`DescribeTable`). **Não** exige Kafka up — GET pode ficar ready com ingestão parada. |
| Liveness | Processo; não prova consumer saudável. |
| Alertas / runbooks automatizados | Docs manuais (`PRODUCTION.md`); sem alert rules as code no repo de app. |
| Load (Gatling) | Manual (`make load-test`); **fora** de `check`/CI gate. |
| Chaos | Scripts locais Toxiproxy/Make — não game-day institucional. |

---

## 8. O que **já está** no código (não listar como gap)

Estes pontos existem e funcionam no nível de desafio/lab; não fingir que faltam:

- Hexagonal + testes (unit, integration, JaCoCo gate).
- `saveIfNewer` atômico no Dynamo + espelho de versão no Redis Lua.
- Retry async multi-tópico + DLT + métricas `retried`/`dlt`.
- CB separados `dynamodb-read` / `dynamodb-write` / `redis` / `kafka-produce`.
- Bulkhead de leitura Dynamo (`Semaphore` + `read-bulkhead-max-concurrent` / timeout) no GET.
- Cache-aside com put-após-save e DEL em put falho.
- Kill switch `TRANSACTIONS_INGESTION_ENABLED`.
- Dockerfile runtime non-root (uid 10001), JRE 21, graceful shutdown.
- OpenAPI estático, erros JSON estáveis (400/404/401/429/503).

---

## 9. Em produção faria (ainda não neste repo)

1. **Auth edge obrigatória** + authZ por conta; actuator privado; secrets fora de env plain em compose demo.
2. **Rate limit / WAF distribuído** no gateway — descartar o mapa in-memory como controle real.
3. **Delayed topics / non-blocking delay** se sleep no retry listener for HOL demais sob outage longo.
4. **DLT replay** (ferramenta + permissões + auditoria) e alertas de lag / DLT rate / CB open.
5. **Infra real**: Dynamo provisionado (PITR, IAM least-privilege), Kafka/MSK com ACLs e RF≥3, Redis gerenciado se cache on, multi-AZ.
6. **IaC + deploy** multi-env, rolling/HPA, network policies.
7. (Opcional) `processed_tx` / sequence monotônico do authorizer se precisarem identity/causalidade além do par `(ts, txId)`.
8. (Opcional) histórico de movimentos se o produto deixar de ser “só latest”.

---

## 10. Resumo em uma frase

É uma **API de consulta de saldo bem construída para desafio/lab** (snapshot authoritativo, newest-wins, retry multi-tópico + backoff, cache fail-open, CB split), **não** um serviço bancário production-grade: auth lab por API key (sem authZ por conta), deps single-node efêmeras, sem replay DLT nem IaC de produção.
