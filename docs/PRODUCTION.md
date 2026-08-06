# Produção e operação

Checklist prático para rodar e operar a API de saldo. Identifiers (`env`, paths, métricas) ficam em inglês como no código.

Alinhado a [`LIMITATIONS.md`](LIMITATIONS.md): o que existe no repo é **ops lab** (Compose, image, probes, CB, bulkhead). IaC, multi-env, gateway e tooling de DLT em AWS real ficam fora.

---

## 1. Runtime pronto

| Item | Onde / como |
|------|-------------|
| Image multi-stage JRE 21 | `Dockerfile` target `runtime` |
| Non-root | `USER 10001` (`appuser`) |
| Heap | `-XX:MaxRAMPercentage=75.0` |
| Graceful shutdown | `server.shutdown=graceful` + `spring.lifecycle.timeout-per-shutdown-phase=30s` |
| Config por env | `application.yaml` — sem secrets no código |
| Health probes | `/actuator/health/liveness`, `/actuator/health/readiness` |
| Logs JSON | Logstash structured console |
| Métricas / traces / **logs** OTLP | mesmo OTLP/gRPC `:4317`; compose `make up` **off**; `obs-up` **on** nos três |
| CB + bulkhead | Resilience4j CB split + bulkhead no GET Dynamo |
| Lag por partição | `MicrometerConsumerListener` → `kafka.consumer.*records.lag*` |
| Cobertura ≥ 90% | JaCoCo no `check` |
| Integração | DynamoDB Local + Redpanda + Redis (E2E) |
| Load (Gatling) | Manual: `make load-test` — **não** é gate de CI |
| CI | GitHub Actions: build / test / docker / CodeQL |

---

## 2. Graceful shutdown

1. Orquestrador envia `SIGTERM` ao container.
2. Spring para de aceitar conexões novas (`server.shutdown=graceful`).
3. Fase de shutdown espera até **30s** (`timeout-per-shutdown-phase`) para:
   - requests HTTP em voo
   - listeners Kafka drenarem o batch em processamento
4. Após o timeout, o processo encerra mesmo com trabalho pendente.

**Ops:**
- Rolling deploy: `terminationGracePeriodSeconds` (K8s) ≥ 30s (+ margem de rede).
- Readiness deve cair **antes** do SIGTERM (preStop / deregister) para não receber tráfego novo.
- Mensagens Kafka não commitadas voltam ao group — `saveIfNewer` tolera redelivery.

---

## 3. Health

| Probe | Path | Significado |
|-------|------|-------------|
| Liveness | `GET /actuator/health/liveness` | Processo vivo (reiniciar se DOWN) |
| Readiness | `GET /actuator/health/readiness` | Pronto para tráfego; inclui **DynamoDB** (`DescribeTable` em `AccountBalances`) |
| Agregado | `GET /actuator/health` | Visão geral (`show-details: when_authorized`) |

**Comportamento:**
- Readiness **não** exige Kafka up — GET de saldo segue se o store responder.
- Com `TRANSACTIONS_INGESTION_ENABLED=false`, a API continua ready (só não consome).
- Endpoints expostos default: `health,info` (`MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE`).
- Públicos mesmo com auth on: `/actuator/**`, `/openapi.yaml`, `/error`.

**Probes sugeridos (K8s / ECS):**
```text
liveness  → GET /actuator/health/liveness
readiness → GET /actuator/health/readiness
```

---

## 4. Circuit breakers e bulkhead

### CBs (Resilience4j)

Mesma config base via `RESILIENCE_CB_*` para todos. Names:

| Name | Caminho | Open |
|------|---------|------|
| `dynamodb-read` | GetItem (GET) | HTTP **503** `DEPENDENCY_UNAVAILABLE` + `Retry-After` |
| `dynamodb-write` | `saveIfNewer` (Kafka) | falha técnica → `….retry-N` → DLT |
| `redis` | cache get/put/invalidate | **fail-open** (bypass); nunca 503 |
| `kafka-produce` | publish retry/DLT | recoverer falha; não martela broker; redelivery depois |

Métricas: `resilience4j.circuitbreaker.*` (tag `name`).

### Bulkhead no GET Dynamo

Semaphore no read path (`DynamoDbAccountBalanceProvider`):

| Env | Default | Efeito |
|-----|---------|--------|
| `DYNAMODB_READ_BULKHEAD_MAX_CONCURRENT` | `64` | máx. GetItem concorrentes |
| `DYNAMODB_READ_BULKHEAD_TIMEOUT_MS` | `50` | espera pelo permit; estoura → 503 (`dynamodb-read`) |

Isola tempestade de leituras do write path e limita fan-out no Dynamo sob pico HTTP.

---

## 5. Variáveis de ambiente

Só o que existe em `application.yaml` / compose (defaults = app local).

### DynamoDB

| Variável | Default | Notas |
|----------|---------|--------|
| `DYNAMODB_ENDPOINT` | `http://localhost:8000` | **Vazio** = AWS real + `DefaultCredentialsProvider` (IAM role) |
| `DYNAMODB_REGION` | `us-east-1` | |
| `ACCOUNT_BALANCES_TABLE_NAME` | `AccountBalances` | PK `account_id` (HASH) |
| `DYNAMODB_CONSISTENT_READ` | `true` | GetItem strongly consistent |
| `DYNAMODB_API_CALL_TIMEOUT_MS` | `5000` | timeout total SDK |
| `DYNAMODB_API_CALL_ATTEMPT_TIMEOUT_MS` | `3000` | por tentativa |
| `DYNAMODB_READ_BULKHEAD_MAX_CONCURRENT` | `64` | ver §4 |
| `DYNAMODB_READ_BULKHEAD_TIMEOUT_MS` | `50` | ver §4 |

Compose local aponta Dynamo via Toxiproxy (`http://toxiproxy:8666`) para chaos de latency.

### Kafka

| Variável | Default | Notas |
|----------|---------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | compose app: `redpanda:9092` |
| `KAFKA_CONSUMER_GROUP_ID` | `balance-transaction-consumer` | fixo entre réplicas |
| `KAFKA_LISTENER_CONCURRENCY` | `3` | ≤ partições do tópico |
| `KAFKA_LISTENER_ACK_MODE` | `batch` | |
| `TRANSACTIONS_TOPIC` | `transacoes-financeiras-processadas` | |
| `TRANSACTIONS_DLT_TOPIC` | `transacoes-financeiras-processadas.DLT` | |
| `TRANSACTIONS_INGESTION_ENABLED` | `true` | kill switch do consumer |
| `TRANSACTIONS_RETRY_MAX_ATTEMPTS` | `3` | tópicos `….retry-1..N` (hops imediatos) |

### Resilience

| Variável | Default |
|----------|---------|
| `RESILIENCE_CB_FAILURE_RATE_THRESHOLD` | `50` |
| `RESILIENCE_CB_SLIDING_WINDOW_SIZE` | `20` |
| `RESILIENCE_CB_MINIMUM_NUMBER_OF_CALLS` | `10` |
| `RESILIENCE_CB_WAIT_DURATION_IN_OPEN_STATE_MS` | `30000` |
| `RESILIENCE_CB_PERMITTED_CALLS_IN_HALF_OPEN` | `5` |

### Cache Redis

| Variável | Default | Notas |
|----------|---------|--------|
| `BALANCE_CACHE_ENABLED` | `true` | on por padrão; `false` desliga |
| `BALANCE_CACHE_REDIS_HOST` | `localhost` | compose: `redis` |
| `BALANCE_CACHE_REDIS_PORT` | `6379` | |
| `BALANCE_CACHE_REDIS_TIMEOUT_MS` | `200` | |
| `BALANCE_CACHE_TTL_SECONDS` | `300` | `0` = sem TTL |
| `BALANCE_CACHE_KEY_PREFIX` | `balance:account:` | |

### Auth + rate limit (lab)

| Variável | Default | Notas |
|----------|---------|--------|
| `API_AUTH_ENABLED` | `true` | exige key em `/balances/**` |
| `API_AUTH_HEADER` | `X-API-Key` | |
| `API_AUTH_KEYS` | _(vazio)_ | CSV extra `key1,key2` |
| `API_AUTH_KEYS_FILE` | `classpath:api-keys.json` | `{"keys":["local-dev-key","reviewer-key"]}` |
| `API_RATE_LIMIT_ENABLED` | `false` | in-memory por key/IP |
| `API_RATE_LIMIT_REQUESTS_PER_MINUTE` | `120` | janela fixa 60s |

### Observabilidade

| Variável | Default app | Compose padrão |
|----------|-------------|----------------|
| `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` | `health,info` | |
| `MANAGEMENT_OTLP_METRICS_EXPORT_ENABLED` | `true` | `false` |
| `MANAGEMENT_OTLP_METRICS_EXPORT_STEP` | `30s` | `5s` em `obs-up` |
| `MANAGEMENT_TRACING_ENABLED` | `true` | `false` |
| `MANAGEMENT_TRACING_SAMPLING_PROBABILITY` | `1.0` | |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` | `http://localhost:4317` | |
| `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_TRANSPORT` | `grpc` | |
| `MANAGEMENT_OPENTELEMETRY_METRICS_EXPORT_OTLP_ENDPOINT` | `http://localhost:4317` | |
| `MANAGEMENT_LOGGING_EXPORT_OTLP_ENABLED` | `true` | logs no **mesmo** OTLP/gRPC que metrics/traces; `make up` força `false`; `obs-up` força `true` |
| `MANAGEMENT_OPENTELEMETRY_LOGGING_EXPORT_OTLP_ENDPOINT` | `http://localhost:4317` | |
| `MANAGEMENT_OPENTELEMETRY_LOGGING_EXPORT_OTLP_TRANSPORT` | `grpc` | |

`make obs-up` sobe SigNoz + liga export (ver `docker-compose.signoz.yml`).

---

## 6. Feature flags (só env)

Sem servidor de flags. Toggle = env + recreate/redeploy.

| Flag | Env | Default | Efeito |
|------|-----|---------|--------|
| ingestion | `TRANSACTIONS_INGESTION_ENABLED` | `true` | `false` → não registra consumer Kafka |
| cache | `BALANCE_CACHE_ENABLED` | `true` | `false` → só DynamoDB (sem Redis) |
| consistent-read | `DYNAMODB_CONSISTENT_READ` | `true` | `false` → GetItem eventually consistent |
| auth | `API_AUTH_ENABLED` | `true` | keys em `api-keys.json` / `API_AUTH_KEYS` |
| rate-limit | `API_RATE_LIMIT_ENABLED` | `false` | 429 após N req/min |

```bash
make up-no-ingest          # ingestion off
make up                    # cache on (default)
make up-no-cache           # cache off
make up-secure             # key=local-dev-key, 120 req/min
DYNAMODB_CONSISTENT_READ=false make up --build
```

---

## 7. Auth e rate limit

```bash
make up-secure
curl -H 'X-API-Key: local-dev-key' http://localhost:8080/balances/<uuid>
```

- Públicos: `/actuator/**`, `/openapi.yaml`, `/error`
- Rate key = API key se presente, senão IP (`X-Forwarded-For` / remote addr)
- `401 UNAUTHORIZED` / `429 RATE_LIMIT_EXCEEDED` + headers `X-RateLimit-*` / `Retry-After`
- **In-memory (1 instância).** Multi-pod → gateway ou Redis compartilhado
- **Prod real:** JWT/mTLS no edge, authZ por conta, actuator em rede privada (ver LIMITATIONS)

---

## 8. Cache Redis

- Default **on**. Compose sobe Redis `:6379` e a app usa cache-aside. Desligar: `BALANCE_CACHE_ENABLED=false` / `make up-no-cache`.
- Runtime: get/put **fail-open** (CB `redis` open → bypass).
- Com cache **on**, Redis precisa estar up no **startup** (Lettuce).
- Put só após Dynamo `saved`; put falho → DEL + métrica `balance.cache{result=put_failed}`.
- Métricas: `balance.cache{result=hit|miss|put_failed}`.
- Load comparativo: `BALANCE_CACHE_ENABLED=false|true` + `make load-test CACHE_MODE=off|on`.

Stale ≤ TTL ainda possível em race/miss antigo (LIMITATIONS).

---

## 9. Observabilidade

| Sinal | Onde |
|-------|------|
| Logs | stdout JSON (`event=...`, `result=...`) |
| Counters negócio | `balance.transactions{result=saved\|ignored_ineligible\|ignored_not_newer\|retried\|dlt}`, `balance.queries{...}`, `balance.cache{...}` |
| CB | `resilience4j.circuitbreaker.*` |
| Lag | `kafka.consumer.*records.lag*` (por topic/partition) |
| Containers listener | `kafka.consumer.listener.containers` |
| Spans | HTTP MVC, Kafka listener, DynamoDB GetItem/PutItem |
| Lab UI | `make obs-up` → http://localhost:3301 (SigNoz) |

Dashboards: [`infra/signoz/DASHBOARDS.md`](../infra/signoz/DASHBOARDS.md).

Compose padrão: OTLP **off**. Não aponte métricas para collector inexistente em prod sem endpoint válido.

---

## 10. Deploy sugerido (AWS / Kafka reais)

Checklist — **infra fora deste repo** (sem Terraform/Helm aqui):

1. **DynamoDB:** tabela `AccountBalances`, HASH `account_id`, on-demand (ou provisioned dimensionado).
2. **IAM:** role da task/pod com `dynamodb:GetItem`, `PutItem`, `DescribeTable` na tabela; **não** setar `DYNAMODB_ENDPOINT` (vazio → AWS + default credentials).
3. **Kafka:** tópico main + `….retry-1..N` + DLT; partições ≥ `KAFKA_LISTENER_CONCURRENCY` × instâncias desejadas no group; ACLs no authorizer (SoT do snapshot).
4. **Redis (cache desativável):** feature de primeira classe; em prod com cache on use Redis managed + network policy. Kill switch: `BALANCE_CACHE_ENABLED=false`.
5. **Image:** build `runtime`, rodar como UID `10001`, resources com headroom para heap 75% RAM.
6. **Env prod:** auth/rate no **gateway** (não confiar só no lab in-app); OTLP → collector do ambiente; sampling de trace &lt; 1.0 sob carga.
7. **Probes:** liveness + readiness (§3); grace period ≥ 30s (§2).
8. **Scale:** horizontal no consumer com **mesmo** `KAFKA_CONSUMER_GROUP_ID`; partições ≥ pods de ingestão.
9. **Kill switch:** `TRANSACTIONS_INGESTION_ENABLED=false` + rolling recreate se precisar parar ingest sem derrubar GET.

---

## 11. Cenários adversos

| Cenário | Comportamento |
|---------|---------------|
| Redelivery (mesmo ts + mesmo `transaction.id`) | Ignorada (`saveIfNewer` = false) |
| Empate de µs, txs distintas | Desempate por `last_transaction_id` (string) |
| Evento fora de ordem (ts menor) | Ignorado |
| Evento mais novo (ts maior) | Sobrescreve atomicamente |
| `DECLINED` / conta `DISABLED` | Ignorado com sucesso |
| JSON/UUID/domínio inválido | Sem retry → DLT |
| DynamoDB down | Write → retry-1..N → DLT; GET → 503 se CB/`SdkException` |
| CB `dynamodb-read` open / bulkhead cheio | GET 503 `DEPENDENCY_UNAVAILABLE` |
| CB `dynamodb-write` open | write falha técnica → retry |
| CB `redis` open | bypass cache; GET via Dynamo |
| CB `kafka-produce` open | não publica retry/DLT; redelivery depois |
| Conta inexistente | 404 JSON estável |
| UUID inválido no path | 400 JSON estável |
| Concorrência no mesmo `account_id` | condição atômica no DynamoDB |
| SIGTERM rolling | graceful 30s; redelivery Kafka ok |

---

## 12. Load, capacity, chaos

```bash
make up && make load-seed && make load-test
# cache on vs off:
# BALANCE_CACHE_ENABLED=true make up --build && make load-test CACHE_MODE=on
```

- Load: [`LOAD.md`](LOAD.md)
- SLOs / dimensionamento: [`CAPACITY.md`](CAPACITY.md)
- Drills locais: [`CHAOS.md`](CHAOS.md) (`make chaos-*`, Toxiproxy)
- Demo revisor: `make review-demo-quick`

Gatling **não** bloqueia CI.

---

## 13. Limitações (ops) — o que ainda falta em prod real

Ver detalhe em [`LIMITATIONS.md`](LIMITATIONS.md). Resumo operacional:

| Já no código / lab | Ainda precisa fora do repo |
|--------------------|----------------------------|
| Non-root image, graceful 30s | IaC, rolling multi-env, HPA policies |
| Probes + CB split + bulkhead GET | Alertas lag/DLT/CB no APM corporativo |
| Lag per partition (Micrometer) | ACLs Kafka + mTLS brokers |
| Auth/rate in-app opcional | Edge JWT/mTLS + authZ por conta; actuator privado |
| Retry multi-tópico + sleep no retry listener | Delayed topics / `@RetryableTopic` se HOL nos retries importar |
| DLT tópico | Processo/tooling de inspeção e replay |
| OTLP lab (SigNoz) | Collector/backends do ambiente |
| Dynamo local / IAM via empty endpoint | Conta AWS, tabela, alarms de throttle |
| Rate limit in-memory | Gateway/Redis multi-pod |

---

## 14. Runbook rápido

### Saldo desatualizado
1. Lag do consumer group (Console Redpanda `:8081` ou `kafka.consumer.*records.lag*`).
2. Mensagens no DLT + tópicos `….retry-N`.
3. Eventos chegando com `timestamp` maior que o snapshot?
4. Readiness / DynamoDB / CB `dynamodb-write`.
5. `TRANSACTIONS_INGESTION_ENABLED` ainda `true`?

### Pico de erros HTTP
1. `/actuator/health` + logs JSON.
2. `balance.queries` / `balance.transactions` + `resilience4j.circuitbreaker.*`.
3. Bulkhead: 503 com store “saudável” → subir `DYNAMODB_READ_BULKHEAD_*` ou scale-out API com cuidado no Dynamo.
4. Throttle Dynamo / latência (Toxiproxy local simula).

### Reprocessar DLT
1. Inspecionar payload + headers de exceção no DLT.
2. Corrigir causa (dado ou infra).
3. Republicar no tópico **principal** (replay manual — sem ferramenta no repo).
4. Confirmar `saved` e GET 200.

### Parar ingestão sem derrubar leitura
```bash
TRANSACTIONS_INGESTION_ENABLED=false make up --build
# ou recreate da task com a env
```

### Cache suspeito
1. `BALANCE_CACHE_ENABLED`?
2. CB `redis` / métricas hit-miss-put_failed.
3. Fail-open esperado: GET deve seguir via Dynamo se Redis cair (`make chaos-redis-stop` no lab).
