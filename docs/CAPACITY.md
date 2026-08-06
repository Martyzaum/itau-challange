# Capacidade

Notas de capacidade da API de saldo a partir de loads manuais locais
(`make load-test` / `make load-kafka` / `make load-mixed`) e do desenho atual
(Kafka → DynamoDB → REST, com cache Redis desativável).

> **Ambiente de medição:** Docker Compose em máquina de dev, DynamoDB Local,
> Redpanda, app single-instance, OTLP/SigNoz opcional. **Não** é bench de
> produção AWS. Serve para dimensionar partições/workers e regredir após mudanças.
>
> Os números da §1 são **ilustrativos** (um laptop, um run). Não são SLO de
> produção nem garantia de throughput.

Como repetir: [`LOAD.md`](LOAD.md). Demo completa (load + chaos + SigNoz):
[`REVIEW.md`](REVIEW.md). Operação e env vars: [`PRODUCTION.md`](PRODUCTION.md).
Limitações conscientes: [`LIMITATIONS.md`](LIMITATIONS.md).

---

## 1. Números medidos (cache off) — ilustrativos

Baseline histórico medido com `BALANCE_CACHE_ENABLED=false`. Default atual da app é **cache on**; comparar com `make up-no-cache` vs `make up`. Cache on muda o caminho de
leitura; comparar com `CACHE_MODE=on|off` / `make up` vs `up-no-cache` (ver §2).

### 1.1 Somente GET — 10 VUs · 5 min · ramp 10s

| Métrica | Valor (ilustrativo) |
|---------|------:|
| Requests | 479 369 |
| Erros (KO) | **0** |
| Throughput médio | **~1 503 rps** |
| p50 / p95 / p99 (client) | 5 / 14 / **26 ms** |
| max | 187 ms |
| Trace p99 HTTP (SigNoz) | ~15 ms |
| Trace p99 GetItem | ~13 ms |
| Error spans | 0 |

### 1.2 Misto GET + Kafka — pesado · 3 min

| Lado | Config | Resultado (ilustrativo) |
|------|--------|-------------------------|
| **HTTP** | 50 VUs · 3m · ramp 15s | **227 024** req · **0 KO** · **~1 107 rps** |
| HTTP p50 / p95 / p99 | | 30 / 119 / **217 ms** |
| HTTP max | | 493 ms |
| **Kafka produce** | 10 workers · 3m · batch 100 | **32 946** msgs (~**183 msg/s**) |
| Spans consume + PutItem | | 33 222 cada · **0 errors** |
| PutItem p99 (trace) | | ~**51 ms** |
| GetItem p99 (trace, sob carga mista) | | ~**104 ms** |
| JVM threads / CPU (pico, host) | | ~83 / ~1.0 (host saturado — anedótico) |

### 1.3 Leitura de ordem de grandeza (local, 1 app)

| Cenário | Ordem de grandeza sustentável (ilustrativa) |
|---------|-----------------------------------------------|
| Só leitura | **~1.0–1.5k rps** GET com p99 client &lt; 50 ms |
| Leitura + ingestão | **~1.0k rps** GET + **~150–200 msg/s** ingest; p99 GET sobe para ~200 ms |
| Erros | **0** nos runs acima (app + DDB local saudáveis) |

Em AWS (DynamoDB on-demand, MSK/Redpanda gerenciado, N réplicas app) o teto é
outro — use estes números como **piso de validação** e regreda com o mesmo
`make load-*` após mudanças de código ou infra.

---

## 2. Cache on vs off

`BALANCE_CACHE_ENABLED` default **true**. Compose sobe Redis e a app usa cache.
Para medir sem cache: `make up-no-cache` / `BALANCE_CACHE_ENABLED=false`.

| | Cache **off** | Cache **on** |
|--|--|--|
| Read path | sempre GetItem | Redis → miss → GetItem → fill |
| Write path | PutItem condicional | PutItem + `putIfNewer` Redis (só se Dynamo salvou) |
| Fail Redis | n/a | fail-open → só DynamoDB; CB `redis` open = bypass |
| Load label | `CACHE_MODE=off` | `CACHE_MODE=on` (só rótulo do relatório Gatling) |

- Cache **on** + alto hit ratio: menor p99 GET e menos RCU DynamoDB.
- Cache **on** + miss cold / TTL baixo: próximo de cache off + custo Redis.
- Multi-worker: versão `(updatedAtMicros, lastTransactionId)` no valor; write usa `putIfNewer`.
- Put falho no Redis → **DEL** da chave (evita stale do payload rejeitado). Ainda pode haver stale ≤ TTL em race — ver [`LIMITATIONS.md`](LIMITATIONS.md).

```bash
make up --build && make load-seed
make load-test CACHE_MODE=on VUS=50 DURATION=1m

make up-no-cache --build && make load-seed
make load-test CACHE_MODE=off VUS=50 DURATION=1m
```

`CACHE_MODE` só rotula o relatório Gatling; o toggle real é `BALANCE_CACHE_ENABLED`.

---

## 3. Resiliência que afeta capacidade

Comportamento sob pressão — não é “mais RPS”, é o que **limita ou isola** carga.

### Circuit breakers (Resilience4j)

| Nome | Uso | Open |
|------|-----|------|
| `dynamodb-read` | GetItem (GET) | GET **503** `DEPENDENCY_UNAVAILABLE` + `Retry-After` |
| `dynamodb-write` | saveIfNewer (Kafka) | falha técnica → path de retry async |
| `redis` | cache get/put/invalidate | **fail-open** / bypass; **nunca** 503 |
| `kafka-produce` | publish retry/DLT | recoverer não publica; não martela broker; redelivery depois |

Config compartilhada via `RESILIENCE_CB_*` (failure rate 50%, janela 20, min calls 10,
open 30s, half-open 5) — ver [`PRODUCTION.md`](PRODUCTION.md).

Métricas: `resilience4j.circuitbreaker.*` + `balance.transactions{result=retried|dlt}` /
`balance.queries{result=…}` / `balance.cache{result=hit|miss}`.

### Bulkhead no GET

Semaphore no caminho de leitura DynamoDB (`DynamoDbAccountBalanceProvider`):

| Env | Default | Papel |
|-----|---------|--------|
| `DYNAMODB_READ_BULKHEAD_MAX_CONCURRENT` | **64** | máx. GetItem concorrentes por instância |
| `DYNAMODB_READ_BULKHEAD_TIMEOUT_MS` | **50** | espera por permit; estouro → `DependencyUnavailableException` (503) |

Isola tempestade de GET do resto da JVM; write Kafka **não** passa por este bulkhead
(CB `dynamodb-write` é domínio separado).

### Retry Kafka multi-tópico + backoff

- Main → `….retry-1..N` → DLT (`TRANSACTIONS_RETRY_MAX_ATTEMPTS` default **3**).
- Main: hop imediato (`FixedBackOff(0,0)`). Retry listeners: exp backoff + jitter via `x-retry-not-before-ms` (default 1s→…≤30s).
- JSON/domínio inválido → DLT direto. NPE/IAE retentam.
- Sob falha de store, retry async **adiciona carga** nos tópicos `….retry-N` — reserve
  headroom de lag ao dimensionar ingest.

Detalhe e trade-offs: [`LIMITATIONS.md`](LIMITATIONS.md), drills: [`CHAOS.md`](CHAOS.md).

---

## 4. Partições, consumers e workers

### Kafka

| Parâmetro | Default local | Orientação |
|-----------|---------------|------------|
| Partições tópico input | **3** (`TRANSACTIONS_TOPIC_PARTITIONS`) | `max(throughput_alvo / thr_por_partition, nº_instâncias_consumer)` |
| `KAFKA_LISTENER_CONCURRENCY` | **3** | threads do listener por instância; alinhar a partições se 1 pod |
| Consumer group | `balance-transaction-consumer` | instâncias ≤ partições (senão idle) |
| Retry | async `….retry-N` + backoff no listener | main não dorme; sleep só no retry hop |
| DLT | `….DLT` | poison/invalid sem retry longo |
| Key (load/seed) | `accountId` | ordenação por conta na partição |

**Regra prática (ilustrativa):** sob ~200 msg/s ingest medidos com 1 consumer local,
3 partições cobrem folga e permitem escalar a **3** pods consumer. Se o alvo de
ingest for maior, medir PutItem p99 e subir partições/pods juntos.

Não dimensione ingest só pelo happy path: sob falha, retry async soma carga nos
tópicos `….retry-N`.

### App HTTP

| Parâmetro | Orientação |
|-----------|------------|
| Réplicas stateless | Escalar no GET; cada réplica tem seu bulkhead (64) |
| DynamoDB | On-demand ou provisioned com headroom no p99 GetItem |
| Redis (cache on) | HA; `BALANCE_CACHE_REDIS_TIMEOUT_MS` default 200; fail-open no código |
| Rate limit in-memory | opcional (`make up-secure`); **single-instance** — multi-pod → gateway/Redis |

### Workers de load

| Ferramenta | Knob | Papel |
|------------|------|--------|
| Gatling | `VUS` / `WORKERS` | concorrência closed-loop no GET |
| Gatling | `RPS` | open-loop (arrival rate) |
| Kafka load | `WORKERS` + `DURATION` + opcional `RPS` | produtores paralelos |
| Misto | `make load-mixed` | Kafka em background + Gatling GET |

Defaults e profiles: [`LOAD.md`](LOAD.md).

---

## 5. Alvos de qualidade (ponto de partida — não SLO contratual)

Ajustar após medição em staging real. Os alvos abaixo são **sugestão ilustrativa**,
ancorados nos runs locais da §1 — **não** há SLO formal nem gate de CI de load.

| Alvo sugerido (ilustrativo) | Base local |
|-----------------------------|------------|
| Disponibilidade GET alta sob lab saudável | 0 erros nos loads §1 |
| Latência GET p99: ordem de dezenas de ms (só-GET cache off) a ~200 ms (misto pesado) | 26 ms só-GET; 217 ms misto |
| Ingest lag sob carga nominal: monitorar; backoff nos hops espaça retries | partições + PutItem + retry async |
| DLT: só poison/invalid (não “retry esgotado” no happy path) | inválidos já vão DLT sem retry |

Assertions Gatling locais (não são SLO de prod): `P99_MS` default 2000,
`MAX_FAIL_PCT` default 1.0 — ver [`LOAD.md`](LOAD.md).

### Degradação (comportamento atual do código)

```text
DynamoDB lento/down
  → write: publish ….retry-N → lag nos tópicos de retry (main segue)
  → GET: lento; bulkhead esgota → 503; CB dynamodb-read open → 503

Redis down + cache on
  → fail-open; leitura = DynamoDB; logs warn; CB redis open = bypass
  → GET não vira 503 só por cache

kafka-produce CB open
  → recoverer não publica retry/DLT; não martela broker; redelivery depois

Pico só GET
  → mais réplicas app; bulkhead por instância; cache on (`BALANCE_CACHE_ENABLED=true`)

Pico ingest
  → mais partições/consumers (≤ partições); watch PutItem p99 e lag
  → sob falha, headroom para ….retry-N
```

---

## 6. Checklist de dimensionamento

1. Fixar alvos de p99 GET e msg/s ingest no **ambiente alvo** (não só laptop).
2. Rodar `make load-test` / `load-kafka` / `load-mixed` (e, se útil, `make review-demo`).
3. Anotar p99 GetItem/PutItem (SigNoz) e estado dos CBs / bulkhead sob stress.
4. Partições ≥ consumers desejados; key = `accountId` nos producers de teste.
5. Se cache on: repetir load `CACHE_MODE=on` e comparar hit ratio / p99 / RCU.
6. Sob chaos (Dynamo pause/latency, Redis stop): confirmar retry + backoff e fail-open — [`CHAOS.md`](CHAOS.md).
7. Atualizar esta página com números de **staging/prod** quando existirem.

---

## 7. Referências

- Load: [`LOAD.md`](LOAD.md)
- Chaos: [`CHAOS.md`](CHAOS.md)
- Review demo: [`REVIEW.md`](REVIEW.md)
- Operação / env: [`PRODUCTION.md`](PRODUCTION.md)
- Limitações: [`LIMITATIONS.md`](LIMITATIONS.md)
- Decisões: [`DECISIONS.md`](DECISIONS.md)
- SigNoz local: `make obs-up` → http://localhost:3301
