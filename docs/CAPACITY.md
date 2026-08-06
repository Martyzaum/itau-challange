# Capacity plan

Baseline de capacidade da API de saldo a partir de loads manuais locais
(`make load-test` / `make load-kafka`) e do desenho atual (Kafka → DynamoDB → REST).

> Ambiente de medição: Docker Compose em uma máquina de dev, DynamoDB Local,
> Redpanda, app single-instance, OTLP/SigNoz opcional. **Não** é bench de produção AWS —
> serve para dimensionar partições/workers e declarar SLOs realistas.

Como repetir: [`docs/LOAD.md`](LOAD.md).

---

## 1. Números medidos (cache off)

### 1.1 Somente GET — 10 VUs · 5 min · ramp 10s

| Métrica | Valor |
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

| Lado | Config | Resultado |
|------|--------|-----------|
| **HTTP** | 50 VUs · 3m · ramp 15s | **227 024** req · **0 KO** · **~1 107 rps** |
| HTTP p50 / p95 / p99 | | 30 / 119 / **217 ms** |
| HTTP max | | 493 ms |
| **Kafka produce** | 10 workers · 3m · batch 100 | **32 946** msgs (~**183 msg/s**) |
| Spans consume + PutItem | | 33 222 cada · **0 errors** |
| PutItem p99 (trace) | | ~**51 ms** |
| GetItem p99 (trace, sob carga mista) | | ~**104 ms** |
| JVM threads / CPU (pico) | | ~83 / ~1.0 (host saturado) |

### 1.3 Leitura

| Cenário | Ordem de grandeza sustentável (local, 1 app) |
|---------|-----------------------------------------------|
| Só leitura | **~1.0–1.5k rps** GET com p99 &lt; 50 ms |
| Leitura + ingestão | **~1.0k rps** GET + **~150–200 msg/s** ingest; p99 GET sobe para ~200 ms |
| Erros | **0** nos runs acima (app + DDB local saudáveis) |

Em AWS (DynamoDB on-demand, MSK/Redpanda gerenciado, N réplicas app) espere
outro teto — usar estes números como **piso de validação** e regredir com o mesmo
`make load-*` após mudanças.

---

## 2. Cache on vs off

| | Cache **off** (default) | Cache **on** (`BALANCE_CACHE_ENABLED=true`) |
|--|--|--|
| Read path | sempre GetItem | Redis → miss → GetItem → fill |
| Write path | PutItem condicional | PutItem + `putIfNewer` Redis |
| Fail Redis | n/a | fail-open → só DynamoDB |
| Load label | `CACHE_MODE=off` | `CACHE_MODE=on` |

**Expectativa** (a validar no ambiente alvo):

- Cache **on** + alto *hit ratio* (contas quentes): p99 GET e RCU DynamoDB caem; throughput sobe.
- Cache **on** + miss cold / TTL baixo: próximo de cache off + custo extra Redis.
- Multi-worker: versão `(updatedAtMicros, lastTransactionId)` no valor cacheado; write usa `putIfNewer`.

Comparar:

```bash
BALANCE_CACHE_ENABLED=false make up --build && make load-seed
make load-test CACHE_MODE=off VUS=50 DURATION=1m

# após PR de cache / make up-cache
BALANCE_CACHE_ENABLED=true make up --build && make load-seed
make load-test CACHE_MODE=on VUS=50 DURATION=1m
```

---

## 3. Partições, consumers e workers HTTP

### Kafka

| Parâmetro | Default local | Orientação |
|-----------|---------------|------------|
| Partições tópico input | **3** | `max(throughput_alvo / throughput_por_partition, nº_instâncias_consumer)` |
| Consumer group | 1 group, N instâncias | Instâncias ≤ partições (senão idle) |
| Retry | síncrono na partição (hoje) | Falha longa de DDB → **lag** na partição; async retry topics (futuro) isolam o main |
| Key | `accountId` | Ordenação por conta na partição |

**Regra prática:** sob ~200 msg/s ingest medidos com 1 consumer, 3 partições
cobrem folga e permitem escalar a **3** pods consumer sem rebalance inútil.
Se ingest alvo for 1k msg/s, medir PutItem p99 e subir partições/pods juntos.

### App HTTP

| Parâmetro | Orientação |
|-----------|------------|
| Réplicas stateless | Escalar no GET; cada uma com seu pool HTTP |
| DynamoDB | On-demand ou provisioned com headroom no p99 GetItem |
| Redis (se cache on) | Cluster/HA; timeout baixo (ex. 200 ms); fail-open já no código |

### Workers de load (Gatling / Kafka produce)

| Ferramenta | Knob | Papel |
|------------|------|--------|
| Gatling | `VUS` / `WORKERS` | concorrência closed-loop no GET |
| Gatling | `RPS` | open-loop (arrival rate) |
| Kafka load | `WORKERS` + `DURATION` + opcional `RPS` | produtores paralelos |

---

## 4. Circuit breaker e retry (impacto qualitativo)

| Mecanismo | Estado | Efeito em capacidade |
|-----------|--------|----------------------|
| CB DynamoDB | planejado | Open → GET 503 rápido (degradação controlada); evita storm e fila infinita no client |
| CB Redis | planejado (fail-open) | Não reduz capacidade de leitura; só desliga cache |
| Retry síncrono Kafka | **atual** | Backoff na partição **reduz** throughput de ingest sob erro DDB |
| Retry topics async | planejado | Main consumer segue; capacidade de ingest degrada menos sob falha transitória |
| DLT | **atual** | Poison não bloqueia para sempre; operacional reprocessa |

Enquanto retry for síncrono, **não** dimensione ingest só pelo happy path — reserve
headroom de partições/lag para janelas de erro.

---

## 5. SLOs sugeridos (ponto de partida)

Ajustar após medição em staging real.

| SLO | Alvo sugerido | Base local |
|-----|---------------|------------|
| Disponibilidade GET | 99.9% mensal | 0 erros nos loads |
| Latência GET p99 | ≤ 100 ms (cache on) / ≤ 200 ms (cache off, região co-located) | 26 ms só-GET; 217 ms misto pesado |
| Latência GET p99 degradado (CB open / DDB lento) | fail-fast &lt; 50 ms com 503 | a validar com CB |
| Ingest lag p99 | &lt; 30 s sob carga nominal | depende de partições + PutItem |
| Taxa DLT | &lt; 0.1% eventos (só poison/invalid) | inválidos já vão DLT sem retry |

### Degradação esperada

```text
DynamoDB lento/down
  → (hoje) retry sync → lag Kafka; GET lento/erro
  → (com CB) GET 503; write segue retry/DLT; UI/client faz backoff

Redis down + cache on
  → fail-open; capacidade = cache off; logs warn

Pico só GET
  → escalar réplicas app + (opcional) cache on

Pico ingest
  → mais partições/consumers; watch PutItem p99 e lag
```

---

## 6. Checklist de dimensionamento

1. Fixar SLO de p99 GET e msg/s ingest alvo.  
2. Rodar `make load-test` / `load-kafka` / misto no ambiente alvo (não só laptop).  
3. Anotar p99 GetItem/PutItem (SigNoz ou X-Ray).  
4. Partições ≥ consumers desejados; key = accountId.  
5. Ligar cache se leitura ≫ escrita e hit ratio alto; repetir load `CACHE_MODE=on`.  
6. Após CB DDB: repetir chaos pause + load (não deve cascatear).  
7. Após retry async: medir lag sob falha vs retry sync.  
8. Atualizar esta página com números de **staging/prod**.

---

## 7. Referências

- Load: [`docs/LOAD.md`](LOAD.md)  
- Operação: [`docs/PRODUCTION.md`](PRODUCTION.md)  
- Decisões (versão composta, cache, OTLP): [`docs/DECISIONS.md`](DECISIONS.md)  
- SigNoz local: `make obs-up` → http://localhost:3301  
