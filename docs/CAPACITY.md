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

Com `BALANCE_CACHE_ENABLED` (default **false**):

| | Cache **off** | Cache **on** |
|--|--|--|
| Read path | sempre GetItem | Redis → miss → GetItem → fill |
| Write path | PutItem condicional | PutItem + `putIfNewer` Redis |
| Fail Redis | n/a | fail-open → só DynamoDB |
| Load label | `CACHE_MODE=off` | `CACHE_MODE=on` |

- Cache **on** + alto hit ratio: menor p99 GET e menos RCU DynamoDB.
- Cache **on** + miss cold / TTL baixo: próximo de cache off + custo Redis.
- Multi-worker: versão `(updatedAtMicros, lastTransactionId)` no valor; write usa `putIfNewer`.

```bash
BALANCE_CACHE_ENABLED=false make up --build && make load-seed
make load-test CACHE_MODE=off VUS=50 DURATION=1m

BALANCE_CACHE_ENABLED=true make up --build && make load-seed
# ou: make up-cache
make load-test CACHE_MODE=on VUS=50 DURATION=1m
```

---

## 3. Partições, consumers e workers

### Kafka

| Parâmetro | Default local | Orientação |
|-----------|---------------|------------|
| Partições tópico input | **3** | `max(throughput_alvo / throughput_por_partition, nº_instâncias_consumer)` |
| Consumer group | 1 group, N instâncias | Instâncias ≤ partições (senão idle) |
| Retry | síncrono na partição | Falha longa de DDB → **lag** na partição (backoff bloqueia o consumer da partição) |
| DLT | tópico `.DLT` | JSON/domínio inválido sem retry longo |
| Key | `accountId` | Ordenação por conta na partição |

**Regra prática:** sob ~200 msg/s ingest medidos com 1 consumer, 3 partições
cobrem folga e permitem escalar a **3** pods consumer. Se o alvo de ingest for maior,
medir PutItem p99 e subir partições/pods juntos.

Não dimensione ingest só pelo happy path: o retry síncrono reduz throughput sob erro
de store — reserve headroom de lag.

### App HTTP

| Parâmetro | Orientação |
|-----------|------------|
| Réplicas stateless | Escalar no GET |
| DynamoDB | On-demand ou provisioned com headroom no p99 GetItem |
| Redis (cache on) | HA; timeout baixo (ex. 200 ms); fail-open no código |

### Workers de load

| Ferramenta | Knob | Papel |
|------------|------|--------|
| Gatling | `VUS` / `WORKERS` | concorrência closed-loop no GET |
| Gatling | `RPS` | open-loop (arrival rate) |
| Kafka load | `WORKERS` + `DURATION` + opcional `RPS` | produtores paralelos |

---

## 4. SLOs sugeridos (ponto de partida)

Ajustar após medição em staging real.

| SLO | Alvo sugerido | Base local |
|-----|---------------|------------|
| Disponibilidade GET | 99.9% mensal | 0 erros nos loads |
| Latência GET p99 | ≤ 100 ms (cache on) / ≤ 200 ms (cache off, região co-located) | 26 ms só-GET; 217 ms misto pesado |
| Ingest lag p99 | &lt; 30 s sob carga nominal | partições + PutItem + retry sync |
| Taxa DLT | &lt; 0.1% eventos (só poison/invalid) | inválidos já vão DLT sem retry |

### Degradação (comportamento atual)

```text
DynamoDB lento/down
  → retry sync no consumer → lag na partição
  → GET lento ou erro 5xx/timeout no client

Redis down + cache on
  → fail-open; leitura = cache off; logs warn

Pico só GET
  → mais réplicas app; opcional cache on

Pico ingest
  → mais partições/consumers; watch PutItem p99 e lag
```

---

## 5. Checklist de dimensionamento

1. Fixar SLO de p99 GET e msg/s ingest alvo.  
2. Rodar `make load-test` / `load-kafka` / misto no ambiente alvo (não só laptop).  
3. Anotar p99 GetItem/PutItem (SigNoz).  
4. Partições ≥ consumers desejados; key = accountId.  
5. Se cache on: repetir load `CACHE_MODE=on` e comparar hit ratio / p99.  
6. Atualizar esta página com números de **staging/prod**.

---

## 6. Referências

- Load: [`docs/LOAD.md`](LOAD.md)  
- Operação: [`docs/PRODUCTION.md`](PRODUCTION.md)  
- Decisões: [`docs/DECISIONS.md`](DECISIONS.md)  
- SigNoz local: `make obs-up` → http://localhost:3301  
