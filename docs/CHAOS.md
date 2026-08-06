# Experimentos de chaos (Compose local)

Drills manuais contra o stack local. **Não** é gate de CI.

Para um único comando que sobe carga **e** roda esses drills enquanto preenche o SigNoz, veja [`REVIEW.md`](REVIEW.md) (`make review-demo`).

Pré-requisito:

```bash
make up          # cache on por default; ou make obs-up / up-no-cache
```

Cada target imprime o que observar. Restaure com o `*-recover` correspondente (ou `make up`).

---

## Automatizado: DynamoDB down → tópicos retry → recovery

Valida o **retry assíncrono** Kafka ponta a ponta com infra real:

1. Pausa o DynamoDB  
2. Produz eventos de transação válidos  
3. Asserta que as keys das contas aparecem em `….retry-1`  
4. Despausa o DynamoDB  
5. Asserta que `GET /balances/{id}` retorna **200** depois que os consumers de retry processam  

O retry é multi-tópico (`main → ….retry-1 → … → DLT`). Main não dorme; retry listeners honram `x-retry-not-before-ms` (exp backoff + jitter). A recovery depende do **unpause** do DynamoDB + consumers processando os hops (após o deadline). Auth default on: scripts usam `X-API-Key: local-dev-key`.

Exige timeouts curtos no SDK DynamoDB (defaults: 5s / 3s via `DYNAMODB_API_CALL_TIMEOUT_MS` / `DYNAMODB_API_CALL_ATTEMPT_TIMEOUT_MS`).
Sem isso, `docker pause dynamodb` congela o TCP e o consumer trava em vez de falhar para o retry.

```bash
make up --build
make chaos-retry-topics
# Defaults do script: COUNT=3 WAIT_RETRY_SEC=60 WAIT_RECOVER_SEC=120
# COUNT=5 WAIT_RETRY_SEC=90 make chaos-retry-topics
```

Script: `infra/chaos/validate-retry-topics.sh` (cleanup no `EXIT` tenta `unpause dynamodb`).

---

## Cenários

### 1. DynamoDB pause

```bash
make chaos-dynamodb-pause
# GET /balances/* e ingest Kafka devem falhar/atrasar (store down)
# Com timeouts curtos no SDK: falhas técnicas vão para ….retry-N (hop imediato)
# Após falhas suficientes: CB open → GET 503 DEPENDENCY_UNAVAILABLE
make chaos-dynamodb-recover
```

### 1b. DynamoDB latency (Toxiproxy)

Tráfego da app passa pelo Toxiproxy (`localhost:8000` no host → proxy `8666` → DynamoDB). Seed/admin também usam o proxy.

```bash
make chaos-dynamodb-latency              # LATENCY_MS=2000 JITTER_MS=500
# GetItem/PutItem lentos; CB pode abrir → GET 503
# métricas: resilience4j.circuitbreaker.* name=dynamodb-read|dynamodb-write
make chaos-dynamodb-latency-clear
```

Scripts: `infra/toxiproxy/add-latency.sh`, `infra/toxiproxy/clear-latency.sh`.  
API Toxiproxy: http://localhost:8474

### 2. Redis stop (cache on)

```bash
make up   # cache já on
make chaos-redis-stop
# GET must keep working (fail-open → DynamoDB only)
# logs: balance_cache_get_failed / put_failed; after enough fails CB redis opens
# open redis CB: balance_cache_circuit_open (still no 503)
make chaos-redis-recover
```

### 3. Kafka / Redpanda stop

```bash
make chaos-kafka-stop
# ingest para; GET ainda funciona se o DynamoDB tiver dados
# publish DLT/retry protegido pelo CB kafka-produce (sem martelar broker quando open)
# lag cresce no consumer group quando o broker volta
make chaos-kafka-recover
# chaos-kafka-recover já sobe redpanda + redpanda-seed
# se recriou o broker do zero e precisar reforçar tópicos: make kafka-seed
```

### 4. Poison → DLT

```bash
make chaos-poison-dlt
# publica JSON inválido no tópico principal
# espera mensagem em transacoes-financeiras-processadas.DLT (sem long retry; not-retryable)
make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT
```

### 5. Flag de ingestion off

```bash
make chaos-ingestion-flag-off
# bean do consumer ausente (TRANSACTIONS_INGESTION_ENABLED=false); eventos novos não processados
# GET ainda serve balances já existentes
make chaos-ingestion-flag-recover
```

---

## Walkthrough empacotado (review)

```bash
make review-demo              # DEMO_PROFILE=quick por padrão
make review-demo-quick        # ~5–8 min com stack quente
make review-demo-full         # janelas maiores / dashboards mais ricos
```

Pipeline: `infra/review/demo-pipeline.sh` — bootstrap (opcional), carga HTTP+Kafka, chaos (poison, latency Dynamo, Redis stop, retry) e fase saudável final. Detalhes e knobs (`SKIP_BOOTSTRAP`, `SKIP_CHAOS`, `SKIP_LOAD`, …): [`REVIEW.md`](REVIEW.md).

---

## Observar

| Sinal | Onde |
|-------|------|
| Logs da app | `make logs` — `event=...` |
| Health | `curl localhost:8080/actuator/health` |
| Tópicos retry | `make kafka-consume TOPIC=transacoes-financeiras-processadas.retry-1` |
| DLT | `make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT` |
| SigNoz | `make obs-up` → http://localhost:3301 |
| Lag | Redpanda Console :8081 · métricas `kafka.consumer.*records.lag*` (MicrometerConsumerListener) |

---

## Segurança

- Somente local; pause/stop afetam **serviços Compose deste projeto**.
- Sempre rode o target de recover correspondente antes de deixar a máquina.
- `chaos-retry-topics` tenta unpause no exit; os drills manuais **não** — recover é sua responsabilidade.
