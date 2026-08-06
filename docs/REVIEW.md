# Pipeline de demo para review

Um comando sobe o stack **com SigNoz**, gera tráfego real (HTTP + Kafka), roda **drills de chaos** e deixa os dashboards populados para walkthrough de code review.

**Não é gate de CI** — só Compose local.

## Início rápido

```bash
# Primeira vez / máquina fria (build de imagens, sobe SigNoz — pode levar vários minutos)
make review-demo

# Stack já no ar com OTLP (make obs-up)
SKIP_BOOTSTRAP=1 make review-demo-quick

# Janelas mais longas (gráficos mais ricos)
make review-demo-full
# ou: DEMO_PROFILE=full make review-demo
```

| Target | O que faz |
|--------|-----------|
| `make review-demo` | Roda o script completo (`DEMO_PROFILE=quick` por padrão) |
| `make review-demo-quick` | Alias: `DEMO_PROFILE=quick` (~5–8 min com stack já quente) |
| `make review-demo-full` | Alias: `DEMO_PROFILE=full` (janelas mixed/HTTP/Kafka mais longas) |

Script: [`infra/review/demo-pipeline.sh`](../infra/review/demo-pipeline.sh).

## O que roda (fases)

Ordem real do script (`phase N:` no log). Fases de load/chaos podem ser puladas com `SKIP_*`.

| # | Fase | Ação | Por que / o que ver no SigNoz |
|---|------|------|-------------------------------|
| 1 | Bootstrap | `BALANCE_CACHE_ENABLED=$CACHE_ENABLED make obs-up` (se `SKIP_BOOTSTRAP≠1`) | App + Redpanda + DynamoDB + Redis + SigNoz; OTLP ligado. Espera liveness; readiness e UI do SigNoz são soft. |
| 2 | Seed | `make load-seed` | Account IDs do Gatling no DynamoDB → `GET /balances/{id}` tende a 200 |
| 3 | Warm-up | `make load-ingest INGEST_COUNT=30` + `make load-test` leve (VUs=3, 10s) | Primeiros metrics/traces. O HTTP do warm-up é **soft-fail**. |
| 4 | Mixed load | `make load-mixed` (`SKIP_LOAD=1` pula 4–5) | Write path (Kafka) + read path (GET) juntos |
| 5 | HTTP baseline | `make load-test PROFILE=custom` (closed model) | Latência da API, cache hit/miss; asserções normais (`P99_MS=3000`, `MAX_FAIL_PCT=2`) |
| 6 | Chaos: poison | `make chaos-poison-dlt` | JSON inválido → DLT; métrica `balance.transactions` com `result=dlt` / tópico DLT |
| 7 | Chaos: latência Dynamo | Toxiproxy (`chaos-dynamodb-latency`) + HTTP load soft | Put/Get lentos, CB pode abrir, possível 503; depois `latency-clear` + cooldown de CB |
| 8 | Chaos: Redis stop | `chaos-redis-stop` + HTTP load soft (só se cache on) | Fail-open: GET continua 200; erros de cache / CB do Redis. Pulado se `CACHE_ENABLED=false` |
| 9 | Chaos: retry | `make chaos-retry-topics COUNT=$RETRY_COUNT` (soft) | Pausa Dynamo → hop async (`retry-1`) → recover + GETs de verificação |
| 10 | Final healthy | `make load-kafka` + `make load-test` (asserções normais) | Dashboards estabilizam no verde |

Fases 6–9 só rodam se `SKIP_CHAOS≠1`.

### Soft-fail (chaos e warm-up HTTP)

- Comandos via `run_soft` **não derrubam** o pipeline se `KEEP_GOING_ON_CHAOS=1` (padrão): só logam `WARN` e seguem.
- Soft hoje: HTTP do warm-up, HTTP sob latência Dynamo, HTTP com Redis parado, `chaos-retry-topics`.
- Baseline (fase 5) e final healthy (fase 10) mantêm SLOs do Gatling (`set -e`).
- Loads de chaos usam SLO folgado de propósito (`P99_MS` alto / `MAX_FAIL_PCT` alto ou 100).

## Knobs

| Env / Make var | Default | Significado |
|----------------|---------|-------------|
| `DEMO_PROFILE` | `quick` | `quick` \| `full` — durações/VUs/workers do demo. **Não** é o `PROFILE` do Gatling (`smoke\|load\|stress\|custom`). O Make expõe `DEMO_PROFILE`; o script aceita fallback legado `PROFILE` e em seguida faz `unset PROFILE` para não vazar nos `make load-*`. |
| `SKIP_BOOTSTRAP` | `0` | `1` = stack já no ar (ex.: `make obs-up` prévio) |
| `SKIP_LOAD` | `0` | `1` = pula mixed + HTTP baseline (seed + warm-up + chaos + final ainda rodam) |
| `SKIP_CHAOS` | `0` | `1` = só load (preenche dashboards sem drills) |
| `CACHE_ENABLED` | `true` | repassado ao `obs-up` como `BALANCE_CACHE_ENABLED`; se `false`, pula chaos de Redis |
| `BASE_URL` | `http://localhost:8080` | alvo da API / health |
| `SIGNOZ_URL` | `http://localhost:3301` | impresso no banner e no summary |
| `KEEP_GOING_ON_CHAOS` | `1` | `0` = falha dura em comandos `run_soft` |

Perfis (valores padrão do script; override por env individual ainda funciona):

| | `quick` | `full` |
|--|---------|--------|
| mixed | 4 workers / 25s | 8 / 60s |
| HTTP baseline | 15 VUs / 20s / ramp 5s | 40 / 45s / 10s |
| HTTP chaos | 8 VUs / 15s | 20 / 30s |
| Kafka final | 3 workers / 20s | 6 / 45s |
| retry count | 3 | 5 |
| CB cooldown | 35s | 40s |

Exemplos:

```bash
SKIP_BOOTSTRAP=1 SKIP_CHAOS=1 make review-demo-quick   # só load (com warm-up + final)
CACHE_ENABLED=false SKIP_BOOTSTRAP=1 make review-demo  # sem cache Redis
DEMO_PROFILE=full SKIP_BOOTSTRAP=1 make review-demo
KEEP_GOING_ON_CHAOS=0 make review-demo                 # soft-fail vira hard-fail
```

## Onde olhar depois da run

| URL / path | Sinal |
|------------|--------|
| http://localhost:3301 | SigNoz UI → **Dashboards** (últimos 15–30 min; catálogo em [`../infra/signoz/DASHBOARDS.md`](../infra/signoz/DASHBOARDS.md)) |
| → *Balance — Overview* | snapshot E2E (ingest + API + cache + lag) |
| → *Balance — Ingestion* | saved/ignored_*/retried/dlt, PutItem |
| → *Balance — API* | latência GET, found/not_found, GetItem |
| → *Balance — Cache (Redis)* | hit/miss/put_failed (exige cache on) |
| → *Balance — Resilience* | CBs dynamodb/redis/kafka-produce, lag |
| → *Balance — Errors & degradation* | DLT, 5xx, CB open, put_failed |
| → *Balance — Health/JVM* | JVM, CPU, listener containers |
| http://localhost:8080/actuator/health | liveness / readiness / Dynamo |
| http://localhost:8081 | Redpanda Console (lag, tópicos) |
| `build/reports/gatling/` | HTML do Gatling |
| `build/review-demo/pipeline-*.log` | transcript do pipeline (`tee`) |

Curls úteis (o summary imprime um `accountId` de amostra):

```bash
curl -s http://localhost:8080/actuator/health | jq .
curl -s http://localhost:8080/balances/<accountId> | jq .

make kafka-consume TOPIC=transacoes-financeiras-processadas.retry-1
make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT
make logs
```

## Segurança (trap de cleanup)

- Mexe **somente** no Compose local (latência Toxiproxy no DynamoDB, pause/unpause DynamoDB, stop/start Redis).
- `trap cleanup_chaos EXIT` sempre tenta limpar side-effects, mesmo com Ctrl+C ou falha:
  - `make chaos-dynamodb-latency-clear`
  - `make chaos-dynamodb-recover`
  - `make chaos-redis-recover`
- No fim feliz o script chama `cleanup_chaos` de novo e remove o trap antes do summary.
- Prefira `SKIP_BOOTSTRAP=1` se já tiver uma sessão longa de `obs-up`.

## Relacionados

- Knobs de load: [`LOAD.md`](LOAD.md)
- Drills de chaos: [`CHAOS.md`](CHAOS.md)
- Capacidade: [`CAPACITY.md`](CAPACITY.md)
- SigNoz: [`../infra/signoz/README.md`](../infra/signoz/README.md)
)
