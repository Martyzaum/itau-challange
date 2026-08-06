# Testes de carga

Testes de carga são **manuais / opcionais** — não entram em `./gradlew check` nem em gates de CI.

Para rodar load + chaos + preenchimento do SigNoz de uma vez (walkthrough do revisor): [`REVIEW.md`](REVIEW.md) → `make review-demo`.

Os knobs espelham o estilo **k6** (`vus`/`workers`, `duration`, ramp, `rps` opcional).

Números de capacidade e SLOs derivados dessas execuções: [`CAPACITY.md`](CAPACITY.md).

## O que mede

| Alvo | Como | Caminho |
|------|------|---------|
| **HTTP read** | Gatling `GetBalanceSimulation` | `GET /balances/{accountId}` |
| **Kafka write** | `infra/load/produce-kafka-load.sh` via `rpk` | tópico `transacoes-financeiras-processadas` |
| **Misto** | ambos em paralelo | leitura + escrita |

Feeder compartilhado: `src/gatling/resources/account-ids.json` — `[{ "accountId": "..." }, ...]`.

## Pré-requisitos

```bash
make up          # app + DynamoDB + Redpanda (+ Redis no compose; cache on por padrão)
make load-seed   # grava as contas do feeder no DynamoDB (necessário para GET 200)
```

Opcional — aquecer o write-path com eventos aleatórios (não usa o feeder fixo):

```bash
make load-ingest                 # COUNT default 100
make load-ingest INGEST_COUNT=500
```

## HTTP — GET /balances

```bash
make load-smoke                              # PROFILE=smoke → 1 VU / 15s
make load-load                               # PROFILE=load  → 20 VUs / 1m
make load-stress                             # PROFILE=stress → 100 VUs / 2m
make load-spike                              # PROFILE=spike → 200 VUs / 30s
make load-test VUS=50 DURATION=1m RAMP=15s
make load-test RPS=100 DURATION=1m           # modelo aberto
make load-test WORKERS=30 DURATION=45s THINK_MS=50
```

Relatórios HTML: `build/reports/gatling/`.

### Knobs (Make → sistema)

| Make var | Alias | Default | Significado |
|----------|-------|---------|-------------|
| `BASE_URL` | | `http://localhost:8080` | API alvo |
| `PROFILE` | | `custom` | `smoke` \| `load` \| `stress` \| `spike` \| `custom` |
| `VUS` | `WORKERS`, `USERS` | do profile / `20` (custom) | Workers concorrentes (**fechado**) |
| `RPS` | | — | Se definido → modelo **aberto** (req/s) |
| `DURATION` | `DURATION_SECONDS` (+sufixo `s`) | do profile / `30s` | `30`, `30s`, `1m`, `2m30s` |
| `RAMP` | `RAMP_UP`, `RAMP_SECONDS` (+`s`) | do profile / `10s` (custom) | Ramp up |
| `RAMP_DOWN` | | do profile / `0s` (custom) | Ramp down (só modelo fechado) |
| `THINK_MS` | | `0` | Pausa entre requests por VU (fechado; usa `pace`) |
| `P99_MS` | | `2000` (Gatling) | Assert p99 (ms) |
| `MAX_FAIL_PCT` | | `1.0` (Gatling) | % máxima de falhas |
| `CACHE_MODE` | | `off` | Só label no relatório / User-Agent |

No Makefile, `P99_MS` / `MAX_FAIL_PCT` / `RAMP*` / `VUS` etc. ficam vazios se omitidos; o preset do `PROFILE` (e os defaults da simulação) preenchem no Gatling.

### Profiles HTTP (`GetBalanceSimulation`)

| `PROFILE` | VUs | Duração steady | Ramp up | Ramp down |
|-----------|-----|----------------|---------|-----------|
| `smoke` | 1 | 15s | 0 | 0 |
| `load` | 20 | 1m | 15s | 10s |
| `stress` | 100 | 2m | 30s | 20s |
| `spike` | 200 | 30s | 5s | 5s |
| `custom` | 20 | 30s | 10s | 0 |

Qualquer prop explícita (`-Dvus`, `-Dduration`, … via Make) **sobrescreve** o preset.

### Modelos (HTTP)

| Modo | Quando | Comportamento |
|------|--------|---------------|
| **Fechado** | sem `RPS` | N workers em loop durante ramp+hold(+rampDown) |
| **Aberto** | `RPS` definido | ~`RPS` usuários one-shot / s (arrival rate); sem ramp down |

## Kafka — carga de ingestão

Produz eventos elegíveis (`APPROVED` + `ENABLED`) para os **mesmos** `accountId` do Gatling, com **timestamps monotônicos** para o `saveIfNewer` continuar gravando.

Chave Kafka = `accountId` (ordenação por conta / partição).

```bash
make load-kafka-smoke                        # PROFILE=smoke → 1 worker / 15s
make load-kafka WORKERS=4 DURATION=1m
make load-kafka PROFILE=stress
make load-kafka WORKERS=8 DURATION=30s RPS=500   # teto ~500 msg/s total
make load-kafka ELIGIBLE_PCT=80                  # 20% ruído DECLINED/DISABLED
```

| Make var | Default | Significado |
|----------|---------|-------------|
| `WORKERS` | do profile / `4` (custom); alias `VUS` se `WORKERS` vazio | Processos produtores em paralelo |
| `DURATION` | do profile / `30s`; alias `DURATION_SECONDS` | Quanto tempo produzir |
| `RPS` | max (sem teto) | Teto total de msgs/s (dividido entre workers) |
| `PROFILE` | `custom` | ver tabela abaixo |
| `BATCH_SIZE` | `50` | Mensagens por chamada `rpk produce` |
| `ELIGIBLE_PCT` | `100` | % `APPROVED`+`ENABLED` (resto `DECLINED` ou `DISABLED`) |
| `KAFKA_TOPIC` | `transacoes-financeiras-processadas` | Tópico alvo |

### Profiles Kafka (`produce-kafka-load.sh`)

| `PROFILE` | Workers | Duração |
|-----------|---------|---------|
| `smoke` | 1 | 15s |
| `load` | 4 | 1m |
| `stress` | 8 | 2m |
| `spike` | 12 | 30s |
| `custom` | 4 | 30s |

Stdout ao final: `produced_total=…` (sem assertions Gatling).

Acompanhar no SigNoz o dashboard **Balance — Ingestion** (`balance.transactions`, spans de consume Kafka, latência PutItem) quando a stack de obs estiver no ar (`make obs-up` / `make review-demo`).

## Misto (read + write)

```bash
make load-mixed WORKERS=4 DURATION=30s
# sobe produtores Kafka em background + Gatling GET com os mesmos workers/duração
```

Comportamento do target:

- Defaults se omitidos: `WORKERS=4` (ou `VUS`), `DURATION=30s`.
- Kafka: `make load-kafka` com `WORKERS` / `DURATION` / `PROFILE` / `RPS` repassados.
- HTTP: `make load-test` com `VUS=<workers>`, `DURATION` igual, **`PROFILE=custom`**, **`RAMP=5s`**, `CACHE_MODE` repassado.
- Aguarda o PID do Kafka ao terminar o Gatling.

## Cache on vs off

```bash
make up                                       # cache on (default)
make load-seed && make load-test CACHE_MODE=on VUS=50 DURATION=1m

make up-no-cache                              # BALANCE_CACHE_ENABLED=false
make load-seed && make load-test CACHE_MODE=off VUS=50 DURATION=1m
```

`CACHE_MODE` só rotula o relatório Gatling. O toggle real é `BALANCE_CACHE_ENABLED` (`make up` / `up-cache` on; `make up-no-cache` off).

Interpretação de p99 / hit ratio / dimensionamento: [`CAPACITY.md`](CAPACITY.md).

## Assertions (somente HTTP)

Na simulação Gatling:

- Requests falhos (global) &lt; `MAX_FAIL_PCT`
- p99 global &lt; `P99_MS`
- Sucesso de `GET /balances/{accountId}` &gt; `100 - MAX_FAIL_PCT`

Carga Kafka reporta `produced_total=…` no stdout — sem assertions Gatling.

## Gradle (somente HTTP)

```bash
./gradlew gatlingRun -DbaseUrl=http://localhost:8080 -Dvus=50 -Dduration=1m -DapiKey=local-dev-key
```

Props úteis: `baseUrl`, `profile`, `vus` / `workers`, `rps`, `duration`, `rampUp` / `ramp`, `rampDown`, `thinkMs`, `p99Ms`, `maxFailPct`, `cacheMode`, `apiKey` (default `local-dev-key`).

`gatlingRun` **não** está ligado ao `check`. Auth da API é **on** por default — Gatling envia `X-API-Key`.
