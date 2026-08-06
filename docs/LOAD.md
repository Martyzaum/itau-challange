# Load tests

Load tests are **manual / optional** — not part of `./gradlew check` or CI gates.

Knobs mirror **k6** (`vus`/`workers`, `duration`, ramp, optional `rps`).

## What it measures

| Target | How | Path |
|--------|-----|------|
| **HTTP read** | Gatling `GetBalanceSimulation` | `GET /balances/{accountId}` |
| **Kafka write** | `infra/load/produce-kafka-load.sh` via `rpk` | topic `transacoes-financeiras-processadas` |
| **Mixed** | both in parallel | read + write |

Shared feeder: `src/gatling/resources/account-ids.json` — `[{ "accountId": "..." }, ...]`.

## Prerequisites

```bash
make up          # app + DynamoDB + Redpanda
make load-seed   # seed feeder accounts into DynamoDB (needed for GET 200s)
```

## HTTP — GET /balances

```bash
make load-smoke                              # 1 VU / 15s
make load-test VUS=50 DURATION=1m RAMP=15s
make load-test RPS=100 DURATION=1m           # open model
make load-stress
```

| Make var | Alias | Default | Meaning |
|----------|-------|---------|---------|
| `BASE_URL` | | `http://localhost:8080` | Target API |
| `PROFILE` | | `custom` | `smoke` \| `load` \| `stress` \| `spike` \| `custom` |
| `VUS` | `WORKERS`, `USERS` | profile / `20` | Concurrent workers (**closed**) |
| `RPS` | | — | If set → **open** model (req/s) |
| `DURATION` | `DURATION_SECONDS` | profile / `30s` | `30`, `30s`, `1m`, `2m30s` |
| `RAMP` | `RAMP_UP` | profile / `10s` | Ramp up |
| `RAMP_DOWN` | | `0s` | Ramp down (closed) |
| `THINK_MS` | | `0` | Pause between requests per VU |
| `P99_MS` | | `2000` | Assert p99 |
| `MAX_FAIL_PCT` | | `1.0` | Max failed % |
| `CACHE_MODE` | | `off` | Report label only |

Reports: `build/reports/gatling/`.

### Models (HTTP)

| Mode | When | Behaviour |
|------|------|-----------|
| **Closed** | no `RPS` | N workers loop for the window |
| **Open** | `RPS` set | ~RPS one-shot users / s |

## Kafka — ingest load

Produces eligible events (`APPROVED` + `ENABLED`) for the **same** account IDs as Gatling, with **monotonic timestamps** so `saveIfNewer` keeps writing.

```bash
make load-kafka-smoke                        # 1 worker / 15s
make load-kafka WORKERS=4 DURATION=1m
make load-kafka PROFILE=stress
make load-kafka WORKERS=8 DURATION=30s RPS=500   # cap ~500 msg/s total
make load-kafka ELIGIBLE_PCT=80                  # 20% DECLINED/DISABLED noise
```

| Make var | Default | Meaning |
|----------|---------|---------|
| `WORKERS` | profile / `4` | Parallel producer processes |
| `DURATION` | profile / `30s` | How long to produce |
| `RPS` | max | Optional total msgs/s cap (split across workers) |
| `PROFILE` | `custom` | `smoke` (1w/15s) · `load` (4w/1m) · `stress` (8w/2m) · `spike` (12w/30s) |
| `BATCH_SIZE` | `50` | Messages per `rpk produce` call |
| `ELIGIBLE_PCT` | `100` | % APPROVED+ENABLED (rest DECLINED/DISABLED) |
| `KAFKA_TOPIC` | `transacoes-financeiras-processadas` | Target topic |

Key = `accountId` (ordering per account / partition).

Watch SigNoz **Balance — Ingestion** (`balance.transactions`, Kafka consume spans, PutItem latency).

## Mixed (read + write)

```bash
make load-mixed WORKERS=4 DURATION=30s
# starts Kafka producers in background + Gatling GET with same workers/duration
```

## Cache on vs off

No Redis yet. HTTP runs hit DynamoDB every time. When cache exists, compare:

```bash
make load-test CACHE_MODE=off VUS=50 DURATION=1m
make load-test CACHE_MODE=on  VUS=50 DURATION=1m
```

## Assertions (HTTP only)

- Failed requests &lt; `MAX_FAIL_PCT`
- Global p99 &lt; `P99_MS`
- GET success &gt; `100 - MAX_FAIL_PCT`

Kafka load reports `produced_total=…` on stdout (no Gatling assertions).

## Gradle (HTTP only)

```bash
./gradlew gatlingRun -DbaseUrl=http://localhost:8080 -Dvus=50 -Dduration=1m
```

`gatlingRun` is **not** hooked into `check`.
