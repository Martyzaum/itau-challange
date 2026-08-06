# Reviewer demo pipeline

One command to **boot the stack with SigNoz**, generate real traffic (HTTP + Kafka), run **chaos drills**, and leave dashboards populated for a code review walkthrough.

**Not a CI gate** — local Compose only.

## Quick start

```bash
# First time / cold machine (builds images, starts SigNoz — can take several minutes)
make review-demo

# Stack already up with OTLP (make obs-up)
SKIP_BOOTSTRAP=1 make review-demo-quick

# Longer windows (richer charts)
make review-demo-full
# or: DEMO_PROFILE=full make review-demo
```

| Target | What |
|--------|------|
| `make review-demo` | Full script (`DEMO_PROFILE=quick` by default) |
| `make review-demo-quick` | Short load/chaos windows (~5–8 min after boot) |
| `make review-demo-full` | Longer mixed/HTTP/Kafka windows |

Script: [`infra/review/demo-pipeline.sh`](../infra/review/demo-pipeline.sh).

## What it runs

| Phase | Action | Why / what to see in SigNoz |
|-------|--------|-----------------------------|
| Bootstrap | `BALANCE_CACHE_ENABLED=true make obs-up` | App + Redpanda + DynamoDB + Redis + SigNoz; OTLP on |
| Seed | `make load-seed` | Gatling account IDs in DynamoDB → GET 200 |
| Warm-up | small Kafka produce + light GET | first metrics/traces |
| Mixed load | `make load-mixed` | write path + read path together |
| HTTP baseline | Gatling GET closed model | API latency, cache hit/miss |
| Chaos: poison | invalid JSON → DLT | `balance.transactions` dlt, DLT topic |
| Chaos: Dynamo latency | Toxiproxy + HTTP load | slow Put/Get, CB open, possible 503 |
| Chaos: Redis stop | cache fail-open under load | GET still 200; cache errors / redis CB |
| Chaos: retry | pause Dynamo → retry-1 → recover | async hop + recovery GETs |
| Final healthy | Kafka + HTTP again | dashboards settle green |

Chaos phases **soft-fail** Gatling SLOs (latency/errors expected). Baseline and final healthy phases keep normal assertions.

## Knobs

| Env / Make var | Default | Meaning |
|----------------|---------|---------|
| `DEMO_PROFILE` / `PROFILE` (script) | `quick` | `quick` \| `full` (durations/VUs). Make uses `DEMO_PROFILE` so it does not clash with Gatling `PROFILE`. |
| `SKIP_BOOTSTRAP` | `0` | `1` = stack already up |
| `SKIP_LOAD` | `0` | `1` = seed + chaos only |
| `SKIP_CHAOS` | `0` | `1` = load only (fill dashboards) |
| `CACHE_ENABLED` | `true` | passed to `obs-up` as `BALANCE_CACHE_ENABLED` |
| `BASE_URL` | `http://localhost:8080` | API target |
| `SIGNOZ_URL` | `http://localhost:3301` | printed in summary |

Examples:

```bash
SKIP_BOOTSTRAP=1 SKIP_CHAOS=1 make review-demo-quick   # load only
CACHE_ENABLED=false SKIP_BOOTSTRAP=1 make review-demo  # no Redis cache
```

## Where to look after the run

| URL | Signal |
|-----|--------|
| http://localhost:3301 | SigNoz UI → **Dashboards** (Ingestion, API, Health/JVM) |
| http://localhost:8080/actuator/health | liveness / readiness / Dynamo |
| http://localhost:8081 | Redpanda Console (lag, topics) |
| `build/reports/gatling/` | Gatling HTML |
| `build/review-demo/*.log` | pipeline transcript |

Also:

```bash
make kafka-consume TOPIC=transacoes-financeiras-processadas.retry-1
make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT
make logs
```

## Safety

- Touches **local Compose** only (pause DynamoDB, stop Redis, Toxiproxy latency).
- Trap always clears latency toxic and unpauses DynamoDB / starts Redis.
- Prefer `SKIP_BOOTSTRAP=1` if you already have a long-lived `obs-up` session.

## Related

- Load knobs: [`LOAD.md`](LOAD.md)
- Chaos drills: [`CHAOS.md`](CHAOS.md)
- Capacity notes: [`CAPACITY.md`](CAPACITY.md)
- SigNoz: [`../infra/signoz/README.md`](../infra/signoz/README.md)
