# SigNoz local (challenge)

Pinned **SigNoz 0.69** + ClickHouse + OTEL collector for local demo of metrics/traces from the balance API.

| URL | What |
|-----|------|
| http://localhost:3301 | SigNoz UI |
| http://localhost:4318 | OTLP HTTP (metrics + traces) |
| http://localhost:4317 | OTLP gRPC |

## Why `nginx-config.conf`?

Only for the **`signoz/frontend`** image (static UI + proxy `/api` → `query-service`).  
It is **not** a reverse proxy in front of the Kotlin app.

```text
Browser → :3301 (frontend/nginx) → query-service → ClickHouse
App     → :4318 (otel-collector) → ClickHouse
```

## Start / stop

From repo root:

```bash
make obs-up      # SigNoz stack + app with OTLP enabled
make obs-down    # tear down SigNoz data stack (keeps main compose project separate)
make obs-ui      # prints UI URL
```

First boot can take **1–3 minutes** (ClickHouse + schema migrator).

## Generate signal

```bash
make kafka-produce-transactions-events TOPIC=transacoes-financeiras-processadas COUNT=20
curl -s "http://localhost:8080/balances/<accountId>"
```

Then in SigNoz: **Services** → `itau-balance-api`, **Traces**, **Dashboards** (see `dashboards/`).

## Data

Runtime data under `infra/signoz/data/` (gitignored). Reset:

```bash
make obs-down
rm -rf infra/signoz/data
```
