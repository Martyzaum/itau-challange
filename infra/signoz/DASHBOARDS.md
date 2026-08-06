# Dashboards

Provisioned automatically into SigNoz on query-service start (`DASHBOARDS_PATH`).

| Dashboard | File |
|-----------|------|
| Balance — Ingestion | `dashboards/balance-ingestion.json` |
| Balance — API | `dashboards/balance-api.json` |
| Balance — Health / JVM | `dashboards/balance-health.json` |

Open: http://localhost:3301 → **Dashboards**

Panels use ClickHouse SQL over metrics/traces from `itau-balance-api` (last 6h window in SQL).

If empty: generate traffic (`make kafka-produce-transactions-events` + `curl /balances/{id}`), wait ~30s for OTLP export, hard-refresh the UI.
