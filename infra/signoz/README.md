# SigNoz local

Single-node ClickHouse (embedded Keeper) + SigNoz 0.69 + OTEL collector.

| URL | Service |
|-----|---------|
| http://localhost:3301 | SigNoz UI |
| localhost:4317 | OTLP **gRPC** (traces + metrics) |
| http://localhost:4318 | OTLP HTTP (collector ainda escuta; app usa gRPC) |

```bash
make obs-up
make obs-down
make obs-logs
```

`make obs-up` already sets app env (no extra config). Overlay: `docker-compose.signoz.yml`.

`nginx-config.conf` is only for the SigNoz frontend image.

Data: `infra/signoz/data/` (gitignored).

Dashboards: `dashboards/`.
