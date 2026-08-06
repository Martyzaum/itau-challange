# SigNoz local

Single-node ClickHouse (embedded Keeper) + SigNoz 0.69 + OTEL collector.

| URL | Service |
|-----|---------|
| http://localhost:3301 | SigNoz UI |
| localhost:4317 | OTLP **gRPC** (traces + metrics + logs) |
| http://localhost:4318 | OTLP HTTP (collector still listens) |

```bash
make obs-up
make obs-down
make obs-logs
```

`make obs-up` sets app env (no extra config). Overlay: `docker-compose.signoz.yml`.

Signals from the app over gRPC `:4317`:
- traces
- metrics
- logs (also keep JSON on stdout)

`nginx-config.conf` is only for the SigNoz frontend image.

Data: `infra/signoz/data/` (gitignored).

Dashboards: `dashboards/`.
