# SigNoz local

Single-node ClickHouse (embedded Keeper, no ZooKeeper) + SigNoz 0.69 + OTEL collector.

| URL | Service |
|-----|---------|
| http://localhost:3301 | SigNoz UI |
| http://localhost:4318 | OTLP HTTP |
| http://localhost:4317 | OTLP gRPC |

```bash
make obs-up
make obs-down
make obs-logs
```

`nginx-config.conf` is only for the SigNoz frontend image.

Data: `infra/signoz/data/` (gitignored).

Dashboards: `dashboards/`.
