# SigNoz local

Stack single-node: **ClickHouse** (Keeper embutido) + **SigNoz 0.69** + **OTEL collector**.

Usada só em desenvolvimento/demo local — não é o stack de produção.

## Portas e URLs

| URL / endpoint | Serviço |
|----------------|---------|
| http://localhost:3301 | SigNoz UI |
| `localhost:4317` | OTLP **gRPC** (traces + metrics + logs) — caminho usado pela app |
| http://localhost:4318 | OTLP HTTP (collector escuta; a app **não** usa por padrão) |

## Comandos

```bash
make obs-up     # sobe app + infra + SigNoz (OTLP ligado)
make obs-down   # derruba o stack (volumes em infra/signoz/data/ são mantidos)
make obs-logs   # tail de otel-collector, query-service, frontend e app
```

`make obs-up` usa três compose files:

```text
docker-compose.yml
docker-compose.signoz.yml          # overlay da app (liga export OTLP)
infra/signoz/docker-compose.yml    # ClickHouse + SigNoz + collector
```

Primeiro boot pode levar **1–3 min** (ClickHouse + schema migrator).

## Overlay da app (`docker-compose.signoz.yml`)

O compose padrão (`make up`) força OTLP **desligado** (metrics, tracing **e** logs) — não há collector no path. O overlay liga os **três** no mesmo gRPC, sem config manual:

- métricas OTLP on (`step` 5s)
- tracing on (sampling 1.0)
- **logs OTLP on** (mesmo `:4317` / gRPC — não é caminho paralelo “opcional”)
- endpoints em `http://otel-collector:4317` com transporte **gRPC**
- `OTEL_SERVICE_NAME=itau-balance-api`
- `depends_on: otel-collector`

Defaults da app (`application.yaml`): metrics + tracing + logs OTLP **enabled=true** (apontam `:4317`). Lab Compose desliga; `obs-up` religa.

## Sinais exportados (gRPC `:4317`)

| Sinal | Transporte |
|-------|------------|
| traces | OTLP gRPC |
| metrics | OTLP gRPC |
| logs | OTLP gRPC (`OpenTelemetryAppender`) **+** JSON stdout (sempre) |

Pipelines do collector: `otel-collector-config.yaml` (receivers OTLP 4317/4318 → ClickHouse `signoz_traces` / `signoz_metrics` / `signoz_logs`).

## Arquivos relevantes

| Arquivo | Papel |
|---------|--------|
| `infra/signoz/docker-compose.yml` | Serviços SigNoz (ClickHouse, migrators, query-service, frontend, otel-collector) |
| `docker-compose.signoz.yml` | Overlay que habilita OTLP na app |
| `otel-collector-config.yaml` | Receivers/processors/exporters OTLP |
| `nginx-config.conf` | Só para a imagem do frontend SigNoz (UI na 3301) |
| `clickhouse-cluster.xml` | Cluster 1 nó + Keeper embutido |
| `dashboards/*.json` | Dashboards provisionados no start (`DASHBOARDS_PATH`) |

## Dados

Persistência local em `infra/signoz/data/` (**gitignored**):

- `data/clickhouse/` — dados do ClickHouse
- `data/signoz/` — SQLite do query-service (`signoz.db`)

`make obs-down` **não** apaga esses volumes.

## Dashboards

Carregados automaticamente no start do query-service. Detalhes e como popular tráfego: [`DASHBOARDS.md`](DASHBOARDS.md).

UI: http://localhost:3301 → **Dashboards**

| Dashboard | Foco |
|-----------|------|
| Overview | pipeline E2E (ingestão + API + cache + lag + latências) |
| Ingestion | saved / ignored_* / retry / DLT + PutItem |
| API | GET latency + GetItem + erros HTTP |
| Cache | hit / miss / put_failed + CB redis |
| Resilience | CBs (dynamodb-read/write, redis, kafka-produce) + lag + retry/DLT |
| Errors | degradação (DLT, retries, span errors, CB open) |
| Health/JVM | runtime JVM + taxa de spans |

## Demo para review

Pipeline que sobe o stack, gera carga e roda chaos para encher os painéis: [`docs/REVIEW.md`](../../docs/REVIEW.md) (`make review-demo`).
