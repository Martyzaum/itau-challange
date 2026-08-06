# Load tests (Gatling)

Load tests are **manual / optional** — not part of `./gradlew check` or CI gates.

Knobs mirror **k6** (`vus`, `duration`, stages/ramp, optional arrival-rate).

## What it measures

| Simulation | Path | Expectation |
|------------|------|-------------|
| `GetBalanceSimulation` | `GET /balances/{accountId}` | 200 for seeded accounts |

Feeder: `src/gatling/resources/account-ids.json` — `[{ "accountId": "..." }, ...]`.

## Prerequisites

```bash
make up          # app + DynamoDB + Redpanda
make load-seed   # put feeder accounts into AccountBalances
```

Optional write-path warm-up (random accounts — does **not** replace `load-seed`):

```bash
make load-ingest INGEST_COUNT=200
```

## Quick start (profiles)

```bash
make load-smoke    # 1 VU, 15s
make load-load     # 20 VUs, ramp 15s, hold 1m, ramp-down 10s
make load-stress   # 100 VUs, 2m
make load-spike    # 200 VUs, ramp 5s, hold 30s
make load-test     # custom defaults (20 VUs / 30s / ramp 10s)
```

## k6-style options

```bash
# closed model — N workers looping (like k6 --vus + --duration)
make load-test VUS=50 DURATION=1m RAMP=15s
make load-test WORKERS=30 DURATION=45s THINK_MS=50

# open model — fixed arrival rate (like k6 constant arrival-rate)
make load-test RPS=100 DURATION=1m RAMP=10s

# thresholds + target
make load-test VUS=40 DURATION=2m P99_MS=500 MAX_FAIL_PCT=0.5 BASE_URL=http://localhost:8080

# profile + overrides
make load-test PROFILE=load VUS=40 DURATION=90s
```

| Make var | Alias | Default | Meaning |
|----------|-------|---------|---------|
| `BASE_URL` | | `http://localhost:8080` | Target API |
| `PROFILE` | | `custom` | `smoke` \| `load` \| `stress` \| `spike` \| `custom` |
| `VUS` | `WORKERS`, `USERS` | profile / `20` | Concurrent workers (**closed** model) |
| `RPS` | | — | If set → **open** model (req/s injected) |
| `DURATION` | `DURATION_SECONDS` (+`s`) | profile / `30s` | Steady hold — `30`, `30s`, `1m`, `2m30s`, `1h` |
| `RAMP` | `RAMP_UP`, `RAMP_SECONDS` | profile / `10s` | Ramp up to target |
| `RAMP_DOWN` | | profile / `0s` | Ramp to 0 (closed only) |
| `THINK_MS` | | `0` | Pause between requests per VU (closed) |
| `P99_MS` | | `2000` | Fail run if global p99 above |
| `MAX_FAIL_PCT` | | `1.0` | Max failed request % |
| `CACHE_MODE` | | `off` | Report label only (`off` / `on`) |

Reports: `build/reports/gatling/` (HTML).

## Models

| Mode | When | Behaviour |
|------|------|-----------|
| **Closed** (default) | `VUS` / `WORKERS` set, no `RPS` | N concurrent workers loop for `RAMP + DURATION + RAMP_DOWN` |
| **Open** | `RPS` set | Inject ~RPS new one-shot users per second |

## Cache on vs off

No Redis yet (planned PR6). Runs are **cache off** (DynamoDB every request).

When cache exists:

```bash
make load-test CACHE_MODE=off VUS=50 DURATION=1m
# recreate app with cache on
make load-test CACHE_MODE=on  VUS=50 DURATION=1m
```

`CACHE_MODE` only labels the report / User-Agent.

## Assertions

- Failed requests &lt; `MAX_FAIL_PCT` (default 1%)
- Global p99 &lt; `P99_MS` (default 2000)
- GET success &gt; `100 - MAX_FAIL_PCT`

## Gradle (without Make)

```bash
./gradlew gatlingRun \
  -DbaseUrl=http://localhost:8080 \
  -Dprofile=custom \
  -Dvus=50 \
  -Dduration=1m \
  -DrampUp=15s \
  -DcacheMode=off
```

Task `gatlingRun` is **not** hooked into `check`.
