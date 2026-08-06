.DEFAULT_GOAL := help

IMAGE := itau-balance-api
COMPOSE := docker compose
COMPOSE_SIGNOZ := docker compose -f docker-compose.yml -f docker-compose.signoz.yml -f infra/signoz/docker-compose.yml
HTTP_DIR := http
COMPOSE_PROJECT := $(notdir $(CURDIR))
PARTITIONS ?= 1
COUNT ?= 100

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the application image
	docker build --target runtime -t $(IMAGE) .

.PHONY: test
test: ## Run tests + coverage gate (min 90%) inside a container
	docker build --target test --progress=plain -t $(IMAGE)-test .

.PHONY: run
run: ## Start the application (foreground)
	$(COMPOSE) up --build

.PHONY: up
up: ## Start the application in the background
	$(COMPOSE) up --build -d

.PHONY: up-cache
up-cache: ## Start stack with Redis balance cache enabled (same as default make up)
	BALANCE_CACHE_ENABLED=true $(COMPOSE) up --build -d

.PHONY: up-no-cache
up-no-cache: ## Start stack with Redis balance cache disabled
	BALANCE_CACHE_ENABLED=false $(COMPOSE) up --build -d

.PHONY: up-no-ingest
up-no-ingest: ## Start stack with Kafka ingestion disabled
	TRANSACTIONS_INGESTION_ENABLED=false $(COMPOSE) up --build -d

.PHONY: up-secure
up-secure: ## Auth is on by default (api-keys.json). Also enables rate limit (key=local-dev-key)
	API_AUTH_ENABLED=true API_AUTH_KEYS=local-dev-key \
	API_RATE_LIMIT_ENABLED=true API_RATE_LIMIT_REQUESTS_PER_MINUTE=$${RATE_LIMIT:-120} \
	$(COMPOSE) up --build -d

.PHONY: logs
logs: ## Tail the application logs (when started with make up)
	$(COMPOSE) logs -f

.PHONY: stop
stop: ## Stop and remove containers started by docker compose
	$(COMPOSE) down

.PHONY: http
http: ## Call all .http files against the running app (no local deps, runs via Docker)
	docker run --rm \
		--add-host=host.docker.internal:host-gateway \
		-v "$(CURDIR)/$(HTTP_DIR)":/http -w /http \
		node:20-alpine sh -c 'npx --yes httpyac send "*.http" --all -e docker'

.PHONY: db-up
db-up: ## Start DynamoDB Local + web console and create AccountBalances table
	$(COMPOSE) up dynamodb dynamodb-seed dynamodb-admin -d

.PHONY: db-seed
db-seed: ## Re-run the seed job (table creation is idempotent, items are overwritten)
	$(COMPOSE) up dynamodb-seed

.PHONY: db-scan
db-scan: ## List account balances currently stored in DynamoDB
	$(COMPOSE) run --rm --entrypoint aws dynamodb-seed \
		dynamodb scan --table-name AccountBalances --endpoint-url http://dynamodb:8000 --region us-east-1

.PHONY: db-down
db-down: ## Stop DynamoDB Local + web console
	$(COMPOSE) stop dynamodb dynamodb-seed dynamodb-admin

.PHONY: kafka-up
kafka-up: ## Start Redpanda + Console and create transactions topics
	$(COMPOSE) up redpanda redpanda-seed redpanda-console -d

.PHONY: kafka-seed
kafka-seed: ## Re-run topic creation (transactions + DLT; idempotent)
	$(COMPOSE) up redpanda-seed

.PHONY: kafka-topic-create
kafka-topic-create: ## Create a Kafka topic on Redpanda (usage: make kafka-topic-create NAME=my-topic [PARTITIONS=3])
	@if [ -z "$(NAME)" ]; then \
		echo "NAME is required, e.g. make kafka-topic-create NAME=my-topic PARTITIONS=3"; \
		exit 1; \
	fi
	$(COMPOSE) run --rm --entrypoint rpk redpanda-seed \
		topic create $(NAME) --brokers redpanda:9092 --partitions $(PARTITIONS) --replicas 1

.PHONY: kafka-produce-accounts-events
kafka-produce-accounts-events: ## DEPRECATED — exits 1. Use kafka-produce-transactions-events (account-only payload is invalid)
	@bash infra/redpanda/produce-accounts-events.sh unused 1

.PHONY: kafka-produce-transactions-events
kafka-produce-transactions-events: ## Produce random transaction+account event JSON messages to a Kafka topic (usage: make kafka-produce-transactions-events TOPIC=my-topic [COUNT=100])
	@if [ -z "$(TOPIC)" ]; then \
		echo "TOPIC is required, e.g. make kafka-produce-transactions-events TOPIC=my-topic COUNT=50"; \
		exit 1; \
	fi
	$(COMPOSE) run --rm --entrypoint /bin/bash redpanda-seed \
		/redpanda-seed/produce-transactions-events.sh $(TOPIC) $(COUNT)

.PHONY: kafka-consume
kafka-consume: ## Print all messages on a Kafka topic (usage: make kafka-consume TOPIC=my-topic)
	@if [ -z "$(TOPIC)" ]; then \
		echo "TOPIC is required, e.g. make kafka-consume TOPIC=my-topic"; \
		exit 1; \
	fi
	$(COMPOSE) run --rm --entrypoint /bin/bash redpanda-seed -c \
		"timeout 5 rpk topic consume $(TOPIC) --brokers redpanda:9092 --format '%v\n' || true"

.PHONY: kafka-down
kafka-down: ## Stop Redpanda + Console
	$(COMPOSE) stop redpanda redpanda-seed redpanda-console

.PHONY: redis-up
redis-up: ## Start Redis (balance cache)
	$(COMPOSE) up redis -d

.PHONY: integration-test
integration-test: db-up kafka-up redis-up ## Run integration tests (DynamoDB + Redpanda + Redis)
	$(COMPOSE) wait dynamodb-seed redpanda-seed
	-$(COMPOSE) stop app 2>/dev/null || true
	./gradlew integrationTest

# --- Load test (Gatling; not part of ./gradlew check) — k6-style knobs ---
# Examples:
#   make load-test PROFILE=smoke
#   make load-test VUS=50 DURATION=1m RAMP=15s
#   make load-test WORKERS=30 DURATION=45s THINK_MS=50
#   make load-test RPS=100 DURATION=1m RAMP=10s
#   make load-smoke / load-load / load-stress / load-spike
BASE_URL ?= http://localhost:8080
PROFILE ?= custom
VUS ?=
WORKERS ?=
RPS ?=
DURATION ?=
RAMP ?=
RAMP_UP ?=
RAMP_DOWN ?=
THINK_MS ?=
P99_MS ?=
MAX_FAIL_PCT ?=
CACHE_MODE ?= off
INGEST_COUNT ?= 100
USERS ?=
RAMP_SECONDS ?=
DURATION_SECONDS ?=

.PHONY: load-seed
load-seed: ## Seed fixed Gatling account IDs into DynamoDB (AccountBalances)
	$(COMPOSE) run --rm \
		-v "$(CURDIR)/infra/load:/load:ro" \
		-v "$(CURDIR)/src/gatling/resources:/gatling-resources:ro" \
		-e ACCOUNT_IDS_JSON=/gatling-resources/account-ids.json \
		--entrypoint /bin/bash dynamodb-seed \
		/load/seed-balance-accounts.sh

.PHONY: load-ingest
load-ingest: ## Optional: produce random transaction events (write-path warm-up)
	$(MAKE) kafka-produce-transactions-events TOPIC=transacoes-financeiras-processadas COUNT=$(INGEST_COUNT)

# Kafka load (write path). Shares DURATION/WORKERS/RPS/PROFILE with HTTP load knobs.
KAFKA_TOPIC ?= transacoes-financeiras-processadas
BATCH_SIZE ?= 50
ELIGIBLE_PCT ?= 100

.PHONY: load-kafka
load-kafka: ## Kafka ingest load (k6-like: WORKERS, DURATION, RPS, PROFILE)
	@workers_val="$(WORKERS)"; \
	if [ -z "$$workers_val" ] && [ -n "$(VUS)" ]; then workers_val="$(VUS)"; fi; \
	dur_val="$(DURATION)"; \
	if [ -z "$$dur_val" ] && [ -n "$(DURATION_SECONDS)" ]; then dur_val="$(DURATION_SECONDS)s"; fi; \
	env_args="-e TOPIC=$(KAFKA_TOPIC) -e PROFILE=$(PROFILE) -e BATCH_SIZE=$(BATCH_SIZE) -e ELIGIBLE_PCT=$(ELIGIBLE_PCT) -e ACCOUNT_IDS_JSON=/gatling-resources/account-ids.json"; \
	[ -n "$$workers_val" ] && env_args="$$env_args -e WORKERS=$$workers_val"; \
	[ -n "$$dur_val" ] && env_args="$$env_args -e DURATION=$$dur_val"; \
	[ -n "$(RPS)" ] && env_args="$$env_args -e RPS=$(RPS)"; \
	echo "kafka-load $$env_args"; \
	$(COMPOSE) run --rm \
		-v "$(CURDIR)/infra/load:/load:ro" \
		-v "$(CURDIR)/src/gatling/resources:/gatling-resources:ro" \
		$$env_args \
		--entrypoint /bin/bash redpanda-seed \
		/load/produce-kafka-load.sh

.PHONY: load-kafka-smoke
load-kafka-smoke: ## Kafka smoke: 1 worker / 15s
	$(MAKE) load-kafka PROFILE=smoke

.PHONY: load-mixed
load-mixed: ## Kafka ingest + Gatling GET in parallel (DURATION/WORKERS shared)
	@dur_val="$(DURATION)"; \
	if [ -z "$$dur_val" ]; then dur_val=30s; fi; \
	workers_val="$(WORKERS)"; \
	if [ -z "$$workers_val" ] && [ -n "$(VUS)" ]; then workers_val="$(VUS)"; fi; \
	if [ -z "$$workers_val" ]; then workers_val=4; fi; \
	echo "mixed load: kafka WORKERS=$$workers_val DURATION=$$dur_val + gatling VUS=$$workers_val DURATION=$$dur_val"; \
	$(MAKE) load-kafka WORKERS=$$workers_val DURATION=$$dur_val PROFILE=$(PROFILE) RPS=$(RPS) & \
	kpid=$$!; \
	$(MAKE) load-test VUS=$$workers_val DURATION=$$dur_val PROFILE=custom RAMP=5s CACHE_MODE=$(CACHE_MODE); \
	wait $$kpid

API_KEY ?= local-dev-key
API_KEY_HEADER ?= X-API-Key

.PHONY: load-test
load-test: ## Gatling GET /balances (k6-like: VUS/WORKERS, DURATION, RAMP, RPS, PROFILE). Auth: API_KEY
	@vus_val="$(VUS)"; \
	if [ -z "$$vus_val" ] && [ -n "$(WORKERS)" ]; then vus_val="$(WORKERS)"; fi; \
	if [ -z "$$vus_val" ] && [ -n "$(USERS)" ]; then vus_val="$(USERS)"; fi; \
	dur_val="$(DURATION)"; \
	if [ -z "$$dur_val" ] && [ -n "$(DURATION_SECONDS)" ]; then dur_val="$(DURATION_SECONDS)s"; fi; \
	ramp_val="$(RAMP)"; \
	if [ -z "$$ramp_val" ] && [ -n "$(RAMP_UP)" ]; then ramp_val="$(RAMP_UP)"; fi; \
	if [ -z "$$ramp_val" ] && [ -n "$(RAMP_SECONDS)" ]; then ramp_val="$(RAMP_SECONDS)s"; fi; \
	args="-DbaseUrl=$(BASE_URL) -Dprofile=$(PROFILE) -DcacheMode=$(CACHE_MODE) -DapiKey=$(API_KEY) -DapiKeyHeader=$(API_KEY_HEADER)"; \
	[ -n "$$vus_val" ] && args="$$args -Dvus=$$vus_val"; \
	[ -n "$(RPS)" ] && args="$$args -Drps=$(RPS)"; \
	[ -n "$$dur_val" ] && args="$$args -Dduration=$$dur_val"; \
	[ -n "$$ramp_val" ] && args="$$args -DrampUp=$$ramp_val"; \
	[ -n "$(RAMP_DOWN)" ] && args="$$args -DrampDown=$(RAMP_DOWN)"; \
	[ -n "$(THINK_MS)" ] && args="$$args -DthinkMs=$(THINK_MS)"; \
	[ -n "$(P99_MS)" ] && args="$$args -Dp99Ms=$(P99_MS)"; \
	[ -n "$(MAX_FAIL_PCT)" ] && args="$$args -DmaxFailPct=$(MAX_FAIL_PCT)"; \
	echo "gatlingRun $$args"; \
	./gradlew gatlingRun $$args

.PHONY: load-smoke
load-smoke: ## Quick sanity: 1 VU / 15s
	$(MAKE) load-test PROFILE=smoke

.PHONY: load-load
load-load: ## Baseline load: 20 VUs / 1m
	$(MAKE) load-test PROFILE=load

.PHONY: load-stress
load-stress: ## Stress: 100 VUs / 2m
	$(MAKE) load-test PROFILE=stress

.PHONY: load-spike
load-spike: ## Spike: 200 VUs / 30s
	$(MAKE) load-test PROFILE=spike

.PHONY: obs-up
obs-up: ## Start app + infra + SigNoz (OTLP on). UI http://localhost:3301
	$(COMPOSE_SIGNOZ) up --build -d
	@echo ""
	@echo "SigNoz UI:     http://localhost:3301"
	@echo "OTLP HTTP:     http://localhost:4318"
	@echo "App:           http://localhost:8080"
	@echo "First boot may take 1-3 min (ClickHouse + migrator)."

.PHONY: obs-down
obs-down: ## Stop SigNoz stack + app overlay (volumes kept under infra/signoz/data)
	$(COMPOSE_SIGNOZ) down --remove-orphans

.PHONY: obs-logs
obs-logs: ## Tail SigNoz + app logs
	$(COMPOSE_SIGNOZ) logs -f otel-collector query-service frontend app

.PHONY: obs-ui
obs-ui: ## Print SigNoz UI URL
	@echo "http://localhost:3301"

# --- Chaos (local Compose; see docs/CHAOS.md) ---
.PHONY: chaos-retry-topics
chaos-retry-topics: ## Pause DynamoDB, produce events, assert retry-1, recover + GET 200
	@chmod +x infra/chaos/validate-retry-topics.sh
	COMPOSE_CMD="$(COMPOSE)" APP_URL=http://localhost:8080 COUNT=$${COUNT:-3} \
		./infra/chaos/validate-retry-topics.sh

.PHONY: chaos-dynamodb-pause
chaos-dynamodb-pause: ## Pause DynamoDB Local (store down)
	$(COMPOSE) pause dynamodb
	@echo "DynamoDB paused. Try GET /balances and ingest. Recover: make chaos-dynamodb-recover"

.PHONY: chaos-dynamodb-recover
chaos-dynamodb-recover: ## Unpause DynamoDB Local
	$(COMPOSE) unpause dynamodb
	@echo "DynamoDB unpaused."

.PHONY: chaos-dynamodb-latency
chaos-dynamodb-latency: ## Add Toxiproxy latency on DynamoDB path (LATENCY_MS=2000 JITTER_MS=500)
	@chmod +x infra/toxiproxy/add-latency.sh
	COMPOSE_CMD="$(COMPOSE)" LATENCY_MS=$${LATENCY_MS:-2000} JITTER_MS=$${JITTER_MS:-500} \
		./infra/toxiproxy/add-latency.sh
	@echo "Latency toxic on. GET may 503 after CB opens. Clear: make chaos-dynamodb-latency-clear"

.PHONY: chaos-dynamodb-latency-clear
chaos-dynamodb-latency-clear: ## Remove Toxiproxy latency toxic from DynamoDB proxy
	@chmod +x infra/toxiproxy/clear-latency.sh
	COMPOSE_CMD="$(COMPOSE)" ./infra/toxiproxy/clear-latency.sh
	@echo "Latency toxic cleared."

.PHONY: chaos-redis-stop
chaos-redis-stop: ## Stop Redis (cache fail-open expected if cache was on)
	$(COMPOSE) stop redis
	@echo "Redis stopped. GET should still work via DynamoDB. Recover: make chaos-redis-recover"

.PHONY: chaos-redis-recover
chaos-redis-recover: ## Start Redis again
	$(COMPOSE) start redis
	@echo "Redis started. Recreate app if cache-enabled and connection was lost: make up-cache"

.PHONY: chaos-kafka-stop
chaos-kafka-stop: ## Stop Redpanda broker
	$(COMPOSE) stop redpanda
	@echo "Kafka/Redpanda stopped. GET may still work. Recover: make chaos-kafka-recover"

.PHONY: chaos-kafka-recover
chaos-kafka-recover: ## Start Redpanda + re-seed topics
	$(COMPOSE) start redpanda
	$(COMPOSE) up redpanda-seed
	@echo "Kafka recovered; topics re-seeded if needed."

.PHONY: chaos-poison-dlt
chaos-poison-dlt: ## Publish invalid JSON to main topic (expect DLT)
	$(COMPOSE) run --rm --entrypoint /bin/bash redpanda-seed -c \
		"printf '%s\t%s\n' poison-account '{not-json' | rpk topic produce transacoes-financeiras-processadas --brokers redpanda:9092 -f '%k\t%v\n'"
	@echo "Poison published. Check DLT: make kafka-consume TOPIC=transacoes-financeiras-processadas.DLT"

.PHONY: chaos-ingestion-flag-off
chaos-ingestion-flag-off: ## Recreate app with ingestion disabled
	TRANSACTIONS_INGESTION_ENABLED=false $(COMPOSE) up --build -d app
	@echo "Ingestion off. New events should not be consumed. Recover: make chaos-ingestion-flag-recover"

.PHONY: chaos-ingestion-flag-recover
chaos-ingestion-flag-recover: ## Recreate app with ingestion enabled
	TRANSACTIONS_INGESTION_ENABLED=true $(COMPOSE) up --build -d app
	@echo "Ingestion on again."

# --- Reviewer demo (load + chaos + SigNoz fill; not a CI gate) ---
# DEMO_PROFILE separate from load PROFILE (smoke|load|stress|…).
DEMO_PROFILE ?= quick
SKIP_BOOTSTRAP ?= 0
SKIP_CHAOS ?= 0
SKIP_LOAD ?= 0
CACHE_ENABLED ?= true

.PHONY: review-demo
review-demo: ## Reviewer pipeline: obs-up + load + chaos + fill SigNoz (DEMO_PROFILE=quick|full)
	@chmod +x infra/review/demo-pipeline.sh
	DEMO_PROFILE=$(DEMO_PROFILE) SKIP_BOOTSTRAP=$(SKIP_BOOTSTRAP) SKIP_CHAOS=$(SKIP_CHAOS) \
	SKIP_LOAD=$(SKIP_LOAD) CACHE_ENABLED=$(CACHE_ENABLED) BASE_URL=$(BASE_URL) \
		./infra/review/demo-pipeline.sh

.PHONY: review-demo-quick
review-demo-quick: ## Alias: DEMO_PROFILE=quick (~5–8 min after warm stack)
	$(MAKE) review-demo DEMO_PROFILE=quick

.PHONY: review-demo-full
review-demo-full: ## Alias: DEMO_PROFILE=full (longer windows, richer dashboards)
	$(MAKE) review-demo DEMO_PROFILE=full

.PHONY: clean-containers
clean-containers: ## Remove every container for this project, running or stopped, including orphans
	$(COMPOSE) down --remove-orphans --volumes
	$(COMPOSE_SIGNOZ) down --remove-orphans --volumes 2>/dev/null || true
	@docker ps -aq --filter "label=com.docker.compose.project=$(COMPOSE_PROJECT)" | xargs -r docker rm -f

.PHONY: clean
clean: ## Remove built images
	docker rmi -f $(IMAGE) $(IMAGE)-test 2>/dev/null || true
