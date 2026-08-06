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
kafka-produce-accounts-events: ## Produce random account-event JSON messages to a Kafka topic (usage: make kafka-produce-accounts-events TOPIC=my-topic [COUNT=100])
	@if [ -z "$(TOPIC)" ]; then \
		echo "TOPIC is required, e.g. make kafka-produce-accounts-events TOPIC=my-topic COUNT=50"; \
		exit 1; \
	fi
	$(COMPOSE) run --rm --entrypoint /bin/bash redpanda-seed \
		/redpanda-seed/produce-accounts-events.sh $(TOPIC) $(COUNT)

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

.PHONY: integration-test
integration-test: db-up kafka-up ## Run all integration tests against live DynamoDB + Redpanda
	$(COMPOSE) wait dynamodb-seed redpanda-seed
	./gradlew integrationTest

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

.PHONY: clean-containers
clean-containers: ## Remove every container for this project, running or stopped, including orphans
	$(COMPOSE) down --remove-orphans --volumes
	$(COMPOSE_SIGNOZ) down --remove-orphans --volumes 2>/dev/null || true
	@docker ps -aq --filter "label=com.docker.compose.project=$(COMPOSE_PROJECT)" | xargs -r docker rm -f

.PHONY: clean
clean: ## Remove built images
	docker rmi -f $(IMAGE) $(IMAGE)-test 2>/dev/null || true
