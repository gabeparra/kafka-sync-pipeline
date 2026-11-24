# Developer entry points for the local lab. Run `make` for the list.

# Pick up optional overrides and pass them on to compose, scripts and tests.
-include .env
export

# Explicit COMPOSE wins; otherwise prefer Docker and fall back to Podman.
COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then echo docker compose; else echo podman compose; fi)

# Silence the banner podman prints when it delegates to an external compose provider.
export PODMAN_COMPOSE_WARNING_LOGS := false

# make seed N=500 ENTITIES=50
N        ?= 100
ENTITIES ?= 20

# make dlq ARGS="replay --error-type retries_exhausted"
ARGS     ?= list
API_PORT ?= 8000
CONSUMER_GROUP ?= reconciler

.DEFAULT_GOAL := help
.PHONY: help up down clean ps logs topics migrate seed publish publisher consumer api \
	dlq demo test test-unit chaos lint lag psql

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-z_-]+:.*## / {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start the stack, wait until healthy, apply migrations, create topics
	$(COMPOSE) up -d
	@scripts/wait_for_stack.sh
	@uv run python -m kafka_sync.migrations
	@scripts/init_topics.sh

down: ## Stop the stack (data volumes are kept)
	$(COMPOSE) down

clean: ## Stop the stack and delete its data volumes
	$(COMPOSE) down --volumes

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

topics: ## Create topics / converge their configs (idempotent)
	@scripts/init_topics.sh

migrate: ## Apply pending database migrations
	@uv run python -m kafka_sync.migrations

seed: ## Insert N sample events over ENTITIES entities into the outbox
	@scripts/seed.sh $(N) $(ENTITIES)

publish: ## Publish everything waiting in the outbox, then exit
	@uv run python -m kafka_sync.producer --once

publisher: ## Run the outbox publisher until interrupted
	@uv run python -m kafka_sync.producer

consumer: ## Run a reconciliation consumer until interrupted (start several to scale out)
	@uv run python -m kafka_sync.consumer

api: ## Serve /health, /metrics and /reconciliations on 127.0.0.1:API_PORT
	@uv run uvicorn --factory kafka_sync.api:create_app --host 127.0.0.1 --port $(API_PORT)

dlq: ## Dead letter queue tool, e.g. make dlq ARGS="show --partition 0 --offset 3"
	@uv run python -m kafka_sync.cli.dlq_replay $(ARGS)

demo: seed publish ## Seed, publish and reconcile one batch end to end, then show lag
	@uv run python -m kafka_sync.consumer --idle-exit 5
	@$(MAKE) --no-print-directory lag

test: ## Run unit and integration tests (stack must be up)
	uv run pytest

test-unit: ## Run only the tests that need no stack
	uv run pytest -m "not integration"

chaos: ## Run the disruptive tests too (restarts the local Kafka container)
	uv run pytest -m disruptive

lint: ## Check formatting and lint rules
	uv run ruff format --check .
	uv run ruff check .

lag: ## Show the reconciliation group's offsets and lag per partition
	@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:19092 --describe --group $(CONSUMER_GROUP)

psql: ## Open a psql shell on the lab database
	@$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'
