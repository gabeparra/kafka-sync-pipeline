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

.DEFAULT_GOAL := help
.PHONY: help up down clean ps logs topics migrate seed test lag psql

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-z_-]+:.*## / {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start the stack, wait until healthy, apply migrations, create topics
	$(COMPOSE) up -d
	@scripts/wait_for_stack.sh
	@scripts/migrate.sh
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
	@scripts/migrate.sh

seed: ## Insert N sample events over ENTITIES entities into the outbox
	@scripts/seed.sh $(N) $(ENTITIES)

test: ## Run the test suite (stack must be up)
	uv run pytest

lag: ## Show consumer group offsets and lag (empty until a consumer has run)
	@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:19092 --describe --all-groups

psql: ## Open a psql shell on the lab database
	@$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'
