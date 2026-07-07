SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
UV      := uv run
RECON_CLI := /opt/recon-venv/bin/python -m flux_recon.cli

.PHONY: help env up down clean ps logs bootstrap jobs jobs-status jobs-down lake-stats recon producers producers-stop test lint fmt

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

.env:
	cp .env.example .env

env: .env ## Create .env from .env.example if missing

up: .env ## Start all services, wait until healthy, bootstrap, submit Flink jobs
	$(COMPOSE) up -d --wait
	$(MAKE) bootstrap
	$(MAKE) jobs

down: ## Stop all services, keep volumes
	$(COMPOSE) --profile producers down

clean: ## Stop all services and delete volumes
	$(COMPOSE) --profile producers down -v --remove-orphans

ps: ## Show service status
	$(COMPOSE) --profile producers ps

logs: ## Follow logs (S=<service> to filter)
	$(COMPOSE) --profile producers logs -f --tail=100 $(S)

bootstrap: .env ## Create topics, register schemas, apply ClickHouse DDL (idempotent)
	$(UV) python scripts/bootstrap.py

jobs: .env ## Render and submit Flink SQL jobs; skips jobs already running
	$(UV) python flink/submit.py up

jobs-down: ## Stop Flink jobs with savepoints (restored by the next `make jobs`)
	$(UV) python flink/submit.py down

jobs-status: ## Flink job state and checkpoint health
	$(UV) python flink/submit.py status

lake-stats: ## Iceberg market.trades row, partition and snapshot counts
	$(COMPOSE) exec airflow $(RECON_CLI) lake-stats

recon: ## Reconcile one closed hour (HOUR=YYYY-MM-DDTHH, UTC); writes recon tables, not Airflow runs
	@test -n "$(HOUR)" || { echo "usage: make recon HOUR=2026-11-11T13"; exit 2; }
	$(COMPOSE) exec airflow sh -c 'set -e; \
		start=$(HOUR):00:00+00:00; end=$$(date -u -d "$(HOUR):00:00Z + 1 hour" +%Y-%m-%dT%H:00:00+00:00); \
		$(RECON_CLI) freshness $$start $$end; \
		$(RECON_CLI) reconcile $$start $$end --run-id manual__$(HOUR)'

producers: .env ## Build and start producers (PRODUCER_MODE=synthetic|live)
	$(COMPOSE) --profile producers up -d --build --wait producers

producers-stop: ## Stop producers
	$(COMPOSE) --profile producers stop producers

test: ## Run unit and contract tests
	$(UV) pytest tests

lint: ## Lint and check formatting
	$(UV) ruff check .
	$(UV) ruff format --check .

fmt: ## Fix lint issues and format
	$(UV) ruff check --fix .
	$(UV) ruff format .
