SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
UV      := uv run

.PHONY: help env up down clean ps logs bootstrap producers producers-stop test lint fmt

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

.env:
	cp .env.example .env

env: .env ## Create .env from .env.example if missing

up: .env ## Start all services, wait until healthy, then bootstrap
	$(COMPOSE) up -d --wait
	$(MAKE) bootstrap

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
