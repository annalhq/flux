SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
UV      := uv run

.PHONY: help env up down clean ps logs test lint fmt

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

.env:
	cp .env.example .env

env: .env ## Create .env from .env.example if missing

up: .env ## Start all services and wait until healthy
	$(COMPOSE) up -d --wait

down: ## Stop all services, keep volumes
	$(COMPOSE) down

clean: ## Stop all services and delete volumes
	$(COMPOSE) down -v --remove-orphans

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Follow logs (S=<service> to filter)
	$(COMPOSE) logs -f --tail=100 $(S)

test: ## Run unit and contract tests
	$(UV) pytest tests

lint: ## Lint and check formatting
	$(UV) ruff check .
	$(UV) ruff format --check .

fmt: ## Fix lint issues and format
	$(UV) ruff check --fix .
	$(UV) ruff format .
