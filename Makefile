# Makefile لـ GlowPipe

.PHONY: help up down logs test clean

# Variables
COMPOSE_FILE = deployment/docker-compose.yml
ENV_FILE = deployment/.env

help:
	@echo "Available commands:"
	@echo "  make up       - Start all services (Airflow, Spark, Kafka, etc.)"
	@echo "  make down     - Stop all services"
	@echo "  make logs     - View logs of all services"
	@echo "  make test     - Run PySpark tests locally (batch)"
	@echo "  make clean    - Remove temporary files and volumes"

up:
	docker-compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) up -d

down:
	docker-compose -f $(COMPOSE_FILE) down

logs:
	docker-compose -f $(COMPOSE_FILE) logs -f

test:
	@echo "Running batch tests..."
	cd batch && python -m pytest tests/

clean:
	docker-compose -f $(COMPOSE_FILE) down -v
	rm -rf batch/airflow/logs/*
	find . -type d -name "__pycache__" -exec rm -rf {} +