.PHONY: help setup start stop restart logs clean test lint

help:
	@echo "Available commands:"
	@echo "  make setup      - Initial setup (create .env, directories)"
	@echo "  make start      - Start all services"
	@echo "  make stop       - Stop all services"
	@echo "  make restart    - Restart all services"
	@echo "  make logs       - View logs"
	@echo "  make clean      - Clean up containers and volumes"
	@echo "  make test       - Run tests"
	@echo "  make lint       - Run linters"

setup:
	@echo "Setting up project..."
	cp .env.example .env
	mkdir -p airflow/dags airflow/logs airflow/plugins
	mkdir -p spark/jobs spark/utils
	mkdir -p ingestion
	mkdir -p dbt/models/staging dbt/models/intermediate dbt/models/marts/core dbt/models/marts/analytics
	mkdir -p tests/unit tests/integration
	mkdir -p notebooks
	mkdir -p docs/images
	@echo "Setup complete! Edit .env file if needed."

start:
	@echo "Starting services..."
	docker compose up -d
	@echo "Services started!"
	@echo "Airflow UI: http://localhost:8080 (airflow/airflow)"
	@echo "MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "Metabase: http://localhost:3000"

stop:
	@echo "Stopping services..."
	docker compose down

restart:
	@echo "Restarting services..."
	docker compose restart

logs:
	docker compose logs -f

clean:
	@echo "Cleaning up..."
	docker compose down -v
	rm -rf airflow/logs/*
	@echo "Cleanup complete!"

test:
	@echo "Running tests..."
	pytest tests/ -v

lint:
	@echo "Running linters..."
	black --check .
	flake8 .
	mypy .