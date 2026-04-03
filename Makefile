COMPOSE := docker compose -f infra/docker/docker-compose.yml
PYTHON := python3

.PHONY: dev init down test lint create-tables backfill dq dbt-run dbt-test

dev:
	$(COMPOSE) up airflow-init minio-init
	$(COMPOSE) up -d postgres minio kafka hive-metastore trino spark airflow-webserver airflow-scheduler

init:
	$(PYTHON) scripts/create_tables.py

down:
	$(COMPOSE) down -v

test:
	pytest -q

lint:
	ruff check .

create-tables:
	$(PYTHON) scripts/create_tables.py

backfill:
	$(PYTHON) scripts/backfill.py --start-date 2026-01-01 --end-date 2026-01-07

dq:
	$(PYTHON) -c "from quality.validator import run_layer_validation; print(run_layer_validation('silver'))"

dbt-run:
	dbt run --project-dir transforms --profiles-dir transforms

dbt-test:
	dbt test --project-dir transforms --profiles-dir transforms

