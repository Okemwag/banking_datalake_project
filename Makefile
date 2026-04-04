COMPOSE := docker compose -f infra/docker/docker-compose.yml
PYTHON := python3
SPARK_EXEC := $(COMPOSE) exec -T spark
AIRFLOW_EXEC := $(COMPOSE) exec -T airflow-webserver

.PHONY: dev init down test lint create-tables backfill dq dbt-run dbt-test

dev:
	$(COMPOSE) up airflow-init minio-init
	$(COMPOSE) up -d postgres minio kafka hive-metastore trino spark airflow-webserver airflow-scheduler

init:
	$(COMPOSE) up -d postgres minio hive-metastore spark
	$(SPARK_EXEC) python3 scripts/create_tables.py

down:
	$(COMPOSE) down -v

test:
	pytest -q

lint:
	ruff check .

create-tables:
	$(COMPOSE) up -d postgres minio hive-metastore spark
	$(SPARK_EXEC) python3 scripts/create_tables.py

backfill:
	$(SPARK_EXEC) python3 scripts/backfill.py --start-date 2026-01-01 --end-date 2026-01-07

dq:
	$(SPARK_EXEC) python3 -c "from quality.validator import run_layer_validation; print(run_layer_validation('silver'))"

dbt-run:
	$(AIRFLOW_EXEC) dbt run --project-dir /opt/project/transforms --profiles-dir /opt/project/transforms

dbt-test:
	$(AIRFLOW_EXEC) dbt test --project-dir /opt/project/transforms --profiles-dir /opt/project/transforms
