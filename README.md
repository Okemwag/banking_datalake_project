# Banking Medallion Lakehouse

Production-style Bronze / Silver / Gold lakehouse scaffold built around Delta Lake on S3-compatible storage, Spark processing, Airflow orchestration, Trino querying, dbt SQL transforms, and explicit handling for schema drift, corrupt data, duplicates, and late arrivals.

## Stack

- Object storage: MinIO locally, S3-compatible interfaces in cloud
- Table format: Delta Lake
- Compute: Spark
- Orchestration: Airflow
- Query layer: Trino + dbt
- Streaming ingress: Kafka
- Data quality: expectation suites + quarantine handling
- Observability: metrics emitters + OpenLineage payload builders

## Repository Layout

- `infra/` contains Terraform and local Docker infrastructure
- `ingestion/` contains Kafka and batch ingestion entrypoints
- `lakehouse/` contains Bronze, Silver, and Gold logic
- `transforms/` contains the dbt project
- `quality/` contains expectation suites and validation logic
- `optimization/` contains compaction, Z-order, vacuum, and partition hygiene utilities
- `orchestration/` contains Airflow DAGs, plugins, and callbacks
- `observability/` contains metrics and lineage helpers
- `catalog/` holds schema and classification metadata
- `scripts/` contains operational entrypoints

## Local Quick Start

1. Copy `.env.example` to `.env`.
2. Run `make dev`.
3. Run `make create-tables`.
4. Seed Kafka with `python3 scripts/seed_kafka.py`.
5. Trigger Airflow DAGs or run the CLI scripts directly.
6. Build dbt models with `make dbt-run` and validate with `make dbt-test`.

## Design Highlights

- Bronze is append-only and keeps raw semantics intact.
- Silver standardizes types, normalizes time zones to UTC, deduplicates on natural keys, and handles late arrivals via merge semantics.
- Gold uses incremental `MERGE INTO` updates for domain marts rather than full-table rewrites.
- Schema evolution is handled through Delta auto-merge rather than hard failures.
- Corrupt or partial records are quarantined into dedicated Delta locations instead of blocking the main path.
- Partitioning is based on ingestion dates to avoid high-cardinality partition explosion.

## Key Paths

- Local compose stack: `infra/docker/docker-compose.yml`
- Airflow DAGs: `orchestration/dags/`
- Delta jobs: `lakehouse/`
- dbt models: `transforms/models/`
- Config: `config/`

